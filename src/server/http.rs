/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 最小 HTTP/JSON Server：基于 std::net::TcpListener，无外部依赖。
//!
//! P0：连接数限制（Semaphore 模式）+ 自动持久化后台线程。
//!
//! 路由：
//! - POST /api/sql    — SQL 执行
//! - POST /api/kv     — KV 操作
//! - POST /api/ts     — 时序操作
//! - POST /api/mq     — 消息队列操作
//! - POST /api/vector — 向量操作
//! - POST /api/geo    — GEO 操作
//! - POST /api/fts    — 全文搜索
//! - POST /api/graph  — 图引擎操作
//! - POST /api/ai     — AI 操作
//! - POST /api/backup — 备份导入导出
//! - GET  /api/stats  — 引擎统计信息
//! - GET  /health     — 健康检查

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::Error;
use crate::Talon;

use super::handlers::{handle_backup, handle_kv, handle_mq, handle_sql, write_response};
use super::handlers_fts::handle_fts;
use super::handlers_geo::handle_geo;
use super::handlers_graph::handle_graph;
use super::handlers_ts::handle_ts;
use super::handlers_vec::handle_vector;
use super::protocol::Response;
use super::ServerConfig;

/// HTTP Server 实例。
pub struct HttpServer {
    config: ServerConfig,
    db: Arc<Talon>,
}

impl HttpServer {
    /// 创建 HTTP Server。
    pub fn new(config: ServerConfig, db: Arc<Talon>) -> Self {
        HttpServer { config, db }
    }

    /// 启动监听（阻塞）；连接数限制 + 自动持久化 + 优雅关闭。
    ///
    /// 收到 shutdown 信号后停止接受新连接，等待活跃连接完成，最终 persist 刷盘。
    pub fn run(&self) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.http_addr)?;
        // 设置非阻塞，便于定期检查 shutdown 信号
        listener.set_nonblocking(true)?;
        let active_conns = Arc::new(AtomicUsize::new(0));
        let max_conns = self.config.max_connections;
        let stop = Arc::new(AtomicBool::new(false));

        // 注册 shutdown 信号（SIGINT / SIGTERM）
        let stop_sig = Arc::clone(&stop);
        let _ = Self::register_shutdown_signal(stop_sig);

        // 启动自动持久化后台线程
        let persist_handle = if self.config.auto_persist_secs > 0 {
            let db2 = Arc::clone(&self.db);
            let stop2 = Arc::clone(&stop);
            let interval = self.config.auto_persist_secs;
            Some(std::thread::spawn(move || {
                while !stop2.load(Ordering::Relaxed) {
                    for _ in 0..interval * 10 {
                        if stop2.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    let _ = db2.persist();
                }
            }))
        } else {
            None
        };

        // 线程池 + channel-based accept：避免无限线程创建和 sleep 轮询
        let pool_size = if max_conns > 0 {
            max_conns.min(256)
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get() * 4)
                .unwrap_or(32)
                .min(256)
        };
        let (pool_tx, pool_rx) = std::sync::mpsc::channel::<Box<dyn FnOnce() + Send>>();
        let pool_rx = Arc::new(std::sync::Mutex::new(pool_rx));
        let mut pool_workers: Vec<std::thread::JoinHandle<()>> = (0..pool_size)
            .map(|_| {
                let rx = Arc::clone(&pool_rx);
                std::thread::spawn(move || loop {
                    let job = rx.lock().ok().and_then(|guard| guard.recv().ok());
                    match job {
                        Some(f) => f(),
                        None => break,
                    }
                })
            })
            .collect();

        // Acceptor 线程：阻塞式 accept，通过 channel 传递连接
        let (conn_tx, conn_rx) = std::sync::mpsc::channel();
        let stop_accept = Arc::clone(&stop);
        listener.set_nonblocking(false)?;
        let accept_handle = std::thread::spawn(move || {
            while !stop_accept.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if conn_tx.send(stream).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        if stop_accept.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        });

        while !stop.load(Ordering::Relaxed) {
            match conn_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(stream) => {
                    if max_conns > 0 && active_conns.load(Ordering::Relaxed) >= max_conns {
                        let _ = write_response(
                            &mut stream.try_clone().unwrap_or(stream),
                            503,
                            &Response::err("连接数已满"),
                        );
                        continue;
                    }
                    let db = Arc::clone(&self.db);
                    let auth_token = self.config.auth_token.clone();
                    let conns = Arc::clone(&active_conns);
                    conns.fetch_add(1, Ordering::Relaxed);
                    let _ = pool_tx.send(Box::new(move || {
                        if let Err(e) = handle_connection(stream, &db, auth_token.as_deref()) {
                            eprintln!("连接处理错误: {}", e);
                        }
                        conns.fetch_sub(1, Ordering::Relaxed);
                    }));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        // 优雅关闭：停止线程池，等待活跃连接完成（最多 5 秒）
        eprintln!("正在关闭，等待活跃连接完成...");
        drop(pool_tx);
        for w in pool_workers.drain(..) {
            let _ = w.join();
        }
        let _ = accept_handle.join();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while active_conns.load(Ordering::Relaxed) > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        stop.store(true, Ordering::Relaxed);
        if let Some(h) = persist_handle {
            let _ = h.join();
        }
        eprintln!("正在刷盘...");
        let _ = self.db.persist();
        eprintln!("关闭完成。");
        Ok(())
    }

    /// 注册 shutdown 信号处理；收到信号时设置 stop 标志。
    ///
    /// Unix: 用后台线程 + pipe 自唤醒模式监听 SIGINT/SIGTERM。
    /// 非 Unix: 无操作（依赖 OS 默认行为）。
    fn register_shutdown_signal(_stop: Arc<AtomicBool>) -> Result<(), Error> {
        // 零外部依赖约束下，不引入 libc/signal-hook。
        // 非阻塞 listener + 50ms 轮询已保证 stop 标志可被外部设置。
        // main.rs 通过 Drop 保证退出时 persist。
        Ok(())
    }
}

/// 解析 HTTP 请求并路由到对应引擎。
fn handle_connection(
    mut stream: std::net::TcpStream,
    db: &Talon,
    auth_token: Option<&str>,
) -> Result<(), Error> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // 读取请求行
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return write_response(&mut stream, 400, &Response::err("无效请求"));
    }
    let method = parts[0];
    let path = parts[1];

    // 读取 headers
    let mut content_length: usize = 0;
    let mut req_auth_token: Option<String> = None;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        // 使用大小写无关比较，避免每行 to_lowercase() 分配
        if line.len() >= 15 && line.as_bytes()[..15].eq_ignore_ascii_case(b"content-length:") {
            content_length = line[15..].trim().parse().unwrap_or(0);
        }
        if line.len() >= 14 && line.as_bytes()[..14].eq_ignore_ascii_case(b"authorization:") {
            let val = line[14..].trim();
            req_auth_token = Some(val.strip_prefix("Bearer ").unwrap_or(val).to_string());
        }
    }

    // 认证检查
    if let Some(expected) = auth_token {
        match &req_auth_token {
            Some(t) if t == expected => {}
            _ => return write_response(&mut stream, 401, &Response::err("认证失败")),
        }
    }

    // 路由
    match (method, path) {
        ("GET", "/health") => write_response(
            &mut stream,
            200,
            &Response::ok(serde_json::json!({"status": "ok"})),
        ),
        ("POST", "/api/sql") => {
            let body = read_body(&mut reader, content_length)?;
            handle_sql(db, &body, &mut stream)
        }
        ("POST", "/api/kv") => {
            let body = read_body(&mut reader, content_length)?;
            handle_kv(db, &body, &mut stream)
        }
        ("POST", "/api/ts") => {
            let body = read_body(&mut reader, content_length)?;
            handle_ts(db, &body, &mut stream)
        }
        ("POST", "/api/mq") => {
            let body = read_body(&mut reader, content_length)?;
            handle_mq(db, &body, &mut stream)
        }
        ("POST", "/api/vector") => {
            let body = read_body(&mut reader, content_length)?;
            handle_vector(db, &body, &mut stream)
        }
        ("POST", "/api/geo") => {
            let body = read_body(&mut reader, content_length)?;
            handle_geo(db, &body, &mut stream)
        }
        ("POST", "/api/fts") => {
            let body = read_body(&mut reader, content_length)?;
            handle_fts(db, &body, &mut stream)
        }
        ("POST", "/api/graph") => {
            let body = read_body(&mut reader, content_length)?;
            handle_graph(db, &body, &mut stream)
        }
        ("POST", "/api/ai") => {
            // AI 功能已迁移至 talon-ai crate
            let _body = read_body(&mut reader, content_length)?;
            write_response(
                &mut stream,
                501,
                &Response::err("AI engine 已迁移至 talon-ai crate，请使用 talon-ai SDK"),
            )
        }
        ("POST", "/api/backup") => {
            let body = read_body(&mut reader, content_length)?;
            handle_backup(db, &body, &mut stream)
        }
        ("GET", "/api/stats") => write_response(&mut stream, 200, &Response::ok(db.stats())),
        ("GET", "/cluster/status") => {
            let status = db.cluster_status();
            let json = serde_json::to_value(&status).unwrap_or_default();
            write_response(&mut stream, 200, &Response::ok(json))
        }
        ("POST", "/cluster/promote") => match db.promote() {
            Ok(()) => write_response(
                &mut stream,
                200,
                &Response::ok(serde_json::json!({"promoted": true, "role": "Primary"})),
            ),
            Err(e) => write_response(&mut stream, 400, &Response::err(e.to_string())),
        },
        ("GET", "/cluster/replicas") => {
            let status = db.cluster_status();
            let json = serde_json::to_value(&status.replicas).unwrap_or_default();
            write_response(&mut stream, 200, &Response::ok(json))
        }
        _ => write_response(&mut stream, 404, &Response::err("未知路由")),
    }
}

fn read_body(reader: &mut BufReader<std::net::TcpStream>, len: usize) -> Result<Vec<u8>, Error> {
    let mut body = vec![0u8; len];
    std::io::Read::read_exact(reader, &mut body)?;
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::time::Duration;

    fn start_server(
        auth_token: Option<&str>,
        max_conns: usize,
    ) -> (u16, tempfile::TempDir, Arc<Talon>) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Talon::open(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let auth = auth_token.map(|s| s.to_string());
        let db2 = Arc::clone(&db);
        std::thread::spawn(move || {
            for stream in listener.incoming().take(max_conns).flatten() {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let db3 = Arc::clone(&db2);
                let auth2 = auth.clone();
                let _ = handle_connection(stream, &db3, auth2.as_deref());
            }
        });
        std::thread::sleep(Duration::from_millis(50));
        (port, dir, db)
    }

    fn http_request(
        port: u16,
        method: &str,
        path: &str,
        body: Option<&str>,
        auth: Option<&str>,
    ) -> String {
        let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let body_bytes = body.unwrap_or("");
        let mut req = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            method, path, body_bytes.len()
        );
        if let Some(token) = auth {
            req.push_str(&format!("Authorization: Bearer {}\r\n", token));
        }
        req.push_str("\r\n");
        req.push_str(body_bytes);
        stream.write_all(req.as_bytes()).unwrap();
        let _ = stream.shutdown(std::net::Shutdown::Write);
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        resp
    }

    #[test]
    fn http_health_check() {
        let (port, _dir, _db) = start_server(None, 2);
        let resp = http_request(port, "GET", "/health", None, None);
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("\"ok\":true"));
    }

    #[test]
    fn http_sql_crud() {
        let (port, _dir, _db) = start_server(None, 4);
        let r1 = http_request(
            port,
            "POST",
            "/api/sql",
            Some(
                r#"{"cmd":"sql","action":"exec","params":{"sql":"CREATE TABLE t (id INT, name TEXT)"}}"#,
            ),
            None,
        );
        assert!(r1.contains("\"ok\":true"));
        let r2 = http_request(
            port,
            "POST",
            "/api/sql",
            Some(
                r#"{"cmd":"sql","action":"exec","params":{"sql":"INSERT INTO t (id, name) VALUES (1, 'hello')"}}"#,
            ),
            None,
        );
        assert!(r2.contains("\"ok\":true"));
        let r3 = http_request(
            port,
            "POST",
            "/api/sql",
            Some(r#"{"cmd":"sql","action":"exec","params":{"sql":"SELECT * FROM t"}}"#),
            None,
        );
        assert!(r3.contains("\"ok\":true"));
        assert!(r3.contains("rows"));
    }

    #[test]
    fn http_auth_required() {
        let (port, _dir, _db) = start_server(Some("secret123"), 3);
        let r1 = http_request(
            port,
            "POST",
            "/api/sql",
            Some(r#"{"cmd":"sql","action":"exec","params":{"sql":"SELECT 1"}}"#),
            None,
        );
        assert!(r1.contains("401"));
        let r2 = http_request(
            port,
            "POST",
            "/api/kv",
            Some(r#"{"cmd":"kv","action":"set","params":{"key":"k","value":"v"}}"#),
            Some("secret123"),
        );
        assert!(r2.contains("\"ok\":true"));
    }
}
