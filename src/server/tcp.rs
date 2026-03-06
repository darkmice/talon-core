/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TCP 二进制帧协议 Server。
//!
//! 帧格式：`[4 byte big-endian length][JSON payload]`
//! 请求/响应与 `talon_execute` JSON-RPC 格式完全一致。
//! 最大帧大小 16 MB，防止恶意超大请求。
//!
//! 网络层使用固定线程池 + channel-based accept，避免无限线程创建和 sleep 轮询。

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::Talon;

use super::ServerConfig;

/// 最大帧大小：16 MB。
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// 固定大小线程池：零外部依赖，基于 mpsc channel 分发任务。
struct ThreadPool {
    sender: Option<std::sync::mpsc::Sender<Box<dyn FnOnce() + Send>>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl ThreadPool {
    fn new(size: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<Box<dyn FnOnce() + Send>>();
        let rx = Arc::new(Mutex::new(rx));
        let workers = (0..size)
            .map(|_| {
                let rx = Arc::clone(&rx);
                std::thread::spawn(move || loop {
                    let job = { rx.lock().ok().and_then(|guard| guard.recv().ok()) };
                    match job {
                        Some(f) => f(),
                        None => break,
                    }
                })
            })
            .collect();
        ThreadPool {
            sender: Some(tx),
            workers,
        }
    }

    fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> bool {
        self.sender
            .as_ref()
            .map(|tx| tx.send(Box::new(f)).is_ok())
            .unwrap_or(false)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.sender.take();
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

/// TCP Server 实例。
pub struct TcpServer {
    config: ServerConfig,
    db: Arc<Talon>,
    /// TCP 监听地址（独立于 HTTP）。
    tcp_addr: String,
}

impl TcpServer {
    /// 创建 TCP Server。
    pub fn new(config: ServerConfig, db: Arc<Talon>, tcp_addr: String) -> Self {
        TcpServer {
            config,
            db,
            tcp_addr,
        }
    }

    /// 启动 TCP 监听（阻塞）。
    /// 使用固定线程池处理连接，acceptor 线程通过 channel 传递新连接，
    /// 主循环通过 recv_timeout 检查 stop 信号，不再 sleep 轮询。
    pub fn run(&self, stop: Arc<AtomicBool>) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.tcp_addr)?;
        let active_conns = Arc::new(AtomicUsize::new(0));
        let max_conns = self.config.max_connections;
        let pool_size = if max_conns > 0 {
            max_conns.min(256)
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get() * 4)
                .unwrap_or(32)
                .min(256)
        };
        let pool = ThreadPool::new(pool_size);

        // Acceptor 线程：阻塞式 accept，通过 channel 传递连接
        let (conn_tx, conn_rx) = std::sync::mpsc::channel();
        let stop_accept = Arc::clone(&stop);
        let accept_handle = std::thread::spawn(move || {
            listener
                .set_nonblocking(false)
                .expect("set_nonblocking failed");
            while !stop_accept.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _addr)) => {
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
                        let _ = write_frame(
                            &mut stream.try_clone().unwrap_or(stream),
                            r#"{"ok":false,"error":"max connections reached"}"#.as_bytes(),
                        );
                        continue;
                    }
                    let db = Arc::clone(&self.db);
                    let auth_token = self.config.auth_token.clone();
                    let conns = Arc::clone(&active_conns);
                    conns.fetch_add(1, Ordering::Relaxed);
                    pool.execute(move || {
                        let _ = handle_tcp_conn(stream, &db, auth_token.as_deref());
                        conns.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        drop(pool);
        let _ = accept_handle.join();
        Ok(())
    }
}

/// 读取一帧：4 字节大端长度 + payload。
///
/// 返回 `None` 表示对端关闭连接。
fn read_frame(r: &mut impl Read) -> Result<Option<Vec<u8>>, Error> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_SIZE {
        return Err(Error::Protocol(format!(
            "帧大小 {} 超过上限 {}",
            len, MAX_FRAME_SIZE
        )));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// 写入一帧：4 字节大端长度 + payload。
fn write_frame(w: &mut impl Write, data: &[u8]) -> Result<(), Error> {
    let len = data.len() as u32;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(data)?;
    w.flush()?;
    Ok(())
}

/// 处理单个 TCP 连接：可选认证 → 循环读帧 → execute_cmd → 写响应帧。
fn handle_tcp_conn(
    stream: std::net::TcpStream,
    db: &Talon,
    auth_token: Option<&str>,
) -> Result<(), Error> {
    // listener 设了 non-blocking，accepted stream 会继承；必须恢复为 blocking。
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(300)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(30)))?;
    let mut reader = std::io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // 认证：如果启用 token，第一帧必须是 {"auth":"<token>"}
    if let Some(expected) = auth_token {
        match read_frame(&mut reader)? {
            Some(frame) => {
                let ok = serde_json::from_slice::<serde_json::Value>(&frame)
                    .ok()
                    .and_then(|v| v.get("auth").and_then(|a| a.as_str()).map(String::from))
                    .is_some_and(|t| t == expected);
                if !ok {
                    let _ = write_frame(
                        &mut writer,
                        r#"{"ok":false,"error":"auth failed"}"#.as_bytes(),
                    );
                    return Ok(());
                }
                write_frame(&mut writer, br#"{"ok":true,"data":{}}"#)?;
            }
            None => return Ok(()),
        }
    }

    // 命令循环
    while let Some(frame) = read_frame(&mut reader)? {
        let cmd_str = String::from_utf8_lossy(&frame);
        let resp = crate::ffi_exec::execute_cmd(db, &cmd_str);
        write_frame(&mut writer, resp.as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn tcp_frame_roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"hello").unwrap();
        assert_eq!(buf.len(), 4 + 5);
        let mut cursor = std::io::Cursor::new(buf);
        let frame = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(frame, b"hello");
    }

    #[test]
    fn tcp_frame_empty_payload() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"").unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let frame = read_frame(&mut cursor).unwrap().unwrap();
        assert!(frame.is_empty());
    }

    #[test]
    fn tcp_frame_eof_returns_none() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        assert!(read_frame(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn tcp_frame_oversized_rejected() {
        let len = MAX_FRAME_SIZE + 1;
        let mut buf = len.to_be_bytes().to_vec();
        buf.extend(vec![0u8; 10]); // 不需要完整 payload
        let mut cursor = std::io::Cursor::new(buf);
        let err = read_frame(&mut cursor).unwrap_err();
        assert!(err.to_string().contains("帧大小"));
    }

    fn start_tcp_server(auth: Option<&str>) -> (u16, tempfile::TempDir, Arc<Talon>) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Talon::open(dir.path()).unwrap());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let auth = auth.map(|s| s.to_string());
        let db2 = Arc::clone(&db);
        std::thread::spawn(move || {
            // 接受最多 3 个连接用于测试
            for stream in listener.incoming().take(3).flatten() {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let db3 = Arc::clone(&db2);
                let auth2 = auth.clone();
                let _ = handle_tcp_conn(stream, &db3, auth2.as_deref());
            }
        });
        std::thread::sleep(Duration::from_millis(50));
        (port, dir, db)
    }

    fn tcp_send_recv(stream: &mut std::net::TcpStream, cmd: &str) -> String {
        write_frame(stream, cmd.as_bytes()).unwrap();
        let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
        let frame = read_frame(&mut reader).unwrap().unwrap();
        String::from_utf8(frame).unwrap()
    }

    #[test]
    fn tcp_kv_roundtrip() {
        let (port, _dir, _db) = start_tcp_server(None);
        let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let r1 = tcp_send_recv(
            &mut stream,
            r#"{"module":"kv","action":"set","params":{"key":"k1","value":"v1"}}"#,
        );
        assert!(r1.contains("\"ok\":true"));

        let r2 = tcp_send_recv(
            &mut stream,
            r#"{"module":"kv","action":"get","params":{"key":"k1"}}"#,
        );
        assert!(r2.contains("\"ok\":true"));
        assert!(r2.contains("v1"));
    }

    #[test]
    fn tcp_auth_required() {
        let (port, _dir, _db) = start_tcp_server(Some("secret"));
        let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // 错误 token
        write_frame(&mut stream, br#"{"auth":"wrong"}"#).unwrap();
        let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
        let frame = read_frame(&mut reader).unwrap().unwrap();
        let resp = String::from_utf8(frame).unwrap();
        assert!(resp.contains("auth failed"));

        // 正确 token
        let mut stream2 = std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream2
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write_frame(&mut stream2, br#"{"auth":"secret"}"#).unwrap();
        let mut reader2 = std::io::BufReader::new(stream2.try_clone().unwrap());
        let frame2 = read_frame(&mut reader2).unwrap().unwrap();
        let resp2 = String::from_utf8(frame2).unwrap();
        assert!(resp2.contains("\"ok\":true"));

        let r = tcp_send_recv(
            &mut stream2,
            r#"{"module":"stats","action":"","params":{}}"#,
        );
        assert!(r.contains("\"ok\":true"));
    }
}
