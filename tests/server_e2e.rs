/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! R4: Server 层 E2E 冒烟测试 — HTTP API 基本功能验证。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use talon::{HttpServer, ServerConfig, Talon};

/// 发送 HTTP 请求并读取响应 body。
fn http_request(addr: &str, method: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let req = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
        method, path, addr, body.len(), body
    );
    stream.write_all(req.as_bytes()).unwrap();
    // 读取全部响应直到 EOF
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => all.extend_from_slice(&buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => break,
        }
    }
    let resp = String::from_utf8_lossy(&all).to_string();
    if let Some(pos) = resp.find("\r\n\r\n") {
        resp[pos + 4..].to_string()
    } else {
        resp
    }
}

fn start_server() -> (
    String,
    Arc<Talon>,
    tempfile::TempDir,
    std::thread::JoinHandle<()>,
) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Talon::open(dir.path()).unwrap());
    // 使用随机端口避免冲突
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let config = ServerConfig {
        http_addr: addr.clone(),
        auth_token: None,
        max_connections: 10,
        auto_persist_secs: 0,
    };
    let db2 = Arc::clone(&db);
    let handle = std::thread::spawn(move || {
        let server = HttpServer::new(config, db2);
        let _ = server.run();
    });
    // 等待 server 启动
    std::thread::sleep(Duration::from_millis(500));
    (addr, db, dir, handle)
}

#[test]
fn e2e_health_check() {
    let (addr, _db, _dir, _handle) = start_server();
    let body = http_request(&addr, "GET", "/health", "");
    assert!(
        body.contains("ok") || body.contains("status"),
        "health check should return status, got: {}",
        body
    );
}

#[test]
fn e2e_sql_crud() {
    let (addr, _db, _dir, _handle) = start_server();

    // CREATE TABLE
    let body = http_request(
        &addr,
        "POST",
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"}}"#,
    );
    assert!(
        body.contains("ok") && body.contains("true"),
        "CREATE TABLE failed: {}",
        body
    );

    // INSERT
    let body = http_request(
        &addr,
        "POST",
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"INSERT INTO test VALUES (1, 'Alice')"}}"#,
    );
    assert!(body.contains("true"), "INSERT failed: {}", body);

    // SELECT
    let body = http_request(
        &addr,
        "POST",
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"SELECT * FROM test WHERE id = 1"}}"#,
    );
    assert!(
        body.contains("Alice"),
        "SELECT should return Alice: {}",
        body
    );
}

#[test]
fn e2e_kv_operations() {
    let (addr, _db, _dir, _handle) = start_server();

    // SET
    let body = http_request(
        &addr,
        "POST",
        "/api/kv",
        r#"{"cmd":"kv","action":"set","params":{"key":"hello","value":"world"}}"#,
    );
    assert!(
        body.contains("\"ok\"") || body.is_empty(),
        "KV SET unexpected response: {}",
        body
    );

    // GET
    let body = http_request(
        &addr,
        "POST",
        "/api/kv",
        r#"{"cmd":"kv","action":"get","params":{"key":"hello"}}"#,
    );
    assert!(
        body.contains("world") || body.contains("\"ok\":true"),
        "KV GET should return world: {}",
        body
    );
}
