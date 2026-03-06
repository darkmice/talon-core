/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 网络模式全量性能基准测试
//! 通过 HTTP API 测试所有引擎：SQL / KV / 时序 / 消息队列 / 向量
//! 规模：10万 / 50万 / 100万 三档海量数据
//! 运行：cargo test --test bench_network_full --release -- --nocapture

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};
use talon::{HttpServer, ServerConfig, Talon};

// ─────────────────────────────────────────────────────────────────────────────
// HTTP 客户端工具
// ─────────────────────────────────────────────────────────────────────────────

fn http_post(addr: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
        path, addr, body.len(), body
    );
    stream.write_all(req.as_bytes()).unwrap();

    let mut all = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => all.extend_from_slice(&buf[..n]),
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break
            }
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

fn p95_us(latencies: &mut [f64]) -> f64 {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((latencies.len() as f64) * 0.95) as usize;
    latencies[idx.min(latencies.len() - 1)]
}

// ─────────────────────────────────────────────────────────────────────────────
// 启动测试服务器（随机端口）
// ─────────────────────────────────────────────────────────────────────────────

struct TestServer {
    addr: String,
    _db: Arc<Talon>,
    _dir: tempfile::TempDir,
    _handle: std::thread::JoinHandle<()>,
}

fn start_test_server() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Talon::open(dir.path()).unwrap());

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let config = ServerConfig {
        http_addr: addr.clone(),
        auth_token: None,
        max_connections: 100,
        auto_persist_secs: 0,
    };
    let db2 = Arc::clone(&db);
    let handle = std::thread::spawn(move || {
        let server = HttpServer::new(config, db2);
        let _ = server.run();
    });
    std::thread::sleep(Duration::from_millis(200));
    TestServer {
        addr,
        _db: db,
        _dir: dir,
        _handle: handle,
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  SQL 引擎 — 网络模式 10 万行
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_sql_100k_crud() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 100_000u64;

    println!("\n╔══ [网络-SQL] 10万行全量 CRUD ══╗");

    // DDL
    http_post(
        addr,
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"CREATE TABLE nperf (id INT, cat TEXT, score INT)"}}"#,
    );
    http_post(
        addr,
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"CREATE INDEX idx_ncat ON nperf(cat)"}}"#,
    );

    // INSERT (batch txn)
    let t0 = Instant::now();
    let batch_sz = 1_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_sz).min(n);
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"BEGIN"}}"#,
        );
        for j in i..end {
            let body = format!(
                r#"{{"cmd":"sql","action":"exec","params":{{"sql":"INSERT INTO nperf VALUES ({}, 'c{}', {})"}}}}"#,
                j,
                j % 100,
                j % 10_000
            );
            http_post(addr, "/api/sql", &body);
        }
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"COMMIT"}}"#,
        );
        i = end;
        if i % 20_000 == 0 {
            println!("  {}万行...", i / 10_000);
        }
    }
    let elapsed_ins = t0.elapsed();
    println!(
        "  INSERT 10万行: {:.2?}  {:.0} rows/s",
        elapsed_ins,
        n as f64 / elapsed_ins.as_secs_f64()
    );

    // SELECT by PK (P95 latency)
    let samples = 500u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let idx = (k * 199 + 3) % n;
        let body = format!(
            r#"{{"cmd":"sql","action":"exec","params":{{"sql":"SELECT * FROM nperf WHERE id={}"}}}}"#,
            idx
        );
        let t0 = Instant::now();
        http_post(addr, "/api/sql", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  SELECT PK P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    // COUNT(*)
    let mut lats = Vec::with_capacity(20);
    for _ in 0..20 {
        let t0 = Instant::now();
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"SELECT COUNT(*) FROM nperf"}}"#,
        );
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  COUNT(*) P95: {:.1}ms", p95_us(&mut lats) / 1000.0);

    // UPDATE by PK
    let t0 = Instant::now();
    let upd_n = 500u64;
    for k in 0..upd_n {
        let idx = (k * 197) % n;
        let body = format!(
            r#"{{"cmd":"sql","action":"exec","params":{{"sql":"UPDATE nperf SET score={} WHERE id={}"}}}}"#,
            k % 9999,
            idx
        );
        http_post(addr, "/api/sql", &body);
    }
    println!(
        "  UPDATE PK: {:.0} ops/s",
        upd_n as f64 / t0.elapsed().as_secs_f64()
    );

    // DELETE by PK
    let t0 = Instant::now();
    let del_n = 500u64;
    for k in 0..del_n {
        let idx = k * 200; // 保证存在
        let body = format!(
            r#"{{"cmd":"sql","action":"exec","params":{{"sql":"DELETE FROM nperf WHERE id={}"}}}}"#,
            idx
        );
        http_post(addr, "/api/sql", &body);
    }
    println!(
        "  DELETE PK: {:.0} ops/s",
        del_n as f64 / t0.elapsed().as_secs_f64()
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  SQL 引擎 — 网络模式 50 万行
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_sql_500k_insert_select() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 500_000u64;

    println!("\n╔══ [网络-SQL] 50万行性能 ══╗");

    http_post(
        addr,
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"CREATE TABLE big500k (id INT, cat TEXT, score INT)"}}"#,
    );

    let t0 = Instant::now();
    let batch_sz = 2_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_sz).min(n);
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"BEGIN"}}"#,
        );
        for j in i..end {
            let body = format!(
                r#"{{"cmd":"sql","action":"exec","params":{{"sql":"INSERT INTO big500k VALUES ({}, 'c{}', {})"}}}}"#,
                j,
                j % 200,
                j % 10_000
            );
            http_post(addr, "/api/sql", &body);
        }
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"COMMIT"}}"#,
        );
        i = end;
        if i % 100_000 == 0 {
            println!("  {}万行...", i / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 50万: {:.2?}  {:.0} rows/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // SELECT COUNT
    let mut lats = Vec::with_capacity(5);
    for _ in 0..5 {
        let t0 = Instant::now();
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"SELECT COUNT(*) FROM big500k"}}"#,
        );
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  COUNT(*) P95: {:.1}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  SQL 引擎 — 网络模式 100 万行
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_sql_1m_insert_agg() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 1_000_000u64;

    println!("\n╔══ [网络-SQL] 100万行性能 ══╗");

    http_post(
        addr,
        "/api/sql",
        r#"{"cmd":"sql","action":"exec","params":{"sql":"CREATE TABLE mil (id INT, cat TEXT, score INT)"}}"#,
    );

    let t0 = Instant::now();
    let batch_sz = 2_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_sz).min(n);
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"BEGIN"}}"#,
        );
        for j in i..end {
            let body = format!(
                r#"{{"cmd":"sql","action":"exec","params":{{"sql":"INSERT INTO mil VALUES ({}, 'c{}', {})"}}}}"#,
                j,
                j % 500,
                j % 10_000
            );
            http_post(addr, "/api/sql", &body);
        }
        http_post(
            addr,
            "/api/sql",
            r#"{"cmd":"sql","action":"exec","params":{"sql":"COMMIT"}}"#,
        );
        i = end;
        if i % 200_000 == 0 {
            println!("  {}万行...", i / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 100万: {:.2?}  {:.0} rows/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // SELECT PK P95
    let samples = 200u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let idx = (k * 4999 + 1) % n;
        let body = format!(
            r#"{{"cmd":"sql","action":"exec","params":{{"sql":"SELECT * FROM mil WHERE id={}"}}}}"#,
            idx
        );
        let t0 = Instant::now();
        http_post(addr, "/api/sql", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  SELECT PK P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    // Aggregate
    for sql in &[
        "SELECT COUNT(*) FROM mil",
        "SELECT SUM(score) FROM mil",
        "SELECT AVG(score) FROM mil",
    ] {
        let mut lats = Vec::with_capacity(3);
        for _ in 0..3 {
            let body = format!(
                r#"{{"cmd":"sql","action":"exec","params":{{"sql":"{}"}}}}"#,
                sql
            );
            let t0 = Instant::now();
            http_post(addr, "/api/sql", &body);
            lats.push(t0.elapsed().as_micros() as f64);
        }
        println!("  {} P95: {:.1}ms", sql, p95_us(&mut lats) / 1000.0);
    }

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  KV 引擎 — 网络模式 10 万条
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_kv_100k_set_get_del() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 100_000u64;

    println!("\n╔══ [网络-KV] 10万条全量测试 ══╗");

    // SET
    let t0 = Instant::now();
    for i in 0..n {
        let body = format!(
            r#"{{"cmd":"kv","action":"set","params":{{"key":"k{:08}","value":"val_{}"}}}}"#,
            i, i
        );
        http_post(addr, "/api/kv", &body);
        if i % 20_000 == 19_999 {
            println!("  SET {}万...", (i + 1) / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  SET 10万: {:.2?}  {:.0} ops/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // GET P95
    let samples = 500u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let idx = (k * 199 + 3) % n;
        let body = format!(
            r#"{{"cmd":"kv","action":"get","params":{{"key":"k{:08}"}}}}"#,
            idx
        );
        let t0 = Instant::now();
        http_post(addr, "/api/kv", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  GET P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    // DEL
    let t0 = Instant::now();
    let del_n = 500u64;
    for k in 0..del_n {
        let body = format!(
            r#"{{"cmd":"kv","action":"del","params":{{"key":"k{:08}"}}}}"#,
            k
        );
        http_post(addr, "/api/kv", &body);
    }
    println!(
        "  DEL: {:.0} ops/s",
        del_n as f64 / t0.elapsed().as_secs_f64()
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  KV 引擎 — 网络模式 50 万条
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_kv_500k_set_get() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 500_000u64;

    println!("\n╔══ [网络-KV] 50万条性能 ══╗");

    let t0 = Instant::now();
    for i in 0..n {
        let body = format!(
            r#"{{"cmd":"kv","action":"set","params":{{"key":"nk{:08}","value":"v{}"}}}}"#,
            i, i
        );
        http_post(addr, "/api/kv", &body);
        if i % 100_000 == 99_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  SET 50万: {:.2?}  {:.0} ops/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // GET P95 (1000 samples)
    let samples = 1_000u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let idx = (k * 499 + 7) % n;
        let body = format!(
            r#"{{"cmd":"kv","action":"get","params":{{"key":"nk{:08}"}}}}"#,
            idx
        );
        let t0 = Instant::now();
        http_post(addr, "/api/kv", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  GET P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  KV 引擎 — 网络模式 100 万条
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_kv_1m_set_get() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 1_000_000u64;

    println!("\n╔══ [网络-KV] 100万条全量 ══╗");

    let t0 = Instant::now();
    for i in 0..n {
        let body = format!(
            r#"{{"cmd":"kv","action":"set","params":{{"key":"mk{:08}","value":"v{}"}}}}"#,
            i, i
        );
        http_post(addr, "/api/kv", &body);
        if i % 200_000 == 199_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  SET 100万: {:.2?}  {:.0} ops/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // GET P95
    let samples = 500u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let idx = (k * 1997 + 1) % n;
        let body = format!(
            r#"{{"cmd":"kv","action":"get","params":{{"key":"mk{:08}"}}}}"#,
            idx
        );
        let t0 = Instant::now();
        http_post(addr, "/api/kv", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  GET P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  时序引擎 — 网络模式 10 万点
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_ts_100k_insert_query() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 100_000u64;

    println!("\n╔══ [网络-TS] 10万数据点 ══╗");

    // 创建时序
    http_post(
        addr,
        "/api/ts",
        r#"{"cmd":"ts","action":"create","params":{"name":"net_metrics","tags":["host","region"],"fields":["cpu","mem"]}}"#,
    );

    let base_ts: i64 = 1_700_000_000_000;
    let t0 = Instant::now();
    let batch_sz = 1_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_sz).min(n);
        for j in i..end {
            let mut tags = std::collections::BTreeMap::new();
            tags.insert("host", format!("host_{}", j % 100));
            tags.insert("region", format!("r_{}", j % 10));
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("cpu", format!("{:.2}", 10.0 + (j % 90) as f64));
            fields.insert("mem", format!("{}", 1024 + j % 4096));
            let point = serde_json::json!({
                "timestamp": base_ts + j as i64,
                "tags": tags,
                "fields": fields
            });
            let body = serde_json::json!({
                "cmd": "ts",
                "action": "insert",
                "params": {"name": "net_metrics", "point": point}
            })
            .to_string();
            http_post(addr, "/api/ts", &body);
        }
        i = end;
        if i % 20_000 == 0 {
            println!("  {}万点...", i / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 10万点: {:.2?}  {:.0} pts/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // QUERY P95
    let samples = 100u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let start = base_ts + k as i64 * 1_000;
        let body = serde_json::json!({
            "cmd": "ts",
            "action": "query",
            "params": {
                "name": "net_metrics",
                "tag_filters": [["host", format!("host_{}", k % 100)]],
                "time_start": start,
                "time_end": start + 5_000i64,
                "limit": 100
            }
        })
        .to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/ts", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  QUERY P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  时序引擎 — 网络模式 100 万点
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_ts_1m_insert_query_agg() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 1_000_000u64;

    println!("\n╔══ [网络-TS] 100万数据点 ══╗");

    http_post(
        addr,
        "/api/ts",
        r#"{"cmd":"ts","action":"create","params":{"name":"net_m1m","tags":["host","region"],"fields":["cpu","mem"]}}"#,
    );

    let base_ts: i64 = 1_700_000_000_000;
    let t0 = Instant::now();
    let batch_sz = 2_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_sz).min(n);
        for j in i..end {
            let mut tags = std::collections::BTreeMap::new();
            tags.insert("host", format!("host_{}", j % 100));
            tags.insert("region", format!("r_{}", j % 10));
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("cpu", format!("{:.2}", 10.0 + (j % 90) as f64));
            fields.insert("mem", format!("{}", 1024 + j % 4096));
            let point = serde_json::json!({
                "timestamp": base_ts + j as i64,
                "tags": tags,
                "fields": fields
            });
            let body = serde_json::json!({
                "cmd": "ts",
                "action": "insert",
                "params": {"name": "net_m1m", "point": point}
            })
            .to_string();
            http_post(addr, "/api/ts", &body);
        }
        i = end;
        if i % 200_000 == 0 {
            println!("  {}万点...", i / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 100万点: {:.2?}  {:.0} pts/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // QUERY P95
    let samples = 50u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let start = base_ts + k as i64 * 20_000;
        let body = serde_json::json!({
            "cmd": "ts",
            "action": "query",
            "params": {
                "name": "net_m1m",
                "tag_filters": [["host", format!("host_{}", k % 100)]],
                "time_start": start,
                "time_end": start + 100_000i64,
                "limit": 100
            }
        })
        .to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/ts", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  QUERY P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    // AGGREGATE
    let mut lats = Vec::with_capacity(3);
    for _ in 0..3 {
        let body = serde_json::json!({
            "cmd": "ts",
            "action": "aggregate",
            "params": {
                "name": "net_m1m",
                "tag_filters": [],
                "time_start": base_ts,
                "time_end": base_ts + n as i64,
                "func": "sum",
                "field": "cpu",
                "interval_ms": null
            }
        })
        .to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/ts", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  AGG SUM P95: {:.1}ms  (目标<500ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  消息队列 — 网络模式 10 万 / 100 万
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_mq_100k_publish_poll() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 100_000u64;

    println!("\n╔══ [网络-MQ] 10万消息 ══╗");

    // 先创建 topic
    http_post(
        addr,
        "/api/mq",
        r#"{"cmd":"mq","action":"create","params":{"topic":"bench_q","max_len":0}}"#,
    );

    let t0 = Instant::now();
    for i in 0..n {
        let body = format!(
            r#"{{"cmd":"mq","action":"publish","params":{{"topic":"bench_q","payload":"msg_{}"}}}}"#,
            i
        );
        http_post(addr, "/api/mq", &body);
        if i % 20_000 == 19_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  PUBLISH 10万: {:.2?}  {:.0} msg/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // POLL P95
    let samples = 200u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for _ in 0..samples {
        let body =
            r#"{"cmd":"mq","action":"poll","params":{"topic":"bench_q","group":"g1","consumer":"c1","count":100}}"#.to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/mq", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  POLL(100) P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn net_mq_1m_publish_poll() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 1_000_000u64;

    println!("\n╔══ [网络-MQ] 100万消息 ══╗");

    // 先创建 topic
    http_post(
        addr,
        "/api/mq",
        r#"{"cmd":"mq","action":"create","params":{"topic":"q1m","max_len":0}}"#,
    );

    let t0 = Instant::now();
    for i in 0..n {
        let body = format!(
            r#"{{"cmd":"mq","action":"publish","params":{{"topic":"q1m","payload":"msg_{}"}}}}"#,
            i
        );
        http_post(addr, "/api/mq", &body);
        if i % 200_000 == 199_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  PUBLISH 100万: {:.2?}  {:.0} msg/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // POLL P95
    let mut lats = Vec::with_capacity(100);
    for _ in 0..100 {
        let body =
            r#"{"cmd":"mq","action":"poll","params":{"topic":"q1m","group":"g1","consumer":"c1","count":100}}"#.to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/mq", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  POLL(100) P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// ██  向量引擎 — 网络模式 1 万 / 10 万
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn net_vector_10k_insert_search() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 10_000u64;
    let dim = 64usize;

    println!("\n╔══ [网络-向量] 1万向量(dim=64) ══╗");

    fn make_vec_json(seed: u64, dim: usize) -> serde_json::Value {
        let v: Vec<f32> = (0..dim)
            .map(|i| {
                ((seed * 6364136223846793005 + i as u64 * 1442695040888963407) % 1000) as f32
                    / 1000.0
            })
            .collect();
        serde_json::json!(v)
    }

    let t0 = Instant::now();
    for i in 0..n {
        let vec = make_vec_json(i, dim);
        let body = serde_json::json!({
            "cmd": "vector",
            "action": "insert",
            "params": {
                "name": "net_vec_10k",
                "id": i,
                "vector": vec
            }
        })
        .to_string();
        http_post(addr, "/api/vector", &body);
        if i % 2_000 == 1_999 {
            println!("  {}千...", (i + 1) / 1_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 1万: {:.2?}  {:.0} vec/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // KNN search P95
    let samples = 100u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let qvec = make_vec_json(k + n, dim);
        let body = serde_json::json!({
            "cmd": "vector",
            "action": "search",
            "params": {
                "name": "net_vec_10k",
                "vector": qvec,
                "k": 10,
                "metric": "cosine"
            }
        })
        .to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/vector", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  KNN(k=10) P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn net_vector_100k_insert_search() {
    let srv = start_test_server();
    let addr = &srv.addr;
    let n = 100_000u64;
    let dim = 64usize;

    println!("\n╔══ [网络-向量] 10万向量(dim=64) ══╗");

    fn make_vec_json(seed: u64, dim: usize) -> serde_json::Value {
        let v: Vec<f32> = (0..dim)
            .map(|i| {
                ((seed * 6364136223846793005 + i as u64 * 1442695040888963407) % 1000) as f32
                    / 1000.0
            })
            .collect();
        serde_json::json!(v)
    }

    let t0 = Instant::now();
    for i in 0..n {
        let vec = make_vec_json(i, dim);
        let body = serde_json::json!({
            "cmd": "vector",
            "action": "insert",
            "params": {
                "name": "net_vec_100k",
                "id": i,
                "vector": vec
            }
        })
        .to_string();
        http_post(addr, "/api/vector", &body);
        if i % 20_000 == 19_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 10万: {:.2?}  {:.0} vec/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    // KNN search P95
    let samples = 50u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let qvec = make_vec_json(k + n, dim);
        let body = serde_json::json!({
            "cmd": "vector",
            "action": "search",
            "params": {
                "name": "net_vec_100k",
                "vector": qvec,
                "k": 10,
                "metric": "cosine"
            }
        })
        .to_string();
        let t0 = Instant::now();
        http_post(addr, "/api/vector", &body);
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  KNN(k=10) P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}
