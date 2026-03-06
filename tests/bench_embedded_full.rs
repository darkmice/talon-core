/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 嵌入模式全量性能 + 内存基准测试
//! 覆盖：SQL / KV / 向量 / 时序 / 消息队列 五大引擎
//! 规模：10万 / 50万 / 100万 三档海量数据
//! 运行：cargo test --test bench_embedded_full --release -- --nocapture

use std::collections::BTreeMap;
use std::time::Instant;
use talon::{DataPoint, Talon, TsSchema};

// ─────────────────────────────────────────────────────────────────────────────
// 公共辅助
// ─────────────────────────────────────────────────────────────────────────────

fn p95_us(latencies: &mut [f64]) -> f64 {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((latencies.len() as f64) * 0.95) as usize;
    latencies[idx.min(latencies.len() - 1)]
}

/// 读取当前进程 RSS（KB）。Linux /proc/self/status；非 Linux 系统调用 ps。
fn rss_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if line.starts_with("VmRSS:") {
                return line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        let pid = std::process::id();
        let out = match std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
        {
            Ok(o) => o,
            Err(_) => return 0,
        };
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0)
    }
}

fn make_ts_point(i: u64, base_ts: i64) -> DataPoint {
    let mut tags = BTreeMap::new();
    tags.insert("host".into(), format!("host_{}", i % 100));
    tags.insert("region".into(), format!("region_{}", i % 10));
    let mut fields = BTreeMap::new();
    fields.insert("cpu".into(), format!("{:.2}", 10.0 + (i % 90) as f64));
    fields.insert("mem".into(), format!("{}", 1024 + (i % 8192)));
    DataPoint {
        timestamp: base_ts + i as i64,
        tags,
        fields,
    }
}

fn random_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.push((s >> 33) as f32 / (u32::MAX as f32) - 0.5);
    }
    v
}

// ═════════════════════════════════════════════════════════════════════════════
// SQL 引擎 — 10万行
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn sql_embedded_100k_insert_select_update_delete() {
    let n: u64 = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!("\n╔══ [嵌入-SQL] 10万行全量 CRUD ══╗");

    db.run_sql("CREATE TABLE perf (id INT, cat TEXT, score INT, payload TEXT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_cat ON perf(cat)").unwrap();

    // INSERT (batch txn)
    let rss_before = rss_kb();
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 1_000).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&format!(
                "INSERT INTO perf VALUES ({}, 'cat_{}', {}, 'p_{}')",
                j,
                j % 100,
                j % 10_000,
                j
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
    }
    db.persist().unwrap();
    let elapsed_ins = t0.elapsed();
    println!(
        "  INSERT  10万: {:.2?}  {:.0} rows/s  ΔMem: {}KB",
        elapsed_ins,
        n as f64 / elapsed_ins.as_secs_f64(),
        rss_kb().saturating_sub(rss_before)
    );

    // SELECT by PK P95
    let samples = 3_000u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let idx = (k * 33 + 7) % n;
        let t0 = Instant::now();
        db.run_sql(&format!("SELECT * FROM perf WHERE id = {}", idx))
            .unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  SELECT PK P95: {:.3}ms  (目标<5ms)",
        p95_us(&mut lats) / 1000.0
    );

    // SELECT WHERE + ORDER BY + LIMIT
    let t0 = Instant::now();
    for k in 0..200u64 {
        db.run_sql(&format!(
            "SELECT * FROM perf WHERE cat='cat_{}' ORDER BY score LIMIT 20",
            k % 100
        ))
        .unwrap();
    }
    println!(
        "  SELECT ORDER LIMIT: {:.0} ops/s",
        200.0 / t0.elapsed().as_secs_f64()
    );

    // COUNT(*) 流式
    let mut lats = Vec::with_capacity(10);
    for _ in 0..10 {
        let t0 = Instant::now();
        db.run_sql("SELECT COUNT(*) FROM perf").unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  COUNT(*) P95: {:.1}ms", p95_us(&mut lats) / 1000.0);

    // SUM / AVG
    for sql in &["SELECT SUM(score) FROM perf", "SELECT AVG(score) FROM perf"] {
        let mut lats = Vec::with_capacity(5);
        for _ in 0..5 {
            let t0 = Instant::now();
            db.run_sql(sql).unwrap();
            lats.push(t0.elapsed().as_micros() as f64);
        }
        println!("  {} P95: {:.1}ms", sql, p95_us(&mut lats) / 1000.0);
    }

    // UPDATE by PK
    let t0 = Instant::now();
    for k in 0..1_000u64 {
        db.run_sql(&format!(
            "UPDATE perf SET score={} WHERE id={}",
            k % 9999,
            (k * 97) % n
        ))
        .unwrap();
    }
    println!(
        "  UPDATE PK: {:.0} ops/s",
        1_000.0 / t0.elapsed().as_secs_f64()
    );

    // DELETE by PK
    let t0 = Instant::now();
    for k in 0..1_000u64 {
        db.run_sql(&format!("DELETE FROM perf WHERE id={}", k * 100))
            .unwrap();
    }
    println!(
        "  DELETE PK: {:.0} ops/s",
        1_000.0 / t0.elapsed().as_secs_f64()
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// SQL 引擎 — 50万行
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn sql_embedded_500k_insert_select() {
    let n: u64 = 500_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!("\n╔══ [嵌入-SQL] 50万行性能 ══╗");

    db.run_sql("CREATE TABLE big (id INT, cat TEXT, score INT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_big_cat ON big(cat)").unwrap();

    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 2_000).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&format!(
                "INSERT INTO big VALUES ({}, 'c{}', {})",
                j,
                j % 200,
                j % 10_000
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
        if i % 100_000 == 0 {
            println!("  {}万...", i / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  INSERT 50万: {:.2?}  {:.0} rows/s",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64()
    );

    let mut lats = Vec::with_capacity(5);
    for _ in 0..5 {
        let t0 = Instant::now();
        db.run_sql("SELECT COUNT(*) FROM big").unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  COUNT(*) P95: {:.1}ms", p95_us(&mut lats) / 1000.0);

    let mut lats = Vec::with_capacity(500);
    for k in 0..500u64 {
        let t0 = Instant::now();
        db.run_sql(&format!("SELECT * FROM big WHERE id={}", (k * 997) % n))
            .unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  SELECT PK P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// SQL 引擎 — 100万行
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn sql_embedded_1m_insert_select_agg() {
    let n: u64 = 1_000_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!("\n╔══ [嵌入-SQL] 100万行性能 ══╗");

    db.run_sql("CREATE TABLE million (id INT, cat TEXT, score INT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_mil_cat ON million(cat)")
        .unwrap();

    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 2_000).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&format!(
                "INSERT INTO million VALUES ({}, 'c{}', {})",
                j,
                j % 500,
                j % 10_000
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
        if i % 200_000 == 0 {
            println!("  {}万...", i / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  INSERT 100万: {:.2?}  {:.0} rows/s",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64()
    );

    let mut lats = Vec::with_capacity(1_000);
    for k in 0..1_000u64 {
        let t0 = Instant::now();
        db.run_sql(&format!(
            "SELECT * FROM million WHERE id={}",
            (k * 997 + 1) % n
        ))
        .unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  SELECT PK P95: {:.3}ms  (目标<5ms)",
        p95_us(&mut lats) / 1000.0
    );

    for sql in &[
        "SELECT COUNT(*) FROM million",
        "SELECT SUM(score) FROM million",
        "SELECT AVG(score) FROM million",
    ] {
        let mut lats = Vec::with_capacity(5);
        for _ in 0..5 {
            let t0 = Instant::now();
            db.run_sql(sql).unwrap();
            lats.push(t0.elapsed().as_micros() as f64);
        }
        println!(
            "  {} P95: {:.1}ms  (目标<500ms)",
            sql,
            p95_us(&mut lats) / 1000.0
        );
    }

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// SQL 内存分析
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn sql_embedded_memory_profile() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!("\n╔══ [嵌入-SQL] 内存分析 ══╗");

    db.run_sql("CREATE TABLE memtbl (id INT, data TEXT)")
        .unwrap();
    let n = 100_000u64;
    let rss_init = rss_kb();

    let mut i = 0u64;
    while i < n {
        let end = (i + 2_000).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&format!(
                "INSERT INTO memtbl VALUES ({}, '{}')",
                j,
                "x".repeat(100)
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
    }
    db.persist().unwrap();
    println!(
        "  填充10万行 RSS增量: {}MB",
        rss_kb().saturating_sub(rss_init) / 1024
    );

    let rss_b = rss_kb();
    db.run_sql("SELECT * FROM memtbl LIMIT 1000").unwrap();
    println!(
        "  SELECT LIMIT 1000 RSS增量: {}KB  (期望<1MB)",
        rss_kb().saturating_sub(rss_b)
    );

    let rss_b = rss_kb();
    db.run_sql("SELECT * FROM memtbl ORDER BY id LIMIT 1000")
        .unwrap();
    println!(
        "  ORDER BY LIMIT 1000 RSS增量: {}KB",
        rss_kb().saturating_sub(rss_b)
    );

    let rss_b = rss_kb();
    db.run_sql("SELECT COUNT(*) FROM memtbl").unwrap();
    println!(
        "  COUNT(*) RSS增量: {}KB  (期望<100KB)",
        rss_kb().saturating_sub(rss_b)
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// KV 引擎 — 10万条
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kv_embedded_100k_full() {
    let n: u64 = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    println!("\n╔══ [嵌入-KV] 10万条全量测试 ══╗");

    // SET batch
    let rss_b = rss_kb();
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 5_000).min(n);
        let mut batch = db.batch();
        for j in i..end {
            kv.set_batch(
                &mut batch,
                format!("k:{:08}", j).as_bytes(),
                &[0x42; 100],
                None,
            )
            .unwrap();
        }
        batch.commit().unwrap();
        i = end;
    }
    db.persist().unwrap();
    println!(
        "  SET batch 10万: {:.2?}  {:.0} ops/s  ΔMem: {}KB",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        rss_kb().saturating_sub(rss_b)
    );

    // GET random P95
    let samples = 10_000u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for k in 0..samples {
        let t0 = Instant::now();
        kv.get(format!("k:{:08}", (k * 13 + 7) % n).as_bytes())
            .unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  GET P95: {:.3}ms  OPS: {:.0}",
        p95_us(&mut lats) / 1000.0,
        samples as f64 / lats.iter().sum::<f64>() * 1e6
    );

    // EXISTS
    let t0 = Instant::now();
    for k in 0..samples {
        kv.exists(format!("k:{:08}", (k * 17) % n).as_bytes())
            .unwrap();
    }
    println!(
        "  EXISTS: {:.0} ops/s",
        samples as f64 / t0.elapsed().as_secs_f64()
    );

    // SCAN prefix
    let scans = 1_000u64;
    let t0 = Instant::now();
    for k in 0..scans {
        kv.scan_prefix_limit(format!("k:{:04}", k % 1000).as_bytes(), 0, 50)
            .unwrap();
    }
    println!(
        "  SCAN prefix: {:.0} ops/s",
        scans as f64 / t0.elapsed().as_secs_f64()
    );

    // DEL
    let t0 = Instant::now();
    for k in 0..2_000u64 {
        kv.del(format!("k:{:08}", k).as_bytes()).unwrap();
    }
    println!("  DEL: {:.0} ops/s", 2_000.0 / t0.elapsed().as_secs_f64());

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// KV 引擎 — 50万条
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kv_embedded_500k_set_get() {
    let n: u64 = 500_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    println!("\n╔══ [嵌入-KV] 50万条性能 ══╗");

    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 10_000).min(n);
        let mut batch = db.batch();
        for j in i..end {
            kv.set_batch(
                &mut batch,
                format!("k:{:08}", j).as_bytes(),
                &[0x42; 64],
                None,
            )
            .unwrap();
        }
        batch.commit().unwrap();
        i = end;
        if i % 100_000 == 0 {
            println!("  {}万...", i / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  SET batch 50万: {:.2?}  {:.0} ops/s",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64()
    );

    let samples = 20_000u64;
    let t0 = Instant::now();
    for k in 0..samples {
        kv.get(format!("k:{:08}", (k * 11 + 3) % n).as_bytes())
            .unwrap();
    }
    println!(
        "  GET random: {:.0} ops/s",
        samples as f64 / t0.elapsed().as_secs_f64()
    );

    // MGET batch=100
    let rounds = 200u64;
    let t0 = Instant::now();
    for r in 0..rounds {
        let keys: Vec<Vec<u8>> = (0..100u64)
            .map(|k| format!("k:{:08}", (r * 100 + k) % n).into_bytes())
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        kv.mget(&refs).unwrap();
    }
    println!(
        "  MGET(100): {:.0} ops/s",
        rounds as f64 * 100.0 / t0.elapsed().as_secs_f64()
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// KV 引擎 — 100万条
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kv_embedded_1m_set_get_mget() {
    let n: u64 = 1_000_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    println!("\n╔══ [嵌入-KV] 100万条全量 ══╗");

    let rss_b = rss_kb();
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 10_000).min(n);
        let mut batch = db.batch();
        for j in i..end {
            kv.set_batch(
                &mut batch,
                format!("k:{:08}", j).as_bytes(),
                &[0x42; 100],
                None,
            )
            .unwrap();
        }
        batch.commit().unwrap();
        i = end;
        if i % 200_000 == 0 {
            println!("  {}万...", i / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  SET batch 100万: {:.2?}  {:.0} ops/s  ΔMem: {}MB",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        rss_kb().saturating_sub(rss_b) / 1024
    );

    let samples = 50_000u64;
    let t0 = Instant::now();
    for k in 0..samples {
        kv.get(format!("k:{:08}", (k * 13 + 7) % n).as_bytes())
            .unwrap();
    }
    println!(
        "  GET random: {:.0} ops/s  (目标>500K)",
        samples as f64 / t0.elapsed().as_secs_f64()
    );

    let rounds = 500u64;
    let t0 = Instant::now();
    for r in 0..rounds {
        let keys: Vec<Vec<u8>> = (0..100u64)
            .map(|k| format!("k:{:08}", (r * 100 + k) % n).into_bytes())
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        kv.mget(&refs).unwrap();
    }
    println!(
        "  MGET(100): {:.0} ops/s",
        rounds as f64 * 100.0 / t0.elapsed().as_secs_f64()
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// KV 内存分析
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn kv_embedded_memory_profile() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    println!("\n╔══ [嵌入-KV] 内存分析 ══╗");

    let n = 100_000u64;
    let rss_init = rss_kb();
    let mut i = 0u64;
    while i < n {
        let end = (i + 5_000).min(n);
        let mut batch = db.batch();
        for j in i..end {
            kv.set_batch(
                &mut batch,
                format!("mk:{:08}", j).as_bytes(),
                &[0xAB; 1024],
                None,
            )
            .unwrap();
        }
        batch.commit().unwrap();
        i = end;
    }
    db.persist().unwrap();
    println!(
        "  10万key × 1KB val 填充后 RSS增量: {}MB",
        rss_kb().saturating_sub(rss_init) / 1024
    );

    let rss_b = rss_kb();
    for k in 0..1_000u64 {
        kv.get(format!("mk:{:08}", k).as_bytes()).unwrap();
    }
    println!(
        "  1000次GET RSS增量: {}KB  (期望接近0)",
        rss_kb().saturating_sub(rss_b)
    );

    let rss_b = rss_kb();
    kv.scan_prefix_limit(b"mk:0", 0, 1000).unwrap();
    println!(
        "  scan_prefix_limit(1000) RSS增量: {}KB",
        rss_kb().saturating_sub(rss_b)
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// 向量引擎 — 1万 / 5万 / 10万
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn vector_embedded_10k_insert_knn() {
    let n = 10_000u64;
    let dim = 128usize;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let ve = db.vector("bench_10k").unwrap();
    println!("\n╔══ [嵌入-向量] 1万向量(dim=128) ══╗");

    let rss_b = rss_kb();
    let t0 = Instant::now();
    for i in 0..n {
        ve.insert(i, &random_vec(i, dim)).unwrap();
    }
    db.persist().unwrap();
    println!(
        "  INSERT 1万: {:.2?}  {:.0} vec/s  ΔMem: {}KB",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        rss_kb().saturating_sub(rss_b)
    );

    let samples = 200u64;
    let mut lats = Vec::with_capacity(samples as usize);
    for i in 0..samples {
        let t0 = Instant::now();
        ve.search(&random_vec(i + n, dim), 10, "cosine").unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  KNN(k=10) P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn vector_embedded_50k_insert_knn() {
    let n = 50_000u64;
    let dim = 128usize;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let ve = db.vector("bench_50k").unwrap();
    println!("\n╔══ [嵌入-向量] 5万向量(dim=128) ══╗");

    let t0 = Instant::now();
    for i in 0..n {
        ve.insert(i, &random_vec(i, dim)).unwrap();
        if i % 10_000 == 9_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    db.persist().unwrap();
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 5万: {:.2?}  {:.0} vec/s",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );

    let mut lats = Vec::with_capacity(100);
    for i in 0..100u64 {
        let t0 = Instant::now();
        ve.search(&random_vec(i + n, dim), 10, "cosine").unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  KNN(k=10) P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn vector_embedded_100k_insert_knn() {
    let n = 100_000u64;
    let dim = 128usize;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let ve = db.vector("bench_100k").unwrap();
    println!("\n╔══ [嵌入-向量] 10万向量(dim=128) ══╗");

    let rss_b = rss_kb();
    let t0 = Instant::now();
    for i in 0..n {
        ve.insert(i, &random_vec(i, dim)).unwrap();
        if i % 20_000 == 19_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    db.persist().unwrap();
    let elapsed = t0.elapsed();
    println!(
        "  INSERT 10万: {:.2?}  {:.0} vec/s  ΔMem: {}MB",
        elapsed,
        n as f64 / elapsed.as_secs_f64(),
        rss_kb().saturating_sub(rss_b) / 1024
    );

    let mut lats = Vec::with_capacity(50);
    for i in 0..50u64 {
        let t0 = Instant::now();
        ve.search(&random_vec(i + n, dim), 10, "cosine").unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  KNN(k=10) P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// 时序引擎 — 10万 / 50万 / 100万
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn ts_embedded_100k_insert_query_agg() {
    let n: u64 = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into(), "mem".into()],
    };
    let ts = db.create_timeseries("m100k", schema).unwrap();
    let base_ts: i64 = 1_700_000_000_000;
    println!("\n╔══ [嵌入-TS] 10万数据点 ══╗");

    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 5_000).min(n);
        let pts: Vec<_> = (i..end).map(|j| make_ts_point(j, base_ts)).collect();
        ts.insert_batch(&pts).unwrap();
        i = end;
    }
    db.persist().unwrap();
    println!(
        "  INSERT 10万点: {:.2?}  {:.0} pts/s",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64()
    );

    // Query P95
    let mut lats = Vec::with_capacity(200);
    for k in 0..200i64 {
        let q = talon::TsQuery {
            tag_filters: vec![("host".into(), format!("host_{}", k % 100))],
            time_start: Some(base_ts + k * 500),
            time_end: Some(base_ts + k * 500 + 5_000),
            desc: false,
            limit: Some(100),
        };
        let t0 = Instant::now();
        ts.query(&q).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  QUERY P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    // Aggregate SUM
    let mut lats = Vec::with_capacity(10);
    for k in 0..10i64 {
        let q = talon::TsAggQuery {
            tag_filters: vec![("host".into(), format!("host_{}", k % 100))],
            time_start: Some(base_ts),
            time_end: Some(base_ts + n as i64),
            field: "cpu".into(),
            func: talon::AggFunc::Sum,
            interval_ms: None,
            sliding_ms: None,
            session_gap_ms: None,
            fill: None,
        };
        let t0 = Instant::now();
        ts.aggregate(&q).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  AGG SUM P95: {:.1}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn ts_embedded_500k_insert_query() {
    let n: u64 = 500_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into(), "mem".into()],
    };
    let ts = db.create_timeseries("m500k", schema).unwrap();
    let base_ts: i64 = 1_700_000_000_000;
    println!("\n╔══ [嵌入-TS] 50万数据点 ══╗");

    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 10_000).min(n);
        let pts: Vec<_> = (i..end).map(|j| make_ts_point(j, base_ts)).collect();
        ts.insert_batch(&pts).unwrap();
        i = end;
        if i % 100_000 == 0 {
            println!("  {}万点...", i / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  INSERT 50万: {:.2?}  {:.0} pts/s",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64()
    );

    let q = talon::TsAggQuery {
        tag_filters: vec![],
        time_start: Some(base_ts),
        time_end: Some(base_ts + n as i64),
        field: "cpu".into(),
        func: talon::AggFunc::Sum,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let t0 = Instant::now();
    ts.aggregate(&q).unwrap();
    println!("  AGG SUM: {:.1}ms", t0.elapsed().as_millis());

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn ts_embedded_1m_insert_query_agg() {
    let n: u64 = 1_000_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into(), "mem".into()],
    };
    let ts = db.create_timeseries("m1m", schema).unwrap();
    let base_ts: i64 = 1_700_000_000_000;
    println!("\n╔══ [嵌入-TS] 100万数据点 ══╗");

    let rss_b = rss_kb();
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + 10_000).min(n);
        let pts: Vec<_> = (i..end).map(|j| make_ts_point(j, base_ts)).collect();
        ts.insert_batch(&pts).unwrap();
        i = end;
        if i % 200_000 == 0 {
            println!("  {}万点...", i / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  INSERT 100万: {:.2?}  {:.0} pts/s  ΔMem: {}MB",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        rss_kb().saturating_sub(rss_b) / 1024
    );

    let mut lats = Vec::with_capacity(100);
    for k in 0..100i64 {
        let q = talon::TsQuery {
            tag_filters: vec![("host".into(), format!("host_{}", k % 100))],
            time_start: Some(base_ts + k * 10_000),
            time_end: Some(base_ts + k * 10_000 + 50_000),
            desc: false,
            limit: Some(100),
        };
        let t0 = Instant::now();
        ts.query(&q).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  QUERY P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    let mut lats = Vec::with_capacity(5);
    for k in 0..5i64 {
        let q = talon::TsAggQuery {
            tag_filters: vec![("host".into(), format!("host_{}", k % 100))],
            time_start: Some(base_ts),
            time_end: Some(base_ts + n as i64),
            field: "cpu".into(),
            func: talon::AggFunc::Sum,
            interval_ms: None,
            sliding_ms: None,
            session_gap_ms: None,
            fill: None,
        };
        let t0 = Instant::now();
        ts.aggregate(&q).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  AGG SUM P95: {:.1}ms  (目标<500ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}

// ═════════════════════════════════════════════════════════════════════════════
// 消息队列 — 10万 / 50万 / 100万
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn mq_embedded_100k_publish_poll() {
    let n: u64 = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    mq.create_topic("bench_q", 0).unwrap();
    println!("\n╔══ [嵌入-MQ] 10万消息 ══╗");

    let rss_b = rss_kb();
    let t0 = Instant::now();
    let payload = vec![0x42u8; 100];
    for _ in 0..n {
        mq.publish("bench_q", &payload).unwrap();
    }
    db.persist().unwrap();
    println!(
        "  PUBLISH 10万: {:.2?}  {:.0} msg/s  ΔMem: {}KB",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        rss_kb().saturating_sub(rss_b)
    );

    let mut lats = Vec::with_capacity(500);
    for _ in 0..500 {
        let t0 = Instant::now();
        mq.poll("bench_q", "g1", "c1", 100).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  POLL(100) P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn mq_embedded_500k_publish_poll() {
    let n: u64 = 500_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    mq.create_topic("q500k", 0).unwrap();
    println!("\n╔══ [嵌入-MQ] 50万消息 ══╗");

    let t0 = Instant::now();
    let payload = vec![0x42u8; 100];
    for i in 0..n {
        mq.publish("q500k", &payload).unwrap();
        if i % 100_000 == 99_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  PUBLISH 50万: {:.2?}  {:.0} msg/s",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64()
    );

    let mut lats = Vec::with_capacity(200);
    for _ in 0..200 {
        let t0 = Instant::now();
        mq.poll("q500k", "g1", "c1", 100).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!("  POLL(100) P95: {:.3}ms", p95_us(&mut lats) / 1000.0);

    println!("╚═══════════════════════════════════════╝");
}

#[test]
fn mq_embedded_1m_publish_poll() {
    let n: u64 = 1_000_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    mq.create_topic("q1m", 0).unwrap();
    println!("\n╔══ [嵌入-MQ] 100万消息 ══╗");

    let rss_b = rss_kb();
    let t0 = Instant::now();
    let payload = vec![0x42u8; 100];
    for i in 0..n {
        mq.publish("q1m", &payload).unwrap();
        if i % 200_000 == 199_999 {
            println!("  {}万...", (i + 1) / 10_000);
        }
    }
    db.persist().unwrap();
    println!(
        "  PUBLISH 100万: {:.2?}  {:.0} msg/s  ΔMem: {}MB",
        t0.elapsed(),
        n as f64 / t0.elapsed().as_secs_f64(),
        rss_kb().saturating_sub(rss_b) / 1024
    );

    let mut lats = Vec::with_capacity(100);
    for _ in 0..100 {
        let t0 = Instant::now();
        mq.poll("q1m", "g1", "c1", 100).unwrap();
        lats.push(t0.elapsed().as_micros() as f64);
    }
    println!(
        "  POLL(100) P95: {:.3}ms  (目标<50ms)",
        p95_us(&mut lats) / 1000.0
    );

    println!("╚═══════════════════════════════════════╝");
}
