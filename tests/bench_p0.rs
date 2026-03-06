/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! P0 性能基准验证：百万行表 + P95 延迟测试。
//! 对标产品需求 6.1 性能基准（P0）。
//! 运行：cargo test --test bench_p0 --release -- --nocapture
//!
//! 测试项：
//!   1. 点查询（主键）  P95 < 5ms    百万行表
//!   2. 范围查询        P95 < 50ms   返回100行，有索引
//!   3. 插入（单条）    > 10,000 QPS WAL=NORMAL
//!   4. 插入（批量）    > 100,000 行/秒
//!   5. 更新（主键）    > 5,000 QPS
//!   6. 删除（主键）    > 5,000 QPS
//!   7. 聚合查询        P95 < 500ms  百万行表 COUNT/SUM/AVG

use std::time::Instant;
use talon::*;

const ROWS: u64 = 1_000_000;
const SAMPLE: u64 = 1_000;

fn p95_us(latencies: &mut [f64]) -> f64 {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((latencies.len() as f64) * 0.95) as usize;
    latencies[idx.min(latencies.len() - 1)]
}

fn fill_table(db: &Talon, rows: u64) {
    println!("  Filling {} rows...", rows);
    let t0 = Instant::now();
    db.run_sql("CREATE TABLE p0 (id INT, cat TEXT, score INT, payload TEXT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_p0_cat ON p0(cat)").unwrap();
    // 批量插入：每次 1000 行事务
    let batch_size = 1000u64;
    let mut i = 0u64;
    while i < rows {
        let end = (i + batch_size).min(rows);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            let cat_id = j % 100;
            db.run_sql(&format!(
                "INSERT INTO p0 (id, cat, score, payload) VALUES ({}, 'cat_{}', {}, 'payload_{}')",
                j,
                cat_id,
                j % 10000,
                j
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
        if i % 100_000 == 0 {
            println!("    {}K rows inserted...", i / 1000);
        }
    }
    db.persist().unwrap(); // 落盘校验
    println!("  Fill done in {:.2?}", t0.elapsed());
}

// ── 1. 点查询（主键）P95 < 5ms ──

#[test]
fn p0_point_query_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!(
        "\n=== P0-1: Point Query by PK ({}M rows) ===",
        ROWS / 1_000_000
    );
    fill_table(&db, ROWS);

    let mut latencies = Vec::with_capacity(SAMPLE as usize);
    let step = ROWS / SAMPLE;
    for s in 0..SAMPLE {
        let pk = s * step;
        let t = Instant::now();
        let result = db
            .run_sql(&format!("SELECT * FROM p0 WHERE id = {}", pk))
            .unwrap();
        let us = t.elapsed().as_micros() as f64;
        latencies.push(us);
        assert!(!result.is_empty(), "PK {} should exist", pk);
    }
    let p95 = p95_us(&mut latencies);
    let p95_ms = p95 / 1000.0;
    let avg_us = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!("  Samples: {}", SAMPLE);
    println!("  Avg: {:.1} us", avg_us);
    println!("  P95: {:.1} us ({:.3} ms)", p95, p95_ms);
    println!("  Target: P95 < 5ms");
    if p95_ms < 5.0 {
        println!("  Result: PASS");
    } else {
        println!("  Result: FAIL (P95 = {:.3} ms)", p95_ms);
    }
}

// ── 2. 范围查询 P95 < 50ms ──

#[test]
fn p0_range_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!(
        "\n=== P0-2: Range Query with Index ({}M rows) ===",
        ROWS / 1_000_000
    );
    fill_table(&db, ROWS);

    let mut latencies = Vec::with_capacity(SAMPLE as usize);
    for s in 0..SAMPLE {
        let cat = format!("cat_{}", s % 100);
        let t = Instant::now();
        let result = db
            .run_sql(&format!("SELECT * FROM p0 WHERE cat = '{}' LIMIT 100", cat))
            .unwrap();
        let us = t.elapsed().as_micros() as f64;
        latencies.push(us);
        assert!(!result.is_empty(), "cat {} should have rows", cat);
    }
    let p95 = p95_us(&mut latencies);
    let p95_ms = p95 / 1000.0;
    let avg_us = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!("  Samples: {}", SAMPLE);
    println!("  Avg: {:.1} us", avg_us);
    println!("  P95: {:.1} us ({:.3} ms)", p95, p95_ms);
    println!("  Target: P95 < 50ms");
    if p95_ms < 50.0 {
        println!("  Result: PASS");
    } else {
        println!("  Result: FAIL (P95 = {:.3} ms)", p95_ms);
    }
}

// ── 3. 插入（单条）> 10,000 QPS ──

#[test]
fn p0_insert_single() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 50_000u64;
    println!("\n=== P0-3: Single INSERT > 10K QPS ===");
    db.run_sql("CREATE TABLE p0_ins (id INT, name TEXT, val INT)")
        .unwrap();
    let t = Instant::now();
    for i in 0..n {
        db.run_sql(&format!(
            "INSERT INTO p0_ins (id, name, val) VALUES ({}, 'n_{}', {})",
            i, i, i
        ))
        .unwrap();
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let qps = n as f64 / elapsed.as_secs_f64();
    println!("  {} inserts in {:.2?}", n, elapsed);
    println!("  QPS: {:.0}", qps);
    println!("  Target: > 10,000 QPS");
    if qps > 10_000.0 {
        println!("  Result: PASS ({:.1}x)", qps / 10_000.0);
    } else {
        println!("  Result: FAIL ({:.0} QPS)", qps);
    }
}

// ── 4. 插入（批量）> 100,000 行/秒 ──

#[test]
fn p0_insert_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 100_000u64;
    let batch_size = 1000u64;
    println!(
        "\n=== P0-4: Batch INSERT > 100K rows/s (batch={}) ===",
        batch_size
    );
    db.run_sql("CREATE TABLE p0_batch (id INT, name TEXT, val INT)")
        .unwrap();
    let t = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_size).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&format!(
                "INSERT INTO p0_batch (id, name, val) VALUES ({}, 'n_{}', {})",
                j, j, j
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let rps = n as f64 / elapsed.as_secs_f64();
    println!("  {} rows in {:.2?}", n, elapsed);
    println!("  Rows/s: {:.0}", rps);
    println!("  Target: > 100,000 rows/s");
    if rps > 100_000.0 {
        println!("  Result: PASS ({:.1}x)", rps / 100_000.0);
    } else {
        println!("  Result: FAIL ({:.0} rows/s)", rps);
    }
}

// ── 5. 更新（主键）> 5,000 QPS ──

#[test]
fn p0_update_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let fill = 10_000u64;
    let n = 5_000u64;
    println!("\n=== P0-5: UPDATE by PK > 5K QPS ===");
    db.run_sql("CREATE TABLE p0_upd (id INT, name TEXT, val INT)")
        .unwrap();
    for i in 0..fill {
        db.run_sql(&format!(
            "INSERT INTO p0_upd (id, name, val) VALUES ({}, 'n_{}', {})",
            i, i, i
        ))
        .unwrap();
    }
    let t = Instant::now();
    for i in 0..n {
        db.run_sql(&format!(
            "UPDATE p0_upd SET val = {} WHERE id = {}",
            i + 999,
            i
        ))
        .unwrap();
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let qps = n as f64 / elapsed.as_secs_f64();
    println!("  {} updates in {:.2?}", n, elapsed);
    println!("  QPS: {:.0}", qps);
    println!("  Target: > 5,000 QPS");
    if qps > 5_000.0 {
        println!("  Result: PASS ({:.1}x)", qps / 5_000.0);
    } else {
        println!("  Result: FAIL ({:.0} QPS)", qps);
    }
}

// ── 6. 删除（主键）> 5,000 QPS ──

#[test]
fn p0_delete_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let fill = 10_000u64;
    let n = 5_000u64;
    println!("\n=== P0-6: DELETE by PK > 5K QPS ===");
    db.run_sql("CREATE TABLE p0_del (id INT, name TEXT, val INT)")
        .unwrap();
    for i in 0..fill {
        db.run_sql(&format!(
            "INSERT INTO p0_del (id, name, val) VALUES ({}, 'n_{}', {})",
            i, i, i
        ))
        .unwrap();
    }
    let t = Instant::now();
    for i in 0..n {
        db.run_sql(&format!("DELETE FROM p0_del WHERE id = {}", i))
            .unwrap();
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let qps = n as f64 / elapsed.as_secs_f64();
    println!("  {} deletes in {:.2?}", n, elapsed);
    println!("  QPS: {:.0}", qps);
    println!("  Target: > 5,000 QPS");
    if qps > 5_000.0 {
        println!("  Result: PASS ({:.1}x)", qps / 5_000.0);
    } else {
        println!("  Result: FAIL ({:.0} QPS)", qps);
    }
}

// ── 7. JOIN 查询 P95 < 200ms ──

#[test]
fn p0_join_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 100_000u64;
    let depts = 1_000u64;
    println!(
        "\n=== P0-8: JOIN Query P95 < 200ms ({}K rows x 2 tables) ===",
        n / 1000
    );

    // Create tables with index
    db.run_sql("CREATE TABLE j_users (id INT, name TEXT, dept_id INT)")
        .unwrap();
    db.run_sql("CREATE TABLE j_depts (id INT, dept_name TEXT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_j_dept ON j_users(dept_id)")
        .unwrap();

    // Fill depts
    println!("  Filling {} depts...", depts);
    db.run_sql("BEGIN").unwrap();
    for i in 0..depts {
        db.run_sql(&format!(
            "INSERT INTO j_depts (id, dept_name) VALUES ({}, 'dept_{}')",
            i, i
        ))
        .unwrap();
    }
    db.run_sql("COMMIT").unwrap();

    // Fill users
    println!("  Filling {} users...", n);
    let batch_size = 1000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_size).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&format!(
                "INSERT INTO j_users (id, name, dept_id) VALUES ({}, 'user_{}', {})",
                j,
                j,
                j % depts
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
        if i % 20_000 == 0 {
            println!("    {}K users inserted...", i / 1000);
        }
    }

    // Benchmark: JOIN with WHERE on specific dept (returns ~100 rows)
    let mut latencies = Vec::with_capacity(100);
    for s in 0..100u64 {
        let dept = s % depts;
        let t = Instant::now();
        let result = db.run_sql(&format!(
            "SELECT name, dept_name FROM j_users JOIN j_depts ON j_users.dept_id = j_depts.id WHERE dept_name = 'dept_{}'",
            dept
        )).unwrap();
        let us = t.elapsed().as_micros() as f64;
        latencies.push(us);
        assert!(!result.is_empty(), "dept_{} should have rows", dept);
    }
    let p95 = p95_us(&mut latencies);
    let p95_ms = p95 / 1000.0;
    let avg_ms = latencies.iter().sum::<f64>() / latencies.len() as f64 / 1000.0;
    println!("  Samples: 100");
    println!("  Avg: {:.1} ms", avg_ms);
    println!("  P95: {:.1} ms", p95_ms);
    println!("  Target: P95 < 200ms");
    if p95_ms < 200.0 {
        println!("  Result: PASS ({:.1}x margin)", 200.0 / p95_ms);
    } else {
        println!("  Result: FAIL (P95 = {:.1} ms)", p95_ms);
    }
}

// ── 7b. 聚合查询 P95 < 500ms ──

#[test]
fn p0_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!(
        "\n=== P0-7: Aggregate Query ({}M rows) P95 < 500ms ===",
        ROWS / 1_000_000
    );
    fill_table(&db, ROWS);

    let queries = [
        "SELECT COUNT(*) FROM p0",
        "SELECT SUM(score) FROM p0",
        "SELECT AVG(score) FROM p0",
    ];
    // M102：warmup 让 LSM compaction 稳定，消除 P95 尾部延迟
    for query in &queries {
        db.run_sql(query).unwrap();
    }
    for query in &queries {
        let mut latencies = Vec::with_capacity(10);
        for _ in 0..10 {
            let t = Instant::now();
            db.run_sql(query).unwrap();
            let us = t.elapsed().as_micros() as f64;
            latencies.push(us);
        }
        let p95 = p95_us(&mut latencies);
        let p95_ms = p95 / 1000.0;
        let avg_ms = latencies.iter().sum::<f64>() / latencies.len() as f64 / 1000.0;
        let pass = if p95_ms < 500.0 { "PASS" } else { "FAIL" };
        println!(
            "  {}: avg={:.1}ms P95={:.1}ms [{}]",
            query, avg_ms, p95_ms, pass
        );
    }
}

// ── 8. 精准落盘校验：close→reopen→逐条验证 ──

#[test]
fn p0_durability_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let rows = 100_000u64;
    println!(
        "\n=== P0-DUR: Durability Verify ({}K rows, close→reopen→verify) ===",
        rows / 1000
    );

    // Phase 1: 写入 → persist → drop
    {
        let db = Talon::open(&path).unwrap();
        fill_table(&db, rows);
        drop(db);
    }

    // Phase 2: reopen → 抽样验证数据完整性
    {
        let db = Talon::open(&path).unwrap();
        let sample = 1000u64;
        let step = rows / sample;
        let mut verified = 0u64;
        for s in 0..sample {
            let pk = s * step;
            let result = db
                .run_sql(&format!("SELECT * FROM p0 WHERE id = {}", pk))
                .unwrap();
            assert!(!result.is_empty(), "PK {} 丢失！数据未落盘", pk);
            assert_eq!(result[0].len(), 4, "PK {} 列数不对", pk);
            // 验证列值正确性
            let expected_cat = format!("cat_{}", pk % 100);
            let expected_score = (pk % 10000) as i64;
            match &result[0][0] {
                Value::Integer(id) => assert_eq!(*id, pk as i64, "PK {} id 值不匹配", pk),
                other => panic!("PK {} id 类型错误: {:?}", pk, other),
            }
            match &result[0][1] {
                Value::Text(cat) => assert_eq!(cat, &expected_cat, "PK {} cat 值不匹配", pk),
                other => panic!("PK {} cat 类型错误: {:?}", pk, other),
            }
            match &result[0][2] {
                Value::Integer(score) => {
                    assert_eq!(*score, expected_score, "PK {} score 值不匹配", pk)
                }
                other => panic!("PK {} score 类型错误: {:?}", pk, other),
            }
            verified += 1;
        }
        // 验证总行数
        let count = db.run_sql("SELECT COUNT(*) FROM p0").unwrap();
        match &count[0][0] {
            Value::Integer(n) => assert_eq!(*n, rows as i64, "总行数不匹配"),
            other => panic!("COUNT 类型错误: {:?}", other),
        }
        println!("  ✅ {} 行抽样验证通过，总行数 {} 正确", verified, rows);
    }
}
