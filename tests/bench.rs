/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 性能基准测试：Talon vs SQLite 对比 + 各引擎吞吐量。
//! 运行：cargo test --test bench --release -- --nocapture

use std::collections::BTreeMap;
use std::time::Instant;
use talon::*;

fn bench<F: FnOnce() -> u64>(label: &str, f: F) -> f64 {
    let start = Instant::now();
    let ops = f();
    let elapsed = start.elapsed();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    println!(
        "  {}: {} ops in {:.2?} ({:.0} ops/s)",
        label, ops, elapsed, ops_per_sec
    );
    ops_per_sec
}

// ── KV 基准 ──────────────────────────────────────────────

#[test]
fn benchmark_kv() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let n = 10_000u64;

    println!("\n=== KV Benchmark ===");
    bench("SET (single)", || {
        for i in 0..n {
            kv.set(format!("key:{}", i).as_bytes(), b"value_data_here", None)
                .unwrap();
        }
        n
    });

    // 批量写入测试
    bench("SET (batch)", || {
        let mut batch = db.batch();
        for i in 0..n {
            kv.set_batch(
                &mut batch,
                format!("bkey:{}", i).as_bytes(),
                b"value_data_here",
                None,
            )
            .unwrap();
        }
        batch.commit().unwrap();
        n
    });

    bench("GET", || {
        for i in 0..n {
            kv.get(format!("key:{}", i).as_bytes()).unwrap();
        }
        n
    });
    bench("EXISTS", || {
        for i in 0..n {
            kv.exists(format!("key:{}", i).as_bytes()).unwrap();
        }
        n
    });
    bench("DEL", || {
        for i in 0..n {
            kv.del(format!("key:{}", i).as_bytes()).unwrap();
        }
        n
    });
}

// ── SQL 基准 ─────────────────────────────────────────────

#[test]
fn benchmark_sql() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 1_000u64;

    println!("\n=== SQL Benchmark ===");
    db.run_sql("CREATE TABLE bench (id INT, name TEXT)")
        .unwrap();

    bench("INSERT (single)", || {
        for i in 0..n {
            db.run_sql(&format!(
                "INSERT INTO bench (id, name) VALUES ({}, 'name_{}')",
                i, i
            ))
            .unwrap();
        }
        n
    });
    bench("SELECT by PK", || {
        for i in 0..n {
            db.run_sql(&format!("SELECT * FROM bench WHERE id={}", i))
                .unwrap();
        }
        n
    });
    bench("SELECT * (full scan)", || {
        db.run_sql("SELECT * FROM bench").unwrap();
        1
    });
}

// ── Vector 基准 ──────────────────────────────────────────

#[test]
fn benchmark_vector() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let ve = db.vector("bench").unwrap();
    let n = 1_000u64;
    let dim = 128;

    println!("\n=== Vector Benchmark (dim={}) ===", dim);
    bench("INSERT (HNSW, batch commit)", || {
        for i in 0..n {
            let vec: Vec<f32> = (0..dim)
                .map(|j| (i * dim as u64 + j as u64) as f32 * 0.001)
                .collect();
            ve.insert(i, &vec).unwrap();
        }
        n
    });
    let query: Vec<f32> = (0..dim).map(|j| j as f32 * 0.001).collect();
    bench("KNN search (k=10)", || {
        for _ in 0..100 {
            ve.search(&query, 10, "cosine").unwrap();
        }
        100
    });
    bench("Batch KNN (5 queries)", || {
        let queries: Vec<&[f32]> = vec![&query; 5];
        for _ in 0..20 {
            ve.batch_search(&queries, 10, "cosine").unwrap();
        }
        100 // 20 * 5
    });
}

// ── TimeSeries 基准 ──────────────────────────────────────

#[test]
fn benchmark_timeseries() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["session".into(), "model".into()],
        fields: vec!["tokens".into(), "latency".into()],
    };
    let ts = db.create_timeseries("bench", schema).unwrap();
    let n = 5_000u64;

    println!("\n=== TimeSeries Benchmark ===");
    bench("INSERT", || {
        for i in 0..n {
            let mut tags = BTreeMap::new();
            tags.insert("session".to_string(), format!("s{}", i % 10));
            tags.insert("model".to_string(), "gpt-4".to_string());
            let mut fields = BTreeMap::new();
            fields.insert("tokens".to_string(), "150".to_string());
            fields.insert("latency".to_string(), "42.5".to_string());
            ts.insert(&DataPoint {
                timestamp: i as i64 * 1000,
                tags,
                fields,
            })
            .unwrap();
        }
        n
    });
    bench("QUERY (tag filter + time range)", || {
        for _ in 0..10 {
            ts.query(&TsQuery {
                tag_filters: vec![("session".into(), "s0".into())],
                time_start: Some(0),
                time_end: Some(2_000_000),
                desc: false,
                limit: Some(100),
            })
            .unwrap();
        }
        10
    });
    bench("AGGREGATE (SUM, 1h interval)", || {
        ts.aggregate(&TsAggQuery {
            tag_filters: vec![("model".into(), "gpt-4".into())],
            time_start: None,
            time_end: None,
            field: "tokens".into(),
            func: AggFunc::Sum,
            interval_ms: Some(3_600_000),
            sliding_ms: None,
            session_gap_ms: None,
            fill: None,
        })
        .unwrap();
        1
    });
}

// ── MQ 基准 ──────────────────────────────────────────────

#[test]
fn benchmark_mq() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    let n = 5_000u64;

    println!("\n=== MQ Benchmark ===");
    mq.create_topic("bench", 0).unwrap();
    bench("PUBLISH", || {
        for i in 0..n {
            mq.publish("bench", format!("msg_{}", i).as_bytes())
                .unwrap();
        }
        n
    });
    bench("POLL (100 msgs)", || {
        let mut total = 0u64;
        loop {
            let msgs = mq.poll("bench", "g1", "c1", 100).unwrap();
            if msgs.is_empty() {
                break;
            }
            for m in &msgs {
                mq.ack("bench", "g1", "c1", m.id).unwrap();
            }
            total += msgs.len() as u64;
        }
        total
    });
}

// ── M68: 大规模性能基准（100K+）──────────────────────────

#[test]
fn benchmark_kv_100k() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let n = 100_000u64;

    println!("\n=== KV 100K Benchmark ===");

    // 批量写入 100K
    let write_ops = bench("SET batch 100K", || {
        let batch_size = 1000u64;
        for batch_start in (0..n).step_by(batch_size as usize) {
            let mut batch = db.batch();
            for i in batch_start..batch_start + batch_size {
                kv.set_batch(
                    &mut batch,
                    format!("bk:{:08}", i).as_bytes(),
                    b"value_data_payload_here",
                    None,
                )
                .unwrap();
            }
            batch.commit().unwrap();
        }
        n
    });
    // debug 模式性能约为 release 的 1/5，降低阈值避免误报
    let write_threshold = if cfg!(debug_assertions) {
        10_000.0
    } else {
        50_000.0
    };
    assert!(
        write_ops > write_threshold,
        "批量写入太慢: {:.0} ops/s (阈值 {:.0})",
        write_ops,
        write_threshold
    );

    // 随机读取 100K
    let read_ops = bench("GET random 100K", || {
        for i in 0..n {
            let idx = (i.wrapping_mul(2654435761)) % n;
            kv.get(format!("bk:{:08}", idx).as_bytes()).unwrap();
        }
        n
    });
    let read_threshold = if cfg!(debug_assertions) {
        10_000.0
    } else {
        50_000.0
    };
    assert!(
        read_ops > read_threshold,
        "读取太慢: {:.0} ops/s (阈值 {:.0})",
        read_ops,
        read_threshold
    );

    // 流式计数 100K（O(1) 内存）
    bench("key_count 100K", || {
        let count = kv.key_count().unwrap();
        assert_eq!(count, n);
        1
    });

    // 分页扫描（O(limit) 内存）
    bench("keys_prefix_limit(0, 100) from 100K", || {
        for _ in 0..100 {
            let page = kv.keys_prefix_limit(b"bk:", 0, 100).unwrap();
            assert_eq!(page.len(), 100);
        }
        100
    });

    // scan_prefix_limit
    bench("scan_prefix_limit(0, 100) from 100K", || {
        for _ in 0..100 {
            let page = kv.scan_prefix_limit(b"bk:", 0, 100).unwrap();
            assert_eq!(page.len(), 100);
        }
        100
    });
}

#[test]
fn benchmark_sql_count_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 10_000u64;

    println!("\n=== SQL COUNT/LIMIT Benchmark ===");
    db.run_sql("CREATE TABLE big_t (id INT, val TEXT)").unwrap();

    // 批量插入 10K 行
    bench("SQL INSERT 10K", || {
        for i in 0..n {
            db.run_sql(&format!(
                "INSERT INTO big_t (id, val) VALUES ({}, 'row_{}')",
                i, i
            ))
            .unwrap();
        }
        n
    });

    // COUNT(*) 流式计数（M66 优化）
    bench("COUNT(*) on 10K rows", || {
        for _ in 0..100 {
            let rows = db.run_sql("SELECT COUNT(*) FROM big_t").unwrap();
            assert_eq!(rows[0][0], Value::Integer(n as i64));
        }
        100
    });

    // SELECT LIMIT（M66 LIMIT 下推）
    bench("SELECT * LIMIT 10 on 10K rows", || {
        for _ in 0..100 {
            let rows = db.run_sql("SELECT * FROM big_t LIMIT 10").unwrap();
            assert_eq!(rows.len(), 10);
        }
        100
    });

    // database_stats（M65）
    bench("database_stats", || {
        for _ in 0..100 {
            let _ = db.database_stats().unwrap();
        }
        100
    });

    // health_check（M65）
    bench("health_check", || {
        for _ in 0..100 {
            let h = db.health_check();
            assert_eq!(h["status"], "healthy");
        }
        100
    });
}

// ── Backup 基准 ──────────────────────────────────────────

#[test]
fn benchmark_backup() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path().join("db")).unwrap();
    let kv = db.kv().unwrap();
    let n = 5_000u64;
    for i in 0..n {
        kv.set(format!("k:{}", i).as_bytes(), b"some_value_data", None)
            .unwrap();
    }

    println!("\n=== Backup Benchmark ===");
    let backup_dir = dir.path().join("backup");
    bench("EXPORT", || {
        db.export(&backup_dir, &["kv"]).unwrap();
        n
    });
    let db2 = Talon::open(dir.path().join("db2")).unwrap();
    bench("IMPORT", || {
        db2.import(&backup_dir).unwrap();
        n
    });
}
