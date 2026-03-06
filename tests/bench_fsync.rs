/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! F-1 fsync 优化基准：对比默认模式 vs manual_journal_persist 模式。
//! 运行：cargo test --test bench_fsync --release -- --nocapture

use std::time::Instant;
use talon::StorageConfig;

fn bench<S: FnOnce(), F: FnOnce() -> u64>(label: &str, setup: S, f: F) -> f64 {
    setup();
    let start = Instant::now();
    let ops = f();
    let elapsed = start.elapsed();
    let ops_sec = ops as f64 / elapsed.as_secs_f64();
    println!(
        "    {}: {} ops in {:.2?} ({:.0} ops/s)",
        label, ops, elapsed, ops_sec
    );
    ops_sec
}

fn compare(label: &str, default_ops: f64, fast_ops: f64) {
    let ratio = fast_ops / default_ops;
    println!(
        "  >> {} — 默认: {:.0}, 高吞吐: {:.0}, 提升: {:.1}x",
        label, default_ops, fast_ops, ratio
    );
}

#[test]
fn bench_fsync_tx_commit() {
    println!("\n================================================================");
    println!("=== F-1: TX 批量提交 — 默认 vs manual_journal_persist ===");
    println!("================================================================");

    let n = 20u64;

    println!("\n  [默认模式]");
    let dir1 = tempfile::tempdir().unwrap();
    let db1 = talon::Talon::open(dir1.path()).unwrap();
    db1.run_sql("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    let default_ops = bench(
        "BEGIN+100INSERT+COMMIT",
        || {},
        || {
            for i in 0..n {
                db1.run_sql("BEGIN").unwrap();
                for j in 0..100u64 {
                    let id = i * 100 + j;
                    db1.run_sql(&format!("INSERT INTO t VALUES ({}, {})", id, id))
                        .unwrap();
                }
                db1.run_sql("COMMIT").unwrap();
            }
            n
        },
    );

    println!("\n  [高吞吐模式 manual_journal_persist=true]");
    let dir2 = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        manual_journal_persist: true,
        ..Default::default()
    };
    let db2 = talon::Talon::open_with_config(dir2.path(), cfg).unwrap();
    db2.run_sql("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    let fast_ops = bench(
        "BEGIN+100INSERT+COMMIT",
        || {},
        || {
            for i in 0..n {
                db2.run_sql("BEGIN").unwrap();
                for j in 0..100u64 {
                    let id = i * 100 + j;
                    db2.run_sql(&format!("INSERT INTO t VALUES ({}, {})", id, id))
                        .unwrap();
                }
                db2.run_sql("COMMIT").unwrap();
            }
            db2.persist().unwrap();
            n
        },
    );

    println!();
    compare("TX COMMIT (100 rows)", default_ops, fast_ops);
}

#[test]
fn bench_fsync_kv_set() {
    println!("\n================================================================");
    println!("=== F-2: KV SET single — 默认 vs manual_journal_persist ===");
    println!("================================================================");

    let n = 10_000u64;

    println!("\n  [默认模式]");
    let dir1 = tempfile::tempdir().unwrap();
    let db1 = talon::Talon::open(dir1.path()).unwrap();
    let kv1 = db1.kv().unwrap();
    let default_ops = bench(
        "KV SET single",
        || {},
        || {
            for i in 0..n {
                kv1.set(format!("k:{:06}", i).as_bytes(), b"val", None)
                    .unwrap();
            }
            n
        },
    );

    println!("\n  [高吞吐模式]");
    let dir2 = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        manual_journal_persist: true,
        ..Default::default()
    };
    let db2 = talon::Talon::open_with_config(dir2.path(), cfg).unwrap();
    let kv2 = db2.kv().unwrap();
    let fast_ops = bench(
        "KV SET single",
        || {},
        || {
            for i in 0..n {
                kv2.set(format!("k:{:06}", i).as_bytes(), b"val", None)
                    .unwrap();
            }
            db2.persist().unwrap();
            n
        },
    );

    println!();
    compare("KV SET single", default_ops, fast_ops);
}

#[test]
fn bench_fsync_create_table() {
    println!("\n================================================================");
    println!("=== F-3: CREATE TABLE — 默认 vs manual_journal_persist ===");
    println!("================================================================");

    let n = 50u64;

    println!("\n  [默认模式]");
    let dir1 = tempfile::tempdir().unwrap();
    let db1 = talon::Talon::open(dir1.path()).unwrap();
    let default_ops = bench(
        "CREATE+DROP TABLE",
        || {},
        || {
            for i in 0..n {
                db1.run_sql(&format!("CREATE TABLE t{} (id INT PRIMARY KEY)", i))
                    .unwrap();
                db1.run_sql(&format!("DROP TABLE t{}", i)).unwrap();
            }
            n * 2
        },
    );

    println!("\n  [高吞吐模式]");
    let dir2 = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        manual_journal_persist: true,
        ..Default::default()
    };
    let db2 = talon::Talon::open_with_config(dir2.path(), cfg).unwrap();
    let fast_ops = bench(
        "CREATE+DROP TABLE",
        || {},
        || {
            for i in 0..n {
                db2.run_sql(&format!("CREATE TABLE t{} (id INT PRIMARY KEY)", i))
                    .unwrap();
                db2.run_sql(&format!("DROP TABLE t{}", i)).unwrap();
            }
            db2.persist().unwrap();
            n * 2
        },
    );

    println!();
    compare("CREATE+DROP TABLE", default_ops, fast_ops);
}

#[test]
fn bench_fsync_vector_insert() {
    println!("\n================================================================");
    println!("=== F-6: Vector INSERT — 默认 vs manual_journal_persist ===");
    println!("================================================================");

    let n = 500u64;
    let dim = 128;
    let vecs: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            (0..dim)
                .map(|d| ((i * dim as u64 + d as u64) as f32).sin())
                .collect()
        })
        .collect();

    println!("\n  [默认模式]");
    let dir1 = tempfile::tempdir().unwrap();
    let db1 = talon::Talon::open(dir1.path()).unwrap();
    let ve1 = db1.vector("bench_vec").unwrap();
    let default_ops = bench(
        "Vector INSERT",
        || {},
        || {
            for (i, v) in vecs.iter().enumerate() {
                ve1.insert(i as u64, v).unwrap();
            }
            n
        },
    );

    println!("\n  [高吞吐模式]");
    let dir2 = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        manual_journal_persist: true,
        ..Default::default()
    };
    let db2 = talon::Talon::open_with_config(dir2.path(), cfg).unwrap();
    let ve2 = db2.vector("bench_vec").unwrap();
    let fast_ops = bench(
        "Vector INSERT",
        || {},
        || {
            for (i, v) in vecs.iter().enumerate() {
                ve2.insert(i as u64, v).unwrap();
            }
            db2.persist().unwrap();
            n
        },
    );

    println!();
    compare("Vector INSERT (HNSW)", default_ops, fast_ops);
}

#[test]
fn bench_fsync_sql_insert_single() {
    println!("\n================================================================");
    println!("=== SQL INSERT single — 默认 vs manual_journal_persist ===");
    println!("================================================================");

    let n = 10_000u64;

    println!("\n  [默认模式]");
    let dir1 = tempfile::tempdir().unwrap();
    let db1 = talon::Talon::open(dir1.path()).unwrap();
    db1.run_sql("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    let default_ops = bench(
        "SQL INSERT",
        || {},
        || {
            for i in 0..n {
                db1.run_sql(&format!("INSERT INTO t VALUES ({}, 'hello_{}')", i, i))
                    .unwrap();
            }
            n
        },
    );

    println!("\n  [高吞吐模式]");
    let dir2 = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        manual_journal_persist: true,
        ..Default::default()
    };
    let db2 = talon::Talon::open_with_config(dir2.path(), cfg).unwrap();
    db2.run_sql("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    let fast_ops = bench(
        "SQL INSERT",
        || {},
        || {
            for i in 0..n {
                db2.run_sql(&format!("INSERT INTO t VALUES ({}, 'hello_{}')", i, i))
                    .unwrap();
            }
            db2.persist().unwrap();
            n
        },
    );

    println!();
    compare("SQL INSERT single", default_ops, fast_ops);
}
