/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Talon vs SQLite 公平对比基准测试。
//! 运行：cargo test --test bench_vs_sqlite --release -- --nocapture
//!
//! 测试相同工作负载下的吞吐量对比：
//! - KV: SET/GET 10K 条
//! - SQL: INSERT/SELECT 10K 条
//! - 批量写入 vs 单条写入

use std::time::Instant;

fn bench<F: FnOnce() -> u64>(label: &str, f: F) -> f64 {
    let start = Instant::now();
    let ops = f();
    let elapsed = start.elapsed();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    println!(
        "    {}: {} ops in {:.2?} ({:.0} ops/s)",
        label, ops, elapsed, ops_per_sec
    );
    ops_per_sec
}

fn compare(label: &str, talon_ops: f64, sqlite_ops: f64) {
    let ratio = talon_ops / sqlite_ops;
    let winner = if ratio > 1.0 { "Talon" } else { "SQLite" };
    println!(
        "  >> {} — Talon: {:.0}, SQLite: {:.0}, 比率: {:.2}x ({}胜)",
        label, talon_ops, sqlite_ops, ratio, winner
    );
}

#[test]
fn benchmark_kv_vs_sqlite() {
    let n = 10_000u64;
    println!("\n============================================================");
    println!("=== KV SET/GET: Talon vs SQLite ({} ops) ===", n);
    println!("============================================================");

    // ── Talon KV (batch) ─────────────────────────────────
    let dir = tempfile::tempdir().unwrap();
    let db = talon::Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();

    println!("\n  [Talon]");
    let talon_set = bench("KV SET (batch)", || {
        let mut batch = db.batch();
        for i in 0..n {
            kv.set_batch(
                &mut batch,
                format!("key:{:06}", i).as_bytes(),
                format!("value_data_{}", i).as_bytes(),
                None,
            )
            .unwrap();
        }
        batch.commit().unwrap();
        n
    });

    let talon_get = bench("KV GET", || {
        for i in 0..n {
            kv.get(format!("key:{:06}", i).as_bytes()).unwrap();
        }
        n
    });

    let talon_set_single = bench("KV SET (single)", || {
        for i in 0..n {
            kv.set(
                format!("skey:{:06}", i).as_bytes(),
                format!("value_data_{}", i).as_bytes(),
                None,
            )
            .unwrap();
        }
        n
    });

    // ── SQLite KV ────────────────────────────────────────
    let sqlite_path = dir.path().join("sqlite.db");
    let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB);",
    )
    .unwrap();

    println!("\n  [SQLite]");
    let sqlite_set = bench("KV SET (batch/txn)", || {
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx
                .prepare_cached("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
                .unwrap();
            for i in 0..n {
                stmt.execute(rusqlite::params![
                    format!("key:{:06}", i),
                    format!("value_data_{}", i).as_bytes()
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();
        n
    });

    let sqlite_get = bench("KV GET", || {
        let mut stmt = conn
            .prepare_cached("SELECT value FROM kv WHERE key = ?1")
            .unwrap();
        for i in 0..n {
            let _: Vec<u8> = stmt
                .query_row(rusqlite::params![format!("key:{:06}", i)], |row| row.get(0))
                .unwrap();
        }
        n
    });

    let sqlite_set_single = bench("KV SET (single, no txn)", || {
        let mut stmt = conn
            .prepare_cached("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
            .unwrap();
        for i in 0..n {
            stmt.execute(rusqlite::params![
                format!("skey:{:06}", i),
                format!("value_data_{}", i).as_bytes()
            ])
            .unwrap();
        }
        n
    });

    println!("\n  [对比结果]");
    compare("SET (batch)", talon_set, sqlite_set);
    compare("GET", talon_get, sqlite_get);
    compare("SET (single)", talon_set_single, sqlite_set_single);
}

#[test]
fn benchmark_sql_vs_sqlite() {
    let n = 10_000u64;
    println!("\n============================================================");
    println!("=== SQL INSERT/SELECT: Talon vs SQLite ({} ops) ===", n);
    println!("============================================================");

    // ── Talon SQL ────────────────────────────────────────
    let dir = tempfile::tempdir().unwrap();
    let db = talon::Talon::open(dir.path()).unwrap();
    db.run_sql("CREATE TABLE bench (id INT, name TEXT)")
        .unwrap();

    println!("\n  [Talon]");
    let talon_insert = bench("SQL INSERT", || {
        for i in 0..n {
            db.run_sql(&format!(
                "INSERT INTO bench (id, name) VALUES ({}, 'name_{}')",
                i, i
            ))
            .unwrap();
        }
        n
    });

    let talon_select_pk = bench("SQL SELECT by PK", || {
        for i in 0..n {
            db.run_sql(&format!("SELECT * FROM bench WHERE id={}", i))
                .unwrap();
        }
        n
    });

    // ── SQLite SQL ───────────────────────────────────────
    let sqlite_path = dir.path().join("sqlite.db");
    let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .unwrap();

    println!("\n  [SQLite]");
    // SQLite 单条 INSERT（无显式事务，公平对比 Talon 的逐条 run_sql）
    let sqlite_insert = bench("SQL INSERT (auto-commit)", || {
        let mut stmt = conn
            .prepare_cached("INSERT INTO bench (id, name) VALUES (?1, ?2)")
            .unwrap();
        for i in 0..n {
            stmt.execute(rusqlite::params![i, format!("name_{}", i)])
                .unwrap();
        }
        n
    });

    let sqlite_select_pk = bench("SQL SELECT by PK", || {
        let mut stmt = conn
            .prepare_cached("SELECT * FROM bench WHERE id = ?1")
            .unwrap();
        for i in 0..n {
            let _: (i64, String) = stmt
                .query_row(rusqlite::params![i], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
        }
        n
    });

    // SQLite 批量 INSERT（事务包裹）
    let sqlite_insert_batch = bench("SQL INSERT (txn batch)", || {
        conn.execute("DELETE FROM bench", []).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx
                .prepare_cached("INSERT INTO bench (id, name) VALUES (?1, ?2)")
                .unwrap();
            for i in 0..n {
                stmt.execute(rusqlite::params![
                    i as i64 + n as i64,
                    format!("name_{}", i)
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();
        n
    });

    println!("\n  [对比结果]");
    compare(
        "INSERT (Talon batch vs SQLite auto-commit)",
        talon_insert,
        sqlite_insert,
    );
    compare("SELECT by PK", talon_select_pk, sqlite_select_pk);
    println!(
        "  >> SQLite txn batch INSERT: {:.0} ops/s (参考值)",
        sqlite_insert_batch
    );
}
