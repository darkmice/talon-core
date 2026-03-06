/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 引擎高级功能基准测试：覆盖 DML/DDL/索引/排序/聚合/事务/EXPLAIN。
//! 运行：cargo test --test bench_sql_advanced --release -- --nocapture

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

// ── DDL ──────────────────────────────────────────────────

#[test]
fn bench_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!("\n=== SQL DDL Benchmark ===");

    // CREATE TABLE + DROP TABLE
    bench("CREATE+DROP TABLE ×100", || {
        for i in 0..100u64 {
            db.run_sql(&format!("CREATE TABLE ddl_{} (id INT, v TEXT)", i))
                .unwrap();
        }
        for i in 0..100u64 {
            db.run_sql(&format!("DROP TABLE ddl_{}", i)).unwrap();
        }
        200
    });

    // ALTER TABLE
    db.run_sql("CREATE TABLE alt (id INT, a TEXT)").unwrap();
    bench("ALTER TABLE ADD/RENAME/DROP ×50", || {
        for i in 0..50u64 {
            db.run_sql(&format!("ALTER TABLE alt ADD COLUMN c{} TEXT", i))
                .unwrap();
        }
        for i in 0..50u64 {
            db.run_sql(&format!("ALTER TABLE alt RENAME COLUMN c{} TO r{}", i, i))
                .unwrap();
        }
        for i in 0..50u64 {
            db.run_sql(&format!("ALTER TABLE alt DROP COLUMN r{}", i))
                .unwrap();
        }
        150
    });

    // CREATE INDEX
    db.run_sql("CREATE TABLE idx_t (id INT, cat TEXT, val INT)")
        .unwrap();
    for i in 0..1000u64 {
        db.run_sql(&format!(
            "INSERT INTO idx_t (id, cat, val) VALUES ({}, 'c{}', {})",
            i,
            i % 10,
            i
        ))
        .unwrap();
    }
    bench("CREATE INDEX on 1K rows", || {
        db.run_sql("CREATE INDEX idx_cat ON idx_t(cat)").unwrap();
        1
    });
}

// ── UPDATE / DELETE ──────────────────────────────────────

#[test]
fn bench_update_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 5_000u64;
    println!("\n=== SQL UPDATE/DELETE Benchmark ===");

    db.run_sql("CREATE TABLE ud (id INT, cat TEXT, score INT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_ud_cat ON ud(cat)").unwrap();
    for i in 0..n {
        db.run_sql(&format!(
            "INSERT INTO ud (id, cat, score) VALUES ({}, 'c{}', {})",
            i,
            i % 20,
            i
        ))
        .unwrap();
    }

    // UPDATE by PK
    bench("UPDATE by PK ×1000", || {
        for i in 0..1000u64 {
            db.run_sql(&format!("UPDATE ud SET score = 0 WHERE id = {}", i))
                .unwrap();
        }
        1000
    });

    // UPDATE by index (M78)
    bench("UPDATE by index ×20 categories", || {
        for i in 0..20u64 {
            db.run_sql(&format!("UPDATE ud SET score = 999 WHERE cat = 'c{}'", i))
                .unwrap();
        }
        20
    });

    // UPDATE AND multi-condition (M78)
    bench("UPDATE AND (index+filter) ×20", || {
        for i in 0..20u64 {
            db.run_sql(&format!(
                "UPDATE ud SET score = 111 WHERE cat = 'c{}' AND score = 999",
                i
            ))
            .unwrap();
        }
        20
    });

    // DELETE by index (M78)
    bench("DELETE by index ×10 categories", || {
        for i in 0..10u64 {
            db.run_sql(&format!("DELETE FROM ud WHERE cat = 'c{}'", i))
                .unwrap();
        }
        10
    });

    // DELETE by PK
    bench("DELETE by PK ×500", || {
        for i in 2500..3000u64 {
            db.run_sql(&format!("DELETE FROM ud WHERE id = {}", i))
                .unwrap();
        }
        500
    });

    // DELETE AND multi-condition
    bench("DELETE AND (PK+filter) ×200", || {
        for i in 3000..3200u64 {
            db.run_sql(&format!("DELETE FROM ud WHERE id = {} AND score = 111", i))
                .unwrap();
        }
        200
    });
}

// ── SELECT 高级查询 ──────────────────────────────────────

#[test]
fn bench_select_advanced() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 5_000u64;
    println!("\n=== SQL SELECT Advanced Benchmark ===");

    db.run_sql("CREATE TABLE sa (id INT, cat TEXT, score INT, tag TEXT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_sa_cat ON sa(cat)").unwrap();
    for i in 0..n {
        db.run_sql(&format!(
            "INSERT INTO sa (id, cat, score, tag) VALUES ({}, 'c{}', {}, 't{}')",
            i,
            i % 50,
            i % 1000,
            i % 5
        ))
        .unwrap();
    }

    // WHERE by index
    bench("SELECT WHERE index=val ×100", || {
        for i in 0..100u64 {
            db.run_sql(&format!("SELECT * FROM sa WHERE cat = 'c{}'", i % 50))
                .unwrap();
        }
        100
    });

    // WHERE AND index + filter (M76)
    bench("SELECT WHERE AND (index+filter) ×100", || {
        for i in 0..100u64 {
            db.run_sql(&format!(
                "SELECT * FROM sa WHERE cat = 'c{}' AND score = {}",
                i % 50,
                i % 1000
            ))
            .unwrap();
        }
        100
    });

    // ORDER BY + LIMIT (TopN M75)
    bench("SELECT ORDER BY + LIMIT 10 ×100", || {
        for _ in 0..100u64 {
            let rows = db
                .run_sql("SELECT * FROM sa ORDER BY score DESC LIMIT 10")
                .unwrap();
            assert_eq!(rows.len(), 10);
        }
        100
    });

    // WHERE + ORDER BY + LIMIT (M77)
    bench("SELECT WHERE + ORDER BY + LIMIT ×100", || {
        for i in 0..100u64 {
            let rows = db
                .run_sql(&format!(
                    "SELECT * FROM sa WHERE cat = 'c{}' ORDER BY score DESC LIMIT 5",
                    i % 50
                ))
                .unwrap();
            assert!(rows.len() <= 5);
        }
        100
    });

    // DISTINCT
    bench("SELECT DISTINCT cat ×100", || {
        for _ in 0..100u64 {
            let rows = db.run_sql("SELECT DISTINCT cat FROM sa").unwrap();
            assert!(!rows.is_empty());
            assert!(rows.len() <= 5000);
        }
        100
    });

    // OFFSET + LIMIT
    bench("SELECT LIMIT 10 OFFSET 100 ×100", || {
        for _ in 0..100u64 {
            let rows = db.run_sql("SELECT * FROM sa LIMIT 10 OFFSET 100").unwrap();
            assert_eq!(rows.len(), 10);
        }
        100
    });

    // WHERE OR
    bench("SELECT WHERE OR ×50", || {
        for i in 0..50u64 {
            db.run_sql(&format!(
                "SELECT * FROM sa WHERE cat = 'c{}' OR cat = 'c{}'",
                i % 50,
                (i + 1) % 50
            ))
            .unwrap();
        }
        50
    });

    // WHERE BETWEEN
    bench("SELECT WHERE BETWEEN ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT * FROM sa WHERE score BETWEEN 100 AND 200")
                .unwrap();
        }
        50
    });

    // WHERE LIKE
    bench("SELECT WHERE LIKE ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT * FROM sa WHERE cat LIKE 'c1%'").unwrap();
        }
        50
    });

    // WHERE IN
    bench("SELECT WHERE IN ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT * FROM sa WHERE cat IN ('c0', 'c1', 'c2')")
                .unwrap();
        }
        50
    });

    // WHERE IS NULL (insert some nulls first)
    db.run_sql("ALTER TABLE sa ADD COLUMN extra TEXT").unwrap();
    bench("SELECT WHERE IS NULL ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT * FROM sa WHERE extra IS NULL LIMIT 10")
                .unwrap();
        }
        50
    });
}

// ── 聚合函数 ─────────────────────────────────────────────

#[test]
fn bench_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 5_000u64;
    println!("\n=== SQL Aggregates Benchmark ===");

    db.run_sql("CREATE TABLE agg (id INT, cat TEXT, val INT)")
        .unwrap();
    for i in 0..n {
        db.run_sql(&format!(
            "INSERT INTO agg (id, cat, val) VALUES ({}, 'c{}', {})",
            i,
            i % 10,
            i
        ))
        .unwrap();
    }

    bench("COUNT(*) on 5K ×100", || {
        for _ in 0..100u64 {
            let rows = db.run_sql("SELECT COUNT(*) FROM agg").unwrap();
            assert_eq!(rows[0][0], Value::Integer(n as i64));
        }
        100
    });

    bench("SUM(val) on 5K ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT SUM(val) FROM agg").unwrap();
        }
        50
    });

    bench("AVG(val) on 5K ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT AVG(val) FROM agg").unwrap();
        }
        50
    });

    bench("MIN(val), MAX(val) on 5K ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT MIN(val), MAX(val) FROM agg").unwrap();
        }
        50
    });

    bench("COUNT(*) WHERE cat='c0' on 5K ×50", || {
        for _ in 0..50u64 {
            db.run_sql("SELECT COUNT(*) FROM agg WHERE cat = 'c0'")
                .unwrap();
        }
        50
    });
}

// ── 事务 + EXPLAIN + INSERT OR REPLACE ───────────────────

#[test]
fn bench_transactions_and_extras() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!("\n=== SQL Transactions/EXPLAIN/Upsert Benchmark ===");

    db.run_sql("CREATE TABLE tx (id INT, v TEXT)").unwrap();

    // Transactions
    bench("BEGIN+INSERT×100+COMMIT ×10", || {
        for batch in 0..10u64 {
            db.run_sql("BEGIN").unwrap();
            for i in 0..100u64 {
                let id = batch * 100 + i;
                db.run_sql(&format!(
                    "INSERT INTO tx (id, v) VALUES ({}, 'v{}')",
                    id, id
                ))
                .unwrap();
            }
            db.run_sql("COMMIT").unwrap();
        }
        10
    });

    // ROLLBACK
    bench("BEGIN+INSERT×50+ROLLBACK ×10", || {
        for _ in 0..10u64 {
            db.run_sql("BEGIN").unwrap();
            for i in 5000..5050u64 {
                db.run_sql(&format!("INSERT INTO tx (id, v) VALUES ({}, 'tmp')", i))
                    .unwrap();
            }
            db.run_sql("ROLLBACK").unwrap();
        }
        10
    });
    // verify rollback worked
    let rows = db.run_sql("SELECT * FROM tx WHERE id = 5000").unwrap();
    assert!(rows.is_empty(), "ROLLBACK should have undone inserts");

    // INSERT OR REPLACE
    bench("INSERT OR REPLACE ×500", || {
        for i in 0..500u64 {
            db.run_sql(&format!(
                "INSERT OR REPLACE INTO tx (id, v) VALUES ({}, 'replaced')",
                i
            ))
            .unwrap();
        }
        500
    });

    // EXPLAIN
    db.run_sql("CREATE TABLE ex (id INT, cat TEXT, n INT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_ex ON ex(cat)").unwrap();
    bench("EXPLAIN ×100", || {
        for _ in 0..100u64 {
            db.run_sql("EXPLAIN SELECT * FROM ex WHERE cat = 'a' AND n = 1")
                .unwrap();
        }
        100
    });

    // SHOW TABLES
    bench("SHOW TABLES ×100", || {
        for _ in 0..100u64 {
            db.run_sql("SHOW TABLES").unwrap();
        }
        100
    });
}
