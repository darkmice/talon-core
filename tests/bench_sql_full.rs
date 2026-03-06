/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 引擎全方位基准（25 项，对标 SQLite / DuckDB）
//! cargo test --test bench_sql_full --release -- --nocapture

use std::path::Path;
use std::time::Instant;
use talon::Talon;

fn rss_kb() -> u64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}
fn dir_size(p: &Path) -> u64 {
    let mut t = 0u64;
    if let Ok(es) = std::fs::read_dir(p) {
        for e in es.flatten() {
            let pp = e.path();
            if pp.is_dir() {
                t += dir_size(&pp);
            } else if let Ok(m) = pp.metadata() {
                t += m.len();
            }
        }
    }
    t
}
fn hb(b: u64) -> String {
    if b >= 1_048_576 {
        format!("{:.1}MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1}KB", b as f64 / 1024.0)
    } else {
        format!("{}B", b)
    }
}
fn pct(l: &mut [f64]) -> (f64, f64, f64, f64, f64) {
    let n = l.len();
    if n == 0 {
        return (0., 0., 0., 0., 0.);
    }
    l.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let a = l.iter().sum::<f64>() / n as f64;
    (
        a,
        l[(n as f64 * 0.5) as usize],
        l[((n as f64 * 0.95) as usize).min(n - 1)],
        l[((n as f64 * 0.99) as usize).min(n - 1)],
        l[n - 1],
    )
}
fn fms(us: f64) -> String {
    if us < 1000.0 {
        format!("{:.1}us", us)
    } else if us < 1e6 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{:.2}s", us / 1e6)
    }
}

#[test]
fn sql_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         SQL 引擎全方位基准（25 项指标）                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let rss0 = rss_kb();

    // 建表
    db.run_sql("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, cat TEXT)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_cat ON bench(cat)").unwrap();

    // S1: 单条 INSERT (10K)
    let s1n = 10_000u64;
    {
        let t0 = Instant::now();
        for i in 0..s1n {
            db.run_sql(&format!(
                "INSERT INTO bench VALUES ({}, 'user_{}', {}, 'cat{}')",
                i,
                i,
                i % 1000,
                i % 100
            ))
            .unwrap();
        }
        db.persist().unwrap();
        println!(
            "S1  | 单条 INSERT (10K)                   | {:>12.0} rows/s",
            s1n as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // S2: 批量 INSERT (1000行/txn, 至 1M)
    let total = 1_000_000u64;
    {
        let t0 = Instant::now();
        let mut ins = s1n;
        while ins < total {
            db.run_sql("BEGIN").unwrap();
            for j in 0..1000u64 {
                let id = ins + j;
                db.run_sql(&format!(
                    "INSERT INTO bench VALUES ({}, 'user_{}', {}, 'cat{}')",
                    id,
                    id,
                    id % 1000,
                    id % 100
                ))
                .unwrap();
            }
            db.run_sql("COMMIT").unwrap();
            ins += 1000;
        }
        db.persist().unwrap();
        let actual = total - s1n;
        println!(
            "S2  | 批量 INSERT (1000行/txn, 至 1M)     | {:>12.0} rows/s",
            actual as f64 / t0.elapsed().as_secs_f64()
        );
    }

    let rss1 = rss_kb();
    let disk = dir_size(dir.path());
    println!("S22 | 1M 行(4列) 磁盘占用                | {}", hb(disk));
    println!(
        "S23 | 1M 行 RSS 增量                      | {}KB",
        rss1 as i64 - rss0 as i64
    );

    // S3: PK 点查
    {
        let s = 10_000usize;
        let mut lat = Vec::with_capacity(s);
        for i in 0..s {
            let id = (i as u64 * 7 + 13) % total;
            let t = Instant::now();
            let _ = db
                .run_sql(&format!("SELECT * FROM bench WHERE id = {}", id))
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, p50, p95, p99, max) = pct(&mut lat);
        let ops = s as f64 / (lat.iter().sum::<f64>() / 1e6);
        println!(
            "S3  | PK 点查 (10K from 1M)               | {:>12.0} ops/s",
            ops
        );
        println!(
            "      Avg={} P50={} P95={} P99={} Max={}",
            fms(avg),
            fms(p50),
            fms(p95),
            fms(p99),
            fms(max)
        );
    }

    // S4: 索引查询
    {
        let s = 1_000usize;
        let mut lat = Vec::with_capacity(s);
        for i in 0..s {
            let t = Instant::now();
            let _ = db
                .run_sql(&format!(
                    "SELECT * FROM bench WHERE cat = 'cat{}' LIMIT 100",
                    i % 100
                ))
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, p99, _) = pct(&mut lat);
        println!(
            "S4  | 索引查询 WHERE cat=? LIMIT 100      | Avg={} P95={} P99={}",
            fms(avg),
            fms(p95),
            fms(p99)
        );
    }

    // S5: 全表扫描
    {
        let t = Instant::now();
        let _ = db
            .run_sql("SELECT * FROM bench WHERE name = 'user_500000'")
            .unwrap();
        println!(
            "S5  | 全表扫描 WHERE name=? (1M)          | {:.1}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // S6: 范围查询
    {
        let s = 100usize;
        let mut lat = Vec::with_capacity(s);
        for i in 0..s {
            let st = (i as u64) * 10000;
            let t = Instant::now();
            let _ = db
                .run_sql(&format!(
                    "SELECT * FROM bench WHERE id BETWEEN {} AND {} ORDER BY score DESC LIMIT 50",
                    st,
                    st + 10000
                ))
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, p99, _) = pct(&mut lat);
        println!(
            "S6  | BETWEEN+ORDER BY+LIMIT 50 (100)     | Avg={} P95={} P99={}",
            fms(avg),
            fms(p95),
            fms(p99)
        );
    }

    // S7: COUNT(*)
    {
        let s = 20usize;
        let mut lat = Vec::with_capacity(s);
        for _ in 0..s {
            let t = Instant::now();
            let _ = db.run_sql("SELECT COUNT(*) FROM bench").unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (_, _, p95, _, _) = pct(&mut lat);
        println!(
            "S7  | COUNT(*) 1M 行                      | P95={}",
            fms(p95)
        );
    }

    // S8: SUM/AVG
    {
        let t = Instant::now();
        let _ = db.run_sql("SELECT SUM(score) FROM bench").unwrap();
        let sum_ms = t.elapsed().as_secs_f64() * 1000.0;
        let t = Instant::now();
        let _ = db.run_sql("SELECT AVG(score) FROM bench").unwrap();
        let avg_ms = t.elapsed().as_secs_f64() * 1000.0;
        println!(
            "S8  | SUM/AVG(score) 1M                   | SUM={:.2}ms AVG={:.2}ms",
            sum_ms, avg_ms
        );
    }

    // S9: GROUP BY
    {
        let t = Instant::now();
        let rows = db
            .run_sql("SELECT cat, COUNT(*), SUM(score) FROM bench GROUP BY cat")
            .unwrap();
        println!(
            "S9  | GROUP BY cat + COUNT + SUM (1M)     | {:.1}ms ({}组)",
            t.elapsed().as_secs_f64() * 1000.0,
            rows.len()
        );
    }

    // S10: ORDER BY + LIMIT
    {
        let s = 50usize;
        let mut lat = Vec::with_capacity(s);
        for _ in 0..s {
            let t = Instant::now();
            let _ = db
                .run_sql("SELECT * FROM bench ORDER BY score DESC LIMIT 10")
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "S10 | ORDER BY score DESC LIMIT 10 (1M)   | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    // S11: INNER JOIN
    {
        db.run_sql("CREATE TABLE cats (cat TEXT PRIMARY KEY, label TEXT)")
            .unwrap();
        for i in 0..100 {
            db.run_sql(&format!(
                "INSERT INTO cats VALUES ('cat{}', 'label{}')",
                i, i
            ))
            .unwrap();
        }
        let s = 20usize;
        let mut lat = Vec::with_capacity(s);
        for _ in 0..s {
            let t = Instant::now();
            let _ = db.run_sql("SELECT b.id, c.label FROM bench b INNER JOIN cats c ON b.cat = c.cat LIMIT 1000").unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "S11 | INNER JOIN (1Mx100) LIMIT 1000      | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    // S12: LEFT JOIN
    {
        let s = 20usize;
        let mut lat = Vec::with_capacity(s);
        for _ in 0..s {
            let t = Instant::now();
            let _ = db.run_sql("SELECT b.id, c.label FROM bench b LEFT JOIN cats c ON b.cat = c.cat LIMIT 1000").unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "S12 | LEFT JOIN (1Mx100) LIMIT 1000       | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    // S13: 子查询
    {
        let t = Instant::now();
        let _ = db.run_sql("SELECT * FROM bench WHERE cat IN (SELECT cat FROM cats WHERE label = 'label42') LIMIT 100").unwrap();
        println!(
            "S13 | 子查询 WHERE IN (SELECT) LIMIT 100  | {:.1}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // S14: UNION ALL
    {
        let t = Instant::now();
        let _ = db.run_sql("SELECT id, name FROM bench WHERE id < 500 UNION ALL SELECT id, name FROM bench WHERE id >= 999500").unwrap();
        println!(
            "S14 | UNION ALL (500+500)                  | {:.1}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // S15: CTE
    {
        let t = Instant::now();
        let _ = db
            .run_sql("WITH top AS (SELECT * FROM bench WHERE score > 990) SELECT COUNT(*) FROM top")
            .unwrap();
        println!(
            "S15 | CTE WITH...AS + COUNT (1M)          | {:.1}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // S16: 窗口函数
    {
        let t = Instant::now();
        let _ = db.run_sql("SELECT id, score, ROW_NUMBER() OVER (ORDER BY score DESC) as rn FROM bench LIMIT 100").unwrap();
        println!(
            "S16 | ROW_NUMBER() OVER() LIMIT 100       | {:.1}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // S17: UPDATE by PK
    {
        let un = 10_000u64;
        let t0 = Instant::now();
        for i in 0..un {
            db.run_sql(&format!(
                "UPDATE bench SET score = {} WHERE id = {}",
                i + 9999,
                i
            ))
            .unwrap();
        }
        db.persist().unwrap();
        println!(
            "S17 | UPDATE by PK (10K)                  | {:>12.0} ops/s",
            un as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // S18: DELETE by PK
    {
        let dn = 10_000u64;
        let t0 = Instant::now();
        for i in 0..dn {
            db.run_sql(&format!("DELETE FROM bench WHERE id = {}", 990000 + i))
                .unwrap();
        }
        db.persist().unwrap();
        println!(
            "S18 | DELETE by PK (10K)                  | {:>12.0} ops/s",
            dn as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // S19: 事务吞吐
    {
        let txn_n = 1_000usize;
        let t0 = Instant::now();
        for i in 0..txn_n {
            db.run_sql("BEGIN").unwrap();
            for j in 0..100u64 {
                let id = total + (i as u64) * 100 + j;
                db.run_sql(&format!(
                    "INSERT INTO bench VALUES ({}, 'tx_{}', {}, 'cat0')",
                    id, id, j
                ))
                .unwrap();
            }
            db.run_sql("COMMIT").unwrap();
        }
        db.persist().unwrap();
        let txn_s = txn_n as f64 / t0.elapsed().as_secs_f64();
        println!(
            "S19 | BEGIN+100INSERT+COMMIT (1K txn)     | {:>8.0} txn/s ({:.0} rows/s)",
            txn_s,
            txn_s * 100.0
        );
    }

    // S20: EXPLAIN
    {
        let s = 10_000usize;
        let t0 = Instant::now();
        for _ in 0..s {
            let _ = db
                .run_sql("EXPLAIN SELECT * FROM bench WHERE id = 42")
                .unwrap();
        }
        println!(
            "S20 | EXPLAIN (10K)                       | {:>12.0} ops/s",
            s as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // S21: 宽表 INSERT
    {
        let cols: Vec<String> = (0..50).map(|i| format!("c{} INTEGER", i)).collect();
        db.run_sql(&format!(
            "CREATE TABLE wide (id INTEGER PRIMARY KEY, {})",
            cols.join(", ")
        ))
        .unwrap();
        let vals: Vec<String> = (0..50).map(|i| format!("{}", i)).collect();
        let vals_str = vals.join(", ");
        let wn = 10_000u64;
        let t0 = Instant::now();
        db.run_sql("BEGIN").unwrap();
        for i in 0..wn {
            db.run_sql(&format!("INSERT INTO wide VALUES ({}, {})", i, vals_str))
                .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        db.persist().unwrap();
        println!(
            "S21 | 宽表 INSERT (50列, 10K)             | {:>12.0} rows/s",
            wn as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // S24: 行大小梯度 (窄/中/宽)
    println!("\n--- 行大小梯度 (10K each) ---");
    for &(label, ncols) in &[("4列", 4usize), ("20列", 20), ("50列", 50)] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let cols: Vec<String> = (0..ncols.saturating_sub(1))
            .map(|i| format!("c{} INTEGER", i))
            .collect();
        let create = if cols.is_empty() {
            "CREATE TABLE t (id INTEGER PRIMARY KEY)".to_string()
        } else {
            format!(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, {})",
                cols.join(", ")
            )
        };
        db2.run_sql(&create).unwrap();
        let vals: Vec<String> = (0..ncols.saturating_sub(1))
            .map(|i| format!("{}", i))
            .collect();
        let vals_str = vals.join(", ");
        let cnt = 10_000u64;
        let t0 = Instant::now();
        db2.run_sql("BEGIN").unwrap();
        for i in 0..cnt {
            if vals_str.is_empty() {
                db2.run_sql(&format!("INSERT INTO t VALUES ({})", i))
                    .unwrap();
            } else {
                db2.run_sql(&format!("INSERT INTO t VALUES ({}, {})", i, vals_str))
                    .unwrap();
            }
        }
        db2.run_sql("COMMIT").unwrap();
        db2.persist().unwrap();
        let ops = cnt as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(d2.path());
        println!(
            "S24 | {:4} INSERT 10K                     | {:>9.0} rows/s | Disk={}",
            label,
            ops,
            hb(disk)
        );
    }

    // S25: TEXT 列大小梯度
    println!("\n--- TEXT 大小梯度 (10K each) ---");
    for &(label, tlen) in &[("50B", 50usize), ("1KB", 1024), ("10KB", 10240)] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        db2.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT)")
            .unwrap();
        let txt = "x".repeat(tlen);
        let cnt = 10_000u64;
        let t0 = Instant::now();
        db2.run_sql("BEGIN").unwrap();
        for i in 0..cnt {
            db2.run_sql(&format!("INSERT INTO t VALUES ({}, '{}')", i, txt))
                .unwrap();
        }
        db2.run_sql("COMMIT").unwrap();
        db2.persist().unwrap();
        let ops = cnt as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(d2.path());
        println!(
            "S25 | TEXT={:4} INSERT 10K                | {:>9.0} rows/s | Disk={}",
            label,
            ops,
            hb(disk)
        );
    }

    println!("\n✅ SQL 引擎 25 项基准完成");
}
