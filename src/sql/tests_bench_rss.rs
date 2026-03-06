/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 实测 RSS 内存基准：用 macOS `ps` 命令测量查询前后真实内存增量。
//! 验证 Top-N 堆排序的 O(limit) 内存特性。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;
use std::time::Instant;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

/// 获取当前进程 RSS（单位：KB）。macOS `ps -o rss=` 返回 KB。
fn rss_kb() -> u64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps 命令失败");
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse::<u64>().unwrap_or(0)
}

const ROW_COUNT: usize = 100_000;

fn setup_table(eng: &mut SqlEngine) {
    eng.run_sql("CREATE TABLE big (id INT, cat TEXT, data TEXT)")
        .unwrap();
    for i in 0..ROW_COUNT {
        let cat = format!("c{}", i % 200);
        let data = "y".repeat(80);
        eng.run_sql(&format!(
            "INSERT INTO big (id, cat, data) VALUES ({}, '{}', '{}')",
            i, cat, data
        ))
        .unwrap();
    }
}

/// 实测1: ORDER BY + LIMIT 1000 — Top-N 堆排序
/// 期望：查询 RSS 增量远小于全表大小（~28MB for 100K rows）
#[test]
fn rss_order_by_limit() {
    let (_dir, mut eng) = tmp_engine();
    setup_table(&mut eng);

    // 预热：触发存储引擎缓存稳定
    let _ = eng.run_sql("SELECT * FROM big LIMIT 1");
    std::thread::sleep(std::time::Duration::from_millis(100));

    let rss_before = rss_kb();
    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM big ORDER BY id LIMIT 1000")
        .unwrap();
    let elapsed = t.elapsed();
    let rss_after = rss_kb();

    assert_eq!(rows.len(), 1000);
    assert_eq!(rows[0][0], Value::Integer(0));
    assert_eq!(rows[999][0], Value::Integer(999));

    let delta_kb = rss_after.saturating_sub(rss_before);
    eprintln!("═══════════════════════════════════════════════════");
    eprintln!("  [ORDER BY+LIMIT] 100K行 Top-N 查1000条");
    eprintln!("  耗时: {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    eprintln!(
        "  RSS 前: {}KB | RSS 后: {}KB | 增量: {}KB",
        rss_before, rss_after, delta_kb
    );
    eprintln!(
        "  理论全表内存: ~{}MB (100K×~300B)",
        ROW_COUNT * 300 / 1024 / 1024
    );
    eprintln!("  Top-N 堆理论: ~{}KB (1000×~300B)", 1000 * 300 / 1024);
    eprintln!("═══════════════════════════════════════════════════");
}

/// 实测2: LIMIT 下推（无 ORDER BY）
#[test]
fn rss_limit_pushdown() {
    let (_dir, mut eng) = tmp_engine();
    setup_table(&mut eng);

    let _ = eng.run_sql("SELECT * FROM big LIMIT 1");
    std::thread::sleep(std::time::Duration::from_millis(100));

    let rss_before = rss_kb();
    let t = Instant::now();
    let rows = eng.run_sql("SELECT * FROM big LIMIT 1000").unwrap();
    let elapsed = t.elapsed();
    let rss_after = rss_kb();

    assert_eq!(rows.len(), 1000);
    let delta_kb = rss_after.saturating_sub(rss_before);
    eprintln!("═══════════════════════════════════════════════════");
    eprintln!("  [LIMIT 下推] 100K行取1000条");
    eprintln!("  耗时: {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    eprintln!(
        "  RSS 前: {}KB | RSS 后: {}KB | 增量: {}KB",
        rss_before, rss_after, delta_kb
    );
    eprintln!("═══════════════════════════════════════════════════");
}

/// 实测3: COUNT(*) 流式计数
#[test]
fn rss_count_star() {
    let (_dir, mut eng) = tmp_engine();
    setup_table(&mut eng);

    let _ = eng.run_sql("SELECT * FROM big LIMIT 1");
    std::thread::sleep(std::time::Duration::from_millis(100));

    let rss_before = rss_kb();
    let t = Instant::now();
    let rows = eng.run_sql("SELECT COUNT(*) FROM big").unwrap();
    let elapsed = t.elapsed();
    let rss_after = rss_kb();

    assert_eq!(rows[0][0], Value::Integer(ROW_COUNT as i64));
    let delta_kb = rss_after.saturating_sub(rss_before);
    eprintln!("═══════════════════════════════════════════════════");
    eprintln!("  [COUNT(*)] 100K行流式计数");
    eprintln!("  耗时: {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    eprintln!(
        "  RSS 前: {}KB | RSS 后: {}KB | 增量: {}KB",
        rss_before, rss_after, delta_kb
    );
    eprintln!("═══════════════════════════════════════════════════");
}

/// 实测4: ORDER BY DESC + LIMIT
#[test]
fn rss_order_by_desc_limit() {
    let (_dir, mut eng) = tmp_engine();
    setup_table(&mut eng);

    let _ = eng.run_sql("SELECT * FROM big LIMIT 1");
    std::thread::sleep(std::time::Duration::from_millis(100));

    let rss_before = rss_kb();
    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM big ORDER BY id DESC LIMIT 10")
        .unwrap();
    let elapsed = t.elapsed();
    let rss_after = rss_kb();

    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0][0], Value::Integer(ROW_COUNT as i64 - 1));
    let delta_kb = rss_after.saturating_sub(rss_before);
    eprintln!("═══════════════════════════════════════════════════");
    eprintln!("  [ORDER BY DESC+LIMIT] 100K行取TOP 10");
    eprintln!("  耗时: {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    eprintln!(
        "  RSS 前: {}KB | RSS 后: {}KB | 增量: {}KB",
        rss_before, rss_after, delta_kb
    );
    eprintln!("═══════════════════════════════════════════════════");
}

/// 实测5: PK 点查
#[test]
fn rss_pk_lookup() {
    let (_dir, mut eng) = tmp_engine();
    setup_table(&mut eng);

    let _ = eng.run_sql("SELECT * FROM big LIMIT 1");
    std::thread::sleep(std::time::Duration::from_millis(100));

    let rss_before = rss_kb();
    let t = Instant::now();
    let rows = eng.run_sql("SELECT * FROM big WHERE id = 50000").unwrap();
    let elapsed = t.elapsed();
    let rss_after = rss_kb();

    assert_eq!(rows.len(), 1);
    let delta_kb = rss_after.saturating_sub(rss_before);
    eprintln!("═══════════════════════════════════════════════════");
    eprintln!("  [PK 点查] 100K行精确查1条");
    eprintln!("  耗时: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
    eprintln!(
        "  RSS 前: {}KB | RSS 后: {}KB | 增量: {}KB",
        rss_before, rss_after, delta_kb
    );
    eprintln!("═══════════════════════════════════════════════════");
}
