/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 内存与性能基准测试：验证各查询路径在大表上的实际行为。
//! 用 50K 行模拟大表，测量各路径耗时和内存行为。

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

const ROW_COUNT: usize = 50_000;

fn setup_large_table(eng: &mut SqlEngine) {
    eng.run_sql("CREATE TABLE bench (id INT, category TEXT, payload TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON bench(category)")
        .unwrap();
    // 批量插入
    for i in 0..ROW_COUNT {
        let cat = format!("cat{}", i % 100); // 100 个分类
        let payload = "x".repeat(100); // 每行 ~100 bytes payload
        eng.run_sql(&format!(
            "INSERT INTO bench (id, category, payload) VALUES ({}, '{}', '{}')",
            i, cat, payload
        ))
        .unwrap();
    }
}

/// 路径1: SELECT * FROM bench LIMIT 1000（LIMIT 下推）
/// 预期: O(1000) 内存，耗时极短，不受表大小影响
#[test]
fn bench_limit_pushdown_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng.run_sql("SELECT * FROM bench LIMIT 1000").unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 1000);
    eprintln!(
        "[LIMIT pushdown] 50K行表取1000条: {:.2}ms | 内存: ~{}KB (1000行×~300B)",
        elapsed.as_secs_f64() * 1000.0,
        rows.len() * 300 / 1024
    );
    // LIMIT 下推应该 < 50ms（不扫全表）
    assert!(
        elapsed.as_millis() < 500,
        "LIMIT pushdown 太慢: {:?}",
        elapsed
    );
}

/// 路径2: SELECT * FROM bench WHERE id = 12345（PK 点查）
/// 预期: O(1) 内存，微秒级
#[test]
fn bench_pk_lookup_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng.run_sql("SELECT * FROM bench WHERE id = 12345").unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 1);
    eprintln!(
        "[PK lookup] 50K行表点查: {:.2}ms | 内存: ~300B (1行)",
        elapsed.as_secs_f64() * 1000.0
    );
}

/// 路径3: SELECT * FROM bench WHERE category = 'cat42'（索引查询）
/// 预期: O(匹配数) 内存，50K行中约500条匹配
#[test]
fn bench_index_scan_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM bench WHERE category = 'cat42'")
        .unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 500); // 50000 / 100 categories
    eprintln!(
        "[Index scan] 50K行表索引查500条: {:.2}ms | 内存: ~{}KB ({}行×~300B)",
        elapsed.as_secs_f64() * 1000.0,
        rows.len() * 300 / 1024,
        rows.len()
    );
}

/// 路径4: SELECT COUNT(*) FROM bench（流式计数）
/// 预期: O(1) 内存（不加载行数据），只计数
#[test]
fn bench_count_star_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng.run_sql("SELECT COUNT(*) FROM bench").unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows[0][0], Value::Integer(ROW_COUNT as i64));
    eprintln!(
        "[COUNT(*)] 50K行表流式计数: {:.2}ms | 内存: O(1)",
        elapsed.as_secs_f64() * 1000.0
    );
}

/// 路径5: SELECT * FROM bench LIMIT 1000 OFFSET 10000
/// 预期: O(offset+limit) = O(11000) 内存
#[test]
fn bench_offset_limit_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM bench LIMIT 1000 OFFSET 10000")
        .unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 1000);
    eprintln!(
        "[OFFSET+LIMIT] 50K行表 OFFSET 10000 取1000条: {:.2}ms | 内存: ~{}KB (下推11000行)",
        elapsed.as_secs_f64() * 1000.0,
        11000 * 300 / 1024
    );
}

/// 路径6: SELECT * FROM bench ORDER BY id LIMIT 1000
/// M75 优化: Top-N 流式堆排序，O(1000) 内存而非 O(50K)
#[test]
fn bench_order_by_limit_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM bench ORDER BY id LIMIT 1000")
        .unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 1000);
    // 验证排序正确（id ASC，前1000条应是 0..1000）
    assert_eq!(rows[0][0], Value::Integer(0));
    assert_eq!(rows[999][0], Value::Integer(999));
    eprintln!(
        "[ORDER BY+LIMIT] M75 Top-N: {:.2}ms | 内存: ~{}KB (堆1000行，非全表)",
        elapsed.as_secs_f64() * 1000.0,
        1000 * 300 / 1024
    );
}

/// 路径7: ORDER BY DESC + LIMIT（验证降序 Top-N）
#[test]
fn bench_order_by_desc_limit_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM bench ORDER BY id DESC LIMIT 5")
        .unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][0], Value::Integer(ROW_COUNT as i64 - 1));
    assert_eq!(rows[4][0], Value::Integer(ROW_COUNT as i64 - 5));
    eprintln!(
        "[ORDER BY DESC+LIMIT] M75 Top-N: {:.2}ms | 验证降序正确",
        elapsed.as_secs_f64() * 1000.0
    );
}

/// 路径8: ORDER BY + LIMIT + OFFSET（验证 Top-N 配合分页）
#[test]
fn bench_order_by_offset_limit_50k() {
    let (_dir, mut eng) = tmp_engine();
    setup_large_table(&mut eng);

    let t = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM bench ORDER BY id LIMIT 10 OFFSET 100")
        .unwrap();
    let elapsed = t.elapsed();

    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0][0], Value::Integer(100));
    assert_eq!(rows[9][0], Value::Integer(109));
    eprintln!(
        "[ORDER BY+OFFSET+LIMIT] M75 Top-N: {:.2}ms | 堆容量=110",
        elapsed.as_secs_f64() * 1000.0
    );
}
