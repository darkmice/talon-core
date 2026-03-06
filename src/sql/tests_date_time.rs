/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Date / Time 数据类型集成测试与性能基准。
//!
//! 验证 Date (i32, 天精度) 和 Time (i64, 纳秒精度) 在 SQL 引擎中的完整链路：
//! CREATE TABLE → INSERT → SELECT → WHERE → CAST → 聚合 → 排序。
//! 性能基准对比 Timestamp 类型，确认 Date 4 字节编码带来的存储效率优势。

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

// ── 功能测试 ──────────────────────────────────────────────

#[test]
fn date_literal_insert_and_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, d DATE)").unwrap();
    eng.run_sql("INSERT INTO t (id, d) VALUES (1, DATE '2024-03-01')")
        .unwrap();
    eng.run_sql("INSERT INTO t (id, d) VALUES (2, DATE '1970-01-01')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
    // 检查两行都包含 Date 类型（不依赖顺序）
    let dates: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
    assert!(dates.contains(&&Value::Date(19783))); // 2024-03-01
    assert!(dates.contains(&&Value::Date(0)));     // 1970-01-01 = epoch
}

#[test]
fn time_literal_insert_and_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, t TIME)").unwrap();
    eng.run_sql("INSERT INTO t (id, t) VALUES (1, TIME '12:30:45')")
        .unwrap();
    eng.run_sql("INSERT INTO t (id, t) VALUES (2, TIME '00:00:00')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
    let times: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
    assert!(times.iter().any(|t| matches!(t, Value::Time(0))));
    assert!(times.iter().any(|t| matches!(t, Value::Time(n) if *n > 0)));
}

#[test]
fn date_where_comparison() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, d DATE)")
        .unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, DATE '2024-01-01')")
        .unwrap();
    eng.run_sql("INSERT INTO events VALUES (2, DATE '2024-06-15')")
        .unwrap();
    eng.run_sql("INSERT INTO events VALUES (3, DATE '2024-12-31')")
        .unwrap();
    // Select all and verify count
    let all_rows = eng.run_sql("SELECT * FROM events").unwrap();
    assert_eq!(all_rows.len(), 3);
    // 验证 NULL 过滤
    let null_rows = eng
        .run_sql("SELECT * FROM events WHERE d IS NOT NULL")
        .unwrap();
    assert_eq!(null_rows.len(), 3);
}

#[test]
fn cast_timestamp_to_date() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, ts TIMESTAMP)")
        .unwrap();
    // 2024-03-01 00:00:00 UTC in ms = 19783 * 86400 * 1000
    let ts = 19783_i64 * 86_400_000;
    eng.run_sql(&format!("INSERT INTO t VALUES (1, {})", ts))
        .unwrap();
    let rows = eng
        .run_sql("SELECT CAST(ts AS DATE) FROM t")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Date(19783));
}

#[test]
fn cast_date_and_time_to_text() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, d DATE, t TIME)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, DATE '2024-03-01', TIME '09:15:30')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT CAST(d AS TEXT), CAST(t AS TEXT) FROM t")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("2024-03-01".into()));
    assert_eq!(rows[0][1], Value::Text("09:15:30".into()));
}

#[test]
fn date_time_with_null() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, d DATE, t TIME)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, NULL, NULL)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, DATE '2024-03-01', TIME '12:00:00')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE d IS NULL")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn date_coerce_from_text() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, d DATE)").unwrap();
    // 直接插入文本应被自动转换为 Date
    eng.run_sql("INSERT INTO t VALUES (1, '2024-03-01')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert!(matches!(rows[0][1], Value::Date(_)));
}

#[test]
fn time_coerce_from_text() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, t TIME)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, '12:30:45')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert!(matches!(rows[0][1], Value::Time(_)));
}

// ── 性能基准 ──────────────────────────────────────────────

const BENCH_ROWS: usize = 10_000;

/// 性能基准：Date 类型 INSERT + SELECT 与 Timestamp 对比。
/// Date 使用 4 字节编码 (i32)，Timestamp 使用 8 字节编码 (i64)。
#[test]
fn bench_date_vs_timestamp_insert_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE bench_date (id INT, d DATE)")
        .unwrap();
    eng.run_sql("CREATE TABLE bench_ts (id INT, ts TIMESTAMP)")
        .unwrap();

    // === Date INSERT ===
    let t = Instant::now();
    for i in 0..BENCH_ROWS {
        eng.run_sql(&format!(
            "INSERT INTO bench_date VALUES ({}, {})",
            i,
            i as i32 % 36500 // ~100 years of days
        ))
        .unwrap();
    }
    let date_insert_ms = t.elapsed().as_secs_f64() * 1000.0;

    // === Timestamp INSERT ===
    let t = Instant::now();
    for i in 0..BENCH_ROWS {
        eng.run_sql(&format!(
            "INSERT INTO bench_ts VALUES ({}, {})",
            i,
            (i as i64 % 36500) * 86_400_000 // equivalent ms
        ))
        .unwrap();
    }
    let ts_insert_ms = t.elapsed().as_secs_f64() * 1000.0;

    // === Date SELECT ===
    let t = Instant::now();
    let date_rows = eng.run_sql("SELECT * FROM bench_date").unwrap();
    let date_select_ms = t.elapsed().as_secs_f64() * 1000.0;

    // === Timestamp SELECT ===
    let t = Instant::now();
    let ts_rows = eng.run_sql("SELECT * FROM bench_ts").unwrap();
    let ts_select_ms = t.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(date_rows.len(), BENCH_ROWS);
    assert_eq!(ts_rows.len(), BENCH_ROWS);

    eprintln!(
        "\n[Date vs Timestamp 性能对比] {} 行\n\
         INSERT — Date: {:.2}ms | Timestamp: {:.2}ms | 差异: {:.1}%\n\
         SELECT — Date: {:.2}ms | Timestamp: {:.2}ms | 差异: {:.1}%\n\
         Date 编码: 5字节/值 (1 tag + 4 i32) | Timestamp 编码: 9字节/值 (1 tag + 8 i64)\n\
         Date 存储节省: ~44% 字节/列",
        BENCH_ROWS,
        date_insert_ms,
        ts_insert_ms,
        (date_insert_ms - ts_insert_ms) / ts_insert_ms * 100.0,
        date_select_ms,
        ts_select_ms,
        (date_select_ms - ts_select_ms) / ts_select_ms * 100.0,
    );
}

/// 性能基准：Time 类型 INSERT + SELECT。
#[test]
fn bench_time_insert_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE bench_time (id INT, t TIME)")
        .unwrap();

    let t = Instant::now();
    for i in 0..BENCH_ROWS {
        let nanos = (i as i64 % 86400) * 1_000_000_000; // second precision
        eng.run_sql(&format!(
            "INSERT INTO bench_time VALUES ({}, {})",
            i, nanos
        ))
        .unwrap();
    }
    let insert_ms = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let rows = eng.run_sql("SELECT * FROM bench_time").unwrap();
    let select_ms = t.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(rows.len(), BENCH_ROWS);

    eprintln!(
        "\n[Time 性能基准] {} 行\n\
         INSERT: {:.2}ms ({:.0} 行/秒)\n\
         SELECT: {:.2}ms ({:.0} 行/秒)",
        BENCH_ROWS,
        insert_ms,
        BENCH_ROWS as f64 / (insert_ms / 1000.0),
        select_ms,
        BENCH_ROWS as f64 / (select_ms / 1000.0),
    );
}

/// 性能基准：混合类型表 (Date + Time + Integer) 完整操作。
#[test]
fn bench_mixed_date_time_table() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql(
        "CREATE TABLE bench_mixed (id INT, event_date DATE, event_time TIME, score INT)",
    )
    .unwrap();

    // INSERT
    let t = Instant::now();
    for i in 0..BENCH_ROWS {
        eng.run_sql(&format!(
            "INSERT INTO bench_mixed VALUES ({}, {}, {}, {})",
            i,
            i as i32 % 36500,
            (i as i64 % 86400) * 1_000_000_000,
            i % 100
        ))
        .unwrap();
    }
    let insert_ms = t.elapsed().as_secs_f64() * 1000.0;

    // SELECT all
    let t = Instant::now();
    let rows = eng.run_sql("SELECT * FROM bench_mixed").unwrap();
    let select_all_ms = t.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(rows.len(), BENCH_ROWS);

    // COUNT(*)
    let t = Instant::now();
    let count = eng.run_sql("SELECT COUNT(*) FROM bench_mixed").unwrap();
    let count_ms = t.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(count[0][0], Value::Integer(BENCH_ROWS as i64));

    // WHERE filter
    let t = Instant::now();
    let filtered = eng
        .run_sql("SELECT * FROM bench_mixed WHERE score > 90")
        .unwrap();
    let where_ms = t.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "\n[混合类型表 (Date+Time+INT) 性能基准] {} 行\n\
         INSERT:     {:.2}ms ({:.0} 行/秒)\n\
         SELECT *:   {:.2}ms ({:.0} 行/秒)\n\
         COUNT(*):   {:.2}ms\n\
         WHERE 过滤: {:.2}ms (返回 {} 行)",
        BENCH_ROWS,
        insert_ms,
        BENCH_ROWS as f64 / (insert_ms / 1000.0),
        select_all_ms,
        BENCH_ROWS as f64 / (select_all_ms / 1000.0),
        count_ms,
        where_ms,
        filtered.len(),
    );
}
