/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 性能优化测试（M66）：COUNT(*) 流式计数、LIMIT 下推、scan_prefix_limit。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

#[test]
fn count_star_uses_streaming_count() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE big (id INT, val TEXT)").unwrap();
    for i in 0..100 {
        eng.run_sql(&format!(
            "INSERT INTO big (id, val) VALUES ({}, 'row{}')",
            i, i
        ))
        .unwrap();
    }
    let rows = eng.run_sql("SELECT COUNT(*) FROM big").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(100));
}

#[test]
fn select_limit_returns_correct_count() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, name TEXT)")
        .unwrap();
    for i in 0..50 {
        eng.run_sql(&format!(
            "INSERT INTO items (id, name) VALUES ({}, 'item{}')",
            i, i
        ))
        .unwrap();
    }
    let rows = eng.run_sql("SELECT * FROM items LIMIT 5").unwrap();
    assert_eq!(rows.len(), 5);
}

#[test]
fn select_limit_1_on_large_table() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE data (id INT, x TEXT)").unwrap();
    for i in 0..200 {
        eng.run_sql(&format!(
            "INSERT INTO data (id, x) VALUES ({}, 'v{}')",
            i, i
        ))
        .unwrap();
    }
    // LIMIT 1 应只返回 1 行（LIMIT 下推到扫描层）
    let rows = eng.run_sql("SELECT * FROM data LIMIT 1").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn select_with_order_by_and_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE sorted (id INT, score INT)")
        .unwrap();
    for i in 0..20 {
        eng.run_sql(&format!(
            "INSERT INTO sorted (id, score) VALUES ({}, {})",
            i,
            20 - i
        ))
        .unwrap();
    }
    // ORDER BY + LIMIT 不下推（需要全量排序后截断）
    let rows = eng
        .run_sql("SELECT * FROM sorted ORDER BY score LIMIT 3")
        .unwrap();
    assert_eq!(rows.len(), 3);
    // 最小 score 应是 1
    assert_eq!(rows[0][1], Value::Integer(1));
}

#[test]
fn scan_prefix_limit_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ks = store.open_keyspace("test_scan").unwrap();
    for i in 0..100u32 {
        ks.set(
            format!("k:{:04}", i).as_bytes(),
            format!("v{}", i).as_bytes(),
        )
        .unwrap();
    }
    // scan first 5
    let pairs = ks.scan_prefix_limit(b"k:", 0, 5).unwrap();
    assert_eq!(pairs.len(), 5);

    // scan with offset
    let pairs2 = ks.scan_prefix_limit(b"k:", 95, 100).unwrap();
    assert_eq!(pairs2.len(), 5); // only 5 remaining after offset 95

    // count_prefix should count all
    let count = ks.count_prefix(b"k:").unwrap();
    assert_eq!(count, 100);
}

#[test]
fn count_prefix_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ks = store.open_keyspace("empty").unwrap();
    assert_eq!(ks.count_prefix(b"").unwrap(), 0);
}

#[test]
fn for_each_key_prefix_early_stop() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ks = store.open_keyspace("iter_test").unwrap();
    for i in 0..50u32 {
        ks.set(format!("x:{:04}", i).as_bytes(), b"val").unwrap();
    }
    let mut collected = Vec::new();
    ks.for_each_key_prefix(b"x:", |key| {
        collected.push(key.to_vec());
        collected.len() < 3 // stop after 3
    })
    .unwrap();
    assert_eq!(collected.len(), 3);
}

// ── M73: EXPLAIN 查询计划 ──

#[test]
fn explain_pk_lookup() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO users (id, name) VALUES (1, 'a')")
        .unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM users WHERE id = 1")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(text.iter().any(|s| s.contains("PK point lookup")));
}

#[test]
fn explain_index_scan() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, cat TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON items(cat)").unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM items WHERE cat = 'a'")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(text.iter().any(|s| s.contains("Index scan")));
}

#[test]
fn explain_full_scan_no_index() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE logs (id INT, level TEXT)")
        .unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM logs WHERE level = 'error'")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(text.iter().any(|s| s.contains("Full table scan + filter")));
    assert!(text.iter().any(|s| s.contains("Warning")));
}

#[test]
fn explain_count_star() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    let plan = eng.run_sql("EXPLAIN SELECT COUNT(*) FROM t").unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(text.iter().any(|s| s.contains("COUNT(*) fast path")));
}

#[test]
fn explain_limit_pushdown() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE big (id INT, v TEXT)").unwrap();
    let plan = eng.run_sql("EXPLAIN SELECT * FROM big LIMIT 10").unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(text.iter().any(|s| s.contains("LIMIT pushdown")));
    assert!(text.iter().any(|s| s.contains("Limit: 10")));
}

// ── M74: OFFSET 分页 ──

#[test]
fn select_offset_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE p (id INT, v TEXT)").unwrap();
    for i in 0..10 {
        eng.run_sql(&format!("INSERT INTO p (id, v) VALUES ({}, 'r{}')", i, i))
            .unwrap();
    }
    // LIMIT 3 OFFSET 0 → 前3条
    let r1 = eng.run_sql("SELECT * FROM p LIMIT 3 OFFSET 0").unwrap();
    assert_eq!(r1.len(), 3);
    // LIMIT 3 OFFSET 3 → 第4-6条
    let r2 = eng.run_sql("SELECT * FROM p LIMIT 3 OFFSET 3").unwrap();
    assert_eq!(r2.len(), 3);
    assert_ne!(r1[0], r2[0]);
    // LIMIT 3 OFFSET 9 → 只剩1条
    let r3 = eng.run_sql("SELECT * FROM p LIMIT 3 OFFSET 9").unwrap();
    assert_eq!(r3.len(), 1);
    // OFFSET beyond range
    let r4 = eng.run_sql("SELECT * FROM p LIMIT 10 OFFSET 100").unwrap();
    assert!(r4.is_empty());
}

#[test]
fn select_offset_with_order_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE s (id INT, name TEXT)").unwrap();
    for i in 0..5 {
        eng.run_sql(&format!(
            "INSERT INTO s (id, name) VALUES ({}, 'n{}')",
            i, i
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM s ORDER BY id LIMIT 2 OFFSET 2")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
}

// ── M76: EXPLAIN 增强 — Top-N 堆排序 + 提前终止 ──

#[test]
fn explain_topn_heap() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t76 (id INT, v TEXT)").unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM t76 ORDER BY id LIMIT 100")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(
        text.iter().any(|s| s.contains("Top-N heap")),
        "应显示 Top-N heap: {:?}",
        text
    );
    assert!(text.iter().any(|s| s.contains("capacity=100")));
}

#[test]
fn explain_topn_heap_with_offset() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t76b (id INT)").unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM t76b ORDER BY id LIMIT 10 OFFSET 50")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    // capacity = limit + offset = 60
    assert!(
        text.iter().any(|s| s.contains("capacity=60")),
        "应显示 capacity=60: {:?}",
        text
    );
}

#[test]
fn explain_early_termination() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t76c (id INT, level TEXT)")
        .unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM t76c WHERE level = 'error' LIMIT 50")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(
        text.iter().any(|s| s.contains("Early termination")),
        "应显示提前终止: {:?}",
        text
    );
    assert!(text.iter().any(|s| s.contains("stop after 50")));
}

#[test]
fn explain_full_sort_no_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t76d (id INT)").unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM t76d ORDER BY id")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(
        text.iter().any(|s| s.contains("full sort")),
        "无 LIMIT 应显示 full sort: {:?}",
        text
    );
}

#[test]
fn explain_offset() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE o (id INT)").unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM o LIMIT 5 OFFSET 10")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(text.iter().any(|s| s.contains("Offset: 10")));
    assert!(text.iter().any(|s| s.contains("Limit: 5")));
}
