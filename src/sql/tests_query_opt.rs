/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M76/M77 查询优化测试：WHERE AND 索引加速 + WHERE+ORDER BY+LIMIT TopN。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

// ── M76: WHERE AND 索引加速 ──

#[test]
fn where_and_index_accel_pk() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m76 (id INT, name TEXT, age INT)")
        .unwrap();
    for i in 0..20 {
        eng.run_sql(&format!(
            "INSERT INTO m76 (id, name, age) VALUES ({}, 'user{}', {})",
            i,
            i,
            20 + i
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM m76 WHERE id = 5 AND age = 25")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(5));
    let rows = eng
        .run_sql("SELECT * FROM m76 WHERE id = 5 AND age = 99")
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn where_and_index_accel_secondary() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m76b (id INT, cat TEXT, status TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON m76b(cat)").unwrap();
    for i in 0..100 {
        let cat = format!("cat{}", i % 5);
        let st = if i % 3 == 0 { "active" } else { "inactive" };
        eng.run_sql(&format!(
            "INSERT INTO m76b (id, cat, status) VALUES ({}, '{}', '{}')",
            i, cat, st
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM m76b WHERE cat = 'cat2' AND status = 'active'")
        .unwrap();
    for row in &rows {
        assert_eq!(row[1], Value::Text("cat2".into()));
        assert_eq!(row[2], Value::Text("active".into()));
    }
    assert!(!rows.is_empty());
}

#[test]
fn where_and_three_conditions() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m76c (id INT, a TEXT, b TEXT, c INT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_a ON m76c(a)").unwrap();
    for i in 0..50 {
        eng.run_sql(&format!(
            "INSERT INTO m76c (id, a, b, c) VALUES ({}, 'x{}', 'y{}', {})",
            i,
            i % 5,
            i % 3,
            i
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM m76c WHERE a = 'x0' AND b = 'y0' AND c = 0")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(0));
}

// ── M77: WHERE + ORDER BY + LIMIT → TopN ──

#[test]
fn where_order_by_limit_topn() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m77 (id INT, cat TEXT, score INT)")
        .unwrap();
    for i in 0..200 {
        let cat = format!("cat{}", i % 10);
        eng.run_sql(&format!(
            "INSERT INTO m77 (id, cat, score) VALUES ({}, '{}', {})",
            i,
            cat,
            i * 3
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM m77 WHERE cat = 'cat5' ORDER BY score LIMIT 3")
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][2], Value::Integer(15));
    assert_eq!(rows[1][2], Value::Integer(45));
    assert_eq!(rows[2][2], Value::Integer(75));
}

#[test]
fn where_order_by_desc_limit_topn() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m77b (id INT, v TEXT, n INT)")
        .unwrap();
    for i in 0..100 {
        let v = if i % 2 == 0 { "even" } else { "odd" };
        eng.run_sql(&format!(
            "INSERT INTO m77b (id, v, n) VALUES ({}, '{}', {})",
            i, v, i
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM m77b WHERE v = 'even' ORDER BY n DESC LIMIT 5")
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][2], Value::Integer(98));
    assert_eq!(rows[4][2], Value::Integer(90));
}

#[test]
fn where_and_order_by_limit_topn() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m77c (id INT, a TEXT, b TEXT, n INT)")
        .unwrap();
    for i in 0..60 {
        eng.run_sql(&format!(
            "INSERT INTO m77c (id, a, b, n) VALUES ({}, 'x{}', 'y{}', {})",
            i,
            i % 3,
            i % 2,
            i
        ))
        .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM m77c WHERE a = 'x0' AND b = 'y0' ORDER BY n DESC LIMIT 2")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][3], Value::Integer(54));
    assert_eq!(rows[1][3], Value::Integer(48));
}

#[test]
fn explain_and_index_accel() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m76e (id INT, cat TEXT, v TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON m76e(cat)").unwrap();
    let plan = eng
        .run_sql("EXPLAIN SELECT * FROM m76e WHERE cat = 'a' AND v = 'b'")
        .unwrap();
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(
        text.iter().any(|s| s.contains("AND index acceleration")),
        "应显示 AND index acceleration: {:?}",
        text
    );
}

// ── M78: DELETE/UPDATE 索引加速 ──

#[test]
fn delete_by_index_single() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m78d (id INT, cat TEXT, v INT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON m78d(cat)").unwrap();
    for i in 0..50 {
        eng.run_sql(&format!(
            "INSERT INTO m78d (id, cat, v) VALUES ({}, 'c{}', {})",
            i,
            i % 5,
            i
        ))
        .unwrap();
    }
    // DELETE WHERE cat = 'c2' → 索引扫描删除，不走全表
    eng.run_sql("DELETE FROM m78d WHERE cat = 'c2'").unwrap();
    let rows = eng.run_sql("SELECT * FROM m78d WHERE cat = 'c2'").unwrap();
    assert!(rows.is_empty());
    // 其他类别不受影响
    let rest = eng.run_sql("SELECT * FROM m78d").unwrap();
    assert_eq!(rest.len(), 40);
}

#[test]
fn delete_and_pk_filter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m78d2 (id INT, name TEXT, age INT)")
        .unwrap();
    for i in 0..10 {
        eng.run_sql(&format!(
            "INSERT INTO m78d2 (id, name, age) VALUES ({}, 'u{}', {})",
            i,
            i,
            20 + i
        ))
        .unwrap();
    }
    // AND: PK + 其他列 → PK lookup + filter
    eng.run_sql("DELETE FROM m78d2 WHERE id = 3 AND age = 23")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78d2 WHERE id = 3").unwrap();
    assert!(rows.is_empty());
    // 不匹配的不删
    eng.run_sql("DELETE FROM m78d2 WHERE id = 5 AND age = 99")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78d2 WHERE id = 5").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn delete_and_index_filter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m78d3 (id INT, cat TEXT, status TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON m78d3(cat)").unwrap();
    for i in 0..30 {
        let st = if i % 3 == 0 { "active" } else { "off" };
        eng.run_sql(&format!(
            "INSERT INTO m78d3 (id, cat, status) VALUES ({}, 'c{}', '{}')",
            i,
            i % 3,
            st
        ))
        .unwrap();
    }
    // AND: 索引列 + 非索引列 → index scan + filter
    eng.run_sql("DELETE FROM m78d3 WHERE cat = 'c0' AND status = 'active'")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78d3 WHERE cat = 'c0'").unwrap();
    // c0 原 10 行全是 active (i%3==0 → status=active)，全删
    assert!(rows.is_empty());
}

#[test]
fn update_by_index_single() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m78u (id INT, cat TEXT, v INT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON m78u(cat)").unwrap();
    for i in 0..50 {
        eng.run_sql(&format!(
            "INSERT INTO m78u (id, cat, v) VALUES ({}, 'c{}', {})",
            i,
            i % 5,
            i
        ))
        .unwrap();
    }
    // UPDATE WHERE cat = 'c1' → 索引扫描更新
    eng.run_sql("UPDATE m78u SET v = 999 WHERE cat = 'c1'")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78u WHERE cat = 'c1'").unwrap();
    assert_eq!(rows.len(), 10);
    for row in &rows {
        assert_eq!(row[2], Value::Integer(999));
    }
}

#[test]
fn update_and_pk_filter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m78u2 (id INT, name TEXT, score INT)")
        .unwrap();
    for i in 0..10 {
        eng.run_sql(&format!(
            "INSERT INTO m78u2 (id, name, score) VALUES ({}, 'u{}', {})",
            i,
            i,
            i * 10
        ))
        .unwrap();
    }
    // AND: PK + 其他列
    eng.run_sql("UPDATE m78u2 SET score = 777 WHERE id = 4 AND name = 'u4'")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78u2 WHERE id = 4").unwrap();
    assert_eq!(rows[0][2], Value::Integer(777));
    // 不匹配的不更新
    eng.run_sql("UPDATE m78u2 SET score = 888 WHERE id = 5 AND name = 'wrong'")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78u2 WHERE id = 5").unwrap();
    assert_eq!(rows[0][2], Value::Integer(50));
}

#[test]
fn update_and_index_filter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE m78u3 (id INT, cat TEXT, status TEXT, n INT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON m78u3(cat)").unwrap();
    for i in 0..30 {
        let st = if i % 2 == 0 { "on" } else { "off" };
        eng.run_sql(&format!(
            "INSERT INTO m78u3 (id, cat, status, n) VALUES ({}, 'c{}', '{}', {})",
            i,
            i % 3,
            st,
            i
        ))
        .unwrap();
    }
    // AND: 索引列 + 非索引列
    eng.run_sql("UPDATE m78u3 SET n = 0 WHERE cat = 'c0' AND status = 'on'")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM m78u3 WHERE cat = 'c0'").unwrap();
    for row in &rows {
        if row[2] == Value::Text("on".into()) {
            assert_eq!(row[3], Value::Integer(0));
        }
    }
}

// ── 范围索引扫描测试 ──

fn setup_range_table(eng: &mut SqlEngine) {
    eng.run_sql("CREATE TABLE rng (id INT, score INT, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_score ON rng(score)").unwrap();
    for i in 0..20 {
        eng.run_sql(&format!(
            "INSERT INTO rng (id, score, name) VALUES ({}, {}, 'u{}')",
            i,
            i * 10,
            i
        ))
        .unwrap();
    }
}

#[test]
fn range_index_gt() {
    let (_dir, mut eng) = tmp_engine();
    setup_range_table(&mut eng);
    // score > 150 → score in {160, 170, 180, 190}
    let rows = eng.run_sql("SELECT id FROM rng WHERE score > 150").unwrap();
    assert_eq!(rows.len(), 4);
    for row in &rows {
        let id = match &row[0] {
            Value::Integer(n) => *n,
            _ => panic!(),
        };
        assert!(id >= 16);
    }
}

#[test]
fn range_index_ge() {
    let (_dir, mut eng) = tmp_engine();
    setup_range_table(&mut eng);
    // score >= 150 → score in {150, 160, 170, 180, 190}
    let rows = eng
        .run_sql("SELECT id FROM rng WHERE score >= 150")
        .unwrap();
    assert_eq!(rows.len(), 5);
}

#[test]
fn range_index_lt() {
    let (_dir, mut eng) = tmp_engine();
    setup_range_table(&mut eng);
    // score < 30 → score in {0, 10, 20}
    let rows = eng.run_sql("SELECT id FROM rng WHERE score < 30").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn range_index_le() {
    let (_dir, mut eng) = tmp_engine();
    setup_range_table(&mut eng);
    // score <= 30 → score in {0, 10, 20, 30}
    let rows = eng.run_sql("SELECT id FROM rng WHERE score <= 30").unwrap();
    assert_eq!(rows.len(), 4);
}

#[test]
fn range_index_between() {
    let (_dir, mut eng) = tmp_engine();
    setup_range_table(&mut eng);
    // score BETWEEN 50 AND 100 → score in {50, 60, 70, 80, 90, 100}
    let rows = eng
        .run_sql("SELECT id FROM rng WHERE score BETWEEN 50 AND 100")
        .unwrap();
    assert_eq!(rows.len(), 6);
}

#[test]
fn range_index_with_and_filter() {
    let (_dir, mut eng) = tmp_engine();
    setup_range_table(&mut eng);
    // score > 100 AND name = 'u15' → only id=15 (score=150)
    let rows = eng
        .run_sql("SELECT id FROM rng WHERE score > 100 AND name = 'u15'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(15));
}
