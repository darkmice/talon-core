/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 引擎高级测试：事务、LIKE/IN/BETWEEN、DISTINCT、ALTER TABLE。

use super::engine::SqlEngine;
use super::parser::{parse, Stmt, WhereExpr, WhereOp};
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

// ── 事务测试 ─────────────────────────────────────────

#[test]
fn tx_begin_commit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    assert!(eng.in_transaction());
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
    eng.run_sql("COMMIT").unwrap();
    assert!(!eng.in_transaction());
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn tx_rollback() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'before')").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'in_tx')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
    eng.run_sql("ROLLBACK").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("before".into()));
}

#[test]
fn tx_read_your_writes() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (42, 'hello')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 42").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("hello".into()));
    eng.run_sql("ROLLBACK").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 42").unwrap();
    assert!(rows.is_empty());
}

#[test]
fn tx_update_in_transaction() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("UPDATE t SET v = 'new' WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("new".into()));
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("new".into()));
}

#[test]
fn tx_delete_in_transaction() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("DELETE FROM t WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    eng.run_sql("ROLLBACK").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn tx_nested_begin_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("BEGIN").unwrap();
    assert!(eng.run_sql("BEGIN").is_err());
    eng.run_sql("ROLLBACK").unwrap();
}

#[test]
fn tx_commit_without_begin_error() {
    let (_dir, mut eng) = tmp_engine();
    assert!(eng.run_sql("COMMIT").is_err());
}

#[test]
fn tx_rollback_without_begin_error() {
    let (_dir, mut eng) = tmp_engine();
    assert!(eng.run_sql("ROLLBACK").is_err());
}

#[test]
fn tx_begin_transaction_syntax() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("BEGIN TRANSACTION").unwrap();
    assert!(eng.in_transaction());
    eng.run_sql("END").unwrap();
    assert!(!eng.in_transaction());
    eng.run_sql("START TRANSACTION").unwrap();
    assert!(eng.in_transaction());
    eng.run_sql("COMMIT").unwrap();
}

#[test]
fn tx_mixed_operations() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'c')").unwrap();
    eng.run_sql("UPDATE t SET v = 'aa' WHERE id = 1").unwrap();
    eng.run_sql("DELETE FROM t WHERE id = 2").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id ASC").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("aa".into()));
    assert_eq!(rows[1][0], Value::Integer(3));
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id ASC").unwrap();
    assert_eq!(rows.len(), 2);
}

// ── M8.1 新增语法测试 ────────────────────────────────

#[test]
fn engine_like() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Alicia')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name LIKE 'Ali%'")
        .unwrap();
    assert_eq!(rows.len(), 2);
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name LIKE '%ob'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name LIKE '_ob'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name NOT LIKE 'Ali%'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Bob".into()));
}

#[test]
fn engine_in() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'c')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id IN (1, 3)").unwrap();
    assert_eq!(rows.len(), 2);
    let rows = eng.run_sql("SELECT * FROM t WHERE id NOT IN (2)").unwrap();
    assert_eq!(rows.len(), 2);
    let rows = eng
        .run_sql("SELECT * FROM t WHERE v IN ('a', 'c')")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn engine_between() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, score INT)").unwrap();
    for i in 1..=5 {
        eng.run_sql(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    let rows = eng
        .run_sql("SELECT * FROM t WHERE score BETWEEN 20 AND 40")
        .unwrap();
    assert_eq!(rows.len(), 3);
    let rows = eng
        .run_sql("SELECT * FROM t WHERE score NOT BETWEEN 20 AND 40")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn engine_distinct() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, city TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Beijing')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Shanghai')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Beijing')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 'Shanghai')").unwrap();
    let rows = eng.run_sql("SELECT DISTINCT * FROM t").unwrap();
    assert_eq!(rows.len(), 4);
    eng.run_sql("CREATE TABLE t2 (id INT, tag TEXT)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 'a')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (3, 'b')").unwrap();
    let rows = eng.run_sql("SELECT DISTINCT * FROM t2").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn engine_between_and_other_conditions() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, score INT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10, 'Alice')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 20, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 30, 'Carol')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 40, 'Dave')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE score BETWEEN 15 AND 35 AND name LIKE '%o%'")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn parse_like_in_between() {
    let stmt = parse("SELECT * FROM t WHERE name LIKE '%test%'").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Leaf(ref c)),
            ..
        } => {
            assert_eq!(c.op, WhereOp::Like);
        }
        _ => panic!("expected Select with LIKE"),
    }
    let stmt = parse("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Leaf(ref c)),
            ..
        } => {
            assert_eq!(c.op, WhereOp::In);
            assert_eq!(c.in_values.len(), 3);
        }
        _ => panic!("expected Select with IN"),
    }
    let stmt = parse("SELECT * FROM t WHERE x BETWEEN 10 AND 20").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Leaf(ref c)),
            ..
        } => {
            assert_eq!(c.op, WhereOp::Between);
            assert!(c.value_high.is_some());
        }
        _ => panic!("expected Select with BETWEEN"),
    }
}

#[test]
fn parse_distinct() {
    let stmt = parse("SELECT DISTINCT * FROM t").unwrap();
    match stmt {
        Stmt::Select { distinct, .. } => assert!(distinct),
        _ => panic!("expected Select with DISTINCT"),
    }
    let stmt = parse("SELECT * FROM t").unwrap();
    match stmt {
        Stmt::Select { distinct, .. } => assert!(!distinct),
        _ => panic!("expected Select without DISTINCT"),
    }
}

// ── Schema 版本化 + ALTER TABLE O(1) 测试 ───────────

#[test]
fn alter_table_add_column_no_backfill() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("ALTER TABLE t ADD COLUMN age INT DEFAULT 18")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0][2], Value::Integer(18));
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol', 25)")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 3").unwrap();
    assert_eq!(rows[0][2], Value::Integer(25));
}

#[test]
fn alter_table_add_column_null_default() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x')").unwrap();
    eng.run_sql("ALTER TABLE t ADD COLUMN extra TEXT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0][2], Value::Null);
}

#[test]
fn alter_table_multiple_add_columns() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1)").unwrap();
    eng.run_sql("ALTER TABLE t ADD COLUMN a INT DEFAULT 10")
        .unwrap();
    eng.run_sql("ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'hi'")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0][1], Value::Integer(10));
    assert_eq!(rows[0][2], Value::Text("hi".into()));
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3);
}

#[test]
fn alter_table_where_on_new_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("ALTER TABLE t ADD COLUMN active BOOLEAN DEFAULT TRUE")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol', FALSE)")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE active = TRUE").unwrap();
    assert_eq!(rows.len(), 2);
    let rows = eng.run_sql("SELECT * FROM t WHERE active = FALSE").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn alter_table_update_new_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("ALTER TABLE t ADD COLUMN score INT DEFAULT 0")
        .unwrap();
    eng.run_sql("UPDATE t SET score = 100 WHERE id = 1")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][2], Value::Integer(100));
}

#[test]
fn now_function_in_insert() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, ts TIMESTAMP)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, NOW())").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
    // NOW() 应该返回一个正整数时间戳
    match &rows[0][1] {
        Value::Timestamp(ts) => assert!(*ts > 0),
        _ => panic!("expected Timestamp, got {:?}", rows[0][1]),
    }
}

#[test]
fn parse_multi_column_order_by() {
    let stmt = parse("SELECT * FROM t ORDER BY age ASC, name DESC").unwrap();
    match stmt {
        Stmt::Select {
            order_by: Some(ref ob),
            ..
        } => {
            assert_eq!(ob.len(), 2);
            assert_eq!(ob[0].0, "age");
            assert!(!ob[0].1); // ASC = false
            assert_eq!(ob[1].0, "name");
            assert!(ob[1].1); // DESC = true
        }
        _ => panic!("expected Select with multi-column ORDER BY"),
    }
}

#[test]
fn parse_not_null_and_default() {
    let stmt =
        parse("CREATE TABLE t (id INT NOT NULL, name TEXT DEFAULT 'anon', age INT)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns.len(), 3);
            assert!(!columns[0].nullable);
            assert!(columns[1].nullable);
            assert_eq!(columns[1].default_value, Some(Value::Text("anon".into())));
            assert!(columns[2].nullable);
            assert!(columns[2].default_value.is_none());
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn drop_index_removes_metadata_and_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = super::engine::SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE di1 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO di1 VALUES (1, 'a', 10), (2, 'b', 20)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_di1_name ON di1(name)")
        .unwrap();
    // 索引应存在：通过 WHERE name 查询走索引
    let rows = eng.run_sql("SELECT id FROM di1 WHERE name = 'a'").unwrap();
    assert_eq!(rows.len(), 1);
    // DROP INDEX
    eng.run_sql("DROP INDEX idx_di1_name").unwrap();
    // 索引元数据应被清理
    let meta = store.open_keyspace("sql_index_meta").unwrap();
    assert!(!meta.contains_key(b"idx:di1:name").unwrap());
    // 查询仍然可用（走全表扫描）
    let rows = eng.run_sql("SELECT id FROM di1 WHERE name = 'b'").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn drop_index_if_exists_no_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = super::engine::SqlEngine::new(&store).unwrap();
    // 不存在的索引 + IF EXISTS → 静默返回
    eng.run_sql("DROP INDEX IF EXISTS nonexistent_idx").unwrap();
}

#[test]
fn drop_index_not_found_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = super::engine::SqlEngine::new(&store).unwrap();
    // 不存在的索引 → 报错
    let err = eng.run_sql("DROP INDEX nonexistent_idx").unwrap_err();
    assert!(err.to_string().contains("索引不存在"));
}

#[test]
fn drop_index_then_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = super::engine::SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE di2 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO di2 VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_di2_val ON di2(val)").unwrap();
    eng.run_sql("DROP INDEX idx_di2_val").unwrap();
    // 重新创建同名索引
    eng.run_sql("CREATE INDEX idx_di2_val ON di2(val)").unwrap();
    // 索引应正常工作
    let rows = eng.run_sql("SELECT id FROM di2 WHERE val = 'y'").unwrap();
    assert_eq!(rows.len(), 1);
}
