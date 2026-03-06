/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M37 测试：ALTER TABLE DROP COLUMN / TRUNCATE TABLE / COUNT(*) 快速路径。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

// ── ALTER TABLE DROP COLUMN ──────────────────────────

#[test]
fn alter_table_drop_column_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT, age INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob', 25)").unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN age").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id ASC").unwrap();
    assert_eq!(rows.len(), 2);
    // 每行只剩 2 列：id, name
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[1][1], Value::Text("Bob".into()));
}

#[test]
fn alter_table_drop_column_pk_rejected() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    let err = eng.run_sql("ALTER TABLE t DROP COLUMN id").unwrap_err();
    assert!(err.to_string().contains("主键"));
}

#[test]
fn alter_table_drop_column_not_found() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    let err = eng
        .run_sql("ALTER TABLE t DROP COLUMN nonexistent")
        .unwrap_err();
    assert!(err.to_string().contains("不存在"));
}

#[test]
fn alter_table_drop_column_describe() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT, age INT)")
        .unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN name").unwrap();
    let rows = eng.run_sql("DESCRIBE t").unwrap();
    // 应只剩 id 和 age
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("id".into()));
    assert_eq!(rows[1][0], Value::Text("age".into()));
}

#[test]
fn alter_table_drop_column_insert_after() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT)")
        .unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    // DROP 后只剩 id, b → INSERT 应只需 2 个值
    eng.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][1], Value::Integer(100));
}

// ── TRUNCATE TABLE ───────────────────────────────────

#[test]
fn truncate_table_clears_data() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'c')").unwrap();
    eng.run_sql("TRUNCATE TABLE t").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert!(rows.is_empty());
}

#[test]
fn truncate_table_not_found() {
    let (_dir, mut eng) = tmp_engine();
    let err = eng.run_sql("TRUNCATE TABLE nonexistent").unwrap_err();
    assert!(err.to_string().contains("表不存在"));
}

#[test]
fn truncate_preserves_schema() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    eng.run_sql("TRUNCATE TABLE t").unwrap();
    // 表结构仍在，可以继续插入
    eng.run_sql("INSERT INTO t VALUES (2, 'new')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[0][1], Value::Text("new".into()));
}

#[test]
fn truncate_without_table_keyword() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    // TRUNCATE t（不带 TABLE 关键字）
    eng.run_sql("TRUNCATE t").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert!(rows.is_empty());
}

// ── COUNT(*) 快速路径 ────────────────────────────────

#[test]
fn count_star_fast_path() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    let rows = eng.run_sql("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(0)]]);
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'c')").unwrap();
    let rows = eng.run_sql("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(3)]]);
}

#[test]
fn count_star_after_delete() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2)").unwrap();
    eng.run_sql("DELETE FROM t WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
}

// ── DROP COLUMN 交互正确性（M38）─────────────────────

#[test]
fn drop_column_then_insert_with_column_names() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT, c TEXT)")
        .unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN b").unwrap();
    // DROP b 后可见列: id, a, c → 指定列名 INSERT
    eng.run_sql("INSERT INTO t (id, a, c) VALUES (1, 'hello', 'world')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("hello".into()));
    assert_eq!(rows[0][2], Value::Text("world".into()));
}

#[test]
fn drop_column_then_update() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old', 99)").unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    // 可见列: id, b → UPDATE b
    eng.run_sql("UPDATE t SET b = 200 WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][1], Value::Integer(200));
}

#[test]
fn drop_column_then_where_on_remaining() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    // WHERE 使用剩余列 b
    let rows = eng.run_sql("SELECT * FROM t WHERE b = 20").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn drop_column_then_order_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 30)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'y', 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'z', 20)").unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY b ASC").unwrap();
    assert_eq!(rows[0][1], Value::Integer(10));
    assert_eq!(rows[1][1], Value::Integer(20));
    assert_eq!(rows[2][1], Value::Integer(30));
}

#[test]
fn drop_column_then_delete() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    eng.run_sql("DELETE FROM t WHERE b = 10").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn drop_column_with_secondary_index() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT, age INT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_name ON t(name)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob', 25)").unwrap();
    // DROP 有索引的列 → 索引应被清理
    eng.run_sql("ALTER TABLE t DROP COLUMN name").unwrap();
    // 查询仍可用（走全表扫描）
    let rows = eng.run_sql("SELECT * FROM t WHERE age = 25").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn drop_multiple_columns() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT, c TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10, 'y')")
        .unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    eng.run_sql("ALTER TABLE t DROP COLUMN b").unwrap();
    // 可见列: id, c
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("y".into()));
    // 新 INSERT 只需 2 列
    eng.run_sql("INSERT INTO t VALUES (2, 'z')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id ASC").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][1], Value::Text("z".into()));
}
