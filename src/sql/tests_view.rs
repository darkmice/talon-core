/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M125：CREATE VIEW / DROP VIEW 测试。

use crate::sql::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn setup() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let engine = SqlEngine::new(&store).unwrap();
    (dir, engine)
}

#[test]
fn view_create_and_select() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    e.run_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    e.run_sql("CREATE VIEW adults AS SELECT * FROM users WHERE age >= 30")
        .unwrap();
    let rows = e.run_sql("SELECT * FROM adults").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn view_create_if_not_exists() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    // 重复创建应报错
    let err = e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap_err();
    assert!(err.to_string().contains("视图已存在"));
    // IF NOT EXISTS 静默跳过
    e.run_sql("CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t")
        .unwrap();
}

#[test]
fn view_drop() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    e.run_sql("DROP VIEW v1").unwrap();
    // 再次删除应报错
    let err = e.run_sql("DROP VIEW v1").unwrap_err();
    assert!(err.to_string().contains("视图不存在"));
    // IF EXISTS 静默跳过
    e.run_sql("DROP VIEW IF EXISTS v1").unwrap();
}

#[test]
fn view_write_protection_insert() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    let err = e.run_sql("INSERT INTO v1 VALUES (1, 'x')").unwrap_err();
    assert!(err.to_string().contains("只读"));
}

#[test]
fn view_write_protection_update() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    let err = e.run_sql("UPDATE v1 SET v = 'b' WHERE id = 1").unwrap_err();
    assert!(err.to_string().contains("只读"));
}

#[test]
fn view_write_protection_delete() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    let err = e.run_sql("DELETE FROM v1 WHERE id = 1").unwrap_err();
    assert!(err.to_string().contains("只读"));
}

#[test]
fn view_show_tables_includes_views() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE alpha (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE VIEW beta AS SELECT * FROM alpha")
        .unwrap();
    let rows = e.run_sql("SHOW TABLES").unwrap();
    assert_eq!(rows.len(), 2);
    // 按名称排序：alpha, beta
    assert_eq!(rows[0][0], Value::Text("alpha".into()));
    assert_eq!(rows[0][1], Value::Text("TABLE".into()));
    assert_eq!(rows[1][0], Value::Text("beta".into()));
    assert_eq!(rows[1][1], Value::Text("VIEW".into()));
}

#[test]
fn view_describe() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 'test', 3.14)").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT id, name, score FROM t")
        .unwrap();
    let rows = e.run_sql("DESCRIBE v1").unwrap();
    assert_eq!(rows.len(), 3);
    // 列名
    assert_eq!(rows[0][0], Value::Text("id".into()));
    assert_eq!(rows[1][0], Value::Text("name".into()));
    assert_eq!(rows[2][0], Value::Text("score".into()));
}

#[test]
fn view_with_where_clause() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, price FLOAT, category TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO items VALUES (1, 9.99, 'A')")
        .unwrap();
    e.run_sql("INSERT INTO items VALUES (2, 29.99, 'B')")
        .unwrap();
    e.run_sql("INSERT INTO items VALUES (3, 5.0, 'A')").unwrap();
    e.run_sql("CREATE VIEW expensive AS SELECT * FROM items WHERE price > 10.0")
        .unwrap();
    let rows = e.run_sql("SELECT * FROM expensive").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn view_select_with_additional_where() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    e.run_sql("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
    e.run_sql("INSERT INTO t VALUES (3, 'x', 30)").unwrap();
    e.run_sql("CREATE VIEW vx AS SELECT * FROM t WHERE a = 'x'")
        .unwrap();
    // 在视图上再加 WHERE 条件
    let rows = e.run_sql("SELECT * FROM vx WHERE b > 15").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(3));
}

#[test]
fn view_name_conflict_with_table() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    // 创建视图与已有表同名应报错
    let err = e.run_sql("CREATE VIEW t1 AS SELECT * FROM t1").unwrap_err();
    assert!(err.to_string().contains("同名表已存在"));
    // 创建表与已有视图同名应报错
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t1").unwrap();
    let err = e
        .run_sql("CREATE TABLE v1 (id INTEGER PRIMARY KEY)")
        .unwrap_err();
    assert!(err.to_string().contains("同名视图已存在"));
}

#[test]
fn view_nested() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    e.run_sql("INSERT INTO t VALUES (2, 20)").unwrap();
    e.run_sql("INSERT INTO t VALUES (3, 30)").unwrap();
    // 视图嵌套：v1 基于 t，v2 基于 v1
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val >= 20")
        .unwrap();
    e.run_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val <= 20")
        .unwrap();
    let rows = e.run_sql("SELECT * FROM v2").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn view_drop_then_recreate() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE v = 'a'")
        .unwrap();
    let rows = e.run_sql("SELECT * FROM v1").unwrap();
    assert_eq!(rows.len(), 1);
    e.run_sql("DROP VIEW v1").unwrap();
    // 重新创建不同定义的视图
    e.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    let rows = e.run_sql("SELECT * FROM v1").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn view_reflects_base_table_changes() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    let rows = e.run_sql("SELECT * FROM v1").unwrap();
    assert_eq!(rows.len(), 1);
    // 基表插入新数据后，视图应反映
    e.run_sql("INSERT INTO t VALUES (2, 200)").unwrap();
    let rows = e.run_sql("SELECT * FROM v1").unwrap();
    assert_eq!(rows.len(), 2);
    // 基表删除后，视图应反映
    e.run_sql("DELETE FROM t WHERE id = 1").unwrap();
    let rows = e.run_sql("SELECT * FROM v1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn view_with_order_by_limit() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO t VALUES (1, 50)").unwrap();
    e.run_sql("INSERT INTO t VALUES (2, 90)").unwrap();
    e.run_sql("INSERT INTO t VALUES (3, 70)").unwrap();
    e.run_sql("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    let rows = e
        .run_sql("SELECT * FROM v1 ORDER BY score DESC LIMIT 2")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Integer(90));
    assert_eq!(rows[1][1], Value::Integer(70));
}
