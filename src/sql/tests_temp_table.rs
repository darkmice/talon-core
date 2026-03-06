/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M126：CREATE TEMP TABLE 临时表测试。

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
fn temp_table_create_and_use() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO tmp VALUES (1, 'hello')").unwrap();
    let rows = e.run_sql("SELECT * FROM tmp").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("hello".into()));
}

#[test]
fn temp_table_temporary_keyword() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TEMPORARY TABLE tmp2 (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO tmp2 VALUES (1, 42)").unwrap();
    let rows = e.run_sql("SELECT * FROM tmp2").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(42));
}

#[test]
fn temp_table_if_not_exists() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TEMP TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    // 重复创建覆盖 schema（与普通表行为一致）
    e.run_sql("CREATE TEMP TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    // IF NOT EXISTS 静默跳过
    e.run_sql("CREATE TEMP TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
}

#[test]
fn temp_table_show_tables_type() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE perm (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    let rows = e.run_sql("SHOW TABLES").unwrap();
    assert_eq!(rows.len(), 2);
    // 按名称排序：perm, tmp
    let perm = rows
        .iter()
        .find(|r| r[0] == Value::Text("perm".into()))
        .unwrap();
    let tmp = rows
        .iter()
        .find(|r| r[0] == Value::Text("tmp".into()))
        .unwrap();
    assert_eq!(perm[1], Value::Text("TABLE".into()));
    assert_eq!(tmp[1], Value::Text("TEMP".into()));
}

#[test]
fn temp_table_drop_explicit() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("INSERT INTO tmp VALUES (1)").unwrap();
    e.run_sql("DROP TABLE tmp").unwrap();
    // 删除后不可访问
    let err = e.run_sql("SELECT * FROM tmp").unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("不存在"));
}

#[test]
fn temp_table_cleanup_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    {
        let mut e = SqlEngine::new(&store).unwrap();
        e.run_sql("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        e.run_sql("INSERT INTO tmp VALUES (1, 'ephemeral')")
            .unwrap();
        let rows = e.run_sql("SELECT * FROM tmp").unwrap();
        assert_eq!(rows.len(), 1);
        // engine drop 时自动清理
    }
    // 新引擎实例不应看到临时表
    let mut e2 = SqlEngine::new(&store).unwrap();
    let rows = e2.run_sql("SHOW TABLES").unwrap();
    assert!(rows.is_empty());
    // 临时表不可访问
    let err = e2.run_sql("SELECT * FROM tmp").unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("不存在"));
}

#[test]
fn temp_table_update_delete() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    e.run_sql("INSERT INTO tmp VALUES (1, 10)").unwrap();
    e.run_sql("INSERT INTO tmp VALUES (2, 20)").unwrap();
    e.run_sql("UPDATE tmp SET score = 99 WHERE id = 1").unwrap();
    let rows = e.run_sql("SELECT score FROM tmp WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Integer(99));
    e.run_sql("DELETE FROM tmp WHERE id = 2").unwrap();
    let rows = e.run_sql("SELECT * FROM tmp").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn temp_table_with_regular_table_no_conflict() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE data (id INTEGER PRIMARY KEY)")
        .unwrap();
    // 临时表与普通表不同名，可以共存
    e.run_sql("CREATE TEMP TABLE cache (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("INSERT INTO data VALUES (1)").unwrap();
    e.run_sql("INSERT INTO cache VALUES (2)").unwrap();
    let d = e.run_sql("SELECT * FROM data").unwrap();
    let c = e.run_sql("SELECT * FROM cache").unwrap();
    assert_eq!(d.len(), 1);
    assert_eq!(c.len(), 1);
}

#[test]
fn temp_table_describe() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TEMP TABLE tmp (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")
        .unwrap();
    let rows = e.run_sql("DESCRIBE tmp").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Text("id".into()));
    assert_eq!(rows[1][0], Value::Text("name".into()));
    assert_eq!(rows[2][0], Value::Text("score".into()));
}
