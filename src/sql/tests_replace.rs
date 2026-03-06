/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! INSERT OR REPLACE 测试。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

#[test]
fn engine_insert_or_replace_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    // OR REPLACE 覆盖已有行
    eng.run_sql("INSERT OR REPLACE INTO t VALUES (1, 'new')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("new".into()));
    // 新 PK 正常插入
    eng.run_sql("INSERT OR REPLACE INTO t VALUES (2, 'two')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn engine_insert_or_replace_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("CREATE INDEX idx_name ON t(name)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'alice')").unwrap();
    // 覆盖：旧索引应被清理，新索引应建立
    eng.run_sql("INSERT OR REPLACE INTO t VALUES (1, 'bob')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE name = 'alice'").unwrap();
    assert_eq!(rows.len(), 0, "旧索引应已清理");
    let rows = eng.run_sql("SELECT * FROM t WHERE name = 'bob'").unwrap();
    assert_eq!(rows.len(), 1, "新索引应已建立");
}

#[test]
fn engine_insert_or_replace_in_tx() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, val TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'v1')").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT OR REPLACE INTO t VALUES (1, 'v2')")
        .unwrap();
    eng.run_sql("INSERT OR REPLACE INTO t VALUES (2, 'v3')")
        .unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("v2".into()));
    assert_eq!(rows[1][1], Value::Text("v3".into()));
}
