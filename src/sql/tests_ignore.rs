/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! INSERT OR IGNORE 测试（M90）。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

#[test]
fn insert_or_ignore_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    // 冲突行静默跳过，不报错
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (1, 'Bob')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Alice".into()), "原始值不变");
}

#[test]
fn insert_or_ignore_no_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, val TEXT)").unwrap();
    // 无冲突时正常插入
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (1, 'a')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn insert_or_ignore_multi_row_partial() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'existing')").unwrap();
    // 多行 INSERT：id=2 冲突跳过，id=1 和 id=3 正常插入
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Text("a".into()));
    assert_eq!(rows[1][1], Value::Text("existing".into()), "冲突行保持原值");
    assert_eq!(rows[2][1], Value::Text("c".into()));
}

#[test]
fn insert_or_ignore_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, val TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'v1')").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (1, 'conflict')")
        .unwrap();
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (2, 'new')")
        .unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("v1".into()), "冲突行不变");
    assert_eq!(rows[1][1], Value::Text("new".into()));
}

#[test]
fn insert_or_ignore_parse_roundtrip() {
    use super::parser::{parse, Stmt};
    let stmt = parse("INSERT OR IGNORE INTO t (id, name) VALUES (1, 'x')").unwrap();
    match stmt {
        Stmt::Insert {
            or_replace,
            or_ignore,
            table,
            ..
        } => {
            assert!(!or_replace);
            assert!(or_ignore);
            assert_eq!(table, "t");
        }
        _ => panic!("expected Insert"),
    }
}
