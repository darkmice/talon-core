/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! JSONB path 查询 + INSERT JSONB 字面量测试。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

// ── JSONB path 查询（底层写入） ──────────────────────────

#[test]
fn jsonb_path_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE docs (id INT, metadata JSONB)")
        .unwrap();

    // 直接通过底层写入 JSONB 行
    let schema = {
        let meta_ks = store.open_keyspace("sql_meta").unwrap();
        let raw = meta_ks.get(b"docs").unwrap().unwrap();
        let mut s: crate::types::Schema = serde_json::from_slice(&raw).unwrap();
        s.ensure_defaults();
        s
    };
    let data_ks = store.open_keyspace("sql_docs").unwrap();
    let json1 = serde_json::json!({"source": "wiki", "lang": "en"});
    let json2 = serde_json::json!({"source": "blog", "lang": "zh"});
    let row1 = vec![Value::Integer(1), Value::Jsonb(json1)];
    let row2 = vec![Value::Integer(2), Value::Jsonb(json2)];
    data_ks
        .set(
            Value::Integer(1).to_bytes().unwrap().as_slice(),
            schema.encode_row(&row1).unwrap().as_slice(),
        )
        .unwrap();
    data_ks
        .set(
            Value::Integer(2).to_bytes().unwrap().as_slice(),
            schema.encode_row(&row2).unwrap().as_slice(),
        )
        .unwrap();
    eng.cache.remove("docs");

    let rows = eng
        .run_sql("SELECT * FROM docs WHERE metadata->>'source' = 'wiki'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    let rows = eng
        .run_sql("SELECT * FROM docs WHERE metadata->>'lang' = 'zh'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    let rows = eng
        .run_sql("SELECT * FROM docs WHERE metadata->>'missing' = 'x'")
        .unwrap();
    assert!(rows.is_empty());
}

// ── M16: INSERT JSONB 字面量 + 隐式类型转换 ──────────────

#[test]
fn insert_jsonb_literal_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE docs (id INT, metadata JSONB)")
        .unwrap();
    eng.run_sql(r#"INSERT INTO docs VALUES (1, '{"source": "wiki", "lang": "en"}')"#)
        .unwrap();
    eng.run_sql(r#"INSERT INTO docs VALUES (2, '{"source": "blog", "lang": "zh"}')"#)
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM docs WHERE metadata->>'source' = 'wiki'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    match &rows[0][1] {
        Value::Jsonb(json) => {
            assert_eq!(json["source"], "wiki");
            assert_eq!(json["lang"], "en");
        }
        other => panic!("expected Jsonb, got {:?}", other),
    }
}

#[test]
fn insert_jsonb_batch_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE docs (id INT, meta JSONB)")
        .unwrap();
    eng.run_sql(r#"INSERT INTO docs VALUES (1, '{"a": 1}'), (2, '{"a": 2}')"#)
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM docs WHERE meta->>'a' = '1'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn insert_jsonb_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE docs (id INT, meta JSONB)")
        .unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql(r#"INSERT INTO docs VALUES (1, '{"key": "val"}')"#)
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM docs WHERE meta->>'key' = 'val'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    eng.run_sql("COMMIT").unwrap();
}

#[test]
fn insert_invalid_json_to_jsonb_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE docs (id INT, meta JSONB)")
        .unwrap();
    let err = eng.run_sql("INSERT INTO docs VALUES (1, 'not json')");
    assert!(err.is_err(), "invalid JSON should fail type check");
}
