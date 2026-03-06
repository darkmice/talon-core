/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M95：SQL 快照隔离集成测试。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng, store)
}

// ── 快照隔离测试 ─────────────────────────────────────

#[test]
fn tx_snapshot_isolation_point_query() {
    let (_dir, mut eng, store) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'before')").unwrap();

    // 开启事务（获取快照）
    eng.run_sql("BEGIN").unwrap();

    // 事务外：模拟另一个写者直接写入 storage
    let ks = store.open_keyspace("sql_t").unwrap();
    let schema = {
        let raw = store
            .open_keyspace("sql_meta")
            .unwrap()
            .get(b"t")
            .unwrap()
            .unwrap();
        let mut s: crate::types::Schema = serde_json::from_slice(&raw).unwrap();
        s.ensure_defaults();
        s
    };
    let row = vec![Value::Integer(2), Value::Text("external".into())];
    let pk = row[0].to_bytes().unwrap();
    let raw = schema.encode_row(&row).unwrap();
    ks.set(&pk, &raw).unwrap();

    // 事务内 SELECT：不应看到外部写入
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(
        rows.len(),
        1,
        "快照隔离：事务内不应看到事务开始后的外部写入"
    );
    assert_eq!(rows[0][1], Value::Text("before".into()));

    eng.run_sql("COMMIT").unwrap();

    // 事务结束后：应看到外部写入
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2, "事务结束后应看到所有数据");
}

#[test]
fn tx_snapshot_read_your_writes() {
    let (_dir, mut eng, _store) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();

    eng.run_sql("BEGIN").unwrap();

    // 事务内写入
    eng.run_sql("INSERT INTO t VALUES (2, 'new_in_tx')")
        .unwrap();
    eng.run_sql("UPDATE t SET v = 'updated' WHERE id = 1")
        .unwrap();

    // 事务内应看到自己的写入（read-your-writes）
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id ASC").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("updated".into()));
    assert_eq!(rows[1][1], Value::Text("new_in_tx".into()));

    eng.run_sql("ROLLBACK").unwrap();

    // 回滚后数据恢复
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("old".into()));
}

#[test]
fn tx_snapshot_isolation_delete_invisible() {
    let (_dir, mut eng, store) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'keep')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'delete_me')")
        .unwrap();

    eng.run_sql("BEGIN").unwrap();

    // 外部删除 id=2
    let ks = store.open_keyspace("sql_t").unwrap();
    let pk = Value::Integer(2).to_bytes().unwrap();
    ks.delete(&pk).unwrap();

    // 事务内应仍看到 2 条（快照隔离）
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2, "快照隔离：外部删除对事务不可见");

    eng.run_sql("COMMIT").unwrap();

    // 事务外应只看到 1 条
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn tx_snapshot_isolation_update_invisible() {
    let (_dir, mut eng, store) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'original')").unwrap();

    eng.run_sql("BEGIN").unwrap();

    // 外部更新 id=1
    let ks = store.open_keyspace("sql_t").unwrap();
    let schema = {
        let raw = store
            .open_keyspace("sql_meta")
            .unwrap()
            .get(b"t")
            .unwrap()
            .unwrap();
        let mut s: crate::types::Schema = serde_json::from_slice(&raw).unwrap();
        s.ensure_defaults();
        s
    };
    let row = vec![Value::Integer(1), Value::Text("modified_externally".into())];
    let pk = row[0].to_bytes().unwrap();
    let raw = schema.encode_row(&row).unwrap();
    ks.set(&pk, &raw).unwrap();

    // 事务内 PK 查询：应看到原始值
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][1],
        Value::Text("original".into()),
        "快照隔离：外部 UPDATE 对事务内 PK 查询不可见"
    );

    // 事务内全表扫描：也应看到原始值
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(
        rows[0][1],
        Value::Text("original".into()),
        "快照隔离：外部 UPDATE 对事务内全表扫描不可见"
    );

    eng.run_sql("COMMIT").unwrap();

    // 事务外应看到更新后的值
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("modified_externally".into()));
}

#[test]
fn non_tx_select_reads_latest() {
    let (_dir, mut eng, store) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'v1')").unwrap();

    // 非事务 SELECT 应始终读最新数据
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);

    // 直接写入 storage
    let ks = store.open_keyspace("sql_t").unwrap();
    let schema = {
        let raw = store
            .open_keyspace("sql_meta")
            .unwrap()
            .get(b"t")
            .unwrap()
            .unwrap();
        let mut s: crate::types::Schema = serde_json::from_slice(&raw).unwrap();
        s.ensure_defaults();
        s
    };
    let row = vec![Value::Integer(2), Value::Text("v2".into())];
    let pk = row[0].to_bytes().unwrap();
    let raw = schema.encode_row(&row).unwrap();
    ks.set(&pk, &raw).unwrap();

    // 非事务 SELECT 立即看到新数据
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2, "非事务 SELECT 应读到最新数据");
}

#[test]
fn snapshot_api_public() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::Store::open(dir.path()).unwrap();
    let ks = store.open_keyspace("test").unwrap();
    ks.set(b"a", b"1").unwrap();

    let snap = store.snapshot();
    ks.set(b"a", b"2").unwrap();
    ks.set(b"b", b"3").unwrap();

    // 快照读
    assert_eq!(snap.get(&ks, b"a").unwrap().as_deref(), Some(b"1" as &[u8]));
    assert_eq!(snap.get(&ks, b"b").unwrap(), None);

    // 当前读
    assert_eq!(ks.get(b"a").unwrap().as_deref(), Some(b"2" as &[u8]));
    assert_eq!(ks.get(b"b").unwrap().as_deref(), Some(b"3" as &[u8]));
}
