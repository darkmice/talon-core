/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! ffi_exec 模块单元测试：覆盖 JSON-RPC 命令路由的各引擎操作。

use super::*;
use crate::Talon;

/// 创建临时数据库实例。
fn tmp_db() -> (tempfile::TempDir, Talon) {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    (dir, db)
}

/// 解析返回 JSON，断言 ok=true 并返回 data。
fn assert_ok(json_str: &str) -> serde_json::Value {
    let v: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert!(
        v["ok"].as_bool().unwrap(),
        "expected ok=true, got: {}",
        json_str
    );
    v["data"].clone()
}

/// 解析返回 JSON，断言 ok=false。
fn assert_err(json_str: &str) {
    let v: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert!(
        !v["ok"].as_bool().unwrap(),
        "expected ok=false, got: {}",
        json_str
    );
}

// ── 基础路由测试 ──

#[test]
fn execute_unknown_module() {
    let (_dir, db) = tmp_db();
    let res = execute_cmd(&db, r#"{"module":"nope","action":"x","params":{}}"#);
    assert_err(&res);
}

#[test]
fn execute_invalid_json() {
    let (_dir, db) = tmp_db();
    let res = execute_cmd(&db, "not json");
    assert_err(&res);
}

#[test]
fn execute_sql_roundtrip() {
    let (_dir, db) = tmp_db();
    let res = execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"CREATE TABLE t (id INT, name TEXT)"}}"#,
    );
    assert_ok(&res);
    let res = execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"INSERT INTO t (id, name) VALUES (1, 'a')"}}"#,
    );
    assert_ok(&res);
    let res = execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"SELECT * FROM t"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["rows"].as_array().unwrap().len(), 1);
}

#[test]
fn execute_kv_roundtrip() {
    let (_dir, db) = tmp_db();
    // set
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"set","params":{"key":"k1","value":"v1"}}"#,
    );
    assert_ok(&res);
    // get
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"get","params":{"key":"k1"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["value"].as_str().unwrap(), "v1");
    // del
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"del","params":{"key":"k1"}}"#,
    );
    let data = assert_ok(&res);
    assert!(data["deleted"].as_bool().unwrap());
    // get after del
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"get","params":{"key":"k1"}}"#,
    );
    let data = assert_ok(&res);
    assert!(data["value"].is_null());
}

#[test]
fn execute_stats() {
    let (_dir, db) = tmp_db();
    let res = execute_cmd(&db, r#"{"module":"stats","action":"","params":{}}"#);
    let data = assert_ok(&res);
    assert_eq!(data["engine"].as_str().unwrap(), "talon");
}

#[test]
fn execute_mq_roundtrip() {
    let (_dir, db) = tmp_db();
    // create topic
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"create","params":{"topic":"t1","max_len":0}}"#,
    );
    assert_ok(&res);
    // publish
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"publish","params":{"topic":"t1","payload":"hello"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["id"].as_u64().unwrap(), 1);
    // poll
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"poll","params":{"topic":"t1","group":"g","consumer":"c","count":10}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["messages"].as_array().unwrap().len(), 1);
}

#[test]
fn execute_vector_roundtrip() {
    let (_dir, db) = tmp_db();
    // insert
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"insert","params":{"name":"emb","id":1,"vector":[0.1,0.2,0.3]}}"#,
    );
    assert_ok(&res);
    // search
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"search","params":{"name":"emb","vector":[0.1,0.2,0.3],"k":1}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["results"].as_array().unwrap().len(), 1);
    // count
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"count","params":{"name":"emb"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["count"].as_u64().unwrap(), 1);
}

// ── M27 新增：KV mset/mget/keys_match ──

#[test]
fn execute_kv_mset_mget() {
    let (_dir, db) = tmp_db();
    // mset
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"mset","params":{"keys":["a","b","c"],"values":["1","2","3"]}}"#,
    );
    assert_ok(&res);
    // mget
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"mget","params":{"keys":["a","b","c","d"]}}"#,
    );
    let data = assert_ok(&res);
    let vals = data["values"].as_array().unwrap();
    assert_eq!(vals.len(), 4);
    assert_eq!(vals[0].as_str().unwrap(), "1");
    assert_eq!(vals[1].as_str().unwrap(), "2");
    assert_eq!(vals[2].as_str().unwrap(), "3");
    assert!(vals[3].is_null());
}

#[test]
fn execute_kv_keys_match() {
    let (_dir, db) = tmp_db();
    // 写入几个 key
    for (k, v) in &[
        ("user:1", "a"),
        ("user:2", "b"),
        ("user:10", "c"),
        ("admin:1", "d"),
    ] {
        let cmd = format!(
            r#"{{"module":"kv","action":"set","params":{{"key":"{}","value":"{}"}}}}"#,
            k, v
        );
        assert_ok(&execute_cmd(&db, &cmd));
    }
    // glob: user:*
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"keys_match","params":{"pattern":"user:*"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["keys"].as_array().unwrap().len(), 3);
    // glob: user:? (单字符)
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"keys_match","params":{"pattern":"user:?"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["keys"].as_array().unwrap().len(), 2);
    // glob: * (全部)
    let res = execute_cmd(
        &db,
        r#"{"module":"kv","action":"keys_match","params":{"pattern":"*"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["keys"].as_array().unwrap().len(), 4);
}

// ── M29 新增：MQ poll block_ms + Vector batch ──

#[test]
fn execute_mq_poll_block_ms_zero() {
    let (_dir, db) = tmp_db();
    assert_ok(&execute_cmd(
        &db,
        r#"{"module":"mq","action":"create","params":{"topic":"t","max_len":0}}"#,
    ));
    // block_ms=0 等同于普通 poll，无消息立即返回
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"poll","params":{"topic":"t","group":"g","consumer":"c","count":10,"block_ms":0}}"#,
    );
    let data = assert_ok(&res);
    assert!(data["messages"].as_array().unwrap().is_empty());
}

#[test]
fn execute_mq_poll_block_ms_with_data() {
    let (_dir, db) = tmp_db();
    assert_ok(&execute_cmd(
        &db,
        r#"{"module":"mq","action":"create","params":{"topic":"t","max_len":0}}"#,
    ));
    assert_ok(&execute_cmd(
        &db,
        r#"{"module":"mq","action":"publish","params":{"topic":"t","payload":"msg1"}}"#,
    ));
    // block_ms=1000 但有数据应立即返回
    let start = std::time::Instant::now();
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"poll","params":{"topic":"t","group":"g","consumer":"c","count":10,"block_ms":1000}}"#,
    );
    let elapsed = start.elapsed();
    let data = assert_ok(&res);
    assert_eq!(data["messages"].as_array().unwrap().len(), 1);
    assert!(elapsed.as_millis() < 500, "should return immediately");
}

#[test]
fn execute_vector_batch_insert_and_search() {
    let (_dir, db) = tmp_db();
    // batch_insert
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"batch_insert","params":{"name":"emb","items":[{"id":1,"vector":[1.0,0.0]},{"id":2,"vector":[0.0,1.0]},{"id":3,"vector":[0.7,0.7]}]}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["inserted"].as_u64().unwrap(), 3);
    // count
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"count","params":{"name":"emb"}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["count"].as_u64().unwrap(), 3);
    // batch_search
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"batch_search","params":{"name":"emb","vectors":[[1.0,0.0],[0.0,1.0]],"k":1}}"#,
    );
    let data = assert_ok(&res);
    let results = data["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_array().unwrap()[0]["id"].as_u64().unwrap(), 1);
    assert_eq!(results[1].as_array().unwrap()[0]["id"].as_u64().unwrap(), 2);
}

// ── M30: MQ SUBSCRIBE 测试 ──

#[test]
fn execute_mq_subscribe_and_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = crate::Talon::open(dir.path()).unwrap();

    // 创建 topic
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"create","params":{"topic":"events","max_len":0}}"#,
    );
    assert_ok(&res);

    // 订阅两个消费者组
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"subscribe","params":{"topic":"events","group":"analytics"}}"#,
    );
    assert_ok(&res);
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"subscribe","params":{"topic":"events","group":"logging"}}"#,
    );
    assert_ok(&res);

    // 列出订阅
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"list_subscriptions","params":{"topic":"events"}}"#,
    );
    let data = assert_ok(&res);
    let groups = data["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2);

    // 取消订阅
    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"unsubscribe","params":{"topic":"events","group":"analytics"}}"#,
    );
    assert_ok(&res);

    let res = execute_cmd(
        &db,
        r#"{"module":"mq","action":"list_subscriptions","params":{"topic":"events"}}"#,
    );
    let data = assert_ok(&res);
    let groups = data["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].as_str().unwrap(), "logging");
}

// ── M34: Vector set_ef_search FFI 测试 ──

#[test]
fn execute_vector_set_ef_search() {
    let (_dir, db) = tmp_db();
    // 先插入向量
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"insert","params":{"name":"emb","id":1,"vector":[1.0,0.0]}}"#,
    );
    assert_ok(&res);
    // set_ef_search
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"set_ef_search","params":{"name":"emb","ef_search":50}}"#,
    );
    assert_ok(&res);
    // 搜索仍正常
    let res = execute_cmd(
        &db,
        r#"{"module":"vector","action":"search","params":{"name":"emb","vector":[1.0,0.0],"k":1}}"#,
    );
    let data = assert_ok(&res);
    assert_eq!(data["results"].as_array().unwrap().len(), 1);
}

// ── ALTER TABLE ADD COLUMN 全路径测试（FFI → Talon::run_sql → SqlEngine）──

/// 辅助：通过 FFI JSON 执行 SQL 并返回 data。
fn ffi_sql(db: &Talon, sql: &str) -> serde_json::Value {
    let cmd = serde_json::json!({"module":"sql","action":"","params":{"sql": sql}});
    let res = execute_cmd(db, &cmd.to_string());
    let v: serde_json::Value = serde_json::from_str(&res).unwrap();
    assert!(
        v["ok"].as_bool().unwrap(),
        "SQL failed: {} — response: {}",
        sql,
        res
    );
    v["data"].clone()
}

/// 辅助：通过 Talon::run_sql 执行 SQL。
fn talon_sql(db: &Talon, sql: &str) -> Vec<Vec<crate::types::Value>> {
    db.run_sql(sql)
        .unwrap_or_else(|e| panic!("run_sql('{}') failed: {}", sql, e))
}

#[test]
fn ffi_alter_table_add_column_basic() {
    let (_dir, db) = tmp_db();
    ffi_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    ffi_sql(&db, "INSERT INTO t VALUES (1, 'Alice')");
    // ADD COLUMN without COLUMN keyword
    ffi_sql(&db, "ALTER TABLE t ADD age INT");
    let data = ffi_sql(&db, "SELECT * FROM t WHERE id = 1");
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_array().unwrap().len(), 3);
}

#[test]
fn ffi_alter_table_add_column_keyword() {
    let (_dir, db) = tmp_db();
    ffi_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    ffi_sql(&db, "INSERT INTO t VALUES (1, 'Alice')");
    // ADD COLUMN with COLUMN keyword
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN age INT");
    let data = ffi_sql(&db, "SELECT * FROM t WHERE id = 1");
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows[0].as_array().unwrap().len(), 3);
}

#[test]
fn ffi_alter_table_add_column_with_default() {
    let (_dir, db) = tmp_db();
    ffi_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    ffi_sql(&db, "INSERT INTO t VALUES (1, 'Alice')");
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN score INT DEFAULT 100");
    let data = ffi_sql(&db, "SELECT * FROM t WHERE id = 1");
    let rows = data["rows"].as_array().unwrap();
    // 旧行的新列应该返回默认值 100
    assert_eq!(rows[0][2], serde_json::json!({"Integer": 100}));
}

#[test]
fn ffi_alter_table_add_column_then_insert() {
    let (_dir, db) = tmp_db();
    ffi_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN age INT DEFAULT 0");
    ffi_sql(&db, "INSERT INTO t VALUES (1, 'Alice', 25)");
    let data = ffi_sql(&db, "SELECT * FROM t WHERE id = 1");
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows[0][2], serde_json::json!({"Integer": 25}));
}

#[test]
fn ffi_alter_table_add_column_then_describe() {
    let (_dir, db) = tmp_db();
    ffi_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN extra FLOAT");
    let data = ffi_sql(&db, "DESCRIBE t");
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    // 第三列名称为 extra
    assert_eq!(rows[2][0], serde_json::json!({"Text": "extra"}));
}

#[test]
fn ffi_alter_table_add_multiple_columns() {
    let (_dir, db) = tmp_db();
    ffi_sql(&db, "CREATE TABLE t (id INT)");
    ffi_sql(&db, "INSERT INTO t VALUES (1)");
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN a INT DEFAULT 10");
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'hello'");
    ffi_sql(&db, "ALTER TABLE t ADD COLUMN c BOOLEAN DEFAULT TRUE");
    let data = ffi_sql(&db, "SELECT * FROM t WHERE id = 1");
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows[0].as_array().unwrap().len(), 4);
    assert_eq!(rows[0][1], serde_json::json!({"Integer": 10}));
    assert_eq!(rows[0][2], serde_json::json!({"Text": "hello"}));
    assert_eq!(rows[0][3], serde_json::json!({"Boolean": true}));
}

#[test]
fn talon_alter_table_add_column_basic() {
    let (_dir, db) = tmp_db();
    talon_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    talon_sql(&db, "INSERT INTO t VALUES (1, 'Alice')");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN age INT");
    let rows = talon_sql(&db, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].len(), 3);
}

#[test]
fn talon_alter_table_add_column_with_default_select_old_rows() {
    let (_dir, db) = tmp_db();
    talon_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    talon_sql(&db, "INSERT INTO t VALUES (1, 'Alice')");
    talon_sql(&db, "INSERT INTO t VALUES (2, 'Bob')");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN score INT DEFAULT 50");
    let rows = talon_sql(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.len(), 3);
        // 旧行新列 = 默认值 50
        assert_eq!(row[2], crate::types::Value::Integer(50));
    }
    // 新行插入带新列
    talon_sql(&db, "INSERT INTO t VALUES (3, 'Carol', 99)");
    let rows = talon_sql(&db, "SELECT * FROM t WHERE id = 3");
    assert_eq!(rows[0][2], crate::types::Value::Integer(99));
}

#[test]
fn talon_alter_table_add_column_types() {
    let (_dir, db) = tmp_db();
    talon_sql(&db, "CREATE TABLE t (id INT)");
    talon_sql(&db, "INSERT INTO t VALUES (1)");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN a TEXT DEFAULT 'x'");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN b FLOAT DEFAULT 3.14");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN c BOOLEAN DEFAULT FALSE");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN d INT");
    let rows = talon_sql(&db, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].len(), 5);
    assert_eq!(rows[0][1], crate::types::Value::Text("x".into()));
    assert_eq!(rows[0][2], crate::types::Value::Float(3.14));
    assert_eq!(rows[0][3], crate::types::Value::Boolean(false));
    assert_eq!(rows[0][4], crate::types::Value::Null);
}

#[test]
fn talon_alter_table_add_column_duplicate_rejected() {
    let (_dir, db) = tmp_db();
    talon_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    let err = db.run_sql("ALTER TABLE t ADD COLUMN name TEXT");
    assert!(err.is_err(), "duplicate column should fail");
}

#[test]
fn talon_alter_table_add_column_update_new_col() {
    let (_dir, db) = tmp_db();
    talon_sql(&db, "CREATE TABLE t (id INT, name TEXT)");
    talon_sql(&db, "INSERT INTO t VALUES (1, 'Alice')");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN active BOOLEAN DEFAULT TRUE");
    talon_sql(&db, "UPDATE t SET active = FALSE WHERE id = 1");
    let rows = talon_sql(&db, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0][2], crate::types::Value::Boolean(false));
}

#[test]
fn talon_alter_table_add_column_where_on_new_col() {
    let (_dir, db) = tmp_db();
    talon_sql(&db, "CREATE TABLE t (id INT)");
    talon_sql(&db, "INSERT INTO t VALUES (1)");
    talon_sql(&db, "INSERT INTO t VALUES (2)");
    talon_sql(&db, "ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'active'");
    talon_sql(&db, "INSERT INTO t VALUES (3, 'inactive')");
    let rows = talon_sql(&db, "SELECT * FROM t WHERE status = 'active'");
    assert_eq!(rows.len(), 2);
}

#[test]
fn execute_sql_returns_columns() {
    let (_dir, db) = tmp_db();
    execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"CREATE TABLE t (id INT, name TEXT, age INT)"}}"#,
    );
    execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"INSERT INTO t VALUES (1, 'alice', 30)"}}"#,
    );
    // SELECT * 应返回所有列名
    let res = execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"SELECT * FROM t"}}"#,
    );
    let data = assert_ok(&res);
    let cols = data["columns"].as_array().expect("columns should be array");
    assert_eq!(cols.len(), 3, "SELECT * should return 3 columns");
    assert_eq!(cols[0].as_str().unwrap(), "id");
    assert_eq!(cols[1].as_str().unwrap(), "name");
    assert_eq!(cols[2].as_str().unwrap(), "age");

    // 显式列
    let res = execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"SELECT name, age FROM t"}}"#,
    );
    let data = assert_ok(&res);
    let cols = data["columns"].as_array().expect("columns should be array");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].as_str().unwrap(), "name");
    assert_eq!(cols[1].as_str().unwrap(), "age");

    // SHOW TABLES
    let res = execute_cmd(
        &db,
        r#"{"module":"sql","action":"","params":{"sql":"SHOW TABLES"}}"#,
    );
    let data = assert_ok(&res);
    let cols = data["columns"].as_array().expect("columns should be array");
    assert_eq!(cols[0].as_str().unwrap(), "table_name");
}

// M40-M42 FFI 测试已拆分到 tests_ai_ext.rs。
