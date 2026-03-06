/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M31 测试：CREATE VECTOR INDEX SQL 执行 + DROP TABLE 级联 + INSERT 自动同步。

use crate::sql::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn setup() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}

/// CREATE VECTOR INDEX → INSERT → SQL vec_search 端到端。
#[test]
fn create_vector_index_and_search() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE docs (id INT, title TEXT, emb VECTOR(3))")
        .unwrap();
    eng.run_sql(
        "CREATE VECTOR INDEX idx_emb ON docs(emb) WITH (metric='cosine', m=16, ef_construction=200)",
    )
    .unwrap();

    // INSERT 后数据应自动同步到 VectorEngine
    eng.run_sql("INSERT INTO docs VALUES (1, 'hello', [0.1, 0.2, 0.3])")
        .unwrap();
    eng.run_sql("INSERT INTO docs VALUES (2, 'world', [0.9, 0.8, 0.7])")
        .unwrap();
    eng.run_sql("INSERT INTO docs VALUES (3, 'test', [0.11, 0.21, 0.31])")
        .unwrap();

    // SQL 向量搜索
    let rows = eng
        .run_sql(
            "SELECT id, title, vec_cosine(emb, [0.1, 0.2, 0.3]) AS dist FROM docs ORDER BY dist LIMIT 2",
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    // 第一个结果应该是 id=1（完全匹配）
    assert_eq!(rows[0][0], Value::Integer(1));
}

/// CREATE VECTOR INDEX 回填已有数据。
#[test]
fn create_vector_index_backfills_existing_data() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE items (id INT, vec VECTOR(2))")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (1, [1.0, 0.0])")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (2, [0.0, 1.0])")
        .unwrap();

    // 创建索引时应回填已有数据
    eng.run_sql("CREATE VECTOR INDEX idx_v ON items(vec)")
        .unwrap();

    // 验证 VectorEngine 有数据
    let ve = crate::vector::VectorEngine::open(&store, "sql_items_vec").unwrap();
    assert_eq!(ve.count().unwrap(), 2);
}

/// DROP TABLE 级联清理向量索引元数据和 VectorEngine 数据。
#[test]
fn drop_table_cascades_vector_index() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE t1 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON t1(v)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, [0.5, 0.5])")
        .unwrap();

    // 确认 VectorEngine 有数据
    let ve = crate::vector::VectorEngine::open(&store, "sql_t1_v").unwrap();
    assert_eq!(ve.count().unwrap(), 1);

    // DROP TABLE 应级联清理
    eng.run_sql("DROP TABLE t1").unwrap();

    // 向量索引元数据应被清理
    let meta_ks = store.open_keyspace("sql_vector_index_meta").unwrap();
    let keys = meta_ks.keys_with_prefix(b"vidx:t1:").unwrap();
    assert!(keys.is_empty(), "向量索引元数据应被清理");
}

/// DROP TABLE IF EXISTS 不报错。
#[test]
fn drop_table_if_exists_no_error() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();
    let result = eng.run_sql("DROP TABLE IF EXISTS nonexistent");
    assert!(result.is_ok());
}

/// DELETE 同步向量索引删除。
#[test]
fn delete_syncs_vector_index() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE d1 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON d1(v)").unwrap();
    eng.run_sql("INSERT INTO d1 VALUES (1, [0.1, 0.2])")
        .unwrap();
    eng.run_sql("INSERT INTO d1 VALUES (2, [0.3, 0.4])")
        .unwrap();

    let ve = crate::vector::VectorEngine::open(&store, "sql_d1_v").unwrap();
    assert_eq!(ve.count().unwrap(), 2);

    // 删除一行
    eng.run_sql("DELETE FROM d1 WHERE id = 1").unwrap();

    // VectorEngine 应标记删除
    let ve2 = crate::vector::VectorEngine::open(&store, "sql_d1_v").unwrap();
    assert_eq!(ve2.count().unwrap(), 1);
}

/// 非 VECTOR 列创建向量索引应报错。
#[test]
fn create_vector_index_on_non_vector_column_fails() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE t2 (id INT, name TEXT)").unwrap();
    let result = eng.run_sql("CREATE VECTOR INDEX idx_name ON t2(name)");
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("不是 VECTOR 类型"));
}

/// vec_distance 自动发现索引 metric。
#[test]
fn vec_distance_auto_discovers_metric() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE m1 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON m1(v) WITH (metric='l2')")
        .unwrap();
    eng.run_sql("INSERT INTO m1 VALUES (1, [1.0, 0.0])")
        .unwrap();
    eng.run_sql("INSERT INTO m1 VALUES (2, [0.0, 1.0])")
        .unwrap();

    // vec_distance 应自动使用 l2 metric
    let rows = eng
        .run_sql("SELECT id, vec_distance(v, [1.0, 0.0]) AS dist FROM m1 ORDER BY dist LIMIT 1")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    // L2 距离应为 0.0（完全匹配）
    if let Value::Float(d) = &rows[0][1] {
        assert!(*d < 0.001, "L2 距离应接近 0，实际: {}", d);
    }
}

/// UPDATE 向量列同步到 VectorEngine。
#[test]
fn update_syncs_vector_index() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE u1 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON u1(v)").unwrap();
    eng.run_sql("INSERT INTO u1 VALUES (1, [1.0, 0.0])")
        .unwrap();
    eng.run_sql("INSERT INTO u1 VALUES (2, [0.0, 1.0])")
        .unwrap();

    // 确认初始状态
    let ve = crate::vector::VectorEngine::open(&store, "sql_u1_v").unwrap();
    assert_eq!(ve.count().unwrap(), 2);

    // UPDATE 向量列
    eng.run_sql("UPDATE u1 SET v = [0.5, 0.5] WHERE id = 1")
        .unwrap();

    // VectorEngine 数量不变（delete old + insert new）
    let ve2 = crate::vector::VectorEngine::open(&store, "sql_u1_v").unwrap();
    assert_eq!(ve2.count().unwrap(), 2);

    // 搜索应返回更新后的向量
    let rows = eng
        .run_sql("SELECT id, vec_distance(v, [0.5, 0.5]) AS dist FROM u1 ORDER BY dist LIMIT 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(1));
    if let Value::Float(d) = &rows[0][1] {
        assert!(*d < 0.001, "更新后距离应接近 0，实际: {}", d);
    }
}

/// UPDATE 非向量列不触发向量同步（无副作用）。
#[test]
fn update_non_vector_column_no_sync() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE u2 (id INT, name TEXT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON u2(v)").unwrap();
    eng.run_sql("INSERT INTO u2 VALUES (1, 'a', [1.0, 0.0])")
        .unwrap();

    let ve = crate::vector::VectorEngine::open(&store, "sql_u2_v").unwrap();
    assert_eq!(ve.count().unwrap(), 1);

    // UPDATE 非向量列
    eng.run_sql("UPDATE u2 SET name = 'b' WHERE id = 1")
        .unwrap();

    // 向量索引不受影响
    let ve2 = crate::vector::VectorEngine::open(&store, "sql_u2_v").unwrap();
    assert_eq!(ve2.count().unwrap(), 1);

    // 搜索仍正常
    let rows = eng
        .run_sql("SELECT id, vec_cosine(v, [1.0, 0.0]) AS dist FROM u2 ORDER BY dist LIMIT 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(1));
}

/// 非缓存路径（executor）INSERT 同步向量索引。
#[test]
fn executor_insert_syncs_vector_index() {
    let (_dir, store) = setup();
    // 先用 SqlEngine 创建表和向量索引
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE e1 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON e1(v)").unwrap();
    drop(eng);

    // 通过 executor（非缓存路径）INSERT
    let plan = crate::sql::planner::Plan {
        stmt: crate::sql::parse("INSERT INTO e1 VALUES (1, [1.0, 0.0])").unwrap(),
    };
    crate::sql::execute(&store, plan).unwrap();

    // 验证 VectorEngine 有数据
    let ve = crate::vector::VectorEngine::open(&store, "sql_e1_v").unwrap();
    assert_eq!(ve.count().unwrap(), 1);
}

/// 非缓存路径（executor）DELETE 同步向量索引。
#[test]
fn executor_delete_syncs_vector_index() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE e2 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON e2(v)").unwrap();
    eng.run_sql("INSERT INTO e2 VALUES (1, [1.0, 0.0])")
        .unwrap();
    eng.run_sql("INSERT INTO e2 VALUES (2, [0.0, 1.0])")
        .unwrap();
    drop(eng);

    let ve = crate::vector::VectorEngine::open(&store, "sql_e2_v").unwrap();
    assert_eq!(ve.count().unwrap(), 2);

    // 通过 executor（非缓存路径）DELETE
    let plan = crate::sql::planner::Plan {
        stmt: crate::sql::parse("DELETE FROM e2 WHERE id = 1").unwrap(),
    };
    crate::sql::execute(&store, plan).unwrap();

    let ve2 = crate::vector::VectorEngine::open(&store, "sql_e2_v").unwrap();
    assert_eq!(ve2.count().unwrap(), 1);
}

/// DROP VECTOR INDEX 删除向量索引元数据和 VectorEngine 数据。
#[test]
fn drop_vector_index_removes_metadata_and_data() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE dv1 (id INT, v VECTOR(2))")
        .unwrap();
    eng.run_sql("CREATE VECTOR INDEX idx_v ON dv1(v)").unwrap();
    eng.run_sql("INSERT INTO dv1 VALUES (1, [1.0, 0.0])")
        .unwrap();

    let ve = crate::vector::VectorEngine::open(&store, "sql_dv1_v").unwrap();
    assert_eq!(ve.count().unwrap(), 1);

    // DROP VECTOR INDEX
    eng.run_sql("DROP VECTOR INDEX idx_v").unwrap();

    // 元数据应被清理
    let meta_ks = store.open_keyspace("sql_vector_index_meta").unwrap();
    let keys = meta_ks.keys_with_prefix(b"vidx:dv1:").unwrap();
    assert!(keys.is_empty(), "向量索引元数据应被清理");
}

/// DROP VECTOR INDEX IF EXISTS 不报错。
#[test]
fn drop_vector_index_if_exists_no_error() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();
    let result = eng.run_sql("DROP VECTOR INDEX IF EXISTS nonexistent");
    assert!(result.is_ok());
}

/// DROP VECTOR INDEX 不存在时报错。
#[test]
fn drop_vector_index_not_found_error() {
    let (_dir, store) = setup();
    let mut eng = SqlEngine::new(&store).unwrap();
    let result = eng.run_sql("DROP VECTOR INDEX nonexistent");
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("不存在"));
}
