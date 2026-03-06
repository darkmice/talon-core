/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量索引元数据辅助函数：读写 `sql_vector_index_meta` keyspace。
//!
//! key 格式：`vidx:{table}:{column}` → JSON `{index_name, table, column, metric, m, ef_construction}`。
//! 供 CREATE VECTOR INDEX / DROP TABLE / INSERT / vec_search 共用。

use crate::storage::Store;
use crate::types::Value;
use crate::Error;

/// M88：将任意 PK 值映射为 VectorEngine 的 u64 ID。
/// Integer → 直接转换；Text/其他 → FNV-1a 哈希。
pub(super) fn pk_to_vec_id(pk: &Value) -> Option<u64> {
    match pk {
        Value::Integer(n) => Some(*n as u64),
        Value::Text(s) => Some(fnv1a_hash(s.as_bytes())),
        Value::Timestamp(ts) => Some(*ts as u64),
        Value::Null => None,
        other => Some(fnv1a_hash(&other.to_bytes().ok()?)),
    }
}

fn fnv1a_hash(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// 向量索引元数据 keyspace 名称。
pub(super) const VEC_IDX_META_KS: &str = "sql_vector_index_meta";

/// 向量索引元数据。
#[derive(Debug, Clone)]
pub(super) struct VecIdxMeta {
    /// 向量列名。
    pub column: String,
}

/// 构造 VectorEngine 名称：`sql_{table}_{column}`。
pub(super) fn vec_engine_name(table: &str, column: &str) -> String {
    format!("sql_{}_{}", table, column)
}

/// 查询指定表的所有向量索引元数据。
/// M87：for_each_kv_prefix 消除 N+1。
pub(super) fn list_vec_indexes(store: &Store, table: &str) -> Result<Vec<VecIdxMeta>, Error> {
    let ks = store.open_keyspace(VEC_IDX_META_KS)?;
    let prefix = format!("vidx:{}:", table);
    let mut result = Vec::new();
    ks.for_each_kv_prefix(prefix.as_bytes(), |_key, raw| {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(raw) {
            result.push(VecIdxMeta {
                column: val["column"].as_str().unwrap_or("").to_string(),
            });
        }
        true
    })?;
    Ok(result)
}

/// 查询指定表+列的向量索引 metric。
pub(super) fn get_vec_index_metric(
    store: &Store,
    table: &str,
    column: &str,
) -> Result<Option<String>, Error> {
    let ks = store.open_keyspace(VEC_IDX_META_KS)?;
    let meta_key = format!("vidx:{}:{}", table, column);
    match ks.get(meta_key.as_bytes())? {
        Some(raw) => {
            let val: serde_json::Value =
                serde_json::from_slice(&raw).map_err(|e| Error::Serialization(e.to_string()))?;
            Ok(Some(val["metric"].as_str().unwrap_or("cosine").to_string()))
        }
        None => Ok(None),
    }
}

/// 删除指定表的所有向量索引元数据和 VectorEngine 数据。
/// 用于 DROP TABLE 级联清理。
pub(super) fn drop_vec_indexes_for_table(store: &Store, table: &str) -> Result<(), Error> {
    let indexes = list_vec_indexes(store, table)?;
    let ks = store.open_keyspace(VEC_IDX_META_KS)?;
    for idx in &indexes {
        // 删除元数据
        let meta_key = format!("vidx:{}:{}", table, &idx.column);
        ks.delete(meta_key.as_bytes())?;
        // 清理 VectorEngine keyspace 数据
        let vec_name = vec_engine_name(table, &idx.column);
        let vec_ks_name = format!("vector_{}", vec_name);
        if let Ok(vec_ks) = store.open_keyspace(&vec_ks_name) {
            let mut keys: Vec<Vec<u8>> = Vec::new();
            vec_ks.for_each_key_prefix(b"", |key| {
                keys.push(key.to_vec());
                true
            })?;
            let mut batch = store.batch();
            for k in &keys {
                batch.remove(&vec_ks, k.clone());
            }
            if !keys.is_empty() {
                batch.commit()?;
            }
        }
    }
    Ok(())
}

/// INSERT 后同步向量数据到 VectorEngine（如果表有向量索引）。
/// `rows` 为刚插入的行数据，`schema` 为表 schema。
/// M89：`has_vec` 为缓存标志，false 时直接跳过，避免 open_keyspace。
pub(super) fn sync_vec_on_insert(
    store: &Store,
    table: &str,
    rows: &[Vec<crate::types::Value>],
    schema: &crate::types::Schema,
    has_vec: bool,
) -> Result<(), Error> {
    if !has_vec {
        return Ok(());
    }
    let indexes = list_vec_indexes(store, table)?;
    if indexes.is_empty() {
        return Ok(());
    }
    for idx in &indexes {
        let col_idx = match schema.column_index_by_name(&idx.column) {
            Some(ci) => ci,
            None => continue,
        };
        let vec_name = vec_engine_name(table, &idx.column);
        let ve = crate::vector::VectorEngine::open(store, &vec_name)?;
        for row in rows {
            if let Value::Vector(ref vec_data) = row[col_idx] {
                if let Some(vid) = pk_to_vec_id(&row[0]) {
                    ve.insert(vid, vec_data)?;
                }
            }
        }
    }
    Ok(())
}

/// UPDATE 后同步向量数据：删除旧向量 + 插入新向量。
/// M89：`has_vec` 为缓存标志。
pub(super) fn sync_vec_on_update(
    store: &Store,
    table: &str,
    old_rows: &[Vec<crate::types::Value>],
    new_rows: &[Vec<crate::types::Value>],
    schema: &crate::types::Schema,
    has_vec: bool,
) -> Result<(), Error> {
    if !has_vec {
        return Ok(());
    }
    let indexes = list_vec_indexes(store, table)?;
    if indexes.is_empty() {
        return Ok(());
    }
    for idx in &indexes {
        let col_idx = match schema.column_index_by_name(&idx.column) {
            Some(ci) => ci,
            None => continue,
        };
        let vec_name = vec_engine_name(table, &idx.column);
        let ve = crate::vector::VectorEngine::open(store, &vec_name)?;
        for (old_row, new_row) in old_rows.iter().zip(new_rows.iter()) {
            // 仅当向量列值不同时才同步
            if old_row[col_idx] == new_row[col_idx] {
                continue;
            }
            // 删除旧向量
            if let Some(vid) = pk_to_vec_id(&old_row[0]) {
                ve.delete(vid)?;
            }
            // 插入新向量
            if let Value::Vector(ref vec_data) = new_row[col_idx] {
                if let Some(vid) = pk_to_vec_id(&new_row[0]) {
                    ve.insert(vid, vec_data)?;
                }
            }
        }
    }
    Ok(())
}

/// DELETE 后从 VectorEngine 删除向量数据（如果表有向量索引）。
/// M89：`has_vec` 为缓存标志。
pub(super) fn sync_vec_on_delete(
    store: &Store,
    table: &str,
    rows: &[Vec<crate::types::Value>],
    has_vec: bool,
) -> Result<(), Error> {
    if !has_vec {
        return Ok(());
    }
    let indexes = list_vec_indexes(store, table)?;
    if indexes.is_empty() {
        return Ok(());
    }
    for idx in &indexes {
        let vec_name = vec_engine_name(table, &idx.column);
        let ve = crate::vector::VectorEngine::open(store, &vec_name)?;
        for row in rows {
            if let Some(vid) = pk_to_vec_id(&row[0]) {
                ve.delete(vid)?;
            }
        }
    }
    Ok(())
}
