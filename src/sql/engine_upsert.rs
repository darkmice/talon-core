/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine ON CONFLICT (UPSERT) 实现。
//! 从 engine_exec.rs 拆分，保持单文件 ≤500 行。

use super::engine::SqlEngine;
use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::parser::{OnConflict, OnConflictValue};
use crate::types::{Schema, Value};
use crate::Error;

impl SqlEngine {
    /// INSERT ... ON CONFLICT DO UPDATE SET col = EXCLUDED.col 实现。
    /// 支持单列 PK 冲突和复合唯一约束冲突。
    pub(super) fn exec_insert_on_conflict(
        &mut self,
        table: &str,
        values: Vec<Vec<Value>>,
        oc: &OnConflict,
        schema: &Schema,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 判断冲突类型：PK 冲突 vs 复合唯一约束冲突
        let pk_col = &schema.columns[0].0;
        let is_pk_conflict = oc.conflict_columns.len() == 1 && oc.conflict_columns[0] == *pk_col;
        if is_pk_conflict {
            self.upsert_by_pk(table, values, oc, schema)
        } else {
            self.upsert_by_unique(table, values, oc, schema)
        }
    }

    /// PK 冲突的 UPSERT（原有逻辑）。
    fn upsert_by_pk(
        &mut self,
        table: &str,
        values: Vec<Vec<Value>>,
        oc: &OnConflict,
        schema: &Schema,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let mut inserted_rows = Vec::new();
        let mut old_rows = Vec::new();
        let mut new_rows = Vec::new();
        // 预收集索引列信息（支持复合索引）
        let idx_cols: Vec<(String, Vec<usize>)> = self
            .cache
            .get(table)
            .unwrap()
            .index_keyspaces
            .keys()
            .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
            .collect();
        for new_row in &values {
            schema.validate_row(new_row)?;
            let pk = new_row
                .first()
                .ok_or_else(|| Error::SqlExec("INSERT 行为空".into()))?;
            let key = pk.to_bytes()?;
            if let Some(old_raw) = self.tx_get(table, &key)? {
                let mut row = schema.decode_row(&old_raw)?;
                let old_row = row.clone();
                // 删旧索引（支持复合索引）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, pk)?;
                    self.tx_index_delete(table, cols_key, &ik)?;
                }
                apply_oc_assignments(&mut row, oc, new_row, schema)?;
                let raw = schema.encode_row(&row)?;
                self.tx_set(table, key, raw)?;
                // 插新索引（支持复合索引）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, pk)?;
                    self.tx_index_set(table, cols_key, ik)?;
                }
                old_rows.push(old_row);
                new_rows.push(row);
            } else {
                let raw = schema.encode_row(new_row)?;
                self.tx_set(table, key, raw)?;
                // 插新索引（支持复合索引）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(new_row, ci, pk)?;
                    self.tx_index_set(table, cols_key, ik)?;
                }
                inserted_rows.push(new_row.clone());
            }
        }
        sync_vec_after_upsert(
            &self.store,
            table,
            &inserted_rows,
            &old_rows,
            &new_rows,
            schema,
            &self.cache,
        )?;
        Ok(vec![])
    }

    /// 复合唯一约束冲突的 UPSERT：扫描全表找匹配复合键的行。
    fn upsert_by_unique(
        &mut self,
        table: &str,
        values: Vec<Vec<Value>>,
        oc: &OnConflict,
        schema: &Schema,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 解析冲突列索引
        let conflict_indices: Vec<usize> = oc
            .conflict_columns
            .iter()
            .map(|c| {
                schema
                    .column_index_by_name(c)
                    .ok_or_else(|| Error::SqlExec(format!("ON CONFLICT 列不存在: {}", c)))
            })
            .collect::<Result<_, _>>()?;

        let mut inserted_rows = Vec::new();
        let mut old_rows = Vec::new();
        let mut new_rows = Vec::new();
        // 预收集索引列信息（支持复合索引）
        let idx_cols: Vec<(String, Vec<usize>)> = self
            .cache
            .get(table)
            .unwrap()
            .index_keyspaces
            .keys()
            .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
            .collect();
        for new_row in &values {
            schema.validate_row(new_row)?;
            // 提取新行的复合键值
            let new_key_vals: Vec<&Value> =
                conflict_indices.iter().map(|&ci| &new_row[ci]).collect();
            // 扫描已有行查找冲突
            let conflict_pk =
                self.find_conflict_row(table, schema, &conflict_indices, &new_key_vals)?;
            if let Some(pk_bytes) = conflict_pk {
                // 冲突：更新已有行
                let old_raw = self
                    .tx_get(table, &pk_bytes)?
                    .ok_or_else(|| Error::SqlExec("ON CONFLICT: 冲突行不存在".into()))?;
                let mut row = schema.decode_row(&old_raw)?;
                let old_row = row.clone();
                let pk = &row[0];
                // 删旧索引（支持复合索引）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, pk)?;
                    self.tx_index_delete(table, cols_key, &ik)?;
                }
                apply_oc_assignments(&mut row, oc, new_row, schema)?;
                let raw = schema.encode_row(&row)?;
                self.tx_set(table, pk_bytes, raw)?;
                // 插新索引（支持复合索引）
                let pk = &row[0];
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, pk)?;
                    self.tx_index_set(table, cols_key, ik)?;
                }
                old_rows.push(old_row);
                new_rows.push(row);
            } else {
                // 无冲突：正常插入
                let pk = new_row
                    .first()
                    .ok_or_else(|| Error::SqlExec("INSERT 行为空".into()))?;
                let key = pk.to_bytes()?;
                let raw = schema.encode_row(new_row)?;
                self.tx_set(table, key, raw)?;
                // 插新索引（支持复合索引）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(new_row, ci, pk)?;
                    self.tx_index_set(table, cols_key, ik)?;
                }
                inserted_rows.push(new_row.clone());
            }
        }
        sync_vec_after_upsert(
            &self.store,
            table,
            &inserted_rows,
            &old_rows,
            &new_rows,
            schema,
            &self.cache,
        )?;
        Ok(vec![])
    }

    /// 扫描全表查找复合键冲突行，返回其 PK 字节。
    /// 事务写缓冲优先于磁盘数据（事务内 DELETE 的行不算冲突）。
    fn find_conflict_row(
        &self,
        table: &str,
        schema: &Schema,
        conflict_indices: &[usize],
        key_vals: &[&Value],
    ) -> Result<Option<Vec<u8>>, Error> {
        // 1. 先检查事务写缓冲（优先级最高）
        if let Some(ref tx) = self.tx {
            for ((t, pk_bytes), val) in &tx.writes {
                if t != table {
                    continue;
                }
                match val {
                    Some(raw) => {
                        if let Ok(row) = schema.decode_row(raw) {
                            let matches = conflict_indices
                                .iter()
                                .zip(key_vals.iter())
                                .all(|(&ci, &expected)| row.get(ci) == Some(expected));
                            if matches {
                                return Ok(Some(pk_bytes.clone()));
                            }
                        }
                    }
                    None => {} // 事务内已删除的行跳过
                }
            }
        }
        // 2. 扫描磁盘数据
        let tc = self.cache.get(table).unwrap();
        let mut found: Option<Vec<u8>> = None;
        let mut scan_err: Option<Error> = None;
        tc.data_ks.for_each_kv_prefix(b"", |_key, raw| {
            match schema.decode_row(raw) {
                Ok(row) => {
                    let matches = conflict_indices
                        .iter()
                        .zip(key_vals.iter())
                        .all(|(&ci, &expected)| row.get(ci) == Some(expected));
                    if matches {
                        if let Some(pk) = row.first() {
                            found = pk.to_bytes().ok();
                        }
                        return false;
                    }
                }
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            }
            true
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }
        // 3. 磁盘找到的行需验证未被事务删除
        if let Some(ref pk_bytes) = found {
            if let Some(ref tx) = self.tx {
                let key = (table.to_string(), pk_bytes.clone());
                if let Some(None) = tx.writes.get(&key) {
                    return Ok(None); // 该行已在事务中被删除，不算冲突
                }
            }
        }
        Ok(found)
    }
}

/// 应用 ON CONFLICT SET 赋值到目标行。
fn apply_oc_assignments(
    row: &mut Vec<Value>,
    oc: &OnConflict,
    new_row: &[Value],
    schema: &Schema,
) -> Result<(), Error> {
    for (col_name, oc_val) in &oc.assignments {
        let ci = schema
            .column_index_by_name(col_name)
            .ok_or_else(|| Error::SqlExec(format!("ON CONFLICT SET 列不存在: {}", col_name)))?;
        if ci == 0 {
            return Err(Error::SqlExec("不允许更新主键列".into()));
        }
        row[ci] = resolve_oc_value(oc_val, new_row, schema)?;
    }
    Ok(())
}

/// UPSERT 后同步向量索引。
fn sync_vec_after_upsert(
    store: &crate::storage::Store,
    table: &str,
    inserted_rows: &[Vec<Value>],
    old_rows: &[Vec<Value>],
    new_rows: &[Vec<Value>],
    schema: &Schema,
    cache: &std::collections::HashMap<String, super::engine::TableCache>,
) -> Result<(), Error> {
    if !inserted_rows.is_empty() {
        let hv = cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_insert(store, table, inserted_rows, schema, hv)?;
    }
    if !old_rows.is_empty() {
        let hv = cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_update(store, table, old_rows, new_rows, schema, hv)?;
    }
    Ok(())
}

/// 解析 ON CONFLICT SET 赋值的值：EXCLUDED.col → 新行对应列值，字面量 → 直接使用。
pub(super) fn resolve_oc_value(
    oc_val: &OnConflictValue,
    new_row: &[Value],
    schema: &Schema,
) -> Result<Value, Error> {
    match oc_val {
        OnConflictValue::Excluded(col) => {
            let ci = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("EXCLUDED.{} 列不存在", col)))?;
            Ok(new_row[ci].clone())
        }
        OnConflictValue::Literal(v) => Ok(v.clone()),
    }
}
