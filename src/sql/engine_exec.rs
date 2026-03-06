/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine 语句执行器：DDL/DML 的缓存路径实现。

use std::collections::HashMap;
use std::sync::Arc;

use super::engine::{SqlEngine, TableCache};
use super::index_key::{composite_index_entry_key, index_entry_key};
use super::parser::OnConflict;
use crate::storage::Keyspace;
use crate::types::{Schema, Value};
use crate::Error;

/// M112：从 cols_key（逗号分隔列名）解析列索引列表。
pub(super) fn resolve_col_indices(schema: &Schema, cols_key: &str) -> Option<Vec<usize>> {
    cols_key
        .split(',')
        .map(|c| schema.column_index_by_name(c))
        .collect()
}

/// M112：根据列索引列表构造索引 entry key（兼容单列和复合索引）。
pub(super) fn build_idx_key(
    row: &[Value],
    col_indices: &[usize],
    pk: &Value,
) -> Result<Vec<u8>, Error> {
    if col_indices.len() == 1 {
        index_entry_key(&row[col_indices[0]], pk)
    } else {
        let vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
        composite_index_entry_key(&vals, pk)
    }
}

impl SqlEngine {
    pub(super) fn exec_create_table(
        &mut self,
        name: String,
        col_defs: Vec<super::parser::ColumnDef>,
        if_not_exists: bool,
        unique_constraints: Vec<Vec<String>>,
        check_constraints: Vec<String>,
        temporary: bool,
        foreign_keys: Vec<(String, String, String)>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M125：视图名冲突检查
        if self.is_view(&name)? {
            return Err(Error::SqlExec(format!(
                "同名视图已存在，无法创建表: {}",
                name
            )));
        }
        if if_not_exists && self.meta_ks.contains_key(name.as_bytes())? {
            return Ok(vec![]);
        }
        let col_count = col_defs.len();
        let columns: Vec<(String, crate::types::ColumnType)> = col_defs
            .iter()
            .map(|c| (c.name.clone(), c.col_type.clone()))
            .collect();
        let column_nullable: Vec<bool> = col_defs.iter().map(|c| c.nullable).collect();
        let column_defaults: Vec<Option<Value>> =
            col_defs.iter().map(|c| c.default_value.clone()).collect();
        let schema = Schema {
            columns,
            version: 0,
            column_defaults,
            column_nullable,
            dropped_columns: vec![],
            unique_constraints,
            auto_increment: col_defs.first().is_some_and(|c| c.auto_increment),
            check_constraints: check_constraints.clone(),
            foreign_keys: foreign_keys
                .iter()
                .map(|(c, rt, rc)| crate::types::ForeignKeyDef {
                    column: c.clone(),
                    ref_table: rt.clone(),
                    ref_column: rc.clone(),
                })
                .collect(),
            table_comment: None,
            column_comments: vec![],
        };
        // M127：验证外键引用的父表和列存在
        for fk in &schema.foreign_keys {
            if !self.ensure_cached(&fk.ref_table)? {
                return Err(Error::SqlExec(format!(
                    "外键引用的父表不存在: {}",
                    fk.ref_table
                )));
            }
            let parent_tc = self.cache.get(&fk.ref_table).unwrap();
            if parent_tc
                .schema
                .column_index_by_name(&fk.ref_column)
                .is_none()
            {
                return Err(Error::SqlExec(format!(
                    "外键引用的列不存在: {}.{}",
                    fk.ref_table, fk.ref_column
                )));
            }
        }
        let _ = col_count;
        let raw = serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
        self.meta_ks.set(name.as_bytes(), &raw)?;
        let data_ks = self.store.open_keyspace(&format!("sql_{}", name))?;
        // M118：解析 CHECK 约束并缓存
        let mut parsed_checks = Vec::with_capacity(check_constraints.len());
        for chk_sql in &check_constraints {
            let expr = super::parser::where_clause::parse_where(chk_sql)
                .map_err(|e| Error::SqlExec(format!("CHECK 约束解析失败: {}: {}", chk_sql, e)))?;
            parsed_checks.push(expr);
        }
        self.cache.insert(
            name.clone(),
            TableCache {
                schema: Arc::new(schema),
                data_ks,
                index_keyspaces: HashMap::new(),
                has_vec_indexes: false,
                unique_indexes: std::collections::HashSet::new(),
                parsed_checks,
            },
        );
        // M126：记录临时表，引擎 drop 时自动清理
        if temporary {
            self.temp_tables.insert(name);
        }
        Ok(vec![])
    }

    pub(super) fn exec_drop_table(
        &mut self,
        name: &str,
        if_exists: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let exists = self.meta_ks.contains_key(name.as_bytes())?;
        if !exists {
            if if_exists {
                return Ok(vec![]);
            }
            return Err(Error::SqlExec(format!("table not found: {}", name)));
        }
        // M127：外键引用检查 — 有子表引用时禁止删除
        self.check_fk_on_drop(name)?;
        self.meta_ks.delete(name.as_bytes())?;
        // M104: 清理 AUTOINCREMENT 计数器
        let counter_key = format!("autoincr:{}", name);
        let _ = self.meta_ks.delete(counter_key.as_bytes());
        let prefix = format!("idx:{}:", name);
        let idx_keys = self.index_meta_ks.keys_with_prefix(prefix.as_bytes())?;
        for k in &idx_keys {
            self.index_meta_ks.delete(k)?;
        }
        // 级联清理向量索引元数据和 VectorEngine 数据
        super::vec_idx::drop_vec_indexes_for_table(&self.store, name)?;
        self.cache.remove(name);
        self.temp_tables.remove(name);
        self.segments.remove_prefix(&format!("sql:{}:", name));
        Ok(vec![])
    }

    pub(super) fn exec_insert(
        &mut self,
        table: &str,
        mut values: Vec<Vec<Value>>,
        or_replace: bool,
        or_ignore: bool,
        on_conflict: Option<&OnConflict>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M125：视图写保护
        if self.is_view(table)? {
            return Err(Error::SqlExec(format!(
                "视图是只读的，不能 INSERT: {}",
                table
            )));
        }
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("table not found: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let schema = tc.schema.clone();
        // M104: AUTOINCREMENT — 自动分配主键
        if schema.auto_increment {
            let counter_key = format!("autoincr:{}", table);
            let mut counter: i64 = self
                .meta_ks
                .get(counter_key.as_bytes())?
                .map(|raw| {
                    if raw.len() == 8 {
                        i64::from_be_bytes(raw[..8].try_into().unwrap_or([0; 8]))
                    } else {
                        0
                    }
                })
                .unwrap_or(0);
            for row in &mut values {
                if !row.is_empty() && (row[0] == Value::Null || row[0] == Value::Integer(0)) {
                    if counter == i64::MAX {
                        return Err(Error::SqlExec("AUTOINCREMENT counter overflow".into()));
                    }
                    counter += 1;
                    row[0] = Value::Integer(counter);
                } else if let Value::Integer(v) = &row[0] {
                    if *v > counter {
                        counter = *v;
                    }
                }
            }
            self.meta_ks
                .set(counter_key.as_bytes(), &counter.to_be_bytes())?;
        }
        // 隐式类型转换（Text → Jsonb 等）
        for row in &mut values {
            schema.coerce_types(row);
        }
        let has_idx = !tc.index_keyspaces.is_empty();
        let in_tx = self.tx.is_some();
        let has_oc = on_conflict.is_some();

        // M118：CHECK 约束校验闭包（缓存引用避免重复查找）
        let tc_ref = self.cache.get(table).unwrap();
        let has_checks = !tc_ref.parsed_checks.is_empty();

        // M120：单行快速路径（含索引维护），避免 HashSet/WriteBatch 开销
        if values.len() == 1 && !has_oc {
            let row = &values[0];
            schema.validate_row(row)?;
            // M118：CHECK 约束校验
            if has_checks {
                let tc = self.cache.get(table).unwrap();
                super::helpers::validate_check_constraints(
                    row,
                    &schema,
                    &tc.parsed_checks,
                    &schema.check_constraints,
                )?;
            }
            // M127：外键约束校验
            self.check_fk_on_insert(table, row, &schema)?;
            let pk = row
                .first()
                .ok_or_else(|| Error::SqlExec("INSERT row is empty".into()))?;
            let key = pk.to_bytes()?;
            if !or_replace && self.tx_get(table, &key)?.is_some() {
                if or_ignore {
                    return Ok(vec![]);
                }
                return Err(Error::SqlExec(format!("duplicate primary key: {:?}", pk)));
            }
            if or_replace {
                self.invalidate_stats(table);
            }
            // M111：唯一索引约束检查（写入前）— M112：支持复合索引
            let tc = self.cache.get(table).unwrap();
            if !tc.unique_indexes.is_empty() {
                let exclude_pk = if or_replace {
                    Some(key.as_slice())
                } else {
                    None
                };
                let tx_writes = self.tx.as_ref().map(|tx| tx.index_writes.as_slice());
                for ui_col in &tc.unique_indexes {
                    if let Some(col_indices) = resolve_col_indices(&schema, ui_col) {
                        if let Some(idx_ks) = tc.index_keyspaces.get(ui_col) {
                            let vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
                            let result = if let Some(writes) = tx_writes {
                                super::index_key::check_unique_violation_tx_composite(
                                    idx_ks, writes, table, ui_col, &vals, exclude_pk,
                                )
                            } else {
                                super::index_key::check_unique_violation_composite(
                                    idx_ks, &vals, exclude_pk,
                                )
                            };
                            if let Err(e) = result {
                                if or_ignore {
                                    return Ok(vec![]);
                                }
                                return Err(e);
                            }
                        }
                    }
                }
            }
            let raw = schema.encode_row(row)?;
            if has_idx && !in_tx {
                // 非事务 + 有索引：用 batch 合并 data + index 写入
                let tc = self.cache.get(table).unwrap();
                let mut batch = self.store.batch();
                // M120：or_replace 时先删旧行索引
                if or_replace {
                    if let Some(old_raw) = tc.data_ks.get(&key)? {
                        let old_row = schema.decode_row(&old_raw)?;
                        let old_pk = old_row.first().unwrap();
                        for (cols_key, idx_ks) in &tc.index_keyspaces {
                            if let Some(ci) = resolve_col_indices(&schema, cols_key) {
                                batch.remove(idx_ks, build_idx_key(&old_row, &ci, old_pk)?);
                            }
                        }
                    }
                }
                batch.insert(&tc.data_ks, key, raw)?;
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if let Some(ci) = resolve_col_indices(&schema, cols_key) {
                        let idx_key = build_idx_key(row, &ci, pk)?;
                        batch.insert(idx_ks, idx_key, Vec::new())?;
                    }
                }
                batch.commit()?;
            } else {
                self.tx_set(table, key, raw)?;
                if has_idx {
                    let tc = self.cache.get(table).unwrap();
                    let idx_refs: Vec<(Vec<usize>, String)> = tc
                        .index_keyspaces
                        .keys()
                        .filter_map(|c| resolve_col_indices(&schema, c).map(|ci| (ci, c.clone())))
                        .collect();
                    for (ci, cols_key) in &idx_refs {
                        let idx_key = build_idx_key(row, ci, pk)?;
                        self.tx_index_set(table, cols_key, idx_key)?;
                    }
                }
            }
            let stats = self.column_stats.entry(table.to_string()).or_default();
            super::engine::accumulate_stats(stats, row);
            let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
            super::vec_idx::sync_vec_on_insert(&self.store, table, &values, &schema, hv)?;
            return Ok(vec![]);
        }

        // ON CONFLICT DO UPDATE 路径
        if let Some(oc) = on_conflict {
            return self.exec_insert_on_conflict(table, values, oc, &schema);
        }

        if in_tx {
            for row in &values {
                schema.validate_row(row)?;
                // M118：CHECK 约束校验（事务路径）
                if has_checks {
                    let tc = self.cache.get(table).unwrap();
                    super::helpers::validate_check_constraints(
                        row,
                        &schema,
                        &tc.parsed_checks,
                        &schema.check_constraints,
                    )?;
                }
                // M127：外键约束校验（事务路径）
                self.check_fk_on_insert(table, row, &schema)?;
                let pk = row
                    .first()
                    .ok_or_else(|| Error::SqlExec("INSERT row is empty".into()))?;
                let key = pk.to_bytes()?;
                // or_replace 模式下需先删旧行索引
                if or_replace {
                    if let Some(old_raw) = self.tx_get(table, &key)? {
                        let old_row = schema.decode_row(&old_raw)?;
                        let tc = self.cache.get(table).unwrap();
                        let idx_cols: Vec<(String, Vec<usize>)> = tc
                            .index_keyspaces
                            .keys()
                            .filter_map(|c| {
                                resolve_col_indices(&schema, c).map(|ci| (c.clone(), ci))
                            })
                            .collect();
                        for (cols_key, ci) in &idx_cols {
                            let ik = build_idx_key(&old_row, ci, pk)?;
                            self.tx_index_delete(table, cols_key, &ik)?;
                        }
                    }
                } else if self.tx_get(table, &key)?.is_some() {
                    if or_ignore {
                        continue; // 静默跳过冲突行
                    }
                    return Err(Error::SqlExec(format!("duplicate primary key: {:?}", pk)));
                }
                let raw = schema.encode_row(row)?;
                // M111：事务路径唯一索引检查（含 tx 缓冲区）— M112：支持复合索引
                let tc = self.cache.get(table).unwrap();
                if !tc.unique_indexes.is_empty() {
                    let exclude_pk = if or_replace {
                        Some(key.as_slice())
                    } else {
                        None
                    };
                    let tx_writes = self.tx.as_ref().map(|tx| tx.index_writes.as_slice());
                    let mut unique_err = None;
                    for ui_col in &tc.unique_indexes {
                        if let Some(col_indices) = resolve_col_indices(&schema, ui_col) {
                            if let Some(idx_ks) = tc.index_keyspaces.get(ui_col) {
                                let vals: Vec<&Value> =
                                    col_indices.iter().map(|&i| &row[i]).collect();
                                let result = if let Some(writes) = tx_writes {
                                    super::index_key::check_unique_violation_tx_composite(
                                        idx_ks, writes, table, ui_col, &vals, exclude_pk,
                                    )
                                } else {
                                    super::index_key::check_unique_violation_composite(
                                        idx_ks, &vals, exclude_pk,
                                    )
                                };
                                if let Err(e) = result {
                                    unique_err = Some(e);
                                    break;
                                }
                            }
                        }
                    }
                    if let Some(e) = unique_err {
                        if or_ignore {
                            continue; // 跳过当前行
                        }
                        return Err(e);
                    }
                }
                self.tx_set(table, key, raw)?;
            }
            // M82：索引写入缓冲到事务，COMMIT 时统一刷出
            if has_idx {
                let tc = self.cache.get(table).unwrap();
                let idx_refs: Vec<(Vec<usize>, String)> = tc
                    .index_keyspaces
                    .keys()
                    .filter_map(|cols_key| {
                        resolve_col_indices(&schema, cols_key).map(|ci| (ci, cols_key.clone()))
                    })
                    .collect();
                for row in &values {
                    let pk = row.first().unwrap();
                    for (ci, cols_key) in &idx_refs {
                        let idx_key = build_idx_key(row, ci, pk)?;
                        self.tx_index_set(table, cols_key, idx_key)?;
                    }
                }
            }
            // 同步向量索引（事务路径）
            let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
            super::vec_idx::sync_vec_on_insert(&self.store, table, &values, &schema, hv)?;
            return Ok(vec![]);
        }

        // M127：外键约束预检查（批量非事务路径）— 在 tc 借用前完成
        if !schema.foreign_keys.is_empty() {
            for row in &values {
                self.check_fk_on_insert(table, row, &schema)?;
            }
        }

        let tc = self.cache.get(table).unwrap();
        let idx_refs: Vec<(Vec<usize>, &Keyspace)> = tc
            .index_keyspaces
            .iter()
            .filter_map(|(cols_key, ks)| resolve_col_indices(&schema, cols_key).map(|ci| (ci, ks)))
            .collect();
        // M111：预收集唯一索引列信息 — M112：支持复合索引
        let unique_refs: Vec<(Vec<usize>, &Keyspace)> = tc
            .unique_indexes
            .iter()
            .filter_map(|cols_key| {
                let ci = resolve_col_indices(&schema, cols_key)?;
                let ks = tc.index_keyspaces.get(cols_key)?;
                Some((ci, ks))
            })
            .collect();
        let mut batch = self.store.batch();
        let data_ks = &tc.data_ks;
        let mut seen_keys = std::collections::HashSet::new();
        for row in &values {
            schema.validate_row(row)?;
            // M118：CHECK 约束校验（批量路径）
            if has_checks {
                let tc2 = self.cache.get(table).unwrap();
                super::helpers::validate_check_constraints(
                    row,
                    &schema,
                    &tc2.parsed_checks,
                    &schema.check_constraints,
                )?;
            }
            let pk = row
                .first()
                .ok_or_else(|| Error::SqlExec("INSERT row is empty".into()))?;
            let key = pk.to_bytes()?;
            // 重复主键检测（含同批次内重复）
            if or_replace {
                // or_replace：删旧行索引
                if let Some(old_raw) = data_ks.get(&key)? {
                    let old_row = schema.decode_row(&old_raw)?;
                    for (ci, idx_ks) in &idx_refs {
                        let old_pk = old_row.first().unwrap();
                        let idx_key = build_idx_key(&old_row, ci, old_pk)?;
                        batch.remove(idx_ks, idx_key);
                    }
                }
                seen_keys.insert(key.clone());
            } else if data_ks.get(&key)?.is_some() || !seen_keys.insert(key.clone()) {
                if or_ignore {
                    continue; // 静默跳过冲突行
                }
                return Err(Error::SqlExec(format!("duplicate primary key: {:?}", pk)));
            }
            let raw = schema.encode_row(row)?;
            // M111：非事务批量路径唯一索引检查 — M112：支持复合索引
            if !unique_refs.is_empty() {
                let exclude_pk = if or_replace {
                    Some(key.as_slice())
                } else {
                    None
                };
                let mut unique_err = None;
                for (ci, idx_ks) in &unique_refs {
                    let vals: Vec<&Value> = ci.iter().map(|&i| &row[i]).collect();
                    if let Err(e) = super::index_key::check_unique_violation_composite(
                        idx_ks, &vals, exclude_pk,
                    ) {
                        unique_err = Some(e);
                        break;
                    }
                }
                if let Some(e) = unique_err {
                    if or_ignore {
                        continue;
                    }
                    return Err(e);
                }
            }
            batch.insert(data_ks, key.clone(), raw)?;
            for (ci, idx_ks) in &idx_refs {
                let idx_key = build_idx_key(row, ci, pk)?;
                batch.insert(idx_ks, idx_key, Vec::new())?;
            }
        }
        batch.commit()?;
        // 同步向量索引（批量路径）
        let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_insert(&self.store, table, &values, &schema, hv)?;
        Ok(vec![])
    }
}
