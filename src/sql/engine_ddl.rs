/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine DDL 操作：SHOW TABLES / DESCRIBE / CREATE/DROP INDEX / TRUNCATE。

use std::sync::Arc;

use super::engine::SqlEngine;
use super::index_key::composite_index_entry_key;
use super::parser::AlterAction;
use crate::types::Value;
use crate::Error;

/// M169：将值转换为目标列类型。
/// 转换规则：TEXT→INTEGER(parse), INTEGER→TEXT(to_string), INTEGER→FLOAT,
/// FLOAT→INTEGER(truncate), BOOLEAN→INTEGER(0/1), NULL 保持 NULL。
fn convert_value(val: &Value, target: &crate::types::ColumnType) -> Result<Value, Error> {
    use crate::types::ColumnType;
    if matches!(val, Value::Null) {
        return Ok(Value::Null);
    }
    match target {
        ColumnType::Integer => match val {
            Value::Integer(_) => Ok(val.clone()),
            Value::Float(f) => Ok(Value::Integer(*f as i64)),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| Error::SqlExec(format!("无法将 '{}' 转换为 INTEGER", s))),
            Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            _ => Err(Error::SqlExec(format!("无法将 {:?} 转换为 INTEGER", val))),
        },
        ColumnType::Float => match val {
            Value::Float(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Float(*i as f64)),
            Value::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| Error::SqlExec(format!("无法将 '{}' 转换为 FLOAT", s))),
            Value::Boolean(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
            _ => Err(Error::SqlExec(format!("无法将 {:?} 转换为 FLOAT", val))),
        },
        ColumnType::Text => match val {
            Value::Text(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Text(i.to_string())),
            Value::Float(f) => Ok(Value::Text(f.to_string())),
            Value::Boolean(b) => Ok(Value::Text(b.to_string())),
            _ => Err(Error::SqlExec(format!("无法将 {:?} 转换为 TEXT", val))),
        },
        ColumnType::Boolean => match val {
            Value::Boolean(_) => Ok(val.clone()),
            Value::Integer(i) => Ok(Value::Boolean(*i != 0)),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Boolean(true)),
                "false" | "0" | "no" => Ok(Value::Boolean(false)),
                _ => Err(Error::SqlExec(format!("无法将 '{}' 转换为 BOOLEAN", s))),
            },
            _ => Err(Error::SqlExec(format!("无法将 {:?} 转换为 BOOLEAN", val))),
        },
        _ => Err(Error::SqlExec(format!("不支持转换到 {:?} 类型", target))),
    }
}

/// M169：公开包装——供 executor_ddl 无状态路径调用。
pub(super) fn convert_value_pub(
    val: &Value,
    target: &crate::types::ColumnType,
) -> Result<Value, Error> {
    convert_value(val, target)
}

/// 流式清空 keyspace：分批删除（每批 1000），O(1) 内存，大表安全。
fn truncate_keyspace(
    ks: &crate::storage::Keyspace,
    store: &crate::storage::Store,
) -> Result<(), Error> {
    loop {
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
        ks.for_each_key_prefix(b"", |key| {
            keys.push(key.to_vec());
            keys.len() < 1000
        })?;
        if keys.is_empty() {
            break;
        }
        let mut batch = store.batch();
        for k in &keys {
            batch.remove(ks, k.clone());
        }
        batch.commit()?;
    }
    Ok(())
}

impl SqlEngine {
    /// CREATE INDEX / CREATE UNIQUE INDEX：在指定列上创建二级索引并回填已有数据。
    /// M112：支持复合索引 `CREATE INDEX idx ON t(a, b, c)`。
    pub(super) fn exec_create_index(
        &mut self,
        index_name: String,
        table: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let cols_key = columns.join(",");
        // IF NOT EXISTS: skip silently if index already registered
        let meta_key = format!("idx:{}:{}", table, cols_key);
        if self.index_meta_ks.get(meta_key.as_bytes())?.is_some() {
            return Ok(vec![]);
        }
        let tc = self.cache.get(table).unwrap();
        let col_indices: Vec<usize> = columns
            .iter()
            .map(|c| {
                tc.schema
                    .column_index_by_name(c)
                    .ok_or_else(|| Error::SqlExec(format!("索引列不存在: {}", c)))
            })
            .collect::<Result<_, _>>()?;
        if col_indices.iter().any(|&i| i == 0) {
            return Err(Error::SqlExec("主键列无需建索引".into()));
        }
        // M111：唯一索引元数据 value 前缀 "u:"，普通索引直接存 index_name
        let meta_val = if unique {
            format!("u:{}", index_name)
        } else {
            index_name.clone()
        };
        let idx_ks_name = format!("idx_{}_{}", table, cols_key.replace(',', "_"));
        let idx_ks = self.store.open_keyspace(&idx_ks_name)?;
        let data_ks = &tc.data_ks;
        let schema = &tc.schema;
        // 流式扫描回填索引（元数据在回填成功后再写入，避免残缺索引）
        let mut backfill_err: Option<Error> = None;
        let mut seen_keys: Option<std::collections::HashSet<Vec<u8>>> = if unique {
            Some(std::collections::HashSet::new())
        } else {
            None
        };
        data_ks.for_each_kv_prefix(b"", |_key, raw| {
            match schema.decode_row(raw) {
                Ok(row) => {
                    let vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
                    match composite_index_entry_key(&vals, &row[0]) {
                        Ok(idx_key) => {
                            // 唯一索引回填时检测重复值
                            if let Some(ref mut seen) = seen_keys {
                                // 用列值组合的编码前缀作为去重 key
                                let mut val_sig = Vec::new();
                                for v in &vals {
                                    if let Ok(enc) = v.to_bytes() {
                                        val_sig.extend(enc);
                                    }
                                }
                                if !seen.insert(val_sig) {
                                    backfill_err = Some(Error::SqlExec(format!(
                                        "UNIQUE 约束冲突: 列 {} 存在重复值",
                                        cols_key
                                    )));
                                    return false;
                                }
                            }
                            if let Err(e) = idx_ks.set(&idx_key, []) {
                                backfill_err = Some(e);
                                return false;
                            }
                        }
                        Err(e) => {
                            backfill_err = Some(e);
                            return false;
                        }
                    }
                }
                Err(e) => {
                    backfill_err = Some(e);
                    return false;
                }
            }
            true
        })?;
        if let Some(e) = backfill_err {
            // 回填失败：清理已写入的索引数据，不写元数据
            truncate_keyspace(&idx_ks, &self.store)?;
            return Err(e);
        }
        // 回填成功后才写入索引元数据，避免残缺索引被后续查询使用
        self.index_meta_ks
            .set(meta_key.as_bytes(), meta_val.as_bytes())?;
        if let Some(tc) = self.cache.get_mut(table) {
            tc.index_keyspaces.insert(cols_key.clone(), idx_ks);
            if unique {
                tc.unique_indexes.insert(cols_key);
            }
        }
        Ok(vec![])
    }

    /// SHOW INDEXES [ON table] — 列出索引信息。
    /// 返回行格式：[index_name, table, column]。
    pub(super) fn exec_show_indexes(
        &self,
        table_filter: Option<&str>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let mut rows = Vec::new();
        let prefix = match table_filter {
            Some(t) => format!("idx:{}:", t),
            None => "idx:".to_string(),
        };
        self.index_meta_ks
            .for_each_kv_prefix(prefix.as_bytes(), |key, val| {
                let key_str = String::from_utf8_lossy(key).to_string();
                let raw_name = String::from_utf8_lossy(val).to_string();
                // M111：剥离 "u:" 前缀，显示 UNIQUE 标记
                let (idx_name, unique_marker) = if let Some(name) = raw_name.strip_prefix("u:") {
                    (name.to_string(), " [UNIQUE]")
                } else {
                    (raw_name, "")
                };
                // key format: "idx:{table}:{column}"
                if let Some(rest) = key_str.strip_prefix("idx:") {
                    if let Some((tbl, col)) = rest.split_once(':') {
                        rows.push(vec![
                            Value::Text(format!("{}{}", idx_name, unique_marker)),
                            Value::Text(tbl.to_string()),
                            Value::Text(col.to_string()),
                        ]);
                    }
                }
                true
            })?;
        Ok(rows)
    }

    /// ALTER TABLE ADD/DROP/RENAME COLUMN：O(1) 操作，只更新 schema 元数据。
    pub(super) fn exec_alter_table(
        &mut self,
        table: &str,
        action: AlterAction,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        match action {
            AlterAction::AddColumn {
                name,
                col_type,
                default,
            } => {
                let tc = self.cache.get(table).unwrap();
                if tc.schema.column_index_by_name(&name).is_some() {
                    return Err(Error::SqlExec(format!("列已存在: {}", name)));
                }
                let mut schema = (*tc.schema).clone();
                schema.columns.push((name, col_type));
                schema.version = schema.version.saturating_add(1);
                schema.column_defaults.push(default);
                schema.column_nullable.push(true);
                let raw =
                    serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.set(table.as_bytes(), &raw)?;
                if let Some(tc) = self.cache.get_mut(table) {
                    tc.schema = Arc::new(schema);
                }
                let seg_key = format!("sql:{}:schema", table);
                self.segments.put(seg_key, raw);
                Ok(vec![])
            }
            AlterAction::DropColumn { name } => {
                let tc = self.cache.get(table).unwrap();
                let phys_idx = tc
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(i, (n, _))| n == &name && !tc.schema.dropped_columns.contains(i))
                    .map(|(i, _)| i)
                    .ok_or_else(|| Error::SqlExec(format!("列不存在或已删除: {}", name)))?;
                if phys_idx == 0 {
                    return Err(Error::SqlExec("不允许删除主键列".into()));
                }
                let mut schema = (*tc.schema).clone();
                schema.dropped_columns.push(phys_idx);
                schema.version = schema.version.saturating_add(1);
                // M112：如果该列参与任何索引（单列或复合），清理索引
                let idx_keys_to_remove: Vec<String> = tc
                    .index_keyspaces
                    .keys()
                    .filter(|cols_key| cols_key.split(',').any(|c| c == name))
                    .cloned()
                    .collect();
                for cols_key in &idx_keys_to_remove {
                    let idx_ks_name = format!("idx_{}_{}", table, cols_key.replace(',', "_"));
                    if let Ok(idx_ks) = self.store.open_keyspace(&idx_ks_name) {
                        truncate_keyspace(&idx_ks, &self.store)?;
                    }
                    let meta_key = format!("idx:{}:{}", table, cols_key);
                    self.index_meta_ks.delete(meta_key.as_bytes())?;
                }
                let raw =
                    serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.set(table.as_bytes(), &raw)?;
                if let Some(tc) = self.cache.get_mut(table) {
                    tc.schema = Arc::new(schema);
                    for cols_key in &idx_keys_to_remove {
                        tc.index_keyspaces.remove(cols_key);
                        tc.unique_indexes.remove(cols_key);
                    }
                }
                let seg_key = format!("sql:{}:schema", table);
                self.segments.put(seg_key, raw);
                Ok(vec![])
            }
            AlterAction::RenameColumn { old_name, new_name } => {
                let tc = self.cache.get(table).unwrap();
                let phys_idx = tc
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(i, (n, _))| n == &old_name && !tc.schema.dropped_columns.contains(i))
                    .map(|(i, _)| i)
                    .ok_or_else(|| Error::SqlExec(format!("列不存在或已删除: {}", old_name)))?;
                if tc.schema.column_index_by_name(&new_name).is_some() {
                    return Err(Error::SqlExec(format!("列名已存在: {}", new_name)));
                }
                let mut schema = (*tc.schema).clone();
                schema.columns[phys_idx].0 = new_name.clone();
                schema.version = schema.version.saturating_add(1);
                // M112：收集所有包含 old_name 的索引 cols_key（单列或复合）
                let affected_idx: Vec<(String, String)> = tc
                    .index_keyspaces
                    .keys()
                    .filter(|cols_key| cols_key.split(',').any(|c| c == old_name))
                    .map(|old_ck| {
                        let new_ck = old_ck
                            .split(',')
                            .map(|c| if c == old_name { &new_name } else { c })
                            .collect::<Vec<_>>()
                            .join(",");
                        (old_ck.clone(), new_ck)
                    })
                    .collect();
                for (old_ck, new_ck) in &affected_idx {
                    let old_meta_key = format!("idx:{}:{}", table, old_ck);
                    let idx_name_raw = self.index_meta_ks.get(old_meta_key.as_bytes())?;
                    self.index_meta_ks.delete(old_meta_key.as_bytes())?;
                    if let Some(raw) = idx_name_raw {
                        let new_meta_key = format!("idx:{}:{}", table, new_ck);
                        self.index_meta_ks.set(new_meta_key.as_bytes(), &raw)?;
                    }
                }
                let raw =
                    serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.set(table.as_bytes(), &raw)?;
                if let Some(tc) = self.cache.get_mut(table) {
                    tc.schema = Arc::new(schema);
                    // M112：更新索引缓存 key（keyspace 实例不变，只改 HashMap key）
                    for (old_ck, new_ck) in &affected_idx {
                        if let Some(ks) = tc.index_keyspaces.remove(old_ck) {
                            tc.index_keyspaces.insert(new_ck.clone(), ks);
                        }
                        if tc.unique_indexes.remove(old_ck) {
                            tc.unique_indexes.insert(new_ck.clone());
                        }
                    }
                }
                let seg_key = format!("sql:{}:schema", table);
                self.segments.put(seg_key, raw);
                Ok(vec![])
            }
            AlterAction::RenameTo { new_name } => {
                // 检查新表名是否已存在
                if self.meta_ks.get(new_name.as_bytes())?.is_some() {
                    return Err(Error::SqlExec(format!("目标表已存在: {}", new_name)));
                }
                let tc = self.cache.get(table).unwrap();
                let schema = tc.schema.clone();
                let idx_cols: Vec<String> = tc.index_keyspaces.keys().cloned().collect();
                let has_vec = tc.has_vec_indexes;

                // 1. 迁移数据 keyspace：old → new
                let new_data_ks = self.store.open_keyspace(&format!("sql_{}", new_name))?;
                let mut copy_err: Option<Error> = None;
                tc.data_ks.for_each_kv_prefix(b"", |key, val| {
                    if let Err(e) = new_data_ks.set(key, val) {
                        copy_err = Some(e);
                        return false;
                    }
                    true
                })?;
                if let Some(e) = copy_err {
                    return Err(e);
                }
                truncate_keyspace(&tc.data_ks, &self.store)?;

                // 2. 迁移二级索引 keyspace + 元数据（M112：复合索引 cols_key 含逗号）
                for cols_key in &idx_cols {
                    let old_meta_key = format!("idx:{}:{}", table, cols_key);
                    let idx_name_raw = self.index_meta_ks.get(old_meta_key.as_bytes())?;
                    self.index_meta_ks.delete(old_meta_key.as_bytes())?;
                    if let Some(raw) = idx_name_raw {
                        let new_meta_key = format!("idx:{}:{}", new_name, cols_key);
                        self.index_meta_ks.set(new_meta_key.as_bytes(), &raw)?;
                    }
                    let ks_suffix = cols_key.replace(',', "_");
                    let old_idx_ks = self
                        .store
                        .open_keyspace(&format!("idx_{}_{}", table, ks_suffix))?;
                    let new_idx_ks = self
                        .store
                        .open_keyspace(&format!("idx_{}_{}", new_name, ks_suffix))?;
                    old_idx_ks.for_each_kv_prefix(b"", |key, val| {
                        if let Err(e) = new_idx_ks.set(key, val) {
                            copy_err = Some(e);
                            return false;
                        }
                        true
                    })?;
                    if let Some(e) = copy_err {
                        return Err(e);
                    }
                    truncate_keyspace(&old_idx_ks, &self.store)?;
                }

                // 3. 迁移向量索引元数据 + keyspace
                if has_vec {
                    let vec_meta_ks = self.store.open_keyspace(super::vec_idx::VEC_IDX_META_KS)?;
                    let prefix = format!("vidx:{}:", table);
                    let vidx_keys = vec_meta_ks.keys_with_prefix(prefix.as_bytes())?;
                    for key in &vidx_keys {
                        if let Some(raw) = vec_meta_ks.get(key)? {
                            if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&raw) {
                                let col = val["column"].as_str().unwrap_or("").to_string();
                                // 更新元数据中的 table 字段
                                val["table"] = serde_json::Value::String(new_name.clone());
                                let new_raw = serde_json::to_vec(&val)
                                    .map_err(|e| Error::Serialization(e.to_string()))?;
                                vec_meta_ks.delete(key)?;
                                let new_key = format!("vidx:{}:{}", new_name, col);
                                vec_meta_ks.set(new_key.as_bytes(), &new_raw)?;
                                // 迁移向量数据 keyspace
                                let old_vec_name = super::vec_idx::vec_engine_name(table, &col);
                                let new_vec_name = super::vec_idx::vec_engine_name(&new_name, &col);
                                let old_vec_ks_name = format!("vector_{}", old_vec_name);
                                let new_vec_ks_name = format!("vector_{}", new_vec_name);
                                if let Ok(old_ks) = self.store.open_keyspace(&old_vec_ks_name) {
                                    let new_ks = self.store.open_keyspace(&new_vec_ks_name)?;
                                    old_ks.for_each_kv_prefix(b"", |k, v| {
                                        if let Err(e) = new_ks.set(k, v) {
                                            copy_err = Some(e);
                                            return false;
                                        }
                                        true
                                    })?;
                                    if let Some(e) = copy_err {
                                        return Err(e);
                                    }
                                    truncate_keyspace(&old_ks, &self.store)?;
                                }
                            }
                        }
                    }
                }

                // 4. 更新 schema 元数据：删旧写新
                let schema_raw =
                    serde_json::to_vec(schema.as_ref()).map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.delete(table.as_bytes())?;
                self.meta_ks.set(new_name.as_bytes(), &schema_raw)?;

                // 4b. M104: 迁移 AUTOINCREMENT 计数器
                if schema.auto_increment {
                    let old_key = format!("autoincr:{}", table);
                    if let Some(raw) = self.meta_ks.get(old_key.as_bytes())? {
                        let new_key = format!("autoincr:{}", new_name);
                        self.meta_ks.set(new_key.as_bytes(), &raw)?;
                        self.meta_ks.delete(old_key.as_bytes())?;
                    }
                }

                // 5. 清理旧缓存，建立新缓存
                self.cache.remove(table);
                self.column_stats.remove(table);
                let old_seg_prefix = format!("sql:{}:", table);
                self.segments.remove_prefix(&old_seg_prefix);
                let seg_key = format!("sql:{}:schema", new_name);
                self.segments.put(seg_key, schema_raw);
                // 让 ensure_cached 重建新表缓存
                self.ensure_cached(&new_name)?;

                Ok(vec![])
            }
            // M165：ALTER TABLE t ALTER COLUMN col SET DEFAULT val
            AlterAction::SetDefault { column, value } => {
                let tc = self.cache.get(table).unwrap();
                let phys_idx = tc
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(i, (n, _))| n == &column && !tc.schema.dropped_columns.contains(i))
                    .map(|(i, _)| i)
                    .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
                let mut schema = (*tc.schema).clone();
                while schema.column_defaults.len() < schema.columns.len() {
                    schema.column_defaults.push(None);
                }
                schema.column_defaults[phys_idx] = Some(value);
                let raw =
                    serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.set(table.as_bytes(), &raw)?;
                if let Some(tc) = self.cache.get_mut(table) {
                    tc.schema = Arc::new(schema);
                }
                let seg_key = format!("sql:{}:schema", table);
                self.segments.put(seg_key, raw);
                Ok(vec![])
            }
            // M165：ALTER TABLE t ALTER COLUMN col DROP DEFAULT
            AlterAction::DropDefault { column } => {
                let tc = self.cache.get(table).unwrap();
                let phys_idx = tc
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(i, (n, _))| n == &column && !tc.schema.dropped_columns.contains(i))
                    .map(|(i, _)| i)
                    .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
                let mut schema = (*tc.schema).clone();
                while schema.column_defaults.len() < schema.columns.len() {
                    schema.column_defaults.push(None);
                }
                schema.column_defaults[phys_idx] = None;
                let raw =
                    serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.set(table.as_bytes(), &raw)?;
                if let Some(tc) = self.cache.get_mut(table) {
                    tc.schema = Arc::new(schema);
                }
                let seg_key = format!("sql:{}:schema", table);
                self.segments.put(seg_key, raw);
                Ok(vec![])
            }
            // M169：ALTER TABLE t ALTER COLUMN col TYPE new_type / MODIFY col new_type
            AlterAction::AlterType { column, new_type } => {
                let tc = self.cache.get(table).unwrap();
                let phys_idx = tc
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(i, (n, _))| n == &column && !tc.schema.dropped_columns.contains(i))
                    .map(|(i, _)| i)
                    .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
                if phys_idx == 0 {
                    return Err(Error::SqlExec("不允许修改主键列类型".into()));
                }
                let old_type = &tc.schema.columns[phys_idx].1;
                if *old_type == new_type {
                    return Ok(vec![]); // 类型相同，无需操作
                }
                // 阶段 1：验证所有行可转换
                let data_ks = &tc.data_ks;
                let schema = &tc.schema;
                let mut convert_err: Option<Error> = None;
                let mut rows_to_update: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
                data_ks.for_each_kv_prefix(b"", |key, raw| {
                    match schema.decode_row(raw) {
                        Ok(mut row) => match convert_value(&row[phys_idx], &new_type) {
                            Ok(new_val) => {
                                row[phys_idx] = new_val;
                                rows_to_update.push((key.to_vec(), row));
                            }
                            Err(e) => {
                                convert_err = Some(e);
                                return false;
                            }
                        },
                        Err(e) => {
                            convert_err = Some(e);
                            return false;
                        }
                    }
                    true
                })?;
                if let Some(e) = convert_err {
                    return Err(e);
                }
                // 阶段 2：更新 schema + 写入转换后的行
                let mut new_schema = (**schema).clone();
                new_schema.columns[phys_idx].1 = new_type;
                new_schema.version = new_schema.version.saturating_add(1);
                for (pk_bytes, row) in &rows_to_update {
                    data_ks.set(pk_bytes, &new_schema.encode_row(row)?)?;
                }
                let raw = serde_json::to_vec(&new_schema)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                self.meta_ks.set(table.as_bytes(), &raw)?;
                if let Some(tc) = self.cache.get_mut(table) {
                    tc.schema = Arc::new(new_schema);
                }
                let seg_key = format!("sql:{}:schema", table);
                self.segments.put(seg_key, raw);
                Ok(vec![])
            }
        }
    }

    /// SHOW TABLES：列出所有表名和视图名，按名称排序。
    /// M125：视图标记为 VIEW 类型（第二列）。
    /// M126：临时表标记为 TEMP 类型。
    pub(super) fn exec_show_tables(&mut self) -> Result<Vec<Vec<Value>>, Error> {
        let keys = self.meta_ks.keys_with_prefix(b"")?;
        let mut rows: Vec<Vec<Value>> = keys
            .iter()
            .filter(|k| !k.starts_with(b"autoincr:") && !k.starts_with(b"view:"))
            .map(|k| {
                let name = String::from_utf8_lossy(k).to_string();
                let type_str = if self.temp_tables.contains(&name) {
                    "TEMP"
                } else {
                    "TABLE"
                };
                vec![Value::Text(name), Value::Text(type_str.to_string())]
            })
            .collect();
        // M125：追加视图
        if let Ok(views) = self.list_views() {
            for v in views {
                rows.push(vec![Value::Text(v), Value::Text("VIEW".to_string())]);
            }
        }
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        });
        Ok(rows)
    }

    /// DESCRIBE table/view：返回列名、类型、是否主键、是否允许 NULL、默认值、外键、注释。
    /// M125：视图返回列名和类型（从视图 SQL 执行结果推断）。
    /// M164：增加第 7 列——列注释。
    pub(super) fn exec_describe(&mut self, table: &str) -> Result<Vec<Vec<Value>>, Error> {
        // M125：检查是否为视图
        if let Some(view_sql) = self.get_view_sql(table)? {
            return self.describe_view(table, &view_sql);
        }
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let mut rows = Vec::new();
        let mut vis_idx = 0usize;
        for (i, (name, col_type)) in tc.schema.columns.iter().enumerate() {
            if tc.schema.dropped_columns.contains(&i) {
                continue;
            }
            let type_str = format!("{:?}", col_type);
            let is_pk = if vis_idx == 0 { "YES" } else { "NO" };
            let nullable = tc.schema.column_nullable.get(i).copied().unwrap_or(true);
            let null_str = if nullable { "YES" } else { "NO" };
            let default_val = tc
                .schema
                .column_defaults
                .get(i)
                .and_then(|d| d.clone())
                .unwrap_or(Value::Null);
            // M127：外键引用信息
            let fk_info = tc
                .schema
                .foreign_keys
                .iter()
                .find(|fk| fk.column == *name)
                .map(|fk| format!("REFERENCES {}({})", fk.ref_table, fk.ref_column))
                .unwrap_or_default();
            // M164：列注释
            let comment = tc
                .schema
                .column_comments
                .get(i)
                .and_then(|c| c.clone())
                .unwrap_or_default();
            rows.push(vec![
                Value::Text(name.clone()),
                Value::Text(type_str),
                Value::Text(is_pk.to_string()),
                Value::Text(null_str.to_string()),
                default_val,
                Value::Text(fk_info),
                Value::Text(comment),
            ]);
            vis_idx += 1;
        }
        Ok(rows)
    }

    /// CREATE VECTOR INDEX：创建 HNSW 索引并回填数据。
    pub(super) fn exec_create_vector_index(
        &mut self,
        index_name: &str,
        table: &str,
        column: &str,
        metric: &str,
        m: usize,
        ef_construction: usize,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let col_idx = tc
            .schema
            .column_index_by_name(column)
            .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
        if !matches!(
            tc.schema.columns[col_idx].1,
            crate::types::ColumnType::Vector(_)
        ) {
            return Err(Error::SqlExec(format!("列 {} 不是 VECTOR 类型", column)));
        }
        let vec_idx_meta = self.store.open_keyspace(super::vec_idx::VEC_IDX_META_KS)?;
        let meta_key = format!("vidx:{}:{}", table, column);
        let meta_val = serde_json::json!({
            "index_name": index_name,
            "table": table,
            "column": column,
            "metric": metric,
            "m": m,
            "ef_construction": ef_construction,
        });
        let raw = serde_json::to_vec(&meta_val).map_err(|e| Error::Serialization(e.to_string()))?;
        vec_idx_meta.set(meta_key.as_bytes(), &raw)?;
        // 创建 VectorEngine 并回填已有数据
        let vec_name = super::vec_idx::vec_engine_name(table, column);
        let ve = crate::vector::VectorEngine::open(&self.store, &vec_name)?;
        // M86：for_each_kv_prefix 消除 N+1 双重查找
        let mut scan_err: Option<Error> = None;
        tc.data_ks.for_each_kv_prefix(b"", |_key, raw| {
            match tc.schema.decode_row(raw) {
                Ok(row) => {
                    if let Value::Vector(ref vec_data) = row[col_idx] {
                        if let Some(vid) = super::vec_idx::pk_to_vec_id(&row[0]) {
                            if let Err(e) = ve.insert(vid, vec_data) {
                                scan_err = Some(e);
                                return false;
                            }
                        }
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
        // M89：更新缓存标志
        if let Some(tc) = self.cache.get_mut(table) {
            tc.has_vec_indexes = true;
        }
        Ok(vec![])
    }

    /// DROP VECTOR INDEX：删除向量索引元数据和 VectorEngine 数据。
    pub(super) fn exec_drop_vector_index(
        &mut self,
        index_name: &str,
        if_exists: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let ks = self.store.open_keyspace(super::vec_idx::VEC_IDX_META_KS)?;
        let all_keys = ks.keys_with_prefix(b"vidx:")?;
        let mut found = false;
        for key in &all_keys {
            if let Some(raw) = ks.get(key)? {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw) {
                    if val["index_name"].as_str() == Some(index_name) {
                        let table = val["table"].as_str().unwrap_or("");
                        let column = val["column"].as_str().unwrap_or("");
                        ks.delete(key)?;
                        // 清理 VectorEngine keyspace 数据
                        let vec_name = super::vec_idx::vec_engine_name(table, column);
                        let vec_ks_name = format!("vector_{}", vec_name);
                        if let Ok(vec_ks) = self.store.open_keyspace(&vec_ks_name) {
                            truncate_keyspace(&vec_ks, &self.store)?;
                        }
                        if let Some(tc) = self.cache.get_mut(table) {
                            // M89
                            tc.has_vec_indexes =
                                !super::vec_idx::list_vec_indexes(&self.store, table)
                                    .unwrap_or_default()
                                    .is_empty();
                        }
                        found = true;
                        break;
                    }
                }
            }
        }
        if !found && !if_exists {
            return Err(Error::SqlExec(format!("向量索引不存在: {}", index_name)));
        }
        Ok(vec![])
    }

    /// DROP INDEX：删除二级索引元数据和索引 keyspace 数据。
    pub(super) fn exec_drop_index(
        &mut self,
        index_name: &str,
        if_exists: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 扫描 sql_index_meta 找到 index_name 对应的 table:column
        let prefix = b"idx:";
        let all_keys = self.index_meta_ks.keys_with_prefix(prefix)?;
        let mut found_table = None;
        let mut found_col = None;
        let mut found_key = None;
        for key in &all_keys {
            if let Some(val) = self.index_meta_ks.get(key)? {
                let stored_name = String::from_utf8_lossy(&val);
                // M111：剥离 "u:" 前缀后比较索引名
                let actual_name = stored_name.strip_prefix("u:").unwrap_or(&stored_name);
                if actual_name == index_name {
                    // key 格式: idx:{table}:{column}
                    let key_str = String::from_utf8_lossy(key);
                    let parts: Vec<&str> = key_str.splitn(3, ':').collect();
                    if parts.len() == 3 {
                        found_table = Some(parts[1].to_string());
                        found_col = Some(parts[2].to_string());
                        found_key = Some(key.clone());
                    }
                    break;
                }
            }
        }
        let (table, column, meta_key) = match (found_table, found_col, found_key) {
            (Some(t), Some(c), Some(k)) => (t, c, k),
            _ => {
                if if_exists {
                    return Ok(vec![]);
                }
                return Err(Error::SqlExec(format!("索引不存在: {}", index_name)));
            }
        };
        // 删除元数据
        self.index_meta_ks.delete(&meta_key)?;
        // 删除索引 keyspace 数据（M112：cols_key 含逗号需替换为下划线）
        let idx_ks_name = format!("idx_{}_{}", table, column.replace(',', "_"));
        if let Ok(idx_ks) = self.store.open_keyspace(&idx_ks_name) {
            truncate_keyspace(&idx_ks, &self.store)?;
        }
        // 从缓存中移除索引 keyspace
        if let Some(tc) = self.cache.get_mut(&table) {
            tc.index_keyspaces.remove(&column);
            tc.unique_indexes.remove(&column);
        }
        Ok(vec![])
    }

    /// TRUNCATE TABLE：快速清空表数据 + 所有索引 + 向量索引。
    /// 流式收集 key → WriteBatch 批量删除，亿级表内存安全。
    pub(super) fn exec_truncate(&mut self, table: &str) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        // 清空数据 keyspace — 流式收集 key 再批量删除
        truncate_keyspace(&tc.data_ks, &self.store)?;
        // 清空所有二级索引 keyspace
        for idx_ks in tc.index_keyspaces.values() {
            truncate_keyspace(idx_ks, &self.store)?;
        }
        // 清空向量索引数据
        let vec_meta_ks = self.store.open_keyspace(super::vec_idx::VEC_IDX_META_KS)?;
        let prefix = format!("vidx:{}:", table);
        let vidx_keys = vec_meta_ks.keys_with_prefix(prefix.as_bytes())?;
        for key in &vidx_keys {
            if let Some(raw) = vec_meta_ks.get(key)? {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw) {
                    let col = val["column"].as_str().unwrap_or("");
                    let vec_name = super::vec_idx::vec_engine_name(table, col);
                    let vec_ks_name = format!("vector_{}", vec_name);
                    if let Ok(vec_ks) = self.store.open_keyspace(&vec_ks_name) {
                        truncate_keyspace(&vec_ks, &self.store)?;
                    }
                }
            }
        }
        // 清理 SegmentManager 缓存
        let seg_prefix = format!("sql:{}:", table);
        self.segments.remove_prefix(&seg_prefix);
        // M104: TRUNCATE 重置 AUTOINCREMENT 计数器
        let counter_key = format!("autoincr:{}", table);
        let _ = self.meta_ks.delete(counter_key.as_bytes());
        Ok(vec![])
    }

    /// M164：COMMENT ON TABLE/COLUMN — 设置表或列注释，持久化到 schema。
    pub(super) fn exec_comment(
        &mut self,
        table: &str,
        column: Option<&str>,
        text: &str,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let mut schema = (*tc.schema).clone();
        if let Some(col_name) = column {
            // 列级注释：找到物理下标
            let phys_idx = schema
                .columns
                .iter()
                .enumerate()
                .find(|(i, (n, _))| n == col_name && !schema.dropped_columns.contains(i))
                .map(|(i, _)| i)
                .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", col_name)))?;
            // 确保 column_comments 与 columns 等长
            while schema.column_comments.len() < schema.columns.len() {
                schema.column_comments.push(None);
            }
            schema.column_comments[phys_idx] = Some(text.to_string());
        } else {
            schema.table_comment = Some(text.to_string());
        }
        let raw = serde_json::to_vec(&schema).map_err(|e| Error::Serialization(e.to_string()))?;
        self.meta_ks.set(table.as_bytes(), &raw)?;
        if let Some(tc) = self.cache.get_mut(table) {
            tc.schema = Arc::new(schema);
        }
        let seg_key = format!("sql:{}:schema", table);
        self.segments.put(seg_key, raw);
        Ok(vec![])
    }

    /// 当 INSERT 指定了列名时，将值映射到 schema 列顺序，缺失列填 DEFAULT 或 NULL。
    pub(super) fn map_insert_columns(
        &mut self,
        table: &str,
        columns: &[String],
        values: Vec<Vec<Value>>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if columns.is_empty() {
            return Ok(values);
        }
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let schema = &tc.schema;
        let col_indices: Vec<usize> = columns
            .iter()
            .map(|c| {
                schema
                    .column_index_by_name(c)
                    .ok_or_else(|| Error::SqlExec(format!("INSERT 列不存在: {}", c)))
            })
            .collect::<Result<_, _>>()?;
        let vis_count = schema.visible_column_count();
        let mut mapped = Vec::with_capacity(values.len());
        for row in values {
            if row.len() != columns.len() {
                return Err(Error::SqlExec(format!(
                    "INSERT 值数量 {} 与列数量 {} 不一致",
                    row.len(),
                    columns.len()
                )));
            }
            let mut full_row = Vec::with_capacity(vis_count);
            for vi in 0..vis_count {
                if let Some(pos) = col_indices.iter().position(|&ci| ci == vi) {
                    full_row.push(row[pos].clone());
                } else {
                    // 可见下标 → 物理下标，取默认值
                    let phys = schema.visible_to_physical(vi);
                    let def = phys
                        .and_then(|p| schema.column_defaults.get(p))
                        .and_then(|d| d.as_ref())
                        .map(crate::types::resolve_default)
                        .unwrap_or(Value::Null);
                    full_row.push(def);
                }
            }
            mapped.push(full_row);
        }
        Ok(mapped)
    }
}
