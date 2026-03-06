/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 执行器 DDL 函数：CREATE INDEX / ALTER TABLE / SHOW TABLES / DESCRIBE。
//! 从 executor.rs 拆分，保持单文件 ≤500 行。

use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::executor::{get_schema, put_schema, INDEX_META_KEYSPACE, META_KEYSPACE};
use super::index_key::composite_index_entry_key;
use super::parser::AlterAction;
use crate::storage::Store;
use crate::types::Value;
use crate::Error;

fn index_keyspace_name(table: &str, cols_key: &str) -> String {
    format!("idx_{}_{}", table, cols_key.replace(',', "_"))
}

fn table_keyspace(table: &str) -> String {
    format!("sql_{}", table)
}

/// 流式清空 keyspace（无状态路径用）：分批删除（每批 1000），O(1) 内存。
fn truncate_ks(ks: &crate::storage::Keyspace, store: &Store) -> Result<(), Error> {
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

pub(super) fn exec_create_index(
    store: &Store,
    index_name: &str,
    table: &str,
    columns: &[String],
    unique: bool,
) -> Result<Vec<Vec<Value>>, Error> {
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    let meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
    let cols_key = columns.join(",");
    let meta_key = format!("idx:{}:{}", table, cols_key);
    // IF NOT EXISTS: skip silently if index already registered
    if meta.get(meta_key.as_bytes())?.is_some() {
        return Ok(vec![]);
    }
    let col_indices: Vec<usize> = columns
        .iter()
        .map(|c| {
            schema
                .column_index_by_name(c)
                .ok_or_else(|| Error::SqlExec(format!("索引列不存在: {}", c)))
        })
        .collect::<Result<_, _>>()?;
    if col_indices.iter().any(|&i| i == 0) {
        return Err(Error::SqlExec("主键列无需建索引".into()));
    }
    let meta_val = if unique {
        format!("u:{}", index_name)
    } else {
        index_name.to_string()
    };
    meta.set(meta_key.as_bytes(), meta_val.as_bytes())?;
    let idx_ks_name = format!("idx_{}_{}", table, cols_key.replace(',', "_"));
    let idx_ks = store.open_keyspace(&idx_ks_name)?;
    let ks = store.open_keyspace(&table_keyspace(table))?;
    let mut scan_err: Option<Error> = None;
    ks.for_each_kv_prefix(b"", |_key, raw| {
        match schema.decode_row(raw) {
            Ok(row) => {
                let vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
                match composite_index_entry_key(&vals, &row[0]) {
                    Ok(ek) => {
                        if let Err(e) = idx_ks.set(&ek, []) {
                            scan_err = Some(e);
                            return false;
                        }
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
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
    Ok(vec![])
}

pub(super) fn exec_alter_table(
    store: &Store,
    table: &str,
    action: AlterAction,
) -> Result<Vec<Vec<Value>>, Error> {
    let Some(mut schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    match action {
        AlterAction::AddColumn {
            name,
            col_type,
            default,
        } => {
            if schema.column_index_by_name(&name).is_some() {
                return Err(Error::SqlExec(format!("列已存在: {}", name)));
            }
            schema.columns.push((name, col_type));
            schema.version = schema.version.saturating_add(1);
            schema.column_defaults.push(default);
            schema.column_nullable.push(true); // ALTER ADD COLUMN 默认允许 NULL
            put_schema(store, table, &schema)?;
            Ok(vec![])
        }
        AlterAction::DropColumn { name } => {
            let phys_idx = schema
                .columns
                .iter()
                .enumerate()
                .find(|(i, (n, _))| n == &name && !schema.dropped_columns.contains(i))
                .map(|(i, _)| i);
            let phys_idx =
                phys_idx.ok_or_else(|| Error::SqlExec(format!("列不存在或已删除: {}", name)))?;
            if phys_idx == 0 {
                return Err(Error::SqlExec("不允许删除主键列".into()));
            }
            schema.dropped_columns.push(phys_idx);
            schema.version = schema.version.saturating_add(1);
            put_schema(store, table, &schema)?;
            Ok(vec![])
        }
        AlterAction::RenameColumn { old_name, new_name } => {
            let phys_idx = schema
                .columns
                .iter()
                .enumerate()
                .find(|(i, (n, _))| n == &old_name && !schema.dropped_columns.contains(i))
                .map(|(i, _)| i);
            let phys_idx = phys_idx
                .ok_or_else(|| Error::SqlExec(format!("列不存在或已删除: {}", old_name)))?;
            if schema.column_index_by_name(&new_name).is_some() {
                return Err(Error::SqlExec(format!("列名已存在: {}", new_name)));
            }
            schema.columns[phys_idx].0 = new_name.clone();
            schema.version = schema.version.saturating_add(1);
            // 更新二级索引元数据 key（如果有）
            let idx_meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
            let old_meta_key = format!("idx:{}:{}", table, old_name);
            if let Some(idx_name_raw) = idx_meta.get(old_meta_key.as_bytes())? {
                idx_meta.delete(old_meta_key.as_bytes())?;
                let new_meta_key = format!("idx:{}:{}", table, new_name);
                idx_meta.set(new_meta_key.as_bytes(), &idx_name_raw)?;
            }
            put_schema(store, table, &schema)?;
            Ok(vec![])
        }
        AlterAction::RenameTo { new_name } => {
            // 检查新表名是否已存在
            let meta = store.open_keyspace(META_KEYSPACE)?;
            if meta.get(new_name.as_bytes())?.is_some() {
                return Err(Error::SqlExec(format!("目标表已存在: {}", new_name)));
            }
            // 迁移数据 keyspace
            let old_ks = store.open_keyspace(&table_keyspace(table))?;
            let new_ks = store.open_keyspace(&table_keyspace(&new_name))?;
            let mut copy_err: Option<Error> = None;
            old_ks.for_each_kv_prefix(b"", |key, val| {
                if let Err(e) = new_ks.set(key, val) {
                    copy_err = Some(e);
                    return false;
                }
                true
            })?;
            if let Some(e) = copy_err {
                return Err(e);
            }
            truncate_ks(&old_ks, store)?;
            // 迁移二级索引
            let idx_meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
            let prefix = format!("idx:{}:", table);
            let idx_keys = idx_meta.keys_with_prefix(prefix.as_bytes())?;
            for key in &idx_keys {
                let col = String::from_utf8_lossy(key)
                    .strip_prefix(&prefix)
                    .unwrap_or("")
                    .to_string();
                if col.is_empty() {
                    continue;
                }
                if let Some(raw) = idx_meta.get(key)? {
                    idx_meta.delete(key)?;
                    let new_key = format!("idx:{}:{}", new_name, col);
                    idx_meta.set(new_key.as_bytes(), &raw)?;
                }
                let old_idx = store.open_keyspace(&index_keyspace_name(table, &col))?;
                let new_idx = store.open_keyspace(&index_keyspace_name(&new_name, &col))?;
                old_idx.for_each_kv_prefix(b"", |k, v| {
                    if let Err(e) = new_idx.set(k, v) {
                        copy_err = Some(e);
                        return false;
                    }
                    true
                })?;
                if let Some(e) = copy_err {
                    return Err(e);
                }
                truncate_ks(&old_idx, store)?;
            }
            // 更新 schema 元数据
            meta.delete(table.as_bytes())?;
            put_schema(store, &new_name, &schema)?;
            // M104: 迁移 AUTOINCREMENT 计数器
            if schema.auto_increment {
                let old_key = format!("autoincr:{}", table);
                if let Some(raw) = meta.get(old_key.as_bytes())? {
                    let new_key = format!("autoincr:{}", new_name);
                    meta.set(new_key.as_bytes(), &raw)?;
                    meta.delete(old_key.as_bytes())?;
                }
            }
            Ok(vec![])
        }
        // M165：SET DEFAULT / DROP DEFAULT（无状态路径）
        AlterAction::SetDefault { column, value } => {
            let phys_idx = schema
                .columns
                .iter()
                .enumerate()
                .find(|(i, (n, _))| n == &column && !schema.dropped_columns.contains(i))
                .map(|(i, _)| i)
                .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
            while schema.column_defaults.len() < schema.columns.len() {
                schema.column_defaults.push(None);
            }
            schema.column_defaults[phys_idx] = Some(value);
            put_schema(store, table, &schema)?;
            Ok(vec![])
        }
        AlterAction::DropDefault { column } => {
            let phys_idx = schema
                .columns
                .iter()
                .enumerate()
                .find(|(i, (n, _))| n == &column && !schema.dropped_columns.contains(i))
                .map(|(i, _)| i)
                .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
            while schema.column_defaults.len() < schema.columns.len() {
                schema.column_defaults.push(None);
            }
            schema.column_defaults[phys_idx] = None;
            put_schema(store, table, &schema)?;
            Ok(vec![])
        }
        // M169：ALTER TYPE（无状态路径）
        AlterAction::AlterType { column, new_type } => {
            let phys_idx = schema
                .columns
                .iter()
                .enumerate()
                .find(|(i, (n, _))| n == &column && !schema.dropped_columns.contains(i))
                .map(|(i, _)| i)
                .ok_or_else(|| Error::SqlExec(format!("列不存在: {}", column)))?;
            if phys_idx == 0 {
                return Err(Error::SqlExec("不允许修改主键列类型".into()));
            }
            if schema.columns[phys_idx].1 == new_type {
                return Ok(vec![]);
            }
            let ks = store.open_keyspace(&table_keyspace(table))?;
            // 阶段 1：验证 + 收集转换后的行
            let mut convert_err: Option<Error> = None;
            let mut rows_to_update: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
            ks.for_each_kv_prefix(b"", |key, raw| {
                match schema.decode_row(raw) {
                    Ok(mut row) => {
                        match super::engine_ddl::convert_value_pub(&row[phys_idx], &new_type) {
                            Ok(new_val) => {
                                row[phys_idx] = new_val;
                                rows_to_update.push((key.to_vec(), row));
                            }
                            Err(e) => {
                                convert_err = Some(e);
                                return false;
                            }
                        }
                    }
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
            // 阶段 2：更新 schema + 写入
            schema.columns[phys_idx].1 = new_type;
            schema.version = schema.version.saturating_add(1);
            for (pk_bytes, row) in &rows_to_update {
                ks.set(pk_bytes, &schema.encode_row(row)?)?;
            }
            put_schema(store, table, &schema)?;
            Ok(vec![])
        }
    }
}

/// SHOW TABLES（无状态路径）。
/// M125：包含视图，标记类型（TABLE/VIEW）。
pub(super) fn exec_show_tables(store: &Store) -> Result<Vec<Vec<Value>>, Error> {
    let meta = store.open_keyspace(META_KEYSPACE)?;
    let keys = meta.keys_with_prefix(b"")?;
    let mut rows: Vec<Vec<Value>> = keys
        .iter()
        .filter(|k| !k.starts_with(b"autoincr:") && !k.starts_with(b"view:"))
        .map(|k| {
            vec![
                Value::Text(String::from_utf8_lossy(k).to_string()),
                Value::Text("TABLE".to_string()),
            ]
        })
        .collect();
    // M125：追加视图
    let view_prefix = b"view:";
    let view_keys = meta.keys_with_prefix(view_prefix)?;
    for vk in &view_keys {
        if let Some(name) = String::from_utf8_lossy(vk)
            .strip_prefix("view:")
            .map(|s| s.to_string())
        {
            rows.push(vec![Value::Text(name), Value::Text("VIEW".to_string())]);
        }
    }
    rows.sort_by(|a, b| {
        let a_s = match &a[0] {
            Value::Text(s) => s.as_str(),
            _ => "",
        };
        let b_s = match &b[0] {
            Value::Text(s) => s.as_str(),
            _ => "",
        };
        a_s.cmp(b_s)
    });
    Ok(rows)
}

/// SHOW INDEXES [ON table]（无状态路径）。
pub(super) fn exec_show_indexes(
    store: &Store,
    table_filter: Option<&str>,
) -> Result<Vec<Vec<Value>>, Error> {
    let meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
    let prefix = match table_filter {
        Some(t) => format!("idx:{}:", t),
        None => "idx:".to_string(),
    };
    let mut rows = Vec::new();
    meta.for_each_kv_prefix(prefix.as_bytes(), |key, val| {
        let key_str = String::from_utf8_lossy(key).to_string();
        let idx_name = String::from_utf8_lossy(val).to_string();
        if let Some(rest) = key_str.strip_prefix("idx:") {
            if let Some((tbl, col)) = rest.split_once(':') {
                rows.push(vec![
                    Value::Text(idx_name),
                    Value::Text(tbl.to_string()),
                    Value::Text(col.to_string()),
                ]);
            }
        }
        true
    })?;
    Ok(rows)
}

/// DESCRIBE table（无状态路径）。
pub(super) fn exec_describe(store: &Store, table: &str) -> Result<Vec<Vec<Value>>, Error> {
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    let mut rows = Vec::new();
    let mut vis_idx = 0usize;
    for (i, (name, col_type)) in schema.columns.iter().enumerate() {
        if schema.dropped_columns.contains(&i) {
            continue;
        }
        let nullable = schema.column_nullable.get(i).copied().unwrap_or(true);
        let null_str = if nullable { "YES" } else { "NO" };
        let default_val = schema
            .column_defaults
            .get(i)
            .and_then(|d| d.clone())
            .unwrap_or(Value::Null);
        rows.push(vec![
            Value::Text(name.clone()),
            Value::Text(format!("{:?}", col_type)),
            Value::Text(if vis_idx == 0 { "YES" } else { "NO" }.to_string()),
            Value::Text(null_str.to_string()),
            default_val,
        ]);
        vis_idx += 1;
    }
    Ok(rows)
}

/// INSERT 列映射（无状态路径）：指定列名时将值映射到 schema 列顺序，缺失列填 DEFAULT 或 NULL。
pub(super) fn map_insert_columns(
    store: &Store,
    table: &str,
    columns: &[String],
    values: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>, Error> {
    if columns.is_empty() {
        return Ok(values);
    }
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
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

/// UPDATE（无状态路径）。
pub(super) fn exec_update(
    store: &Store,
    table: &str,
    assignments: &[(String, super::parser::SetExpr)],
    where_clause: Option<&super::parser::WhereExpr>,
) -> Result<Vec<Vec<Value>>, Error> {
    use super::executor::{
        get_schema, indexed_columns, row_key, scan_rows, table_keyspace,
    };
    use super::helpers::single_eq_condition;
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    let ks = store.open_keyspace(&table_keyspace(table))?;
    let idx_cols = indexed_columns(store, table)?;
    let targets = if let Some(expr) = where_clause {
        if let Some((col, val)) = single_eq_condition(expr) {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", col)))?;
            if col_idx == 0 {
                let key = row_key(val)?;
                match ks.get(&key)? {
                    Some(raw) => vec![(key, schema.decode_row(&raw)?)],
                    None => vec![],
                }
            } else {
                scan_rows(&ks, &schema, Some(expr))?
            }
        } else {
            scan_rows(&ks, &schema, Some(expr))?
        }
    } else {
        scan_rows(&ks, &schema, None)?
    };
    let mut updated = 0u64;
    let mut old_rows = Vec::new();
    let mut new_rows = Vec::new();
    // M118：预解析 CHECK 约束（非缓存 UPDATE 路径）
    let parsed_checks: Vec<super::parser::WhereExpr> = if !schema.check_constraints.is_empty() {
        schema
            .check_constraints
            .iter()
            .map(|s| super::parser::where_clause::parse_where(s))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };
    // 预解析索引列信息（支持复合索引）
    let idx_info: Vec<(Vec<usize>, crate::storage::Keyspace)> = idx_cols
        .iter()
        .filter_map(|cn| {
            let ci = resolve_col_indices(&schema, cn)?;
            let idx_ks_name = super::executor::index_keyspace_name(table, cn);
            let idx_ks = store.open_keyspace(&idx_ks_name).ok()?;
            Some((ci, idx_ks))
        })
        .collect();
    for (pk_bytes, mut row) in targets {
        let pk = row
            .first()
            .cloned()
            .ok_or_else(|| Error::SqlExec("行为空".into()))?;
        // 删旧索引（支持复合索引）
        for (ci, idx_ks) in &idx_info {
            idx_ks.delete(&build_idx_key(&row, ci, &pk)?)?;
        }
        let old_row = row.clone();
        for (col_name, expr) in assignments {
            let ci = schema
                .column_index_by_name(col_name)
                .ok_or_else(|| Error::SqlExec(format!("UPDATE 列不存在: {}", col_name)))?;
            if ci == 0 {
                return Err(Error::SqlExec("不允许 UPDATE 主键列".into()));
            }
            row[ci] = super::engine_update::resolve_set_expr(expr, &row[ci])?;
        }
        // M118：CHECK 约束校验（非缓存 UPDATE 路径）
        if !parsed_checks.is_empty() {
            super::helpers::validate_check_constraints(
                &row,
                &schema,
                &parsed_checks,
                &schema.check_constraints,
            )?;
        }
        ks.set(&pk_bytes, &schema.encode_row(&row)?)?;
        // 插新索引（支持复合索引）
        for (ci, idx_ks) in &idx_info {
            idx_ks.set(&build_idx_key(&row, ci, &pk)?, [])?;
        }
        old_rows.push(old_row);
        new_rows.push(row);
        updated += 1;
    }
    // 同步向量索引（UPDATE = delete old + insert new）
    super::vec_idx::sync_vec_on_update(store, table, &old_rows, &new_rows, &schema, true)?;
    Ok(vec![vec![Value::Integer(updated as i64)]])
}

/// 删除指定行列表（含索引维护 + 向量同步）。
pub(super) fn delete_rows(
    store: &Store,
    table: &str,
    ks: &crate::storage::Keyspace,
    schema: &crate::types::Schema,
    idx_cols: &[String],
    rows: &[(Vec<u8>, Vec<Value>)],
) -> Result<(), Error> {
    let deleted_rows: Vec<Vec<Value>> = rows.iter().map(|(_, r)| r.clone()).collect();
    for (pk_bytes, row) in rows {
        for cn in idx_cols {
            if let Some(ci) = resolve_col_indices(schema, cn) {
                let idx_ks_name = super::executor::index_keyspace_name(table, cn);
                if let Ok(idx_ks) = store.open_keyspace(&idx_ks_name) {
                    idx_ks.delete(&build_idx_key(row, &ci, &row[0])?)?;
                }
            }
        }
        ks.delete(pk_bytes)?;
    }
    super::vec_idx::sync_vec_on_delete(store, table, &deleted_rows, true)?;
    Ok(())
}

/// INSERT ... ON CONFLICT DO UPDATE SET ... 非缓存路径实现。
pub(super) fn exec_insert_on_conflict(
    store: &Store,
    table: &str,
    ks: &crate::storage::Keyspace,
    schema: &crate::types::Schema,
    idx_cols: &[String],
    values: Vec<Vec<Value>>,
    oc: &super::parser::OnConflict,
) -> Result<Vec<Vec<Value>>, Error> {
    use super::executor::row_key;
    // 预解析索引列信息（支持复合索引）
    let idx_info: Vec<(String, Vec<usize>, crate::storage::Keyspace)> = idx_cols
        .iter()
        .filter_map(|cn| {
            let ci = resolve_col_indices(schema, cn)?;
            let idx_ks_name = super::executor::index_keyspace_name(table, cn);
            let idx_ks = store.open_keyspace(&idx_ks_name).ok()?;
            Some((cn.clone(), ci, idx_ks))
        })
        .collect();
    let mut inserted_rows = Vec::new();
    let mut old_rows = Vec::new();
    let mut new_rows = Vec::new();
    for new_row in &values {
        schema.validate_row(new_row)?;
        let pk = new_row
            .first()
            .ok_or_else(|| Error::SqlExec("INSERT 行为空".into()))?;
        let key = row_key(pk)?;
        if let Some(old_raw) = ks.get(&key)? {
            let mut row = schema.decode_row(&old_raw)?;
            // 删旧索引（支持复合索引）
            for (_, ci, idx_ks) in &idx_info {
                idx_ks.delete(&build_idx_key(&row, ci, pk)?)?;
            }
            let old_row = row.clone();
            for (col_name, oc_val) in &oc.assignments {
                let ci = schema.column_index_by_name(col_name).ok_or_else(|| {
                    Error::SqlExec(format!("ON CONFLICT SET 列不存在: {}", col_name))
                })?;
                if ci == 0 {
                    return Err(Error::SqlExec("不允许更新主键列".into()));
                }
                row[ci] = resolve_oc_val(oc_val, new_row, schema)?;
            }
            ks.set(&key, &schema.encode_row(&row)?)?;
            // 插新索引（支持复合索引）
            for (_, ci, idx_ks) in &idx_info {
                idx_ks.set(&build_idx_key(&row, ci, pk)?, [])?;
            }
            old_rows.push(old_row);
            new_rows.push(row);
        } else {
            ks.set(&key, &schema.encode_row(new_row)?)?;
            // 插新索引（支持复合索引）
            for (_, ci, idx_ks) in &idx_info {
                idx_ks.set(&build_idx_key(new_row, ci, pk)?, [])?;
            }
            inserted_rows.push(new_row.clone());
        }
    }
    if !inserted_rows.is_empty() {
        super::vec_idx::sync_vec_on_insert(store, table, &inserted_rows, schema, true)?;
    }
    if !old_rows.is_empty() {
        super::vec_idx::sync_vec_on_update(store, table, &old_rows, &new_rows, schema, true)?;
    }
    Ok(vec![])
}

/// 解析 ON CONFLICT SET 赋值值。
fn resolve_oc_val(
    oc_val: &super::parser::OnConflictValue,
    new_row: &[Value],
    schema: &crate::types::Schema,
) -> Result<Value, Error> {
    match oc_val {
        super::parser::OnConflictValue::Excluded(col) => {
            let ci = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("EXCLUDED.{} 列不存在", col)))?;
            Ok(new_row[ci].clone())
        }
        super::parser::OnConflictValue::Literal(v) => Ok(v.clone()),
    }
}
