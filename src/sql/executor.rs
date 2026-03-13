/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 执行器：基于 storage 的 CRUD（无缓存路径）。
//! 表数据 keyspace = sql_{table}，主键为第一列。

use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::helpers::{row_matches, single_eq_condition};
use super::index_key::{index_entry_key, index_scan_prefix, parse_index_pk};
use super::parser::{Stmt, WhereExpr};
use super::planner::Plan;
use crate::storage::Store;
use crate::types::{Schema, Value};
use crate::Error;

pub(super) const META_KEYSPACE: &str = "sql_meta";
pub(super) const INDEX_META_KEYSPACE: &str = "sql_index_meta";

type RowEntry = (Vec<u8>, Vec<Value>);

pub(super) fn table_keyspace(table: &str) -> String {
    format!("sql_{}", table)
}
pub(super) fn index_keyspace_name(table: &str, cols_key: &str) -> String {
    format!("idx_{}_{}", table, cols_key.replace(',', "_"))
}
pub(super) fn get_schema(store: &Store, table: &str) -> Result<Option<Schema>, Error> {
    let meta = store.open_keyspace(META_KEYSPACE)?;
    let raw = meta.get(table.as_bytes())?;
    match raw {
        Some(b) => {
            let mut schema: Schema =
                serde_json::from_slice(&b).map_err(|e| Error::Serialization(e.to_string()))?;
            schema.ensure_defaults();
            Ok(Some(schema))
        }
        None => Ok(None),
    }
}

pub(super) fn put_schema(store: &Store, table: &str, schema: &Schema) -> Result<(), Error> {
    let meta = store.open_keyspace(META_KEYSPACE)?;
    let raw = serde_json::to_vec(schema).map_err(|e| Error::Serialization(e.to_string()))?;
    meta.set(table.as_bytes(), &raw)?;
    Ok(())
}
pub(super) fn row_key(pk: &Value) -> Result<Vec<u8>, Error> {
    pk.to_bytes()
}
pub(super) fn has_index(store: &Store, table: &str, column: &str) -> Result<bool, Error> {
    let meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
    meta.contains_key(format!("idx:{}:{}", table, column).as_bytes())
}
pub(super) fn index_insert(
    store: &Store,
    table: &str,
    column: &str,
    col_val: &Value,
    pk: &Value,
) -> Result<(), Error> {
    let ks = store.open_keyspace(&index_keyspace_name(table, column))?;
    ks.set(&index_entry_key(col_val, pk)?, [])
}
pub(super) fn index_delete(
    store: &Store,
    table: &str,
    column: &str,
    col_val: &Value,
    pk: &Value,
) -> Result<(), Error> {
    let ks = store.open_keyspace(&index_keyspace_name(table, column))?;
    ks.delete(&index_entry_key(col_val, pk)?)
}
pub(super) fn index_lookup(
    store: &Store,
    table: &str,
    column: &str,
    col_val: &Value,
) -> Result<Vec<Vec<u8>>, Error> {
    let ks = store.open_keyspace(&index_keyspace_name(table, column))?;
    let prefix = index_scan_prefix(col_val)?;
    let keys = ks.keys_with_prefix(&prefix)?;
    Ok(keys.iter().filter_map(|key| parse_index_pk(key)).collect())
}
pub(super) fn indexed_columns(store: &Store, table: &str) -> Result<Vec<String>, Error> {
    let meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
    let prefix = format!("idx:{}:", table);
    let keys = meta.keys_with_prefix(prefix.as_bytes())?;
    Ok(keys
        .iter()
        .filter_map(|key| {
            String::from_utf8_lossy(key)
                .strip_prefix(&prefix)
                .map(|s| s.to_string())
        })
        .collect())
}
/// 全表扫描，可选条件过滤，返回 (key, row) 列表。
/// M69：流式迭代，O(1) key 内存，仅收集匹配行。
/// M86：for_each_kv_prefix 消除 key+get N+1 双重查找。
pub(super) fn scan_rows(
    ks: &crate::storage::Keyspace,
    schema: &Schema,
    filter: Option<&WhereExpr>,
) -> Result<Vec<RowEntry>, Error> {
    let mut result = Vec::new();
    let mut last_err: Option<Error> = None;
    ks.for_each_kv_prefix(b"", |key, raw| {
        match schema.decode_row(raw) {
            Ok(row) => {
                if let Some(expr) = filter {
                    match row_matches(&row, schema, expr) {
                        Ok(true) => result.push((key.to_vec(), row)),
                        Ok(false) => {}
                        Err(e) => {
                            last_err = Some(e);
                            return false;
                        }
                    }
                } else {
                    result.push((key.to_vec(), row));
                }
            }
            Err(e) => {
                last_err = Some(e);
                return false;
            }
        }
        true
    })?;
    if let Some(e) = last_err {
        return Err(e);
    }
    Ok(result)
}
/// 执行计划，返回结果行。
pub fn execute(store: &Store, plan: Plan) -> Result<Vec<Vec<Value>>, Error> {
    match plan.stmt {
        Stmt::Union { .. } => Err(Error::SqlExec("UNION 仅在缓存路径支持".into())),
        Stmt::CreateTable {
            name,
            columns,
            if_not_exists,
            unique_constraints,
            check_constraints,
            temporary: _,
            foreign_keys,
        } => {
            if if_not_exists {
                let meta = store.open_keyspace(META_KEYSPACE)?;
                if meta.contains_key(name.as_bytes())? {
                    return Ok(vec![]);
                }
            }
            let cols = columns
                .iter()
                .map(|c| (c.name.clone(), c.col_type.clone()))
                .collect();
            let column_nullable = columns.iter().map(|c| c.nullable).collect();
            let column_defaults = columns.iter().map(|c| c.default_value.clone()).collect();
            let schema = Schema {
                columns: cols,
                version: 0,
                column_defaults,
                column_nullable,
                dropped_columns: vec![],
                unique_constraints,
                auto_increment: columns.first().is_some_and(|c| c.auto_increment),
                check_constraints,
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
            put_schema(store, &name, &schema)?;
            let _ = store.open_keyspace(&table_keyspace(&name))?;
            Ok(vec![])
        }
        Stmt::DropTable { name, if_exists } => {
            let meta = store.open_keyspace(META_KEYSPACE)?;
            if !meta.contains_key(name.as_bytes())? {
                if if_exists {
                    return Ok(vec![]);
                }
                return Err(Error::SqlExec(format!("表不存在: {}", name)));
            }
            meta.delete(name.as_bytes())?;
            // M104: 清理 AUTOINCREMENT 计数器
            let counter_key = format!("autoincr:{}", name);
            let _ = meta.delete(counter_key.as_bytes());
            let idx_meta = store.open_keyspace(INDEX_META_KEYSPACE)?;
            let prefix = format!("idx:{}:", name);
            for k in &idx_meta.keys_with_prefix(prefix.as_bytes())? {
                idx_meta.delete(k)?;
            }
            // 级联清理向量索引
            super::vec_idx::drop_vec_indexes_for_table(store, &name)?;
            Ok(vec![])
        }
        Stmt::Insert {
            table,
            columns,
            values,
            or_replace,
            or_ignore,
            on_conflict,
            returning: _,
            source_select,
        } => {
            // M102: INSERT INTO ... SELECT — 先执行 SELECT 获取行数据
            let values = if let Some(sel) = source_select {
                let rows = execute(store, super::planner::plan(*sel))?;
                if !columns.is_empty() {
                    super::executor_ddl::map_insert_columns(store, &table, &columns, rows)?
                } else {
                    rows
                }
            } else {
                super::executor_ddl::map_insert_columns(store, &table, &columns, values)?
            };
            exec_insert(
                store,
                &table,
                values,
                or_replace,
                or_ignore,
                on_conflict.as_ref(),
            )
        }
        Stmt::Select {
            table,
            columns,
            where_clause,
            order_by,
            limit,
            offset,
            distinct,
            vec_search,
            geo_search: _,
            join: _,
            group_by: _,
            having: _,
            ctes: _,
            window_functions: _,
            distinct_on: _,
        } => super::executor_select::exec_select(
            store,
            &table,
            &columns,
            where_clause.as_ref(),
            order_by.as_deref(),
            limit,
            offset,
            distinct,
            vec_search.as_ref(),
        ),
        Stmt::Delete {
            table,
            where_clause,
            returning: _,
            using_table: _,
        } => exec_delete(store, &table, where_clause.as_ref()),
        Stmt::Update {
            table,
            assignments,
            where_clause,
            returning: _,
            from_table: _,
            order_by: _,
            limit: _,
        } => super::executor_ddl::exec_update(store, &table, &assignments, where_clause.as_ref()),
        Stmt::CreateIndex {
            index_name,
            table,
            columns,
            unique,
        } => super::executor_ddl::exec_create_index(store, &index_name, &table, &columns, unique),
        Stmt::Begin | Stmt::Commit | Stmt::Rollback => {
            Err(Error::SqlExec("事务语句仅在 SqlEngine 中支持".into()))
        }
        Stmt::Savepoint { .. } | Stmt::Release { .. } | Stmt::RollbackTo { .. } => {
            Err(Error::SqlExec("SAVEPOINT 仅在 SqlEngine 中支持".into()))
        }
        Stmt::AlterTable { table, action } => {
            super::executor_ddl::exec_alter_table(store, &table, action)
        }
        Stmt::ShowTables => super::executor_ddl::exec_show_tables(store),
        Stmt::ShowIndexes { table } => {
            super::executor_ddl::exec_show_indexes(store, table.as_deref())
        }
        Stmt::Describe { table } => super::executor_ddl::exec_describe(store, &table),
        Stmt::CreateVectorIndex { .. } => Err(Error::SqlExec(
            "CREATE VECTOR INDEX 仅在 SqlEngine 中支持".into(),
        )),
        Stmt::DropVectorIndex { .. } => Err(Error::SqlExec(
            "DROP VECTOR INDEX 仅在 SqlEngine 中支持".into(),
        )),
        Stmt::DropIndex { .. } => Err(Error::SqlExec("DROP INDEX 仅在 SqlEngine 中支持".into())),
        Stmt::Truncate { .. } => Err(Error::SqlExec("TRUNCATE 仅在 SqlEngine 中支持".into())),
        Stmt::Explain { .. } => Err(Error::SqlExec("EXPLAIN 仅在 SqlEngine 中支持".into())),
        Stmt::CreateView { .. } => Err(Error::SqlExec("CREATE VIEW 仅在 SqlEngine 中支持".into())),
        Stmt::DropView { .. } => Err(Error::SqlExec("DROP VIEW 仅在 SqlEngine 中支持".into())),
        Stmt::Comment { .. } => Err(Error::SqlExec("COMMENT ON 仅在 SqlEngine 中支持".into())),
        Stmt::Analyze { .. } => Err(Error::SqlExec("ANALYZE 仅在 SqlEngine 中支持".into())),
    }
}
fn exec_insert(
    store: &Store,
    table: &str,
    mut values: Vec<Vec<Value>>,
    or_replace: bool,
    or_ignore: bool,
    on_conflict: Option<&super::parser::OnConflict>,
) -> Result<Vec<Vec<Value>>, Error> {
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    let ks = store.open_keyspace(&table_keyspace(table))?;
    let idx_cols = indexed_columns(store, table)?;
    // M104: AUTOINCREMENT — 自动分配主键（stateless 路径）
    if schema.auto_increment {
        let meta = store.open_keyspace(META_KEYSPACE)?;
        let counter_key = format!("autoincr:{}", table);
        let mut counter: i64 = meta
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
        meta.set(counter_key.as_bytes(), &counter.to_be_bytes())?;
    }
    // 隐式类型转换（Text → Jsonb 等）
    for row in &mut values {
        schema.coerce_types(row);
    }
    if let Some(oc) = on_conflict {
        return super::executor_ddl::exec_insert_on_conflict(
            store, table, &ks, &schema, &idx_cols, values, oc,
        );
    }
    if values.len() == 1 && on_conflict.is_none() {
        let row = &values[0];
        schema.validate_row(row)?;
        // M118：CHECK 约束校验（非缓存单行路径）
        if !schema.check_constraints.is_empty() {
            let parsed: Vec<_> = schema
                .check_constraints
                .iter()
                .map(|s| super::parser::where_clause::parse_where(s))
                .collect::<Result<Vec<_>, _>>()?;
            super::helpers::validate_check_constraints(
                row,
                &schema,
                &parsed,
                &schema.check_constraints,
            )?;
        }
        let pk = row
            .first()
            .ok_or_else(|| Error::SqlExec("INSERT 行为空".into()))?;
        let key = row_key(pk)?;
        if or_replace {
            // 替换模式：删除旧行的二级索引条目
            if let Some(old_raw) = ks.get(&key)? {
                let old_row = schema.decode_row(&old_raw)?;
                let old_pk = old_row.first().unwrap();
                for col in &idx_cols {
                    let ci = schema.column_index_by_name(col).unwrap_or(usize::MAX);
                    if ci < old_row.len() {
                        let _ = index_delete(store, table, col, &old_row[ci], old_pk);
                    }
                }
            }
        } else if ks.get(&key)?.is_some() {
            if or_ignore {
                return Ok(vec![]);
            }
            return Err(Error::SqlExec(format!("主键重复: {:?}", pk)));
        }
        ks.set(&key, &schema.encode_row(row)?)?;
        // 维护二级索引：为新行插入索引条目
        for col in &idx_cols {
            let ci = schema.column_index_by_name(col).unwrap_or(usize::MAX);
            if ci < row.len() {
                index_insert(store, table, col, &row[ci], pk)?;
            }
        }
        // 同步向量索引
        super::vec_idx::sync_vec_on_insert(store, table, &values, &schema, true)?;
        return Ok(vec![]);
    }
    let idx_keyspaces: Vec<(Vec<usize>, crate::storage::Keyspace)> = idx_cols
        .iter()
        .filter_map(|col_name| {
            let ci = resolve_col_indices(&schema, col_name)?;
            let idx_ks = store
                .open_keyspace(&index_keyspace_name(table, col_name))
                .ok()?;
            Some((ci, idx_ks))
        })
        .collect();
    let mut batch = store.batch();
    let mut seen_keys = std::collections::HashSet::new();
    let mut inserted_rows = Vec::new();
    // M118：预解析 CHECK 约束（非缓存批量路径）
    let parsed_checks: Vec<super::parser::WhereExpr> = if !schema.check_constraints.is_empty() {
        schema
            .check_constraints
            .iter()
            .map(|s| super::parser::where_clause::parse_where(s))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };
    for row in values {
        schema.validate_row(&row)?;
        // M118：CHECK 约束校验
        if !parsed_checks.is_empty() {
            super::helpers::validate_check_constraints(
                &row,
                &schema,
                &parsed_checks,
                &schema.check_constraints,
            )?;
        }
        let pk = row
            .first()
            .ok_or_else(|| Error::SqlExec("INSERT 行为空".into()))?;
        let key = row_key(pk)?;
        if or_replace {
            // or_replace：删旧行索引
            if let Some(old_raw) = ks.get(&key)? {
                let old_row = schema.decode_row(&old_raw)?;
                let old_pk = old_row.first().unwrap();
                for (ci, idx_ks) in &idx_keyspaces {
                    batch.remove(idx_ks, build_idx_key(&old_row, ci, old_pk)?);
                }
            }
            seen_keys.insert(key.clone());
        } else if ks.get(&key)?.is_some() || !seen_keys.insert(key.clone()) {
            if or_ignore {
                continue; // 静默跳过冲突行
            }
            return Err(Error::SqlExec(format!("主键重复: {:?}", pk)));
        }
        let raw = schema.encode_row(&row)?;
        batch.insert(&ks, key.clone(), raw)?;
        for (ci, idx_ks) in &idx_keyspaces {
            batch.insert(idx_ks, build_idx_key(&row, ci, pk)?, Vec::new())?;
        }
        inserted_rows.push(row);
    }
    batch.commit()?;
    // 同步向量索引
    super::vec_idx::sync_vec_on_insert(store, table, &inserted_rows, &schema, true)?;
    Ok(vec![])
}
fn exec_delete(
    store: &Store,
    table: &str,
    where_clause: Option<&WhereExpr>,
) -> Result<Vec<Vec<Value>>, Error> {
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    let ks = store.open_keyspace(&table_keyspace(table))?;
    let idx_cols = indexed_columns(store, table)?;
    if let Some(expr) = where_clause {
        if let Some((col, val)) = single_eq_condition(expr) {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", col)))?;
            if col_idx == 0 {
                let key = row_key(val)?;
                if let Some(raw) = ks.get(&key)? {
                    let row = schema.decode_row(&raw)?;
                    if !idx_cols.is_empty() {
                        super::executor_ddl::delete_rows(
                            store,
                            table,
                            &ks,
                            &schema,
                            &idx_cols,
                            &[(key, row)],
                        )?;
                    } else {
                        // 无二级索引路径：单独同步向量删除
                        super::vec_idx::sync_vec_on_delete(
                            store,
                            table,
                            std::slice::from_ref(&row),
                            true,
                        )?;
                        ks.delete(&key)?;
                    }
                } else {
                    ks.delete(&key)?;
                }
            } else {
                delete_matching(store, table, &ks, &schema, &idx_cols, expr)?;
            }
        } else {
            delete_matching(store, table, &ks, &schema, &idx_cols, expr)?;
        }
    } else {
        // 无 WHERE：全表删除（含索引维护）
        let all = scan_rows(&ks, &schema, None)?;
        super::executor_ddl::delete_rows(store, table, &ks, &schema, &idx_cols, &all)?;
    }
    Ok(vec![])
}

fn delete_matching(
    store: &Store,
    table: &str,
    ks: &crate::storage::Keyspace,
    schema: &Schema,
    idx_cols: &[String],
    expr: &WhereExpr,
) -> Result<(), Error> {
    // 尝试通过 split_conjunction 拆分 AND 条件，查找可索引加速的谓词
    let predicates = super::optimizer::split_conjunction(expr);
    for (pred_idx, pred) in predicates.iter().enumerate() {
        if let Some((col, val)) = single_eq_condition(pred) {
            let ci = schema.column_index_by_name(col).unwrap_or(usize::MAX);
            if ci == 0 {
                // 主键等值条件：精确定位删除
                let key = row_key(val)?;
                if let Some(raw) = ks.get(&key)? {
                    let row = schema.decode_row(&raw)?;
                    // 验证剩余条件
                    let rest_match = predicates
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != pred_idx)
                        .all(|(_, p)| row_matches(&row, schema, p).unwrap_or(false));
                    if rest_match {
                        super::executor_ddl::delete_rows(
                            store,
                            table,
                            ks,
                            schema,
                            idx_cols,
                            &[(key, row)],
                        )?;
                    }
                }
                return Ok(());
            } else if has_index(store, table, col)? {
                // 二级索引等值条件：索引查找 + 过滤
                let mut targets = Vec::new();
                for pk_bytes in index_lookup(store, table, col, val)? {
                    if let Some(raw) = ks.get(&pk_bytes)? {
                        let row = schema.decode_row(&raw)?;
                        let rest_match = predicates
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != pred_idx)
                            .all(|(_, p)| row_matches(&row, schema, p).unwrap_or(false));
                        if rest_match {
                            targets.push((pk_bytes, row));
                        }
                    }
                }
                super::executor_ddl::delete_rows(store, table, ks, schema, idx_cols, &targets)?;
                return Ok(());
            }
        }
    }
    // 无可用索引：回退全表扫描
    let targets = scan_rows(ks, schema, Some(expr))?;
    super::executor_ddl::delete_rows(store, table, ks, schema, idx_cols, &targets)
}
