/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine DELETE 执行器：单行/索引/全表删除 + PK-first 安全模式。
//! 从 engine_exec.rs 拆分，保持单文件 ≤500 行。

use super::engine::{RowEntry, SqlEngine};
use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::helpers::{row_matches, single_eq_condition};
use super::index_key::{index_scan_prefix, parse_index_pk};
use super::parser::WhereExpr;
use super::topn::extract_indexed_eq;
use crate::types::{Schema, Value};
use crate::Error;

impl SqlEngine {
    pub(super) fn exec_delete(
        &mut self,
        table: &str,
        where_clause: Option<&WhereExpr>,
        returning: Option<&[String]>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M125：视图写保护
        if self.is_view(table)? {
            return Err(Error::SqlExec(format!(
                "视图是只读的，不能 DELETE: {}",
                table
            )));
        }
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("table not found: {}", table)));
        }
        // R-STATS-1: DELETE 使列统计失效
        self.invalidate_stats(table);
        let schema = self.cache.get(table).unwrap().schema.clone();
        let Some(expr) = where_clause else {
            // 无 WHERE：全表删除（含索引维护）
            return self.delete_all(table, &schema, returning);
        };

        if let Some((col, val)) = single_eq_condition(expr) {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("WHERE column not found: {}", col)))?;
            if col_idx == 0 {
                let key = val.to_bytes()?;
                let mut deleted_row: Option<Vec<Value>> = None;
                if let Some(raw) = self.tx_get(table, &key)? {
                    let row = schema.decode_row(&raw)?;
                    // M127：外键约束检查 — 单行 PK 删除
                    self.check_fk_on_delete(table, std::slice::from_ref(&row), &schema)?;
                    // M83：使用 tx_index_delete 缓冲索引删除，保证事务原子性
                    let tc = self.cache.get(table).unwrap();
                    let idx_cols: Vec<(String, Vec<usize>)> = tc
                        .index_keyspaces
                        .keys()
                        .filter_map(|c| resolve_col_indices(&schema, c).map(|ci| (c.clone(), ci)))
                        .collect();
                    for (cols_key, ci) in &idx_cols {
                        let ik = build_idx_key(&row, ci, val)?;
                        self.tx_index_delete(table, cols_key, &ik)?;
                    }
                    let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
                    super::vec_idx::sync_vec_on_delete(
                        &self.store,
                        table,
                        std::slice::from_ref(&row),
                        hv,
                    )?;
                    deleted_row = Some(row);
                }
                self.tx_delete(table, &key)?;
                let rows: Vec<Vec<Value>> = deleted_row.into_iter().collect();
                return Ok(project_returning(returning, &rows, &schema));
            } else if self
                .cache
                .get(table)
                .unwrap()
                .index_keyspaces
                .contains_key(col)
            {
                // M78：单列索引加速 DELETE
                let deleted = self.delete_by_index(table, &schema, col, val, None, returning)?;
                return Ok(deleted);
            } else {
                let deleted = self.delete_matching(table, &schema, expr, returning)?;
                return Ok(deleted);
            }
        } else {
            // M78：AND 多条件索引加速
            let pk_col = schema.columns[0].0.clone();
            let idx_cols: Vec<&str> = self
                .cache
                .get(table)
                .unwrap()
                .index_keyspaces
                .keys()
                .map(|s| s.as_str())
                .collect();
            if let Some((icol, ival, rest)) = extract_indexed_eq(expr, &pk_col, &idx_cols) {
                let ci = schema.column_index_by_name(icol).unwrap();
                if ci == 0 {
                    // PK lookup + filter rest
                    let key = ival.to_bytes()?;
                    if let Some(raw) = self.tx_get(table, &key)? {
                        let row = schema.decode_row(&raw)?;
                        let pass = rest
                            .iter()
                            .all(|e| row_matches(&row, &schema, e).unwrap_or(false));
                        if pass {
                            let deleted =
                                self.delete_rows_ret(table, &schema, &[(key, row)], returning)?;
                            return Ok(deleted);
                        }
                    }
                    return Ok(vec![]);
                } else {
                    let deleted =
                        self.delete_by_index(table, &schema, icol, ival, Some(&rest), returning)?;
                    return Ok(deleted);
                }
            } else {
                let deleted = self.delete_matching(table, &schema, expr, returning)?;
                return Ok(deleted);
            }
        }
    }

    /// M78：通过索引扫描找到候选行并删除，可选剩余条件过滤。
    /// M105：支持 RETURNING 列投影。
    fn delete_by_index(
        &mut self,
        table: &str,
        schema: &Schema,
        col: &str,
        val: &Value,
        rest_filters: Option<&[&WhereExpr]>,
        returning: Option<&[String]>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let targets = self.scan_by_index(table, schema, col, val, rest_filters)?;
        let rows: Vec<Vec<Value>> = targets.iter().map(|(_, r)| r.clone()).collect();
        self.delete_rows(table, schema, &targets)?;
        Ok(project_returning(returning, &rows, schema))
    }

    /// 删除指定行列表（含索引维护+向量同步）。
    /// M79：非事务模式下使用 Batch 合并所有删除为一次提交。
    fn delete_rows(
        &mut self,
        table: &str,
        schema: &Schema,
        targets: &[(Vec<u8>, Vec<Value>)],
    ) -> Result<(), Error> {
        // M127：外键约束检查 — 删除前检查是否被子表引用
        let rows_to_check: Vec<Vec<Value>> = targets.iter().map(|(_, r)| r.clone()).collect();
        if !rows_to_check.is_empty() {
            self.check_fk_on_delete(table, &rows_to_check, schema)?;
        }
        let deleted_rows = rows_to_check;
        if self.tx.is_some() {
            // M82：索引删除缓冲到事务
            let tc = self.cache.get(table).unwrap();
            let idx_cols: Vec<(String, Vec<usize>)> = tc
                .index_keyspaces
                .keys()
                .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
                .collect();
            for (pk_bytes, row) in targets {
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(row, ci, &row[0])?;
                    self.tx_index_delete(table, cols_key, &ik)?;
                }
                self.tx_delete(table, pk_bytes)?;
            }
        } else {
            let tc = self.cache.get(table).unwrap();
            let idx_cols: Vec<(String, Vec<usize>)> = tc
                .index_keyspaces
                .keys()
                .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
                .collect();
            let mut batch = self.store.batch();
            for (pk_bytes, row) in targets {
                for (cols_key, ci) in &idx_cols {
                    if let Some(idx_ks) = tc.index_keyspaces.get(cols_key) {
                        batch.remove(idx_ks, build_idx_key(row, ci, &row[0])?);
                    }
                }
                batch.remove(&tc.data_ks, pk_bytes.clone());
            }
            batch.commit()?;
        }
        let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_delete(&self.store, table, &deleted_rows, hv)?;
        Ok(())
    }

    /// M78：通过索引扫描返回候选行，可选剩余条件过滤。
    /// M83：使用 for_each_key_prefix 流式扫描，消除中间 Vec<Vec<u8>> 分配。
    pub(super) fn scan_by_index(
        &self,
        table: &str,
        schema: &Schema,
        col: &str,
        val: &Value,
        rest: Option<&[&WhereExpr]>,
    ) -> Result<Vec<RowEntry>, Error> {
        let tc = self.cache.get(table).unwrap();
        let idx_ks = tc.index_keyspaces.get(col).unwrap();
        let prefix = index_scan_prefix(val)?;
        let mut result = Vec::new();
        let mut scan_err: Option<Error> = None;
        idx_ks.for_each_key_prefix(&prefix, |pk_key| {
            if let Some(pk_bytes) = parse_index_pk(pk_key) {
                match self.tx_get(table, &pk_bytes) {
                    Ok(Some(raw)) => match schema.decode_row(&raw) {
                        Ok(row) => {
                            if let Some(filters) = rest {
                                if !filters
                                    .iter()
                                    .all(|e| row_matches(&row, schema, e).unwrap_or(false))
                                {
                                    return true;
                                }
                            }
                            result.push((pk_bytes.to_vec(), row));
                        }
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    },
                    Ok(None) => {}
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
            }
            true
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }
        Ok(result)
    }

    /// 范围索引扫描：扫描 `[start, end)` 字节范围内的索引条目。
    ///
    /// `start`/`end` 为有序编码的范围边界，由 `index_key::range_bounds` 或
    /// `index_key::between_bounds` 生成。
    pub(super) fn scan_by_index_range(
        &self,
        table: &str,
        schema: &Schema,
        col: &str,
        start: &[u8],
        end: &[u8],
        rest: Option<&[&WhereExpr]>,
    ) -> Result<Vec<RowEntry>, Error> {
        let tc = self.cache.get(table).unwrap();
        let idx_ks = tc
            .index_keyspaces
            .get(col)
            .ok_or_else(|| Error::SqlExec(format!("索引不存在: {}", col)))?;
        let mut result = Vec::new();
        let mut scan_err: Option<Error> = None;
        idx_ks.for_each_kv_range(start, end, |key, _val| {
            if let Some(pk_bytes) = parse_index_pk(key) {
                match self.tx_get(table, &pk_bytes) {
                    Ok(Some(raw)) => match schema.decode_row(&raw) {
                        Ok(row) => {
                            if let Some(filters) = rest {
                                if !filters
                                    .iter()
                                    .all(|e| row_matches(&row, schema, e).unwrap_or(false))
                                {
                                    return true;
                                }
                            }
                            result.push((pk_bytes.to_vec(), row));
                        }
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    },
                    Ok(None) => {}
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
            }
            true
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }
        Ok(result)
    }

    /// 流式扫描匹配行并删除 — PK-first 两阶段，亿级表内存安全。
    /// M105：支持 RETURNING 列投影。
    fn delete_matching(
        &mut self,
        table: &str,
        schema: &Schema,
        expr: &WhereExpr,
        returning: Option<&[String]>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let pks = self.tx_collect_matching_pks(table, schema, expr)?;
        let deleted = self.delete_rows_by_pks_collect(table, schema, &pks)?;
        Ok(project_returning(returning, &deleted, schema))
    }

    /// 全表删除 — PK-first 两阶段，亿级表内存安全。
    /// M105：支持 RETURNING 列投影。
    fn delete_all(
        &mut self,
        table: &str,
        schema: &Schema,
        returning: Option<&[String]>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let pks = self.tx_collect_all_pks(table)?;
        let deleted = self.delete_rows_by_pks_collect(table, schema, &pks)?;
        Ok(project_returning(returning, &deleted, schema))
    }

    /// M105：按 PK 逐个删除并收集被删除行（RETURNING 用）。
    fn delete_rows_ret(
        &mut self,
        table: &str,
        schema: &Schema,
        targets: &[(Vec<u8>, Vec<Value>)],
        returning: Option<&[String]>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let rows: Vec<Vec<Value>> = targets.iter().map(|(_, r)| r.clone()).collect();
        self.delete_rows(table, schema, targets)?;
        Ok(project_returning(returning, &rows, schema))
    }

    /// PK-first DELETE + 收集被删除行。
    fn delete_rows_by_pks_collect(
        &mut self,
        table: &str,
        schema: &Schema,
        pks: &[Vec<u8>],
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M127：外键约束检查 — 先收集待删除行，检查是否被子表引用
        let mut pre_rows: Vec<Vec<Value>> = Vec::new();
        for pk_bytes in pks {
            let raw = if self.tx.is_some() {
                self.tx_get(table, pk_bytes)?
            } else {
                self.cache
                    .get(table)
                    .and_then(|tc| tc.data_ks.get(pk_bytes).ok())
                    .flatten()
            };
            if let Some(r) = raw {
                pre_rows.push(schema.decode_row(&r)?);
            }
        }
        if !pre_rows.is_empty() {
            self.check_fk_on_delete(table, &pre_rows, schema)?;
        }
        let mut deleted_rows: Vec<Vec<Value>> = Vec::new();
        if self.tx.is_some() {
            let tc = self.cache.get(table).unwrap();
            let idx_cols: Vec<(String, Vec<usize>)> = tc
                .index_keyspaces
                .keys()
                .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
                .collect();
            for pk_bytes in pks {
                let raw = match self.tx_get(table, pk_bytes)? {
                    Some(r) => r,
                    None => continue,
                };
                let row = schema.decode_row(&raw)?;
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, &row[0])?;
                    self.tx_index_delete(table, cols_key, &ik)?;
                }
                self.tx_delete(table, pk_bytes)?;
                deleted_rows.push(row);
            }
        } else {
            let tc = self.cache.get(table).unwrap();
            let idx_cols: Vec<(String, Vec<usize>)> = tc
                .index_keyspaces
                .keys()
                .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
                .collect();
            let mut batch = self.store.batch();
            for pk_bytes in pks {
                let raw = match tc.data_ks.get(pk_bytes)? {
                    Some(r) => r,
                    None => continue,
                };
                let row = schema.decode_row(&raw)?;
                for (cols_key, ci) in &idx_cols {
                    if let Some(idx_ks) = tc.index_keyspaces.get(cols_key) {
                        batch.remove(idx_ks, build_idx_key(&row, ci, &row[0])?);
                    }
                }
                batch.remove(&tc.data_ks, pk_bytes.clone());
                deleted_rows.push(row);
            }
            batch.commit()?;
        }
        let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_delete(&self.store, table, &deleted_rows, hv)?;
        Ok(deleted_rows)
    }
}

/// M105：RETURNING 列投影。无 RETURNING 时返回空；`*` 返回全行；否则按列名投影。
fn project_returning(
    returning: Option<&[String]>,
    rows: &[Vec<Value>],
    schema: &Schema,
) -> Vec<Vec<Value>> {
    let Some(ret_cols) = returning else {
        return vec![];
    };
    if ret_cols.len() == 1 && ret_cols[0] == "*" {
        return rows.to_vec();
    }
    let indices: Vec<usize> = ret_cols
        .iter()
        .filter_map(|c| schema.column_index_by_name(c))
        .collect();
    rows.iter()
        .map(|row| {
            indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}
