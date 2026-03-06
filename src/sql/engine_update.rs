/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine UPDATE 执行器（从 engine_exec.rs 拆分，保持 ≤500 行）。

use super::engine::{RowEntry, SqlEngine};
use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::helpers::{row_matches, single_eq_condition};
use super::parser::{ArithOp, SetExpr, WhereExpr};
use super::topn::extract_indexed_eq;
use crate::types::{Schema, Value};
use crate::Error;

impl SqlEngine {
    pub(super) fn exec_update(
        &mut self,
        table: &str,
        assignments: &[(String, SetExpr)],
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M125：视图写保护
        if self.is_view(table)? {
            return Err(Error::SqlExec(format!(
                "视图是只读的，不能 UPDATE: {}",
                table
            )));
        }
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        // R-STATS-1: UPDATE 使列统计失效
        self.invalidate_stats(table);
        let schema = self.cache.get(table).unwrap().schema.clone();
        // M117: ORDER BY / LIMIT 需要全量收集+排序，走专用路径
        let has_order = order_by.is_some() || limit.is_some();
        if has_order {
            return self.exec_update_ordered(
                table,
                &schema,
                assignments,
                where_clause,
                order_by,
                limit,
            );
        }
        let targets = if let Some(expr) = where_clause {
            if let Some((col, val)) = single_eq_condition(expr) {
                let col_idx = schema
                    .column_index_by_name(col)
                    .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", col)))?;
                if col_idx == 0 {
                    let key = val.to_bytes()?;
                    match self.tx_get(table, &key)? {
                        Some(raw) => vec![(key, schema.decode_row(&raw)?)],
                        None => vec![],
                    }
                } else if self
                    .cache
                    .get(table)
                    .unwrap()
                    .index_keyspaces
                    .contains_key(col)
                {
                    // M78：单列索引加速 UPDATE
                    self.scan_by_index(table, &schema, col, val, None)?
                } else {
                    // 流式 PK 收集 → 按 PK 逐个 get+decode（避免全表行加载到内存）
                    let pks = self.tx_collect_matching_pks(table, &schema, expr)?;
                    return self.apply_updates_by_pks(table, &schema, assignments, pks);
                }
            } else {
                // M78：AND 多条件索引加速 UPDATE
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
                        let key = ival.to_bytes()?;
                        match self.tx_get(table, &key)? {
                            Some(raw) => {
                                let row = schema.decode_row(&raw)?;
                                let pass = rest
                                    .iter()
                                    .all(|e| row_matches(&row, &schema, e).unwrap_or(false));
                                if pass {
                                    vec![(key, row)]
                                } else {
                                    vec![]
                                }
                            }
                            None => vec![],
                        }
                    } else {
                        self.scan_by_index(table, &schema, icol, ival, Some(&rest))?
                    }
                } else {
                    // 流式 PK 收集 → 按 PK 逐个 get+decode
                    let pks = self.tx_collect_matching_pks(table, &schema, expr)?;
                    return self.apply_updates_by_pks(table, &schema, assignments, pks);
                }
            }
        } else {
            // 无 WHERE：流式收集全表 PK → 按 PK 逐个 get+decode
            let pks = self.tx_collect_all_pks(table)?;
            return self.apply_updates_by_pks(table, &schema, assignments, pks);
        };
        self.apply_updates(table, &schema, assignments, targets)
    }

    /// M117: UPDATE ... ORDER BY ... LIMIT n — 排序后限制更新行数。
    ///
    /// 全量扫描匹配行 → 排序 → 截断 → apply_updates。
    fn exec_update_ordered(
        &mut self,
        table: &str,
        schema: &Schema,
        assignments: &[(String, SetExpr)],
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        use super::helpers::value_cmp;
        // 全量扫描收集匹配行
        // 使用事务感知扫描，确保事务内的未提交写入可见
        let all_rows = self.tx_scan_all(table)?;
        let early_limit = if order_by.is_none() { limit } else { None };
        let mut targets: Vec<RowEntry> = Vec::new();
        for (pk, row) in all_rows {
            let pass = where_clause
                .map(|expr| row_matches(&row, schema, expr).unwrap_or(false))
                .unwrap_or(true);
            if pass {
                targets.push((pk, row));
                if let Some(n) = early_limit {
                    if targets.len() >= n as usize {
                        break;
                    }
                }
            }
        }
        // 排序
        if let Some(ob) = order_by {
            let col_indices: Vec<(usize, bool)> = ob
                .iter()
                .map(|(col, desc, _)| {
                    schema
                        .column_index_by_name(col)
                        .map(|i| (i, *desc))
                        .ok_or_else(|| Error::SqlExec(format!("ORDER BY 列不存在: {}", col)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !col_indices.is_empty() {
                targets.sort_by(|a, b| {
                    for &(idx, desc) in &col_indices {
                        let cmp =
                            value_cmp(&a.1[idx], &b.1[idx]).unwrap_or(std::cmp::Ordering::Equal);
                        let cmp = if desc { cmp.reverse() } else { cmp };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        // 截断
        if let Some(n) = limit {
            targets.truncate(n as usize);
        }
        self.apply_updates(table, schema, assignments, targets)
    }

    /// 对目标行应用 UPDATE 赋值（含索引维护+向量同步）。
    /// M79：非事务模式下使用 Batch 合并所有写操作为一次提交。
    fn apply_updates(
        &mut self,
        table: &str,
        schema: &Schema,
        assignments: &[(String, SetExpr)],
        targets: Vec<RowEntry>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 预先验证赋值列，禁止 UPDATE 主键
        let assign_indexes: Vec<(usize, &SetExpr)> = assignments
            .iter()
            .map(|(col_name, expr)| {
                let ci = schema
                    .column_index_by_name(col_name)
                    .ok_or_else(|| Error::SqlExec(format!("UPDATE 列不存在: {}", col_name)));
                ci.map(|ci| {
                    if ci == 0 {
                        Err(Error::SqlExec("不允许 UPDATE 主键列".into()))
                    } else {
                        Ok((ci, expr))
                    }
                })
            })
            .collect::<Result<Vec<Result<_, _>>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let mut old_rows = Vec::with_capacity(targets.len());
        let mut new_rows = Vec::with_capacity(targets.len());

        if self.tx.is_some() {
            // M82：事务模式 — 索引写入缓冲到事务，COMMIT 时统一刷出
            let tc = self.cache.get(table).unwrap();
            let idx_cols: Vec<(String, Vec<usize>)> = tc
                .index_keyspaces
                .keys()
                .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
                .collect();
            // M111：预收集唯一索引列名
            let unique_cols: Vec<String> = tc.unique_indexes.iter().cloned().collect();
            let _ = tc;
            for (pk_bytes, mut row) in targets {
                let pk = row[0].clone();
                // 删除旧索引（缓冲）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, &pk)?;
                    self.tx_index_delete(table, cols_key, &ik)?;
                }
                let old_row = row.clone();
                for &(ci, expr) in &assign_indexes {
                    row[ci] = resolve_set_expr(expr, &row[ci])?;
                }
                // M118：CHECK 约束校验（事务 UPDATE 路径）
                let tc_chk = self.cache.get(table).unwrap();
                if !tc_chk.parsed_checks.is_empty() {
                    super::helpers::validate_check_constraints(
                        &row,
                        schema,
                        &tc_chk.parsed_checks,
                        &schema.check_constraints,
                    )?;
                }
                // M127：外键约束校验（事务 UPDATE 路径）
                // 子表 FK 列变更 → 新值必须在父表存在
                self.check_fk_on_insert(table, &row, schema)?;
                // 父表被引用列变更 → 旧值不能被子表引用
                self.check_fk_on_delete(table, std::slice::from_ref(&old_row), schema)?;
                // M111：UPDATE 唯一索引检查（事务路径）— M112：复合索引
                for uc in &unique_cols {
                    if let Some(col_indices) = resolve_col_indices(schema, uc) {
                        let old_vals: Vec<&Value> =
                            col_indices.iter().map(|&i| &old_row[i]).collect();
                        let new_vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
                        if old_vals != new_vals {
                            let tc = self.cache.get(table).unwrap();
                            if let Some(idx_ks) = tc.index_keyspaces.get(uc) {
                                let tx_writes =
                                    self.tx.as_ref().map(|tx| tx.index_writes.as_slice());
                                let result = if let Some(writes) = tx_writes {
                                    super::index_key::check_unique_violation_tx_composite(
                                        idx_ks,
                                        writes,
                                        table,
                                        uc,
                                        &new_vals,
                                        Some(&pk_bytes),
                                    )
                                } else {
                                    super::index_key::check_unique_violation_composite(
                                        idx_ks,
                                        &new_vals,
                                        Some(&pk_bytes),
                                    )
                                };
                                result?;
                            }
                        }
                    }
                }
                let new_raw = schema.encode_row(&row)?;
                self.tx_set(table, pk_bytes, new_raw)?;
                // 插入新索引（缓冲）
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, &pk)?;
                    self.tx_index_set(table, cols_key, ik)?;
                }
                old_rows.push(old_row);
                new_rows.push(row);
            }
        } else {
            // 非事务模式：收集所有变更，一次 Batch commit — M112：复合索引
            // M127：外键约束预检查（非事务 UPDATE 路径）— 在 batch 循环前完成
            if !schema.foreign_keys.is_empty() {
                for (_, row) in &targets {
                    let mut new_row = row.clone();
                    for &(ci, expr) in &assign_indexes {
                        new_row[ci] = resolve_set_expr(expr, &row[ci])?;
                    }
                    self.check_fk_on_insert(table, &new_row, schema)?;
                }
            }
            {
                let old_rows_ref: Vec<Vec<Value>> =
                    targets.iter().map(|(_, r)| r.clone()).collect();
                if !old_rows_ref.is_empty() {
                    self.check_fk_on_delete(table, &old_rows_ref, schema)?;
                }
            }
            let tc = self.cache.get(table).unwrap();
            let mut batch = self.store.batch();
            for (pk_bytes, mut row) in targets {
                let pk = row[0].clone();
                // 删除旧索引
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if let Some(ci) = resolve_col_indices(schema, cols_key) {
                        batch.remove(idx_ks, build_idx_key(&row, &ci, &pk)?);
                    }
                }
                let old_row = row.clone();
                // 应用赋值
                for &(ci, expr) in &assign_indexes {
                    row[ci] = resolve_set_expr(expr, &row[ci])?;
                }
                // M118：CHECK 约束校验（非事务 UPDATE 路径）
                if !tc.parsed_checks.is_empty() {
                    super::helpers::validate_check_constraints(
                        &row,
                        schema,
                        &tc.parsed_checks,
                        &schema.check_constraints,
                    )?;
                }
                // M111：UPDATE 唯一索引检查（非事务路径）— M112：复合索引
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if tc.unique_indexes.contains(cols_key) {
                        if let Some(col_indices) = resolve_col_indices(schema, cols_key) {
                            let old_vals: Vec<&Value> =
                                col_indices.iter().map(|&i| &old_row[i]).collect();
                            let new_vals: Vec<&Value> =
                                col_indices.iter().map(|&i| &row[i]).collect();
                            if old_vals != new_vals {
                                super::index_key::check_unique_violation_composite(
                                    idx_ks,
                                    &new_vals,
                                    Some(&pk_bytes),
                                )?;
                            }
                        }
                    }
                }
                let new_raw = schema.encode_row(&row)?;
                batch.insert(&tc.data_ks, pk_bytes, new_raw)?;
                // 插入新索引
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if let Some(ci) = resolve_col_indices(schema, cols_key) {
                        batch.insert(idx_ks, build_idx_key(&row, &ci, &pk)?, Vec::new())?;
                    }
                }
                old_rows.push(old_row);
                new_rows.push(row);
            }
            batch.commit()?;
        }
        let updated = new_rows.len() as i64;
        // 同步向量索引（UPDATE = delete old + insert new）
        let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_update(&self.store, table, &old_rows, &new_rows, schema, hv)?;
        Ok(vec![vec![Value::Integer(updated)]])
    }

    /// PK-first UPDATE：按 PK 逐个 get→decode→modify→encode→put。
    /// 内存从 O(N×row_size) 降为 O(N×pk_size) + O(batch×row_size)，亿级表安全。
    fn apply_updates_by_pks(
        &mut self,
        table: &str,
        schema: &Schema,
        assignments: &[(String, SetExpr)],
        pks: Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let assign_indexes: Vec<(usize, &SetExpr)> = assignments
            .iter()
            .map(|(col_name, expr)| {
                let ci = schema
                    .column_index_by_name(col_name)
                    .ok_or_else(|| Error::SqlExec(format!("UPDATE 列不存在: {}", col_name)));
                ci.map(|ci| {
                    if ci == 0 {
                        Err(Error::SqlExec("不允许 UPDATE 主键列".into()))
                    } else {
                        Ok((ci, expr))
                    }
                })
            })
            .collect::<Result<Vec<Result<_, _>>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let mut old_rows = Vec::with_capacity(pks.len());
        let mut new_rows = Vec::with_capacity(pks.len());

        if self.tx.is_some() {
            let tc = self.cache.get(table).unwrap();
            let idx_cols: Vec<(String, Vec<usize>)> = tc
                .index_keyspaces
                .keys()
                .filter_map(|c| resolve_col_indices(schema, c).map(|ci| (c.clone(), ci)))
                .collect();
            // M111：预收集唯一索引列名
            let unique_cols: Vec<String> = tc.unique_indexes.iter().cloned().collect();
            let _ = tc;
            for pk_bytes in pks {
                let raw = match self.tx_get(table, &pk_bytes)? {
                    Some(r) => r,
                    None => continue,
                };
                let mut row = schema.decode_row(&raw)?;
                let pk = row[0].clone();
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, &pk)?;
                    self.tx_index_delete(table, cols_key, &ik)?;
                }
                let old_row = row.clone();
                for &(ci, expr) in &assign_indexes {
                    row[ci] = resolve_set_expr(expr, &row[ci])?;
                }
                // M118：CHECK 约束校验（事务 PK-first UPDATE 路径）
                {
                    let tc_chk = self.cache.get(table).unwrap();
                    if !tc_chk.parsed_checks.is_empty() {
                        super::helpers::validate_check_constraints(
                            &row,
                            schema,
                            &tc_chk.parsed_checks,
                            &schema.check_constraints,
                        )?;
                    }
                }
                // M127：外键约束校验（事务 PK-first UPDATE 路径）
                self.check_fk_on_insert(table, &row, schema)?;
                self.check_fk_on_delete(table, std::slice::from_ref(&old_row), schema)?;
                // M111：UPDATE 唯一索引检查（事务 PK-first 路径）— M112：复合索引
                for uc in &unique_cols {
                    if let Some(col_indices) = resolve_col_indices(schema, uc) {
                        let old_vals: Vec<&Value> =
                            col_indices.iter().map(|&i| &old_row[i]).collect();
                        let new_vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
                        if old_vals != new_vals {
                            let tc = self.cache.get(table).unwrap();
                            if let Some(idx_ks) = tc.index_keyspaces.get(uc) {
                                let tx_writes =
                                    self.tx.as_ref().map(|tx| tx.index_writes.as_slice());
                                let result = if let Some(writes) = tx_writes {
                                    super::index_key::check_unique_violation_tx_composite(
                                        idx_ks,
                                        writes,
                                        table,
                                        uc,
                                        &new_vals,
                                        Some(&pk_bytes),
                                    )
                                } else {
                                    super::index_key::check_unique_violation_composite(
                                        idx_ks,
                                        &new_vals,
                                        Some(&pk_bytes),
                                    )
                                };
                                result?;
                            }
                        }
                    }
                }
                let new_raw = schema.encode_row(&row)?;
                self.tx_set(table, pk_bytes, new_raw)?;
                for (cols_key, ci) in &idx_cols {
                    let ik = build_idx_key(&row, ci, &pk)?;
                    self.tx_index_set(table, cols_key, ik)?;
                }
                old_rows.push(old_row);
                new_rows.push(row);
            }
        } else {
            // M127：外键约束预检查（非事务 PK-first UPDATE 路径）
            if !schema.foreign_keys.is_empty() {
                for pk_bytes in &pks {
                    if let Some(raw) = self.cache.get(table).unwrap().data_ks.get(pk_bytes)? {
                        let row = schema.decode_row(&raw)?;
                        let mut new_row = row.clone();
                        for &(ci, expr) in &assign_indexes {
                            new_row[ci] = resolve_set_expr(expr, &row[ci])?;
                        }
                        self.check_fk_on_insert(table, &new_row, schema)?;
                    }
                }
            }
            {
                // 检查父表被引用列是否被子表引用
                let mut pre_rows: Vec<Vec<Value>> = Vec::new();
                let tc_pre = self.cache.get(table).unwrap();
                for pk_bytes in &pks {
                    if let Some(raw) = tc_pre.data_ks.get(pk_bytes)? {
                        pre_rows.push(schema.decode_row(&raw)?);
                    }
                }
                if !pre_rows.is_empty() {
                    self.check_fk_on_delete(table, &pre_rows, schema)?;
                }
            }
            let tc = self.cache.get(table).unwrap();
            let mut batch = self.store.batch();
            for pk_bytes in &pks {
                let raw = match tc.data_ks.get(pk_bytes)? {
                    Some(r) => r,
                    None => continue,
                };
                let mut row = schema.decode_row(&raw)?;
                let pk = row[0].clone();
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if let Some(ci) = resolve_col_indices(schema, cols_key) {
                        batch.remove(idx_ks, build_idx_key(&row, &ci, &pk)?);
                    }
                }
                let old_row = row.clone();
                for &(ci, expr) in &assign_indexes {
                    row[ci] = resolve_set_expr(expr, &row[ci])?;
                }
                // M118：CHECK 约束校验（非事务 PK-first UPDATE 路径）
                if !tc.parsed_checks.is_empty() {
                    super::helpers::validate_check_constraints(
                        &row,
                        schema,
                        &tc.parsed_checks,
                        &schema.check_constraints,
                    )?;
                }
                // M111：UPDATE 唯一索引检查（非事务 PK-first 路径）— M112：复合索引
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if tc.unique_indexes.contains(cols_key) {
                        if let Some(col_indices) = resolve_col_indices(schema, cols_key) {
                            let old_vals: Vec<&Value> =
                                col_indices.iter().map(|&i| &old_row[i]).collect();
                            let new_vals: Vec<&Value> =
                                col_indices.iter().map(|&i| &row[i]).collect();
                            if old_vals != new_vals {
                                super::index_key::check_unique_violation_composite(
                                    idx_ks,
                                    &new_vals,
                                    Some(pk_bytes.as_slice()),
                                )?;
                            }
                        }
                    }
                }
                let new_raw = schema.encode_row(&row)?;
                batch.insert(&tc.data_ks, pk_bytes.clone(), new_raw)?;
                for (cols_key, idx_ks) in &tc.index_keyspaces {
                    if let Some(ci) = resolve_col_indices(schema, cols_key) {
                        batch.insert(idx_ks, build_idx_key(&row, &ci, &pk)?, Vec::new())?;
                    }
                }
                old_rows.push(old_row);
                new_rows.push(row);
            }
            batch.commit()?;
        }
        let updated = new_rows.len() as i64;
        let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_update(&self.store, table, &old_rows, &new_rows, schema, hv)?;
        Ok(vec![vec![Value::Integer(updated)]])
    }
}

/// M110：根据 SetExpr 计算新值。
pub(super) fn resolve_set_expr(expr: &SetExpr, current: &Value) -> Result<Value, Error> {
    match expr {
        SetExpr::Literal(v) => Ok(v.clone()),
        SetExpr::ColumnRef(..) => {
            // ColumnRef 由 exec_update_from 直接处理，不应走到这里
            Err(Error::SqlExec("ColumnRef 不能在非 FROM 更新中使用".into()))
        }
        SetExpr::ColumnArith(_, op, rhs) => {
            // NULL 参与运算 → NULL（SQL 标准）
            if matches!(current, Value::Null) || matches!(rhs, Value::Null) {
                return Ok(Value::Null);
            }
            match (current, rhs) {
                (Value::Integer(a), Value::Integer(b)) => {
                    let result = match op {
                        ArithOp::Add => a
                            .checked_add(*b)
                            .ok_or_else(|| Error::SqlExec("整数溢出".into()))?,
                        ArithOp::Sub => a
                            .checked_sub(*b)
                            .ok_or_else(|| Error::SqlExec("整数溢出".into()))?,
                        ArithOp::Mul => a
                            .checked_mul(*b)
                            .ok_or_else(|| Error::SqlExec("整数溢出".into()))?,
                        ArithOp::Div => {
                            if *b == 0 {
                                return Err(Error::SqlExec("除零错误".into()));
                            }
                            a / b
                        }
                    };
                    Ok(Value::Integer(result))
                }
                (Value::Float(a), Value::Float(b)) => {
                    let result = match op {
                        ArithOp::Add => a + b,
                        ArithOp::Sub => a - b,
                        ArithOp::Mul => a * b,
                        ArithOp::Div => {
                            if *b == 0.0 {
                                return Err(Error::SqlExec("除零错误".into()));
                            }
                            a / b
                        }
                    };
                    Ok(Value::Float(result))
                }
                (Value::Integer(a), Value::Float(b)) => {
                    let a = *a as f64;
                    let result = match op {
                        ArithOp::Add => a + b,
                        ArithOp::Sub => a - b,
                        ArithOp::Mul => a * b,
                        ArithOp::Div => {
                            if *b == 0.0 {
                                return Err(Error::SqlExec("除零错误".into()));
                            }
                            a / b
                        }
                    };
                    Ok(Value::Float(result))
                }
                (Value::Float(a), Value::Integer(b)) => {
                    let b = *b as f64;
                    let result = match op {
                        ArithOp::Add => a + b,
                        ArithOp::Sub => a - b,
                        ArithOp::Mul => a * b,
                        ArithOp::Div => {
                            if b == 0.0 {
                                return Err(Error::SqlExec("除零错误".into()));
                            }
                            a / b
                        }
                    };
                    Ok(Value::Float(result))
                }
                _ => Err(Error::SqlExec(
                    "SET 算术表达式仅支持 INTEGER/FLOAT 类型".into(),
                )),
            }
        }
    }
}
