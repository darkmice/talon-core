/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M116: UPDATE ... FROM 跨表更新执行。
//!
//! 语法：`UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id`
//! 实现：先全表扫描源表构建 lookup map，再遍历目标表匹配更新。

use super::engine::SqlEngine;
use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::engine_update::resolve_set_expr;
use super::helpers::row_matches;
use super::parser::{SetExpr, WhereCondition, WhereExpr, WhereOp};
use crate::types::Value;
use crate::Error;
use std::collections::HashMap;

impl SqlEngine {
    /// 执行 UPDATE ... FROM 跨表更新。
    ///
    /// 策略：
    /// 1. 从 WHERE 条件中提取跨表等值连接条件（`t1.col = t2.col`）
    /// 2. 扫描源表构建 join_col → row 的 lookup map
    /// 3. 遍历目标表，对匹配行应用 SET 赋值
    pub(super) fn exec_update_from(
        &mut self,
        target: &str,
        source: &str,
        assignments: &[(String, SetExpr)],
        where_clause: Option<&WhereExpr>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(target)? {
            return Err(Error::SqlExec(format!("目标表不存在: {}", target)));
        }
        if !self.ensure_cached(source)? {
            return Err(Error::SqlExec(format!("源表不存在: {}", source)));
        }
        self.invalidate_stats(target);
        let t_schema = self.cache.get(target).unwrap().schema.clone();
        let s_schema = self.cache.get(source).unwrap().schema.clone();

        // 从 WHERE 中提取跨表连接条件和各表过滤条件
        let (join_conds, target_filters, source_filters) =
            extract_cross_table_conditions(where_clause, target, source)?;
        if join_conds.is_empty() {
            return Err(Error::SqlExec(
                "UPDATE ... FROM 需要至少一个跨表等值连接条件 (t1.col = t2.col)".into(),
            ));
        }

        // 扫描源表，构建 join key → source row 的 lookup map
        let src_join_col = &join_conds[0].1;
        let src_col_idx = s_schema
            .column_index_by_name(src_join_col)
            .ok_or_else(|| Error::SqlExec(format!("源表列不存在: {}.{}", source, src_join_col)))?;
        let mut lookup: HashMap<Vec<u8>, Vec<Value>> = HashMap::new();
        let mut scan_err: Option<Error> = None;
        let src_tc = self.cache.get(source).unwrap();
        src_tc.data_ks.for_each_kv_prefix(b"", |_key, raw| {
            match s_schema.decode_row(raw) {
                Ok(row) => {
                    // 应用源表过滤条件
                    let pass = source_filters
                        .iter()
                        .all(|f| row_matches(&row, &s_schema, f).unwrap_or(false));
                    if pass {
                        if !matches!(row[src_col_idx], Value::Null) {
                            if let Ok(jk) = row[src_col_idx].to_bytes() {
                                lookup.insert(jk, row);
                            }
                        }
                    }
                    true
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }

        // 遍历目标表，匹配并收集需要更新的行
        let tgt_join_col = &join_conds[0].0;
        let tgt_col_idx = t_schema.column_index_by_name(tgt_join_col).ok_or_else(|| {
            Error::SqlExec(format!("目标表列不存在: {}.{}", target, tgt_join_col))
        })?;
        // 预计算额外 join 条件的列索引，避免扫描时重复查找
        let extra_join_indices: Vec<(usize, usize)> = join_conds[1..]
            .iter()
            .map(|(tc, sc)| {
                let ti = t_schema
                    .column_index_by_name(tc)
                    .ok_or_else(|| Error::SqlExec(format!("目标表列不存在: {}.{}", target, tc)))?;
                let si = s_schema
                    .column_index_by_name(sc)
                    .ok_or_else(|| Error::SqlExec(format!("源表列不存在: {}.{}", source, sc)))?;
                Ok((ti, si))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut updates: Vec<(Vec<u8>, Vec<Value>, Vec<Value>)> = Vec::new();
        let mut scan_err2: Option<Error> = None;
        let tgt_tc = self.cache.get(target).unwrap();
        tgt_tc.data_ks.for_each_kv_prefix(b"", |key, raw| {
            match t_schema.decode_row(raw) {
                Ok(row) => {
                    let pass = target_filters
                        .iter()
                        .all(|f| row_matches(&row, &t_schema, f).unwrap_or(false));
                    if !pass {
                        return true;
                    }
                    if !matches!(row[tgt_col_idx], Value::Null) {
                        if let Ok(jk) = row[tgt_col_idx].to_bytes() {
                            if let Some(src_row) = lookup.get(&jk) {
                                // 验证额外 join 条件（多列连接，使用预计算索引）
                                let extra_ok = extra_join_indices
                                    .iter()
                                    .all(|&(ti, si)| row[ti] == src_row[si]);
                                if extra_ok {
                                    updates.push((key.to_vec(), row, src_row.clone()));
                                }
                            }
                        }
                    }
                    true
                }
                Err(e) => {
                    scan_err2 = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = scan_err2 {
            return Err(e);
        }

        // 预计算索引列信息（支持复合索引）
        let tc = self.cache.get(target).unwrap();
        let idx_cols: Vec<(String, Vec<usize>)> = tc
            .index_keyspaces
            .keys()
            .filter_map(|c| resolve_col_indices(&t_schema, c).map(|ci| (c.clone(), ci)))
            .collect();
        let has_vec = tc.has_vec_indexes;
        let _ = tc;

        // 应用更新（含索引维护）
        let mut count = 0usize;
        let mut old_rows = Vec::new();
        let mut new_rows = Vec::new();
        for (pk_key, mut row, src_row) in updates {
            let pk = row[0].clone();
            // 删除旧索引条目
            for (cols_key, ci) in &idx_cols {
                let ik = build_idx_key(&row, ci, &pk)?;
                self.tx_index_delete(target, cols_key, &ik)?;
            }
            let old_row = row.clone();
            for (col_name, expr) in assignments {
                let ci = t_schema
                    .column_index_by_name(col_name)
                    .ok_or_else(|| Error::SqlExec(format!("目标列不存在: {}", col_name)))?;
                let new_val = match expr {
                    SetExpr::ColumnRef(_tbl, ref_col) => {
                        let si = s_schema.column_index_by_name(ref_col).ok_or_else(|| {
                            Error::SqlExec(format!("源表列不存在: {}.{}", source, ref_col))
                        })?;
                        src_row[si].clone()
                    }
                    other => resolve_set_expr(other, &row[ci])?,
                };
                row[ci] = new_val;
            }
            let encoded = t_schema.encode_row(&row)?;
            self.tx_set(target, pk_key, encoded)?;
            // 插入新索引条目
            for (cols_key, ci) in &idx_cols {
                let ik = build_idx_key(&row, ci, &pk)?;
                self.tx_index_set(target, cols_key, ik)?;
            }
            old_rows.push(old_row);
            new_rows.push(row);
            count += 1;
        }
        // 同步向量索引
        super::vec_idx::sync_vec_on_update(&self.store, target, &old_rows, &new_rows, &t_schema, has_vec)?;
        Ok(vec![vec![Value::Integer(count as i64)]])
    }
}

/// 从 WHERE 表达式中提取跨表连接条件和各表独立过滤条件。
///
/// 跨表条件：`t1.col = t2.col` → (target_col, source_col)
/// 目标表过滤：`t1.active = 1` → WhereExpr
/// 源表过滤：`t2.status = 'ok'` → WhereExpr
pub(super) fn extract_cross_table_conditions(
    where_clause: Option<&WhereExpr>,
    target: &str,
    source: &str,
) -> Result<(Vec<(String, String)>, Vec<WhereExpr>, Vec<WhereExpr>), Error> {
    let mut joins = Vec::new();
    let mut t_filters = Vec::new();
    let mut s_filters = Vec::new();

    let Some(expr) = where_clause else {
        return Ok((joins, t_filters, s_filters));
    };

    let leaves = collect_and_leaves(expr);
    for leaf in leaves {
        if let WhereExpr::Leaf(cond) = leaf {
            if cond.op == WhereOp::Eq {
                let col = &cond.column;
                // 检测 t1.col = t2.col 模式（值侧也是列引用）
                if let Value::Text(ref val_text) = cond.value {
                    if let (Some((lt, lc)), Some((rt, rc))) =
                        (split_table_col(col), split_table_col(val_text))
                    {
                        if lt == target && rt == source {
                            joins.push((lc.to_string(), rc.to_string()));
                            continue;
                        } else if lt == source && rt == target {
                            joins.push((rc.to_string(), lc.to_string()));
                            continue;
                        }
                    }
                }
                // 带表前缀的单表过滤
                if let Some((t, c)) = split_table_col(col) {
                    let stripped = WhereExpr::Leaf(WhereCondition {
                        column: c.to_string(),
                        op: cond.op,
                        value: cond.value.clone(),
                        in_values: cond.in_values.clone(),
                        value_high: cond.value_high.clone(),
                        jsonb_path: cond.jsonb_path.clone(),
                        subquery: cond.subquery.clone(),
                        escape_char: cond.escape_char,
                        value_column: cond.value_column.clone(),
                    });
                    if t == target {
                        t_filters.push(stripped);
                    } else if t == source {
                        s_filters.push(stripped);
                    }
                    continue;
                }
            }
            // 无表前缀的条件默认归目标表
            t_filters.push(leaf.clone());
        }
    }
    Ok((joins, t_filters, s_filters))
}

/// 收集 AND 连接的所有叶子节点。
fn collect_and_leaves(expr: &WhereExpr) -> Vec<&WhereExpr> {
    match expr {
        WhereExpr::And(children) => children.iter().flat_map(collect_and_leaves).collect(),
        other => vec![other],
    }
}

/// 分割 `table.col` 为 (table, col)。
fn split_table_col(s: &str) -> Option<(&str, &str)> {
    let dot = s.find('.')?;
    let table = s[..dot].trim();
    let col = s[dot + 1..].trim();
    if table.is_empty() || col.is_empty() {
        return None;
    }
    Some((table, col))
}
