/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine SELECT 执行器（从 engine_exec.rs 拆分，保持 ≤500 行）。
//! M75：Top-N 堆排序 + WHERE+LIMIT 提前终止优化。

use super::engine::SqlEngine;
use super::helpers::{
    compute_aggregates, dedup_rows, parse_agg_columns, project_columns, row_matches,
    single_eq_condition, value_cmp,
};
use super::index_key;
use super::index_key::{index_scan_prefix, parse_index_pk};
use super::parser::WhereExpr;
use super::topn;
use super::topn::TopNHeap;
use super::topn::{extract_indexed_eq, extract_indexed_range};
use crate::types::Value;
use crate::Error;

impl SqlEngine {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_select(
        &mut self,
        table: &str,
        columns: &[String],
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
        distinct: bool,
        vec_search: Option<&super::parser::VecSearchExpr>,
        distinct_on: Option<&[String]>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M80：聚合快速路径 — 流式累加，避免全表行收集
        if vec_search.is_none() && order_by.is_none() && !distinct {
            if let Some(result) = self.try_aggregate_fast(table, columns, where_clause)? {
                return Ok(result);
            }
        }

        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let schema = tc.schema.clone();

        // M75：解析 ORDER BY 列索引（vec_search 有自己的排序，跳过）
        let col_indices: Option<Vec<(usize, bool, Option<bool>)>> = if vec_search.is_none() {
            if let Some(ob) = order_by {
                Some(
                    ob.iter()
                        .map(|(col, desc, nf)| {
                            schema
                                .column_index_by_name(col)
                                .ok_or_else(|| {
                                    Error::SqlExec(format!("ORDER BY 列不存在: {}", col))
                                })
                                .map(|idx| (idx, *desc, *nf))
                        })
                        .collect::<Result<_, _>>()?,
                )
            } else {
                None
            }
        } else {
            None
        };

        let is_vec = vec_search.is_some();
        // M75：判断是否可以用 Top-N 堆排序（ORDER BY + LIMIT + 非向量搜索）
        let topn_cap = if !is_vec && col_indices.is_some() && !distinct {
            limit.map(|l| l.saturating_add(offset.unwrap_or(0)) as usize)
        } else {
            None
        };

        let mut rows = Vec::new();

        if let Some(expr) = where_clause {
            if let Some((col, val)) = single_eq_condition(expr) {
                let ci = schema
                    .column_index_by_name(col)
                    .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", col)))?;
                if ci == 0 {
                    let key = val.to_bytes()?;
                    if let Some(raw) = self.tx_get(table, &key)? {
                        rows.push(schema.decode_row(&raw)?);
                    }
                } else if let Some(idx_ks) = tc.index_keyspaces.get(col) {
                    let prefix = index_scan_prefix(val)?;
                    let mut scan_err: Option<Error> = None;
                    // R-IDX-LIMIT: 索引扫描 LIMIT 下推（无 ORDER BY / DISTINCT 时）
                    let idx_limit = if order_by.is_none() && !distinct {
                        limit.map(|l| l.saturating_add(offset.unwrap_or(0)) as usize)
                    } else {
                        None
                    };
                    idx_ks.for_each_key_prefix(&prefix, |pk_key| {
                        if let Some(pk_bytes) = parse_index_pk(pk_key) {
                            match self.tx_get(table, &pk_bytes) {
                                Ok(Some(raw)) => match schema.decode_row(&raw) {
                                    Ok(row) => {
                                        rows.push(row);
                                        if let Some(cap) = idx_limit {
                                            if rows.len() >= cap {
                                                return false;
                                            }
                                        }
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
                } else if let Some(cap) = topn_cap {
                    // M90：WHERE(无索引) + ORDER BY + LIMIT → 流式过滤+TopN
                    let mut heap = TopNHeap::new(col_indices.clone().unwrap(), cap);
                    let schema2 = schema.clone();
                    self.tx_for_each_row(table, |row| {
                        if row_matches(&row, &schema2, expr)? {
                            heap.push(row);
                        }
                        Ok(true)
                    })?;
                    let sorted = heap.into_sorted();
                    let off = offset.unwrap_or(0) as usize;
                    rows = if off < sorted.len() {
                        sorted.into_iter().skip(off).collect()
                    } else {
                        vec![]
                    };
                    return self
                        .post_select(rows, columns, &schema, None, is_vec, table, order_by, limit);
                } else {
                    // M90：无索引 WHERE + LIMIT 流式扫描
                    let early_stop = if !is_vec && !distinct {
                        limit.map(|l| l.saturating_add(offset.unwrap_or(0)) as usize)
                    } else {
                        None
                    };
                    let schema2 = schema.clone();
                    self.tx_for_each_row(table, |row| {
                        if row_matches(&row, &schema2, expr)? {
                            rows.push(row);
                            if let Some(cap) = early_stop {
                                if rows.len() >= cap {
                                    return Ok(false);
                                }
                            }
                        }
                        Ok(true)
                    })?;
                }
            } else {
                // M76：AND 多条件索引加速 — 提取索引列做索引扫描，剩余条件内存过滤
                let pk_col = &schema.columns[0].0;
                let idx_cols: Vec<&str> = tc.index_keyspaces.keys().map(|s| s.as_str()).collect();
                if let Some((icol, ival, rest)) = extract_indexed_eq(expr, pk_col, &idx_cols) {
                    let ci = schema.column_index_by_name(icol).unwrap();
                    let candidate_rows = if ci == 0 {
                        // PK lookup
                        let key = ival.to_bytes()?;
                        match self.tx_get(table, &key)? {
                            Some(raw) => vec![schema.decode_row(&raw)?],
                            None => vec![],
                        }
                    } else {
                        // Index scan
                        let idx_ks = tc.index_keyspaces.get(icol).unwrap();
                        let prefix = index_scan_prefix(ival)?;
                        let mut tmp = Vec::new();
                        let mut scan_err: Option<Error> = None;
                        idx_ks.for_each_key_prefix(&prefix, |pk_key| {
                            if let Some(pk_bytes) = parse_index_pk(pk_key) {
                                match self.tx_get(table, &pk_bytes) {
                                    Ok(Some(raw)) => match schema.decode_row(&raw) {
                                        Ok(row) => tmp.push(row),
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
                        tmp
                    };
                    // 用剩余条件过滤
                    for row in candidate_rows {
                        let pass = rest
                            .iter()
                            .all(|e| row_matches(&row, &schema, e).unwrap_or(false));
                        if pass {
                            rows.push(row);
                        }
                    }
                } else if let Some((range_cond, rest)) = extract_indexed_range(expr, &idx_cols) {
                    // 范围索引扫描：Gt/Ge/Lt/Le/Between
                    let col = range_cond.column();
                    let (start, end) = match &range_cond {
                        topn::IndexRangeCond::Gt(_, v) => {
                            index_key::range_bounds(v, index_key::RangeOp::Gt)?
                        }
                        topn::IndexRangeCond::Ge(_, v) => {
                            index_key::range_bounds(v, index_key::RangeOp::Ge)?
                        }
                        topn::IndexRangeCond::Lt(_, v) => {
                            index_key::range_bounds(v, index_key::RangeOp::Lt)?
                        }
                        topn::IndexRangeCond::Le(_, v) => {
                            index_key::range_bounds(v, index_key::RangeOp::Le)?
                        }
                        topn::IndexRangeCond::Between(_, lo, hi) => {
                            index_key::between_bounds(lo, hi)?
                        }
                    };
                    let rest_refs: Vec<&WhereExpr> = rest.to_vec();
                    let filter = if rest_refs.is_empty() {
                        None
                    } else {
                        Some(rest_refs.as_slice())
                    };
                    let candidate_rows =
                        self.scan_by_index_range(table, &schema, col, &start, &end, filter)?;
                    for (_pk, row) in candidate_rows {
                        rows.push(row);
                    }
                } else if let Some(cap) = topn_cap {
                    // M90：复杂WHERE + ORDER BY + LIMIT → 流式过滤+TopN
                    let mut heap = TopNHeap::new(col_indices.clone().unwrap(), cap);
                    let schema2 = schema.clone();
                    self.tx_for_each_row(table, |row| {
                        if row_matches(&row, &schema2, expr)? {
                            heap.push(row);
                        }
                        Ok(true)
                    })?;
                    let sorted = heap.into_sorted();
                    let off = offset.unwrap_or(0) as usize;
                    rows = if off < sorted.len() {
                        sorted.into_iter().skip(off).collect()
                    } else {
                        vec![]
                    };
                    return self
                        .post_select(rows, columns, &schema, None, is_vec, table, order_by, limit);
                } else {
                    // M90：全表扫描 + 流式过滤 + 提前终止
                    let early_stop = if !is_vec && !distinct {
                        limit.map(|l| l.saturating_add(offset.unwrap_or(0)) as usize)
                    } else {
                        None
                    };
                    let schema2 = schema.clone();
                    self.tx_for_each_row(table, |row| {
                        if row_matches(&row, &schema2, expr)? {
                            rows.push(row);
                            if let Some(cap) = early_stop {
                                if rows.len() >= cap {
                                    return Ok(false);
                                }
                            }
                        }
                        Ok(true)
                    })?;
                }
            }
        } else if let Some(cap) = topn_cap {
            // M75：无 WHERE + ORDER BY + LIMIT → 流式 Top-N，O(cap) 内存，亿级安全
            let mut heap = TopNHeap::new(col_indices.clone().unwrap(), cap);
            self.tx_scan_topn(table, &mut heap)?;
            let sorted = heap.into_sorted();
            // 应用 OFFSET
            let off = offset.unwrap_or(0) as usize;
            rows = if off < sorted.len() {
                sorted.into_iter().skip(off).collect()
            } else {
                vec![]
            };
            // topn 已完成排序+截断，跳过后续 ORDER BY / OFFSET / LIMIT
            return self.post_select(rows, columns, &schema, None, is_vec, table, order_by, limit);
        } else {
            // M66：无 ORDER BY → LIMIT 下推
            let can_pushdown = order_by.is_none() && !distinct && !is_vec;
            let scan_limit = if can_pushdown {
                match (limit, offset) {
                    (Some(l), Some(o)) => Some(l.saturating_add(o)),
                    (Some(l), None) => Some(l),
                    _ => None,
                }
            } else {
                None
            };
            for (_pk, row) in self.tx_scan_with_limit(table, scan_limit)? {
                rows.push(row);
            }
        }

        // 普通排序（无 LIMIT 或 DISTINCT 等场景）
        if !is_vec {
            if let Some(ref ci) = col_indices {
                rows.sort_by(|a, b| {
                    for &(idx, desc, nulls_first) in ci {
                        let av = &a[idx];
                        let bv = &b[idx];
                        // NULLS FIRST/LAST：默认 ASC→NULLS LAST, DESC→NULLS FIRST
                        let nf = nulls_first.unwrap_or(desc);
                        match (matches!(av, Value::Null), matches!(bv, Value::Null)) {
                            (true, true) => continue,
                            (true, false) => {
                                return if nf {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            }
                            (false, true) => {
                                return if nf {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            }
                            _ => {}
                        }
                        let cmp = value_cmp(av, bv).unwrap_or(std::cmp::Ordering::Equal);
                        let cmp = if desc { cmp.reverse() } else { cmp };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        if distinct && distinct_on.is_none() {
            dedup_rows(&mut rows);
        }
        // M160: DISTINCT ON (col1, col2, ...) — 按指定列去重，保留每组排序后第一行
        if let Some(don_cols) = distinct_on {
            let don_indices: Result<Vec<usize>, Error> = don_cols
                .iter()
                .map(|c| {
                    schema
                        .column_index_by_name(c)
                        .ok_or_else(|| Error::SqlExec(format!("DISTINCT ON 列不存在: {}", c)))
                })
                .collect();
            let don_indices = don_indices?;
            use std::hash::Hasher;
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                for &i in &don_indices {
                    super::helpers::hash_value(&row[i], &mut h);
                }
                seen.insert(h.finish())
            });
        }
        if !is_vec {
            if let Some(off) = offset {
                let off = off as usize;
                if off >= rows.len() {
                    rows.clear();
                } else {
                    rows = rows.split_off(off);
                }
            }
            if let Some(n) = limit {
                rows.truncate(n as usize);
            }
        }
        self.post_select(
            rows, columns, &schema, vec_search, is_vec, table, order_by, limit,
        )
    }

    /// 后处理：聚合函数、向量搜索、列投影。
    #[allow(clippy::too_many_arguments)]
    fn post_select(
        &self,
        rows: Vec<Vec<Value>>,
        columns: &[String],
        schema: &crate::types::Schema,
        vec_search: Option<&super::parser::VecSearchExpr>,
        _is_vec: bool,
        table: &str,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if let Some(aggs) = parse_agg_columns(columns) {
            let agg_row = compute_aggregates(&rows, &aggs, schema)?;
            return Ok(vec![agg_row]);
        }
        if let Some(vs) = vec_search {
            let mut resolved_vs = vs.clone();
            if resolved_vs.metric == "distance" {
                if let Ok(Some(m)) =
                    super::vec_idx::get_vec_index_metric(&self.store, table, &vs.column)
                {
                    resolved_vs.metric = m;
                }
            }
            return super::vec_search::exec_vec_search(
                rows,
                columns,
                schema,
                &resolved_vs,
                order_by,
                limit,
            );
        }
        project_columns(rows, columns, schema)
    }
}
