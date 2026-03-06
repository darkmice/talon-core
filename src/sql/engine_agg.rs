/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M80：流式聚合执行器 — 避免全表行解码 + 内存收集。
//!
//! 优化路径：
//! - COUNT(*) 无 WHERE → key_count（零解码）
//! - SUM/AVG/MIN/MAX 无 WHERE → 流式扫描单列累加
//! - 聚合 + WHERE → 流式过滤 + 累加（不收集中间行）

use super::engine::SqlEngine;
use super::engine_agg_acc::AggAccumulator;
use super::helpers::{parse_agg_columns, row_matches, single_eq_condition};
use super::index_key::{index_scan_prefix, parse_index_pk};
use super::parser::WhereExpr;
use super::topn::extract_indexed_eq;
use crate::types::{Schema, Value};
use crate::Error;

impl SqlEngine {
    /// M80：尝试走流式聚合快速路径。返回 Some 表示已处理，None 表示回退到普通 SELECT。
    pub(super) fn try_aggregate_fast(
        &mut self,
        table: &str,
        columns: &[String],
        where_clause: Option<&WhereExpr>,
    ) -> Result<Option<Vec<Vec<Value>>>, Error> {
        let aggs = match parse_agg_columns(columns) {
            Some(a) => a,
            None => return Ok(None),
        };
        // JSON_OBJECTAGG 需要双列访问，不走流式快速路径，回退到 compute_aggregates
        if aggs
            .iter()
            .any(|(t, _)| matches!(t, super::helpers::AggType::JsonObjectAgg(_)))
        {
            return Ok(None);
        }
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let schema = self.cache.get(table).unwrap().schema.clone();

        // ── 快速路径 1：COUNT(*) 无 WHERE → key_count ──
        if where_clause.is_none() && aggs.len() == 1 && aggs[0].1 == "*" {
            let count = self.fast_count_star(table)?;
            return Ok(Some(vec![vec![Value::Integer(count)]]));
        }

        // ── M93 方案B：column_stats O(1) 快速路径 ──
        if where_clause.is_none() {
            if let Some(row) = self.try_stats_fast(table, &aggs, &schema) {
                return Ok(Some(vec![row]));
            }
        }

        // 解析聚合列索引
        let col_indices: Vec<Option<usize>> = aggs
            .iter()
            .map(|(_, col_name)| {
                if col_name == "*" {
                    Ok(None)
                } else {
                    schema
                        .column_index_by_name(col_name)
                        .map(Some)
                        .ok_or_else(|| Error::SqlExec(format!("聚合列不存在: {}", col_name)))
                }
            })
            .collect::<Result<_, _>>()?;

        let mut acc = AggAccumulator::new(&aggs, Some(&schema));

        if let Some(expr) = where_clause {
            // ── 快速路径 2：聚合 + WHERE → 流式过滤 + 累加 ──
            self.stream_aggregate_with_where(table, &schema, expr, &col_indices, &mut acc)?;
        } else {
            // ── 快速路径 3：聚合无 WHERE → 流式扫描 + 累加 ──
            self.stream_aggregate_no_where(table, &schema, &col_indices, &mut acc)?;
        }

        Ok(Some(vec![acc.finish()]))
    }

    /// COUNT(*) 无 WHERE 快速路径。
    /// M117：优先 column_stats O(1) → M93 索引扫描 → data 扫描。
    fn fast_count_star(&self, table: &str) -> Result<i64, Error> {
        // M117：column_stats 缓存命中时 O(1) 返回
        if let Some(ts) = self.column_stats.get(table) {
            if let Some(cs) = ts.values().next() {
                return Ok(cs.count);
            }
        }
        let tc = self.cache.get(table).unwrap();
        if self.tx.is_none() {
            // M93：有索引时走索引 keyspace 计数
            if let Some(idx_ks) = tc.index_keyspaces.values().next() {
                let count = idx_ks.count_prefix(b"")?;
                return Ok(count as i64);
            }
            let count = tc.data_ks.count_prefix(b"")?;
            return Ok(count as i64);
        }
        // 有事务：基础计数 + overlay 调整
        let mut base = tc.data_ks.count_prefix(b"")? as i64;
        if let Some(ref tx) = self.tx {
            for ((t, pk), value) in &tx.writes {
                if t != table {
                    continue;
                }
                match value {
                    Some(_) => {
                        // 新增或覆盖 — 需检查是否原本存在
                        if tc.data_ks.get(pk)?.is_none() {
                            base += 1;
                        }
                    }
                    None => {
                        // 删除 — 原本存在才减
                        if tc.data_ks.get(pk)?.is_some() {
                            base -= 1;
                        }
                    }
                }
            }
        }
        Ok(base)
    }

    /// 流式聚合：无 WHERE，直接遍历存储累加。
    /// M93 方案A：零分配字节级聚合 — 纯数值单列聚合直接从 raw bytes 读 f64，
    /// 消除 1M 次 Vec<Value> 堆分配。多列/混合类型回退到稀疏解码。
    fn stream_aggregate_no_where(
        &self,
        table: &str,
        schema: &Schema,
        col_indices: &[Option<usize>],
        acc: &mut AggAccumulator,
    ) -> Result<(), Error> {
        let tc = self.cache.get(table).unwrap();
        let tx_keys: std::collections::HashSet<&[u8]> = if let Some(ref tx) = self.tx {
            tx.writes
                .iter()
                .filter(|((t, _), _)| t == table)
                .map(|((_, k), _)| k.as_slice())
                .collect()
        } else {
            std::collections::HashSet::new()
        };

        let (sparse_targets, sparse_indices) = build_sparse_map(col_indices);

        // M93 方案A：检测是否可以走零分配快速路径
        // 条件：纯数值聚合（SUM/AVG/MIN/MAX）+ 单目标列 + 无事务
        let zero_alloc = sparse_targets.len() == 1 && self.tx.is_none() && acc.is_all_numeric();

        let mut scan_err: Option<Error> = None;
        if sparse_targets.is_empty() {
            // 纯 COUNT(*)
            tc.data_ks.for_each_kv_prefix(b"", |key, _raw| {
                if !tx_keys.is_empty() && tx_keys.contains(key) {
                    return true;
                }
                acc.feed(&[], &sparse_indices);
                true
            })?;
        } else if zero_alloc {
            // M93 方案A：零分配字节级聚合 — 直接从 raw bytes 读 f64
            let phys_col = sparse_targets[0];
            let col_is_int = matches!(
                schema.columns.get(phys_col).map(|(_, t)| t),
                Some(crate::types::ColumnType::Integer) | Some(crate::types::ColumnType::Timestamp)
            );
            tc.data_ks.for_each_kv_prefix(b"", |_key, raw| {
                if raw.len() < 2 {
                    return true;
                }
                let payload = &raw[2..];
                if crate::types::row_codec::is_json_payload(payload) {
                    match schema.decode_columns_sparse(raw, &sparse_targets) {
                        Ok(row) => acc.feed(&row, &sparse_indices),
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    }
                    return true;
                }
                match crate::types::row_codec::read_column_f64(payload, phys_col) {
                    Ok(val) => {
                        // 喂所有累加器：COUNT(*) 用 None 路径，数值聚合用 val
                        for (i, si) in sparse_indices.iter().enumerate() {
                            if si.is_some() {
                                acc.feed_f64(val, i, col_is_int);
                            } else {
                                // COUNT(*) — 无条件 +1
                                acc.feed_f64(Some(1.0), i, true);
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
        } else {
            // 多列/混合类型：稀疏解码
            tc.data_ks.for_each_kv_prefix(b"", |key, raw| {
                if !tx_keys.is_empty() && tx_keys.contains(key) {
                    return true;
                }
                match schema.decode_columns_sparse(raw, &sparse_targets) {
                    Ok(row) => acc.feed(&row, &sparse_indices),
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
                true
            })?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        // 追加事务中存活的行
        if let Some(ref tx) = self.tx {
            for ((t, _), value) in &tx.writes {
                if t != table {
                    continue;
                }
                if let Some(raw) = value {
                    if sparse_targets.is_empty() {
                        acc.feed(&[], &sparse_indices);
                    } else {
                        let row = schema.decode_columns_sparse(raw, &sparse_targets)?;
                        acc.feed(&row, &sparse_indices);
                    }
                }
            }
        }
        Ok(())
    }

    /// 流式聚合：有 WHERE，尝试索引加速，否则全扫描过滤累加。
    fn stream_aggregate_with_where(
        &mut self,
        table: &str,
        schema: &Schema,
        expr: &WhereExpr,
        col_indices: &[Option<usize>],
        acc: &mut AggAccumulator,
    ) -> Result<(), Error> {
        // 尝试索引加速
        if let Some((col, val)) = single_eq_condition(expr) {
            let ci = schema.column_index_by_name(col);
            if let Some(0) = ci {
                // PK lookup
                let key = val.to_bytes()?;
                if let Some(raw) = self.tx_get(table, &key)? {
                    let row = schema.decode_row(&raw)?;
                    acc.feed(&row, col_indices);
                }
                return Ok(());
            }
            let tc = self.cache.get(table).unwrap();
            if let Some(idx_ks) = tc.index_keyspaces.get(col) {
                let prefix = index_scan_prefix(val)?;
                let mut scan_err: Option<Error> = None;
                idx_ks.for_each_key_prefix(&prefix, |pk_key| {
                    if let Some(pk_bytes) = parse_index_pk(pk_key) {
                        match self.tx_get(table, &pk_bytes) {
                            Ok(Some(raw)) => match schema.decode_row(&raw) {
                                Ok(row) => acc.feed(&row, col_indices),
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
                return Ok(());
            }
        }
        // AND 多条件索引加速
        let pk_col = schema.columns[0].0.clone();
        let tc = self.cache.get(table).unwrap();
        let idx_cols: Vec<&str> = tc.index_keyspaces.keys().map(|s| s.as_str()).collect();
        if let Some((icol, ival, rest)) = extract_indexed_eq(expr, &pk_col, &idx_cols) {
            let ci = schema.column_index_by_name(icol).unwrap();
            if ci == 0 {
                let key = ival.to_bytes()?;
                if let Some(raw) = self.tx_get(table, &key)? {
                    let row = schema.decode_row(&raw)?;
                    if rest
                        .iter()
                        .all(|e| row_matches(&row, schema, e).unwrap_or(false))
                    {
                        acc.feed(&row, col_indices);
                    }
                }
            } else {
                let targets = self.scan_by_index(table, schema, icol, ival, Some(&rest))?;
                for (_, row) in &targets {
                    acc.feed(row, col_indices);
                }
            }
            return Ok(());
        }
        // M90：流式扫描 + 过滤 + 流式累加
        let schema2 = schema.clone();
        self.tx_for_each_row(table, |row| {
            if row_matches(&row, &schema2, expr)? {
                acc.feed(&row, col_indices);
            }
            Ok(true)
        })
    }
}

// try_stats_fast 和 build_sparse_map 已提取到 engine_agg_stats.rs
use super::engine_agg_stats::build_sparse_map;
