/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M92: JOIN 执行器 — INNER JOIN + LEFT JOIN + Hash Join。
//!
//! 策略：
//! - PK 列 → PK 点查（最优）
//! - 有索引 → Index Lookup
//! - 无索引 → Hash Join（预构建 HashMap，O(M+N)）
//! Hash Join 使用零 JSON 的二进制哈希键，避免 serde_json 序列化开销。

use std::collections::{HashMap, HashSet};

use super::engine::SqlEngine;
use super::helpers::{row_matches, value_cmp};
use super::index_key::{index_scan_prefix, parse_index_pk};
use super::parser::{JoinClause, JoinType, WhereExpr};
use crate::types::{Schema, Value};
use crate::Error;

/// 高效二进制哈希键：替代 `Value::to_bytes()`（serde_json），
/// Hash Join build/probe 阶段零 JSON 开销。
fn value_hash_key(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![0x00],
        Value::Integer(n) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(0x01);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Float(f) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(0x02);
            buf.extend_from_slice(&f.to_bits().to_le_bytes());
            buf
        }
        Value::Text(s) => {
            let mut buf = Vec::with_capacity(1 + s.len());
            buf.push(0x03);
            buf.extend_from_slice(s.as_bytes());
            buf
        }
        Value::Boolean(b) => vec![0x04, *b as u8],
        Value::Blob(b) => {
            let mut buf = Vec::with_capacity(1 + b.len());
            buf.push(0x05);
            buf.extend_from_slice(b);
            buf
        }
        Value::Timestamp(t) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(0x06);
            buf.extend_from_slice(&t.to_le_bytes());
            buf
        }
        _ => v.to_bytes().unwrap_or_default(),
    }
}

/// 右表查找策略。
enum RightLookup {
    Pk,
    Index(String),
    /// R-JOIN-2: 预构建 HashMap 避免 O(N×M) 重复扫描。
    HashCache(HashMap<Vec<u8>, Vec<Vec<Value>>>),
}

impl SqlEngine {
    /// 执行 SELECT ... JOIN 查询。
    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_select_join(
        &mut self,
        left_table: &str,
        columns: &[String],
        join: &JoinClause,
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // M106: CROSS JOIN — 笛卡尔积，无 ON 条件
        if join.join_type == JoinType::Cross {
            return self.exec_cross_join(
                left_table,
                columns,
                join,
                where_clause,
                order_by,
                limit,
                offset,
            );
        }
        // M107: NATURAL JOIN — 自动匹配同名列
        if join.join_type == JoinType::Natural {
            return self.exec_natural_join(
                left_table,
                columns,
                join,
                where_clause,
                order_by,
                limit,
                offset,
            );
        }
        // M121：RIGHT JOIN → 互换左右表 + LEFT JOIN 语义
        let is_right_join = join.join_type == JoinType::Right;
        let (eff_left, eff_right, eff_left_col, eff_right_col) = if is_right_join {
            (&*join.table, left_table, &*join.right_col, &*join.left_col)
        } else {
            (left_table, &*join.table, &*join.left_col, &*join.right_col)
        };

        if !self.ensure_cached(eff_left)? {
            return Err(Error::SqlExec(format!("table not found: {}", eff_left)));
        }
        if !self.ensure_cached(eff_right)? {
            return Err(Error::SqlExec(format!("table not found: {}", eff_right)));
        }

        let left_schema = self.cache.get(eff_left).unwrap().schema.clone();
        let right_schema = self.cache.get(eff_right).unwrap().schema.clone();
        let left_col_idx = left_schema
            .column_index_by_name(eff_left_col)
            .ok_or_else(|| {
                Error::SqlExec(format!(
                    "JOIN ON left column not found: {}.{}",
                    eff_left, eff_left_col
                ))
            })?;
        let right_col_idx = right_schema
            .column_index_by_name(eff_right_col)
            .ok_or_else(|| {
                Error::SqlExec(format!(
                    "JOIN ON right column not found: {}.{}",
                    eff_right, eff_right_col
                ))
            })?;

        // R-JOIN-2: 选择右表查找策略
        let is_full_join = join.join_type == JoinType::Full;
        let is_left_join = join.join_type == JoinType::Left || is_right_join || is_full_join;
        let right_is_pk = right_col_idx == 0;
        let right_has_index = self
            .cache
            .get(eff_right)
            .unwrap()
            .index_keyspaces
            .contains_key(eff_right_col);

        // WHERE 下推：INNER JOIN 时将仅涉及右表的 WHERE 条件提前过滤右表，
        // 避免对左表每行做无效的 PK/索引查找。
        let (right_where, remaining_where) = if !is_left_join && !is_full_join {
            if let Some(wc) = where_clause {
                split_where_for_pushdown(wc, &left_schema, &right_schema, eff_right)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        let has_pushdown = right_where.is_some();

        // FULL JOIN 强制走 HashCache（需跟踪右表已匹配行）
        let lookup = if has_pushdown {
            // 右表 WHERE 下推：预过滤后构建 HashCache，避免 O(N) 无效查找
            let rw = right_where.as_ref().unwrap();
            let mut map: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
            let all = self.tx_scan_all(eff_right)?;
            for (_pk, row) in all {
                if row.len() > right_col_idx
                    && row_matches(&row, &right_schema, rw).unwrap_or(false)
                {
                    let key = value_hash_key(&row[right_col_idx]);
                    map.entry(key).or_default().push(row);
                }
            }
            RightLookup::HashCache(map)
        } else if !is_full_join && right_is_pk {
            RightLookup::Pk
        } else if !is_full_join && right_has_index {
            RightLookup::Index(eff_right_col.to_string())
        } else {
            // Hash Join：预构建 HashMap（二进制哈希键，零 JSON 开销）
            let mut map: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
            let all = self.tx_scan_all(eff_right)?;
            for (_pk, row) in all {
                if row.len() > right_col_idx {
                    let key = value_hash_key(&row[right_col_idx]);
                    map.entry(key).or_default().push(row);
                }
            }
            RightLookup::HashCache(map)
        };

        // 下推后的有效 WHERE：仅包含未被下推的条件
        let effective_where: Option<&WhereExpr> = if has_pushdown {
            remaining_where.as_ref()
        } else {
            where_clause
        };

        let merged_schema = merge_schemas(&left_schema, &right_schema);
        let left_col_count = left_schema.columns.len();
        let right_col_count = right_schema.columns.len();
        let right_table_owned = eff_right.to_string();
        let right_schema2 = right_schema.clone();
        // FULL JOIN：跟踪右表已匹配的 key（bytes）
        let mut matched_right_keys: HashSet<Vec<u8>> = HashSet::new();

        // M118：无 ORDER BY 时可提前终止（LIMIT + OFFSET）
        // FULL JOIN 不能提前终止（需扫描全部左表以跟踪右表匹配）
        let early_stop = if order_by.is_none() && !is_full_join {
            limit.map(|l| offset.unwrap_or(0) + l)
        } else {
            None
        };
        // R-JOIN-1: 流式扫描左表
        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        self.tx_for_each_row(left_table, |left_row| {
            if left_row.len() <= left_col_idx {
                return Ok(true);
            }
            let join_val = &left_row[left_col_idx];
            let right_matches = match &lookup {
                RightLookup::Pk => {
                    let key = join_val.to_bytes()?;
                    match self.tx_get(&right_table_owned, &key)? {
                        Some(raw) => vec![right_schema2.decode_row(&raw)?],
                        None => vec![],
                    }
                }
                RightLookup::Index(col) => {
                    self.join_lookup_index(&right_table_owned, &right_schema2, col, join_val)?
                }
                RightLookup::HashCache(map) => {
                    let key = value_hash_key(join_val);
                    map.get(&key).cloned().unwrap_or_default()
                }
            };
            if right_matches.is_empty() {
                if is_left_join {
                    let mut merged = left_row;
                    merged.extend(std::iter::repeat(Value::Null).take(right_col_count));
                    if passes_where(&merged, effective_where, &merged_schema) {
                        result_rows.push(merged);
                    }
                }
            } else {
                // FULL JOIN：记录已匹配的右表 key
                if is_full_join {
                    let key = value_hash_key(join_val);
                    matched_right_keys.insert(key);
                }
                for right_row in right_matches {
                    let mut merged = left_row.clone();
                    merged.extend(right_row);
                    if passes_where(&merged, effective_where, &merged_schema) {
                        result_rows.push(merged);
                    }
                }
            }
            // M118：提前终止
            if let Some(stop) = early_stop {
                Ok(result_rows.len() < stop as usize)
            } else {
                Ok(true)
            }
        })?;

        // M162: FULL JOIN — 追加右表中未匹配的行（左表列填 NULL）
        if is_full_join {
            if let RightLookup::HashCache(ref map) = lookup {
                for (key, rows) in map {
                    if !matched_right_keys.contains(key) {
                        for right_row in rows {
                            let mut merged: Vec<Value> = std::iter::repeat(Value::Null)
                                .take(left_col_count)
                                .collect();
                            merged.extend(right_row.iter().cloned());
                            if passes_where(&merged, effective_where, &merged_schema) {
                                result_rows.push(merged);
                            }
                        }
                    }
                }
            }
        }

        // ORDER BY
        if let Some(ob) = order_by {
            let col_indices: Vec<(usize, bool, bool)> = ob
                .iter()
                .filter_map(|(col, desc, nf)| {
                    find_col_in_merged(col, &merged_schema, left_col_count)
                        .map(|idx| (idx, *desc, nf.unwrap_or(*desc)))
                })
                .collect();
            if !col_indices.is_empty() {
                result_rows.sort_by(|a, b| {
                    for &(idx, desc, nf) in &col_indices {
                        let av = &a[idx];
                        let bv = &b[idx];
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
        // DISTINCT (R-JOIN-4)
        // OFFSET + LIMIT
        if let Some(off) = offset {
            let off = off as usize;
            if off >= result_rows.len() {
                result_rows.clear();
            } else {
                result_rows = result_rows.split_off(off);
            }
        }
        if let Some(n) = limit {
            result_rows.truncate(n as usize);
        }

        // 链式 JOIN：如果有 next，将当前结果作为左表与下一张表 JOIN
        let (mut final_rows, final_schema, final_left_count) = if let Some(ref next_jc) = join.next
        {
            self.exec_chain_join(result_rows, merged_schema, next_jc, where_clause)?
        } else {
            (result_rows, merged_schema, left_col_count)
        };

        // ORDER BY（在链式 JOIN 完成后统一执行）
        // 注意：上面已经做过 ORDER BY，但链式 JOIN 后需要重新排序
        if join.next.is_some() {
            if let Some(ob) = order_by {
                let col_indices: Vec<(usize, bool, bool)> = ob
                    .iter()
                    .filter_map(|(col, desc, nf)| {
                        find_col_in_merged(col, &final_schema, final_left_count)
                            .map(|idx| (idx, *desc, nf.unwrap_or(*desc)))
                    })
                    .collect();
                if !col_indices.is_empty() {
                    final_rows.sort_by(|a, b| {
                        for &(idx, desc, nf) in &col_indices {
                            let av = &a[idx];
                            let bv = &b[idx];
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
            if let Some(off) = offset {
                let off = off as usize;
                if off >= final_rows.len() {
                    final_rows.clear();
                } else {
                    final_rows = final_rows.split_off(off);
                }
            }
            if let Some(n) = limit {
                final_rows.truncate(n as usize);
            }
        }

        // R-JOIN-3: 列投影（支持 table.col 精确定位）
        project_columns_joined(final_rows, columns, &final_schema, final_left_count)
    }

    /// M106: CROSS JOIN — 笛卡尔积执行。
    fn exec_cross_join(
        &mut self,
        left_table: &str,
        columns: &[String],
        join: &JoinClause,
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let right_table = &join.table;
        if !self.ensure_cached(left_table)? {
            return Err(Error::SqlExec(format!("table not found: {}", left_table)));
        }
        if !self.ensure_cached(right_table)? {
            return Err(Error::SqlExec(format!("table not found: {}", right_table)));
        }
        let left_schema = self.cache.get(left_table).unwrap().schema.clone();
        let right_schema = self.cache.get(right_table).unwrap().schema.clone();
        let merged_schema = merge_schemas(&left_schema, &right_schema);
        let left_col_count = left_schema.columns.len();
        // 预加载右表全部行
        let right_rows: Vec<Vec<Value>> = self
            .tx_scan_all(right_table)?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        let early_stop = if order_by.is_none() {
            limit.map(|l| offset.unwrap_or(0) + l)
        } else {
            None
        };
        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        self.tx_for_each_row(left_table, |left_row| {
            for right_row in &right_rows {
                let mut merged = left_row.clone();
                merged.extend_from_slice(right_row);
                if passes_where(&merged, where_clause, &merged_schema) {
                    result_rows.push(merged);
                }
                if let Some(stop) = early_stop {
                    if result_rows.len() >= stop as usize {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        })?;
        // ORDER BY
        if let Some(ob) = order_by {
            let col_indices: Vec<(usize, bool, bool)> = ob
                .iter()
                .filter_map(|(col, desc, nf)| {
                    find_col_in_merged(col, &merged_schema, left_col_count)
                        .map(|i| (i, *desc, nf.unwrap_or(*desc)))
                })
                .collect();
            if !col_indices.is_empty() {
                result_rows.sort_by(|a, b| {
                    for &(idx, desc, nf) in &col_indices {
                        let av = &a[idx];
                        let bv = &b[idx];
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
        // OFFSET + LIMIT
        if let Some(off) = offset {
            let off = off as usize;
            if off >= result_rows.len() {
                result_rows.clear();
            } else {
                result_rows = result_rows.split_off(off);
            }
        }
        if let Some(n) = limit {
            result_rows.truncate(n as usize);
        }
        project_columns_joined(result_rows, columns, &merged_schema, left_col_count)
    }

    /// M107: NATURAL JOIN — 自动匹配同名列做等值连接。
    fn exec_natural_join(
        &mut self,
        left_table: &str,
        columns: &[String],
        join: &JoinClause,
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let right_table = &join.table;
        if !self.ensure_cached(left_table)? {
            return Err(Error::SqlExec(format!("table not found: {}", left_table)));
        }
        if !self.ensure_cached(right_table)? {
            return Err(Error::SqlExec(format!("table not found: {}", right_table)));
        }
        let left_schema = self.cache.get(left_table).unwrap().schema.clone();
        let right_schema = self.cache.get(right_table).unwrap().schema.clone();
        // 找同名列
        let mut common_cols: Vec<(usize, usize)> = Vec::new();
        for (li, (lname, _)) in left_schema.columns.iter().enumerate() {
            for (ri, (rname, _)) in right_schema.columns.iter().enumerate() {
                if lname.eq_ignore_ascii_case(rname) {
                    common_cols.push((li, ri));
                }
            }
        }
        if common_cols.is_empty() {
            // 无同名列 → 退化为 CROSS JOIN
            return self.exec_cross_join(
                left_table,
                columns,
                join,
                where_clause,
                order_by,
                limit,
                offset,
            );
        }
        let merged_schema = merge_schemas(&left_schema, &right_schema);
        let left_col_count = left_schema.columns.len();
        // 预加载右表
        let right_rows: Vec<Vec<Value>> = self
            .tx_scan_all(right_table)?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        let early_stop = if order_by.is_none() {
            limit.map(|l| offset.unwrap_or(0) + l)
        } else {
            None
        };
        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        self.tx_for_each_row(left_table, |left_row| {
            for right_row in &right_rows {
                // 检查所有同名列是否相等
                let matched = common_cols.iter().all(|&(li, ri)| {
                    li < left_row.len() && ri < right_row.len() && left_row[li] == right_row[ri]
                });
                if matched {
                    let mut merged = left_row.clone();
                    merged.extend_from_slice(right_row);
                    if passes_where(&merged, where_clause, &merged_schema) {
                        result_rows.push(merged);
                    }
                }
                if let Some(stop) = early_stop {
                    if result_rows.len() >= stop as usize {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        })?;
        // ORDER BY
        if let Some(ob) = order_by {
            let col_indices: Vec<(usize, bool, bool)> = ob
                .iter()
                .filter_map(|(col, desc, nf)| {
                    find_col_in_merged(col, &merged_schema, left_col_count)
                        .map(|i| (i, *desc, nf.unwrap_or(*desc)))
                })
                .collect();
            if !col_indices.is_empty() {
                result_rows.sort_by(|a, b| {
                    for &(idx, desc, nf) in &col_indices {
                        let av = &a[idx];
                        let bv = &b[idx];
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
        if let Some(off) = offset {
            let off = off as usize;
            if off >= result_rows.len() {
                result_rows.clear();
            } else {
                result_rows = result_rows.split_off(off);
            }
        }
        if let Some(n) = limit {
            result_rows.truncate(n as usize);
        }
        project_columns_joined(result_rows, columns, &merged_schema, left_col_count)
    }

    /// 链式 JOIN：将中间结果行（已合并的左+右）与下一张表 JOIN。
    /// 递归处理 `next` 链，支持任意数量的表。
    fn exec_chain_join(
        &mut self,
        left_rows: Vec<Vec<Value>>,
        left_schema: Schema,
        join: &JoinClause,
        where_clause: Option<&WhereExpr>,
    ) -> Result<(Vec<Vec<Value>>, Schema, usize), Error> {
        let right_table = &join.table;
        if !self.ensure_cached(right_table)? {
            return Err(Error::SqlExec(format!("table not found: {}", right_table)));
        }
        let right_schema = self.cache.get(right_table).unwrap().schema.clone();

        let left_col_idx = left_schema
            .column_index_by_name(&join.left_col)
            .ok_or_else(|| {
                Error::SqlExec(format!(
                    "chain JOIN left column not found: {}",
                    join.left_col
                ))
            })?;
        let right_col_idx = right_schema
            .column_index_by_name(&join.right_col)
            .ok_or_else(|| {
                Error::SqlExec(format!(
                    "chain JOIN right column not found: {}.{}",
                    right_table, join.right_col
                ))
            })?;

        // 预构建右表 HashMap
        let mut right_map: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
        let all = self.tx_scan_all(right_table)?;
        for (_pk, row) in all {
            if row.len() > right_col_idx {
                let key = row[right_col_idx].to_bytes().unwrap_or_default();
                right_map.entry(key).or_default().push(row);
            }
        }

        let merged_schema = merge_schemas(&left_schema, &right_schema);
        let left_col_count = left_schema.columns.len();
        let right_col_count = right_schema.columns.len();
        let is_left_join = join.join_type == JoinType::Left;

        let mut result_rows = Vec::new();
        for left_row in left_rows {
            if left_row.len() <= left_col_idx {
                continue;
            }
            let join_val = &left_row[left_col_idx];
            let key = join_val.to_bytes().unwrap_or_default();
            let right_matches = right_map.get(&key).cloned().unwrap_or_default();

            if right_matches.is_empty() {
                if is_left_join {
                    let mut merged = left_row;
                    merged.extend(std::iter::repeat(Value::Null).take(right_col_count));
                    if passes_where(&merged, where_clause, &merged_schema) {
                        result_rows.push(merged);
                    }
                }
            } else {
                for right_row in right_matches {
                    let mut merged = left_row.clone();
                    merged.extend(right_row);
                    if passes_where(&merged, where_clause, &merged_schema) {
                        result_rows.push(merged);
                    }
                }
            }
        }

        // 递归处理下一个 JOIN
        if let Some(ref next_jc) = join.next {
            self.exec_chain_join(result_rows, merged_schema, next_jc, where_clause)
        } else {
            Ok((result_rows, merged_schema, left_col_count))
        }
    }

    /// 索引扫描右表。
    fn join_lookup_index(
        &self,
        table: &str,
        schema: &Schema,
        col: &str,
        val: &Value,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let tc = self.cache.get(table).unwrap();
        let idx_ks = tc.index_keyspaces.get(col).unwrap();
        let prefix = index_scan_prefix(val)?;
        let mut rows = Vec::new();
        let mut scan_err: Option<Error> = None;
        idx_ks.for_each_key_prefix(&prefix, |pk_key| {
            if let Some(pk_bytes) = parse_index_pk(pk_key) {
                match self.tx_get(table, &pk_bytes) {
                    Ok(Some(raw)) => match schema.decode_row(&raw) {
                        Ok(row) => rows.push(row),
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
        Ok(rows)
    }
}

/// 合并两个 schema 为一个（左表列 + 右表列）。
fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.iter().cloned());
    let mut nullable = left.column_nullable.clone();
    nullable.extend(right.column_nullable.iter().cloned());
    let mut defaults = left.column_defaults.clone();
    defaults.extend(right.column_defaults.iter().cloned());
    let mut dropped = left.dropped_columns.clone();
    dropped.extend(right.dropped_columns.iter().cloned());
    Schema {
        columns,
        version: 0,
        column_defaults: defaults,
        column_nullable: nullable,
        dropped_columns: dropped,
        unique_constraints: vec![],
        auto_increment: false,
        check_constraints: vec![],
        foreign_keys: vec![],
        table_comment: None,
        column_comments: vec![],
    }
}

/// WHERE 过滤（合并行上）。
fn passes_where(row: &[Value], where_clause: Option<&WhereExpr>, schema: &Schema) -> bool {
    match where_clause {
        Some(expr) => row_matches(row, schema, expr).unwrap_or(false),
        None => true,
    }
}

/// 判断列名是否仅属于右表（无歧义时才可下推）。
fn is_right_only_column(
    col: &str,
    left_schema: &Schema,
    right_schema: &Schema,
    right_table: &str,
) -> bool {
    if let Some(dot) = col.rfind('.') {
        let table_part = &col[..dot];
        let col_part = &col[dot + 1..];
        table_part.eq_ignore_ascii_case(right_table)
            && right_schema.column_index_by_name(col_part).is_some()
    } else {
        // 无前缀：必须仅存在于右表，不能有歧义
        right_schema.column_index_by_name(col).is_some()
            && left_schema.column_index_by_name(col).is_none()
    }
}

/// 递归检查 WhereExpr 是否仅引用右表列。
fn where_references_only_right(
    expr: &WhereExpr,
    left_schema: &Schema,
    right_schema: &Schema,
    right_table: &str,
) -> bool {
    match expr {
        WhereExpr::Leaf(cond) => {
            if !is_right_only_column(&cond.column, left_schema, right_schema, right_table) {
                return false;
            }
            if let Some(ref vc) = cond.value_column {
                if !is_right_only_column(vc, left_schema, right_schema, right_table) {
                    return false;
                }
            }
            true
        }
        WhereExpr::And(children) | WhereExpr::Or(children) => children
            .iter()
            .all(|c| where_references_only_right(c, left_schema, right_schema, right_table)),
    }
}

/// 将 WHERE 拆分为（右表专属条件, 剩余条件）。
/// 仅拆分顶层 AND；OR 和嵌套表达式作为整体判断。
fn split_where_for_pushdown(
    where_clause: &WhereExpr,
    left_schema: &Schema,
    right_schema: &Schema,
    right_table: &str,
) -> (Option<WhereExpr>, Option<WhereExpr>) {
    match where_clause {
        WhereExpr::And(children) => {
            let mut right_only = Vec::new();
            let mut remaining = Vec::new();
            for child in children {
                if where_references_only_right(child, left_schema, right_schema, right_table) {
                    right_only.push(child.clone());
                } else {
                    remaining.push(child.clone());
                }
            }
            let ro = match right_only.len() {
                0 => None,
                1 => Some(right_only.remove(0)),
                _ => Some(WhereExpr::And(right_only)),
            };
            let rem = match remaining.len() {
                0 => None,
                1 => Some(remaining.remove(0)),
                _ => Some(WhereExpr::And(remaining)),
            };
            (ro, rem)
        }
        _ => {
            if where_references_only_right(where_clause, left_schema, right_schema, right_table) {
                (Some(where_clause.clone()), None)
            } else {
                (None, Some(where_clause.clone()))
            }
        }
    }
}

/// R-JOIN-3: 在合并 schema 中查找列，支持 `table.col` 歧义消解。
/// 无前缀时返回第一个匹配；有 table 前缀时根据 left_col_count 定位。
fn find_col_in_merged(col: &str, schema: &Schema, left_col_count: usize) -> Option<usize> {
    if let Some(dot_pos) = col.rfind('.') {
        let col_name = &col[dot_pos + 1..];
        // 在右表部分查找（偏移 left_col_count）
        for (i, (name, _)) in schema.columns[left_col_count..].iter().enumerate() {
            if name.eq_ignore_ascii_case(col_name) {
                return Some(left_col_count + i);
            }
        }
        // 回退到左表
        for (i, (name, _)) in schema.columns[..left_col_count].iter().enumerate() {
            if name.eq_ignore_ascii_case(col_name) {
                return Some(i);
            }
        }
        None
    } else {
        schema.column_index_by_name(col)
    }
}

/// JOIN 结果列投影：支持 `*`、`col`、`table.col` 形式。
fn project_columns_joined(
    rows: Vec<Vec<Value>>,
    columns: &[String],
    schema: &Schema,
    left_col_count: usize,
) -> Result<Vec<Vec<Value>>, Error> {
    if columns.len() == 1 && columns[0] == "*" {
        return Ok(rows);
    }
    let indices: Vec<usize> = columns
        .iter()
        .map(|col| {
            find_col_in_merged(col, schema, left_col_count)
                .ok_or_else(|| Error::SqlExec(format!("JOIN SELECT column not found: {}", col)))
        })
        .collect::<Result<_, _>>()?;
    Ok(rows
        .into_iter()
        .map(|row| indices.iter().map(|&i| row[i].clone()).collect())
        .collect())
}
