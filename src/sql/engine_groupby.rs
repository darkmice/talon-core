/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GROUP BY + HAVING 执行器。
//!
//! 支持语法：
//! `SELECT col, COUNT(*), SUM(val) FROM t WHERE ... GROUP BY col HAVING COUNT(*) > 5 ORDER BY ... LIMIT N`
//!
//! 实现：流式扫描 → 按 group key 聚合到 HashMap → HAVING 过滤 → 排序 → LIMIT。

use std::collections::HashMap;

use super::engine::SqlEngine;
use super::helpers::{parse_agg_func, row_matches, strip_spaces_outside_quotes, value_cmp};
use super::parser::WhereExpr;
use crate::types::{Schema, Value};
use crate::Error;

/// GROUP BY 解析结果：(group_col_indices, agg_specs: [(func, col, output_idx, separator)])。
/// separator 仅 GROUP_CONCAT 使用，其他聚合为空字符串。
type GroupParseResult = (Vec<usize>, Vec<(String, String, usize, String)>);

/// 从 Value 提取 f64。
fn val_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

/// Value → serde_json::Value 转换（JSON 聚合序列化用）。
fn value_to_json_val(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(n) => serde_json::json!(*n),
        Value::Float(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::json!(s),
        Value::Boolean(b) => serde_json::json!(*b),
        other => serde_json::json!(format!("{:?}", other)),
    }
}

/// 解析 SELECT 列为 group key 列和聚合列。
/// 返回 (group_col_indices, agg_specs)。
/// agg_specs: (func_name, col_name, output_index, separator)。
fn parse_group_columns(
    columns: &[String],
    schema: &Schema,
    group_by: &[String],
) -> Result<GroupParseResult, Error> {
    let mut group_indices = Vec::new();
    for gb_col in group_by {
        let idx = schema
            .column_index_by_name(gb_col)
            .ok_or_else(|| Error::SqlExec(format!("GROUP BY 列不存在: {}", gb_col)))?;
        group_indices.push(idx);
    }
    // 从原始列字符串直接解析 GROUP_CONCAT 的分隔符
    let mut agg_specs = Vec::new();
    for (out_idx, col) in columns.iter().enumerate() {
        if let Some((func, arg)) = parse_agg_func(col) {
            let (col_name, sep) = if func == "GROUP_CONCAT" {
                // arg 可能包含分隔符如 "name,';'" — 需拆分
                // 同时支持 STRING_AGG 别名
                let trimmed = strip_spaces_outside_quotes(col.trim());
                let upper = trimmed.to_uppercase();
                let prefix = if upper.starts_with("GROUP_CONCAT(") {
                    "GROUP_CONCAT("
                } else if upper.starts_with("STRING_AGG(") {
                    "STRING_AGG("
                } else {
                    ""
                };
                if !prefix.is_empty() && upper.ends_with(')') {
                    let inner = &trimmed[prefix.len()..trimmed.len() - 1];
                    if let Some(comma_pos) = inner.find(',') {
                        let cn = inner[..comma_pos].trim().to_ascii_lowercase();
                        let sep_raw = inner[comma_pos + 1..].trim();
                        let s = if (sep_raw.starts_with('\'') && sep_raw.ends_with('\''))
                            || (sep_raw.starts_with('"') && sep_raw.ends_with('"'))
                        {
                            sep_raw[1..sep_raw.len() - 1].to_string()
                        } else {
                            sep_raw.to_string()
                        };
                        (cn, s)
                    } else {
                        (arg.to_ascii_lowercase(), ",".to_string())
                    }
                } else {
                    (arg.to_ascii_lowercase(), ",".to_string())
                }
            } else if func == "JSON_OBJECTAGG" {
                // JSON_OBJECTAGG(key_col, val_col) — sep 字段存储 val_col
                let trimmed = col.trim().replace(' ', "");
                let upper = trimmed.to_uppercase();
                if upper.starts_with("JSON_OBJECTAGG(") && upper.ends_with(')') {
                    let inner = &trimmed["JSON_OBJECTAGG(".len()..trimmed.len() - 1];
                    if let Some(comma_pos) = inner.find(',') {
                        let key_col = inner[..comma_pos].trim().to_ascii_lowercase();
                        let val_col = inner[comma_pos + 1..].trim().to_ascii_lowercase();
                        // 验证 val_col 存在
                        if schema.column_index_by_name(&val_col).is_none() {
                            return Err(Error::SqlExec(format!(
                                "JSON_OBJECTAGG 值列不存在: {}",
                                val_col
                            )));
                        }
                        (key_col, val_col)
                    } else {
                        (arg.to_ascii_lowercase(), arg.to_ascii_lowercase())
                    }
                } else {
                    (arg.to_ascii_lowercase(), arg.to_ascii_lowercase())
                }
            } else if func == "PERCENTILE_CONT" || func == "PERCENTILE_DISC" {
                // PERCENTILE_CONT(0.5, col) — sep 字段存储 fraction
                let trimmed = col.trim().replace(' ', "");
                let upper = trimmed.to_uppercase();
                let prefix = if func == "PERCENTILE_CONT" {
                    "PERCENTILE_CONT("
                } else {
                    "PERCENTILE_DISC("
                };
                if upper.starts_with(prefix) && upper.ends_with(')') {
                    let inner = &trimmed[prefix.len()..trimmed.len() - 1];
                    if let Some(comma_pos) = inner.find(',') {
                        let frac_str = inner[..comma_pos].trim();
                        let cn = inner[comma_pos + 1..].trim().to_ascii_lowercase();
                        (cn, frac_str.to_string())
                    } else {
                        // 单参数：默认 fraction=0.5
                        (arg.to_ascii_lowercase(), "0.5".to_string())
                    }
                } else {
                    (arg.to_ascii_lowercase(), "0.5".to_string())
                }
            } else {
                (arg, String::new())
            };
            agg_specs.push((func, col_name, out_idx, sep));
        }
    }
    Ok((group_indices, agg_specs))
}

/// 分组聚合累加器。
struct GroupAccumulator {
    count: i64,
    sums: HashMap<usize, f64>,
    mins: HashMap<usize, Value>,
    maxs: HashMap<usize, Value>,
    /// GROUP_CONCAT 累加：col_idx → (parts, separator)。
    concats: HashMap<usize, (Vec<String>, String)>,
    /// Welford 在线算法：col_idx → (count, mean, m2)。
    welford: HashMap<usize, (u64, f64, f64)>,
    /// JSON_ARRAYAGG 累加：col_idx → json values。
    json_arrays: HashMap<usize, Vec<serde_json::Value>>,
    /// JSON_OBJECTAGG 累加：col_idx → (map, val_col_name)。
    json_objects: HashMap<usize, (serde_json::Map<String, serde_json::Value>, String)>,
    /// BOOL_AND 累加：col_idx → Option<bool>。
    bool_ands: HashMap<usize, Option<bool>>,
    /// BOOL_OR 累加：col_idx → Option<bool>。
    bool_ors: HashMap<usize, Option<bool>>,
    /// ARRAY_AGG 累加：col_idx → json values（跳过 NULL）。
    array_aggs: HashMap<usize, Vec<serde_json::Value>>,
    /// PERCENTILE_CONT/DISC 累加：col_idx → (fraction, values)。
    percentiles: HashMap<usize, (f64, Vec<f64>)>,
    /// 存储 group key 值（第一行的 group 列值）。
    group_key_values: Vec<Value>,
}

impl GroupAccumulator {
    fn new(group_key: Vec<Value>) -> Self {
        GroupAccumulator {
            count: 0,
            sums: HashMap::new(),
            mins: HashMap::new(),
            maxs: HashMap::new(),
            concats: HashMap::new(),
            welford: HashMap::new(),
            json_arrays: HashMap::new(),
            json_objects: HashMap::new(),
            bool_ands: HashMap::new(),
            bool_ors: HashMap::new(),
            array_aggs: HashMap::new(),
            percentiles: HashMap::new(),
            group_key_values: group_key,
        }
    }

    fn accumulate(
        &mut self,
        row: &[Value],
        agg_specs: &[(String, String, usize, String)],
        schema: &Schema,
    ) {
        self.count += 1;
        for (func, col_name, _, sep) in agg_specs {
            if col_name == "*" {
                continue;
            }
            if let Some(ci) = schema.column_index_by_name(col_name) {
                let val = &row[ci];
                match func.as_str() {
                    "SUM" | "AVG" => {
                        if let Some(f) = val_to_f64(val) {
                            *self.sums.entry(ci).or_insert(0.0) += f;
                        }
                    }
                    "MIN" => {
                        let entry = self.mins.entry(ci).or_insert_with(|| val.clone());
                        if value_cmp(val, entry).map(|o| o.is_lt()).unwrap_or(false) {
                            *entry = val.clone();
                        }
                    }
                    "MAX" => {
                        let entry = self.maxs.entry(ci).or_insert_with(|| val.clone());
                        if value_cmp(val, entry).map(|o| o.is_gt()).unwrap_or(false) {
                            *entry = val.clone();
                        }
                    }
                    "GROUP_CONCAT" => {
                        if !matches!(val, Value::Null) {
                            let text = match val {
                                Value::Text(s) => s.clone(),
                                Value::Integer(n) => n.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::Boolean(b) => b.to_string(),
                                _ => continue,
                            };
                            let entry = self
                                .concats
                                .entry(ci)
                                .or_insert_with(|| (Vec::new(), sep.clone()));
                            entry.0.push(text);
                        }
                    }
                    "STDDEV" | "VARIANCE" => {
                        if let Some(f) = val_to_f64(val) {
                            let entry = self.welford.entry(ci).or_insert((0, 0.0, 0.0));
                            entry.0 += 1;
                            let delta = f - entry.1;
                            entry.1 += delta / entry.0 as f64;
                            let delta2 = f - entry.1;
                            entry.2 += delta * delta2;
                        }
                    }
                    "JSON_ARRAYAGG" => {
                        let arr = self.json_arrays.entry(ci).or_insert_with(Vec::new);
                        arr.push(value_to_json_val(val));
                    }
                    "JSON_OBJECTAGG" => {
                        // sep 字段存储 val_col 名称
                        if let Some(vi) = schema.column_index_by_name(sep) {
                            let key = match val {
                                Value::Text(s) => s.clone(),
                                Value::Integer(n) => n.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::Boolean(b) => b.to_string(),
                                Value::Null => continue,
                                other => format!("{:?}", other),
                            };
                            let entry = self
                                .json_objects
                                .entry(ci)
                                .or_insert_with(|| (serde_json::Map::new(), sep.clone()));
                            entry.0.insert(key, value_to_json_val(&row[vi]));
                        }
                    }
                    "BOOL_AND" => {
                        let b = match val {
                            Value::Boolean(v) => Some(*v),
                            Value::Integer(n) => Some(*n != 0),
                            Value::Null => None,
                            _ => None,
                        };
                        if let Some(v) = b {
                            let entry = self.bool_ands.entry(ci).or_insert(None);
                            *entry = Some(entry.unwrap_or(true) && v);
                        }
                    }
                    "BOOL_OR" => {
                        let b = match val {
                            Value::Boolean(v) => Some(*v),
                            Value::Integer(n) => Some(*n != 0),
                            Value::Null => None,
                            _ => None,
                        };
                        if let Some(v) = b {
                            let entry = self.bool_ors.entry(ci).or_insert(None);
                            *entry = Some(entry.unwrap_or(false) || v);
                        }
                    }
                    "ARRAY_AGG" => {
                        if !matches!(val, Value::Null) {
                            let arr = self.array_aggs.entry(ci).or_insert_with(Vec::new);
                            arr.push(value_to_json_val(val));
                        }
                    }
                    "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                        if let Some(f) = val_to_f64(val) {
                            let frac: f64 = sep.parse().unwrap_or(0.5);
                            let entry = self.percentiles.entry(ci).or_insert((frac, Vec::new()));
                            entry.1.push(f);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn finish_row(
        &self,
        columns: &[String],
        agg_specs: &[(String, String, usize, String)],
        schema: &Schema,
        group_by: &[String],
    ) -> Vec<Value> {
        let mut row = vec![Value::Null; columns.len()];
        // 填充 group key 列
        for (i, gb_col) in group_by.iter().enumerate() {
            for (out_idx, col) in columns.iter().enumerate() {
                if col == gb_col && i < self.group_key_values.len() {
                    row[out_idx] = self.group_key_values[i].clone();
                }
            }
        }
        // 填充聚合列
        for (func, col_name, out_idx, _) in agg_specs {
            let val = match func.as_str() {
                "COUNT" => Value::Integer(self.count),
                "SUM" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.sums.get(&ci) {
                            Some(&s) if s == s.trunc() && s.abs() < 9.007_199_254_740_992e15 => {
                                Value::Integer(s as i64)
                            }
                            Some(&s) => Value::Float(s),
                            None => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "AVG" => {
                    if self.count == 0 {
                        Value::Null
                    } else if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.sums.get(&ci) {
                            Some(&s) => Value::Float(s / self.count as f64),
                            None => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "MIN" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        self.mins.get(&ci).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                "MAX" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        self.maxs.get(&ci).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                "GROUP_CONCAT" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.concats.get(&ci) {
                            Some((parts, sep)) if !parts.is_empty() => Value::Text(parts.join(sep)),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "STDDEV" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.welford.get(&ci) {
                            Some(&(c, _, m2)) if c > 0 => Value::Float((m2 / c as f64).sqrt()),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "VARIANCE" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.welford.get(&ci) {
                            Some(&(c, _, m2)) if c > 0 => Value::Float(m2 / c as f64),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "JSON_ARRAYAGG" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.json_arrays.get(&ci) {
                            Some(arr) => Value::Text(
                                serde_json::to_string(arr).unwrap_or_else(|_| "[]".into()),
                            ),
                            None => Value::Text("[]".into()),
                        }
                    } else {
                        Value::Text("[]".into())
                    }
                }
                "JSON_OBJECTAGG" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.json_objects.get(&ci) {
                            Some((map, _)) => Value::Text(
                                serde_json::to_string(&serde_json::Value::Object(map.clone()))
                                    .unwrap_or_else(|_| "{}".into()),
                            ),
                            None => Value::Text("{}".into()),
                        }
                    } else {
                        Value::Text("{}".into())
                    }
                }
                "BOOL_AND" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.bool_ands.get(&ci) {
                            Some(Some(v)) => Value::Boolean(*v),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "BOOL_OR" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.bool_ors.get(&ci) {
                            Some(Some(v)) => Value::Boolean(*v),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "ARRAY_AGG" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.array_aggs.get(&ci) {
                            Some(arr) if !arr.is_empty() => Value::Text(
                                serde_json::to_string(arr).unwrap_or_else(|_| "[]".into()),
                            ),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "PERCENTILE_CONT" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.percentiles.get(&ci) {
                            Some((frac, vals)) if !vals.is_empty() => {
                                let mut sorted = vals.clone();
                                sorted.sort_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                let n = sorted.len();
                                let idx = frac * (n - 1) as f64;
                                let lo = idx.floor() as usize;
                                let hi = idx.ceil() as usize;
                                let v = if lo == hi {
                                    sorted[lo]
                                } else {
                                    sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo as f64)
                                };
                                Value::Float(v)
                            }
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "PERCENTILE_DISC" => {
                    if let Some(ci) = schema.column_index_by_name(col_name) {
                        match self.percentiles.get(&ci) {
                            Some((frac, vals)) if !vals.is_empty() => {
                                let mut sorted = vals.clone();
                                sorted.sort_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                let n = sorted.len();
                                let idx = (frac * n as f64).ceil() as usize;
                                let idx = idx.clamp(1, n) - 1;
                                Value::Float(sorted[idx])
                            }
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };
            row[*out_idx] = val;
        }
        row
    }
}

impl SqlEngine {
    /// GROUP BY 执行：流式扫描 → HashMap 分组聚合 → HAVING 过滤 → 排序 → LIMIT。
    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_group_by(
        &mut self,
        table: &str,
        columns: &[String],
        where_clause: Option<&WhereExpr>,
        group_by: &[String],
        having: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let schema = self.cache.get(table).unwrap().schema.clone();
        let (group_indices, agg_specs) = parse_group_columns(columns, &schema, group_by)?;

        // 流式扫描，按 group key 聚合
        let mut groups: HashMap<Vec<u8>, GroupAccumulator> = HashMap::new();
        self.tx_for_each_row(table, |row| {
            // WHERE 过滤
            if let Some(expr) = where_clause {
                if !row_matches(&row, &schema, expr).unwrap_or(false) {
                    return Ok(true);
                }
            }
            // 提取 group key：先算 bytes key，仅新 group 才 clone 值
            let key_bytes = {
                let mut buf = Vec::with_capacity(group_indices.len() * 16);
                for &i in &group_indices {
                    match &row[i] {
                        Value::Null => buf.push(0),
                        Value::Integer(n) => {
                            buf.push(1);
                            buf.extend_from_slice(&n.to_le_bytes());
                        }
                        Value::Float(f) => {
                            buf.push(2);
                            let b = if *f == 0.0 { 0.0f64 } else { *f };
                            buf.extend_from_slice(&b.to_le_bytes());
                        }
                        Value::Text(s) => {
                            buf.push(3);
                            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                            buf.extend_from_slice(s.as_bytes());
                        }
                        Value::Boolean(b) => {
                            buf.push(4);
                            buf.push(if *b { 1 } else { 0 });
                        }
                        other => {
                            buf.push(255);
                            let s = format!("{:?}", other);
                            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                            buf.extend_from_slice(s.as_bytes());
                        }
                    }
                }
                buf
            };
            let acc = groups.entry(key_bytes).or_insert_with(|| {
                let gk: Vec<Value> = group_indices.iter().map(|&i| row[i].clone()).collect();
                GroupAccumulator::new(gk)
            });
            acc.accumulate(&row, &agg_specs, &schema);
            Ok(true)
        })?;

        // 构建结果行
        let mut result_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
        for acc in groups.values() {
            let row = acc.finish_row(columns, &agg_specs, &schema, group_by);
            // HAVING 过滤
            if let Some(having_expr) = having {
                if !having_matches(&row, columns, having_expr)? {
                    continue;
                }
            }
            result_rows.push(row);
        }

        // ORDER BY
        if let Some(ob) = order_by {
            let col_map: HashMap<&str, usize> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.as_str(), i))
                .collect();
            result_rows.sort_by(|a, b| {
                for (col, desc, nulls_first) in ob {
                    if let Some(&idx) = col_map.get(col.as_str()) {
                        let av = &a[idx];
                        let bv = &b[idx];
                        // NULLS FIRST/LAST 处理
                        let nf = nulls_first.unwrap_or(if *desc { true } else { false });
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
                        let cmp = if *desc { cmp.reverse() } else { cmp };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // LIMIT
        if let Some(n) = limit {
            result_rows.truncate(n as usize);
        }

        Ok(result_rows)
    }
}

/// 将 Value 列表序列化为确定性字节 key（HashMap/HashSet 去重用）。
/// GROUP BY 分组和 UNION 去重共用此函数。
pub(super) fn values_to_bytes(vals: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(vals.len() * 16);
    for v in vals {
        match v {
            Value::Null => buf.push(0),
            Value::Integer(i) => {
                buf.push(1);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(2);
                // 规范化：-0.0 → 0.0
                let bits = if *f == 0.0 { 0.0f64 } else { *f };
                buf.extend_from_slice(&bits.to_le_bytes());
            }
            Value::Text(s) => {
                buf.push(3);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Boolean(b) => {
                buf.push(4);
                buf.push(if *b { 1 } else { 0 });
            }
            _ => {
                buf.push(255);
                let s = format!("{:?}", v);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
        }
    }
    buf
}

/// HAVING 过滤：对聚合结果行进行条件匹配。
/// 简化实现：支持 `COUNT(*) > N` / `SUM(col) >= N` 等聚合条件。
fn having_matches(row: &[Value], columns: &[String], expr: &WhereExpr) -> Result<bool, Error> {
    match expr {
        WhereExpr::And(exprs) => {
            for e in exprs {
                if !having_matches(row, columns, e)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        WhereExpr::Or(exprs) => {
            for e in exprs {
                if having_matches(row, columns, e)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        WhereExpr::Leaf(cond) => {
            // 查找列名在 columns 中的位置（支持聚合函数名如 "COUNT(*)"）
            let col_name = &cond.column;
            let col_idx = columns
                .iter()
                .position(|c| {
                    c == col_name
                        || c.to_uppercase() == col_name.to_uppercase()
                        || c.to_uppercase().replace(' ', "")
                            == col_name.to_uppercase().replace(' ', "")
                })
                .ok_or_else(|| Error::SqlExec(format!("HAVING 列不存在: {}", col_name)))?;
            let actual = &row[col_idx];
            use super::parser::WhereOp;
            match cond.op {
                WhereOp::Eq => Ok(actual == &cond.value),
                WhereOp::Ne => Ok(actual != &cond.value),
                WhereOp::Gt => Ok(value_cmp(actual, &cond.value)
                    .map(|o| o.is_gt())
                    .unwrap_or(false)),
                WhereOp::Ge => Ok(value_cmp(actual, &cond.value)
                    .map(|o| !o.is_lt())
                    .unwrap_or(false)),
                WhereOp::Lt => Ok(value_cmp(actual, &cond.value)
                    .map(|o| o.is_lt())
                    .unwrap_or(false)),
                WhereOp::Le => Ok(value_cmp(actual, &cond.value)
                    .map(|o| !o.is_gt())
                    .unwrap_or(false)),
                _ => Err(Error::SqlExec(format!(
                    "HAVING 不支持操作符: {:?}",
                    cond.op
                ))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Talon;

    #[test]
    fn group_by_count() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE orders (id INT, category TEXT, amount INT)")
            .unwrap();
        db.run_sql("INSERT INTO orders VALUES (1, 'A', 10)")
            .unwrap();
        db.run_sql("INSERT INTO orders VALUES (2, 'B', 20)")
            .unwrap();
        db.run_sql("INSERT INTO orders VALUES (3, 'A', 30)")
            .unwrap();
        db.run_sql("INSERT INTO orders VALUES (4, 'B', 40)")
            .unwrap();
        db.run_sql("INSERT INTO orders VALUES (5, 'A', 50)")
            .unwrap();
        let rows = db
            .run_sql("SELECT category, COUNT(*) FROM orders GROUP BY category")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // 找到 A 和 B 的行
        for row in &rows {
            let cat = format!("{:?}", row[0]);
            let count = &row[1];
            if cat.contains("A") {
                assert_eq!(count, &crate::types::Value::Integer(3));
            } else {
                assert_eq!(count, &crate::types::Value::Integer(2));
            }
        }
    }

    #[test]
    fn group_by_sum_avg() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE sales (id INT, region TEXT, revenue INT)")
            .unwrap();
        db.run_sql("INSERT INTO sales VALUES (1, 'east', 100)")
            .unwrap();
        db.run_sql("INSERT INTO sales VALUES (2, 'west', 200)")
            .unwrap();
        db.run_sql("INSERT INTO sales VALUES (3, 'east', 300)")
            .unwrap();
        let rows = db
            .run_sql("SELECT region, SUM(revenue), AVG(revenue) FROM sales GROUP BY region")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_having() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE items (id INT, cat TEXT, price INT)")
            .unwrap();
        for i in 0..10 {
            let cat = if i % 3 == 0 { "X" } else { "Y" };
            db.run_sql(&format!(
                "INSERT INTO items VALUES ({}, '{}', {})",
                i,
                cat,
                i * 10
            ))
            .unwrap();
        }
        // X 有 4 个 (0,3,6,9), Y 有 6 个
        let rows = db
            .run_sql("SELECT cat, COUNT(*) FROM items GROUP BY cat HAVING COUNT(*) > 4")
            .unwrap();
        assert_eq!(rows.len(), 1, "only Y should pass HAVING");
    }

    #[test]
    fn group_by_with_where() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE log (id INT, level TEXT, msg TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO log VALUES (1, 'ERROR', 'fail')")
            .unwrap();
        db.run_sql("INSERT INTO log VALUES (2, 'INFO', 'ok')")
            .unwrap();
        db.run_sql("INSERT INTO log VALUES (3, 'ERROR', 'crash')")
            .unwrap();
        db.run_sql("INSERT INTO log VALUES (4, 'WARN', 'slow')")
            .unwrap();
        db.run_sql("INSERT INTO log VALUES (5, 'ERROR', 'timeout')")
            .unwrap();
        let rows = db
            .run_sql("SELECT level, COUNT(*) FROM log WHERE level != 'INFO' GROUP BY level")
            .unwrap();
        // ERROR=3, WARN=1 (INFO filtered out by WHERE)
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_order_limit() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE scores (id INT, team TEXT, pts INT)")
            .unwrap();
        for i in 0..20 {
            let team = format!("t{}", i % 5);
            db.run_sql(&format!(
                "INSERT INTO scores VALUES ({}, '{}', {})",
                i, team, i
            ))
            .unwrap();
        }
        let rows = db
            .run_sql(
                "SELECT team, COUNT(*) FROM scores GROUP BY team ORDER BY COUNT(*) DESC LIMIT 3",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_concat_default_sep() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE tags (id INT, cat TEXT, tag TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO tags VALUES (1, 'A', 'rust')")
            .unwrap();
        db.run_sql("INSERT INTO tags VALUES (2, 'A', 'go')")
            .unwrap();
        db.run_sql("INSERT INTO tags VALUES (3, 'B', 'python')")
            .unwrap();
        db.run_sql("INSERT INTO tags VALUES (4, 'A', 'java')")
            .unwrap();
        let rows = db
            .run_sql("SELECT cat, GROUP_CONCAT(tag) FROM tags GROUP BY cat")
            .unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            if row[0] == crate::types::Value::Text("A".into()) {
                // 默认逗号分隔，顺序不确定但应包含三个元素
                if let crate::types::Value::Text(ref s) = row[1] {
                    let parts: Vec<&str> = s.split(',').collect();
                    assert_eq!(parts.len(), 3);
                } else {
                    panic!("expected Text");
                }
            } else {
                assert_eq!(row[1], crate::types::Value::Text("python".into()));
            }
        }
    }

    #[test]
    fn group_concat_custom_sep() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE items (id INT, grp TEXT, name TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO items VALUES (1, 'X', 'a')")
            .unwrap();
        db.run_sql("INSERT INTO items VALUES (2, 'X', 'b')")
            .unwrap();
        db.run_sql("INSERT INTO items VALUES (3, 'X', 'c')")
            .unwrap();
        let rows = db
            .run_sql("SELECT grp, GROUP_CONCAT(name, ';') FROM items GROUP BY grp")
            .unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Text(ref s) = rows[0][1] {
            let parts: Vec<&str> = s.split(';').collect();
            assert_eq!(parts.len(), 3);
        } else {
            panic!("expected Text");
        }
    }

    #[test]
    fn group_concat_no_group_by() {
        // 无 GROUP BY 的 GROUP_CONCAT（全表聚合）
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE words (id INT, word TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO words VALUES (1, 'hello')").unwrap();
        db.run_sql("INSERT INTO words VALUES (2, 'world')").unwrap();
        let rows = db
            .run_sql("SELECT GROUP_CONCAT(word, ' ') FROM words")
            .unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Text(ref s) = rows[0][0] {
            assert!(s.contains("hello"));
            assert!(s.contains("world"));
        } else {
            panic!("expected Text");
        }
    }

    #[test]
    fn group_concat_with_integers() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE nums (id INT, grp TEXT, val INT)")
            .unwrap();
        db.run_sql("INSERT INTO nums VALUES (1, 'A', 10)").unwrap();
        db.run_sql("INSERT INTO nums VALUES (2, 'A', 20)").unwrap();
        db.run_sql("INSERT INTO nums VALUES (3, 'A', 30)").unwrap();
        let rows = db
            .run_sql("SELECT grp, GROUP_CONCAT(val, '-') FROM nums GROUP BY grp")
            .unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Text(ref s) = rows[0][1] {
            let parts: Vec<&str> = s.split('-').collect();
            assert_eq!(parts.len(), 3);
        } else {
            panic!("expected Text");
        }
    }

    #[test]
    fn stddev_variance_no_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE scores (id INT, val INT)").unwrap();
        // 值: 2, 4, 4, 4, 5, 5, 7, 9 → 总体方差=4.0, 总体标准差=2.0
        for (i, v) in [2, 4, 4, 4, 5, 5, 7, 9].iter().enumerate() {
            db.run_sql(&format!("INSERT INTO scores VALUES ({}, {})", i + 1, v))
                .unwrap();
        }
        let rows = db.run_sql("SELECT STDDEV(val) FROM scores").unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Float(s) = rows[0][0] {
            assert!((s - 2.0).abs() < 0.01, "stddev={}", s);
        } else {
            panic!("expected Float, got {:?}", rows[0][0]);
        }
        let rows = db.run_sql("SELECT VARIANCE(val) FROM scores").unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Float(v) = rows[0][0] {
            assert!((v - 4.0).abs() < 0.01, "variance={}", v);
        } else {
            panic!("expected Float, got {:?}", rows[0][0]);
        }
    }

    #[test]
    fn stddev_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE metrics (id INT, model TEXT, latency INT)")
            .unwrap();
        // model=gpt4: 10, 20, 30 → mean=20, var=66.67, stddev≈8.165
        // model=llama: 5, 5, 5 → mean=5, var=0, stddev=0
        for (i, (m, v)) in [
            ("gpt4", 10),
            ("gpt4", 20),
            ("gpt4", 30),
            ("llama", 5),
            ("llama", 5),
            ("llama", 5),
        ]
        .iter()
        .enumerate()
        {
            db.run_sql(&format!(
                "INSERT INTO metrics VALUES ({}, '{}', {})",
                i + 1,
                m,
                v
            ))
            .unwrap();
        }
        let rows = db
            .run_sql("SELECT model, STDDEV(latency) FROM metrics GROUP BY model ORDER BY model")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // gpt4: stddev ≈ 8.165
        if let crate::types::Value::Float(s) = rows[0][1] {
            assert!((s - 8.165).abs() < 0.01, "gpt4 stddev={}", s);
        } else {
            panic!("expected Float");
        }
        // llama: stddev = 0
        if let crate::types::Value::Float(s) = rows[1][1] {
            assert!(s.abs() < 0.001, "llama stddev={}", s);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn json_arrayagg_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE products (id INT, cat TEXT, name TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO products VALUES (1, 'fruit', 'apple')")
            .unwrap();
        db.run_sql("INSERT INTO products VALUES (2, 'fruit', 'banana')")
            .unwrap();
        db.run_sql("INSERT INTO products VALUES (3, 'veggie', 'carrot')")
            .unwrap();
        let rows = db
            .run_sql("SELECT cat, JSON_ARRAYAGG(name) FROM products GROUP BY cat ORDER BY cat")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // fruit: ["apple","banana"] 或 ["banana","apple"]
        if let crate::types::Value::Text(ref s) = rows[0][1] {
            let arr: Vec<serde_json::Value> = serde_json::from_str(s).unwrap();
            assert_eq!(arr.len(), 2);
            let strs: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert!(strs.contains(&"apple"));
            assert!(strs.contains(&"banana"));
        } else {
            panic!("expected Text, got {:?}", rows[0][1]);
        }
        // veggie: ["carrot"]
        if let crate::types::Value::Text(ref s) = rows[1][1] {
            let arr: Vec<serde_json::Value> = serde_json::from_str(s).unwrap();
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0].as_str().unwrap(), "carrot");
        } else {
            panic!("expected Text");
        }
    }

    #[test]
    fn json_arrayagg_no_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE vals (id INT, num INT)").unwrap();
        db.run_sql("INSERT INTO vals VALUES (1, 10)").unwrap();
        db.run_sql("INSERT INTO vals VALUES (2, 20)").unwrap();
        db.run_sql("INSERT INTO vals VALUES (3, 30)").unwrap();
        let rows = db.run_sql("SELECT JSON_ARRAYAGG(num) FROM vals").unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Text(ref s) = rows[0][0] {
            let arr: Vec<serde_json::Value> = serde_json::from_str(s).unwrap();
            assert_eq!(arr.len(), 3);
            let nums: Vec<i64> = arr.iter().map(|v| v.as_i64().unwrap()).collect();
            assert!(nums.contains(&10));
            assert!(nums.contains(&20));
            assert!(nums.contains(&30));
        } else {
            panic!("expected Text, got {:?}", rows[0][0]);
        }
    }

    #[test]
    fn json_objectagg_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE config (id INT, section TEXT, ckey TEXT, cval TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO config VALUES (1, 'db', 'host', 'localhost')")
            .unwrap();
        db.run_sql("INSERT INTO config VALUES (2, 'db', 'port', '5432')")
            .unwrap();
        db.run_sql("INSERT INTO config VALUES (3, 'app', 'name', 'talon')")
            .unwrap();
        let rows = db
            .run_sql("SELECT section, JSON_OBJECTAGG(ckey, cval) FROM config GROUP BY section ORDER BY section")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // app: {"name":"talon"}
        if let crate::types::Value::Text(ref s) = rows[0][1] {
            let obj: serde_json::Value = serde_json::from_str(s).unwrap();
            assert_eq!(obj["name"], "talon");
        } else {
            panic!("expected Text, got {:?}", rows[0][1]);
        }
        // db: {"host":"localhost","port":"5432"}
        if let crate::types::Value::Text(ref s) = rows[1][1] {
            let obj: serde_json::Value = serde_json::from_str(s).unwrap();
            assert_eq!(obj["host"], "localhost");
            assert_eq!(obj["port"], "5432");
        } else {
            panic!("expected Text");
        }
    }

    #[test]
    fn json_objectagg_no_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE kvt (id INT, k TEXT, v INT)")
            .unwrap();
        db.run_sql("INSERT INTO kvt VALUES (1, 'a', 1)").unwrap();
        db.run_sql("INSERT INTO kvt VALUES (2, 'b', 2)").unwrap();
        db.run_sql("INSERT INTO kvt VALUES (3, 'c', 3)").unwrap();
        let rows = db.run_sql("SELECT JSON_OBJECTAGG(k, v) FROM kvt").unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Text(ref s) = rows[0][0] {
            let obj: serde_json::Value = serde_json::from_str(s).unwrap();
            assert_eq!(obj["a"], 1);
            assert_eq!(obj["b"], 2);
            assert_eq!(obj["c"], 3);
        } else {
            panic!("expected Text, got {:?}", rows[0][0]);
        }
    }

    #[test]
    fn json_arrayagg_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE mixed (id INT, val TEXT)").unwrap();
        db.run_sql("INSERT INTO mixed VALUES (1, 'hello')").unwrap();
        db.run_sql("INSERT INTO mixed VALUES (2, NULL)").unwrap();
        db.run_sql("INSERT INTO mixed VALUES (3, 'world')").unwrap();
        let rows = db.run_sql("SELECT JSON_ARRAYAGG(val) FROM mixed").unwrap();
        assert_eq!(rows.len(), 1);
        if let crate::types::Value::Text(ref s) = rows[0][0] {
            let arr: Vec<serde_json::Value> = serde_json::from_str(s).unwrap();
            assert_eq!(arr.len(), 3); // NULL 也包含在数组中
            assert!(arr.contains(&serde_json::Value::Null));
        } else {
            panic!("expected Text, got {:?}", rows[0][0]);
        }
    }

    // ── M155: STRING_AGG (PostgreSQL 兼容别名) ──

    #[test]
    fn string_agg_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE sa1 (id INT, cat TEXT, name TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO sa1 VALUES (1, 'A', 'alice')")
            .unwrap();
        db.run_sql("INSERT INTO sa1 VALUES (2, 'A', 'bob')")
            .unwrap();
        db.run_sql("INSERT INTO sa1 VALUES (3, 'B', 'carol')")
            .unwrap();
        let rows = db
            .run_sql("SELECT cat, STRING_AGG(name, ', ') FROM sa1 GROUP BY cat ORDER BY cat")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], crate::types::Value::Text("alice, bob".into()));
        assert_eq!(rows[1][1], crate::types::Value::Text("carol".into()));
    }

    #[test]
    fn string_agg_no_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE sa2 (id INT, word TEXT)").unwrap();
        db.run_sql("INSERT INTO sa2 VALUES (1, 'hello')").unwrap();
        db.run_sql("INSERT INTO sa2 VALUES (2, 'world')").unwrap();
        let rows = db.run_sql("SELECT STRING_AGG(word, ' ') FROM sa2").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], crate::types::Value::Text("hello world".into()));
    }

    #[test]
    fn string_agg_default_sep() {
        // STRING_AGG 不带分隔符时默认逗号（宽容处理）
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE sa3 (id INT, tag TEXT)").unwrap();
        db.run_sql("INSERT INTO sa3 VALUES (1, 'a')").unwrap();
        db.run_sql("INSERT INTO sa3 VALUES (2, 'b')").unwrap();
        let rows = db.run_sql("SELECT STRING_AGG(tag) FROM sa3").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], crate::types::Value::Text("a,b".into()));
    }

    // ── M156: BOOL_AND / BOOL_OR 布尔聚合 ──

    #[test]
    fn bool_and_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE flags (id INT, grp TEXT, active BOOLEAN)")
            .unwrap();
        db.run_sql("INSERT INTO flags VALUES (1, 'A', TRUE)")
            .unwrap();
        db.run_sql("INSERT INTO flags VALUES (2, 'A', TRUE)")
            .unwrap();
        db.run_sql("INSERT INTO flags VALUES (3, 'B', TRUE)")
            .unwrap();
        db.run_sql("INSERT INTO flags VALUES (4, 'B', FALSE)")
            .unwrap();
        let rows = db
            .run_sql("SELECT grp, BOOL_AND(active) FROM flags GROUP BY grp ORDER BY grp")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // A: all true → true
        assert_eq!(rows[0][1], crate::types::Value::Boolean(true));
        // B: one false → false
        assert_eq!(rows[1][1], crate::types::Value::Boolean(false));
    }

    #[test]
    fn bool_and_no_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE checks (id INT, ok BOOLEAN)")
            .unwrap();
        db.run_sql("INSERT INTO checks VALUES (1, TRUE)").unwrap();
        db.run_sql("INSERT INTO checks VALUES (2, TRUE)").unwrap();
        db.run_sql("INSERT INTO checks VALUES (3, TRUE)").unwrap();
        let rows = db.run_sql("SELECT BOOL_AND(ok) FROM checks").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], crate::types::Value::Boolean(true));
    }

    #[test]
    fn bool_or_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE perms (id INT, role TEXT, can_write BOOLEAN)")
            .unwrap();
        db.run_sql("INSERT INTO perms VALUES (1, 'admin', TRUE)")
            .unwrap();
        db.run_sql("INSERT INTO perms VALUES (2, 'admin', FALSE)")
            .unwrap();
        db.run_sql("INSERT INTO perms VALUES (3, 'guest', FALSE)")
            .unwrap();
        db.run_sql("INSERT INTO perms VALUES (4, 'guest', FALSE)")
            .unwrap();
        let rows = db
            .run_sql("SELECT role, BOOL_OR(can_write) FROM perms GROUP BY role ORDER BY role")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // admin: one true → true
        assert_eq!(rows[0][1], crate::types::Value::Boolean(true));
        // guest: all false → false
        assert_eq!(rows[1][1], crate::types::Value::Boolean(false));
    }

    #[test]
    fn bool_or_all_null() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE nullflags (id INT, flag BOOLEAN)")
            .unwrap();
        db.run_sql("INSERT INTO nullflags VALUES (1, NULL)")
            .unwrap();
        db.run_sql("INSERT INTO nullflags VALUES (2, NULL)")
            .unwrap();
        let rows = db.run_sql("SELECT BOOL_OR(flag) FROM nullflags").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], crate::types::Value::Null);
    }
}
