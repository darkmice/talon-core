/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 嵌入式向量搜索执行：对 SELECT 结果行计算向量距离并排序。
//!
//! 支持 `vec_distance(col, [...])` / `vec_cosine` / `vec_l2` / `vec_dot`。
//! 距离列追加到每行末尾，ORDER BY alias 时按距离排序。

use std::collections::BinaryHeap;

use crate::cross::distance::{cosine_distance, dot_distance, l2_distance};
use crate::types::{Schema, Value};
use crate::Error;

use super::parser::VecSearchExpr;

/// 对行集执行向量搜索：计算距离列、排序、截断、投影。
///
/// 流程：
/// 1. 从每行的向量列提取 f32 数据
/// 2. 计算与 query_vec 的距离
/// 3. 将距离追加为行末尾列
/// 4. 如果 ORDER BY 引用了距离别名，按距离排序
/// 5. 应用 LIMIT
/// 6. 投影输出列
pub(super) fn exec_vec_search(
    rows: Vec<Vec<Value>>,
    columns: &[String],
    schema: &Schema,
    vs: &VecSearchExpr,
    order_by: Option<&[(String, bool, Option<bool>)]>,
    limit: Option<u64>,
) -> Result<Vec<Vec<Value>>, Error> {
    let col_idx = schema
        .column_index_by_name(&vs.column)
        .ok_or_else(|| Error::SqlExec(format!("向量列不存在: {}", vs.column)))?;

    // "distance" 表示使用索引定义的度量，需要从外部传入已解析的 metric
    let resolved_metric = vs.metric.as_str();
    let dist_fn = match resolved_metric {
        "cosine" | "distance" => cosine_distance,
        "l2" => l2_distance,
        "dot" => dot_distance,
        other => return Err(Error::SqlExec(format!("未知向量距离度量: {}", other))),
    };

    // ORDER BY 距离判断（提前，用于选择 Top-K 路径）
    let first_ob = order_by.and_then(|ob| ob.first());
    let order_by_dist = match (first_ob, &vs.alias) {
        (Some((col, _, _)), Some(alias)) if col == alias => true,
        (Some((col, _, _)), _) if col == "dist" || col == "distance" || col == "score" => true,
        _ => false,
    };
    let desc = first_ob.map(|(_, d, _)| *d).unwrap_or(false);

    // M98 优化：ORDER BY 距离 + LIMIT 时走 Top-K 堆路径
    // O(N log K) 时间、O(K) 内存，替代 O(N log N) / O(N) 全量排序
    let scored_rows = if let (true, Some(lim)) = (order_by_dist, limit) {
        let k = lim as usize;
        topk_vec_search(&rows, col_idx, &vs.query_vec, dist_fn, k, desc)?
    } else {
        let mut all: Vec<(Vec<Value>, f32)> = Vec::with_capacity(rows.len());
        for row in rows {
            let vec_data = extract_vec_f32(&row[col_idx])?;
            if vec_data.len() != vs.query_vec.len() {
                return Err(Error::VectorDimMismatch(vs.query_vec.len(), vec_data.len()));
            }
            let dist = dist_fn(&vs.query_vec, &vec_data);
            all.push((row, dist));
        }
        if order_by_dist {
            all.sort_by(|a, b| {
                let cmp = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
                if desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
        if let Some(n) = limit {
            all.truncate(n as usize);
        }
        all
    };

    // 构建输出行：投影普通列 + 距离列
    let output_col_indices = resolve_output_columns(columns, schema, vs)?;
    let result: Vec<Vec<Value>> = scored_rows
        .into_iter()
        .map(|(row, dist)| {
            let mut out = Vec::with_capacity(output_col_indices.len());
            for &oci in &output_col_indices {
                match oci {
                    OutputCol::Schema(i) => out.push(row[i].clone()),
                    OutputCol::Distance => out.push(Value::Float(dist as f64)),
                }
            }
            out
        })
        .collect();

    Ok(result)
}

/// 输出列类型：来自 schema 的列或距离计算列。
#[derive(Clone, Copy)]
enum OutputCol {
    /// schema 中的列索引。
    Schema(usize),
    /// 向量距离计算列。
    Distance,
}

/// 解析 SELECT 列列表，确定输出列顺序。
/// `columns` 中的 "*" 展开为全部 schema 列，距离别名映射为 Distance。
fn resolve_output_columns(
    columns: &[String],
    schema: &Schema,
    vs: &VecSearchExpr,
) -> Result<Vec<OutputCol>, Error> {
    let mut result = Vec::new();
    for col in columns {
        let trimmed = col.trim();
        if trimmed == "*" {
            for i in 0..schema.columns.len() {
                result.push(OutputCol::Schema(i));
            }
            // * 展开后追加距离列
            result.push(OutputCol::Distance);
        } else if is_distance_alias(trimmed, vs) {
            result.push(OutputCol::Distance);
        } else if let Some(idx) = schema.column_index_by_name(trimmed) {
            result.push(OutputCol::Schema(idx));
        } else {
            return Err(Error::SqlExec(format!("SELECT 列不存在: {}", trimmed)));
        }
    }
    // 如果没有显式包含距离列且不是 *，追加距离列
    if !result.iter().any(|c| matches!(c, OutputCol::Distance)) {
        result.push(OutputCol::Distance);
    }
    Ok(result)
}

/// 检查列名是否是距离别名。
fn is_distance_alias(col: &str, vs: &VecSearchExpr) -> bool {
    if let Some(ref alias) = vs.alias {
        col.eq_ignore_ascii_case(alias)
    } else {
        col.eq_ignore_ascii_case("dist")
            || col.eq_ignore_ascii_case("distance")
            || col.eq_ignore_ascii_case("score")
    }
}

/// 从 Value 中提取 f32 向量数据。
fn extract_vec_f32(value: &Value) -> Result<Vec<f32>, Error> {
    match value {
        Value::Vector(v) => Ok(v.clone()),
        Value::Null => Err(Error::SqlExec("向量列值为 NULL".into())),
        _ => Err(Error::SqlExec(format!("列类型不是 VECTOR: {:?}", value))),
    }
}

/// M98：Top-K 堆向量搜索 — O(N log K) 时间、O(K) 内存。
/// 对 N 行数据只维护 K 个最近（或最远）结果，避免全量排序。
fn topk_vec_search(
    rows: &[Vec<Value>],
    col_idx: usize,
    query_vec: &[f32],
    dist_fn: fn(&[f32], &[f32]) -> f32,
    k: usize,
    desc: bool,
) -> Result<Vec<(Vec<Value>, f32)>, Error> {
    // desc=false（升序，最小距离优先）→ max-heap 淘汰最大
    // desc=true（降序，最大值优先）→ min-heap 淘汰最小
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for row in rows {
        let vec_data = extract_vec_f32(&row[col_idx])?;
        if vec_data.len() != query_vec.len() {
            return Err(Error::VectorDimMismatch(query_vec.len(), vec_data.len()));
        }
        let dist = dist_fn(query_vec, &vec_data);
        let item = HeapItem {
            dist,
            desc,
            row: row.clone(),
        };
        if heap.len() < k {
            heap.push(item);
        } else if let Some(top) = heap.peek() {
            let dominated = if desc {
                dist > top.dist
            } else {
                dist < top.dist
            };
            if dominated {
                heap.pop();
                heap.push(item);
            }
        }
    }
    let mut result: Vec<(Vec<Value>, f32)> =
        heap.into_iter().map(|item| (item.row, item.dist)).collect();
    result.sort_by(|a, b| {
        let cmp = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
        if desc {
            cmp.reverse()
        } else {
            cmp
        }
    });
    Ok(result)
}

/// Top-K 堆元素：按距离排序。
/// desc=false → max-heap（堆顶是最大距离，被淘汰的是最远的）
/// desc=true → min-heap（堆顶是最小距离，被淘汰的是最近的）
struct HeapItem {
    dist: f32,
    desc: bool,
    row: Vec<Value>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // desc=false: max-heap（大的在堆顶）→ 正常比较
        // desc=true: min-heap（小的在堆顶）→ 反转比较
        let cmp = self
            .dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal);
        if self.desc {
            cmp.reverse()
        } else {
            cmp
        }
    }
}
