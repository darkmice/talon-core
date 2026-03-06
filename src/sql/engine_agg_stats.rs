/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M93 方案B：表级列运行统计 O(1) 聚合快速路径。

use super::engine::SqlEngine;
use super::helpers::AggType;
use crate::types::{Schema, Value};

impl SqlEngine {
    /// M93 方案B：从 column_stats 缓存读取 O(1) 聚合结果。
    /// 返回 Some(row) 表示命中缓存，None 表示回退到全扫描。
    pub(super) fn try_stats_fast(
        &self,
        table: &str,
        aggs: &[(AggType, String)],
        schema: &Schema,
    ) -> Option<Vec<Value>> {
        let table_stats = self.column_stats.get(table)?;
        let mut result = Vec::with_capacity(aggs.len());
        for (agg_type, col_name) in aggs {
            if col_name == "*" {
                match agg_type {
                    AggType::Count => {
                        let count: i64 = table_stats.values().next().map(|s| s.count).unwrap_or(0);
                        result.push(Value::Integer(count));
                    }
                    _ => return None,
                }
            } else {
                let ci = schema.column_index_by_name(col_name)?;
                let cs = table_stats.get(&ci)?;
                match agg_type {
                    AggType::Sum => {
                        if cs.is_int {
                            result.push(Value::Integer(cs.sum as i64));
                        } else {
                            result.push(Value::Float(cs.sum));
                        }
                    }
                    AggType::Avg => {
                        if cs.count > 0 {
                            result.push(Value::Float(cs.sum / cs.count as f64));
                        } else {
                            result.push(Value::Null);
                        }
                    }
                    AggType::Count => {
                        result.push(Value::Integer(cs.count));
                    }
                    _ => return None,
                }
            }
        }
        Some(result)
    }
}

/// R-PERF-1: 累加行数据到列统计（纯函数，无堆分配）。
pub(super) fn accumulate_stats(
    stats: &mut std::collections::HashMap<usize, super::engine::ColumnStats>,
    row: &[Value],
) {
    for (ci, val) in row.iter().enumerate() {
        let (v, is_int) = match val {
            Value::Integer(n) => (*n as f64, true),
            Value::Float(f) => (*f, false),
            _ => continue,
        };
        let cs = stats.entry(ci).or_insert(super::engine::ColumnStats {
            sum: 0.0,
            count: 0,
            is_int,
        });
        cs.sum += v;
        cs.count += 1;
        if !is_int {
            cs.is_int = false;
        }
    }
}

/// M93：从 col_indices（聚合列索引，None=COUNT(*)）构建稀疏解码映射。
pub(super) fn build_sparse_map(col_indices: &[Option<usize>]) -> (Vec<usize>, Vec<Option<usize>>) {
    let mut targets: Vec<usize> = col_indices.iter().filter_map(|&ci| ci).collect();
    targets.sort_unstable();
    targets.dedup();
    let remap: std::collections::HashMap<usize, usize> = targets
        .iter()
        .enumerate()
        .map(|(pos, &orig)| (orig, pos))
        .collect();
    let sparse_indices: Vec<Option<usize>> = col_indices
        .iter()
        .map(|ci| ci.and_then(|c| remap.get(&c).copied()))
        .collect();
    (targets, sparse_indices)
}
