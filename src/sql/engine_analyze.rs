/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M197：ANALYZE TABLE — 收集表级统计信息用于查询优化。
//!
//! 实现参考 PostgreSQL ANALYZE / SQLite ANALYZE 的核心逻辑：
//! - 全表扫描一次，收集每列的统计信息
//! - 统计信息包括：行数、空值数、min/max、NDV（近似区分度）
//! - 返回统计结果作为查询结果集（便于用户查看）
//!
//! 统计信息可用于优化器做基于成本的查询优化（CBO）：
//! - 高 NDV 列适合索引扫描
//! - 低 NDV 列适合全表扫描
//! - min/max 用于范围查询边界判断

use super::engine::SqlEngine;
use crate::types::Value;
use crate::Error;

/// 单列统计信息。
#[derive(Debug)]
struct ColAnalysis {
    name: String,
    row_count: u64,
    null_count: u64,
    /// 近似 NDV（Number of Distinct Values），使用 HashSet 计数。
    distinct_count: u64,
    min_val: Option<Value>,
    max_val: Option<Value>,
}

impl SqlEngine {
    /// 执行 ANALYZE table：全表扫描收集统计信息并返回结果集。
    ///
    /// 返回格式（每行一列）：
    /// `[column_name, row_count, null_count, distinct_count, min, max, selectivity]`
    pub(super) fn exec_analyze(&mut self, table: &str) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("表不存在: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let schema = tc.schema.clone();
        let col_count = schema.columns.len();

        // 初始化每列的分析状态
        let mut analyses: Vec<ColAnalysis> = schema
            .columns
            .iter()
            .map(|(name, _)| ColAnalysis {
                name: name.clone(),
                row_count: 0,
                null_count: 0,
                distinct_count: 0,
                min_val: None,
                max_val: None,
            })
            .collect();

        // 每列的 distinct 值集合（使用 hash 去重，内存友好）
        let mut distinct_sets: Vec<std::collections::HashSet<u64>> =
            (0..col_count).map(|_| std::collections::HashSet::new()).collect();

        // 全表扫描一次
        self.tx_for_each_row(table, |row| {
            for (ci, val) in row.iter().enumerate() {
                if ci >= col_count {
                    break;
                }
                let a = &mut analyses[ci];
                a.row_count += 1;

                if matches!(val, Value::Null) {
                    a.null_count += 1;
                    continue;
                }

                // NDV: 用值的 hash 近似统计
                let hash = hash_value(val);
                distinct_sets[ci].insert(hash);

                // min/max 更新
                if a.min_val.is_none() || val_lt(val, a.min_val.as_ref().unwrap()) {
                    a.min_val = Some(val.clone());
                }
                if a.max_val.is_none() || val_gt(val, a.max_val.as_ref().unwrap()) {
                    a.max_val = Some(val.clone());
                }
            }
            Ok(true)
        })?;

        // 收集 distinct count
        for (ci, a) in analyses.iter_mut().enumerate() {
            a.distinct_count = distinct_sets[ci].len() as u64;
        }

        // 构建结果集：每行7列 [col, rows, nulls, distinct, min, max, selectivity]
        let mut rows = Vec::with_capacity(col_count);
        for a in &analyses {
            let selectivity = if a.row_count > 0 && a.distinct_count > 0 {
                a.distinct_count as f64 / a.row_count as f64
            } else {
                0.0
            };
            rows.push(vec![
                Value::Text(a.name.clone()),
                Value::Integer(a.row_count as i64),
                Value::Integer(a.null_count as i64),
                Value::Integer(a.distinct_count as i64),
                a.min_val.clone().unwrap_or(Value::Null),
                a.max_val.clone().unwrap_or(Value::Null),
                Value::Float((selectivity * 10000.0).round() / 10000.0), // 4 位精度
            ]);
        }
        Ok(rows)
    }
}

/// 计算值的 hash（用于 NDV 估算）。
fn hash_value(val: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match val {
        Value::Integer(n) => n.hash(&mut hasher),
        Value::Float(f) => {
            // f64 不实现 Hash，用 bits 替代
            f.to_bits().hash(&mut hasher);
        }
        Value::Text(s) => s.hash(&mut hasher),
        Value::Boolean(b) => b.hash(&mut hasher),
        Value::Blob(b) => b.hash(&mut hasher),
        Value::Timestamp(t) => t.hash(&mut hasher),
        Value::Date(d) => d.hash(&mut hasher),
        Value::Time(t) => t.hash(&mut hasher),
        Value::GeoPoint(lat, lng) => {
            lat.to_bits().hash(&mut hasher);
            lng.to_bits().hash(&mut hasher);
        }
        Value::Jsonb(s) => s.hash(&mut hasher),
        Value::Vector(v) => {
            for f in v {
                f.to_bits().hash(&mut hasher);
            }
        }
        Value::Null => 0u8.hash(&mut hasher),
        Value::Placeholder(_) => 0u8.hash(&mut hasher),
    }
    hasher.finish()
}

/// 值小于比较。
fn val_lt(a: &Value, b: &Value) -> bool {
    super::helpers::value_cmp(a, b) == Some(std::cmp::Ordering::Less)
}

/// 值大于比较。
fn val_gt(a: &Value, b: &Value) -> bool {
    super::helpers::value_cmp(a, b) == Some(std::cmp::Ordering::Greater)
}
