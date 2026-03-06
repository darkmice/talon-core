/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M177：窗口函数执行器。
//!
//! 在基础查询行收集完成后，计算窗口函数并注入额外列。
//! 流程：分区 → 排序 → 计算 → 注入列到每行末尾。

use super::helpers::value_cmp;
use super::parser::{WindowExpr, WindowFuncKind};
use crate::types::{Schema, Value};
use crate::Error;

/// 投影：从扩展行（原始列 + 窗口列）中选取用户请求的列。
///
/// 窗口函数列追加在原始 schema 列之后，通过别名查找。
/// `columns` 为 `["*"]` 时返回扩展后的全部列。
pub(super) fn project_with_window(
    rows: Vec<Vec<Value>>,
    columns: &[String],
    schema: &Schema,
    win_fns: &[WindowExpr],
) -> Result<Vec<Vec<Value>>, Error> {
    if columns.len() == 1 && columns[0] == "*" {
        return Ok(rows);
    }
    let base_col_count = schema.columns.len();
    // 展开列索引列表（`*` 展开为所有 schema 列）
    let mut indices: Vec<usize> = Vec::with_capacity(columns.len() + base_col_count);
    for col in columns {
        if col == "*" {
            // 展开 `*` 为所有 schema 列
            for i in 0..base_col_count {
                indices.push(i);
            }
        } else if let Some(idx) = schema.column_index_by_name(col) {
            indices.push(idx);
        } else {
            // 查窗口函数别名
            let mut found = false;
            for (i, wf) in win_fns.iter().enumerate() {
                if wf.alias == *col {
                    indices.push(base_col_count + i);
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(Error::SqlExec(format!("列不存在: {}", col)));
            }
        }
    }
    Ok(rows
        .iter()
        .map(|row| {
            indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect())
}

/// 执行窗口函数，返回扩展后的行（原始列 + 窗口函数列）。
///
/// `rows` 为基础查询结果（已完成 WHERE / JOIN / GROUP BY 等处理）。
/// 每个 `WindowExpr` 在每行末尾追加一列。
pub(super) fn apply_window_functions(
    mut rows: Vec<Vec<Value>>,
    schema: &Schema,
    win_fns: &[WindowExpr],
) -> Result<Vec<Vec<Value>>, Error> {
    if win_fns.is_empty() || rows.is_empty() {
        return Ok(rows);
    }

    // 为每个窗口函数计算结果列
    for wf in win_fns {
        let col_values = compute_window(schema, &rows, wf)?;
        // 追加到每行末尾
        for (i, row) in rows.iter_mut().enumerate() {
            row.push(col_values[i].clone());
        }
    }

    Ok(rows)
}

/// 计算单个窗口函数，返回每行对应的值。
fn compute_window(
    schema: &Schema,
    rows: &[Vec<Value>],
    wf: &WindowExpr,
) -> Result<Vec<Value>, Error> {
    let n = rows.len();
    let mut result = vec![Value::Null; n];

    // 构建分区键索引
    let partition_indices: Vec<usize> = wf
        .partition_by
        .iter()
        .map(|col| {
            schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("PARTITION BY 列不存在: {}", col)))
        })
        .collect::<Result<_, _>>()?;

    // 构建排序键索引
    let order_indices: Vec<(usize, bool)> = wf
        .order_by
        .iter()
        .map(|(col, desc)| {
            schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("ORDER BY 列不存在: {}", col)))
                .map(|idx| (idx, *desc))
        })
        .collect::<Result<_, _>>()?;

    // 构建行索引并按分区+排序键排序
    let mut indices: Vec<usize> = (0..n).collect();
    indices.sort_by(|&a, &b| {
        // 先按分区键
        for &pi in &partition_indices {
            let cmp = value_cmp(&rows[a][pi], &rows[b][pi]).unwrap_or(std::cmp::Ordering::Equal);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        // 再按排序键
        for &(oi, desc) in &order_indices {
            let cmp = value_cmp(&rows[a][oi], &rows[b][oi]).unwrap_or(std::cmp::Ordering::Equal);
            let cmp = if desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    // 分区：找到每个分区的边界
    let partitions = build_partitions(&indices, rows, &partition_indices);

    // 对每个分区计算窗口函数
    for partition in &partitions {
        compute_partition(schema, rows, wf, partition, &order_indices, &mut result)?;
    }

    Ok(result)
}

/// 构建分区：返回 Vec<Vec<usize>>，每个内部 Vec 是同一分区内的行原始索引（已排序）。
fn build_partitions(
    sorted_indices: &[usize],
    rows: &[Vec<Value>],
    partition_indices: &[usize],
) -> Vec<Vec<usize>> {
    if sorted_indices.is_empty() {
        return vec![];
    }
    if partition_indices.is_empty() {
        // 无分区 → 所有行在一个分区
        return vec![sorted_indices.to_vec()];
    }

    let mut partitions = Vec::new();
    let mut current = vec![sorted_indices[0]];

    for &idx in &sorted_indices[1..] {
        let prev = *current.last().unwrap();
        let same = partition_indices.iter().all(|&pi| {
            value_cmp(&rows[prev][pi], &rows[idx][pi]) == Some(std::cmp::Ordering::Equal)
        });
        if same {
            current.push(idx);
        } else {
            partitions.push(std::mem::take(&mut current));
            current.push(idx);
        }
    }
    if !current.is_empty() {
        partitions.push(current);
    }
    partitions
}

/// 在单个分区内计算窗口函数值。
fn compute_partition(
    schema: &Schema,
    rows: &[Vec<Value>],
    wf: &WindowExpr,
    partition: &[usize],
    order_indices: &[(usize, bool)],
    result: &mut [Value],
) -> Result<(), Error> {
    let n = partition.len();

    match &wf.func {
        WindowFuncKind::RowNumber => {
            for (rank, &orig_idx) in partition.iter().enumerate() {
                result[orig_idx] = Value::Integer((rank + 1) as i64);
            }
        }

        WindowFuncKind::Rank => {
            let mut rank = 1usize;
            result[partition[0]] = Value::Integer(1);
            for i in 1..n {
                if !order_equal(rows, partition[i], partition[i - 1], order_indices) {
                    rank = i + 1;
                }
                result[partition[i]] = Value::Integer(rank as i64);
            }
        }

        WindowFuncKind::DenseRank => {
            let mut rank = 1usize;
            result[partition[0]] = Value::Integer(1);
            for i in 1..n {
                if !order_equal(rows, partition[i], partition[i - 1], order_indices) {
                    rank += 1;
                }
                result[partition[i]] = Value::Integer(rank as i64);
            }
        }

        WindowFuncKind::Ntile(buckets) => {
            let buckets = *buckets.max(&1);
            for (i, &orig_idx) in partition.iter().enumerate() {
                let bucket = (i * buckets / n) + 1;
                result[orig_idx] = Value::Integer(bucket as i64);
            }
        }

        WindowFuncKind::Lag {
            col,
            offset,
            default,
        } => {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("LAG 列不存在: {}", col)))?;
            let def = default.clone().unwrap_or(Value::Null);
            for (i, &orig_idx) in partition.iter().enumerate() {
                result[orig_idx] = if i >= *offset {
                    rows[partition[i - offset]][col_idx].clone()
                } else {
                    def.clone()
                };
            }
        }

        WindowFuncKind::Lead {
            col,
            offset,
            default,
        } => {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("LEAD 列不存在: {}", col)))?;
            let def = default.clone().unwrap_or(Value::Null);
            for (i, &orig_idx) in partition.iter().enumerate() {
                result[orig_idx] = if i + offset < n {
                    rows[partition[i + offset]][col_idx].clone()
                } else {
                    def.clone()
                };
            }
        }

        WindowFuncKind::Count => {
            let count = Value::Integer(n as i64);
            for &orig_idx in partition {
                result[orig_idx] = count.clone();
            }
        }

        WindowFuncKind::Sum(col) => {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("SUM 列不存在: {}", col)))?;
            let sum = sum_values(partition, rows, col_idx);
            for &orig_idx in partition {
                result[orig_idx] = sum.clone();
            }
        }

        WindowFuncKind::Avg(col) => {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("AVG 列不存在: {}", col)))?;
            let sum = sum_values(partition, rows, col_idx);
            let avg = match sum {
                Value::Integer(s) => Value::Float(s as f64 / n as f64),
                Value::Float(s) => Value::Float(s / n as f64),
                _ => Value::Null,
            };
            for &orig_idx in partition {
                result[orig_idx] = avg.clone();
            }
        }

        WindowFuncKind::Min(col) => {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("MIN 列不存在: {}", col)))?;
            let mut min = Value::Null;
            for &orig_idx in partition {
                let v = &rows[orig_idx][col_idx];
                if matches!(v, Value::Null) {
                    continue;
                }
                if matches!(min, Value::Null)
                    || value_cmp(v, &min) == Some(std::cmp::Ordering::Less)
                {
                    min = v.clone();
                }
            }
            for &orig_idx in partition {
                result[orig_idx] = min.clone();
            }
        }

        WindowFuncKind::Max(col) => {
            let col_idx = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("MAX 列不存在: {}", col)))?;
            let mut max = Value::Null;
            for &orig_idx in partition {
                let v = &rows[orig_idx][col_idx];
                if matches!(v, Value::Null) {
                    continue;
                }
                if matches!(max, Value::Null)
                    || value_cmp(v, &max) == Some(std::cmp::Ordering::Greater)
                {
                    max = v.clone();
                }
            }
            for &orig_idx in partition {
                result[orig_idx] = max.clone();
            }
        }
    }

    Ok(())
}

/// 比较两行在 ORDER BY 键上是否相等（用于 RANK / DENSE_RANK）。
fn order_equal(rows: &[Vec<Value>], a: usize, b: usize, order_indices: &[(usize, bool)]) -> bool {
    order_indices
        .iter()
        .all(|&(oi, _)| value_cmp(&rows[a][oi], &rows[b][oi]) == Some(std::cmp::Ordering::Equal))
}

/// 对分区内指定列求和。
fn sum_values(partition: &[usize], rows: &[Vec<Value>], col_idx: usize) -> Value {
    let mut int_sum: i64 = 0;
    let mut float_sum: f64 = 0.0;
    let mut has_float = false;
    for &orig_idx in partition {
        match &rows[orig_idx][col_idx] {
            Value::Integer(v) => int_sum = int_sum.saturating_add(*v),
            Value::Float(v) => {
                float_sum += v;
                has_float = true;
            }
            _ => {}
        }
    }
    if has_float {
        Value::Float(float_sum + int_sum as f64)
    } else {
        Value::Integer(int_sum)
    }
}
