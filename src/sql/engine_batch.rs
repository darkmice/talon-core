/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine 批量插入：跳过 SQL 解析，直接 encode + WriteBatch。
//!
//! 从 engine.rs 拆分，减少单文件行数。

use crate::storage::Keyspace;
use crate::types::Value;
use crate::Error;

use super::engine::accumulate_stats;
use super::engine_exec::{build_idx_key, resolve_col_indices};

impl super::engine::SqlEngine {
    /// M93：原生批量插入 — 跳过 SQL 解析，直接 encode + WriteBatch（宽表 3-5x 提升）。
    pub fn batch_insert_rows(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<Value>>,
    ) -> Result<(), Error> {
        if !self.ensure_cached(table)? {
            return Err(Error::SqlExec(format!("table not found: {}", table)));
        }
        let tc = self.cache.get(table).unwrap();
        let schema = tc.schema.clone();
        // 列映射：将 columns 参数顺序映射到 schema 列顺序
        let col_map: Vec<usize> = if columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            columns
                .iter()
                .map(|c| {
                    schema
                        .column_index_by_name(c)
                        .ok_or_else(|| Error::SqlExec(format!("column not found: {}", c)))
                })
                .collect::<Result<_, _>>()?
        };
        let col_count = schema.columns.len();
        let data_ks = &tc.data_ks;
        // 收集索引列信息（支持复合索引）
        let idx_refs: Vec<(Vec<usize>, &Keyspace)> = tc
            .index_keyspaces
            .iter()
            .filter_map(|(col_name, ks)| resolve_col_indices(&schema, col_name).map(|ci| (ci, ks)))
            .collect();
        let stats = self.column_stats.entry(table.to_string()).or_default();
        let mut batch = self.store.batch();
        let mut full_row = vec![Value::Null; col_count];
        for mut input_row in rows {
            for v in full_row.iter_mut() {
                *v = Value::Null;
            }
            for (src_i, &dst_i) in col_map.iter().enumerate() {
                if src_i < input_row.len() {
                    std::mem::swap(&mut full_row[dst_i], &mut input_row[src_i]);
                }
            }
            let pk = full_row
                .first()
                .ok_or_else(|| Error::SqlExec("row is empty".into()))?;
            let key = pk.to_bytes()?;
            let raw = schema.encode_row(&full_row)?;
            batch.insert(data_ks, key.clone(), raw)?;
            for (ci, idx_ks) in &idx_refs {
                batch.insert(idx_ks, build_idx_key(&full_row, ci, pk)?, Vec::new())?;
            }
            accumulate_stats(stats, &full_row);
        }
        batch.commit()?;
        Ok(())
    }
}
