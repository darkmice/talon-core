/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine 快速路径：INSERT 单行快速插入。
//! 从 engine.rs 拆分，保持单文件 ≤500 行。

use super::engine::SqlEngine;
use super::helpers::{fast_parse_row, fast_parse_value, find_close_paren};
use crate::types::Value;
use crate::Error;

/// 零分配大小写无关关键字搜索（替代 to_ascii_uppercase + contains）。
fn contains_keyword_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
}

impl SqlEngine {
    /// 快速路径：检测 `SELECT * FROM <table> WHERE <col> = <val>` 模式。
    /// 仅处理单条件 Eq + 主键列 + 无 ORDER BY/LIMIT 的情况。
    pub(super) fn try_fast_pk_select(
        &mut self,
        sql: &str,
    ) -> Result<Option<Vec<Vec<Value>>>, Error> {
        let sql = sql.trim().trim_end_matches(';').trim();
        let bytes = sql.as_bytes();
        if bytes.len() < 20 || !bytes[..6].eq_ignore_ascii_case(b"SELECT") {
            return Ok(None);
        }
        let rest = &sql[6..].trim_start();
        if rest.as_bytes().first().copied() != Some(b'*') {
            return Ok(None);
        }
        let rest = rest[1..].trim_start();
        if rest.len() < 5 || !rest.as_bytes()[..4].eq_ignore_ascii_case(b"FROM") {
            return Ok(None);
        }
        let rest = rest[4..].trim_start();
        let table_end = rest.find(|c: char| c.is_whitespace()).unwrap_or(0);
        if table_end == 0 {
            return Ok(None);
        }
        let table = rest[..table_end].trim_matches('`');
        let rest = rest[table_end..].trim_start();
        if rest.len() < 6 || !rest.as_bytes()[..5].eq_ignore_ascii_case(b"WHERE") {
            return Ok(None);
        }
        let rest = rest[5..].trim_start();
        if contains_keyword_ci(rest.as_bytes(), b" AND ")
            || contains_keyword_ci(rest.as_bytes(), b" OR ")
        {
            return Ok(None);
        }
        let eq_pos = match rest.find('=') {
            Some(p) => p,
            None => return Ok(None),
        };
        if eq_pos > 0 && matches!(rest.as_bytes()[eq_pos - 1], b'!' | b'<' | b'>') {
            return Ok(None);
        }
        let col = rest[..eq_pos].trim();
        let val_str = rest[eq_pos + 1..].trim();
        let pk_val = match fast_parse_value(val_str) {
            Some(v) => v,
            None => return Ok(None),
        };
        if !self.ensure_cached(table)? {
            return Ok(None);
        }
        let tc = self.cache.get(table).unwrap();
        if tc.schema.column_index_by_name(col) != Some(0) {
            return Ok(None);
        }
        let key = pk_val.to_bytes()?;
        match self.tx_get(table, &key)? {
            Some(raw) => {
                let row = self.cache.get(table).unwrap().schema.decode_row(&raw)?;
                Ok(Some(vec![row]))
            }
            None => Ok(Some(vec![])),
        }
    }

    /// M123-E：快速路径 INSERT — 支持有列名 + 有索引 + 事务内。
    /// 覆盖 `INSERT INTO <table> [(col1,...)] VALUES (<vals>)` 单行模式。
    /// 也支持 `INSERT OR REPLACE INTO ...`。
    pub(super) fn try_fast_insert(&mut self, sql: &str) -> Result<Option<Vec<Vec<Value>>>, Error> {
        let sql = sql.trim().trim_end_matches(';').trim();
        let bytes = sql.as_bytes();
        if bytes.len() < 20 || !bytes[..6].eq_ignore_ascii_case(b"INSERT") {
            return Ok(None);
        }
        let after_insert = sql[6..].trim_start();
        let (or_replace, or_ignore, rest) = if after_insert
            .as_bytes()
            .get(..2)
            .is_some_and(|b| b.eq_ignore_ascii_case(b"OR"))
        {
            let after_or = after_insert[2..].trim_start();
            let kw_end = after_or
                .find(|c: char| c.is_whitespace())
                .unwrap_or(after_or.len());
            let kw = &after_or[..kw_end];
            let is_replace = kw.eq_ignore_ascii_case("REPLACE");
            let is_ignore = kw.eq_ignore_ascii_case("IGNORE");
            if !is_replace && !is_ignore {
                return Ok(None);
            }
            let after_kw = after_or[kw_end..].trim_start();
            if !after_kw
                .as_bytes()
                .get(..4)
                .is_some_and(|b| b.eq_ignore_ascii_case(b"INTO"))
            {
                return Ok(None);
            }
            (is_replace, is_ignore, after_kw[4..].trim_start())
        } else if after_insert
            .as_bytes()
            .get(..4)
            .is_some_and(|b| b.eq_ignore_ascii_case(b"INTO"))
        {
            (false, false, after_insert[4..].trim_start())
        } else {
            return Ok(None);
        };
        let table_end = rest
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(0);
        if table_end == 0 {
            return Ok(None);
        }
        let table = rest[..table_end].trim_matches('`');
        let rest = rest[table_end..].trim_start();
        // M123-E：支持 (col1, col2, ...) VALUES (...) 格式
        // 跳过列名列表，直接定位到 VALUES 后的值
        let rest = if rest.starts_with('(') {
            // 可能是列名列表或直接的 VALUES(...)
            // 检测括号后是否跟 VALUES 关键字
            let close = match find_close_paren(rest) {
                Some(p) => p,
                None => return Ok(None),
            };
            let after_paren = rest[close + 1..].trim_start();
            if after_paren.len() >= 6 && after_paren.as_bytes()[..6].eq_ignore_ascii_case(b"VALUES")
            {
                // 有列名列表 → 回退慢路径（列顺序可能不同于 schema，快速路径无法重排）
                return Ok(None);
            } else {
                // 没有 VALUES → 这个括号就是 VALUES 的值括号
                rest
            }
        } else {
            rest
        };
        if !rest
            .as_bytes()
            .get(..6)
            .is_some_and(|b| b.eq_ignore_ascii_case(b"VALUES"))
        {
            return Ok(None);
        }
        let rest = rest[6..].trim_start();
        if !rest.starts_with('(') {
            return Ok(None);
        }
        let close = match find_close_paren(rest) {
            Some(p) => p,
            None => return Ok(None),
        };
        let after = rest[close + 1..].trim();
        if !after.is_empty() && after != ";" {
            return Ok(None);
        }
        let inner = &rest[1..close];
        // M123-F：单 pass 解析，合并 split + parse 为一次遍历
        let mut row = match fast_parse_row(inner) {
            Some(r) => r,
            None => return Ok(None),
        };
        if !self.ensure_cached(table)? {
            return Ok(None);
        }
        let tc = self.cache.get(table).unwrap();
        // M123-E：列数不匹配（部分列 INSERT + DEFAULT）→ 回退慢路径
        if row.len() != tc.schema.visible_column_count() {
            return Ok(None);
        }
        // M127：有外键约束 → 回退慢路径（FK 检查需要 &mut self）
        if !tc.schema.foreign_keys.is_empty() {
            return Ok(None);
        }
        // M104: AUTOINCREMENT — 快速路径也需要自动分配主键
        if tc.schema.auto_increment
            && !row.is_empty()
            && (row[0] == Value::Null || row[0] == Value::Integer(0))
        {
            // 回退慢路径，让 exec_insert 处理 auto-increment 逻辑
            return Ok(None);
        }
        // M104: AUTOINCREMENT — 显式 ID 也需更新高水位计数器
        if tc.schema.auto_increment {
            if let Some(Value::Integer(v)) = row.first() {
                let counter_key = format!("autoincr:{}", table);
                let cur: i64 = self
                    .meta_ks
                    .get(counter_key.as_bytes())?
                    .map(|raw| {
                        if raw.len() == 8 {
                            i64::from_be_bytes(raw[..8].try_into().unwrap_or([0; 8]))
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0);
                if *v > cur {
                    self.meta_ks.set(counter_key.as_bytes(), &v.to_be_bytes())?;
                }
            }
        }
        tc.schema.coerce_types(&mut row);
        tc.schema.validate_row(&row)?;
        // M118：CHECK 约束校验（快速 INSERT 路径）
        if !tc.parsed_checks.is_empty() {
            super::helpers::validate_check_constraints(
                &row,
                &tc.schema,
                &tc.parsed_checks,
                &tc.schema.check_constraints,
            )?;
        }
        let pk = row
            .first()
            .ok_or_else(|| Error::SqlExec("INSERT row is empty".into()))?;
        let key = pk.to_bytes()?;
        if !or_replace && self.tx_get(table, &key)?.is_some() {
            if or_ignore {
                return Ok(Some(vec![]));
            }
            return Err(Error::SqlExec(format!("duplicate primary key: {:?}", pk)));
        }
        // M111：快速路径唯一索引约束检查 — M112：支持复合索引
        let tc = self.cache.get(table).unwrap();
        if !tc.unique_indexes.is_empty() {
            let exclude_pk = if or_replace {
                Some(key.as_slice())
            } else {
                None
            };
            let tx_writes = self.tx.as_ref().map(|tx| tx.index_writes.as_slice());
            for ui_col in &tc.unique_indexes {
                if let Some(col_indices) =
                    super::engine_exec::resolve_col_indices(&tc.schema, ui_col)
                {
                    if let Some(idx_ks) = tc.index_keyspaces.get(ui_col) {
                        let vals: Vec<&Value> = col_indices.iter().map(|&i| &row[i]).collect();
                        let result = if let Some(writes) = tx_writes {
                            super::index_key::check_unique_violation_tx_composite(
                                idx_ks, writes, table, ui_col, &vals, exclude_pk,
                            )
                        } else {
                            super::index_key::check_unique_violation_composite(
                                idx_ks, &vals, exclude_pk,
                            )
                        };
                        if let Err(e) = result {
                            if or_ignore {
                                return Ok(Some(vec![]));
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
        let schema = tc.schema.clone();
        let raw = schema.encode_row(&row)?;
        let has_idx = !tc.index_keyspaces.is_empty();
        let in_tx = self.tx.is_some();

        // M123-E：统一写入路径（事务/非事务 × 有索引/无索引）— M112：复合索引
        if has_idx && !in_tx {
            let tc = self.cache.get(table).unwrap();
            let mut batch = self.store.batch();
            if or_replace {
                if let Some(old_raw) = tc.data_ks.get(&key)? {
                    let old_row = schema.decode_row(&old_raw)?;
                    let old_pk = old_row.first().unwrap();
                    for (cols_key, idx_ks) in &tc.index_keyspaces {
                        if let Some(ci) = super::engine_exec::resolve_col_indices(&schema, cols_key)
                        {
                            batch.remove(
                                idx_ks,
                                super::engine_exec::build_idx_key(&old_row, &ci, old_pk)?,
                            );
                        }
                    }
                }
            }
            batch.insert(&tc.data_ks, key, raw)?;
            for (cols_key, idx_ks) in &tc.index_keyspaces {
                if let Some(ci) = super::engine_exec::resolve_col_indices(&schema, cols_key) {
                    let idx_key = super::engine_exec::build_idx_key(&row, &ci, pk)?;
                    batch.insert(idx_ks, idx_key, Vec::new())?;
                }
            }
            batch.commit()?;
        } else if has_idx && in_tx {
            self.tx_set(table, key, raw)?;
            let tc = self.cache.get(table).unwrap();
            for cols_key in tc.index_keyspaces.keys().cloned().collect::<Vec<_>>() {
                if let Some(ci) = super::engine_exec::resolve_col_indices(&schema, &cols_key) {
                    let idx_key = super::engine_exec::build_idx_key(&row, &ci, pk)?;
                    self.tx_index_set(table, &cols_key, idx_key)?;
                }
            }
        } else {
            // 无索引
            self.tx_set(table, key, raw)?;
        }
        let stats = self.column_stats.entry(table.to_string()).or_default();
        super::engine::accumulate_stats(stats, &row);
        let hv = self.cache.get(table).is_some_and(|c| c.has_vec_indexes);
        super::vec_idx::sync_vec_on_insert(&self.store, table, &[row], &schema, hv)?;
        Ok(Some(vec![]))
    }
}
