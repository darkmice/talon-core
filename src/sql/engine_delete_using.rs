/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M163: DELETE ... USING 跨表删除执行。
//!
//! 语法：`DELETE FROM t1 USING t2 WHERE t1.id = t2.id [AND ...]`
//! 实现：复用 `engine_update_from` 的跨表条件提取，扫描源表构建 lookup，
//! 遍历目标表匹配后删除。

use super::engine::SqlEngine;
use super::engine_exec::{build_idx_key, resolve_col_indices};
use super::engine_update_from::extract_cross_table_conditions;
use super::helpers::row_matches;
use super::parser::WhereExpr;
use crate::types::Value;
use crate::Error;
use std::collections::HashMap;

impl SqlEngine {
    /// M163: 执行 DELETE ... USING 跨表删除。
    ///
    /// 策略：
    /// 1. 从 WHERE 中提取跨表等值连接条件
    /// 2. 扫描源表构建 join_col → exists 的 lookup set
    /// 3. 遍历目标表，匹配行收集 PK 后批量删除
    pub(super) fn exec_delete_using(
        &mut self,
        target: &str,
        source: &str,
        where_clause: Option<&WhereExpr>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        if !self.ensure_cached(target)? {
            return Err(Error::SqlExec(format!("目标表不存在: {}", target)));
        }
        if !self.ensure_cached(source)? {
            return Err(Error::SqlExec(format!("源表不存在: {}", source)));
        }
        // 视图写保护
        if self.is_view(target)? {
            return Err(Error::SqlExec(format!(
                "视图是只读的，不能 DELETE: {}",
                target
            )));
        }
        self.invalidate_stats(target);
        let t_schema = self.cache.get(target).unwrap().schema.clone();
        let s_schema = self.cache.get(source).unwrap().schema.clone();

        // 提取跨表连接条件和各表过滤条件
        let (join_conds, target_filters, source_filters) =
            extract_cross_table_conditions(where_clause, target, source)?;
        if join_conds.is_empty() {
            return Err(Error::SqlExec(
                "DELETE ... USING 需要至少一个跨表等值连接条件 (t1.col = t2.col)".into(),
            ));
        }

        // 扫描源表，构建 join key 的 lookup set
        let src_join_col = &join_conds[0].1;
        let src_col_idx = s_schema
            .column_index_by_name(src_join_col)
            .ok_or_else(|| Error::SqlExec(format!("源表列不存在: {}.{}", source, src_join_col)))?;
        let mut lookup: HashMap<Vec<u8>, Vec<Value>> = HashMap::new();
        let mut scan_err: Option<Error> = None;
        let src_tc = self.cache.get(source).unwrap();
        src_tc
            .data_ks
            .for_each_kv_prefix(b"", |_key, raw| match s_schema.decode_row(raw) {
                Ok(row) => {
                    let pass = source_filters
                        .iter()
                        .all(|f| row_matches(&row, &s_schema, f).unwrap_or(false));
                    if pass && !matches!(row[src_col_idx], Value::Null) {
                        if let Ok(jk) = row[src_col_idx].to_bytes() {
                            lookup.insert(jk, row);
                        }
                    }
                    true
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            })?;
        if let Some(e) = scan_err {
            return Err(e);
        }

        // 遍历目标表，收集需要删除的行 PK
        let tgt_join_col = &join_conds[0].0;
        let tgt_col_idx = t_schema.column_index_by_name(tgt_join_col).ok_or_else(|| {
            Error::SqlExec(format!("目标表列不存在: {}.{}", target, tgt_join_col))
        })?;
        // 预计算额外 join 条件的列索引
        let extra_join_indices: Vec<(usize, usize)> = join_conds[1..]
            .iter()
            .map(|(tc, sc)| {
                let ti = t_schema
                    .column_index_by_name(tc)
                    .ok_or_else(|| Error::SqlExec(format!("目标表列不存在: {}.{}", target, tc)))?;
                let si = s_schema
                    .column_index_by_name(sc)
                    .ok_or_else(|| Error::SqlExec(format!("源表列不存在: {}.{}", source, sc)))?;
                Ok((ti, si))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let mut to_delete: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
        let mut scan_err2: Option<Error> = None;
        let tgt_tc = self.cache.get(target).unwrap();
        tgt_tc
            .data_ks
            .for_each_kv_prefix(b"", |key, raw| match t_schema.decode_row(raw) {
                Ok(row) => {
                    let pass = target_filters
                        .iter()
                        .all(|f| row_matches(&row, &t_schema, f).unwrap_or(false));
                    if !pass {
                        return true;
                    }
                    if !matches!(row[tgt_col_idx], Value::Null) {
                        if let Ok(jk) = row[tgt_col_idx].to_bytes() {
                            if let Some(src_row) = lookup.get(&jk) {
                                let extra_ok = extra_join_indices
                                    .iter()
                                    .all(|&(ti, si)| row[ti] == src_row[si]);
                                if extra_ok {
                                    to_delete.push((key.to_vec(), row));
                                }
                            }
                        }
                    }
                    true
                }
                Err(e) => {
                    scan_err2 = Some(e);
                    false
                }
            })?;
        if let Some(e) = scan_err2 {
            return Err(e);
        }

        // 外键约束检查
        let rows_ref: Vec<Vec<Value>> = to_delete.iter().map(|(_, r)| r.clone()).collect();
        self.check_fk_on_delete(target, &rows_ref, &t_schema)?;

        // 批量删除（含索引维护）
        let tc = self.cache.get(target).unwrap();
        let idx_cols: Vec<(String, Vec<usize>)> = tc
            .index_keyspaces
            .keys()
            .filter_map(|c| resolve_col_indices(&t_schema, c).map(|ci| (c.clone(), ci)))
            .collect();
        let has_vec = tc.has_vec_indexes;
        let count = to_delete.len();
        for (pk_key, row) in &to_delete {
            // 维护二级索引（支持复合索引）
            for (cols_key, ci) in &idx_cols {
                let ik = build_idx_key(row, ci, &row[0])?;
                self.tx_index_delete(target, cols_key, &ik)?;
            }
            self.tx_delete(target, pk_key)?;
        }
        // 向量索引同步
        if has_vec {
            super::vec_idx::sync_vec_on_delete(&self.store, target, &rows_ref, true)?;
        }
        Ok(vec![vec![Value::Integer(count as i64)]])
    }
}
