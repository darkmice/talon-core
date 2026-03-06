/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M127：FOREIGN KEY 约束检查（INSERT/UPDATE 子表 → 父表存在性，DELETE/DROP 父表 → 子表引用检查）。

use super::engine::SqlEngine;
use crate::types::{Schema, Value};
use crate::Error;

impl SqlEngine {
    /// INSERT/UPDATE 时检查外键约束：子表行的外键值必须在父表中存在。
    /// NULL 值跳过检查（SQL 标准行为）。
    pub(super) fn check_fk_on_insert(
        &mut self,
        table: &str,
        row: &[Value],
        schema: &Schema,
    ) -> Result<(), Error> {
        if schema.foreign_keys.is_empty() {
            return Ok(());
        }
        for fk in &schema.foreign_keys {
            let child_idx = match schema.column_index_by_name(&fk.column) {
                Some(i) => i,
                None => continue,
            };
            let val = &row[child_idx];
            if matches!(val, Value::Null) {
                continue; // NULL 不检查
            }
            // 确保父表已缓存
            if !self.ensure_cached(&fk.ref_table)? {
                return Err(Error::SqlExec(format!(
                    "外键引用的父表不存在: {}",
                    fk.ref_table
                )));
            }
            let parent_tc = self.cache.get(&fk.ref_table).unwrap();
            let parent_col_idx = parent_tc
                .schema
                .column_index_by_name(&fk.ref_column)
                .ok_or_else(|| {
                    Error::SqlExec(format!(
                        "外键引用的列不存在: {}.{}",
                        fk.ref_table, fk.ref_column
                    ))
                })?;
            // 父表引用列是主键（idx 0）→ 直接点查
            if parent_col_idx == 0 {
                let key = val.to_bytes()?;
                if self.tx_get(&fk.ref_table, &key)?.is_none() {
                    return Err(Error::SqlExec(format!(
                        "外键约束失败: {}.{} = {:?} 在 {}.{} 中不存在",
                        table, fk.column, val, fk.ref_table, fk.ref_column
                    )));
                }
            } else {
                // 非主键列：全表扫描查找（初版简化，后续可用索引加速）
                let found = self.fk_value_exists_in_table(&fk.ref_table, parent_col_idx, val)?;
                if !found {
                    return Err(Error::SqlExec(format!(
                        "外键约束失败: {}.{} = {:?} 在 {}.{} 中不存在",
                        table, fk.column, val, fk.ref_table, fk.ref_column
                    )));
                }
            }
        }
        Ok(())
    }

    /// DELETE/UPDATE 父表行时检查是否有子表引用。
    /// 扫描所有已知表的 schema，找到引用当前表的外键，检查是否有子行引用被删除的值。
    pub(super) fn check_fk_on_delete(
        &mut self,
        parent_table: &str,
        deleted_rows: &[Vec<Value>],
        parent_schema: &Schema,
    ) -> Result<(), Error> {
        if deleted_rows.is_empty() {
            return Ok(());
        }
        // 收集所有引用 parent_table 的子表外键
        let child_fks = self.find_child_foreign_keys(parent_table)?;
        if child_fks.is_empty() {
            return Ok(());
        }
        for (child_table, child_col, ref_col) in &child_fks {
            let ref_col_idx = match parent_schema.column_index_by_name(ref_col) {
                Some(i) => i,
                None => continue,
            };
            if !self.ensure_cached(child_table)? {
                continue;
            }
            let child_schema = self.cache.get(child_table).unwrap().schema.clone();
            let child_col_idx = match child_schema.column_index_by_name(child_col) {
                Some(i) => i,
                None => continue,
            };
            for row in deleted_rows {
                let parent_val = &row[ref_col_idx];
                if matches!(parent_val, Value::Null) {
                    continue;
                }
                // 检查子表是否有行引用该值
                let found =
                    self.fk_value_exists_in_table(child_table, child_col_idx, parent_val)?;
                if found {
                    return Err(Error::SqlExec(format!(
                        "外键约束阻止删除: {}.{} = {:?} 被 {}.{} 引用",
                        parent_table, ref_col, parent_val, child_table, child_col
                    )));
                }
            }
        }
        Ok(())
    }

    /// DROP TABLE 时检查是否有子表外键引用该表。
    pub(super) fn check_fk_on_drop(&mut self, table: &str) -> Result<(), Error> {
        let child_fks = self.find_child_foreign_keys(table)?;
        if !child_fks.is_empty() {
            let (child_table, child_col, _) = &child_fks[0];
            return Err(Error::SqlExec(format!(
                "无法删除表 {}: 被 {}.{} 的外键引用",
                table, child_table, child_col
            )));
        }
        Ok(())
    }

    /// 扫描所有表 schema，找到引用 `parent_table` 的外键定义。
    /// 返回 Vec<(子表名, 子列名, 父列名)>。
    fn find_child_foreign_keys(
        &mut self,
        parent_table: &str,
    ) -> Result<Vec<(String, String, String)>, Error> {
        let all_keys = self.meta_ks.keys_with_prefix(b"")?;
        let mut result = Vec::new();
        for key in &all_keys {
            // 跳过非表 schema 的 key
            if key.starts_with(b"autoincr:") || key.starts_with(b"view:") {
                continue;
            }
            let table_name = String::from_utf8_lossy(key).to_string();
            if table_name == parent_table {
                continue; // 不检查自引用
            }
            if !self.ensure_cached(&table_name)? {
                continue;
            }
            let tc = self.cache.get(&table_name).unwrap();
            for fk in &tc.schema.foreign_keys {
                if fk.ref_table == parent_table {
                    result.push((table_name.clone(), fk.column.clone(), fk.ref_column.clone()));
                }
            }
        }
        Ok(result)
    }

    /// 检查表中指定列是否存在某个值（提前终止扫描）。
    fn fk_value_exists_in_table(
        &self,
        table: &str,
        col_idx: usize,
        target: &Value,
    ) -> Result<bool, Error> {
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        let schema = &tc.schema;
        let mut found = false;
        let mut scan_err: Option<Error> = None;
        tc.data_ks.for_each_kv_prefix(b"", |_key, raw| {
            match schema.decode_row(raw) {
                Ok(row) => {
                    if col_idx < row.len() && row[col_idx] == *target {
                        found = true;
                        return false; // 提前终止
                    }
                }
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            }
            true
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }
        Ok(found)
    }
}
