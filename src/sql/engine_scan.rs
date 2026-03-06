/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine 扫描方法：tx_scan_all / tx_scan_topn / tx_scan_with_limit / tx_for_each_row。
//! 从 engine.rs 拆分，保持单文件 ≤500 行。

use std::collections::HashMap;

use super::engine::{RowEntry, SqlEngine};
use crate::types::Value;
use crate::Error;

impl SqlEngine {
    pub(super) fn tx_scan_all(&self, table: &str) -> Result<Vec<RowEntry>, Error> {
        self.tx_scan_with_limit(table, None)
    }

    /// M75/M80/M87：流式 Top-N 扫描，O(capacity) 内存，事务 overlay 正确。
    pub(super) fn tx_scan_topn(
        &self,
        table: &str,
        heap: &mut super::topn::TopNHeap,
    ) -> Result<(), Error> {
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        let schema = &tc.schema;
        let tx_pks: std::collections::HashSet<&[u8]> = if let Some(ref tx) = self.tx {
            tx.writes
                .iter()
                .filter(|((t, _), _)| t == table)
                .map(|((_, k), _)| k.as_slice())
                .collect()
        } else {
            std::collections::HashSet::new()
        };
        let mut scan_err: Option<Error> = None;
        // M95：事务内从快照扫描，非事务直接扫 data_ks
        if let Some(ref tx) = self.tx {
            tx.snapshot
                .for_each_kv_prefix(&tc.data_ks, b"", |key, raw| {
                    if tx_pks.contains(key) {
                        return true;
                    }
                    match schema.decode_row(raw) {
                        Ok(row) => heap.push(row),
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    }
                    true
                })?;
        } else {
            tc.data_ks.for_each_kv_prefix(b"", |key, raw| {
                if tx_pks.contains(key) {
                    return true;
                }
                match schema.decode_row(raw) {
                    Ok(row) => heap.push(row),
                    Err(e) => {
                        scan_err = Some(e);
                        return false;
                    }
                }
                true
            })?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        if let Some(ref tx) = self.tx {
            for ((t, _), value) in &tx.writes {
                if t != table {
                    continue;
                }
                if let Some(raw) = value {
                    let row = schema.decode_row(raw)?;
                    heap.push(row);
                }
            }
        }
        Ok(())
    }

    /// 带 LIMIT 的全表扫描。
    pub(super) fn tx_scan_with_limit(
        &self,
        table: &str,
        limit: Option<u64>,
    ) -> Result<Vec<RowEntry>, Error> {
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        if self.tx.is_none() {
            if let Some(n) = limit {
                return tc
                    .data_ks
                    .scan_prefix_limit(b"", 0, n)?
                    .into_iter()
                    .map(|(key, raw)| Ok((key, tc.schema.decode_row(&raw)?)))
                    .collect();
            }
        }
        let mut result_map: HashMap<Vec<u8>, Vec<Value>> = HashMap::new();
        let mut scan_err: Option<Error> = None;
        // M95：事务内从快照扫描
        let scan_fn = |key: &[u8], raw: &[u8]| -> bool {
            match tc.schema.decode_row(raw) {
                Ok(row) => {
                    result_map.insert(key.to_vec(), row);
                }
                Err(e) => {
                    scan_err = Some(e);
                    return false;
                }
            }
            true
        };
        if let Some(ref tx) = self.tx {
            tx.snapshot.for_each_kv_prefix(&tc.data_ks, b"", scan_fn)?;
        } else {
            tc.data_ks.for_each_kv_prefix(b"", scan_fn)?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        if let Some(ref tx) = self.tx {
            for ((t, pk_bytes), value) in &tx.writes {
                if t != table {
                    continue;
                }
                match value {
                    Some(raw) => {
                        let row = tc.schema.decode_row(raw)?;
                        result_map.insert(pk_bytes.clone(), row);
                    }
                    None => {
                        result_map.remove(pk_bytes);
                    }
                }
            }
        }
        let mut result: Vec<RowEntry> = result_map.into_iter().collect();
        if let Some(n) = limit {
            result.truncate(n as usize);
        }
        Ok(result)
    }

    /// 流式收集匹配行的 PK bytes — UPDATE/DELETE 用。
    /// 只保留 PK（通常 8-32 字节），不持有完整行，亿级表内存安全。
    pub(super) fn tx_collect_matching_pks(
        &self,
        table: &str,
        schema: &crate::types::Schema,
        expr: &super::parser::WhereExpr,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut pks: Vec<Vec<u8>> = Vec::new();
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        if self.tx.is_none() {
            let mut scan_err: Option<Error> = None;
            tc.data_ks
                .for_each_kv_prefix(b"", |key, raw| match schema.decode_row(raw) {
                    Ok(row) => {
                        match super::helpers::row_matches(&row, schema, expr) {
                            Ok(true) => pks.push(key.to_vec()),
                            Ok(false) => {}
                            Err(e) => {
                                scan_err = Some(e);
                                return false;
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
        } else {
            // 事务内需 overlay，回退到 tx_scan_all + filter
            let all = self.tx_scan_all(table)?;
            for (pk, row) in all {
                if super::helpers::row_matches(&row, schema, expr).unwrap_or(false) {
                    pks.push(pk);
                }
            }
        }
        Ok(pks)
    }

    /// 流式收集全表所有 PK bytes — 无 WHERE 的 DELETE/UPDATE 用。
    pub(super) fn tx_collect_all_pks(&self, table: &str) -> Result<Vec<Vec<u8>>, Error> {
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        if self.tx.is_none() {
            let mut pks: Vec<Vec<u8>> = Vec::new();
            tc.data_ks.for_each_kv_prefix(b"", |key, _raw| {
                pks.push(key.to_vec());
                true
            })?;
            return Ok(pks);
        }
        // 事务内 overlay
        let all = self.tx_scan_all(table)?;
        Ok(all.into_iter().map(|(pk, _)| pk).collect())
    }

    /// M90：流式行扫描—非事务时零中间分配，事务时 overlay 后迭代。
    /// M95：非事务直接扫 data_ks，事务内扫快照（确保隔离）。
    pub(super) fn tx_for_each_row<F>(&self, table: &str, mut f: F) -> Result<(), Error>
    where
        F: FnMut(Vec<Value>) -> Result<bool, Error>,
    {
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        if self.tx.is_none() {
            let mut scan_err: Option<Error> = None;
            tc.data_ks
                .for_each_kv_prefix(b"", |_key, raw| match tc.schema.decode_row(raw) {
                    Ok(row) => match f(row) {
                        Ok(true) => true,
                        Ok(false) => false,
                        Err(e) => {
                            scan_err = Some(e);
                            false
                        }
                    },
                    Err(e) => {
                        scan_err = Some(e);
                        false
                    }
                })?;
            return match scan_err {
                Some(e) => Err(e),
                None => Ok(()),
            };
        }
        let all = self.tx_scan_all(table)?;
        for (_, row) in all {
            if !f(row)? {
                break;
            }
        }
        Ok(())
    }
}
