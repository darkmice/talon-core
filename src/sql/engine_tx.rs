/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine 事务控制：BEGIN/COMMIT/ROLLBACK + 事务读写缓冲。
//! 从 engine.rs 拆分，保持单文件 ≤500 行。

use std::collections::HashMap;

use super::engine::{SqlEngine, TxState};
use crate::storage::Keyspace;
use crate::types::Value;
use crate::Error;

impl SqlEngine {
    // ── 事务控制 ──────────────────────────────────────────
    pub(super) fn exec_begin(&mut self) -> Result<Vec<Vec<Value>>, Error> {
        if self.tx.is_some() {
            return Err(Error::SqlExec("已在事务中，不支持嵌套事务".into()));
        }
        self.tx = Some(TxState {
            writes: HashMap::new(),
            index_writes: Vec::new(),
            snapshot: self.store.snapshot(),
            savepoints: Vec::new(),
        });
        Ok(vec![])
    }
    /// M82：事务提交 — 数据写入 + 索引写入统一刷出到单个 WriteBatch。
    pub(super) fn exec_commit(&mut self) -> Result<Vec<Vec<Value>>, Error> {
        let tx = self
            .tx
            .take()
            .ok_or_else(|| Error::SqlExec("未在事务中，无法 COMMIT".into()))?;
        if tx.writes.is_empty() && tx.index_writes.is_empty() {
            return Ok(vec![]);
        }
        let mut batch = self.store.batch();
        // 数据行
        for ((table, pk_bytes), value) in &tx.writes {
            if !self.ensure_cached(table)? {
                return Err(Error::SqlExec(format!("table not found: {}", table)));
            }
            let tc = self.cache.get(table).unwrap();
            match value {
                Some(raw) => batch.insert(&tc.data_ks, pk_bytes.clone(), raw.clone())?,
                None => batch.remove(&tc.data_ks, pk_bytes.clone()),
            }
        }
        // M82：索引行
        for (table, col, key, is_insert) in &tx.index_writes {
            if let Some(tc) = self.cache.get(table.as_str()) {
                if let Some(idx_ks) = tc.index_keyspaces.get(col.as_str()) {
                    if *is_insert {
                        batch.insert(idx_ks, key.clone(), vec![])?;
                    } else {
                        batch.remove(idx_ks, key.clone());
                    }
                }
            }
        }
        batch.commit()?;
        Ok(vec![])
    }

    pub(super) fn exec_rollback(&mut self) -> Result<Vec<Vec<Value>>, Error> {
        if self.tx.is_none() {
            return Err(Error::SqlExec("未在事务中，无法 ROLLBACK".into()));
        }
        self.tx = None;
        Ok(vec![])
    }

    /// M110：SAVEPOINT name — 克隆当前 writes 快照，记录 index_writes 长度。
    pub(super) fn exec_savepoint(&mut self, name: &str) -> Result<Vec<Vec<Value>>, Error> {
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| Error::SqlExec("未在事务中，无法 SAVEPOINT".into()))?;
        let writes_snap = tx.writes.clone();
        let idx_len = tx.index_writes.len();
        tx.savepoints.push((name.to_string(), writes_snap, idx_len));
        Ok(vec![])
    }

    /// M110：RELEASE SAVEPOINT name — 移除保存点（保留当前写入状态）。
    pub(super) fn exec_release(&mut self, name: &str) -> Result<Vec<Vec<Value>>, Error> {
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| Error::SqlExec("未在事务中，无法 RELEASE".into()))?;
        let pos = tx
            .savepoints
            .iter()
            .rposition(|(n, _, _)| n == name)
            .ok_or_else(|| Error::SqlExec(format!("保存点不存在: {}", name)))?;
        tx.savepoints.truncate(pos);
        Ok(vec![])
    }

    /// M110：ROLLBACK TO SAVEPOINT name — 恢复 writes 并截断 index_writes。
    pub(super) fn exec_rollback_to(&mut self, name: &str) -> Result<Vec<Vec<Value>>, Error> {
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| Error::SqlExec("未在事务中，无法 ROLLBACK TO".into()))?;
        let pos = tx
            .savepoints
            .iter()
            .rposition(|(n, _, _)| n == name)
            .ok_or_else(|| Error::SqlExec(format!("保存点不存在: {}", name)))?;
        // 截断到 pos+1，然后 pop 出目标保存点，避免 clone
        tx.savepoints.truncate(pos + 1);
        let (_, writes_snap, idx_len) = tx.savepoints.pop().unwrap();
        tx.writes = writes_snap;
        tx.index_writes.truncate(idx_len);
        Ok(vec![])
    }

    pub(super) fn tx_set(
        &mut self,
        table: &str,
        pk_bytes: Vec<u8>,
        raw: Vec<u8>,
    ) -> Result<(), Error> {
        if let Some(ref mut tx) = self.tx {
            tx.writes.insert((table.to_string(), pk_bytes), Some(raw));
            Ok(())
        } else {
            let tc = self
                .cache
                .get(table)
                .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
            tc.data_ks.set(&pk_bytes, &raw)
        }
    }

    /// M82：索引写入 — 事务模式缓冲，非事务模式直接写。
    pub(super) fn tx_index_set(
        &mut self,
        table: &str,
        col: &str,
        key: Vec<u8>,
    ) -> Result<(), Error> {
        if let Some(ref mut tx) = self.tx {
            tx.index_writes.push((table.into(), col.into(), key, true));
            return Ok(());
        }
        let idx_ks = self.get_idx_ks(table, col)?;
        idx_ks.set(&key, [])
    }

    /// M82：索引删除 — 事务模式缓冲，非事务模式直接写。
    pub(super) fn tx_index_delete(
        &mut self,
        table: &str,
        col: &str,
        key: &[u8],
    ) -> Result<(), Error> {
        if let Some(ref mut tx) = self.tx {
            tx.index_writes
                .push((table.into(), col.into(), key.to_vec(), false));
            return Ok(());
        }
        let idx_ks = self.get_idx_ks(table, col)?;
        idx_ks.delete(key)
    }

    fn get_idx_ks(&self, table: &str, col: &str) -> Result<&Keyspace, Error> {
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        tc.index_keyspaces
            .get(col)
            .ok_or_else(|| Error::SqlExec(format!("索引不存在: {}", col)))
    }

    pub(super) fn tx_delete(&mut self, table: &str, pk_bytes: &[u8]) -> Result<(), Error> {
        if let Some(ref mut tx) = self.tx {
            tx.writes
                .insert((table.to_string(), pk_bytes.to_vec()), None);
            Ok(())
        } else {
            let tc = self
                .cache
                .get(table)
                .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
            tc.data_ks.delete(pk_bytes)
        }
    }

    /// M88：写缓冲 O(1) lookup。M95：事务内回退到快照读，保证隔离。
    pub(super) fn tx_get(&self, table: &str, pk_bytes: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(ref tx) = self.tx {
            let key = (table.to_string(), pk_bytes.to_vec());
            if let Some(v) = tx.writes.get(&key) {
                return Ok(v.clone());
            }
            let tc = self
                .cache
                .get(table)
                .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
            return tx.snapshot.get(&tc.data_ks, pk_bytes);
        }
        let tc = self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec(format!("表未缓存: {}", table)))?;
        tc.data_ks.get(pk_bytes)
    }
}
