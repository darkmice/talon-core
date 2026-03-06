/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! OpLog（操作日志）— 主从复制的基石。
//!
//! 所有写操作追加到 `__oplog__` keyspace，key = LSN（大端 u64），
//! value = 序列化的 OpLogEntry。从节点通过 LSN 增量拉取并回放。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::storage::{Keyspace, Store};

use super::operation::Operation;

/// OpLog keyspace 名称。
const OPLOG_KEYSPACE: &str = "__oplog__";

/// OpLog 元数据 keyspace 名称（存储 LSN 等持久化状态）。
const OPLOG_META_KEYSPACE: &str = "__oplog_meta__";

/// 元数据 key：当前最大 LSN。
const META_KEY_MAX_LSN: &[u8] = b"max_lsn";

/// 操作日志条目（M111：二进制编码）。
#[derive(Debug, Clone, PartialEq)]
pub struct OpLogEntry {
    /// 全局递增序列号。
    pub lsn: u64,
    /// 写入时间戳（毫秒）。
    pub timestamp_ms: u64,
    /// 操作内容。
    pub op: Operation,
}

impl OpLogEntry {
    /// M111：序列化为二进制字节。格式：[u64 LE lsn][u64 LE ts][Operation bytes]
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let op_bytes = self.op.to_bytes()?;
        let mut buf = Vec::with_capacity(16 + op_bytes.len());
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        buf.extend_from_slice(&op_bytes);
        Ok(buf)
    }

    /// M111：从二进制字节反序列化。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 16 {
            return Err(Error::Serialization("OpLogEntry 数据不足".into()));
        }
        let lsn = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let timestamp_ms = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let op = Operation::from_bytes(&bytes[16..])?;
        Ok(OpLogEntry {
            lsn,
            timestamp_ms,
            op,
        })
    }

    /// 序列化为 JSON（用于复制协议传输，可读性好）。
    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        self.to_bytes()
    }

    /// 从 JSON 反序列化。
    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        Self::from_bytes(bytes)
    }
}

/// OpLog 截断配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpLogConfig {
    /// OpLog 最大条目数；超出时截断已确认的旧条目。0 = 不限制。
    pub max_entries: u64,
    /// OpLog 最大保留时间（秒）；超出时截断。0 = 不限制。
    pub max_age_secs: u64,
}

impl Default for OpLogConfig {
    fn default() -> Self {
        Self {
            max_entries: 1_000_000,
            max_age_secs: 86400, // 24 小时
        }
    }
}

/// OpLog 管理器 — 负责追加、读取、截断操作日志。
///
/// 线程安全：内部 Mutex 保护写入路径，LSN 用 AtomicU64。
pub struct OpLog {
    /// OpLog 数据 keyspace。
    ks: Keyspace,
    /// 元数据 keyspace（持久化 LSN 等）。
    meta_ks: Keyspace,
    /// Bug 39：保留 Store 引用用于创建 WriteBatch。
    store: Store,
    /// 当前最大 LSN（内存缓存，启动时从持久化恢复）。
    current_lsn: AtomicU64,
    /// 当前最小 LSN（用于截断判断）。
    min_lsn: AtomicU64,
    /// 写入锁（保证 LSN 严格递增）。
    write_lock: Mutex<()>,
    /// 截断配置。
    config: OpLogConfig,
}

impl OpLog {
    /// 打开或创建 OpLog。从持久化元数据恢复 LSN。
    pub fn open(store: &Store, config: OpLogConfig) -> Result<Self, Error> {
        let ks = store.open_keyspace(OPLOG_KEYSPACE)?;
        let meta_ks = store.open_keyspace(OPLOG_META_KEYSPACE)?;

        // 恢复 max_lsn
        let max_lsn = match meta_ks.get(META_KEY_MAX_LSN)? {
            Some(bytes) if bytes.len() == 8 => u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            _ => 0,
        };

        // 恢复 min_lsn：扫描第一个 key
        let min_lsn = Self::scan_min_lsn(&ks, max_lsn);

        Ok(Self {
            ks,
            meta_ks,
            store: store.clone(),
            current_lsn: AtomicU64::new(max_lsn),
            min_lsn: AtomicU64::new(min_lsn),
            write_lock: Mutex::new(()),
            config,
        })
    }

    /// 追加一条操作到 OpLog；返回分配的 LSN。
    pub fn append(&self, op: Operation) -> Result<u64, Error> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| Error::LockPoisoned("oplog write lock".into()))?;

        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst) + 1;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = OpLogEntry {
            lsn,
            timestamp_ms: now_ms,
            op,
        };

        let lsn_bytes = lsn.to_be_bytes();
        let value = entry.to_bytes()?;
        // Bug 39：entry + max_lsn 原子写入，防止崩溃后 LSN 不一致
        let mut batch = self.store.batch();
        batch.insert(&self.ks, lsn_bytes.to_vec(), value)?;
        batch.insert(&self.meta_ks, META_KEY_MAX_LSN.to_vec(), lsn_bytes.to_vec())?;
        batch.commit()?;

        // 首条写入时更新 min_lsn
        if self.min_lsn.load(Ordering::Relaxed) == 0 {
            self.min_lsn.store(lsn, Ordering::Relaxed);
        }

        Ok(lsn)
    }

    /// 读取指定 LSN 的条目。
    pub fn get(&self, lsn: u64) -> Result<Option<OpLogEntry>, Error> {
        let key = lsn.to_be_bytes();
        match self.ks.get(key)? {
            Some(bytes) => Ok(Some(OpLogEntry::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    /// 读取 `(from_lsn, to_lsn]` 范围内的条目（不含 from_lsn，含 to_lsn）。
    /// 最多返回 `limit` 条。
    pub fn range(
        &self,
        from_lsn: u64,
        to_lsn: u64,
        limit: usize,
    ) -> Result<Vec<OpLogEntry>, Error> {
        if from_lsn >= to_lsn || limit == 0 {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        // 安全加 1，避免 u64::MAX 溢出
        let start_lsn = from_lsn.saturating_add(1);
        let start = start_lsn.to_be_bytes();
        let end_inclusive = to_lsn.to_be_bytes();

        for guard in self.ks.inner.range(start..=end_inclusive) {
            let (_, v) = guard.into_inner().map_err(Error::Storage)?;
            entries.push(OpLogEntry::from_bytes(&v)?);
            if entries.len() >= limit {
                break;
            }
        }
        Ok(entries)
    }

    /// 当前最大 LSN。
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }

    /// 当前最小 LSN（截断后的起始点）。
    pub fn min_lsn(&self) -> u64 {
        self.min_lsn.load(Ordering::Relaxed)
    }

    /// 截断 `<= up_to_lsn` 的所有条目。
    ///
    /// 用于清理已被所有从节点确认的旧日志。
    /// 返回截断的条目数。
    /// 调用方必须已持有 write_lock，或通公 truncate_safe 调用。
    fn truncate_inner(&self, up_to_lsn: u64) -> Result<u64, Error> {
        let mut count = 0u64;
        let min = self.min_lsn.load(Ordering::Relaxed);
        if min == 0 || up_to_lsn < min {
            return Ok(0);
        }

        let end = std::cmp::min(up_to_lsn, self.current_lsn.load(Ordering::SeqCst));
        // 批量删除，减少单条 delete 开销
        let batch_size = 1000u64;
        let mut cursor = min;
        while cursor <= end {
            let batch_end = std::cmp::min(cursor + batch_size - 1, end);
            for lsn in cursor..=batch_end {
                let key = lsn.to_be_bytes();
                self.ks.delete(key)?;
                count += 1;
            }
            cursor = match batch_end.checked_add(1) {
                Some(next) => next,
                None => break, // u64::MAX reached
            };
        }

        self.min_lsn.store(end + 1, Ordering::Relaxed);
        Ok(count)
    }

    /// 截断 `<= up_to_lsn` 的所有条目（线程安全，自动加锁）。
    pub fn truncate(&self, up_to_lsn: u64) -> Result<u64, Error> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| Error::LockPoisoned("oplog write lock".into()))?;
        self.truncate_inner(up_to_lsn)
    }

    /// 基于配置自动截断过期条目。
    ///
    /// `min_confirmed_lsn` 为所有从节点已确认的最小 LSN，
    /// 只截断 `<= min_confirmed_lsn` 且满足 age/count 条件的条目。
    /// 无从节点时传 `current_lsn` 表示全部可截断。
    pub fn auto_truncate(&self, min_confirmed_lsn: u64) -> Result<u64, Error> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| Error::LockPoisoned("oplog write lock".into()))?;

        let current = self.current_lsn.load(Ordering::SeqCst);
        let min = self.min_lsn.load(Ordering::Relaxed);
        if min == 0 || current == 0 {
            return Ok(0);
        }

        let mut truncate_to = min_confirmed_lsn;

        // 按条目数截断
        if self.config.max_entries > 0 {
            let total = current.saturating_sub(min) + 1;
            if total > self.config.max_entries {
                let excess_cutoff = current.saturating_sub(self.config.max_entries);
                truncate_to = std::cmp::min(truncate_to, excess_cutoff);
            }
        }

        // 按时间截断
        if self.config.max_age_secs > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let cutoff_ms = now_ms.saturating_sub(self.config.max_age_secs * 1000);

            // 从 min_lsn 开始扫描，找到第一个 timestamp >= cutoff 的条目
            let mut age_cutoff = min.saturating_sub(1);
            for lsn in min..=std::cmp::min(truncate_to, current) {
                match self.get(lsn)? {
                    Some(entry) if entry.timestamp_ms < cutoff_ms => {
                        age_cutoff = lsn;
                    }
                    _ => break,
                }
            }
            truncate_to = std::cmp::min(truncate_to, age_cutoff);
        }

        if truncate_to >= min {
            self.truncate_inner(truncate_to)
        } else {
            Ok(0)
        }
    }

    /// 条目总数（近似值，基于 LSN 差值）。
    pub fn entry_count(&self) -> u64 {
        let max = self.current_lsn.load(Ordering::Relaxed);
        let min = self.min_lsn.load(Ordering::Relaxed);
        if max >= min && min > 0 {
            max - min + 1
        } else {
            0
        }
    }

    /// 扫描 keyspace 中最小的 LSN。
    fn scan_min_lsn(ks: &Keyspace, max_lsn: u64) -> u64 {
        if max_lsn == 0 {
            return 0;
        }
        // 取第一个 key
        if let Some(guard) = ks.inner.iter().next() {
            if let Ok((k, _)) = guard.into_inner() {
                if k.len() == 8 {
                    return u64::from_be_bytes(k[..8].try_into().unwrap());
                }
            }
        }
        0
    }
}

#[cfg(test)]
#[path = "oplog_tests.rs"]
mod tests;
