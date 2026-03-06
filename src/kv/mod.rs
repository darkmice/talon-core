/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! KV 引擎：SET/GET/DEL/MSET/MGET/TTL/EXPIRE/EXISTS/KEYS prefix/INCR，namespace 约定（ns:key）。
//!
//! M1.3 实现；依赖 storage + types。TTL 以 value 前 8 字节存过期时间戳，GET 时惰性过期。
//! M4.5 增加后台 TTL 清理线程。
//! M4：通过 SegmentManager 追踪热 key range，TTL 清理优化为分段扫描。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Error;
use crate::storage::{Batch, Keyspace, SegmentManager, Snapshot, Store};

const TTL_HEADER_LEN: usize = 8;
/// 单个 value 最大 16 MB（含 TTL 头）。
const MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;

/// KV 命令执行器；绑定 Store 的 `kv` keyspace。
/// M4：通过 SegmentManager 追踪热 key range 访问。
pub struct KvEngine {
    keyspace: Keyspace,
    /// M85：保留 Store 引用用于创建 WriteBatch。
    store: Store,
    /// 统一段管理器。
    segments: SegmentManager,
}

/// 后台 TTL 清理句柄；drop 时自动停止清理线程。
pub struct TtlCleaner {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for TtlCleaner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl KvEngine {
    /// 打开 KV 引擎（使用 store 的 `kv` keyspace）。
    pub fn open(store: &Store) -> Result<Self, Error> {
        let keyspace = store.open_keyspace("kv")?;
        let segments = store.segment_manager().clone();
        Ok(KvEngine {
            keyspace,
            store: store.clone(),
            segments,
        })
    }

    /// 启动后台 TTL 清理线程；interval_secs 为扫描间隔（秒）。
    /// 返回 TtlCleaner 句柄，drop 时自动停止。
    pub fn start_ttl_cleaner(&self, interval_secs: u64) -> TtlCleaner {
        let ks = self.keyspace.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        // 最小间隔 1 秒，防止忙循环
        let interval = interval_secs.max(1);
        let handle = std::thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                let _ = purge_expired_keys(&ks);
                // 分段 sleep，便于快速响应 stop 信号
                for _ in 0..interval * 10 {
                    if stop2.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        });
        TtlCleaner {
            stop,
            handle: Some(handle),
        }
    }

    /// 写入 key；ttl_secs 为 None 表示无过期。value 存为 `[8 字节过期时间戳][payload]`，无 TTL 时时间戳为 0。
    /// P0：value 大小校验，超过 16 MB 拒绝写入。
    pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<(), Error> {
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ValueTooLarge(value.len(), MAX_VALUE_SIZE));
        }
        let expiry = ttl_secs.map(|s| now_secs().saturating_add(s));
        let raw = encode_value_with_ttl(value, expiry);
        self.keyspace.set(key, &raw)
    }

    /// 读取 key；若已过期则惰性删除并返回 None。返回值为 payload（不含 8 字节头）。
    /// 无 TTL 快速路径：复用 Vec 内存，drain 头 8 字节，消除额外分配。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let raw = self.keyspace.get(key)?;
        let Some(mut raw) = raw else {
            return Ok(None);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(None);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 {
            if now_secs() >= expiry {
                let _ = self.keyspace.delete(key);
                return Ok(None);
            }
        }
        // 统一快速路径：原地截去 8 字节头，零额外分配（有/无 TTL 均适用）
        raw.drain(..TTL_HEADER_LEN);
        Ok(Some(raw))
    }

    /// 删除 key；直接写 tombstone，不检查是否存在（省去一次 LSM 查找）。
    pub fn del(&self, key: &[u8]) -> Result<(), Error> {
        self.keyspace.delete(key)
    }

    /// 批量删除：WriteBatch 合并为一次 journal write，大幅提升批量删除吞吐。
    /// 空数组直接返回，不产生 I/O。
    pub fn mdel(&self, keys: &[&[u8]]) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut batch = self.store.batch();
        for k in keys {
            batch.remove(&self.keyspace, k.to_vec());
        }
        batch.commit()
    }

    /// 按前缀批量删除：删除所有以 `prefix` 为前缀的 key，返回删除数量。
    ///
    /// 分批删除（每批 1000），O(1) 内存，大数据量安全。
    /// 空前缀会删除整个 KV 空间中的所有 key（危险操作，需谨慎）。
    pub fn del_prefix(&self, prefix: &[u8]) -> Result<u64, Error> {
        let mut total: u64 = 0;
        loop {
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
            self.keyspace.for_each_key_prefix(prefix, |key| {
                keys.push(key.to_vec());
                keys.len() < 1000
            })?;
            if keys.is_empty() {
                break;
            }
            let mut batch = self.store.batch();
            let count = keys.len() as u64;
            for k in keys {
                batch.remove(&self.keyspace, k);
            }
            batch.commit()?;
            total += count;
        }
        Ok(total)
    }

    /// 批量设置；无 TTL。keys 与 values 长度须一致。
    /// M85：使用 WriteBatch 合并为一次 journal write，大幅提升吞吐。
    pub fn mset(&self, keys: &[&[u8]], values: &[&[u8]]) -> Result<(), Error> {
        if keys.len() != values.len() {
            return Err(Error::Serialization(
                "mset keys and values length mismatch".to_string(),
            ));
        }
        let mut batch = self.store.batch();
        for (k, v) in keys.iter().zip(values.iter()) {
            self.set_batch(&mut batch, k, v, None)?;
        }
        batch.commit()
    }

    /// 高性能批量写入：通过 WriteBatch 合并多次写入为一次 journal write。
    /// 适用于大量数据导入场景，吞吐量远高于逐条 set。
    pub fn set_batch(
        &self,
        batch: &mut Batch,
        key: &[u8],
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Result<(), Error> {
        let expiry = ttl_secs.map(|s| now_secs().saturating_add(s));
        let raw = encode_value_with_ttl(value, expiry);
        batch.insert(&self.keyspace, key.to_vec(), raw)
    }

    /// 高性能批量设置多个 key-value 对；通过 WriteBatch 一次性提交。
    pub fn mset_batch(
        &self,
        batch: &mut Batch,
        keys: &[&[u8]],
        values: &[&[u8]],
    ) -> Result<(), Error> {
        if keys.len() != values.len() {
            return Err(Error::Serialization(
                "mset_batch keys and values length mismatch".to_string(),
            ));
        }
        for (k, v) in keys.iter().zip(values.iter()) {
            self.set_batch(batch, k, v, None)?;
        }
        Ok(())
    }

    /// 批量获取；返回与 keys 同序的 `Option<Vec<u8>>`。
    pub fn mget(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// 是否存在 key（会触发惰性 TTL 过期检查，已过期的 key 返回 false）。
    /// 内联 TTL 检查，避免 get() 的 payload Vec 拷贝。
    pub fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        let raw = self.keyspace.get(key)?;
        let Some(raw) = raw else { return Ok(false) };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(false);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            let _ = self.keyspace.delete(key);
            return Ok(false);
        }
        Ok(true)
    }

    /// 设置 key 的 TTL（秒）；key 须已存在，否则无效果。
    pub fn expire(&self, key: &[u8], secs: u64) -> Result<(), Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(());
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(());
        }
        let payload = raw[TTL_HEADER_LEN..].to_vec();
        let expiry = now_secs().saturating_add(secs);
        let new_raw = encode_value_with_ttl(&payload, Some(expiry));
        self.keyspace.set(key, &new_raw)
    }

    /// 剩余 TTL（秒）；无 TTL 或已过期返回 None。
    pub fn ttl(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(None);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(None);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry == 0 {
            return Ok(None);
        }
        let now = now_secs();
        if now >= expiry {
            let _ = self.keyspace.delete(key);
            return Ok(None);
        }
        Ok(Some(expiry - now))
    }

    /// 设置毫秒级过期时间（对标 Redis PEXPIRE）。
    ///
    /// 内部存储精度为秒，毫秒值向上取整到最近的秒。
    /// key 不存在时静默返回。
    pub fn pexpire(&self, key: &[u8], millis: u64) -> Result<(), Error> {
        // 向上取整：(millis + 999) / 1000
        let secs = millis.saturating_add(999) / 1000;
        self.expire(key, secs)
    }

    /// 剩余 TTL（毫秒）；无 TTL 或已过期返回 None（对标 Redis PTTL）。
    ///
    /// 内部存储精度为秒，返回值为 `remaining_secs * 1000`。
    pub fn pttl(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        self.ttl(key)
            .map(|opt| opt.map(|secs| secs.saturating_mul(1000)))
    }

    /// 移除 key 的 TTL，使其永久存储（对标 Redis PERSIST）。
    ///
    /// 返回 `true` 表示成功移除了 TTL；`false` 表示 key 不存在、已过期或本身无 TTL。
    pub fn persist(&self, key: &[u8]) -> Result<bool, Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(false);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(false);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry == 0 {
            // 本身无 TTL
            return Ok(false);
        }
        if now_secs() >= expiry {
            // 已过期，清理并返回 false
            let _ = self.keyspace.delete(key);
            return Ok(false);
        }
        // 原地将 expiry 置为 0（永久），零额外分配
        let mut raw = raw;
        raw[..TTL_HEADER_LEN].copy_from_slice(&0u64.to_be_bytes());
        self.keyspace.set(key, &raw)?;
        Ok(true)
    }

    /// 返回 key 的值类型（对标 Redis TYPE）。
    ///
    /// - `"string"`：值为有效 UTF-8 文本
    /// - `"bytes"`：值为非 UTF-8 二进制数据
    /// - `"none"`：key 不存在或已过期
    ///
    /// AI 场景：Agent 工具缓存判断值类型（JSON 文本 vs 二进制 embedding）。
    pub fn key_type(&self, key: &[u8]) -> Result<&'static str, Error> {
        match self.get(key)? {
            None => Ok("none"),
            Some(v) => Ok(if std::str::from_utf8(&v).is_ok() {
                "string"
            } else {
                "bytes"
            }),
        }
    }

    /// 随机返回一个未过期的 key（对标 Redis RANDOMKEY）。
    ///
    /// 实现：前缀扫描取第一个有效 key（非真随机，但 LSM-Tree 无高效随机访问）。
    /// 空库返回 `None`。
    pub fn random_key(&self) -> Result<Option<Vec<u8>>, Error> {
        let now = now_secs();
        let mut result = None;
        self.keyspace.for_each_kv_prefix(b"", |key, raw| {
            if raw.len() >= TTL_HEADER_LEN {
                let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry == 0 || now < expiry {
                    result = Some(key.to_vec());
                    return false; // 找到一个即停止
                }
            }
            true
        })?;
        Ok(result)
    }

    /// 设置 key 在指定 Unix 时间戳（秒）过期。
    ///
    /// key 不存在或已过期返回 `false`。成功设置返回 `true`。
    /// `timestamp` 为 0 等同于 `persist`（移除过期）。
    /// 对标 Redis `EXPIREAT`。
    pub fn expire_at(&self, key: &[u8], timestamp: u64) -> Result<bool, Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(false);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(false);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            let _ = self.keyspace.delete(key);
            return Ok(false);
        }
        // 原地修改 TTL 头
        let mut raw = raw;
        raw[..TTL_HEADER_LEN].copy_from_slice(&timestamp.to_be_bytes());
        self.keyspace.set(key, &raw)?;
        Ok(true)
    }

    /// 返回 key 的过期 Unix 时间戳（秒）。
    ///
    /// key 不存在或已过期返回 `None`。无过期（永久）返回 `Some(0)`。
    /// 对标 Redis `EXPIRETIME`。
    pub fn expire_time(&self, key: &[u8]) -> Result<Option<u64>, Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(None);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(None);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            let _ = self.keyspace.delete(key);
            return Ok(None);
        }
        Ok(Some(expiry))
    }

    /// 获取 key 对应 value 的字节长度（不含 TTL 头部）。
    ///
    /// key 不存在或已过期返回 `None`（对标 Redis STRLEN）。
    pub fn strlen(&self, key: &[u8]) -> Result<Option<usize>, Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(None);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(None);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            let _ = self.keyspace.delete(key);
            return Ok(None);
        }
        Ok(Some(raw.len() - TTL_HEADER_LEN))
    }

    /// 追加 value 到 key 现有值末尾；key 不存在则等价于 `set(key, value, None)`。
    ///
    /// 返回追加后的总字节长度（不含 TTL 头部）。已有 TTL 保留不变。
    /// 对标 Redis `APPEND`：空 value 允许，过期 key 视为不存在。
    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<usize, Error> {
        let raw = self.keyspace.get(key)?;
        match raw {
            Some(r) if r.len() >= TTL_HEADER_LEN => {
                let expiry = u64::from_be_bytes(r[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry != 0 && now_secs() >= expiry {
                    // 已过期，清理后当新 key 创建
                    let _ = self.keyspace.delete(key);
                    self.set(key, value, None)?;
                    return Ok(value.len());
                }
                // 保留原 TTL 头 + 原 payload + 新 value
                let new_payload_len = (r.len() - TTL_HEADER_LEN) + value.len();
                if new_payload_len > MAX_VALUE_SIZE {
                    return Err(Error::ValueTooLarge(new_payload_len, MAX_VALUE_SIZE));
                }
                let mut buf = Vec::with_capacity(r.len() + value.len());
                buf.extend_from_slice(&r);
                buf.extend_from_slice(value);
                self.keyspace.set(key, &buf)?;
                Ok(buf.len() - TTL_HEADER_LEN)
            }
            _ => {
                // key 不存在
                self.set(key, value, None)?;
                Ok(value.len())
            }
        }
    }

    /// 获取 key 对应 value 的 `[start, end]` 字节子串（闭区间）。
    ///
    /// 负索引从末尾计算（-1 = 最后一字节）。key 不存在或已过期返回空 `Vec`。
    /// 索引越界自动截断，`start > end` 返回空。对标 Redis `GETRANGE`。
    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Result<Vec<u8>, Error> {
        let Some(raw) = self.keyspace.get(key)? else {
            return Ok(Vec::new());
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(Vec::new());
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            let _ = self.keyspace.delete(key);
            return Ok(Vec::new());
        }
        let payload = &raw[TTL_HEADER_LEN..];
        let len = payload.len() as i64;
        if len == 0 {
            return Ok(Vec::new());
        }
        // 负索引转换（saturating 防止 i64 溢出）
        let s = if start < 0 {
            len.saturating_add(start).max(0)
        } else {
            start
        } as usize;
        let e = if end < 0 {
            len.saturating_add(end).max(0)
        } else {
            end.min(len - 1)
        } as usize;
        if s >= payload.len() || s > e {
            return Ok(Vec::new());
        }
        Ok(payload[s..=e].to_vec())
    }

    /// 从 `offset` 处覆写值，返回覆写后的总长度。
    ///
    /// key 不存在时创建零填充值。`offset` 超出当前长度时中间用 `\0` 填充。
    /// 空 `value` 时不修改，返回当前长度（key 不存在返回 0）。
    /// 保留原有 TTL。对标 Redis `SETRANGE`。
    pub fn setrange(&self, key: &[u8], offset: usize, value: &[u8]) -> Result<usize, Error> {
        if value.is_empty() {
            // 空 value：返回当前长度，不修改
            let Some(raw) = self.keyspace.get(key)? else {
                return Ok(0);
            };
            if raw.len() < TTL_HEADER_LEN {
                return Ok(0);
            }
            let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
            if expiry != 0 && now_secs() >= expiry {
                let _ = self.keyspace.delete(key);
                return Ok(0);
            }
            return Ok(raw.len() - TTL_HEADER_LEN);
        }
        let new_end = offset.checked_add(value.len()).ok_or_else(|| {
            Error::Serialization("SETRANGE offset + value length overflow".into())
        })?;
        let raw = self.keyspace.get(key)?;
        match raw {
            Some(r) if r.len() >= TTL_HEADER_LEN => {
                let expiry = u64::from_be_bytes(r[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry != 0 && now_secs() >= expiry {
                    // 已过期，当新 key 创建
                    let _ = self.keyspace.delete(key);
                    let mut buf = vec![0u8; TTL_HEADER_LEN + new_end];
                    // TTL = 0（无过期）
                    buf[TTL_HEADER_LEN + offset..TTL_HEADER_LEN + new_end].copy_from_slice(value);
                    self.keyspace.set(key, &buf)?;
                    return Ok(new_end);
                }
                let old_payload_len = r.len() - TTL_HEADER_LEN;
                let final_len = old_payload_len.max(new_end);
                let mut buf = Vec::with_capacity(TTL_HEADER_LEN + final_len);
                buf.extend_from_slice(&r[..TTL_HEADER_LEN]); // 保留 TTL
                buf.extend_from_slice(&r[TTL_HEADER_LEN..]);
                buf.resize(TTL_HEADER_LEN + final_len, 0); // 零填充扩展
                buf[TTL_HEADER_LEN + offset..TTL_HEADER_LEN + new_end].copy_from_slice(value);
                self.keyspace.set(key, &buf)?;
                Ok(final_len)
            }
            _ => {
                // key 不存在，创建零填充值
                let mut buf = vec![0u8; TTL_HEADER_LEN + new_end];
                buf[TTL_HEADER_LEN + offset..TTL_HEADER_LEN + new_end].copy_from_slice(value);
                self.keyspace.set(key, &buf)?;
                Ok(new_end)
            }
        }
    }

    /// 原子设置新值并返回旧值。
    ///
    /// key 不存在或已过期返回 `None`。新值写入时清除 TTL（与 Redis `GETSET` 一致）。
    pub fn getset(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let old_raw = self.keyspace.get(key)?;
        // 写入新值（TTL=0，无过期）
        self.set(key, value, None)?;
        let Some(raw) = old_raw else {
            return Ok(None);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(None);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            return Ok(None);
        }
        Ok(Some(raw[TTL_HEADER_LEN..].to_vec()))
    }

    /// 重命名 key：将 `src` 重命名为 `dst`，TTL 随迁。
    ///
    /// `src` 不存在或已过期返回错误。`dst` 已存在则覆盖。
    /// `src == dst` 时直接返回 Ok。对标 Redis `RENAME`。
    pub fn rename(&self, src: &[u8], dst: &[u8]) -> Result<(), Error> {
        if src == dst {
            return Ok(());
        }
        let Some(raw) = self.keyspace.get(src)? else {
            return Err(Error::Serialization("ERR no such key".into()));
        };
        if raw.len() < TTL_HEADER_LEN {
            return Err(Error::Serialization("ERR no such key".into()));
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            let _ = self.keyspace.delete(src);
            return Err(Error::Serialization("ERR no such key".into()));
        }
        // raw 包含完整 TTL 头 + payload，WriteBatch 原子迁移
        let mut batch = self.store.batch();
        batch.insert(&self.keyspace, dst.to_vec(), raw)?;
        batch.remove(&self.keyspace, src.to_vec());
        batch.commit()
    }

    /// 列出以 prefix 为前缀的 key（含 TTL 过滤，已过期 key 不返回）。
    pub fn keys_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let mut result = Vec::new();
        let now = now_secs();
        self.keyspace.for_each_kv_prefix(prefix, |key, raw| {
            if raw.len() >= TTL_HEADER_LEN {
                let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry == 0 || now < expiry {
                    result.push(key.to_vec());
                }
            }
            true
        })?;
        Ok(result)
    }

    /// 列出匹配 glob 模式的 key。支持 `*`（任意字符序列）和 `?`（单字符）。
    /// 实现：提取模式前缀做 prefix scan + 内联 TTL 检查，消除 N+1 查找。
    pub fn keys_match(&self, pattern: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let prefix = glob_prefix(pattern);
        let mut result = Vec::new();
        let now = now_secs();
        self.keyspace.for_each_kv_prefix(&prefix, |key, raw| {
            if glob_match(pattern, key) && raw.len() >= TTL_HEADER_LEN {
                let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry == 0 || now < expiry {
                    result.push(key.to_vec());
                }
            }
            true
        })?;
        Ok(result)
    }

    /// 原子自增；key 不存在则视为 0 再 +1。值存为 8 字节大端 i64。
    /// 注意：当前为 get+set 两步操作，SWMR 单写模型下安全；Server 模式多写时需加锁。
    pub fn incr(&self, key: &[u8]) -> Result<i64, Error> {
        self.incrby(key, 1)
    }

    /// M112：原子自增指定步长；key 不存在则视为 0。返回增后的值。
    pub fn incrby(&self, key: &[u8], delta: i64) -> Result<i64, Error> {
        let cur = self.get(key)?;
        let prev = match cur.as_deref() {
            Some(b) if b.len() >= 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&b[..8]);
                i64::from_be_bytes(buf)
            }
            _ => 0i64,
        };
        let next = prev.saturating_add(delta);
        self.set(key, &next.to_be_bytes(), None)?;
        Ok(next)
    }

    /// M112：原子自减指定步长；等价于 incrby(key, -delta)。
    pub fn decrby(&self, key: &[u8], delta: i64) -> Result<i64, Error> {
        self.incrby(key, -delta)
    }

    /// 原子自减 1；等价于 `decrby(key, 1)`。对标 Redis `DECR`。
    pub fn decr(&self, key: &[u8]) -> Result<i64, Error> {
        self.decrby(key, 1)
    }

    /// M85：原子浮点自增；key 不存在则视为 0.0。返回增后的值。
    /// 值存为 8 字节 IEEE 754 f64 大端编码。
    /// 注意：同一 key 不应混用 `incrby`（i64）和 `incrbyfloat`（f64）。
    pub fn incrbyfloat(&self, key: &[u8], delta: f64) -> Result<f64, Error> {
        if delta.is_nan() {
            return Err(Error::Serialization("INCRBYFLOAT delta is NaN".into()));
        }
        let cur = self.get(key)?;
        let prev = match cur.as_deref() {
            Some(b) if b.len() >= 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&b[..8]);
                f64::from_be_bytes(buf)
            }
            _ => 0.0f64,
        };
        let next = prev + delta;
        if next.is_infinite() || next.is_nan() {
            return Err(Error::Serialization(
                "INCRBYFLOAT result is infinite or NaN".into(),
            ));
        }
        self.set(key, &next.to_be_bytes(), None)?;
        Ok(next)
    }

    /// M112：SET if Not eXists — key 不存在时写入并返回 true，已存在时不操作返回 false。
    pub fn setnx(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<bool, Error> {
        if self.get(key)?.is_some() {
            return Ok(false);
        }
        self.set(key, value, ttl_secs)?;
        Ok(true)
    }

    /// 分页列出以 prefix 为前缀的 key。O(offset + limit) 时间，O(limit) 内存。
    ///
    /// 亿级数据安全。含惰性 TTL 过期检查。
    /// M87：for_each_kv_prefix 消除 key+get N+1，内联 TTL 检查。
    pub fn keys_prefix_limit(
        &self,
        prefix: &[u8],
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut result = Vec::with_capacity(limit.min(1024) as usize);
        let mut skipped = 0u64;
        let now = now_secs();
        self.keyspace.for_each_kv_prefix(prefix, |key, raw| {
            if raw.len() < TTL_HEADER_LEN {
                return true;
            }
            let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
            if expiry != 0 && now >= expiry {
                return true;
            }
            if skipped < offset {
                skipped += 1;
                return true;
            }
            result.push(key.to_vec());
            (result.len() as u64) < limit
        })?;
        Ok(result)
    }

    /// 分页扫描以 prefix 为前缀的 key-value 对。O(limit) 内存。
    ///
    /// 亿级数据安全。含惰性 TTL 过期检查。
    /// M83：使用 for_each_kv_prefix 消除双重查找。
    pub fn scan_prefix_limit(
        &self,
        prefix: &[u8],
        offset: u64,
        limit: u64,
    ) -> Result<Vec<crate::storage::KvPair>, Error> {
        let mut result = Vec::with_capacity(limit.min(1024) as usize);
        let mut skipped = 0u64;
        let now = now_secs();
        self.keyspace.for_each_kv_prefix(prefix, |key, raw| {
            // TTL 检查：raw 前 8 字节是过期时间戳
            if raw.len() >= TTL_HEADER_LEN {
                let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry != 0 && now >= expiry {
                    return true; // expired, skip
                }
                if skipped < offset {
                    skipped += 1;
                    return true;
                }
                result.push((key.to_vec(), raw[TTL_HEADER_LEN..].to_vec()));
                (result.len() as u64) < limit
            } else {
                true // malformed, skip
            }
        })?;
        Ok(result)
    }

    /// 高效计数：KV 键总数（O(1) 内存，流式扫描）。
    ///
    /// 亿级数据安全，不将 key 加载到 Vec。
    pub fn key_count(&self) -> Result<u64, Error> {
        self.keyspace.count_prefix(b"")
    }

    /// KV 引擎磁盘空间占用（字节），基于 fjall SST 统计。
    pub fn disk_space(&self) -> u64 {
        self.keyspace.disk_space()
    }

    /// 获取段管理器引用（用于查看缓存统计）。
    pub fn segment_manager(&self) -> &SegmentManager {
        &self.segments
    }

    // ── M95 快照读 API ────────────────────────────────────

    /// M95：快照读 key；读取快照时刻的值，含 TTL 过期检查（但不执行惰性删除）。
    pub fn snapshot_get(&self, snap: &Snapshot, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let raw = snap.get(&self.keyspace, key)?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        if raw.len() < TTL_HEADER_LEN {
            return Ok(None);
        }
        let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
        if expiry != 0 && now_secs() >= expiry {
            return Ok(None);
        }
        Ok(Some(raw[TTL_HEADER_LEN..].to_vec()))
    }

    /// M95：快照分页扫描 key-value。读取快照时刻数据，含 TTL 检查。
    pub fn snapshot_scan_prefix_limit(
        &self,
        snap: &Snapshot,
        prefix: &[u8],
        offset: u64,
        limit: u64,
    ) -> Result<Vec<crate::storage::KvPair>, Error> {
        let mut result = Vec::with_capacity(limit.min(1024) as usize);
        let mut skipped = 0u64;
        let now = now_secs();
        snap.for_each_kv_prefix(&self.keyspace, prefix, |key, raw| {
            if raw.len() >= TTL_HEADER_LEN {
                let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry != 0 && now >= expiry {
                    return true;
                }
                if skipped < offset {
                    skipped += 1;
                    return true;
                }
                result.push((key.to_vec(), raw[TTL_HEADER_LEN..].to_vec()));
                (result.len() as u64) < limit
            } else {
                true
            }
        })?;
        Ok(result)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn encode_value_with_ttl(value: &[u8], expiry_ts: Option<u64>) -> Vec<u8> {
    let mut raw = Vec::with_capacity(TTL_HEADER_LEN + value.len());
    raw.extend_from_slice(&expiry_ts.unwrap_or(0).to_be_bytes());
    raw.extend_from_slice(value);
    raw
}

/// 提取 glob 模式中第一个通配符之前的固定前缀，用于 prefix scan 缩小范围。
fn glob_prefix(pattern: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::new();
    for &b in pattern {
        if b == b'*' || b == b'?' {
            break;
        }
        prefix.push(b);
    }
    prefix
}

/// 简单 glob 匹配：`*` 匹配任意长度，`?` 匹配单字节。
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0usize);
    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// 扫描 kv keyspace，删除所有已过期的 key。返回清理数量。
/// M69：使用流式 for_each_key_prefix 替代 keys_with_prefix，O(1) 内存，亿级安全。
/// 先收集过期 key（批量 1000），再批量删除，避免迭代中修改。
/// M83：使用 for_each_kv_prefix 消除双重查找。
fn purge_expired_keys(ks: &Keyspace) -> Result<u64, Error> {
    let now = now_secs();
    let mut purged = 0u64;
    loop {
        let mut expired_keys = Vec::new();
        ks.for_each_kv_prefix(b"", |key, raw| {
            if raw.len() >= TTL_HEADER_LEN {
                let expiry = u64::from_be_bytes(raw[..TTL_HEADER_LEN].try_into().unwrap());
                if expiry != 0 && now >= expiry {
                    expired_keys.push(key.to_vec());
                    return expired_keys.len() < 1000;
                }
            }
            true
        })?;
        if expired_keys.is_empty() {
            break;
        }
        for key in &expired_keys {
            ks.delete(key)?;
        }
        purged += expired_keys.len() as u64;
    }
    Ok(purged)
}

#[cfg(test)]
mod tests;
