/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 单个 keyspace：对应一种逻辑存储（如 default、kv、sql_meta、vector_* 等）。
//! 薄封装 fjall::Keyspace，提供字节级 get/set/delete/contains；上层负责序列化。

use fjall::Keyspace as FjallKeyspace;

use crate::error::Error;

/// Key-Value 对（字节向量）。
pub type KvPair = (Vec<u8>, Vec<u8>);

/// 一个 keyspace 的句柄；底层对应 fjall 的一棵 LSM。
pub struct Keyspace {
    pub(crate) inner: FjallKeyspace,
}

impl Keyspace {
    /// 写入 key-value；key 最多 65536 字节，value 最多 2^32 字节（fjall 限制）。
    pub fn set<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let k = key.as_ref();
        let v = value.as_ref();
        if k.len() > 65536 {
            return Err(Error::KeyTooLong(k.len()));
        }
        self.inner.insert(k, v).map_err(Error::Storage)
    }

    /// 读取 key；不存在返回 `Ok(None)`。
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        let out = self.inner.get(key).map_err(Error::Storage)?;
        Ok(out.map(|v| v.to_vec()))
    }

    /// 删除 key；不存在也返回 `Ok(())`。
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.inner.remove(key.as_ref()).map_err(Error::Storage)
    }

    /// 是否存在 key。
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, Error> {
        self.inner.contains_key(key).map_err(Error::Storage)
    }

    /// 是否为空（O(log N)）。
    pub fn is_empty(&self) -> Result<bool, Error> {
        self.inner.is_empty().map_err(Error::Storage)
    }

    /// 底层 keyspace 路径（供调试或运维）。
    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    /// 磁盘空间占用（字节），基于 fjall 内部 SST 文件统计。
    pub fn disk_space(&self) -> u64 {
        self.inner.disk_space()
    }

    /// 近似 key 数量（O(1)，基于 fjall 内部计数器）。
    ///
    /// 注意：在高并发或 compaction 后可能不精确，仅用于估算。
    pub fn approximate_len(&self) -> usize {
        self.inner.approximate_len()
    }

    /// 流式计数：统计以 prefix 为前缀的 key 数量。
    ///
    /// O(N) 时间，**O(1) 内存** — 不将 key 加载到 Vec。
    /// 亿级数据安全。
    pub fn count_prefix(&self, prefix: &[u8]) -> Result<u64, Error> {
        let mut count = 0u64;
        for entry in self.inner.prefix(prefix) {
            let _ = entry.key().map_err(Error::Storage)?;
            count += 1;
        }
        Ok(count)
    }

    /// 流式计数：统计 >= start_key 的 key 数量（O(1) 内存）。
    pub fn count_range_from(&self, start_key: &[u8]) -> Result<u64, Error> {
        let mut count = 0u64;
        for entry in self.inner.range(start_key..) {
            let _ = entry.key().map_err(Error::Storage)?;
            count += 1;
        }
        Ok(count)
    }

    /// 流式遍历以 prefix 为前缀的 key。
    ///
    /// 回调返回 `false` 时提前终止。O(1) 内存。
    /// 亿级数据安全。
    pub fn for_each_key_prefix<F>(&self, prefix: &[u8], mut f: F) -> Result<u64, Error>
    where
        F: FnMut(&[u8]) -> bool,
    {
        let mut count = 0u64;
        for entry in self.inner.prefix(prefix) {
            let key = entry.key().map_err(Error::Storage)?;
            count += 1;
            if !f(&key) {
                break;
            }
        }
        Ok(count)
    }

    /// 分页扫描：返回以 prefix 为前缀的前 limit 个 key-value 对，跳过前 offset 个。
    ///
    /// O(offset + limit) 时间，**O(limit) 内存**。
    /// 亿级数据安全：LIMIT 10 只分配 10 个元素。
    /// M83：修复双重查找，使用 `into_inner()` 一次获取 key+value。
    pub fn scan_prefix_limit(
        &self,
        prefix: &[u8],
        offset: u64,
        limit: u64,
    ) -> Result<Vec<KvPair>, Error> {
        let mut result = Vec::with_capacity(limit.min(1024) as usize);
        let mut skipped = 0u64;
        for guard in self.inner.prefix(prefix) {
            let (key, value) = guard.into_inner().map_err(Error::Storage)?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            result.push((key.to_vec(), value.to_vec()));
            if result.len() as u64 >= limit {
                break;
            }
        }
        Ok(result)
    }

    /// M80：流式遍历以 prefix 为前缀的 key-value 对。
    ///
    /// 使用 `Guard::into_inner()` 一次获取 key+value，避免双重查找。
    /// 回调返回 `false` 时提前终止。O(1) 内存。
    pub fn for_each_kv_prefix<F>(&self, prefix: &[u8], mut f: F) -> Result<u64, Error>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let mut count = 0u64;
        for guard in self.inner.prefix(prefix) {
            let (key, value) = guard.into_inner().map_err(Error::Storage)?;
            count += 1;
            if !f(&key, &value) {
                break;
            }
        }
        Ok(count)
    }

    /// M94：按 key 范围 [start, end) 遍历，回调返回 false 提前终止。
    pub fn for_each_kv_range<F>(&self, start: &[u8], end: &[u8], mut f: F) -> Result<u64, Error>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let mut count = 0u64;
        for guard in self.inner.range(start..end) {
            let (key, value) = guard.into_inner().map_err(Error::Storage)?;
            count += 1;
            if !f(&key, &value) {
                break;
            }
        }
        Ok(count)
    }

    /// 列出以 prefix 为前缀的所有 key（用于 KV KEYS 等）。
    ///
    /// ⚠️ 将所有 key 加载到内存，大数据量场景慎用。
    /// 优先使用 `count_prefix` 或 `for_each_prefix`。
    pub fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        self.inner
            .prefix(prefix)
            .map(|guard| guard.key().map(|k| k.to_vec()).map_err(Error::Storage))
            .collect()
    }

    /// 列出 >= start_key 的所有 key（用于 MQ range scan 等）。
    ///
    /// ⚠️ 将所有 key 加载到内存，大数据量场景慎用。
    pub fn keys_from(&self, start_key: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        self.inner
            .range(start_key..)
            .map(|guard| guard.key().map(|k| k.to_vec()).map_err(Error::Storage))
            .collect()
    }
}

// 避免 Keyspace 暴露 fjall 的 Keyspace 类型，但内部仍可 clone 句柄
impl Clone for Keyspace {
    fn clone(&self) -> Self {
        Keyspace {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for Keyspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyspace")
            .field("path", &self.inner.path())
            .finish()
    }
}
