/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 跨 keyspace 一致性快照（MVCC 读视图）。
//!
//! 基于 fjall `Database::snapshot()` 实现，记录 LSM 序列号，
//! 快照内读操作只看到快照创建时刻及之前的数据，不受后续写入影响。
//! 读不阻塞写，写不阻塞读。

use fjall::Readable;

use super::keyspace::Keyspace;
use super::KvPair;
use crate::error::Error;

/// 跨 keyspace 一致性快照。
///
/// 通过 `Store::snapshot()` 创建。实现 MVCC 读视图：
/// - 快照内 get/prefix/range 只返回快照时刻的数据
/// - 后续写入对快照不可见
/// - 零拷贝创建（仅记录序列号，不复制数据）
/// - **注意**：长期持有快照会阻止 LSM compaction 回收旧版本数据，应及时释放
pub struct Snapshot {
    pub(crate) inner: fjall::Snapshot,
}

impl Snapshot {
    /// 快照读：读取快照时刻 key 的值。不存在返回 `Ok(None)`。
    pub fn get(&self, ks: &Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let out = self.inner.get(&ks.inner, key).map_err(Error::Storage)?;
        Ok(out.map(|v| v.to_vec()))
    }

    /// 快照前缀扫描：回调返回 false 提前终止。O(1) 内存。
    pub fn for_each_kv_prefix<F>(
        &self,
        ks: &Keyspace,
        prefix: &[u8],
        mut f: F,
    ) -> Result<u64, Error>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let mut count = 0u64;
        for guard in self.inner.prefix(&ks.inner, prefix) {
            let (key, value) = guard.into_inner().map_err(Error::Storage)?;
            count += 1;
            if !f(&key, &value) {
                break;
            }
        }
        Ok(count)
    }

    /// 快照范围扫描 [start, end)：回调返回 false 提前终止。
    pub fn for_each_kv_range<F>(
        &self,
        ks: &Keyspace,
        start: &[u8],
        end: &[u8],
        mut f: F,
    ) -> Result<u64, Error>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let mut count = 0u64;
        for guard in self.inner.range(&ks.inner, start..end) {
            let (key, value) = guard.into_inner().map_err(Error::Storage)?;
            count += 1;
            if !f(&key, &value) {
                break;
            }
        }
        Ok(count)
    }

    /// 快照分页扫描：跳过 offset 个，返回 limit 个。
    pub fn scan_prefix_limit(
        &self,
        ks: &Keyspace,
        prefix: &[u8],
        offset: u64,
        limit: u64,
    ) -> Result<Vec<KvPair>, Error> {
        let mut result = Vec::with_capacity(limit.min(1024) as usize);
        let mut skipped = 0u64;
        for guard in self.inner.prefix(&ks.inner, prefix) {
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

    /// 快照 key 是否存在。
    pub fn contains_key(&self, ks: &Keyspace, key: &[u8]) -> Result<bool, Error> {
        self.inner
            .contains_key(&ks.inner, key)
            .map_err(Error::Storage)
    }
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        Snapshot {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot").finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Store;

    #[test]
    fn snapshot_isolation_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ks = store.open_keyspace("test").unwrap();

        // 写入初始数据
        ks.set(b"k1", b"v1").unwrap();
        ks.set(b"k2", b"v2").unwrap();

        // 获取快照
        let snap = store.snapshot();

        // 快照后写入新数据
        ks.set(b"k3", b"v3_new").unwrap();
        ks.set(b"k1", b"v1_updated").unwrap();
        ks.delete(b"k2").unwrap();

        // 快照读：应看到快照时刻的数据
        assert_eq!(
            snap.get(&ks, b"k1").unwrap().as_deref(),
            Some(b"v1" as &[u8])
        );
        assert_eq!(
            snap.get(&ks, b"k2").unwrap().as_deref(),
            Some(b"v2" as &[u8])
        );
        assert_eq!(snap.get(&ks, b"k3").unwrap(), None); // 快照后写入，不可见

        // 当前读：应看到最新数据
        assert_eq!(
            ks.get(b"k1").unwrap().as_deref(),
            Some(b"v1_updated" as &[u8])
        );
        assert_eq!(ks.get(b"k2").unwrap(), None); // 已删除
        assert_eq!(ks.get(b"k3").unwrap().as_deref(), Some(b"v3_new" as &[u8]));
    }

    #[test]
    fn snapshot_prefix_scan_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ks = store.open_keyspace("test").unwrap();

        ks.set(b"user:1", b"alice").unwrap();
        ks.set(b"user:2", b"bob").unwrap();

        let snap = store.snapshot();

        // 快照后新增
        ks.set(b"user:3", b"charlie").unwrap();

        // 快照前缀扫描：只看到 2 个
        let mut count = 0;
        snap.for_each_kv_prefix(&ks, b"user:", |_k, _v| {
            count += 1;
            true
        })
        .unwrap();
        assert_eq!(count, 2);

        // 当前前缀扫描：看到 3 个
        let mut current_count = 0;
        ks.for_each_kv_prefix(b"user:", |_k, _v| {
            current_count += 1;
            true
        })
        .unwrap();
        assert_eq!(current_count, 3);
    }

    #[test]
    fn snapshot_contains_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ks = store.open_keyspace("test").unwrap();

        ks.set(b"exists", b"yes").unwrap();
        let snap = store.snapshot();
        ks.delete(b"exists").unwrap();

        assert!(snap.contains_key(&ks, b"exists").unwrap());
        assert!(!ks.contains_key(b"exists").unwrap());
    }
}
