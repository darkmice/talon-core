/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 存储层：基于 fjall LSM-Tree 的 keyspace 管理、读写与事务边界。
//!
//! M1.1：fjall 集成、keyspace 创建/打开、薄封装。
//! M7：新增 WriteBatch 批量写入支持，大幅提升写入吞吐。
//! M4：新增 SegmentManager 统一热/冷分层缓存 + StorageConfig。

mod keyspace;
pub mod segment;
mod snapshot;

pub use keyspace::{Keyspace, KvPair};
pub use segment::{CacheStats, EvictionHandle, SegmentManager, StorageConfig};
pub use snapshot::Snapshot;

use fjall::{Database, KeyspaceCreateOptions};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::error::Error;

/// 存储引擎：多 keyspace，底层 fjall。
/// 单库多 keyspace，与 v2 五模对应（关系/KV/时序/MQ/向量各 keyspace）。
/// M4：内置 `SegmentManager` 提供统一热/冷分层缓存。
#[derive(Clone)]
pub struct Store {
    db: Database,
    /// 数据库目录路径。
    db_path: std::path::PathBuf,
    /// 统一段管理器（所有引擎共享）。
    segments: SegmentManager,
    /// Keyspace 缓存：避免重复调用 fjall keyspace() 的磁盘 I/O。
    ks_cache: Arc<RwLock<HashMap<String, Keyspace>>>,
}

/// 原子批量写入；合并多次写入为一次 journal write，大幅提升吞吐。
/// 通过 `Store::batch()` 创建，调用 `commit()` 原子提交。
pub struct Batch {
    inner: fjall::OwnedWriteBatch,
}

impl Store {
    /// 打开或创建数据库目录；底层使用 fjall 3.x，使用默认配置。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with_config(path, StorageConfig::default())
    }

    /// 打开或创建数据库目录，使用自定义配置。
    pub fn open_with_config(path: impl AsRef<Path>, config: StorageConfig) -> Result<Self, Error> {
        let path = path.as_ref();
        let db = Database::builder(path)
            .manual_journal_persist(config.manual_journal_persist)
            .open()
            .map_err(Error::Storage)?;
        let segments = SegmentManager::new(config);
        Ok(Store {
            db,
            db_path: path.to_path_buf(),
            segments,
            ks_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// 打开或创建指定 keyspace（对应关系/KV/时序/MQ/向量等）。
    /// M93：默认开启 LZ4 压缩（业界标准，RocksDB/LevelDB 均默认开启）。
    /// name 须非空且不超过 255 字符（fjall 限制）。
    pub fn open_keyspace(&self, name: &str) -> Result<Keyspace, Error> {
        if name.is_empty() || name.len() > 255 {
            return Err(Error::InvalidKeyspaceName);
        }
        // 快速路径：读锁查缓存
        if let Ok(cache) = self.ks_cache.read() {
            if let Some(ks) = cache.get(name) {
                return Ok(ks.clone());
            }
        }
        // 慢路径：写锁创建并缓存
        let inner = self
            .db
            .keyspace(name, || {
                KeyspaceCreateOptions::default().data_block_compression_policy(
                    fjall::config::CompressionPolicy::all(fjall::CompressionType::Lz4),
                )
            })
            .map_err(Error::Storage)?;
        let ks = Keyspace { inner };
        if let Ok(mut cache) = self.ks_cache.write() {
            cache.insert(name.to_string(), ks.clone());
        }
        Ok(ks)
    }

    /// 创建跨 keyspace 一致性快照（MVCC 读视图）。
    /// 零成本创建：仅记录 LSM 序列号，不复制数据。
    /// 快照内读操作只看到创建时刻的数据，后续写入不可见。
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            inner: self.db.snapshot(),
        }
    }

    /// 创建原子批量写入句柄；可跨 keyspace 写入，commit 时一次性提交。
    pub fn batch(&self) -> Batch {
        Batch {
            inner: self.db.batch(),
        }
    }

    /// 刷盘到磁盘，保证此前写入持久化（依赖 fjall PersistMode）。
    pub fn persist(&self) -> Result<(), Error> {
        self.db
            .persist(fjall::PersistMode::SyncAll)
            .map_err(Error::Storage)
    }

    /// 获取统一段管理器引用（所有引擎共享）。
    pub fn segment_manager(&self) -> &SegmentManager {
        &self.segments
    }

    /// 数据库目录总磁盘占用（字节）。递归遍历所有文件。
    pub fn disk_usage(&self) -> u64 {
        dir_size(&self.db_path)
    }

    /// 数据库目录路径。
    pub fn path(&self) -> &Path {
        &self.db_path
    }
}

/// 递归计算目录大小（字节）。
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let ft = entry.file_type();
            if let Ok(ft) = ft {
                if ft.is_file() {
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                } else if ft.is_dir() {
                    total += dir_size(&entry.path());
                }
            }
        }
    }
    total
}

impl Batch {
    /// 向批量写入中添加一条 key-value 插入。
    /// key 最多 65536 字节（与 Keyspace::set 一致）。
    pub fn insert(
        &mut self,
        ks: &Keyspace,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), Error> {
        let k = key.into();
        if k.len() > 65536 {
            return Err(Error::KeyTooLong(k.len()));
        }
        self.inner.insert(&ks.inner, k, value.into());
        Ok(())
    }

    /// 向批量写入中添加一条删除操作。
    pub fn remove(&mut self, ks: &Keyspace, key: impl Into<Vec<u8>>) {
        self.inner.remove(&ks.inner, key.into());
    }

    /// 原子提交所有缓冲的写入操作。
    pub fn commit(self) -> Result<(), Error> {
        self.inner.commit().map_err(Error::Storage)
    }

    /// 当前缓冲的操作数量。
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_open_creates_dir_and_keyspace_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ks = store.open_keyspace("test").unwrap();
        ks.set(b"k1", b"v1").unwrap();
        assert_eq!(ks.get(b"k1").unwrap().as_deref(), Some(b"v1" as &[u8]));
        ks.delete(b"k1").unwrap();
        assert!(ks.get(b"k1").unwrap().is_none());
    }

    #[test]
    fn store_open_keyspace_rejects_empty_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert!(store.open_keyspace("").is_err());
    }

    #[test]
    fn keyspace_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ks = store.open_keyspace("empty").unwrap();
        assert!(ks.is_empty().unwrap());
        ks.set(b"x", b"y").unwrap();
        assert!(!ks.is_empty().unwrap());
    }
}
