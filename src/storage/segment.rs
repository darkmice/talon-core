/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 统一段管理器：LRU 热/冷分层 + 稀疏目录 + 后台淘汰线程。
//!
//! M4 实现：所有五大引擎通过 `SegmentManager` 接入热/冷分层，
//! 避免各引擎重复实现。fjall 自身管理数据层的 block cache + mmap，
//! 本模块管理 Talon 自建索引结构的内存生命周期。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 存储引擎配置：控制 fjall block cache 大小与 Talon 索引缓存参数。
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// 稀疏目录锚点间隔（默认 10000）。
    pub anchor_interval: usize,
    /// LRU 淘汰阈值（默认 30 分钟）。
    pub lru_threshold: Duration,
    /// 全局内存预算（字节）；0 表示不限制。
    pub memory_budget: usize,
    /// LRU 后台扫描间隔（默认 60 秒）。
    pub eviction_interval: Duration,
    /// M90：手动 journal 持久化模式。开启后 batch.commit() 不自动 fsync，
    /// 需调用 Store::persist() 手动刷盘。数据仍 crash-safe，但非持久化直到 persist。
    /// 适用于高吞吐写入场景（事务批量写入可提升 5-20x）。默认 false。
    pub manual_journal_persist: bool,
}

impl StorageConfig {
    /// 保守配置：每次写入立即 fsync，保证持久化但吞吐较低。
    /// 适用于金融交易等不可丢失数据的场景。
    pub fn safe() -> Self {
        StorageConfig {
            manual_journal_persist: false,
            ..Default::default()
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            anchor_interval: 10_000,
            lru_threshold: Duration::from_secs(30 * 60),
            memory_budget: 0,
            eviction_interval: Duration::from_secs(60),
            manual_journal_persist: true,
        }
    }
}

/// LRU 缓存条目：存储任意字节数据 + 访问时间 + 估算大小。
struct CacheEntry {
    /// 缓存的数据。
    data: Vec<u8>,
    /// 最后访问时间。
    last_access: Instant,
    /// 数据大小（字节），用于内存预算计算。
    size: usize,
}

/// 统一段管理器：为所有引擎提供 LRU 热/冷分层缓存。
///
/// 每个引擎通过 namespace 前缀隔离缓存空间：
/// - `sql:{table}:{segment}` — SQL 索引段
/// - `kv:{range}` — KV bloom filter / 索引块
/// - `ts:{table}:{partition}` — 时序分区索引
/// - `mq:{topic}:{segment}` — MQ 偏移索引
/// - `vec:{name}:{node}` — 向量 HNSW 节点
pub struct SegmentManager {
    inner: Arc<Mutex<SegmentManagerInner>>,
    config: StorageConfig,
}

struct SegmentManagerInner {
    /// key → 缓存条目。
    entries: HashMap<String, CacheEntry>,
    /// 当前总内存占用（字节）。
    total_size: usize,
    /// 缓存命中次数（统计用）。
    hits: u64,
    /// 缓存未命中次数（统计用）。
    misses: u64,
}

/// 后台 LRU 淘汰线程句柄；drop 时自动停止。
pub struct EvictionHandle {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for EvictionHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// 缓存统计信息。
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// 当前缓存条目数。
    pub entry_count: usize,
    /// 当前总内存占用（字节）。
    pub total_size: usize,
    /// 缓存命中次数。
    pub hits: u64,
    /// 缓存未命中次数。
    pub misses: u64,
}

impl SegmentManager {
    /// 创建段管理器。
    pub fn new(config: StorageConfig) -> Self {
        SegmentManager {
            inner: Arc::new(Mutex::new(SegmentManagerInner {
                entries: HashMap::new(),
                total_size: 0,
                hits: 0,
                misses: 0,
            })),
            config,
        }
    }

    /// 使用默认配置创建段管理器。
    pub fn with_defaults() -> Self {
        Self::new(StorageConfig::default())
    }

    /// 获取缓存数据；命中时更新访问时间。
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.get_mut(key) {
            entry.last_access = Instant::now();
            let data = entry.data.clone();
            inner.hits += 1;
            Some(data)
        } else {
            inner.misses += 1;
            None
        }
    }

    /// 写入缓存数据；若超出内存预算则先淘汰最旧条目。
    pub fn put(&self, key: String, data: Vec<u8>) {
        let size = data.len();
        let mut inner = self.inner.lock().unwrap();
        // 移除旧条目（如果存在）
        if let Some(old) = inner.entries.remove(&key) {
            inner.total_size = inner.total_size.saturating_sub(old.size);
        }
        // 内存预算检查：淘汰最旧条目直到有空间
        let budget = self.config.memory_budget;
        if budget > 0 {
            while inner.total_size + size > budget && !inner.entries.is_empty() {
                if let Some(oldest_key) = find_oldest(&inner.entries) {
                    if let Some(removed) = inner.entries.remove(&oldest_key) {
                        inner.total_size = inner.total_size.saturating_sub(removed.size);
                    }
                } else {
                    break;
                }
            }
        }
        inner.entries.insert(
            key,
            CacheEntry {
                data,
                last_access: Instant::now(),
                size,
            },
        );
        inner.total_size += size;
    }

    /// 移除指定缓存条目。
    pub fn remove(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.remove(key) {
            inner.total_size = inner.total_size.saturating_sub(entry.size);
        }
    }

    /// 移除指定前缀的所有缓存条目（用于 DROP TABLE 等场景）。
    pub fn remove_prefix(&self, prefix: &str) {
        let mut inner = self.inner.lock().unwrap();
        let keys: Vec<String> = inner
            .entries
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(entry) = inner.entries.remove(&key) {
                inner.total_size = inner.total_size.saturating_sub(entry.size);
            }
        }
    }

    /// 执行一次 LRU 淘汰：移除超过阈值未访问的条目。返回淘汰数量。
    pub fn evict(&self) -> usize {
        let threshold = self.config.lru_threshold;
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        let expired_keys: Vec<String> = inner
            .entries
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_access) > threshold)
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired_keys.len();
        for key in expired_keys {
            if let Some(entry) = inner.entries.remove(&key) {
                inner.total_size = inner.total_size.saturating_sub(entry.size);
            }
        }
        count
    }

    /// 获取缓存统计信息。
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().unwrap();
        CacheStats {
            entry_count: inner.entries.len(),
            total_size: inner.total_size,
            hits: inner.hits,
            misses: inner.misses,
        }
    }

    /// 获取配置引用。
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// 启动后台 LRU 淘汰线程；返回句柄，drop 时自动停止。
    pub fn start_eviction(&self) -> EvictionHandle {
        let inner = Arc::clone(&self.inner);
        let threshold = self.config.lru_threshold;
        let interval = self.config.eviction_interval;
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                // 分段 sleep，便于快速响应 stop 信号
                let steps = (interval.as_millis() / 100).max(1) as u64;
                for _ in 0..steps {
                    if stop2.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                // 执行淘汰
                let now = Instant::now();
                let mut guard = inner.lock().unwrap();
                let expired: Vec<String> = guard
                    .entries
                    .iter()
                    .filter(|(_, e)| now.duration_since(e.last_access) > threshold)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in expired {
                    if let Some(entry) = guard.entries.remove(&key) {
                        guard.total_size = guard.total_size.saturating_sub(entry.size);
                    }
                }
            }
        });
        EvictionHandle {
            stop,
            handle: Some(handle),
        }
    }
}

impl Clone for SegmentManager {
    fn clone(&self) -> Self {
        SegmentManager {
            inner: Arc::clone(&self.inner),
            config: self.config.clone(),
        }
    }
}

/// 找到最旧的缓存条目 key。
fn find_oldest(entries: &HashMap<String, CacheEntry>) -> Option<String> {
    entries
        .iter()
        .min_by_key(|(_, e)| e.last_access)
        .map(|(k, _)| k.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_manager_put_get_remove() {
        let sm = SegmentManager::with_defaults();
        sm.put("sql:users:seg0".into(), b"index_data".to_vec());
        assert!(sm.get("sql:users:seg0").is_some());
        assert_eq!(sm.get("sql:users:seg0").unwrap(), b"index_data");
        sm.remove("sql:users:seg0");
        assert!(sm.get("sql:users:seg0").is_none());
    }

    #[test]
    fn segment_manager_lru_eviction() {
        let config = StorageConfig {
            lru_threshold: Duration::from_millis(50),
            ..Default::default()
        };
        let sm = SegmentManager::new(config);
        sm.put("a".into(), b"data_a".to_vec());
        std::thread::sleep(Duration::from_millis(80));
        sm.put("b".into(), b"data_b".to_vec()); // 新写入，不应被淘汰
        let evicted = sm.evict();
        assert_eq!(evicted, 1); // 只有 "a" 超时
        assert!(sm.get("a").is_none());
        assert!(sm.get("b").is_some());
    }

    #[test]
    fn segment_manager_memory_budget() {
        let config = StorageConfig {
            memory_budget: 20, // 20 字节预算
            ..Default::default()
        };
        let sm = SegmentManager::new(config);
        sm.put("a".into(), vec![0u8; 10]);
        sm.put("b".into(), vec![0u8; 10]);
        // 预算已满（20 字节），再写入应淘汰最旧的
        sm.put("c".into(), vec![0u8; 10]);
        let stats = sm.stats();
        assert!(stats.total_size <= 20);
        // "a" 应被淘汰（最旧）
        assert!(sm.get("a").is_none());
    }

    #[test]
    fn segment_manager_remove_prefix() {
        let sm = SegmentManager::with_defaults();
        sm.put("sql:t1:seg0".into(), b"d1".to_vec());
        sm.put("sql:t1:seg1".into(), b"d2".to_vec());
        sm.put("sql:t2:seg0".into(), b"d3".to_vec());
        sm.put("kv:range0".into(), b"d4".to_vec());
        sm.remove_prefix("sql:t1:");
        assert!(sm.get("sql:t1:seg0").is_none());
        assert!(sm.get("sql:t1:seg1").is_none());
        assert!(sm.get("sql:t2:seg0").is_some());
        assert!(sm.get("kv:range0").is_some());
    }

    #[test]
    fn segment_manager_stats() {
        let sm = SegmentManager::with_defaults();
        sm.put("k1".into(), vec![0u8; 100]);
        sm.put("k2".into(), vec![0u8; 200]);
        let _ = sm.get("k1"); // hit
        let _ = sm.get("k3"); // miss
        let stats = sm.stats();
        assert_eq!(stats.entry_count, 2);
        assert_eq!(stats.total_size, 300);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn segment_manager_clone_shares_state() {
        let sm1 = SegmentManager::with_defaults();
        let sm2 = sm1.clone();
        sm1.put("shared".into(), b"data".to_vec());
        assert!(sm2.get("shared").is_some());
    }

    #[test]
    fn segment_manager_overwrite_updates_size() {
        let sm = SegmentManager::with_defaults();
        sm.put("k".into(), vec![0u8; 100]);
        assert_eq!(sm.stats().total_size, 100);
        sm.put("k".into(), vec![0u8; 50]);
        assert_eq!(sm.stats().total_size, 50);
    }
}
