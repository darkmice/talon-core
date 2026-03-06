/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序数据保留策略：后台清理线程 + list_timeseries。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::Error;
use crate::storage::Store;

use super::{TsEngine, TS_META_KEYSPACE};

/// 列出所有已创建的时序表名称。
pub fn list_timeseries(store: &Store) -> Result<Vec<String>, Error> {
    let meta_ks = store.open_keyspace(TS_META_KEYSPACE)?;
    let keys = meta_ks.keys_with_prefix(b"")?;
    let names: Vec<String> = keys
        .iter()
        .filter_map(|k| {
            let s = std::str::from_utf8(k).ok()?;
            // 排除 retention 元数据 key（格式 "{name}:retention"）
            if s.contains(':') {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect();
    Ok(names)
}

/// 删除时序表：清除数据 keyspace、TAG 索引 keyspace 和元数据。
///
/// 表不存在时返回 `Error::TimeSeries`。
/// 注意：如果有 `TsEngine` 实例仍在使用该表，调用方需自行确保不再使用。
pub fn drop_timeseries(store: &Store, name: &str) -> Result<(), Error> {
    let meta_ks = store.open_keyspace(TS_META_KEYSPACE)?;
    // 检查表是否存在
    if meta_ks.get(name.as_bytes())?.is_none() {
        return Err(Error::TimeSeries(format!(
            "timeseries '{}' does not exist",
            name
        )));
    }
    // 分批删除数据 keyspace（每批 1000），O(1) 内存，亿级数据安全
    let data_ks = store.open_keyspace(&super::ts_keyspace_name(name))?;
    loop {
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
        data_ks.for_each_key_prefix(b"", |key| {
            keys.push(key.to_vec());
            keys.len() < 1000
        })?;
        if keys.is_empty() {
            break;
        }
        let mut batch = store.batch();
        for k in &keys {
            batch.remove(&data_ks, k.clone());
        }
        batch.commit()?;
    }
    // 分批删除 TAG 索引 keyspace
    let tag_ks = store.open_keyspace(&super::ts_tag_index_name(name))?;
    loop {
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
        tag_ks.for_each_key_prefix(b"", |key| {
            keys.push(key.to_vec());
            keys.len() < 1000
        })?;
        if keys.is_empty() {
            break;
        }
        let mut batch = store.batch();
        for k in &keys {
            batch.remove(&tag_ks, k.clone());
        }
        batch.commit()?;
    }
    // 最后删除元数据（数据清完后再删元数据，崩溃恢复时仍可重试清理）
    let mut batch = store.batch();
    batch.remove(&meta_ks, name.as_bytes().to_vec());
    let ret_key = format!("{}:retention", name);
    batch.remove(&meta_ks, ret_key.into_bytes());
    batch.commit()?;
    Ok(())
}

/// 时序表描述信息（schema + retention + 数据点数量）。
#[derive(Debug, Clone)]
pub struct TsInfo {
    /// 表名。
    pub name: String,
    /// Schema（TAG 列 + FIELD 列）。
    pub schema: super::TsSchema,
    /// 数据保留策略（毫秒），None 表示永久保留。
    pub retention_ms: Option<u64>,
    /// 数据点数量。注意：大表计数需全量扫描，可能耗时。
    pub point_count: u64,
}

/// 查看时序表详细信息：schema、retention、数据点数量。
///
/// 表不存在时返回 `Error::TimeSeries`。
/// 注意：`point_count` 通过流式扫描计数，大表可能耗时。
pub fn describe_timeseries(store: &Store, name: &str) -> Result<TsInfo, Error> {
    let meta_ks = store.open_keyspace(TS_META_KEYSPACE)?;
    let raw = meta_ks
        .get(name.as_bytes())?
        .ok_or_else(|| Error::TimeSeries(format!("timeseries '{}' does not exist", name)))?;
    let schema: super::TsSchema =
        serde_json::from_slice(&raw).map_err(|e| Error::TimeSeries(e.to_string()))?;
    // 读取 retention
    let ret_key = format!("{}:retention", name);
    let retention_ms = match meta_ks.get(ret_key.as_bytes())? {
        Some(v) if v.len() == 8 => Some(u64::from_be_bytes(v[..8].try_into().unwrap())),
        _ => None,
    };
    // 流式计数数据点
    let data_ks = store.open_keyspace(&super::ts_keyspace_name(name))?;
    let count = data_ks.count_prefix(b"")?;
    Ok(TsInfo {
        name: name.to_string(),
        schema,
        retention_ms,
        point_count: count,
    })
}

/// 后台时序数据保留策略清理句柄；drop 时自动停止清理线程。
///
/// 定期扫描所有设置了 retention 的时序表，执行 `purge_expired` 清理过期数据。
/// 参考 KV TtlCleaner 模式。
pub struct TsRetentionCleaner {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for TsRetentionCleaner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// 启动后台时序 retention 清理线程。
///
/// `interval_secs` 为扫描间隔（秒），最小 1 秒。
/// 返回 `TsRetentionCleaner` 句柄，drop 时自动停止。
pub fn start_ts_retention_cleaner(store: &Store, interval_secs: u64) -> TsRetentionCleaner {
    let store = store.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let interval = interval_secs.max(1);
    let handle = std::thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            // 扫描所有时序表，对设置了 retention 的执行 purge
            if let Ok(names) = list_timeseries(&store) {
                for name in &names {
                    if stop2.load(Ordering::Relaxed) {
                        return;
                    }
                    if let Ok(ts) = TsEngine::open(&store, name) {
                        let _ = ts.purge_expired();
                    }
                }
            }
            // 分段 sleep，便于快速响应 stop 信号
            for _ in 0..interval * 10 {
                if stop2.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    });
    TsRetentionCleaner {
        stop,
        handle: Some(handle),
    }
}
