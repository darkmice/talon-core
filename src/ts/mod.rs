/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序引擎：时序表创建/写入/范围查询/简单聚合。
//!
//! M2.1 实现；依赖 storage + types。
//! 数据按 keyspace `ts_{name}` 存储，key = `{tag_hash}:{timestamp_be}`。
//! M91：value 使用二进制编码（encoding.rs），替代 JSON，提升 3-10x。
//! Schema 存储在 `ts_meta` keyspace。

mod aggregate;
mod encoding;
pub mod line_protocol;
mod regex_query;
pub(crate) mod retention;
mod snapshot;

use crate::error::Error;
use crate::storage::{Keyspace, SegmentManager, Store};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const TS_META_KEYSPACE: &str = "ts_meta";

fn ts_keyspace_name(name: &str) -> String {
    format!("ts_{}", name)
}

/// 时序表 Schema：TAG 列（用于过滤）+ FIELD 列（数据列）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsSchema {
    /// TAG 列名列表（用于索引过滤）。
    pub tags: Vec<String>,
    /// FIELD 列名列表（数据列）。
    pub fields: Vec<String>,
}

/// 时序数据点：时间戳 + tags + fields。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// 毫秒级时间戳。
    pub timestamp: i64,
    /// TAG 键值对。
    pub tags: BTreeMap<String, String>,
    /// FIELD 键值对（字符串编码，上层负责类型转换）。
    pub fields: BTreeMap<String, String>,
}

/// 时序查询条件。
#[derive(Debug, Clone, Default)]
pub struct TsQuery {
    /// TAG 过滤条件（精确匹配）。
    pub tag_filters: Vec<(String, String)>,
    /// 时间范围起始（含），毫秒。
    pub time_start: Option<i64>,
    /// 时间范围结束（不含），毫秒。
    pub time_end: Option<i64>,
    /// 结果排序：true = DESC。
    pub desc: bool,
    /// 最大返回条数。
    pub limit: Option<usize>,
}

/// 聚合类型。
#[derive(Debug, Clone, Copy)]
pub enum AggFunc {
    /// 求和。
    Sum,
    /// 计数。
    Count,
    /// 平均值。
    Avg,
    /// 最小值。
    Min,
    /// 最大值。
    Max,
    /// 时间窗口内第一个值。
    First,
    /// 时间窗口内最后一个值。
    Last,
    /// 总体标准差（Welford 在线算法）。
    Stddev,
}

/// 时间桶填充策略（仅在 `interval_ms` 有值时生效）。
#[derive(Debug, Clone)]
pub enum FillStrategy {
    /// 跳过空桶（默认行为）。
    None,
    /// 空桶填 NaN，count=0。
    Null,
    /// 空桶填固定值。
    Value(f64),
    /// 用前一个桶的值填充。
    Previous,
    /// 线性插值（边界无法插值时填 NaN）。
    Linear,
}

/// 聚合查询条件。
#[derive(Debug, Clone)]
pub struct TsAggQuery {
    /// TAG 过滤条件。
    pub tag_filters: Vec<(String, String)>,
    /// 时间范围起始（含），毫秒。
    pub time_start: Option<i64>,
    /// 时间范围结束（不含），毫秒。
    pub time_end: Option<i64>,
    /// 聚合的 FIELD 名。
    pub field: String,
    /// 聚合函数。
    pub func: AggFunc,
    /// 按时间间隔分组（毫秒），None 表示全局聚合。
    pub interval_ms: Option<i64>,
    /// 滑动步长（毫秒），None 表示不滑动（等同 interval_ms）。
    /// 对标 TDengine `SLIDING(5m)`。仅在 `interval_ms` 有值时生效。
    pub sliding_ms: Option<i64>,
    /// 会话窗口间隔（毫秒），相邻数据点间隔超过此值则开新桶。
    /// 对标 TDengine `SESSION(ts, 10m)`。与 `interval_ms` 互斥，优先级更高。
    pub session_gap_ms: Option<i64>,
    /// 空桶填充策略，None 表示不填充（等同 FillStrategy::None）。
    pub fill: Option<FillStrategy>,
}

/// 聚合结果桶。
#[derive(Debug, Clone)]
pub struct AggBucket {
    /// 桶起始时间戳（毫秒）；全局聚合时为 0。
    pub bucket_start: i64,
    /// 聚合结果值。
    pub value: f64,
    /// 桶内数据点数。
    pub count: u64,
}

/// 时序引擎；绑定 Store 的 ts_{name} keyspace。
/// M4：通过 SegmentManager 追踪热/冷时间分区。
pub struct TsEngine {
    name: String,
    keyspace: Keyspace,
    schema: TsSchema,
    /// M86：保留 Store 引用用于创建 WriteBatch。
    store: Store,
    /// 统一段管理器。
    segments: SegmentManager,
    /// ts_meta keyspace（用于 retention 元数据读写）。
    meta_ks: Keyspace,
    /// Tag 组合注册表 keyspace：key=tag_hash(8B), value=tag_values JSON。
    /// 部分 tag 查询时扫描注册表 → 过滤匹配组合 → 精确 prefix scan。
    tag_index_ks: Keyspace,
}

fn ts_tag_index_name(name: &str) -> String {
    format!("ts_{}_tags", name)
}

/// 生成数据点的存储 key：tag_hash(8 bytes) + timestamp(8 bytes BE)。
/// tag_hash 用于按 TAG 组合分组，timestamp 用于范围扫描。
fn make_key(tags: &BTreeMap<String, String>, timestamp: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    make_key_into(tags, timestamp, &mut key);
    key
}

/// 写入 key 到已有 buffer（避免每次分配）。
fn make_key_into(tags: &BTreeMap<String, String>, timestamp: i64, buf: &mut Vec<u8>) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    for (k, v) in tags {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    let tag_hash = hasher.finish();
    buf.extend_from_slice(&tag_hash.to_be_bytes());
    buf.extend_from_slice(&timestamp.to_be_bytes());
}

impl TsEngine {
    /// 创建或打开时序表。
    pub fn create(store: &Store, name: &str, schema: TsSchema) -> Result<Self, Error> {
        let meta_ks = store.open_keyspace(TS_META_KEYSPACE)?;
        let raw = serde_json::to_vec(&schema).map_err(|e| Error::TimeSeries(e.to_string()))?;
        meta_ks.set(name.as_bytes(), &raw)?;
        let keyspace = store.open_keyspace(&ts_keyspace_name(name))?;
        let tag_index_ks = store.open_keyspace(&ts_tag_index_name(name))?;
        let segments = store.segment_manager().clone();
        Ok(TsEngine {
            name: name.to_string(),
            keyspace,
            schema,
            store: store.clone(),
            segments,
            meta_ks,
            tag_index_ks,
        })
    }

    /// 打开已有时序表。
    pub fn open(store: &Store, name: &str) -> Result<Self, Error> {
        let meta_ks = store.open_keyspace(TS_META_KEYSPACE)?;
        let raw = meta_ks
            .get(name.as_bytes())?
            .ok_or_else(|| Error::TimeSeries(format!("时序表不存在: {}", name)))?;
        let schema: TsSchema =
            serde_json::from_slice(&raw).map_err(|e| Error::TimeSeries(e.to_string()))?;
        let keyspace = store.open_keyspace(&ts_keyspace_name(name))?;
        let tag_index_ks = store.open_keyspace(&ts_tag_index_name(name))?;
        let segments = store.segment_manager().clone();
        let seg_key = format!("ts:{}:schema", name);
        segments.put(seg_key, raw.to_vec());
        Ok(TsEngine {
            name: name.to_string(),
            keyspace,
            schema,
            store: store.clone(),
            segments,
            meta_ks,
            tag_index_ks,
        })
    }

    /// 写入一个数据点。
    /// M91：使用二进制编码替代 JSON。
    /// Bug 35：data+tag_index 改为 WriteBatch 原子提交。
    pub fn insert(&self, point: &DataPoint) -> Result<(), Error> {
        let key = make_key(&point.tags, point.timestamp);
        let val = encoding::encode_point(&self.schema, point)?;
        let hash_bytes: [u8; 8] = key[..8].try_into().unwrap();
        // 注册 tag 组合到倒排索引（幂等）
        self.register_tag_combo(&point.tags, &hash_bytes)?;
        self.keyspace.set(&key, &val)
    }

    /// 批量写入数据点。
    /// M86：使用 WriteBatch 合并为一次 journal write。
    /// M91：使用二进制编码替代 JSON。
    pub fn insert_batch(&self, points: &[DataPoint]) -> Result<(), Error> {
        let mut batch = self.store.batch();
        let mut seen_hashes = std::collections::HashSet::with_capacity(64);
        let mut key_buf = Vec::with_capacity(16);
        for p in points {
            // 复用 key buffer 减少分配
            key_buf.clear();
            make_key_into(&p.tags, p.timestamp, &mut key_buf);
            // 注册 tag 组合（批内去重，只对新 hash 注册）
            let hash_bytes: [u8; 8] = key_buf[..8].try_into().unwrap();
            if seen_hashes.insert(hash_bytes) {
                self.register_tag_combo_batch(&mut batch, &p.tags, &hash_bytes)?;
            }
            let val = encoding::encode_point(&self.schema, p)?;
            batch.insert(&self.keyspace, key_buf.clone(), val)?;
        }
        batch.commit()
    }

    /// 注册 tag 组合到倒排索引（幂等：已存在则跳过）。
    /// 增加碰撞检测：若 hash 相同但 tags 不同，线性探测寻找空位（最多 16 次）。
    /// 性能优化：先 serialize 一次 tags，后续通过 memcmp 比对 bytes，避免反序列化开销。
    fn register_tag_combo(
        &self,
        tags: &BTreeMap<String, String>,
        hash_bytes: &[u8],
    ) -> Result<(), Error> {
        // 预序列化：只做一次，后续用 bytes 直接比对（memcmp）
        let tag_bytes = serde_json::to_vec(tags).map_err(|e| Error::TimeSeries(e.to_string()))?;
        let mut probe_hash = u64::from_be_bytes(
            hash_bytes.try_into().unwrap_or([0u8; 8]),
        );
        // 最多探测 16 次，防止极端碰撞链导致性能退化
        for probe in 0..16u32 {
            let key = if probe == 0 {
                hash_bytes.to_vec()
            } else {
                probe_hash.to_be_bytes().to_vec()
            };
            if let Some(existing) = self.tag_index_ks.get(&key)? {
                // 快速路径：bytes 直接比对，避免 JSON 反序列化
                if &existing[..] == &tag_bytes[..] {
                    return Ok(()); // 幂等：完全相同，跳过
                }
                // 慢路径：bytes 不等，可能是碰撞，继续探测
                probe_hash = probe_hash.wrapping_add(1);
                continue;
            }
            // 空位：写入（复用已序列化的 bytes）
            return self.tag_index_ks.set(&key, &tag_bytes);
        }
        Err(Error::TimeSeries(
            "tag hash collision chain too long (>16 probes), consider rebuilding index".into(),
        ))
    }

    /// 批量注册 tag 组合（通过 WriteBatch）。
    /// 与 `register_tag_combo` 相同的碰撞检测 + memcmp 优化。
    fn register_tag_combo_batch(
        &self,
        batch: &mut crate::storage::Batch,
        tags: &BTreeMap<String, String>,
        hash_bytes: &[u8; 8],
    ) -> Result<(), Error> {
        let tag_bytes = serde_json::to_vec(tags).map_err(|e| Error::TimeSeries(e.to_string()))?;
        let mut probe_hash = u64::from_be_bytes(*hash_bytes);
        for probe in 0..16u32 {
            let key = if probe == 0 {
                hash_bytes.to_vec()
            } else {
                probe_hash.to_be_bytes().to_vec()
            };
            if let Some(existing) = self.tag_index_ks.get(&key)? {
                if &existing[..] == &tag_bytes[..] {
                    return Ok(());
                }
                probe_hash = probe_hash.wrapping_add(1);
                continue;
            }
            batch.insert(&self.tag_index_ks, key, tag_bytes)?;
            return Ok(());
        }
        Err(Error::TimeSeries(
            "tag hash collision chain too long (>16 probes) in batch".into(),
        ))
    }

    /// 范围查询。M94：tag+time key 范围剪枝。M104：ASC+LIMIT 提前终止。
    /// M200：部分 tag 查询通过 tag 注册表索引，避免全表扫描。
    pub fn query(&self, q: &TsQuery) -> Result<Vec<DataPoint>, Error> {
        let prefix = self.tag_prefix(&q.tag_filters);
        // M200：部分 tag 时，通过 tag 注册表找到匹配的 hash prefix 列表
        // M202：无 tag 但有 time range 时，也走多 prefix 路径避免全表扫描
        if prefix.is_empty() && (q.time_start.is_some() || q.time_end.is_some()) {
            let prefixes = if q.tag_filters.is_empty() {
                self.all_tag_prefixes()?
            } else {
                self.find_matching_tag_prefixes(&q.tag_filters)?
            };
            if !prefixes.is_empty() {
                return self.query_multi_prefix(q, &prefixes);
            }
        }
        let partial_prefixes = if prefix.is_empty() && !q.tag_filters.is_empty() {
            self.find_matching_tag_prefixes(&q.tag_filters)?
        } else {
            vec![]
        };
        if !partial_prefixes.is_empty() {
            return self.query_multi_prefix(q, &partial_prefixes);
        }
        let mut results = Vec::new();
        let mut scan_err: Option<Error> = None;
        let schema = &self.schema;
        let has_range = prefix.len() == 8 && (q.time_start.is_some() || q.time_end.is_some());
        // M104：ASC + LIMIT + 完整 tag prefix → key 内 timestamp 有序，可提前终止
        let early_limit = if !q.desc && prefix.len() == 8 {
            q.limit
        } else {
            None
        };
        let scan_cb = |key: &[u8], raw: &[u8]| -> bool {
            if key.len() != 16 {
                return true;
            }
            let ts = i64::from_be_bytes(key[8..16].try_into().unwrap());
            if let Some(start) = q.time_start {
                if ts < start {
                    return true;
                }
            }
            if let Some(end) = q.time_end {
                if ts >= end {
                    return !has_range;
                }
            }
            if !q.tag_filters.is_empty() && !encoding::tags_match(schema, raw, &q.tag_filters) {
                return true;
            }
            match encoding::decode_point(schema, ts, raw) {
                Ok(point) => {
                    results.push(point);
                    if let Some(lim) = early_limit {
                        results.len() < lim // M104：达到 limit 即终止
                    } else {
                        true
                    }
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        };
        if has_range {
            let mut sk = Vec::with_capacity(16);
            sk.extend_from_slice(&prefix);
            sk.extend_from_slice(&q.time_start.unwrap_or(i64::MIN).to_be_bytes());
            let mut ek = Vec::with_capacity(16);
            ek.extend_from_slice(&prefix);
            ek.extend_from_slice(&q.time_end.unwrap_or(i64::MAX).to_be_bytes());
            self.keyspace.for_each_kv_range(&sk, &ek, scan_cb)?;
        } else {
            self.keyspace.for_each_kv_prefix(&prefix, scan_cb)?;
        }
        if let Some(e) = scan_err {
            return Err(e);
        }
        if q.desc {
            results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        }
        // M104：ASC 提前终止时已有序且已截断，无需再排序/截断
        if early_limit.is_none() {
            if !q.desc {
                results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
            }
            if let Some(limit) = q.limit {
                results.truncate(limit);
            }
        }
        Ok(results)
    }

    /// M202：获取所有 tag hash prefix（用于无 tag 时间范围查询）。
    fn all_tag_prefixes(&self) -> Result<Vec<Vec<u8>>, Error> {
        let mut prefixes = Vec::new();
        self.tag_index_ks.for_each_key_prefix(b"", |hash_key| {
            if hash_key.len() == 8 {
                prefixes.push(hash_key.to_vec());
            }
            true
        })?;
        Ok(prefixes)
    }

    /// M200：部分 tag 查询 — 逐个匹配 prefix 扫描，支持 time range + limit。
    fn query_multi_prefix(
        &self,
        q: &TsQuery,
        prefixes: &[Vec<u8>],
    ) -> Result<Vec<DataPoint>, Error> {
        let mut results = Vec::new();
        let schema = &self.schema;
        let limit = q.limit.unwrap_or(usize::MAX);
        for pfx in prefixes {
            if pfx.len() != 8 {
                continue;
            }
            let has_range = q.time_start.is_some() || q.time_end.is_some();
            let mut scan_err: Option<Error> = None;
            let scan_cb = |key: &[u8], raw: &[u8]| -> bool {
                if key.len() != 16 {
                    return true;
                }
                let ts = i64::from_be_bytes(key[8..16].try_into().unwrap());
                if let Some(start) = q.time_start {
                    if ts < start {
                        return true;
                    }
                }
                if let Some(end) = q.time_end {
                    if ts >= end {
                        return !has_range;
                    }
                }
                match encoding::decode_point(schema, ts, raw) {
                    Ok(point) => {
                        results.push(point);
                        true
                    }
                    Err(e) => {
                        scan_err = Some(e);
                        false
                    }
                }
            };
            if has_range {
                let mut sk = Vec::with_capacity(16);
                sk.extend_from_slice(pfx);
                sk.extend_from_slice(&q.time_start.unwrap_or(i64::MIN).to_be_bytes());
                let mut ek = Vec::with_capacity(16);
                ek.extend_from_slice(pfx);
                ek.extend_from_slice(&q.time_end.unwrap_or(i64::MAX).to_be_bytes());
                self.keyspace.for_each_kv_range(&sk, &ek, scan_cb)?;
            } else {
                self.keyspace.for_each_kv_prefix(pfx, scan_cb)?;
            }
            if let Some(e) = scan_err {
                return Err(e);
            }
        }
        if q.desc {
            results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        } else {
            results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }
        results.truncate(limit);
        Ok(results)
    }

    /// 获取表名。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取 schema。
    pub fn schema(&self) -> &TsSchema {
        &self.schema
    }

    /// 获取段管理器引用（用于查看缓存统计）。
    pub fn segment_manager(&self) -> &SegmentManager {
        &self.segments
    }

    // ── Retention Policy ─────────────────────────────────

    /// 设置数据保留策略：超过 `duration_ms` 毫秒的数据点将在 `purge_expired` 时被清理。
    /// 设为 0 表示清除保留策略（永久保留）。
    pub fn set_retention(&self, duration_ms: u64) -> Result<(), Error> {
        let key = format!("{}:retention", self.name);
        if duration_ms == 0 {
            self.meta_ks.delete(key.as_bytes())?;
        } else {
            self.meta_ks
                .set(key.as_bytes(), duration_ms.to_be_bytes())?;
        }
        Ok(())
    }

    /// 查询当前保留策略（毫秒）；None 表示永久保留。
    pub fn get_retention(&self) -> Result<Option<u64>, Error> {
        let key = format!("{}:retention", self.name);
        match self.meta_ks.get(key.as_bytes())? {
            Some(raw) if raw.len() == 8 => {
                Ok(Some(u64::from_be_bytes(raw[..8].try_into().unwrap())))
            }
            _ => Ok(None),
        }
    }

    /// 清理过期数据点：删除 timestamp < (now - retention_duration) 的所有数据。
    /// 返回删除的数据点数量。若未设置保留策略则返回 0。
    pub fn purge_expired(&self) -> Result<u64, Error> {
        let retention_ms = match self.get_retention()? {
            Some(d) => d,
            None => return Ok(0),
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now_ms.saturating_sub(retention_ms) as i64;
        self.purge_before(cutoff)
    }

    /// 按 TAG 条件删除匹配的数据点，返回删除数量。
    /// 分批删除（每批 1000），O(1) 内存，亿级数据安全。
    /// M91：使用二进制 tag 匹配替代 JSON 解码。
    pub fn purge_by_tag(&self, tag_filters: &[(String, String)]) -> Result<u64, Error> {
        if tag_filters.is_empty() {
            return Ok(0);
        }
        let mut purged = 0u64;
        let schema = &self.schema;
        loop {
            let mut batch_keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
            self.keyspace.for_each_kv_prefix(b"", |key, raw| {
                if encoding::tags_match(schema, raw, tag_filters) {
                    batch_keys.push(key.to_vec());
                    return batch_keys.len() < 1000;
                }
                true
            })?;
            if batch_keys.is_empty() {
                break;
            }
            let mut batch = self.store.batch();
            for key in &batch_keys {
                batch.remove(&self.keyspace, key.clone());
            }
            batch.commit()?;
            purged += batch_keys.len() as u64;
        }
        Ok(purged)
    }

    /// 清理 timestamp < cutoff_ms 的所有数据点。返回删除数量。
    /// 分批删除（每批 1000），O(1) 内存，亿级数据安全。
    pub fn purge_before(&self, cutoff_ms: i64) -> Result<u64, Error> {
        let mut purged = 0u64;
        loop {
            let mut batch_keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
            self.keyspace.for_each_key_prefix(b"", |key| {
                if key.len() == 16 {
                    let ts = i64::from_be_bytes(key[8..16].try_into().unwrap());
                    if ts < cutoff_ms {
                        batch_keys.push(key.to_vec());
                        return batch_keys.len() < 1000;
                    }
                }
                true
            })?;
            if batch_keys.is_empty() {
                break;
            }
            let mut batch = self.store.batch();
            for key in &batch_keys {
                batch.remove(&self.keyspace, key.clone());
            }
            batch.commit()?;
            purged += batch_keys.len() as u64;
        }
        Ok(purged)
    }

    /// 列出指定 TAG 的所有唯一值。
    ///
    /// 扫描 TAG 索引（非数据 keyspace），复杂度 O(唯一 tag 组合数)。
    /// tag_name 不在 schema 中或无数据时返回空 Vec。
    /// 对标 InfluxDB `SHOW TAG VALUES WITH KEY = "host"`。
    pub fn tag_values(&self, tag_name: &str) -> Result<Vec<String>, Error> {
        let mut values = std::collections::HashSet::new();
        self.tag_index_ks.for_each_kv_prefix(b"", |_key, raw| {
            if let Ok(tags) = serde_json::from_slice::<BTreeMap<String, String>>(raw) {
                if let Some(v) = tags.get(tag_name) {
                    values.insert(v.clone());
                }
            }
            true
        })?;
        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// 列出所有 TAG 的所有唯一值。
    ///
    /// 返回 `BTreeMap<tag_name, Vec<value>>`，每个 TAG 的值已排序去重。
    /// 对标 InfluxDB `SHOW TAG VALUES`。
    pub fn all_tag_values(&self) -> Result<BTreeMap<String, Vec<String>>, Error> {
        let mut map: BTreeMap<String, std::collections::HashSet<String>> = BTreeMap::new();
        self.tag_index_ks.for_each_kv_prefix(b"", |_key, raw| {
            if let Ok(tags) = serde_json::from_slice::<BTreeMap<String, String>>(raw) {
                for (k, v) in tags {
                    map.entry(k).or_default().insert(v);
                }
            }
            true
        })?;
        let result: BTreeMap<String, Vec<String>> = map
            .into_iter()
            .map(|(k, set)| {
                let mut vals: Vec<String> = set.into_iter().collect();
                vals.sort();
                (k, vals)
            })
            .collect();
        Ok(result)
    }
}

pub use retention::{
    describe_timeseries, drop_timeseries, list_timeseries, start_ts_retention_cleaner, TsInfo,
    TsRetentionCleaner,
};

#[cfg(test)]
mod tests;
