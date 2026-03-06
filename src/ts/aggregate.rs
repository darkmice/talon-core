/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序聚合查询：aggregate + tag 组合注册表 + 部分 tag 索引加速。
//! 从 mod.rs 拆分，保持单文件 ≤500 行。

use super::encoding;
use super::{AggBucket, AggFunc, TsAggQuery, TsEngine};
use crate::error::Error;
use std::collections::BTreeMap;

/// 流式累加器：支持 Sum/Count/Avg/Min/Max/First/Last/Stddev。
struct AggAccum {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    first: f64,
    last: f64,
    /// Welford 在线算法：running mean。
    w_mean: f64,
    /// Welford 在线算法：偏差平方和 M2。
    w_m2: f64,
}

impl AggAccum {
    fn new() -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: f64::NAN,
            last: f64::NAN,
            w_mean: 0.0,
            w_m2: 0.0,
        }
    }

    /// 添加一个值到累加器。
    fn push(&mut self, val: f64) {
        self.sum += val;
        self.count += 1;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
        if self.count == 1 {
            self.first = val;
        }
        self.last = val;
        // Welford 在线算法
        let delta = val - self.w_mean;
        self.w_mean += delta / self.count as f64;
        let delta2 = val - self.w_mean;
        self.w_m2 += delta * delta2;
    }

    /// 根据聚合函数类型提取结果值。
    fn result(&self, func: AggFunc) -> f64 {
        match func {
            AggFunc::Count => self.count as f64,
            AggFunc::Sum => self.sum,
            AggFunc::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFunc::Min => self.min,
            AggFunc::Max => self.max,
            AggFunc::First => self.first,
            AggFunc::Last => self.last,
            AggFunc::Stddev => {
                if self.count == 0 {
                    0.0
                } else {
                    (self.w_m2 / self.count as f64).sqrt()
                }
            }
        }
    }
}

impl TsEngine {
    /// 聚合查询：对指定 FIELD 按时间间隔聚合。
    /// M91：流式累加，不再全量物化 DataPoint。直接从二进制读取 f64。
    /// 部分 tag 匹配时使用 tag 注册表索引加速，避免全表扫描。
    pub fn aggregate(&self, q: &TsAggQuery) -> Result<Vec<AggBucket>, Error> {
        let field_index = self
            .schema
            .fields
            .iter()
            .position(|f| f == &q.field)
            .ok_or_else(|| Error::TimeSeries(format!("FIELD 不存在: {}", q.field)))?;
        let prefix = self.tag_prefix(&q.tag_filters);
        let has_range = prefix.len() == 8 && (q.time_start.is_some() || q.time_end.is_some());
        let mut accum: BTreeMap<i64, AggAccum> = BTreeMap::new();
        let mut scan_err: Option<Error> = None;
        // 会话窗口状态
        let session_gap = q.session_gap_ms.filter(|&g| g > 0);
        let mut session_last_ts: i64 = i64::MIN;
        let mut session_bucket_start: i64 = 0;
        let schema = &self.schema;
        let mut agg_cb = |key: &[u8], raw: &[u8]| -> bool {
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
            // 单次遍历同时读取 field 值 + 检查 tag 匹配
            let val = match encoding::read_field_if_tags_match(
                schema,
                raw,
                field_index,
                &q.tag_filters,
            ) {
                Some(v) => v,
                None => {
                    if !q.tag_filters.is_empty()
                        && !encoding::tags_match(schema, raw, &q.tag_filters)
                    {
                        return true;
                    }
                    match encoding::decode_point(schema, ts, raw) {
                        Ok(p) => match p.fields.get(&q.field).and_then(|s| s.parse::<f64>().ok()) {
                            Some(v) => v,
                            None => return true,
                        },
                        Err(e) => {
                            scan_err = Some(e);
                            return false;
                        }
                    }
                }
            };
            // 会话窗口：session_gap_ms 优先于 interval_ms
            if let Some(gap) = session_gap {
                if session_last_ts == i64::MIN || (ts - session_last_ts) > gap {
                    // 首个点或间隔超过 gap → 开启新会话桶
                    session_bucket_start = ts;
                }
                session_last_ts = ts;
                let entry = accum
                    .entry(session_bucket_start)
                    .or_insert_with(AggAccum::new);
                entry.push(val);
                return true;
            }
            let bucket_key = match q.interval_ms {
                Some(interval) if interval > 0 => {
                    let sliding = q.sliding_ms.unwrap_or(interval);
                    let sliding = if sliding > 0 { sliding } else { interval };
                    if sliding >= interval {
                        // 无滑动或步长 >= 窗口：每个点只属于一个桶
                        (ts / interval) * interval
                    } else {
                        // 滑动窗口：点可能属于多个桶
                        // 安全上限：单点最多分配到 10000 个桶，防止极端 sliding 值
                        let max_buckets = 10_000i64;
                        let first_bucket = ((ts - interval + 1) / sliding).max(0) * sliding;
                        let mut b = first_bucket;
                        let mut count = 0i64;
                        while b <= ts && count < max_buckets {
                            if ts >= b && ts < b + interval {
                                let entry = accum.entry(b).or_insert_with(AggAccum::new);
                                entry.push(val);
                            }
                            b += sliding;
                            count += 1;
                        }
                        return true; // 已手动插入，跳过下面的单桶插入
                    }
                }
                _ => 0,
            };
            let entry = accum.entry(bucket_key).or_insert_with(AggAccum::new);
            entry.push(val);
            true
        };
        // 部分 tag 匹配：通过 tag 注册表找到匹配的 hash prefix，逐个精确扫描
        let scan_prefixes: Vec<Vec<u8>> = if prefix.is_empty() && !q.tag_filters.is_empty() {
            let mp = self.find_matching_tag_prefixes(&q.tag_filters)?;
            if mp.is_empty() {
                vec![vec![]] // 注册表为空（旧数据），回退全表扫描
            } else {
                mp
            }
        } else {
            vec![prefix.clone()]
        };
        for pfx in &scan_prefixes {
            let pfx_has_range = pfx.len() == 8 && (q.time_start.is_some() || q.time_end.is_some());
            if pfx_has_range {
                let mut sk = Vec::with_capacity(16);
                sk.extend_from_slice(pfx);
                sk.extend_from_slice(&q.time_start.unwrap_or(i64::MIN).to_be_bytes());
                let mut ek = Vec::with_capacity(16);
                ek.extend_from_slice(pfx);
                ek.extend_from_slice(&q.time_end.unwrap_or(i64::MAX).to_be_bytes());
                self.keyspace.for_each_kv_range(&sk, &ek, &mut agg_cb)?;
            } else {
                self.keyspace.for_each_kv_prefix(pfx, &mut agg_cb)?;
            }
        }
        if let Some(e) = scan_err {
            return Err(e);
        }

        let mut results = Vec::with_capacity(accum.len());
        for (bucket_start, acc) in &accum {
            results.push(AggBucket {
                bucket_start: *bucket_start,
                value: acc.result(q.func),
                count: acc.count,
            });
        }
        // 填充策略后处理
        if let (Some(interval), Some(ref fill)) = (q.interval_ms, &q.fill) {
            if interval > 0 && !matches!(fill, super::FillStrategy::None) {
                results = apply_fill(&results, q, interval);
            }
        }
        Ok(results)
    }

    /// 从 tag 注册表查找匹配部分 tag 过滤条件的所有 hash prefix。
    pub(super) fn find_matching_tag_prefixes(
        &self,
        tag_filters: &[(String, String)],
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut prefixes = Vec::new();
        self.tag_index_ks
            .for_each_kv_prefix(b"", |hash_key, tag_json| {
                if hash_key.len() != 8 {
                    return true;
                }
                if let Ok(tags) = serde_json::from_slice::<BTreeMap<String, String>>(tag_json) {
                    let matched = tag_filters
                        .iter()
                        .all(|(k, v)| tags.get(k).map(|tv| tv == v).unwrap_or(false));
                    if matched {
                        prefixes.push(hash_key.to_vec());
                    }
                }
                true
            })?;
        Ok(prefixes)
    }

    /// 计算 tag 过滤条件的 key prefix（用于 prefix scan 优化）。
    pub(super) fn tag_prefix(&self, tag_filters: &[(String, String)]) -> Vec<u8> {
        if !tag_filters.is_empty() && tag_filters.len() == self.schema.tags.len() {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            let mut tags = BTreeMap::new();
            for (k, v) in tag_filters {
                tags.insert(k.clone(), v.clone());
            }
            for (k, v) in &tags {
                k.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            hasher.finish().to_be_bytes().to_vec()
        } else {
            vec![]
        }
    }
}

/// 填充策略生成的最大桶数量上限，防止超大时间范围导致 OOM。
const FILL_MAX_BUCKETS: usize = 100_000;

/// 对聚合结果应用填充策略，补齐缺失的时间桶。
fn apply_fill(buckets: &[AggBucket], q: &TsAggQuery, interval: i64) -> Vec<AggBucket> {
    if buckets.is_empty() {
        return Vec::new();
    }
    // 确定时间范围
    let range_start = q
        .time_start
        .unwrap_or(buckets.first().unwrap().bucket_start);
    let range_end = q
        .time_end
        .unwrap_or(buckets.last().unwrap().bucket_start + interval);
    // 对齐到桶边界
    let start = (range_start / interval) * interval;
    // 安全检查：桶数量上限
    let estimated = ((range_end - start) / interval) as usize;
    if estimated > FILL_MAX_BUCKETS {
        // 超出上限，直接返回原始结果不填充
        return buckets.to_vec();
    }
    // 构建已有桶的查找表
    let existing: BTreeMap<i64, &AggBucket> = buckets.iter().map(|b| (b.bucket_start, b)).collect();
    let fill = q.fill.as_ref().unwrap();
    let capacity = estimated.min(buckets.len() * 2 + 16);
    let mut filled = Vec::with_capacity(capacity);
    let mut ts = start;
    // 追踪前一个有值桶（Previous + Linear 共用）
    let mut prev_val = f64::NAN;
    let mut prev_bucket_start = i64::MIN;
    // Linear 插值：预计算下一个有值桶的迭代器，避免 O(n*k) 前向扫描
    let is_linear = matches!(fill, super::FillStrategy::Linear);
    // 排序后的已有桶时间戳列表（用于 Linear 二分查找下一个桶）
    let sorted_keys: Vec<i64> = if is_linear {
        existing.keys().copied().collect()
    } else {
        Vec::new()
    };
    while ts < range_end {
        if let Some(b) = existing.get(&ts) {
            prev_val = b.value;
            prev_bucket_start = b.bucket_start;
            filled.push((*b).clone());
        } else {
            let value = match fill {
                super::FillStrategy::Null | super::FillStrategy::None => f64::NAN,
                super::FillStrategy::Value(v) => *v,
                super::FillStrategy::Previous => prev_val,
                super::FillStrategy::Linear => {
                    // 二分查找下一个有值桶 O(log n)
                    let next_val = match sorted_keys.binary_search(&ts) {
                        Ok(_) => unreachable!(), // ts 不在 existing 中
                        Err(pos) => sorted_keys.get(pos).and_then(|&k| existing.get(&k)),
                    };
                    if prev_bucket_start == i64::MIN {
                        // 无前值，无法插值
                        f64::NAN
                    } else {
                        match next_val {
                            Some(n) => {
                                let ratio = (ts - prev_bucket_start) as f64
                                    / (n.bucket_start - prev_bucket_start) as f64;
                                prev_val + ratio * (n.value - prev_val)
                            }
                            None => f64::NAN,
                        }
                    }
                }
            };
            filled.push(AggBucket {
                bucket_start: ts,
                value,
                count: 0,
            });
        }
        ts += interval;
    }
    filled
}

impl TsEngine {
    /// 会话窗口聚合（对标 TDengine `SESSION(ts, gap)`）。
    ///
    /// 按时间间隔自动分割会话：相邻数据点时间差超过 `gap_ms` 则开启新桶。
    /// 每个桶的 `bucket_start` 为该会话第一个数据点的时间戳。
    ///
    /// AI 场景：Agent 对话会话分段、用户行为分析。
    pub fn aggregate_session(
        &self,
        tag_filters: &[(String, String)],
        time_start: Option<i64>,
        time_end: Option<i64>,
        field: &str,
        func: super::AggFunc,
        gap_ms: i64,
    ) -> Result<Vec<super::AggBucket>, Error> {
        let _field_index = self
            .schema
            .fields
            .iter()
            .position(|f| f == field)
            .ok_or_else(|| Error::TimeSeries(format!("FIELD 不存在: {}", field)))?;

        // 查询所有匹配数据点（ASC）
        let points = self.query(&super::TsQuery {
            tag_filters: tag_filters.to_vec(),
            time_start,
            time_end,
            desc: false,
            limit: None,
        })?;

        if points.is_empty() {
            return Ok(vec![]);
        }

        let mut buckets: Vec<super::AggBucket> = Vec::new();
        let mut acc = AggAccum::new();
        let mut session_start = points[0].timestamp;
        let mut prev_ts = points[0].timestamp;

        for p in &points {
            let val = match p.fields.get(field).and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => v,
                None => continue,
            };

            if p.timestamp - prev_ts > gap_ms && acc.count > 0 {
                buckets.push(super::AggBucket {
                    bucket_start: session_start,
                    value: acc.result(func),
                    count: acc.count,
                });
                acc = AggAccum::new();
                session_start = p.timestamp;
            }
            acc.push(val);
            prev_ts = p.timestamp;
        }

        if acc.count > 0 {
            buckets.push(super::AggBucket {
                bucket_start: session_start,
                value: acc.result(func),
                count: acc.count,
            });
        }

        Ok(buckets)
    }

    /// 状态窗口聚合（对标 TDengine `STATE_WINDOW(col)`）。
    ///
    /// 按指定字段值的变化自动分桶：`state_field` 值改变时开启新桶。
    /// 每个桶的 `bucket_start` 为该状态段第一个数据点的时间戳。
    ///
    /// AI 场景：Agent 运行状态变化检测（如 running→idle 分段统计）。
    pub fn aggregate_state_window(
        &self,
        tag_filters: &[(String, String)],
        time_start: Option<i64>,
        time_end: Option<i64>,
        field: &str,
        func: super::AggFunc,
        state_field: &str,
    ) -> Result<Vec<super::AggBucket>, Error> {
        let _fi = self
            .schema
            .fields
            .iter()
            .position(|f| f == field)
            .ok_or_else(|| Error::TimeSeries(format!("FIELD 不存在: {}", field)))?;

        let points = self.query(&super::TsQuery {
            tag_filters: tag_filters.to_vec(),
            time_start,
            time_end,
            desc: false,
            limit: None,
        })?;

        if points.is_empty() {
            return Ok(vec![]);
        }

        let mut buckets: Vec<super::AggBucket> = Vec::new();
        let mut acc = AggAccum::new();
        let mut seg_start = points[0].timestamp;
        let mut prev_state = points[0]
            .fields
            .get(state_field)
            .cloned()
            .unwrap_or_default();

        for p in &points {
            let cur_state = p.fields.get(state_field).cloned().unwrap_or_default();
            let val = match p.fields.get(field).and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => v,
                None => continue,
            };

            if cur_state != prev_state && acc.count > 0 {
                buckets.push(super::AggBucket {
                    bucket_start: seg_start,
                    value: acc.result(func),
                    count: acc.count,
                });
                acc = AggAccum::new();
                seg_start = p.timestamp;
                prev_state = cur_state;
            }
            acc.push(val);
        }

        if acc.count > 0 {
            buckets.push(super::AggBucket {
                bucket_start: seg_start,
                value: acc.result(func),
                count: acc.count,
            });
        }

        Ok(buckets)
    }
}

impl TsEngine {
    /// 降采样：将本表数据按时间间隔聚合后写入目标时序表（对标 InfluxDB CQ / TDengine 流计算）。
    ///
    /// - `target`：目标时序表（需预先创建，schema 须包含 `field` 字段）
    /// - `field`：聚合的 FIELD 名
    /// - `func`：聚合函数（Avg / Sum / Count / Min / Max / First / Last / Stddev）
    /// - `interval_ms`：降采样间隔（毫秒），必须 > 0
    /// - `tag_filters`：TAG 过滤条件，匹配的数据参与聚合；空表示全局
    /// - `time_start` / `time_end`：时间范围（毫秒），None 表示不限
    ///
    /// 返回写入目标表的数据点数。
    ///
    /// AI 场景：Agent 监控数据从秒级降采样为分钟级/小时级，节省存储。
    pub fn downsample(
        &self,
        target: &TsEngine,
        field: &str,
        func: AggFunc,
        interval_ms: i64,
        tag_filters: &[(String, String)],
        time_start: Option<i64>,
        time_end: Option<i64>,
    ) -> Result<u64, Error> {
        if interval_ms <= 0 {
            return Err(Error::TimeSeries("降采样间隔必须 > 0".into()));
        }
        // 构建聚合查询
        let q = super::TsAggQuery {
            tag_filters: tag_filters.to_vec(),
            time_start,
            time_end,
            field: field.to_string(),
            func,
            interval_ms: Some(interval_ms),
            sliding_ms: None,
            session_gap_ms: None,
            fill: None,
        };
        let buckets = self.aggregate(&q)?;
        if buckets.is_empty() {
            return Ok(0);
        }
        // 构建 tag map（保留过滤条件中的 tag）
        let tags: BTreeMap<String, String> = tag_filters
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // 将聚合桶转为 DataPoint 批量写入目标表
        let points: Vec<super::DataPoint> = buckets
            .iter()
            .map(|b| super::DataPoint {
                timestamp: b.bucket_start,
                tags: tags.clone(),
                fields: {
                    let mut f = BTreeMap::new();
                    f.insert(field.to_string(), b.value.to_string());
                    f
                },
            })
            .collect();
        target.insert_batch(&points)?;
        Ok(points.len() as u64)
    }
}
