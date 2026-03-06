/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量元数据类型定义与过滤匹配逻辑。
//!
//! M76：支持 metadata pre-filter，RAG 场景核心能力。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 元数据值类型：支持字符串、整数、浮点、布尔。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetaValue {
    /// 字符串值。
    String(String),
    /// 整数值。
    Int(i64),
    /// 浮点值。
    Float(f64),
    /// 布尔值。
    Bool(bool),
}

impl MetaValue {
    /// 尝试作为 f64 比较值（Int 和 Float 均可）。
    fn as_f64(&self) -> Option<f64> {
        match self {
            MetaValue::Int(i) => Some(*i as f64),
            MetaValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

/// 元数据过滤操作符。
#[derive(Debug, Clone)]
pub enum MetaFilterOp {
    /// 等于。
    Eq(MetaValue),
    /// 不等于。
    Ne(MetaValue),
    /// 大于（仅数值类型）。
    Gt(MetaValue),
    /// 小于（仅数值类型）。
    Lt(MetaValue),
    /// 大于等于（仅数值类型）。
    Gte(MetaValue),
    /// 小于等于（仅数值类型）。
    Lte(MetaValue),
    /// 值在列表中（任一匹配）。
    In(Vec<MetaValue>),
}

/// 单个元数据过滤条件。
#[derive(Debug, Clone)]
pub struct MetaFilter {
    /// 字段名。
    pub field: String,
    /// 过滤操作。
    pub op: MetaFilterOp,
}

/// 过滤搜索时的 over-fetch 倍数（第一轮）。
pub(super) const FILTER_OVER_FETCH_1: usize = 4;
/// 过滤搜索时的 over-fetch 倍数（第二轮）。
pub(super) const FILTER_OVER_FETCH_2: usize = 16;

/// metadata 存储 key 前缀：`m:{id_be}`。
pub(super) fn meta_key(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(b"m:");
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// 检查单个 metadata 值是否匹配过滤条件。
fn value_matches(val: &MetaValue, op: &MetaFilterOp) -> bool {
    match op {
        MetaFilterOp::Eq(expected) => val == expected,
        MetaFilterOp::Ne(expected) => val != expected,
        MetaFilterOp::Gt(threshold) => cmp_numeric(val, threshold, |a, b| a > b),
        MetaFilterOp::Lt(threshold) => cmp_numeric(val, threshold, |a, b| a < b),
        MetaFilterOp::Gte(threshold) => cmp_numeric(val, threshold, |a, b| a >= b),
        MetaFilterOp::Lte(threshold) => cmp_numeric(val, threshold, |a, b| a <= b),
        MetaFilterOp::In(list) => list.iter().any(|v| val == v),
    }
}

/// 数值比较辅助：将两个 MetaValue 转为 f64 后比较。
fn cmp_numeric<F: Fn(f64, f64) -> bool>(a: &MetaValue, b: &MetaValue, f: F) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(va), Some(vb)) => f(va, vb),
        _ => false,
    }
}

/// 检查一组 metadata 是否满足所有过滤条件（AND 语义）。
pub(super) fn matches_filters(
    metadata: &HashMap<String, MetaValue>,
    filters: &[MetaFilter],
) -> bool {
    filters.iter().all(|f| {
        metadata
            .get(&f.field)
            .map(|v| value_matches(v, &f.op))
            .unwrap_or(false)
    })
}

/// 从 JSON bytes 反序列化 metadata。
pub(super) fn decode_metadata(raw: &[u8]) -> Option<HashMap<String, MetaValue>> {
    serde_json::from_slice(raw).ok()
}

/// 序列化 metadata 为 JSON bytes。
pub(super) fn encode_metadata(
    metadata: &HashMap<String, MetaValue>,
) -> Result<Vec<u8>, crate::error::Error> {
    serde_json::to_vec(metadata).map_err(|e| crate::error::Error::Serialization(e.to_string()))
}

// ── VectorEngine metadata 方法 ──

use super::VectorEngine;
use crate::error::Error;

impl VectorEngine {
    /// 设置向量的元数据（覆盖写入）。
    pub fn set_metadata(
        &self,
        id: u64,
        metadata: &HashMap<String, MetaValue>,
    ) -> Result<(), Error> {
        let raw = encode_metadata(metadata)?;
        self.keyspace.set(&meta_key(id), &raw)
    }

    /// 获取向量的元数据。
    pub fn get_metadata(&self, id: u64) -> Result<Option<HashMap<String, MetaValue>>, Error> {
        match self.keyspace.get(&meta_key(id))? {
            Some(raw) => Ok(decode_metadata(&raw)),
            None => Ok(None),
        }
    }

    /// 删除向量的元数据。
    pub fn delete_metadata(&self, id: u64) -> Result<(), Error> {
        self.keyspace.delete(&meta_key(id))
    }

    /// 带元数据过滤的 KNN 搜索。
    ///
    /// 使用 over-fetch 策略：先搜索 k*4 个候选，过滤后不足 k 个则扩大到 k*16。
    /// 过滤条件为 AND 语义（所有条件必须同时满足）。
    pub fn search_with_filter(
        &self,
        query: &[f32],
        k: usize,
        metric: &str,
        filters: &[MetaFilter],
    ) -> Result<Vec<(u64, f32)>, Error> {
        if filters.is_empty() {
            return self.search(query, k, metric);
        }
        // 第一轮：over-fetch 4x
        let fetch_1 = k.saturating_mul(FILTER_OVER_FETCH_1);
        let candidates = self.search(query, fetch_1, metric)?;
        let mut results = self.apply_meta_filter(&candidates, filters)?;
        if results.len() >= k {
            results.truncate(k);
            return Ok(results);
        }
        // 第二轮：over-fetch 16x
        let fetch_2 = k.saturating_mul(FILTER_OVER_FETCH_2);
        let candidates = self.search(query, fetch_2, metric)?;
        results = self.apply_meta_filter(&candidates, filters)?;
        results.truncate(k);
        Ok(results)
    }

    /// 对搜索候选结果应用 metadata 过滤。
    fn apply_meta_filter(
        &self,
        candidates: &[(u64, f32)],
        filters: &[MetaFilter],
    ) -> Result<Vec<(u64, f32)>, Error> {
        let mut results = Vec::with_capacity(candidates.len());
        for &(id, dist) in candidates {
            if let Some(raw) = self.keyspace.get(&meta_key(id))? {
                if let Some(meta) = decode_metadata(&raw) {
                    if matches_filters(&meta, filters) {
                        results.push((id, dist));
                    }
                }
            }
            // 无 metadata 的向量不匹配任何过滤条件
        }
        Ok(results)
    }

    /// 插入向量并同时设置元数据。
    pub fn insert_with_metadata(
        &self,
        id: u64,
        vec: &[f32],
        metadata: &HashMap<String, MetaValue>,
    ) -> Result<(), Error> {
        self.insert(id, vec)?;
        self.set_metadata(id, metadata)?;
        Ok(())
    }
}
