/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 距离函数（委托到 cross::distance）与向量序列化/堆辅助结构。

use crate::error::Error;

// ── 距离函数：统一使用 cross::distance 规范实现 ──────────

pub(super) use crate::cross::distance::{cosine_distance, dot_distance, DistFn};

/// 根据度量名称返回距离函数。
pub(super) fn dist_fn(metric: &str) -> Result<DistFn, Error> {
    crate::cross::distance::resolve_dist_fn(metric)
        .map_err(|_| Error::Serialization(format!("未知度量: {}", metric)))
}

// ── 向量序列化 ────────────────────────────────────────────

/// 向量序列化：预分配精确容量，零 realloc（对标 Qdrant 向量编码路径）。
pub(super) fn serialize_vec(vec: &[f32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(vec.len() * 4);
    for f in vec {
        buf.extend_from_slice(&f.to_le_bytes());
    }
    buf
}

pub(super) fn deserialize_vec(raw: &[u8]) -> Result<Vec<f32>, Error> {
    if raw.len() % 4 != 0 {
        return Err(Error::Serialization("向量字节长非 4 的倍数".into()));
    }
    let count = raw.len() / 4;
    // 零拷贝快速路径：little-endian 平台直接 memcpy 整个块
    // Apple Silicon (aarch64) 和 x86_64 都是 little-endian
    #[cfg(target_endian = "little")]
    {
        let mut vec = vec![0.0f32; count];
        // Safety: f32 和 [u8;4] 在 LE 平台上布局相同
        unsafe {
            std::ptr::copy_nonoverlapping(raw.as_ptr(), vec.as_mut_ptr() as *mut u8, raw.len());
        }
        Ok(vec)
    }
    #[cfg(not(target_endian = "little"))]
    {
        Ok(raw
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
}

// ── 小顶堆 / 大顶堆辅助 ─────────────────────────────────

/// (distance, id) — 用于小顶堆（最近的在堆顶）。
#[derive(Clone, PartialEq)]
pub(super) struct MinItem(pub f32, pub u64);

impl Eq for MinItem {}

impl PartialOrd for MinItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 反转：BinaryHeap 是大顶堆，反转后变小顶堆
        other
            .0
            .partial_cmp(&self.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// (distance, id) — 用于大顶堆（最远的在堆顶）。
#[derive(Clone, PartialEq)]
pub(super) struct MaxItem(pub f32, pub u64);

impl Eq for MaxItem {}

impl PartialOrd for MaxItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}
