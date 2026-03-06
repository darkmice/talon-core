/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQ8 标量量化：f32 → u8 压缩，约 4:1 压缩比，精度损失 <2%。

use crate::error::Error;

use super::distance::{cosine_distance, dot_distance};

/// SQ8 标量量化参数（M106：二进制编码）。
#[derive(Debug, Clone)]
pub(super) struct QuantizationParams {
    pub mins: Vec<f32>,
    pub maxs: Vec<f32>,
}
impl QuantizationParams {
    pub fn encode(&self) -> Vec<u8> {
        let dim = self.mins.len();
        let mut buf = Vec::with_capacity(4 + dim * 8);
        buf.extend_from_slice(&(dim as u32).to_le_bytes());
        for &v in &self.mins {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        for &v in &self.maxs {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }
    pub fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() < 4 {
            return Err(Error::Serialization("QuantizationParams 数据不足".into()));
        }
        let dim = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
        let expected = 4 + dim * 8;
        if raw.len() < expected {
            return Err(Error::Serialization(
                "QuantizationParams 维度数据不足".into(),
            ));
        }
        let mut mins = Vec::with_capacity(dim);
        let mut maxs = Vec::with_capacity(dim);
        for i in 0..dim {
            let off = 4 + i * 4;
            mins.push(f32::from_le_bytes(raw[off..off + 4].try_into().unwrap()));
        }
        for i in 0..dim {
            let off = 4 + dim * 4 + i * 4;
            maxs.push(f32::from_le_bytes(raw[off..off + 4].try_into().unwrap()));
        }
        Ok(QuantizationParams { mins, maxs })
    }
}

/// 量化向量 key 前缀：`q:{id_be}`
pub(super) fn quant_key(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(b"q:");
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// 将 f32 向量量化为 u8 数组。
pub(super) fn quantize_vec(vec: &[f32], params: &QuantizationParams) -> Vec<u8> {
    debug_assert_eq!(vec.len(), params.mins.len(), "量化维度不匹配");
    vec.iter()
        .enumerate()
        .map(|(i, &v)| {
            let min = params.mins[i];
            let max = params.maxs[i];
            let range = max - min;
            if range <= f32::EPSILON {
                128u8 // 常量维度映射到中间值
            } else {
                let clamped = v.clamp(min, max);
                ((clamped - min) / range * 255.0).round() as u8
            }
        })
        .collect()
}

/// 将 u8 量化向量解码为 f32。
pub(super) fn dequantize_vec(quantized: &[u8], params: &QuantizationParams) -> Vec<f32> {
    debug_assert_eq!(quantized.len(), params.mins.len(), "反量化维度不匹配");
    quantized
        .iter()
        .enumerate()
        .map(|(i, &q)| {
            let min = params.mins[i];
            let max = params.maxs[i];
            min + (q as f32) / 255.0 * (max - min)
        })
        .collect()
}

/// 从一组向量中统计每维度的 min/max，生成量化参数。
/// 支持 &[Vec<f32>] 和 &[&[f32]] 避免不必要的克隆。
pub(super) fn compute_quantization_params(vecs: &[Vec<f32>]) -> Option<QuantizationParams> {
    compute_quantization_params_ref(&vecs.iter().map(|v| v.as_slice()).collect::<Vec<_>>())
}

/// 从引用切片计算量化参数，避免向量数据克隆。
pub(super) fn compute_quantization_params_ref(vecs: &[&[f32]]) -> Option<QuantizationParams> {
    if vecs.is_empty() {
        return None;
    }
    let dim = vecs[0].len();
    if dim == 0 {
        return None;
    }
    let mut mins = vec![f32::INFINITY; dim];
    let mut maxs = vec![f32::NEG_INFINITY; dim];
    for vec in vecs {
        for (i, &v) in vec.iter().enumerate() {
            if v < mins[i] {
                mins[i] = v;
            }
            if v > maxs[i] {
                maxs[i] = v;
            }
        }
    }
    // 对常量维度加微小 epsilon 避免除零
    for i in 0..dim {
        if (maxs[i] - mins[i]).abs() <= f32::EPSILON {
            maxs[i] = mins[i] + 1.0;
        }
    }
    Some(QuantizationParams { mins, maxs })
}

// ── 量化域距离函数（直接在 u8 上计算，避免解码开销）────────

/// 量化域 L2 距离（近似）：在 u8 空间计算，再缩放回原始空间。
pub(super) fn quantized_l2_distance(a: &[u8], b: &[u8], params: &QuantizationParams) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let range = params.maxs[i] - params.mins[i];
        let scale = range / 255.0;
        let diff = (a[i] as f32 - b[i] as f32) * scale;
        sum += diff * diff;
    }
    sum.sqrt()
}

/// 量化域 cosine 距离（近似）：解码后计算（cosine 对缩放敏感，需完整解码）。
pub(super) fn quantized_cosine_distance(a: &[u8], b: &[u8], params: &QuantizationParams) -> f32 {
    let va = dequantize_vec(a, params);
    let vb = dequantize_vec(b, params);
    cosine_distance(&va, &vb)
}

/// 量化域 dot 距离（近似）：解码后计算。
pub(super) fn quantized_dot_distance(a: &[u8], b: &[u8], params: &QuantizationParams) -> f32 {
    let va = dequantize_vec(a, params);
    let vb = dequantize_vec(b, params);
    dot_distance(&va, &vb)
}

/// 量化距离函数类型别名。
pub(super) type QuantDistFn = fn(&[u8], &[u8], &QuantizationParams) -> f32;

/// 根据度量名称返回量化距离函数。
pub(super) fn quant_dist_fn(metric: &str) -> Result<QuantDistFn, Error> {
    match metric.to_lowercase().as_str() {
        "cosine" => Ok(quantized_cosine_distance),
        "l2" | "euclidean" => Ok(quantized_l2_distance),
        "dot" => Ok(quantized_dot_distance),
        _ => Err(Error::Serialization(format!("未知度量: {}", metric))),
    }
}
