/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 跨引擎联合查询共享的向量距离度量函数。
//!
//! 使用手动 4-lane 循环展开模拟 SIMD 效果：
//! - 减少循环控制开销
//! - 帮助编译器自动向量化（auto-vectorization）
//! - 在 `-C opt-level=3` 下编译器会生成真正的 SIMD 指令（SSE/AVX/NEON）
//! - 兼容所有平台，零外部依赖

use crate::error::Error;

/// 向量距离度量函数类型。
pub(crate) type DistFn = fn(&[f32], &[f32]) -> f32;

/// 余弦距离：`1 - cosine_similarity`。
/// 8-lane 展开 + 多累加器消除循环依赖，利于编译器自动向量化（AVX2/NEON）。
#[inline]
pub(crate) fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }
    let n = a.len();
    let chunks = n / 8;

    let (mut dot0, mut dot1, mut dot2, mut dot3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut dot4, mut dot5, mut dot6, mut dot7) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut na0, mut na1, mut na2, mut na3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut na4, mut na5, mut na6, mut na7) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut nb0, mut nb1, mut nb2, mut nb3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut nb4, mut nb5, mut nb6, mut nb7) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);

    let a_chunks = a[..chunks * 8].chunks_exact(8);
    let b_chunks = b[..chunks * 8].chunks_exact(8);
    for (ac, bc) in a_chunks.zip(b_chunks) {
        dot0 += ac[0] * bc[0];
        dot1 += ac[1] * bc[1];
        dot2 += ac[2] * bc[2];
        dot3 += ac[3] * bc[3];
        dot4 += ac[4] * bc[4];
        dot5 += ac[5] * bc[5];
        dot6 += ac[6] * bc[6];
        dot7 += ac[7] * bc[7];
        na0 += ac[0] * ac[0];
        na1 += ac[1] * ac[1];
        na2 += ac[2] * ac[2];
        na3 += ac[3] * ac[3];
        na4 += ac[4] * ac[4];
        na5 += ac[5] * ac[5];
        na6 += ac[6] * ac[6];
        na7 += ac[7] * ac[7];
        nb0 += bc[0] * bc[0];
        nb1 += bc[1] * bc[1];
        nb2 += bc[2] * bc[2];
        nb3 += bc[3] * bc[3];
        nb4 += bc[4] * bc[4];
        nb5 += bc[5] * bc[5];
        nb6 += bc[6] * bc[6];
        nb7 += bc[7] * bc[7];
    }

    let mut dot = ((dot0 + dot1) + (dot2 + dot3)) + ((dot4 + dot5) + (dot6 + dot7));
    let mut norm_a = ((na0 + na1) + (na2 + na3)) + ((na4 + na5) + (na6 + na7));
    let mut norm_b = ((nb0 + nb1) + (nb2 + nb3)) + ((nb4 + nb5) + (nb6 + nb7));

    let start = chunks * 8;
    for i in start..n {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        1.0
    } else {
        1.0 - dot / denom
    }
}

/// L2（欧几里得）距离。
/// 8-lane 展开 + 多累加器，利于编译器自动向量化（AVX2/NEON）。
#[inline]
pub(crate) fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }
    let n = a.len();
    let chunks = n / 8;

    let (mut s0, mut s1, mut s2, mut s3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut s4, mut s5, mut s6, mut s7) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);

    let a_chunks = a[..chunks * 8].chunks_exact(8);
    let b_chunks = b[..chunks * 8].chunks_exact(8);
    for (ac, bc) in a_chunks.zip(b_chunks) {
        let d0 = ac[0] - bc[0];
        let d1 = ac[1] - bc[1];
        let d2 = ac[2] - bc[2];
        let d3 = ac[3] - bc[3];
        let d4 = ac[4] - bc[4];
        let d5 = ac[5] - bc[5];
        let d6 = ac[6] - bc[6];
        let d7 = ac[7] - bc[7];
        s0 += d0 * d0;
        s1 += d1 * d1;
        s2 += d2 * d2;
        s3 += d3 * d3;
        s4 += d4 * d4;
        s5 += d5 * d5;
        s6 += d6 * d6;
        s7 += d7 * d7;
    }

    let mut sum = ((s0 + s1) + (s2 + s3)) + ((s4 + s5) + (s6 + s7));
    let start = chunks * 8;
    for i in start..n {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

/// 负点积距离（dot product 越大越相似，取负使其与 distance 语义一致）。
/// 8-lane 展开 + 多累加器，利于编译器自动向量化（AVX2/NEON）。
#[inline]
pub(crate) fn dot_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }
    let n = a.len();
    let chunks = n / 8;

    let (mut d0, mut d1, mut d2, mut d3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
    let (mut d4, mut d5, mut d6, mut d7) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);

    let a_chunks = a[..chunks * 8].chunks_exact(8);
    let b_chunks = b[..chunks * 8].chunks_exact(8);
    for (ac, bc) in a_chunks.zip(b_chunks) {
        d0 += ac[0] * bc[0];
        d1 += ac[1] * bc[1];
        d2 += ac[2] * bc[2];
        d3 += ac[3] * bc[3];
        d4 += ac[4] * bc[4];
        d5 += ac[5] * bc[5];
        d6 += ac[6] * bc[6];
        d7 += ac[7] * bc[7];
    }

    let mut dot = ((d0 + d1) + (d2 + d3)) + ((d4 + d5) + (d6 + d7));
    let start = chunks * 8;
    for i in start..n {
        dot += a[i] * b[i];
    }
    -dot
}

/// 解析距离度量函数。
pub(crate) fn resolve_dist_fn(metric: &str) -> Result<DistFn, Error> {
    match metric.to_ascii_lowercase().as_str() {
        "cosine" => Ok(cosine_distance),
        "l2" | "euclidean" => Ok(l2_distance),
        "dot" => Ok(dot_distance),
        _ => Err(Error::Serialization(format!(
            "unsupported metric: {}, use cosine/l2/dot",
            metric
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_functions() {
        let a = [1.0, 0.0, 0.0];
        let b = [0.0, 1.0, 0.0];
        let cd = cosine_distance(&a, &b);
        assert!((cd - 1.0).abs() < 0.01);
        let ld = l2_distance(&a, &b);
        assert!((ld - std::f32::consts::SQRT_2).abs() < 0.01);
        let dd = dot_distance(&a, &b);
        assert!((dd - 0.0).abs() < 0.01);
    }

    #[test]
    fn distance_large_vectors() {
        let dim = 128;
        let a: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..dim).map(|i| ((dim - i) as f32) * 0.01).collect();

        let cd = cosine_distance(&a, &b);
        assert!(cd > 0.0 && cd < 2.0);
        let ld = l2_distance(&a, &b);
        assert!(ld > 0.0);
        let dd = dot_distance(&a, &b);
        assert!(dd < 0.0); // negative of positive dot product
    }

    #[test]
    fn distance_odd_dimensions() {
        let a = [1.0, 2.0, 3.0, 4.0, 5.0];
        let b = [5.0, 4.0, 3.0, 2.0, 1.0];
        let cd = cosine_distance(&a, &b);
        assert!(cd > 0.0 && cd < 1.0);
    }
}
