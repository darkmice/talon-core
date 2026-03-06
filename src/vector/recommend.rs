/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量推荐与上下文搜索：recommend / discover。
//! 从 mod.rs 拆分，保持单文件 ≤500 行。

use super::distance::dist_fn;
use super::VectorEngine;
use crate::error::Error;

impl VectorEngine {
    /// 基于正负例推荐：找到与 positive 向量相似、远离 negative 向量的结果。
    ///
    /// 算法：`query = avg(positive) - avg(negative)`，归一化后执行 ANN 搜索。
    /// 对标 Qdrant `recommend` API。positive 不可为空，negative 可为空（退化为均值搜索）。
    pub fn recommend(
        &self,
        positive: &[&[f32]],
        negative: &[&[f32]],
        top_k: usize,
        metric: &str,
    ) -> Result<Vec<(u64, f32)>, Error> {
        if positive.is_empty() {
            return Err(Error::Vector(
                "recommend requires at least one positive example".into(),
            ));
        }
        let dim = positive[0].len();
        // 计算正例均值
        let mut query = vec![0.0f32; dim];
        for v in positive {
            if v.len() != dim {
                return Err(Error::VectorDimMismatch(dim, v.len()));
            }
            for (q, &x) in query.iter_mut().zip(v.iter()) {
                *q += x;
            }
        }
        let pos_n = positive.len() as f32;
        for q in query.iter_mut() {
            *q /= pos_n;
        }
        // 减去负例均值
        if !negative.is_empty() {
            let mut neg_avg = vec![0.0f32; dim];
            for v in negative {
                if v.len() != dim {
                    return Err(Error::VectorDimMismatch(dim, v.len()));
                }
                for (n, &x) in neg_avg.iter_mut().zip(v.iter()) {
                    *n += x;
                }
            }
            let neg_n = negative.len() as f32;
            for (q, n) in query.iter_mut().zip(neg_avg.iter()) {
                *q -= n / neg_n;
            }
        }
        // L2 归一化
        let norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-10 {
            for q in query.iter_mut() {
                *q /= norm;
            }
        }
        self.search(&query, top_k, metric)
    }

    /// 上下文搜索：给定目标向量和多组 (positive, negative) 对，
    /// 找到更接近每组 positive 而非 negative 的向量。
    ///
    /// 算法：先用 target 做 ANN 搜索获取候选集，再用上下文对重排序。
    /// 对标 Qdrant `discover` API。
    pub fn discover(
        &self,
        target: &[f32],
        context: &[(&[f32], &[f32])],
        top_k: usize,
        metric: &str,
    ) -> Result<Vec<(u64, f32)>, Error> {
        if context.is_empty() {
            return self.search(target, top_k, metric);
        }
        let distance = dist_fn(metric)?;
        // 扩大候选集确保召回率
        let candidates = self.search(target, top_k * 4, metric)?;
        let mut scored: Vec<(u64, f32)> = Vec::with_capacity(candidates.len());
        for &(id, base_dist) in &candidates {
            let vec = match self.load_vec(id)? {
                Some(v) => v,
                None => continue,
            };
            // 上下文重排序：每组 (pos, neg) 贡献 sigmoid(dist_neg - dist_pos)
            let mut ctx_score: f32 = 0.0;
            for &(pos, neg) in context {
                let d_pos = distance(&vec, pos);
                let d_neg = distance(&vec, neg);
                ctx_score += 1.0 / (1.0 + (-(d_neg - d_pos)).exp());
            }
            // 综合分数：base_dist 越小越好（取负），ctx_score 越大越好
            let final_score = -base_dist + ctx_score;
            scored.push((id, final_score));
        }
        // 按 final_score 降序排序
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);
        Ok(scored)
    }
}
