/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! BM25 评分算法：对标 Elasticsearch 默认评分。
//!
//! BM25(q, d) = Σ IDF(t) × (tf(t,d) × (k1+1)) / (tf(t,d) + k1 × (1 - b + b × dl/avgdl))
//! IDF(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))

/// BM25 参数（与 ES 默认一致）。
pub(super) const K1: f64 = 1.2;
pub(super) const B: f64 = 0.75;

/// 计算单个 term 的 IDF。
/// N = 文档总数, df = 包含该 term 的文档数。
pub(super) fn idf(doc_count: u64, df: u64) -> f64 {
    let n = doc_count as f64;
    let df = df as f64;
    ((1.0 + (n - df + 0.5) / (df + 0.5)).ln()).max(0.0)
}

/// 计算单个 term 对单个文档的 BM25 分数。
/// tf = term 在文档中出现的次数, dl = 文档长度(token 数), avgdl = 平均文档长度。
pub(super) fn term_score(tf: u32, dl: u32, avgdl: f64, idf_val: f64) -> f64 {
    let tf = tf as f64;
    let dl = dl as f64;
    let norm_tf = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
    idf_val * norm_tf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idf_basic() {
        // 1000 文档中出现 10 次的 term
        let v = idf(1000, 10);
        assert!(v > 0.0);
        assert!(v < 10.0);
    }

    #[test]
    fn idf_rare_term_higher() {
        let common = idf(1000, 500);
        let rare = idf(1000, 5);
        assert!(rare > common, "rare={} should > common={}", rare, common);
    }

    #[test]
    fn term_score_basic() {
        let s = term_score(3, 100, 120.0, 2.0);
        assert!(s > 0.0);
    }

    #[test]
    fn higher_tf_higher_score() {
        let idf_v = idf(1000, 10);
        let s1 = term_score(1, 100, 100.0, idf_v);
        let s3 = term_score(3, 100, 100.0, idf_v);
        assert!(s3 > s1);
    }
}
