/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Hybrid Search：FTS BM25 + Vector KNN 融合搜索（Reciprocal Rank Fusion）。
//!
//! 对标 Elasticsearch 8.x knn_search + match query 混合检索。
//! RAG 核心能力：关键词精确匹配 + 语义相似度，两路融合。
//!
//! 融合算法：RRF (Reciprocal Rank Fusion)
//! RRF_score(d) = Σ 1 / (k + rank_i(d))，k=60（业界默认）。
//! 不依赖分数归一化，只看排名位置，适合异构分数源融合。

use crate::error::Error;
use crate::storage::Store;
use crate::vector::metadata::{MetaFilter, MetaFilterOp, MetaValue};
use crate::vector::VectorEngine;
use std::collections::{BTreeMap, HashMap};

use super::FtsEngine;

/// RRF 默认常数（与 ES、LangChain EnsembleRetriever 一致）。
const RRF_K: f64 = 60.0;

/// 候选池大小软上限，防止误传极大值导致 OOM。
const MAX_CANDIDATES: usize = 10_000;

/// Hybrid Search 命中结果。
#[derive(Debug, Clone)]
pub struct HybridHit {
    /// 文档 ID。
    pub doc_id: String,
    /// 融合后的 RRF 分数。
    pub rrf_score: f64,
    /// BM25 分数（如果全文搜索命中）。
    pub bm25_score: Option<f64>,
    /// 向量距离（如果向量搜索命中）。
    pub vector_dist: Option<f32>,
    /// 文档原始字段。
    pub fields: BTreeMap<String, String>,
}

/// Hybrid Search 查询参数。
#[derive(Debug, Clone)]
pub struct HybridQuery<'a> {
    /// FTS 索引名。
    pub fts_index: &'a str,
    /// 向量索引名。
    pub vec_index: &'a str,
    /// 全文搜索查询文本。
    pub query_text: &'a str,
    /// 向量查询（与向量索引同维度）。
    pub query_vec: &'a [f32],
    /// 向量距离度量（"cosine" / "l2" / "dot"）。
    pub metric: &'a str,
    /// 最终返回数量。
    pub limit: usize,
    /// FTS 路权重（默认 1.0）。
    pub fts_weight: f64,
    /// Vector 路权重（默认 1.0）。
    pub vec_weight: f64,
    /// 向量搜索候选池大小（对标 ES `num_candidates`）。
    /// `None` 时默认 `limit * 3`。值越大召回率越高但越慢。
    /// 建议范围：`limit` ~ `limit * 10`，小于 `limit` 时自动提升至 `limit`。
    pub num_candidates: Option<usize>,
    /// 前置过滤条件：(field, value) 精确匹配，AND 语义。
    /// FTS 路：搜索后按文档字段过滤。
    /// Vector 路：转为 metadata Eq 过滤。
    /// 对标 ES knn_search `filter` 参数。
    pub pre_filter: Option<Vec<(&'a str, &'a str)>>,
}

/// 执行 Hybrid Search：FTS BM25 + Vector KNN，RRF 融合。
///
/// 流程：
/// 1. FTS 搜索 Top-N（N = limit × 3，扩大召回）
/// 2. Vector KNN 搜索 Top-N
/// 3. RRF 融合两路排名 → 按 rrf_score 排序取 Top-limit
pub fn hybrid_search(store: &Store, q: &HybridQuery) -> Result<Vec<HybridHit>, Error> {
    let recall_n = q
        .num_candidates
        .unwrap_or(q.limit.saturating_mul(3))
        .max(q.limit)
        .min(MAX_CANDIDATES);
    // 1. FTS BM25 搜索（pre_filter 后过滤）
    let fts = FtsEngine::open(store)?;
    let fts_all = fts.search(q.fts_index, q.query_text, recall_n)?;
    let fts_hits: Vec<_> = if let Some(ref filters) = q.pre_filter {
        fts_all
            .into_iter()
            .filter(|hit| {
                filters.iter().all(|&(field, value)| {
                    hit.fields.get(field).map(|v| v == value).unwrap_or(false)
                })
            })
            .collect()
    } else {
        fts_all
    };
    // 2. Vector KNN 搜索（pre_filter 转 metadata filter）
    let vec_engine = VectorEngine::open(store, q.vec_index)?;
    let vec_hits = if let Some(ref filters) = q.pre_filter {
        let meta_filters: Vec<MetaFilter> = filters
            .iter()
            .map(|&(field, value)| MetaFilter {
                field: field.to_string(),
                op: MetaFilterOp::Eq(MetaValue::String(value.to_string())),
            })
            .collect();
        vec_engine.search_with_filter(q.query_vec, recall_n, q.metric, &meta_filters)?
    } else {
        vec_engine.search(q.query_vec, recall_n, q.metric)?
    };
    // 3. RRF 融合
    let cap = fts_hits.len() + vec_hits.len();
    let mut rrf_scores: HashMap<String, f64> = HashMap::with_capacity(cap);
    let mut bm25_map: HashMap<String, f64> = HashMap::with_capacity(fts_hits.len());
    let mut vec_map: HashMap<String, f32> = HashMap::with_capacity(vec_hits.len());
    let mut fields_map: HashMap<String, BTreeMap<String, String>> = HashMap::with_capacity(cap);
    // FTS 路排名 → RRF 分数
    for (rank, hit) in fts_hits.iter().enumerate() {
        let rrf = q.fts_weight / (RRF_K + (rank + 1) as f64);
        *rrf_scores.entry(hit.doc_id.clone()).or_insert(0.0) += rrf;
        bm25_map.insert(hit.doc_id.clone(), hit.score);
        fields_map.insert(hit.doc_id.clone(), hit.fields.clone());
    }
    // Vector 路排名 → RRF 分数
    // vec_hits 中 id 是 u64，需要转为 doc_id 字符串
    for (rank, &(vec_id, dist)) in vec_hits.iter().enumerate() {
        let doc_id = vec_id.to_string();
        let rrf = q.vec_weight / (RRF_K + (rank + 1) as f64);
        *rrf_scores.entry(doc_id.clone()).or_insert(0.0) += rrf;
        vec_map.insert(doc_id, dist);
    }
    // 尝试从 FTS 补充缺失的 fields
    for doc_id in rrf_scores.keys() {
        if !fields_map.contains_key(doc_id) {
            if let Ok(Some(fields)) = fts.get_doc(q.fts_index, doc_id) {
                fields_map.insert(doc_id.clone(), fields);
            }
        }
    }
    // 按 RRF 分数排序
    let mut results: Vec<HybridHit> = rrf_scores
        .into_iter()
        .map(|(doc_id, rrf_score)| HybridHit {
            bm25_score: bm25_map.get(&doc_id).copied(),
            vector_dist: vec_map.get(&doc_id).copied(),
            fields: fields_map.remove(&doc_id).unwrap_or_default(),
            doc_id,
            rrf_score,
        })
        .collect();
    results.sort_by(|a, b| {
        b.rrf_score
            .partial_cmp(&a.rrf_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(q.limit);
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::{FtsConfig, FtsDoc};
    use crate::storage::Store;

    #[test]
    fn hybrid_fts_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("docs", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("text".into(), "Rust programming language".into())]),
            },
        )
        .unwrap();
        let _ = VectorEngine::open(&store, "vecs").unwrap();
        let hits = hybrid_search(
            &store,
            &HybridQuery {
                fts_index: "docs",
                vec_index: "vecs",
                query_text: "rust",
                query_vec: &[0.0; 4],
                metric: "cosine",
                limit: 10,
                fts_weight: 1.0,
                vec_weight: 1.0,
                num_candidates: None,
                pre_filter: None,
            },
        )
        .unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].doc_id, "1");
        assert!(hits[0].bm25_score.is_some());
        assert!(hits[0].vector_dist.is_none());
    }

    #[test]
    fn hybrid_both_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("docs", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("text".into(), "machine learning AI".into())]),
            },
        )
        .unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "2".into(),
                fields: BTreeMap::from([("text".into(), "deep learning neural network".into())]),
            },
        )
        .unwrap();
        let vec_engine = VectorEngine::open(&store, "vecs").unwrap();
        vec_engine.insert(1, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        vec_engine.insert(2, &[0.0, 1.0, 0.0, 0.0]).unwrap();
        let hits = hybrid_search(
            &store,
            &HybridQuery {
                fts_index: "docs",
                vec_index: "vecs",
                query_text: "machine learning",
                query_vec: &[0.1, 0.9, 0.0, 0.0],
                metric: "cosine",
                limit: 10,
                fts_weight: 1.0,
                vec_weight: 1.0,
                num_candidates: None,
                pre_filter: None,
            },
        )
        .unwrap();
        assert!(hits.len() >= 2);
        for hit in &hits {
            assert!(hit.rrf_score > 0.0);
        }
    }

    #[test]
    fn rrf_weight_affects_ranking() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("w", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "w",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("t".into(), "alpha beta".into())]),
            },
        )
        .unwrap();
        let _ = VectorEngine::open(&store, "wv").unwrap();
        let hits = hybrid_search(
            &store,
            &HybridQuery {
                fts_index: "w",
                vec_index: "wv",
                query_text: "alpha",
                query_vec: &[0.0; 4],
                metric: "cosine",
                limit: 10,
                fts_weight: 0.0,
                vec_weight: 1.0,
                num_candidates: None,
                pre_filter: None,
            },
        )
        .unwrap();
        for hit in &hits {
            assert!(hit.rrf_score == 0.0 || hit.rrf_score < 0.001);
        }
    }

    #[test]
    fn num_candidates_custom_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("nc", &FtsConfig::default()).unwrap();
        for i in 0..20 {
            fts.index_doc(
                "nc",
                &FtsDoc {
                    doc_id: format!("{}", i),
                    fields: BTreeMap::from([(
                        "text".into(),
                        format!("document about rust topic {}", i),
                    )]),
                },
            )
            .unwrap();
        }
        let vec_engine = VectorEngine::open(&store, "ncv").unwrap();
        for i in 0u64..20 {
            let mut v = [0.0f32; 4];
            v[(i % 4) as usize] = 1.0;
            vec_engine.insert(i, &v).unwrap();
        }
        // num_candidates = 5，limit = 3 → 两路各召回 5 条，RRF 融合后取 3
        let hits = hybrid_search(
            &store,
            &HybridQuery {
                fts_index: "nc",
                vec_index: "ncv",
                query_text: "rust",
                query_vec: &[1.0, 0.0, 0.0, 0.0],
                metric: "cosine",
                limit: 3,
                fts_weight: 1.0,
                vec_weight: 1.0,
                num_candidates: Some(5),
                pre_filter: None,
            },
        )
        .unwrap();
        assert!(hits.len() <= 3, "should respect limit");
        assert!(!hits.is_empty(), "should have results");
        // num_candidates 小于默认 limit*3=9，验证不会 panic 且结果合理
        for hit in &hits {
            assert!(hit.rrf_score > 0.0);
        }
    }

    #[test]
    fn num_candidates_less_than_limit_clamped() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("nc2", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "nc2",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("text".into(), "hello world".into())]),
            },
        )
        .unwrap();
        let _ = VectorEngine::open(&store, "nc2v").unwrap();
        // num_candidates=1, limit=5 → .max(limit) 应把 recall_n 提升到 5
        let hits = hybrid_search(
            &store,
            &HybridQuery {
                fts_index: "nc2",
                vec_index: "nc2v",
                query_text: "hello",
                query_vec: &[0.0; 4],
                metric: "cosine",
                limit: 5,
                fts_weight: 1.0,
                vec_weight: 1.0,
                num_candidates: Some(1),
                pre_filter: None,
            },
        )
        .unwrap();
        // 即使 num_candidates=1，.max(limit) 确保 recall_n >= limit
        assert!(!hits.is_empty());
    }

    #[test]
    fn pre_filter_fts_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("pf", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "pf",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([
                    ("text".into(), "rust programming".into()),
                    ("ns".into(), "tenant_a".into()),
                ]),
            },
        )
        .unwrap();
        fts.index_doc(
            "pf",
            &FtsDoc {
                doc_id: "2".into(),
                fields: BTreeMap::from([
                    ("text".into(), "rust systems".into()),
                    ("ns".into(), "tenant_b".into()),
                ]),
            },
        )
        .unwrap();
        let _ = VectorEngine::open(&store, "pfv").unwrap();
        // pre_filter: ns=tenant_a → 只返回 doc1
        let hits = hybrid_search(
            &store,
            &HybridQuery {
                fts_index: "pf",
                vec_index: "pfv",
                query_text: "rust",
                query_vec: &[0.0; 4],
                metric: "cosine",
                limit: 10,
                fts_weight: 1.0,
                vec_weight: 1.0,
                num_candidates: None,
                pre_filter: Some(vec![("ns", "tenant_a")]),
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, "1");
    }
}
