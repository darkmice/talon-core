/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 多字段搜索：按字段权重加权 BM25 评分。
//!
//! M101：对标 Elasticsearch multi_match，支持字段级权重。
//! 两阶段策略：先用全文档倒排索引初筛候选，再逐字段重评分。

use std::collections::BTreeMap;

use super::bm25;
use super::tokenizer::{self, Analyzer};
use super::{
    decode_inv_entry, df_key, doc_ks_name, hash_bytes, highlight_fields, inv_ks_name, inv_prefix,
    meta_ks_name, read_u64, FtsEngine, SearchHit, META_DOC_COUNT, META_TOTAL_LEN,
};
use crate::error::Error;
use std::collections::HashMap;

/// 多字段搜索查询。
#[derive(Debug, Clone)]
pub struct MultiFieldQuery {
    /// 搜索文本。
    pub query: String,
    /// 字段名 → 权重（如 `{"title": 2.0, "body": 1.0}`）。
    /// 空 map 时搜索所有字段，权重均为 1.0。
    pub field_weights: BTreeMap<String, f64>,
}

impl FtsEngine {
    /// 多字段搜索：按字段权重加权 BM25 评分。
    ///
    /// 两阶段策略：
    /// 1. 用全文档倒排索引做 BM25 初筛，取 `limit * 3` 个候选
    /// 2. 对候选文档逐字段分词，计算字段级加权 BM25，重新排序
    ///
    /// `field_weights` 为空时退化为普通 `search()`（所有字段权重 1.0）。
    pub fn search_multi_field(
        &self,
        name: &str,
        query: &MultiFieldQuery,
        limit: usize,
    ) -> Result<Vec<SearchHit>, Error> {
        let inv_ks = self.store.open_keyspace(&inv_ks_name(name))?;
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        let doc_count = read_u64(&stat_ks, META_DOC_COUNT);
        let total_len = read_u64(&stat_ks, META_TOTAL_LEN);
        if doc_count == 0 {
            return Ok(vec![]);
        }
        let avgdl = total_len as f64 / doc_count as f64;
        let tokens = tokenizer::tokenize(&query.query, Analyzer::Standard);
        if tokens.is_empty() {
            return Ok(vec![]);
        }

        // ── 阶段一：全文档倒排索引初筛 ──
        let candidate_limit = limit.saturating_mul(3).max(30);
        let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
        let mut term_idfs: Vec<(String, f64)> = Vec::with_capacity(tokens.len());

        for term in &tokens {
            let dk = df_key(term);
            let df = stat_ks
                .get(&dk)?
                .map(|v| {
                    if v.len() >= 8 {
                        u64::from_le_bytes(v[..8].try_into().unwrap())
                    } else {
                        0
                    }
                })
                .unwrap_or(0);
            if df == 0 {
                continue;
            }
            let idf_val = bm25::idf(doc_count, df);
            term_idfs.push((term.clone(), idf_val));
            let prefix = inv_prefix(term);
            inv_ks.for_each_kv_prefix(&prefix, |key, val| {
                if key.len() != 16 {
                    return true;
                }
                if let Some((tf, dl, _)) = decode_inv_entry(val) {
                    let doc_id_hash = key[8..16].to_vec();
                    let s = bm25::term_score(tf, dl, avgdl, idf_val);
                    *scores.entry(doc_id_hash).or_insert(0.0) += s;
                }
                true
            })?;
        }

        if scores.is_empty() {
            return Ok(vec![]);
        }

        // 取 Top candidate_limit 候选
        let mut scored: Vec<(Vec<u8>, f64)> = scores.into_iter().collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(candidate_limit);

        let candidate_hashes: HashMap<Vec<u8>, f64> = scored.into_iter().collect();

        // ── 阶段二：逐字段重评分 ──
        let use_weights = !query.field_weights.is_empty();
        let mut results = Vec::with_capacity(candidate_hashes.len());

        doc_ks.for_each_kv_prefix(b"", |key, val| {
            let doc_id = String::from_utf8_lossy(key).to_string();
            let dh = hash_bytes(&doc_id);
            if !candidate_hashes.contains_key(dh.as_slice()) {
                return true;
            }
            let fields: BTreeMap<String, String> = match serde_json::from_slice(val) {
                Ok(f) => f,
                Err(_) => return true,
            };

            let final_score = if use_weights {
                field_weighted_score(&fields, &term_idfs, avgdl, &query.field_weights)
            } else {
                // 无权重时用阶段一的分数
                candidate_hashes.get(dh.as_slice()).copied().unwrap_or(0.0)
            };

            let highlights = highlight_fields(&fields, &tokens);
            results.push(SearchHit {
                doc_id,
                score: final_score,
                fields,
                highlights,
            });
            true
        })?;

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        Ok(results)
    }
}

/// 计算字段级加权 BM25 分数。
///
/// 对每个字段独立分词计算 TF，结合全局 IDF 和 avgdl，乘以字段权重后求和。
/// 采用 best_fields 策略（对标 ES multi_match 默认模式）：取各字段中最高分 × 权重。
/// 未在 `weights` 中指定的字段不参与评分（权重视为 0）。
fn field_weighted_score(
    fields: &BTreeMap<String, String>,
    term_idfs: &[(String, f64)],
    avgdl: f64,
    weights: &BTreeMap<String, f64>,
) -> f64 {
    let mut best_score = 0.0_f64;

    for (field_name, field_text) in fields {
        let weight = weights.get(field_name).copied().unwrap_or(0.0);
        if weight <= 0.0 {
            // 未指定权重的字段不参与评分（除非 weights 为空，但调用方已处理）
            // 如果用户只指定了部分字段，未指定的字段权重为 0
            continue;
        }

        let field_tokens = tokenizer::tokenize(field_text, Analyzer::Standard);
        let field_len = field_tokens.len() as u32;
        if field_len == 0 {
            continue;
        }

        // 统计该字段中各 term 的 TF
        let mut tf_map: HashMap<&str, u32> = HashMap::new();
        for t in &field_tokens {
            *tf_map.entry(t.as_str()).or_insert(0) += 1;
        }

        // 计算该字段的 BM25 分数
        let mut field_score = 0.0;
        for (term, idf_val) in term_idfs {
            let tf = tf_map.get(term.as_str()).copied().unwrap_or(0);
            if tf > 0 {
                field_score += bm25::term_score(tf, field_len, avgdl, *idf_val);
            }
        }

        let weighted = field_score * weight;
        if weighted > best_score {
            best_score = weighted;
        }
    }

    best_score
}
