/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 布尔查询：AND / OR / NOT 组合搜索。
//!
//! M77：对标 Elasticsearch bool query，支持 must/should/must_not。

use std::collections::{BTreeMap, HashMap, HashSet};

use super::bm25;
use super::tokenizer::{self, Analyzer};
use super::{
    decode_inv_entry, df_key, doc_ks_name, hash_bytes, highlight_fields, inv_ks_name, inv_prefix,
    meta_ks_name, read_u64, FtsEngine, SearchHit, META_DOC_COUNT, META_TOTAL_LEN,
};
use crate::error::Error;

/// 布尔查询条件。
#[derive(Debug, Clone, Default)]
pub struct BoolQuery {
    /// AND：所有 term 必须出现在文档中。
    pub must: Vec<String>,
    /// OR：至少一个 term 出现即贡献分数（must 为空时作为主查询）。
    pub should: Vec<String>,
    /// NOT：排除包含这些 term 的文档。
    pub must_not: Vec<String>,
}

/// 单个 term 的倒排扫描结果：doc_id_hash → (tf, doc_len)。
struct TermPostings {
    df: u64,
    docs: HashMap<[u8; 8], (u32, u32)>,
}

impl FtsEngine {
    /// 布尔查询搜索。
    ///
    /// - `must`：所有 term 必须出现（AND 语义），取文档交集
    /// - `should`：贡献额外 BM25 分数（OR 语义）
    /// - `must_not`：排除包含这些 term 的文档
    /// - `must` 和 `should` 均为空时返回空结果
    pub fn search_bool(
        &self,
        name: &str,
        query: &BoolQuery,
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

        // 分词
        let must_tokens = tokenize_terms(&query.must);
        let should_tokens = tokenize_terms(&query.should);
        let not_tokens = tokenize_terms(&query.must_not);

        if must_tokens.is_empty() && should_tokens.is_empty() {
            return Ok(vec![]);
        }

        // 收集 must terms 的倒排数据（按 df 升序，最稀有的先处理）
        let mut must_postings = collect_postings(&must_tokens, &inv_ks, &stat_ks)?;
        must_postings.sort_by_key(|p| p.df);

        // 计算 must 交集
        let must_set: Option<HashSet<[u8; 8]>> = if must_postings.is_empty() {
            None
        } else {
            let mut iter = must_postings.iter();
            let first = iter.next().unwrap();
            let mut set: HashSet<[u8; 8]> = first.docs.keys().copied().collect();
            for p in iter {
                set.retain(|id| p.docs.contains_key(id));
                if set.is_empty() {
                    return Ok(vec![]);
                }
            }
            Some(set)
        };

        // 收集 must_not 文档集合
        let not_set: HashSet<[u8; 8]> = if not_tokens.is_empty() {
            HashSet::new()
        } else {
            let not_postings = collect_postings(&not_tokens, &inv_ks, &stat_ks)?;
            not_postings
                .iter()
                .flat_map(|p| p.docs.keys().copied())
                .collect()
        };

        // 计算 BM25 分数（预分配容量：must 交集大小或 should 估计）
        let cap = must_set.as_ref().map(|s| s.len()).unwrap_or(64);
        let mut scores: HashMap<[u8; 8], f64> = HashMap::with_capacity(cap);

        // must terms 贡献分数
        for p in &must_postings {
            let idf_val = bm25::idf(doc_count, p.df);
            for (doc_hash, &(tf, dl)) in &p.docs {
                if let Some(ref ms) = must_set {
                    if !ms.contains(doc_hash) {
                        continue;
                    }
                }
                if not_set.contains(doc_hash) {
                    continue;
                }
                let s = bm25::term_score(tf, dl, avgdl, idf_val);
                *scores.entry(*doc_hash).or_insert(0.0) += s;
            }
        }

        // should terms 贡献分数
        let should_postings = collect_postings(&should_tokens, &inv_ks, &stat_ks)?;
        for p in &should_postings {
            let idf_val = bm25::idf(doc_count, p.df);
            for (doc_hash, &(tf, dl)) in &p.docs {
                if not_set.contains(doc_hash) {
                    continue;
                }
                // must 非空时，should 只对 must 交集内的文档加分
                if let Some(ref ms) = must_set {
                    if !ms.contains(doc_hash) {
                        continue;
                    }
                }
                let s = bm25::term_score(tf, dl, avgdl, idf_val);
                *scores.entry(*doc_hash).or_insert(0.0) += s;
            }
        }

        // 排序取 Top-N
        let mut scored: Vec<([u8; 8], f64)> = scores.into_iter().collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        // 反查文档
        let target_hashes: HashMap<[u8; 8], f64> = scored.into_iter().collect();
        let all_query_tokens: Vec<String> = must_tokens
            .iter()
            .chain(should_tokens.iter())
            .cloned()
            .collect();
        let mut results = Vec::with_capacity(target_hashes.len());
        doc_ks.for_each_kv_prefix(b"", |key, val| {
            let doc_id = String::from_utf8_lossy(key).to_string();
            let dh = hash_bytes(&doc_id);
            if let Some(&score) = target_hashes.get(&dh) {
                if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                    let highlights = highlight_fields(&fields, &all_query_tokens);
                    results.push(SearchHit {
                        doc_id,
                        score,
                        fields,
                        highlights,
                    });
                }
            }
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

/// 对输入字符串列表分词并去重。
fn tokenize_terms(terms: &[String]) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();
    for t in terms {
        for tok in tokenizer::tokenize(t, Analyzer::Standard) {
            if seen.insert(tok.clone()) {
                tokens.push(tok);
            }
        }
    }
    tokens
}

/// 收集一组 term 的倒排索引数据。
fn collect_postings(
    tokens: &[String],
    inv_ks: &crate::storage::Keyspace,
    stat_ks: &crate::storage::Keyspace,
) -> Result<Vec<TermPostings>, Error> {
    let mut result = Vec::with_capacity(tokens.len());
    for term in tokens {
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
        let mut docs = HashMap::new();
        if df > 0 {
            let prefix = inv_prefix(term);
            inv_ks.for_each_kv_prefix(&prefix, |key, val| {
                if key.len() == 16 {
                    if let Some((tf, dl, _positions)) = decode_inv_entry(val) {
                        let mut dh = [0u8; 8];
                        dh.copy_from_slice(&key[8..16]);
                        docs.insert(dh, (tf, dl));
                    }
                }
                true
            })?;
        }
        result.push(TermPostings { df, docs });
    }
    Ok(result)
}
