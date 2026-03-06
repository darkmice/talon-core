/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 短语搜索：匹配 token 按顺序连续出现的文档。
//!
//! M78：对标 Elasticsearch match_phrase，基于倒排索引位置信息实现。

use std::collections::{BTreeMap, HashMap};

use super::bm25;
use super::tokenizer::{self, Analyzer};
use super::{
    decode_inv_entry, df_key, doc_ks_name, hash_bytes, highlight_fields, inv_ks_name, inv_prefix,
    meta_ks_name, read_u64, FtsEngine, SearchHit, META_DOC_COUNT, META_TOTAL_LEN,
};
use crate::error::Error;

/// 单个 term 在某文档中的倒排数据。
struct TermDocEntry {
    tf: u32,
    dl: u32,
    positions: Vec<u32>,
}

/// 单个 term 的全部倒排数据：doc_hash → entry。
struct TermPostings {
    df: u64,
    docs: HashMap<[u8; 8], TermDocEntry>,
}

impl FtsEngine {
    /// 短语搜索：匹配短语中所有 token 按顺序连续出现的文档。
    ///
    /// 对 `phrase` 分词后，要求所有 token 在文档中按顺序连续出现
    /// （position 差为 1）。使用 BM25 评分。
    ///
    /// 如果倒排索引无位置信息（旧格式数据），回退到普通 AND 搜索。
    pub fn search_phrase(
        &self,
        name: &str,
        phrase: &str,
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

        let tokens = tokenizer::tokenize(phrase, Analyzer::Standard);
        if tokens.is_empty() {
            return Ok(vec![]);
        }
        // 单 token 短语退化为普通搜索
        if tokens.len() == 1 {
            return self.search(name, phrase, limit);
        }

        // 收集所有 term 的倒排数据（含位置）
        let mut all_postings = Vec::with_capacity(tokens.len());
        for token in &tokens {
            all_postings.push(collect_term_postings(token, &inv_ks, &stat_ks)?);
        }

        // 按 df 最小的 term 开始做交集（但保持原始顺序用于位置匹配）
        // 先找交集文档集合
        let mut candidate_docs: Vec<[u8; 8]> = {
            // 找 df 最小的 term 作为起始集
            let min_idx = all_postings
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.df)
                .map(|(i, _)| i)
                .unwrap_or(0);
            let start_keys: Vec<[u8; 8]> = all_postings[min_idx].docs.keys().copied().collect();
            start_keys
                .into_iter()
                .filter(|dh| all_postings.iter().all(|p| p.docs.contains_key(dh)))
                .collect()
        };

        if candidate_docs.is_empty() {
            return Ok(vec![]);
        }

        // 对交集文档做短语位置匹配
        let mut scores: HashMap<[u8; 8], f64> = HashMap::with_capacity(candidate_docs.len());

        candidate_docs.retain(|dh| {
            // 检查所有 term 是否都有位置信息（旧格式兼容）
            let has_positions = all_postings.iter().all(|p| {
                p.docs
                    .get(dh)
                    .map(|e| !e.positions.is_empty())
                    .unwrap_or(false)
            });
            if !has_positions {
                // 无位置信息时回退到 AND 匹配（旧格式兼容）
                return true;
            }
            // 检查是否存在连续位置序列
            phrase_match(dh, &all_postings)
        });

        if candidate_docs.is_empty() {
            return Ok(vec![]);
        }

        // 计算 BM25 分数
        for dh in &candidate_docs {
            let mut score = 0.0;
            for p in &all_postings {
                if let Some(entry) = p.docs.get(dh) {
                    let idf_val = bm25::idf(doc_count, p.df);
                    score += bm25::term_score(entry.tf, entry.dl, avgdl, idf_val);
                }
            }
            scores.insert(*dh, score);
        }

        // 排序取 Top-N
        let mut scored: Vec<([u8; 8], f64)> = scores.into_iter().collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        // 反查文档
        let target_hashes: HashMap<[u8; 8], f64> = scored.into_iter().collect();
        let mut results = Vec::with_capacity(target_hashes.len());
        doc_ks.for_each_kv_prefix(b"", |key, val| {
            let doc_id = String::from_utf8_lossy(key).to_string();
            let dh = hash_bytes(&doc_id);
            if let Some(&score) = target_hashes.get(&dh) {
                if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                    let highlights = highlight_fields(&fields, &tokens);
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

/// 检查文档中是否存在短语的连续位置序列。
///
/// 算法：以第一个 term 的每个位置为起点，检查后续 term 是否在 pos+1, pos+2, ... 处出现。
/// 使用排序位置列表 + 二分查找，时间复杂度 O(P0 × (N-1) × log(Pi))。
fn phrase_match(doc_hash: &[u8; 8], postings: &[TermPostings]) -> bool {
    let first = match postings[0].docs.get(doc_hash) {
        Some(e) => e,
        None => return false,
    };
    // 对第一个 term 的每个位置尝试匹配
    'outer: for &start_pos in &first.positions {
        for (offset, p) in postings.iter().enumerate().skip(1) {
            let target_pos = match start_pos.checked_add(offset as u32) {
                Some(p) => p,
                None => continue 'outer,
            };
            let entry = match p.docs.get(doc_hash) {
                Some(e) => e,
                None => continue 'outer,
            };
            // 二分查找目标位置
            if entry.positions.binary_search(&target_pos).is_err() {
                continue 'outer;
            }
        }
        return true; // 找到完整短语匹配
    }
    false
}

/// 收集单个 term 的倒排数据（含位置信息）。
fn collect_term_postings(
    term: &str,
    inv_ks: &crate::storage::Keyspace,
    stat_ks: &crate::storage::Keyspace,
) -> Result<TermPostings, Error> {
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
                if let Some((tf, dl, positions)) = decode_inv_entry(val) {
                    let mut dh = [0u8; 8];
                    dh.copy_from_slice(&key[8..16]);
                    docs.insert(dh, TermDocEntry { tf, dl, positions });
                }
            }
            true
        })?;
    }
    Ok(TermPostings { df, docs })
}
