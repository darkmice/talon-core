/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Fuzzy 搜索：Levenshtein 编辑距离 + 模糊 term 匹配。
//!
//! 支持在全文搜索中对拼写错误的查询进行容错匹配。

use crate::error::Error;
use crate::storage::{Keyspace, Store};
use std::collections::{BTreeMap, HashMap};

/// 计算两个字符串之间的 Levenshtein 编辑距离。
/// 使用单行 DP 优化空间复杂度为 O(min(m, n))。
pub(super) fn levenshtein(a: &str, b: &str) -> u32 {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();
    if m == 0 {
        return n as u32;
    }
    if n == 0 {
        return m as u32;
    }
    // 确保 b 是较短的串（空间优化）
    if m < n {
        return levenshtein(b, a);
    }
    let mut prev: Vec<u32> = (0..=n as u32).collect();
    let mut curr = vec![0u32; n + 1];
    for (i, &ac) in a_chars.iter().enumerate() {
        curr[0] = (i + 1) as u32;
        for (j, &bc) in b_chars.iter().enumerate() {
            let cost = if ac == bc { 0 } else { 1 };
            curr[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(curr[j] + 1);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

/// term 注册表 key 前缀。
pub(super) const TERM_PREFIX: &[u8] = b"t:";

/// 构造 term 注册表 key：`t:{char_len:u16be}:{term_bytes}`。
/// 长度分桶：fuzzy 搜索时只扫描相近长度的桶，避免全量扫描。
pub(super) fn term_reg_key(term: &str) -> Vec<u8> {
    let char_len = term.chars().count().min(u16::MAX as usize) as u16;
    let mut k = Vec::with_capacity(4 + term.len());
    k.extend_from_slice(TERM_PREFIX);
    k.extend_from_slice(&char_len.to_be_bytes());
    k.push(b':');
    k.extend_from_slice(term.as_bytes());
    k
}

/// 构造指定字符长度的 term 桶前缀：`t:{char_len:u16be}:`。
fn term_bucket_prefix(char_len: u16) -> Vec<u8> {
    let mut k = Vec::with_capacity(5);
    k.extend_from_slice(TERM_PREFIX);
    k.extend_from_slice(&char_len.to_be_bytes());
    k.push(b':');
    k
}

/// 从 term 注册表 key 中提取 term 文本。
/// key 格式：`t:{u16be}:{term_bytes}`。
pub(super) fn extract_term(key: &[u8]) -> Option<&str> {
    // 前缀 "t:" (2字节) + len (2字节) + ":" (1字节) = 5 字节
    if key.len() > 5 && key.starts_with(TERM_PREFIX) {
        std::str::from_utf8(&key[5..]).ok()
    } else {
        None
    }
}

/// Fuzzy 搜索核心实现：对查询 token 做模糊匹配后执行 BM25 搜索。
pub(super) fn search_fuzzy_impl(
    _store: &Store,
    inv_ks: &Keyspace,
    doc_ks: &Keyspace,
    stat_ks: &Keyspace,
    query: &str,
    max_dist: u32,
    limit: usize,
) -> Result<Vec<super::SearchHit>, Error> {
    let doc_count = super::read_u64(stat_ks, super::META_DOC_COUNT);
    let total_len = super::read_u64(stat_ks, super::META_TOTAL_LEN);
    if doc_count == 0 {
        return Ok(vec![]);
    }
    let avgdl = total_len as f64 / doc_count as f64;
    let tokens = super::tokenizer::tokenize(query, super::Analyzer::Standard);
    if tokens.is_empty() {
        return Ok(vec![]);
    }
    // 长度分桶优化：只扫描 [token_len - max_dist, token_len + max_dist] 范围的桶
    let mut expanded_terms: Vec<String> = Vec::new();
    for token in &tokens {
        let token_len = token.chars().count();
        let lo = token_len.saturating_sub(max_dist as usize);
        let hi = token_len
            .saturating_add(max_dist as usize)
            .min(u16::MAX as usize);
        for bucket_len in lo..=hi {
            let prefix = term_bucket_prefix(bucket_len as u16);
            stat_ks.for_each_kv_prefix(&prefix, |key, _| {
                if let Some(term) = extract_term(key) {
                    if levenshtein(token, term) <= max_dist {
                        expanded_terms.push(term.to_string());
                    }
                }
                true
            })?;
        }
    }
    expanded_terms.sort();
    expanded_terms.dedup();
    if expanded_terms.is_empty() {
        return Ok(vec![]);
    }
    // 用扩展后的 term 集合执行 BM25 搜索
    let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
    for term in &expanded_terms {
        let dk = super::df_key(term);
        let df = stat_ks
            .get(&dk)?
            .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
            .unwrap_or(0);
        if df == 0 {
            continue;
        }
        let idf_val = super::bm25::idf(doc_count, df);
        let prefix = super::inv_prefix(term);
        inv_ks.for_each_kv_prefix(&prefix, |key, val| {
            if key.len() != 16 {
                return true;
            }
            if let Some((tf, dl, _positions)) = super::decode_inv_entry(val) {
                let doc_id_hash = key[8..16].to_vec();
                let s = super::bm25::term_score(tf, dl, avgdl, idf_val);
                *scores.entry(doc_id_hash).or_insert(0.0) += s;
            }
            true
        })?;
    }
    // 排序取 Top-N
    let mut scored: Vec<(Vec<u8>, f64)> = scores.into_iter().collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    let mut results = Vec::with_capacity(scored.len());
    let target_hashes: HashMap<Vec<u8>, f64> = scored.into_iter().collect();
    doc_ks.for_each_kv_prefix(b"", |key, val| {
        let doc_id = String::from_utf8_lossy(key).to_string();
        let dh = super::hash_bytes(&doc_id);
        if let Some(&score) = target_hashes.get(dh.as_slice()) {
            if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                results.push(super::SearchHit {
                    doc_id,
                    score,
                    fields,
                    highlights: BTreeMap::new(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_identical() {
        assert_eq!(levenshtein("hello", "hello"), 0);
    }

    #[test]
    fn levenshtein_one_edit() {
        assert_eq!(levenshtein("hello", "hallo"), 1);
        assert_eq!(levenshtein("hello", "hell"), 1);
        assert_eq!(levenshtein("hello", "helloo"), 1);
    }

    #[test]
    fn levenshtein_two_edits() {
        assert_eq!(levenshtein("machine", "machne"), 1);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
    }

    #[test]
    fn levenshtein_empty() {
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("", ""), 0);
    }

    #[test]
    fn levenshtein_chinese() {
        assert_eq!(levenshtein("机器学习", "机器"), 2);
        assert_eq!(levenshtein("人工智能", "人工智慧"), 1);
    }

    #[test]
    fn term_reg_key_roundtrip() {
        let key = term_reg_key("hello");
        assert_eq!(extract_term(&key), Some("hello"));
    }
}
