/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 正则表达式搜索：对标 Elasticsearch `regexp` 查询。
//!
//! M136：扫描 term 注册表，对每个已注册 term 做正则匹配，
//! 匹配的 term 执行 BM25 搜索。使用 `regex` crate（O(n) 保证，无 ReDoS 风险）。

use std::collections::{BTreeMap, HashMap};

use regex::Regex;

use super::bm25;
use super::fuzzy::{extract_term, TERM_PREFIX};
use super::{
    decode_inv_entry, df_key, doc_ks_name, hash_bytes, inv_ks_name, inv_prefix, meta_ks_name,
    read_u64, FtsEngine, SearchHit, META_DOC_COUNT, META_TOTAL_LEN,
};
use crate::error::Error;

impl FtsEngine {
    /// 正则表达式搜索：对标 Elasticsearch `regexp` 查询。
    ///
    /// 扫描 term 注册表中所有已注册 term，对匹配正则模式的 term
    /// 执行 BM25 搜索。正则自动锚定为全匹配（`^pattern$`）。
    ///
    /// 使用 `regex` crate，保证 O(n) 时间复杂度，无 ReDoS 风险。
    ///
    /// # 参数
    /// - `name`: 索引名称
    /// - `pattern`: 正则表达式（自动锚定为全匹配）
    /// - `limit`: 最大返回数量
    ///
    /// # 错误
    /// - `Error::FullTextSearch`: 正则表达式编译失败
    ///
    /// # 示例
    /// ```ignore
    /// let hits = fts.search_regexp("docs", "rust.*", 10)?;
    /// let hits = fts.search_regexp("docs", "v\\d+\\.\\d+", 10)?;
    /// ```
    pub fn search_regexp(
        &self,
        name: &str,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, Error> {
        if pattern.is_empty() {
            return Ok(vec![]);
        }

        // 编译正则（自动锚定为全匹配）
        let anchored = if pattern.starts_with('^') && pattern.ends_with('$') {
            pattern.to_string()
        } else if pattern.starts_with('^') {
            format!("{}$", pattern)
        } else if pattern.ends_with('$') {
            format!("^{}", pattern)
        } else {
            format!("^{}$", pattern)
        };
        let re = Regex::new(&anchored)
            .map_err(|e| Error::FullTextSearch(format!("invalid regexp: {}", e)))?;

        let inv_ks = self.store.open_keyspace(&inv_ks_name(name))?;
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        let doc_count = read_u64(&stat_ks, META_DOC_COUNT);
        let total_len = read_u64(&stat_ks, META_TOTAL_LEN);
        if doc_count == 0 {
            return Ok(vec![]);
        }
        let avgdl = total_len as f64 / doc_count as f64;

        // 扫描 term 注册表，收集匹配的 term
        let mut matched_terms: Vec<String> = Vec::new();
        stat_ks.for_each_kv_prefix(TERM_PREFIX, |key, _| {
            if let Some(term) = extract_term(key) {
                if re.is_match(term) {
                    matched_terms.push(term.to_string());
                }
            }
            true
        })?;

        if matched_terms.is_empty() {
            return Ok(vec![]);
        }
        matched_terms.sort();
        matched_terms.dedup();

        // 用匹配的 term 集合执行 BM25 搜索
        let mut scores: HashMap<[u8; 8], f64> = HashMap::new();
        for term in &matched_terms {
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
            let prefix = inv_prefix(term);
            inv_ks.for_each_kv_prefix(&prefix, |key, val| {
                if key.len() == 16 {
                    if let Some((tf, dl, _)) = decode_inv_entry(val) {
                        let mut dh = [0u8; 8];
                        dh.copy_from_slice(&key[8..16]);
                        let s = bm25::term_score(tf, dl, avgdl, idf_val);
                        *scores.entry(dh).or_insert(0.0) += s;
                    }
                }
                true
            })?;
        }

        if scores.is_empty() {
            return Ok(vec![]);
        }

        // 排序取 Top-N
        let mut scored: Vec<([u8; 8], f64)> = scores.into_iter().collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        // 反查文档
        let target: HashMap<[u8; 8], f64> = scored.into_iter().collect();
        let mut results = Vec::with_capacity(target.len());
        doc_ks.for_each_kv_prefix(b"", |key, val| {
            let doc_id = String::from_utf8_lossy(key).to_string();
            let dh = hash_bytes(&doc_id);
            if let Some(&score) = target.get(&dh) {
                if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                    results.push(SearchHit {
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
}
