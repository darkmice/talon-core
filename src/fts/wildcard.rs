/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 通配符搜索：支持 `*`（任意字符序列）和 `?`（单个字符）。
//!
//! M135：对标 Elasticsearch `wildcard` 查询。
//! 扫描 term 注册表，对每个已注册 term 做通配符匹配，匹配的 term 执行 BM25 搜索。

use std::collections::{BTreeMap, HashMap};

use super::bm25;
use super::fuzzy::{extract_term, TERM_PREFIX};
use super::{
    decode_inv_entry, df_key, doc_ks_name, hash_bytes, inv_ks_name, inv_prefix, meta_ks_name,
    read_u64, FtsEngine, SearchHit, META_DOC_COUNT, META_TOTAL_LEN,
};
use crate::error::Error;

/// 通配符匹配：`*` 匹配零或多个字符，`?` 匹配恰好一个字符。
///
/// 使用贪心双指针算法，时间复杂度 O(m × n) 最坏情况（连续 `*`），
/// 平均情况接近 O(m + n)。
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (plen, tlen) = (p.len(), t.len());
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star_pi = usize::MAX; // 上一个 * 在 pattern 中的位置
    let mut star_ti = 0usize; // 上一个 * 匹配到 text 的位置

    while ti < tlen {
        if pi < plen && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < plen && p[pi] == '*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            // 回溯：让 * 多匹配一个字符
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    // 消耗 pattern 尾部的 *
    while pi < plen && p[pi] == '*' {
        pi += 1;
    }
    pi == plen
}

impl FtsEngine {
    /// 通配符搜索：支持 `*`（任意字符序列）和 `?`（单个字符）。
    ///
    /// 对标 Elasticsearch `wildcard` 查询。扫描 term 注册表中所有已注册 term，
    /// 对匹配通配符模式的 term 执行 BM25 搜索。
    ///
    /// # 参数
    /// - `name`: 索引名称
    /// - `pattern`: 通配符模式（`*` = 任意序列，`?` = 单字符）
    /// - `limit`: 最大返回数量
    ///
    /// # 示例
    /// ```ignore
    /// let hits = fts.search_wildcard("docs", "rust*", 10)?;
    /// let hits = fts.search_wildcard("docs", "te?t", 10)?;
    /// ```
    pub fn search_wildcard(
        &self,
        name: &str,
        pattern: &str,
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

        let pat_lower = pattern.to_lowercase();
        if pat_lower.is_empty() {
            return Ok(vec![]);
        }

        // 扫描 term 注册表，收集匹配的 term
        let mut matched_terms: Vec<String> = Vec::new();
        stat_ks.for_each_kv_prefix(TERM_PREFIX, |key, _| {
            if let Some(term) = extract_term(key) {
                if wildcard_match(&pat_lower, term) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_star_basic() {
        assert!(wildcard_match("rust*", "rust"));
        assert!(wildcard_match("rust*", "rusty"));
        assert!(wildcard_match("rust*", "rustacean"));
        assert!(!wildcard_match("rust*", "trust"));
    }

    #[test]
    fn wildcard_question_mark() {
        assert!(wildcard_match("te?t", "test"));
        assert!(wildcard_match("te?t", "text"));
        assert!(!wildcard_match("te?t", "tet"));
        assert!(!wildcard_match("te?t", "teest"));
    }

    #[test]
    fn wildcard_combined() {
        assert!(wildcard_match("*prog*", "programming"));
        assert!(wildcard_match("*prog*", "reprogram"));
        assert!(wildcard_match("r?st*", "rust"));
        assert!(wildcard_match("r?st*", "rusty"));
        assert!(!wildcard_match("r?st*", "roost"));
    }

    #[test]
    fn wildcard_edge_cases() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("**", "test"));
        assert!(!wildcard_match("?", ""));
        assert!(wildcard_match("?", "a"));
        assert!(wildcard_match("", ""));
        assert!(!wildcard_match("", "a"));
    }

    #[test]
    fn wildcard_unicode() {
        assert!(wildcard_match("机器*", "机器学习"));
        assert!(wildcard_match("?器学习", "机器学习"));
        assert!(!wildcard_match("?学习", "机器学习"));
    }
}
