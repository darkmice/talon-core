/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 精确匹配搜索：不分词，直接查倒排索引。
//!
//! M134：对标 Elasticsearch `term` / `terms` 查询。
//! 用于 RAG 管道中按 doc_id/category 精确过滤、Agent 工具缓存精确查找等场景。

use std::collections::BTreeMap;

use super::bm25;
use super::{
    decode_inv_entry, df_key, doc_ks_name, hash_bytes, inv_ks_name, meta_ks_name, read_u64,
    FtsEngine, SearchHit, META_DOC_COUNT, META_TOTAL_LEN,
};
use crate::error::Error;

impl FtsEngine {
    /// 精确匹配搜索：不分词，直接在倒排索引中查找原始 term。
    ///
    /// 对标 Elasticsearch `term` 查询。输入 term 仅做小写化处理，
    /// 不经过分词器拆分，适用于 keyword 类字段的精确过滤。
    ///
    /// # 参数
    /// - `name`: 索引名称
    /// - `field`: 字段名（当前实现为全字段匹配，field 用于结果过滤）
    /// - `term`: 精确匹配的词（仅小写化，不分词）
    /// - `limit`: 最大返回数量
    ///
    /// # 示例
    /// ```ignore
    /// let hits = fts.search_term("docs", "category", "programming", 10)?;
    /// ```
    pub fn search_term(
        &self,
        name: &str,
        field: &str,
        term: &str,
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

        // 仅小写化，不分词
        let normalized = term.to_lowercase();

        // 查 df
        let dk = df_key(&normalized);
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
            return Ok(vec![]);
        }

        // 扫描倒排索引收集文档
        let idf_val = bm25::idf(doc_count, df);
        let prefix = super::inv_prefix(&normalized);
        let mut scored: Vec<([u8; 8], f64)> = Vec::new();
        inv_ks.for_each_kv_prefix(&prefix, |key, val| {
            if key.len() == 16 {
                if let Some((tf, dl, _)) = decode_inv_entry(val) {
                    let mut dh = [0u8; 8];
                    dh.copy_from_slice(&key[8..16]);
                    let s = bm25::term_score(tf, dl, avgdl, idf_val);
                    scored.push((dh, s));
                }
            }
            true
        })?;

        // 按分数排序取 Top-N
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        // 反查文档，按 field 过滤精确匹配
        let target: std::collections::HashMap<[u8; 8], f64> = scored.into_iter().collect();
        let mut results = Vec::with_capacity(target.len());
        let field_owned = field.to_string();
        let term_lower = normalized.clone();
        doc_ks.for_each_kv_prefix(b"", |key, val| {
            let doc_id = String::from_utf8_lossy(key).to_string();
            let dh = hash_bytes(&doc_id);
            if let Some(&score) = target.get(&dh) {
                if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                    // field 过滤：检查指定字段是否包含该 term（小写比较）
                    let field_match = if field_owned.is_empty() {
                        true
                    } else if let Some(fv) = fields.get(&field_owned) {
                        fv.to_lowercase().contains(&term_lower)
                    } else {
                        false
                    };
                    if field_match {
                        let mut highlights = BTreeMap::new();
                        if !field_owned.is_empty() {
                            if let Some(fv) = fields.get(&field_owned) {
                                highlights.insert(
                                    field_owned.clone(),
                                    fv.replace(&term_lower, &format!("<em>{}</em>", term_lower)),
                                );
                            }
                        }
                        results.push(SearchHit {
                            doc_id,
                            score,
                            fields,
                            highlights,
                        });
                    }
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

    /// 多值精确匹配搜索：对标 Elasticsearch `terms` 查询。
    ///
    /// 匹配任一 term 即命中，分数为所有匹配 term 的 BM25 分数之和。
    ///
    /// # 参数
    /// - `name`: 索引名称
    /// - `field`: 字段名（空字符串表示全字段）
    /// - `terms`: 精确匹配的词列表
    /// - `limit`: 最大返回数量
    pub fn search_terms(
        &self,
        name: &str,
        field: &str,
        terms: &[&str],
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

        // 去重并小写化
        let mut seen = std::collections::HashSet::new();
        let unique_terms: Vec<String> = terms
            .iter()
            .map(|t| t.to_lowercase())
            .filter(|t| seen.insert(t.clone()))
            .collect();

        let mut scores: std::collections::HashMap<[u8; 8], f64> = std::collections::HashMap::new();
        let mut matched_terms: Vec<String> = Vec::new();

        for normalized in &unique_terms {
            let dk = df_key(&normalized);
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
            matched_terms.push(normalized.clone());
            let idf_val = bm25::idf(doc_count, df);
            let prefix = super::inv_prefix(&normalized);
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

        let target: std::collections::HashMap<[u8; 8], f64> = scored.into_iter().collect();
        let mut results = Vec::with_capacity(target.len());
        let field_owned = field.to_string();
        doc_ks.for_each_kv_prefix(b"", |key, val| {
            let doc_id = String::from_utf8_lossy(key).to_string();
            let dh = hash_bytes(&doc_id);
            if let Some(&score) = target.get(&dh) {
                if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                    let field_match = if field_owned.is_empty() {
                        true
                    } else if let Some(fv) = fields.get(&field_owned) {
                        let fv_lower = fv.to_lowercase();
                        matched_terms.iter().any(|mt| fv_lower.contains(mt))
                    } else {
                        false
                    };
                    if field_match {
                        results.push(SearchHit {
                            doc_id,
                            score,
                            fields,
                            highlights: BTreeMap::new(),
                        });
                    }
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
