/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 聚合分析（对标 ES terms aggregation）。

use std::collections::{BTreeMap, HashMap};

use crate::error::Error;

use super::{doc_ks_name, FtsEngine};

/// 聚合桶：字段值 + 文档计数。
#[derive(Debug, Clone)]
pub struct AggBucketFts {
    /// 字段值。
    pub key: String,
    /// 匹配的文档数量。
    pub doc_count: u64,
}

impl FtsEngine {
    /// 按字段值聚合（terms aggregation），返回每个唯一值的文档计数。
    ///
    /// 对标 ES `{"aggs": {"categories": {"terms": {"field": "category"}}}}`。
    /// `top_n` 为 None 时返回所有桶，否则返回 doc_count 最高的前 N 个。
    ///
    /// AI 场景：RAG 索引分析（按 category 统计文档分布）、Agent 工具缓存统计。
    pub fn aggregate_terms(
        &self,
        name: &str,
        field: &str,
        top_n: Option<usize>,
    ) -> Result<Vec<AggBucketFts>, Error> {
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let mut counts: HashMap<String, u64> = HashMap::new();

        doc_ks.for_each_kv_prefix(b"", |_key, val| {
            if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                if let Some(v) = fields.get(field) {
                    *counts.entry(v.clone()).or_default() += 1;
                }
            }
            true
        })?;

        let mut buckets: Vec<AggBucketFts> = counts
            .into_iter()
            .map(|(key, doc_count)| AggBucketFts { key, doc_count })
            .collect();

        // 按 doc_count DESC 排序，相同计数按 key ASC
        buckets.sort_by(|a, b| b.doc_count.cmp(&a.doc_count).then(a.key.cmp(&b.key)));

        if let Some(n) = top_n {
            buckets.truncate(n);
        }

        Ok(buckets)
    }
}

/// 搜索建议项：词项 + 文档频率。
#[derive(Debug, Clone)]
pub struct SuggestItem {
    /// 匹配的词项。
    pub term: String,
    /// 包含该词项的文档数量。
    pub doc_freq: u64,
}

impl FtsEngine {
    /// 前缀搜索建议（对标 ES Completion Suggester）。
    ///
    /// 扫描索引中所有文档，分词后按前缀匹配，返回 doc_freq 最高的前 `top_n` 个词项。
    ///
    /// AI 场景：RAG 查询辅助（输入 "rus" 提示 "rust"）、Agent 工具名自动补全。
    pub fn suggest(
        &self,
        name: &str,
        prefix: &str,
        top_n: usize,
    ) -> Result<Vec<SuggestItem>, Error> {
        if prefix.is_empty() || top_n == 0 {
            return Ok(vec![]);
        }

        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let prefix_lower = prefix.to_lowercase();

        // term → 包含该 term 的文档 ID 集合（去重）
        let mut term_docs: HashMap<String, u64> = HashMap::new();

        doc_ks.for_each_kv_prefix(b"", |_key, val| {
            if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                // 对每个字段值分词，收集匹配前缀的 token
                let mut seen = std::collections::HashSet::new();
                for (_field_name, field_val) in &fields {
                    let tokens =
                        super::tokenizer::tokenize(field_val, super::tokenizer::Analyzer::Standard);
                    for token in tokens {
                        if token.starts_with(&prefix_lower) && seen.insert(token.clone()) {
                            *term_docs.entry(token).or_default() += 1;
                        }
                    }
                }
            }
            true
        })?;

        let mut items: Vec<SuggestItem> = term_docs
            .into_iter()
            .map(|(term, doc_freq)| SuggestItem { term, doc_freq })
            .collect();

        // 按 doc_freq DESC，相同频率按 term ASC
        items.sort_by(|a, b| b.doc_freq.cmp(&a.doc_freq).then(a.term.cmp(&b.term)));
        items.truncate(top_n);

        Ok(items)
    }
}

/// 搜索排序方式。
#[derive(Debug, Clone)]
pub enum FtsSortBy {
    /// 按 BM25 分数排序（默认行为）。
    Score,
    /// 按字段值排序（数值优先比较，回退字符串比较）。
    Field {
        /// 排序字段名。
        name: String,
        /// true = 降序。
        desc: bool,
    },
}

impl FtsEngine {
    /// 带自定义排序的全文搜索（对标 ES `sort` 参数）。
    ///
    /// 先执行 BM25 搜索获取所有匹配文档，再按指定方式重排序后截断。
    /// `FtsSortBy::Score` 等同于普通 `search()`。
    ///
    /// AI 场景：RAG 检索按时间排序获取最新文档。
    pub fn search_sorted(
        &self,
        name: &str,
        query: &str,
        sort: &FtsSortBy,
        limit: usize,
    ) -> Result<Vec<super::SearchHit>, Error> {
        // 先获取所有匹配结果（内部不限制数量）
        let mut hits = self.search(name, query, usize::MAX)?;

        match sort {
            FtsSortBy::Score => {
                // 已按 score DESC 排序，直接截断
            }
            FtsSortBy::Field { name: field, desc } => {
                hits.sort_by(|a, b| {
                    let va = a
                        .fields
                        .get(field.as_str())
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    let vb = b
                        .fields
                        .get(field.as_str())
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    // 尝试数值比较
                    let cmp = match (va.parse::<f64>(), vb.parse::<f64>()) {
                        (Ok(fa), Ok(fb)) => {
                            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
                        }
                        _ => va.cmp(vb),
                    };
                    if *desc {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                });
            }
        }

        hits.truncate(limit);
        Ok(hits)
    }
}
