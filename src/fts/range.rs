/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 范围查询：对标 Elasticsearch `range` 查询。
//!
//! M137：在文档字段值上做数值/字符串范围比较。
//! 不走倒排索引，直接扫描文档存储。

use std::collections::BTreeMap;

use super::{doc_ks_name, meta_ks_name, read_u64, FtsEngine, SearchHit, META_DOC_COUNT};
use crate::error::Error;

/// 范围查询条件。
///
/// 对标 Elasticsearch `range` 查询。支持 `gte`/`gt`/`lte`/`lt` 四种边界。
/// 先尝试数值比较（f64），失败则做字符串字典序比较。
#[derive(Debug, Clone, Default)]
pub struct RangeQuery {
    /// 目标字段名。
    pub field: String,
    /// 大于等于（inclusive lower bound）。
    pub gte: Option<String>,
    /// 大于（exclusive lower bound）。
    pub gt: Option<String>,
    /// 小于等于（inclusive upper bound）。
    pub lte: Option<String>,
    /// 小于（exclusive upper bound）。
    pub lt: Option<String>,
}

/// 比较两个值：先尝试 f64 数值比较，失败则字符串字典序。
/// 返回 Ordering。
fn compare_values(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(fa), Ok(fb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

/// 检查字段值是否满足范围条件。
fn matches_range(value: &str, query: &RangeQuery) -> bool {
    if let Some(ref gte) = query.gte {
        if compare_values(value, gte) == std::cmp::Ordering::Less {
            return false;
        }
    }
    if let Some(ref gt) = query.gt {
        if compare_values(value, gt) != std::cmp::Ordering::Greater {
            return false;
        }
    }
    if let Some(ref lte) = query.lte {
        if compare_values(value, lte) == std::cmp::Ordering::Greater {
            return false;
        }
    }
    if let Some(ref lt) = query.lt {
        if compare_values(value, lt) != std::cmp::Ordering::Less {
            return false;
        }
    }
    true
}

impl FtsEngine {
    /// 范围查询搜索：对标 Elasticsearch `range` 查询。
    ///
    /// 在文档的指定字段上做范围比较。先尝试数值比较（f64），
    /// 失败则做字符串字典序比较。匹配文档的分数固定为 1.0。
    ///
    /// # 参数
    /// - `name`: 索引名称
    /// - `query`: 范围查询条件（field + gte/gt/lte/lt）
    /// - `limit`: 最大返回数量
    ///
    /// # 示例
    /// ```ignore
    /// let hits = fts.search_range("docs", &RangeQuery {
    ///     field: "score".into(),
    ///     gte: Some("0.8".into()),
    ///     lte: Some("1.0".into()),
    ///     ..Default::default()
    /// }, 10)?;
    /// ```
    pub fn search_range(
        &self,
        name: &str,
        query: &RangeQuery,
        limit: usize,
    ) -> Result<Vec<SearchHit>, Error> {
        // 无边界条件 → 返回空
        if query.gte.is_none() && query.gt.is_none() && query.lte.is_none() && query.lt.is_none() {
            return Ok(vec![]);
        }
        if query.field.is_empty() {
            return Ok(vec![]);
        }

        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        let doc_count = read_u64(&stat_ks, META_DOC_COUNT);
        if doc_count == 0 {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        let field_name = &query.field;

        doc_ks.for_each_kv_prefix(b"", |key, val| {
            if results.len() >= limit {
                return false; // 提前终止
            }
            let doc_id = String::from_utf8_lossy(key).to_string();
            if let Ok(fields) = serde_json::from_slice::<BTreeMap<String, String>>(val) {
                if let Some(fv) = fields.get(field_name) {
                    if matches_range(fv, query) {
                        results.push(SearchHit {
                            doc_id,
                            score: 1.0,
                            fields,
                            highlights: BTreeMap::new(),
                        });
                    }
                }
            }
            true
        })?;

        results.truncate(limit);
        Ok(results)
    }
}
