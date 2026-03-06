/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 全文搜索 FFI 执行函数：create_index / index / search / fuzzy / hybrid 等。
//!
//! 从 ffi_exec/mod.rs 拆分，保持单文件行数限制。

use crate::fts::{FtsConfig, FtsDoc};
use crate::Talon;

use super::{err_json, ok_json, str_p, u64_p};

/// FTS 模块入口。
pub(crate) fn exec_fts(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    let name = str_p(params, "name");
    match action {
        "create_index" => match db.fts() {
            Ok(f) => match f.create_index(name, &FtsConfig::default()) {
                Ok(()) => ok_json(serde_json::json!({})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "drop_index" => match db.fts() {
            Ok(f) => match f.drop_index(name) {
                Ok(()) => ok_json(serde_json::json!({})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "index" => {
            let doc_id = str_p(params, "doc_id");
            let fields = parse_fields(params);
            let doc = FtsDoc {
                doc_id: doc_id.to_string(),
                fields,
            };
            match db.fts() {
                Ok(f) => match f.index_doc(name, &doc) {
                    Ok(()) => ok_json(serde_json::json!({})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "index_batch" => {
            let docs_raw = params
                .get("docs")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let docs: Vec<FtsDoc> = docs_raw
                .iter()
                .filter_map(|d| {
                    let doc_id = d.get("doc_id")?.as_str()?.to_string();
                    let fields = d
                        .get("fields")
                        .and_then(|f| serde_json::from_value(f.clone()).ok())
                        .unwrap_or_default();
                    Some(FtsDoc { doc_id, fields })
                })
                .collect();
            match db.fts() {
                Ok(f) => match f.index_doc_batch(name, &docs) {
                    Ok(()) => ok_json(serde_json::json!({"count": docs.len()})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "delete" => {
            let doc_id = str_p(params, "doc_id");
            match db.fts() {
                Ok(f) => match f.delete_doc(name, doc_id) {
                    Ok(deleted) => ok_json(serde_json::json!({"deleted": deleted})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "get" => {
            let doc_id = str_p(params, "doc_id");
            match db.fts_read() {
                Ok(f) => match f.get_doc(name, doc_id) {
                    Ok(Some(fields)) => ok_json(serde_json::json!({"fields": fields})),
                    Ok(None) => ok_json(serde_json::json!(null)),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "search" => {
            let query = str_p(params, "query");
            let limit = u64_p(params, "limit").max(1) as usize;
            match db.fts_read() {
                Ok(f) => match f.search(name, query, limit) {
                    Ok(hits) => {
                        let arr: Vec<serde_json::Value> = hits
                            .iter()
                            .map(|h| {
                                serde_json::json!({
                                    "doc_id": h.doc_id, "score": h.score, "fields": h.fields
                                })
                            })
                            .collect();
                        ok_json(serde_json::json!({"hits": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "search_fuzzy" => {
            let query = str_p(params, "query");
            let max_dist = params.get("max_dist").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
            let limit = u64_p(params, "limit").max(1) as usize;
            match db.fts_read() {
                Ok(f) => match f.search_fuzzy(name, query, max_dist, limit) {
                    Ok(hits) => {
                        let arr: Vec<serde_json::Value> = hits
                            .iter()
                            .map(|h| {
                                serde_json::json!({
                                    "doc_id": h.doc_id, "score": h.score, "fields": h.fields
                                })
                            })
                            .collect();
                        ok_json(serde_json::json!({"hits": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "hybrid_search" => {
            let query_text = str_p(params, "query");
            let vec_index = str_p(params, "vec_index");
            let query_vec: Vec<f32> = params
                .get("vector")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_f64().map(|f| f as f32))
                        .collect()
                })
                .unwrap_or_default();
            let metric = params
                .get("metric")
                .and_then(|v| v.as_str())
                .unwrap_or("cosine");
            let limit = u64_p(params, "limit").max(1) as usize;
            let fts_weight = params
                .get("fts_weight")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            let vec_weight = params
                .get("vec_weight")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            let num_candidates = params
                .get("num_candidates")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            let pre_filter_owned: Vec<(String, String)> = params
                .get("pre_filter")
                .and_then(|v| v.as_object())
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let pre_filter_refs: Vec<(&str, &str)> = pre_filter_owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let pre_filter = if pre_filter_refs.is_empty() {
                None
            } else {
                Some(pre_filter_refs)
            };
            let q = crate::fts::hybrid::HybridQuery {
                fts_index: name,
                vec_index,
                query_text,
                query_vec: &query_vec,
                metric,
                limit,
                fts_weight,
                vec_weight,
                num_candidates,
                pre_filter,
            };
            match crate::fts::hybrid::hybrid_search(db.store(), &q) {
                Ok(hits) => {
                    let arr: Vec<serde_json::Value> = hits
                        .iter()
                        .map(|h| {
                            serde_json::json!({
                                "doc_id": h.doc_id, "rrf_score": h.rrf_score,
                                "bm25_score": h.bm25_score, "vector_dist": h.vector_dist,
                                "fields": h.fields
                            })
                        })
                        .collect();
                    ok_json(serde_json::json!({"hits": arr}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "add_alias" => {
            let alias = params.get("alias").and_then(|v| v.as_str()).unwrap_or("");
            let index = params.get("index").and_then(|v| v.as_str()).unwrap_or(name);
            match db.fts_read() {
                Ok(f) => match f.add_alias(alias, index) {
                    Ok(()) => ok_json(serde_json::json!({"ok": true})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "remove_alias" => {
            let alias = params.get("alias").and_then(|v| v.as_str()).unwrap_or(name);
            match db.fts_read() {
                Ok(f) => match f.remove_alias(alias) {
                    Ok(()) => ok_json(serde_json::json!({"ok": true})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "reindex" => match db.fts_read() {
            Ok(f) => match f.reindex(name) {
                Ok(count) => ok_json(serde_json::json!({"reindexed": count})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "close_index" => match db.fts_read() {
            Ok(f) => match f.close_index(name) {
                Ok(()) => ok_json(serde_json::json!({"ok": true})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "open_index" => match db.fts_read() {
            Ok(f) => match f.open_index(name) {
                Ok(()) => ok_json(serde_json::json!({"ok": true})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "get_mapping" => match db.fts_read() {
            Ok(f) => match f.get_mapping(name) {
                Ok(m) => ok_json(serde_json::json!({
                    "name": m.name,
                    "analyzer": m.analyzer,
                    "doc_count": m.doc_count,
                    "fields": m.fields,
                })),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "list_indexes" => match db.fts_read() {
            Ok(f) => match f.list_indexes() {
                Ok(indexes) => {
                    let arr: Vec<serde_json::Value> = indexes
                        .iter()
                        .map(|i| {
                            serde_json::json!({
                                "name": i.name,
                                "doc_count": i.doc_count,
                            })
                        })
                        .collect();
                    ok_json(serde_json::json!({"indexes": arr}))
                }
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        _ => err_json(&format!("未知 fts action: {}", action)),
    }
}

fn parse_fields(params: &serde_json::Value) -> std::collections::BTreeMap<String, String> {
    params
        .get("fields")
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default()
}
