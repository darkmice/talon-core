/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 全文搜索 HTTP 路由处理函数。

use std::net::TcpStream;

use crate::error::Error;
use crate::fts::{FtsConfig, FtsDoc};
use crate::Talon;

use super::handlers::{parse_request, write_response};
use super::protocol::Response;

/// FTS 路由：create_index / drop_index / index / index_batch / delete / get / search / hybrid_search。
pub(super) fn handle_fts(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let name = req
        .params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let resp = match req.action.as_str() {
        "create_index" => {
            let config = FtsConfig::default();
            match db.fts()?.create_index(name, &config) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "drop_index" => match db.fts()?.drop_index(name) {
            Ok(()) => Response::ok_empty(),
            Err(e) => Response::err(e.to_string()),
        },
        "index" => {
            let doc_id = req
                .params
                .get("doc_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let fields = parse_fields(&req.params);
            let doc = FtsDoc {
                doc_id: doc_id.to_string(),
                fields,
            };
            match db.fts()?.index_doc(name, &doc) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "index_batch" => {
            let docs_raw = req
                .params
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
            match db.fts()?.index_doc_batch(name, &docs) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "delete" => {
            let doc_id = req
                .params
                .get("doc_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match db.fts()?.delete_doc(name, doc_id) {
                Ok(deleted) => Response::ok(serde_json::json!({"deleted": deleted})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "get" => {
            let doc_id = req
                .params
                .get("doc_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match db.fts_read()?.get_doc(name, doc_id) {
                Ok(Some(fields)) => Response::ok(serde_json::json!({"fields": fields})),
                Ok(None) => Response::ok(serde_json::json!(null)),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "search" => {
            let query = req
                .params
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let limit = req
                .params
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db.fts_read()?.search(name, query, limit) {
                Ok(hits) => {
                    let results: Vec<serde_json::Value> = hits
                        .iter()
                        .map(|h| {
                            serde_json::json!({
                                "doc_id": h.doc_id,
                                "score": h.score,
                                "fields": h.fields,
                            })
                        })
                        .collect();
                    Response::ok(serde_json::json!({"hits": results}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "hybrid_search" => {
            let query_text = req
                .params
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let vec_index = req
                .params
                .get("vec_index")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let query_vec: Vec<f32> = req
                .params
                .get("vector")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_f64().map(|f| f as f32))
                        .collect()
                })
                .unwrap_or_default();
            let metric = req
                .params
                .get("metric")
                .and_then(|v| v.as_str())
                .unwrap_or("cosine");
            let limit = req
                .params
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10)
                .max(1) as usize;
            let fts_weight = req
                .params
                .get("fts_weight")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            let vec_weight = req
                .params
                .get("vec_weight")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            let num_candidates = req
                .params
                .get("num_candidates")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            let pre_filter_owned: Vec<(String, String)> = req
                .params
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
                    let results: Vec<serde_json::Value> = hits
                        .iter()
                        .map(|h| {
                            serde_json::json!({
                                "doc_id": h.doc_id,
                                "rrf_score": h.rrf_score,
                                "bm25_score": h.bm25_score,
                                "vector_dist": h.vector_dist,
                                "fields": h.fields,
                            })
                        })
                        .collect();
                    Response::ok(serde_json::json!({"hits": results}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "add_alias" => {
            let alias = req
                .params
                .get("alias")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let index = req
                .params
                .get("index")
                .and_then(|v| v.as_str())
                .unwrap_or(name);
            match db.fts_read()?.add_alias(alias, index) {
                Ok(()) => Response::ok(serde_json::json!({"ok": true})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "remove_alias" => {
            let alias = req
                .params
                .get("alias")
                .and_then(|v| v.as_str())
                .unwrap_or(name);
            match db.fts_read()?.remove_alias(alias) {
                Ok(()) => Response::ok(serde_json::json!({"ok": true})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "reindex" => match db.fts_read()?.reindex(name) {
            Ok(count) => Response::ok(serde_json::json!({"reindexed": count})),
            Err(e) => Response::err(e.to_string()),
        },
        "close_index" => match db.fts_read()?.close_index(name) {
            Ok(()) => Response::ok(serde_json::json!({"ok": true})),
            Err(e) => Response::err(e.to_string()),
        },
        "open_index" => match db.fts_read()?.open_index(name) {
            Ok(()) => Response::ok(serde_json::json!({"ok": true})),
            Err(e) => Response::err(e.to_string()),
        },
        "get_mapping" => match db.fts_read()?.get_mapping(name) {
            Ok(m) => Response::ok(serde_json::json!({
                "name": m.name,
                "analyzer": m.analyzer,
                "doc_count": m.doc_count,
                "fields": m.fields,
            })),
            Err(e) => Response::err(e.to_string()),
        },
        "list_indexes" => match db.fts_read()?.list_indexes() {
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
                Response::ok(serde_json::json!({"indexes": arr}))
            }
            Err(e) => Response::err(e.to_string()),
        },
        _ => Response::err(format!("未知 FTS 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}

/// 从 params 中解析 fields 字段。
fn parse_fields(params: &serde_json::Value) -> std::collections::BTreeMap<String, String> {
    params
        .get("fields")
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default()
}
