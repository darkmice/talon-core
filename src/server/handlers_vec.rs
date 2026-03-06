/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量引擎 HTTP 路由处理：insert/search/delete/count/batch_insert/batch_search。

use std::net::TcpStream;

use crate::error::Error;
use crate::Talon;

use super::handlers::{parse_request, write_response};
use super::protocol::Response;

/// 向量路由：insert/search/delete/count/batch_insert/batch_search。
pub(super) fn handle_vector(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let resp = match req.action.as_str() {
        "insert" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            let vec: Vec<f32> = req
                .params
                .get("vector")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            match db.vector(name).and_then(|ve| ve.insert(id, &vec)) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "search" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let vec: Vec<f32> = req
                .params
                .get("vector")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let k = req.params.get("k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
            let metric = req
                .params
                .get("metric")
                .and_then(|v| v.as_str())
                .unwrap_or("cosine");
            match db.vector(name).and_then(|ve| ve.search(&vec, k, metric)) {
                Ok(results) => {
                    let items: Vec<serde_json::Value> = results
                        .iter()
                        .map(|(id, dist)| serde_json::json!({"id": id, "distance": dist}))
                        .collect();
                    Response::ok(serde_json::json!({"results": items}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "delete" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.vector(name).and_then(|ve| ve.delete(id)) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "count" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            match db.vector(name).and_then(|ve| ve.count()) {
                Ok(n) => Response::ok(serde_json::json!({"count": n})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "batch_insert" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let items: Vec<(u64, Vec<f32>)> = req
                .params
                .get("items")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let refs: Vec<(u64, &[f32])> =
                items.iter().map(|(id, v)| (*id, v.as_slice())).collect();
            match db.vector(name).and_then(|ve| ve.insert_batch(&refs)) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "batch_search" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let queries: Vec<Vec<f32>> = req
                .params
                .get("queries")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let k = req.params.get("k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
            let metric = req
                .params
                .get("metric")
                .and_then(|v| v.as_str())
                .unwrap_or("cosine");
            let refs: Vec<&[f32]> = queries.iter().map(|q| q.as_slice()).collect();
            match db
                .vector(name)
                .and_then(|ve| ve.batch_search(&refs, k, metric))
            {
                Ok(results) => {
                    let items: Vec<Vec<serde_json::Value>> = results
                        .iter()
                        .map(|rs| {
                            rs.iter()
                                .map(|(id, d)| serde_json::json!({"id": id, "distance": d}))
                                .collect()
                        })
                        .collect();
                    Response::ok(serde_json::json!({"results": items}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "set_ef_search" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let ef = req
                .params
                .get("ef_search")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db.vector(name).and_then(|ve| ve.set_ef_search(ef)) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        _ => Response::err(format!("未知 Vector 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}
