/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph HTTP 路由处理函数：CRUD + 遍历 + 算法。
//!
//! 路由：`POST /api/graph`
//! action: create / add_vertex / get_vertex / update_vertex / delete_vertex /
//!         add_edge / get_edge / delete_edge / neighbors / vertices_by_label /
//!         edges_by_label / vertex_count / edge_count / out_edges / in_edges /
//!         bfs / shortest_path / weighted_shortest_path /
//!         degree_centrality / pagerank

use std::collections::BTreeMap;
use std::net::TcpStream;

use crate::error::Error;
use crate::graph::Direction;
use crate::Talon;

use super::handlers::{parse_request, write_response};
use super::protocol::Response;

/// Graph 路由入口。
pub(super) fn handle_graph(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let graph_name = req
        .params
        .get("graph")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let resp = match req.action.as_str() {
        "create" => match db.graph()?.create(graph_name) {
            Ok(()) => Response::ok_empty(),
            Err(e) => Response::err(e.to_string()),
        },
        "add_vertex" => {
            let label = req
                .params
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let props = parse_props(&req.params);
            match db.graph()?.add_vertex(graph_name, label, &props) {
                Ok(id) => Response::ok(serde_json::json!({"vertex_id": id})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "get_vertex" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.graph_read()?.get_vertex(graph_name, id) {
                Ok(Some(v)) => Response::ok(serde_json::json!({
                    "id": v.id, "label": v.label, "properties": v.properties
                })),
                Ok(None) => Response::ok(serde_json::json!(null)),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "update_vertex" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            let props = parse_props(&req.params);
            match db.graph()?.update_vertex(graph_name, id, &props) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "delete_vertex" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.graph()?.delete_vertex(graph_name, id) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "add_edge" => {
            let from = req.params.get("from").and_then(|v| v.as_u64()).unwrap_or(0);
            let to = req.params.get("to").and_then(|v| v.as_u64()).unwrap_or(0);
            let label = req
                .params
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let props = parse_props(&req.params);
            match db.graph()?.add_edge(graph_name, from, to, label, &props) {
                Ok(id) => Response::ok(serde_json::json!({"edge_id": id})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "get_edge" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.graph_read()?.get_edge(graph_name, id) {
                Ok(Some(e)) => Response::ok(serde_json::json!({
                    "id": e.id, "from": e.from, "to": e.to,
                    "label": e.label, "properties": e.properties
                })),
                Ok(None) => Response::ok(serde_json::json!(null)),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "delete_edge" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.graph()?.delete_edge(graph_name, id) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "neighbors" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            let dir = parse_direction(&req.params);
            match db.graph_read()?.neighbors(graph_name, id, dir) {
                Ok(ids) => Response::ok(serde_json::json!({"neighbors": ids})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "out_edges" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.graph_read()?.out_edges(graph_name, id) {
                Ok(edges) => Response::ok(serde_json::json!({"edges": edges_to_json(&edges)})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "in_edges" => {
            let id = req.params.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            match db.graph_read()?.in_edges(graph_name, id) {
                Ok(edges) => Response::ok(serde_json::json!({"edges": edges_to_json(&edges)})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "vertices_by_label" => {
            let label = req
                .params
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match db.graph_read()?.vertices_by_label(graph_name, label) {
                Ok(vs) => {
                    let arr: Vec<serde_json::Value> = vs
                        .iter()
                        .map(|v| {
                            serde_json::json!({
                                "id": v.id, "label": v.label, "properties": v.properties
                            })
                        })
                        .collect();
                    Response::ok(serde_json::json!({"vertices": arr}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "edges_by_label" => {
            let label = req
                .params
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match db.graph_read()?.edges_by_label(graph_name, label) {
                Ok(edges) => Response::ok(serde_json::json!({"edges": edges_to_json(&edges)})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "vertex_count" => match db.graph_read()?.vertex_count(graph_name) {
            Ok(n) => Response::ok(serde_json::json!({"count": n})),
            Err(e) => Response::err(e.to_string()),
        },
        "edge_count" => match db.graph_read()?.edge_count(graph_name) {
            Ok(n) => Response::ok(serde_json::json!({"count": n})),
            Err(e) => Response::err(e.to_string()),
        },
        // ── 遍历与算法 ──
        "bfs" => {
            let start = req
                .params
                .get("start")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let max_depth = req
                .params
                .get("max_depth")
                .and_then(|v| v.as_u64())
                .unwrap_or(3) as usize;
            let dir = parse_direction(&req.params);
            match db.graph_read()?.bfs(graph_name, start, max_depth, dir) {
                Ok(results) => {
                    let arr: Vec<serde_json::Value> = results
                        .iter()
                        .map(|(id, depth)| serde_json::json!({"id": id, "depth": depth}))
                        .collect();
                    Response::ok(serde_json::json!({"results": arr}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "shortest_path" => {
            let from = req.params.get("from").and_then(|v| v.as_u64()).unwrap_or(0);
            let to = req.params.get("to").and_then(|v| v.as_u64()).unwrap_or(0);
            let max_depth = req
                .params
                .get("max_depth")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db
                .graph_read()?
                .shortest_path(graph_name, from, to, max_depth)
            {
                Ok(Some(path)) => Response::ok(serde_json::json!({"path": path})),
                Ok(None) => Response::ok(serde_json::json!({"path": null})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "weighted_shortest_path" => {
            let from = req.params.get("from").and_then(|v| v.as_u64()).unwrap_or(0);
            let to = req.params.get("to").and_then(|v| v.as_u64()).unwrap_or(0);
            let max_depth = req
                .params
                .get("max_depth")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            let weight_key = req
                .params
                .get("weight_key")
                .and_then(|v| v.as_str())
                .unwrap_or("weight");
            match db
                .graph_read()?
                .weighted_shortest_path(graph_name, from, to, max_depth, weight_key)
            {
                Ok(Some((path, cost))) => {
                    Response::ok(serde_json::json!({"path": path, "cost": cost}))
                }
                Ok(None) => Response::ok(serde_json::json!({"path": null, "cost": null})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "degree_centrality" => {
            let limit = req
                .params
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db.graph_read()?.degree_centrality(graph_name, limit) {
                Ok(degrees) => {
                    let arr: Vec<serde_json::Value> = degrees
                        .iter()
                        .map(|(id, out_d, in_d)| {
                            serde_json::json!({
                                "id": id, "out_degree": out_d, "in_degree": in_d,
                                "total_degree": out_d + in_d
                            })
                        })
                        .collect();
                    Response::ok(serde_json::json!({"results": arr}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "pagerank" => {
            let damping = req
                .params
                .get("damping")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.85);
            let iterations = req
                .params
                .get("iterations")
                .and_then(|v| v.as_u64())
                .unwrap_or(20) as usize;
            let limit = req
                .params
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db
                .graph_read()?
                .pagerank(graph_name, damping, iterations, limit)
            {
                Ok(ranks) => {
                    let arr: Vec<serde_json::Value> = ranks
                        .iter()
                        .map(|(id, score)| serde_json::json!({"id": id, "score": score}))
                        .collect();
                    Response::ok(serde_json::json!({"results": arr}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        _ => Response::err(format!("未知 Graph 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}

/// 解析 properties 参数（JSON object → BTreeMap<String, String>）。
fn parse_props(params: &serde_json::Value) -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    if let Some(obj) = params.get("properties").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            let val = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => v.to_string(),
            };
            props.insert(k.clone(), val);
        }
    }
    props
}

/// 解析 direction 参数。
fn parse_direction(params: &serde_json::Value) -> Direction {
    match params
        .get("direction")
        .and_then(|v| v.as_str())
        .unwrap_or("out")
    {
        "in" => Direction::In,
        "both" => Direction::Both,
        _ => Direction::Out,
    }
}

/// 将 Edge 数组序列化为 JSON 数组。
fn edges_to_json(edges: &[crate::graph::Edge]) -> Vec<serde_json::Value> {
    edges
        .iter()
        .map(|e| {
            serde_json::json!({
                "id": e.id, "from": e.from, "to": e.to,
                "label": e.label, "properties": e.properties
            })
        })
        .collect()
}
