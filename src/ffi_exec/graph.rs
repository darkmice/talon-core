/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph FFI 执行函数：CRUD + 遍历 + 算法。
//!
//! 从 ffi_exec/mod.rs 拆分，保持单文件行数限制。

use std::collections::BTreeMap;

use crate::Talon;

use super::{err_json, ok_json, str_p, u64_p};

/// Graph 模块入口。
pub(crate) fn exec_graph(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    let graph_name = str_p(params, "graph");
    match action {
        "create" => match db.graph() {
            Ok(g) => match g.create(graph_name) {
                Ok(()) => ok_json(serde_json::json!({})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "add_vertex" => {
            let label = str_p(params, "label");
            let props = parse_props(params);
            match db.graph() {
                Ok(g) => match g.add_vertex(graph_name, label, &props) {
                    Ok(id) => ok_json(serde_json::json!({"vertex_id": id})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "get_vertex" => {
            let id = u64_p(params, "id");
            match db.graph_read() {
                Ok(g) => match g.get_vertex(graph_name, id) {
                    Ok(Some(v)) => ok_json(serde_json::json!({
                        "id": v.id, "label": v.label, "properties": v.properties
                    })),
                    Ok(None) => ok_json(serde_json::json!(null)),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "update_vertex" => {
            let id = u64_p(params, "id");
            let props = parse_props(params);
            match db.graph() {
                Ok(g) => match g.update_vertex(graph_name, id, &props) {
                    Ok(()) => ok_json(serde_json::json!({})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "delete_vertex" => {
            let id = u64_p(params, "id");
            match db.graph() {
                Ok(g) => match g.delete_vertex(graph_name, id) {
                    Ok(()) => ok_json(serde_json::json!({})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "add_edge" => {
            let from = u64_p(params, "from");
            let to = u64_p(params, "to");
            let label = str_p(params, "label");
            let props = parse_props(params);
            match db.graph() {
                Ok(g) => match g.add_edge(graph_name, from, to, label, &props) {
                    Ok(id) => ok_json(serde_json::json!({"edge_id": id})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "get_edge" => {
            let id = u64_p(params, "id");
            match db.graph_read() {
                Ok(g) => match g.get_edge(graph_name, id) {
                    Ok(Some(e)) => ok_json(serde_json::json!({
                        "id": e.id, "from": e.from, "to": e.to,
                        "label": e.label, "properties": e.properties
                    })),
                    Ok(None) => ok_json(serde_json::json!(null)),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "delete_edge" => {
            let id = u64_p(params, "id");
            match db.graph() {
                Ok(g) => match g.delete_edge(graph_name, id) {
                    Ok(()) => ok_json(serde_json::json!({})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "neighbors" => {
            let id = u64_p(params, "id");
            let dir = parse_direction(params);
            match db.graph_read() {
                Ok(g) => match g.neighbors(graph_name, id, dir) {
                    Ok(ids) => ok_json(serde_json::json!({"neighbors": ids})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "out_edges" => {
            let id = u64_p(params, "id");
            match db.graph_read() {
                Ok(g) => match g.out_edges(graph_name, id) {
                    Ok(edges) => ok_json(serde_json::json!({"edges": edges_json(&edges)})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "in_edges" => {
            let id = u64_p(params, "id");
            match db.graph_read() {
                Ok(g) => match g.in_edges(graph_name, id) {
                    Ok(edges) => ok_json(serde_json::json!({"edges": edges_json(&edges)})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "vertices_by_label" => {
            let label = str_p(params, "label");
            match db.graph_read() {
                Ok(g) => match g.vertices_by_label(graph_name, label) {
                    Ok(vs) => {
                        let arr: Vec<serde_json::Value> = vs
                            .iter()
                            .map(|v| {
                                serde_json::json!({
                                    "id": v.id, "label": v.label, "properties": v.properties
                                })
                            })
                            .collect();
                        ok_json(serde_json::json!({"vertices": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "vertex_count" => match db.graph_read() {
            Ok(g) => match g.vertex_count(graph_name) {
                Ok(n) => ok_json(serde_json::json!({"count": n})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "edge_count" => match db.graph_read() {
            Ok(g) => match g.edge_count(graph_name) {
                Ok(n) => ok_json(serde_json::json!({"count": n})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        // ── 遍历与算法 ──
        "bfs" => {
            let start = u64_p(params, "start");
            let max_depth = u64_p(params, "max_depth").max(1) as usize;
            let dir = parse_direction(params);
            match db.graph_read() {
                Ok(g) => match g.bfs(graph_name, start, max_depth, dir) {
                    Ok(results) => {
                        let arr: Vec<serde_json::Value> = results
                            .iter()
                            .map(|(id, depth)| serde_json::json!({"id": id, "depth": depth}))
                            .collect();
                        ok_json(serde_json::json!({"results": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "shortest_path" => {
            let from = u64_p(params, "from");
            let to = u64_p(params, "to");
            let max_depth = u64_p(params, "max_depth").max(1) as usize;
            match db.graph_read() {
                Ok(g) => match g.shortest_path(graph_name, from, to, max_depth) {
                    Ok(Some(path)) => ok_json(serde_json::json!({"path": path})),
                    Ok(None) => ok_json(serde_json::json!({"path": null})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "weighted_shortest_path" => {
            let from = u64_p(params, "from");
            let to = u64_p(params, "to");
            let max_depth = u64_p(params, "max_depth").max(1) as usize;
            let weight_key = str_p(params, "weight_key");
            let wk = if weight_key.is_empty() {
                "weight"
            } else {
                weight_key
            };
            match db.graph_read() {
                Ok(g) => match g.weighted_shortest_path(graph_name, from, to, max_depth, wk) {
                    Ok(Some((path, cost))) => {
                        ok_json(serde_json::json!({"path": path, "cost": cost}))
                    }
                    Ok(None) => ok_json(serde_json::json!({"path": null, "cost": null})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "degree_centrality" => {
            let limit = u64_p(params, "limit").max(1) as usize;
            match db.graph_read() {
                Ok(g) => match g.degree_centrality(graph_name, limit) {
                    Ok(degrees) => {
                        let arr: Vec<serde_json::Value> = degrees
                            .iter()
                            .map(|(id, od, ind)| {
                                serde_json::json!({
                                    "id": id, "out_degree": od, "in_degree": ind,
                                    "total_degree": od + ind
                                })
                            })
                            .collect();
                        ok_json(serde_json::json!({"results": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "pagerank" => {
            let damping = params
                .get("damping")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.85);
            let iterations = u64_p(params, "iterations").max(1) as usize;
            let limit = u64_p(params, "limit").max(1) as usize;
            match db.graph_read() {
                Ok(g) => match g.pagerank(graph_name, damping, iterations, limit) {
                    Ok(ranks) => {
                        let arr: Vec<serde_json::Value> = ranks
                            .iter()
                            .map(|(id, score)| serde_json::json!({"id": id, "score": score}))
                            .collect();
                        ok_json(serde_json::json!({"results": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        _ => err_json(&format!("未知 graph action: {}", action)),
    }
}

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

fn parse_direction(params: &serde_json::Value) -> crate::graph::Direction {
    match params
        .get("direction")
        .and_then(|v| v.as_str())
        .unwrap_or("out")
    {
        "in" => crate::graph::Direction::In,
        "both" => crate::graph::Direction::Both,
        _ => crate::graph::Direction::Out,
    }
}

fn edges_json(edges: &[crate::graph::Edge]) -> Vec<serde_json::Value> {
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
