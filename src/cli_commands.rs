/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! CLI 命令构建函数：将用户输入解析为 FFI JSON 命令。
//!
//! 从 cli.rs 拆分，保持单文件行数限制。

/// 构建 :vec 命令 JSON。
pub(super) fn build_vec(p: &[&str]) -> Result<serde_json::Value, String> {
    if p.len() < 2 {
        return Err(":vec 需要子命令：count/search".into());
    }
    match p[1] {
        "count" => {
            if p.len() < 3 {
                return Err(":vec count <name>".into());
            }
            Ok(serde_json::json!({"module":"vector","action":"count","params":{"name":p[2]}}))
        }
        "search" => {
            if p.len() < 4 {
                return Err(":vec search <name> <k> <v1,v2,...>".into());
            }
            let (k_s, v_s) = p[3].split_once(' ').unwrap_or(("5", p[3]));
            let k: usize = k_s.parse().unwrap_or(5);
            let vec: Vec<f32> = v_s
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            Ok(
                serde_json::json!({"module":"vector","action":"search","params":{"name":p[2],"vector":vec,"k":k}}),
            )
        }
        _ => Err(format!("未知 vec 子命令: {}", p[1])),
    }
}

/// 构建 :ts 命令 JSON。
pub(super) fn build_ts(p: &[&str]) -> Result<serde_json::Value, String> {
    if p.len() < 2 {
        return Err(":ts 需要子命令：query/list".into());
    }
    match p[1] {
        "list" => {
            Ok(serde_json::json!({"module":"sql","action":"query","params":{"sql":"SHOW TABLES"}}))
        }
        "query" => {
            if p.len() < 3 {
                return Err(":ts query <name>".into());
            }
            Ok(serde_json::json!({"module":"ts","action":"query","params":{"name":p[2]}}))
        }
        _ => Err(format!("未知 ts 子命令: {}", p[1])),
    }
}

/// 构建 :ai 命令 JSON。
pub(super) fn build_ai(p: &[&str]) -> Result<serde_json::Value, String> {
    if p.len() < 2 {
        return Err(":ai 需要子命令：sessions/session/history/memory/docs/doc".into());
    }
    match p[1] {
        "sessions" => Ok(serde_json::json!({"module":"ai","action":"list_sessions","params":{}})),
        "session" => {
            if p.len() < 3 {
                return Err(":ai session <id>".into());
            }
            Ok(serde_json::json!({"module":"ai","action":"get_session","params":{"id":p[2]}}))
        }
        "history" => {
            if p.len() < 3 {
                return Err(":ai history <session_id> [limit]".into());
            }
            let mut pm = serde_json::json!({"session_id":p[2]});
            if p.len() >= 4 {
                if let Ok(l) = p[3].trim().parse::<u64>() {
                    pm["limit"] = l.into();
                }
            }
            Ok(serde_json::json!({"module":"ai","action":"get_history","params":pm}))
        }
        "memory" => {
            let sub = if p.len() >= 3 { p[2].trim() } else { "" };
            if sub == "count" {
                Ok(serde_json::json!({"module":"ai","action":"memory_count","params":{}}))
            } else {
                Err(":ai memory count".into())
            }
        }
        "docs" => Ok(serde_json::json!({"module":"ai","action":"list_documents","params":{}})),
        "doc" => {
            let sub = if p.len() >= 3 { p[2].trim() } else { "" };
            if sub == "count" {
                return Ok(
                    serde_json::json!({"module":"ai","action":"document_count","params":{}}),
                );
            }
            sub.parse::<u64>().map(|id| serde_json::json!({"module":"ai","action":"get_document","params":{"doc_id":id}}))
                .map_err(|_| ":ai doc <id> 或 :ai doc count".into())
        }
        _ => Err(format!("未知 ai 子命令: {}", p[1])),
    }
}

/// 构建 :graph 命令 JSON。
pub(super) fn build_graph(p: &[&str]) -> Result<serde_json::Value, String> {
    if p.len() < 2 {
        return Err(
            ":graph 需要子命令：create/add_vertex/get_vertex/add_edge/neighbors/bfs/count/pagerank"
                .into(),
        );
    }
    match p[1] {
        "create" => {
            if p.len() < 3 {
                return Err(":graph create <name>".into());
            }
            Ok(serde_json::json!({"module":"graph","action":"create","params":{"graph":p[2]}}))
        }
        "add_vertex" => {
            if p.len() < 4 {
                return Err(":graph add_vertex <graph> <label>".into());
            }
            Ok(
                serde_json::json!({"module":"graph","action":"add_vertex","params":{"graph":p[2],"label":p[3]}}),
            )
        }
        "get_vertex" => {
            if p.len() < 4 {
                return Err(":graph get_vertex <graph> <id>".into());
            }
            let id: u64 = p[3].parse().map_err(|_| "id 必须为整数".to_string())?;
            Ok(
                serde_json::json!({"module":"graph","action":"get_vertex","params":{"graph":p[2],"id":id}}),
            )
        }
        "add_edge" => {
            if p.len() < 4 {
                return Err(":graph add_edge <graph> <from> <to> <label>".into());
            }
            let rest: Vec<&str> = p[3].splitn(3, ' ').collect();
            if rest.len() < 3 {
                return Err(":graph add_edge <graph> <from> <to> <label>".into());
            }
            let from: u64 = rest[0].parse().map_err(|_| "from 必须为整数".to_string())?;
            let to: u64 = rest[1].parse().map_err(|_| "to 必须为整数".to_string())?;
            Ok(
                serde_json::json!({"module":"graph","action":"add_edge","params":{"graph":p[2],"from":from,"to":to,"label":rest[2]}}),
            )
        }
        "neighbors" => {
            if p.len() < 4 {
                return Err(":graph neighbors <graph> <id> [out|in|both]".into());
            }
            let rest: Vec<&str> = p[3].splitn(2, ' ').collect();
            let id: u64 = rest[0].parse().map_err(|_| "id 必须为整数".to_string())?;
            let dir = rest.get(1).copied().unwrap_or("out");
            Ok(
                serde_json::json!({"module":"graph","action":"neighbors","params":{"graph":p[2],"id":id,"direction":dir}}),
            )
        }
        "bfs" => {
            if p.len() < 4 {
                return Err(":graph bfs <graph> <start> [depth]".into());
            }
            let rest: Vec<&str> = p[3].splitn(2, ' ').collect();
            let start: u64 = rest[0]
                .parse()
                .map_err(|_| "start 必须为整数".to_string())?;
            let depth: u64 = rest.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
            Ok(
                serde_json::json!({"module":"graph","action":"bfs","params":{"graph":p[2],"start":start,"max_depth":depth,"direction":"out"}}),
            )
        }
        "count" => {
            if p.len() < 3 {
                return Err(":graph count <graph>".into());
            }
            Ok(
                serde_json::json!({"module":"graph","action":"vertex_count","params":{"graph":p[2]}}),
            )
        }
        "pagerank" => {
            if p.len() < 3 {
                return Err(":graph pagerank <graph> [limit]".into());
            }
            let rest = if p.len() >= 4 { p[3] } else { "10" };
            let limit: u64 = rest
                .split(' ')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            Ok(
                serde_json::json!({"module":"graph","action":"pagerank","params":{"graph":p[2],"damping":0.85,"iterations":20,"limit":limit}}),
            )
        }
        "degree" => {
            if p.len() < 3 {
                return Err(":graph degree <graph> [limit]".into());
            }
            let rest = if p.len() >= 4 { p[3] } else { "10" };
            let limit: u64 = rest
                .split(' ')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            Ok(
                serde_json::json!({"module":"graph","action":"degree_centrality","params":{"graph":p[2],"limit":limit}}),
            )
        }
        _ => Err(format!("未知 graph 子命令: {}", p[1])),
    }
}

/// 构建 :geo 命令 JSON。
pub(super) fn build_geo(p: &[&str]) -> Result<serde_json::Value, String> {
    if p.len() < 2 {
        return Err(":geo 需要子命令：search/create/add/members".into());
    }
    match p[1] {
        "create" => {
            if p.len() < 3 {
                return Err(":geo create <name>".into());
            }
            Ok(serde_json::json!({"module":"geo","action":"create","params":{"name":p[2]}}))
        }
        "add" => {
            if p.len() < 4 {
                return Err(":geo add <name> <key> <lng> <lat>".into());
            }
            let rest: Vec<&str> = p[3].splitn(3, ' ').collect();
            if rest.len() < 3 {
                return Err(":geo add <name> <key> <lng> <lat>".into());
            }
            let lng: f64 = rest[1]
                .parse()
                .map_err(|_| "lng 必须为浮点数".to_string())?;
            let lat: f64 = rest[2]
                .parse()
                .map_err(|_| "lat 必须为浮点数".to_string())?;
            Ok(
                serde_json::json!({"module":"geo","action":"add","params":{"name":p[2],"key":rest[0],"lng":lng,"lat":lat}}),
            )
        }
        "search" => {
            if p.len() < 4 {
                return Err(":geo search <name> <lng> <lat> <radius>".into());
            }
            let rest: Vec<&str> = p[3].splitn(3, ' ').collect();
            if rest.len() < 3 {
                return Err(":geo search <name> <lng> <lat> <radius>".into());
            }
            let lng: f64 = rest[0]
                .parse()
                .map_err(|_| "lng 必须为浮点数".to_string())?;
            let lat: f64 = rest[1]
                .parse()
                .map_err(|_| "lat 必须为浮点数".to_string())?;
            let radius: f64 = rest[2]
                .parse()
                .map_err(|_| "radius 必须为浮点数".to_string())?;
            Ok(
                serde_json::json!({"module":"geo","action":"search","params":{"name":p[2],"lng":lng,"lat":lat,"radius":radius,"unit":"m","count":20}}),
            )
        }
        "members" => {
            if p.len() < 3 {
                return Err(":geo members <name>".into());
            }
            Ok(serde_json::json!({"module":"geo","action":"members","params":{"name":p[2]}}))
        }
        _ => Err(format!("未知 geo 子命令: {}", p[1])),
    }
}

/// 构建 :fts 命令 JSON。
pub(super) fn build_fts(p: &[&str]) -> Result<serde_json::Value, String> {
    if p.len() < 2 {
        return Err(":fts 需要子命令：search/create/index".into());
    }
    match p[1] {
        "create" => {
            if p.len() < 3 {
                return Err(":fts create <name>".into());
            }
            Ok(serde_json::json!({"module":"fts","action":"create_index","params":{"name":p[2]}}))
        }
        "search" => {
            if p.len() < 4 {
                return Err(":fts search <name> <query>".into());
            }
            Ok(
                serde_json::json!({"module":"fts","action":"search","params":{"name":p[2],"query":p[3],"limit":10}}),
            )
        }
        "fuzzy" => {
            if p.len() < 4 {
                return Err(":fts fuzzy <name> <query>".into());
            }
            Ok(
                serde_json::json!({"module":"fts","action":"search_fuzzy","params":{"name":p[2],"query":p[3],"max_dist":1,"limit":10}}),
            )
        }
        _ => Err(format!("未知 fts 子命令: {}", p[1])),
    }
}
