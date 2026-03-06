/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FFI 通用命令路由：解析 JSON 命令并分发到对应引擎。
//!
//! M25 实现。由 `talon_execute` C ABI 入口调用。

use crate::sql::{parse, Stmt};
use crate::Talon;

/// 解析 JSON 命令并路由到对应引擎模块。
///
/// 输入格式：`{"module":"...","action":"...","params":{...}}`
/// 输出格式：`{"ok":true,"data":{...}}` 或 `{"ok":false,"error":"..."}`
pub fn execute_cmd(db: &Talon, cmd_str: &str) -> String {
    let cmd: serde_json::Value = match serde_json::from_str(cmd_str) {
        Ok(v) => v,
        Err(e) => return err_json(&format!("JSON 解析失败: {}", e)),
    };
    let module = cmd.get("module").and_then(|v| v.as_str()).unwrap_or("");
    let action = cmd.get("action").and_then(|v| v.as_str()).unwrap_or("");
    let params = cmd.get("params").cloned().unwrap_or(serde_json::json!({}));
    match module {
        "sql" => exec_sql(db, &params),
        "kv" => exec_kv(db, action, &params),
        "ts" => ts::exec_ts(db, action, &params),
        "mq" => mq::exec_mq(db, action, &params),
        "vector" => exec_vector(db, action, &params),
        "graph" => graph::exec_graph(db, action, &params),
        "geo" => geo::exec_geo(db, action, &params),
        "fts" => fts::exec_fts(db, action, &params),
        "ai" => err_json("AI engine 已迁移至 talon-ai crate，请使用 talon-ai SDK"),
        "backup" => exec_backup(db, action, &params),
        "stats" => ok_json(db.stats()),
        "database_stats" => match db.database_stats() {
            Ok(s) => ok_json(s),
            Err(e) => err_json(&e.to_string()),
        },
        "health_check" => ok_json(db.health_check()),
        "cluster" => exec_cluster(db, action),
        _ => err_json(&format!("未知模块: {}", module)),
    }
}

pub(crate) fn ok_json(data: serde_json::Value) -> String {
    serde_json::json!({"ok": true, "data": data}).to_string()
}

pub(crate) fn err_json(msg: &str) -> String {
    serde_json::json!({"ok": false, "error": msg}).to_string()
}

pub(crate) fn str_p<'a>(params: &'a serde_json::Value, key: &str) -> &'a str {
    params.get(key).and_then(|v| v.as_str()).unwrap_or("")
}

pub(crate) fn u64_p(params: &serde_json::Value, key: &str) -> u64 {
    params.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
}

// ── SQL ──

fn exec_sql(db: &Talon, params: &serde_json::Value) -> String {
    let sql = str_p(params, "sql");
    match db.run_sql(sql) {
        Ok(rows) => {
            let columns = extract_result_columns(db, sql);
            ok_json(serde_json::json!({"rows": rows, "columns": columns}))
        }
        Err(e) => err_json(&e.to_string()),
    }
}

/// 从 SQL 中提取结果列名（尽力而为，不影响查询执行）。
fn extract_result_columns(db: &Talon, sql: &str) -> Vec<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let prefix = if trimmed.len() > 16 && trimmed.is_char_boundary(16) {
        &trimmed[..16]
    } else {
        trimmed
    };
    let upper16 = prefix.to_uppercase();
    if upper16.starts_with("SHOW TABLES") {
        return vec!["table_name".into()];
    }
    if upper16.starts_with("DESCRIBE ") || upper16.starts_with("DESC ") {
        return vec![
            "column".into(),
            "type".into(),
            "primary_key".into(),
            "nullable".into(),
            "default".into(),
            "foreign_key".into(),
            "comment".into(),
        ];
    }
    let stmt = match parse(trimmed) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    match stmt {
        Stmt::Select {
            columns,
            table,
            join,
            ..
        } => {
            if columns.len() == 1 && columns[0] == "*" {
                if join.is_some() {
                    return vec![];
                }
                // SELECT * — 从表 schema 获取列名
                match db.run_sql(&format!("DESCRIBE `{}`", table)) {
                    Ok(desc_rows) => desc_rows
                        .iter()
                        .filter_map(|r| {
                            r.first().and_then(|v| match v {
                                crate::types::Value::Text(s) => Some(s.clone()),
                                _ => None,
                            })
                        })
                        .collect(),
                    Err(_) => vec![],
                }
            } else {
                // 显式列：提取别名或列名
                columns
                    .iter()
                    .map(|c| {
                        // "expr AS alias" → alias（大小写不敏感查找最后一个 " AS "）
                        let bytes = c.as_bytes();
                        let mut pos = None;
                        for i in (0..bytes.len().saturating_sub(3)).rev() {
                            if (bytes[i] == b' ')
                                && (bytes[i + 1] == b'A' || bytes[i + 1] == b'a')
                                && (bytes[i + 2] == b'S' || bytes[i + 2] == b's')
                                && (bytes[i + 3] == b' ')
                            {
                                pos = Some(i + 4);
                                break;
                            }
                        }
                        if let Some(p) = pos {
                            c[p..].trim().to_string()
                        } else {
                            c.clone()
                        }
                    })
                    .collect()
            }
        }
        _ => vec![],
    }
}

// ── KV ──

fn exec_kv(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    // M96：读操作用 kv_read()（并发），写操作用 kv()（独占）
    match action {
        // ── 写操作（独占锁）──
        "set" | "del" | "incr" | "incrby" | "decrby" | "mset" | "expire" | "setnx" => {
            let kv = match db.kv() {
                Ok(k) => k,
                Err(e) => return err_json(&e.to_string()),
            };
            exec_kv_write(db, &kv, action, params)
        }
        // ── 读操作（共享锁）──
        _ => {
            let kv = match db.kv_read() {
                Ok(k) => k,
                Err(e) => return err_json(&e.to_string()),
            };
            exec_kv_read(&kv, action, params)
        }
    }
}

fn exec_kv_write(
    db: &Talon,
    kv: &crate::KvEngine,
    action: &str,
    params: &serde_json::Value,
) -> String {
    match action {
        "set" => {
            let key = str_p(params, "key");
            let value = str_p(params, "value");
            let ttl = params.get("ttl").and_then(|v| v.as_u64());
            match kv.set(key.as_bytes(), value.as_bytes(), ttl) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::KvSet {
                        key: key.as_bytes().to_vec(),
                        value: value.as_bytes().to_vec(),
                        ttl_secs: ttl,
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "del" => {
            let key = str_p(params, "key");
            let existed = kv.exists(key.as_bytes()).unwrap_or(false);
            match kv.del(key.as_bytes()) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::KvDel {
                        key: key.as_bytes().to_vec(),
                    });
                    ok_json(serde_json::json!({"deleted": existed}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "incr" => {
            let key = str_p(params, "key");
            match kv.incr(key.as_bytes()) {
                Ok(n) => {
                    let _ = db.append_oplog(crate::Operation::KvIncr {
                        key: key.as_bytes().to_vec(),
                        new_value: n,
                    });
                    ok_json(serde_json::json!({"value": n}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "mset" => {
            let keys: Vec<String> = params
                .get("keys")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let values: Vec<String> = params
                .get("values")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let k_refs: Vec<&[u8]> = keys.iter().map(|s| s.as_bytes()).collect();
            let v_refs: Vec<&[u8]> = values.iter().map(|s| s.as_bytes()).collect();
            match kv.mset(&k_refs, &v_refs) {
                Ok(()) => {
                    for (k, v) in keys.iter().zip(values.iter()) {
                        let _ = db.append_oplog(crate::Operation::KvSet {
                            key: k.as_bytes().to_vec(),
                            value: v.as_bytes().to_vec(),
                            ttl_secs: None,
                        });
                    }
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "expire" => {
            let key = str_p(params, "key");
            let secs = u64_p(params, "seconds");
            match kv.expire(key.as_bytes(), secs) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::KvExpire {
                        key: key.as_bytes().to_vec(),
                        secs,
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "incrby" => {
            let key = str_p(params, "key");
            let delta = params.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            match kv.incrby(key.as_bytes(), delta) {
                Ok(n) => {
                    let _ = db.append_oplog(crate::Operation::KvIncr {
                        key: key.as_bytes().to_vec(),
                        new_value: n,
                    });
                    ok_json(serde_json::json!({"value": n}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "decrby" => {
            let key = str_p(params, "key");
            let delta = params.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            match kv.decrby(key.as_bytes(), delta) {
                Ok(n) => {
                    let _ = db.append_oplog(crate::Operation::KvIncr {
                        key: key.as_bytes().to_vec(),
                        new_value: n,
                    });
                    ok_json(serde_json::json!({"value": n}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "setnx" => {
            let key = str_p(params, "key");
            let value = str_p(params, "value");
            let ttl = params.get("ttl").and_then(|v| v.as_u64());
            match kv.setnx(key.as_bytes(), value.as_bytes(), ttl) {
                Ok(was_set) => {
                    if was_set {
                        let _ = db.append_oplog(crate::Operation::KvSet {
                            key: key.as_bytes().to_vec(),
                            value: value.as_bytes().to_vec(),
                            ttl_secs: ttl,
                        });
                    }
                    ok_json(serde_json::json!({"set": was_set}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        _ => err_json(&format!("未知 KV 写操作: {}", action)),
    }
}

fn exec_kv_read(kv: &crate::KvEngine, action: &str, params: &serde_json::Value) -> String {
    match action {
        "get" => match kv.get(str_p(params, "key").as_bytes()) {
            Ok(Some(v)) => ok_json(serde_json::json!({"value": String::from_utf8_lossy(&v)})),
            Ok(None) => ok_json(serde_json::json!({"value": null})),
            Err(e) => err_json(&e.to_string()),
        },
        "exists" => match kv.exists(str_p(params, "key").as_bytes()) {
            Ok(b) => ok_json(serde_json::json!({"exists": b})),
            Err(e) => err_json(&e.to_string()),
        },
        "keys" => match kv.keys_prefix(str_p(params, "prefix").as_bytes()) {
            Ok(keys) => {
                let strs: Vec<String> = keys
                    .iter()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .collect();
                ok_json(serde_json::json!({"keys": strs}))
            }
            Err(e) => err_json(&e.to_string()),
        },
        "keys_match" => match kv.keys_match(str_p(params, "pattern").as_bytes()) {
            Ok(keys) => {
                let strs: Vec<String> = keys
                    .iter()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .collect();
                ok_json(serde_json::json!({"keys": strs}))
            }
            Err(e) => err_json(&e.to_string()),
        },
        "mget" => {
            let keys: Vec<String> = params
                .get("keys")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let k_refs: Vec<&[u8]> = keys.iter().map(|s| s.as_bytes()).collect();
            match kv.mget(&k_refs) {
                Ok(vals) => {
                    let items: Vec<serde_json::Value> = vals
                        .iter()
                        .map(|v| match v {
                            Some(b) => serde_json::json!(String::from_utf8_lossy(b)),
                            None => serde_json::json!(null),
                        })
                        .collect();
                    ok_json(serde_json::json!({"values": items}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "ttl" => match kv.ttl(str_p(params, "key").as_bytes()) {
            Ok(t) => ok_json(serde_json::json!({"ttl": t})),
            Err(e) => err_json(&e.to_string()),
        },
        // M67: 分页扫描（亿级安全）
        "keys_limit" => {
            let prefix = str_p(params, "prefix");
            let offset = u64_p(params, "offset");
            let limit = u64_p(params, "limit").max(1);
            match kv.keys_prefix_limit(prefix.as_bytes(), offset, limit) {
                Ok(keys) => {
                    let strs: Vec<String> = keys
                        .iter()
                        .map(|k| String::from_utf8_lossy(k).to_string())
                        .collect();
                    ok_json(serde_json::json!({"keys": strs}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "scan_limit" => {
            let prefix = str_p(params, "prefix");
            let offset = u64_p(params, "offset");
            let limit = u64_p(params, "limit").max(1);
            match kv.scan_prefix_limit(prefix.as_bytes(), offset, limit) {
                Ok(pairs) => {
                    let items: Vec<serde_json::Value> = pairs
                        .iter()
                        .map(|(k, v)| {
                            serde_json::json!({
                                "key": String::from_utf8_lossy(k),
                                "value": String::from_utf8_lossy(v),
                            })
                        })
                        .collect();
                    ok_json(serde_json::json!({"items": items}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "count" => match kv.key_count() {
            Ok(n) => ok_json(serde_json::json!({"count": n})),
            Err(e) => err_json(&e.to_string()),
        },
        _ => err_json(&format!("未知 KV 操作: {}", action)),
    }
}

// ── TS（委托到 ts 子模块） ──

mod ts;

// ── MQ（委托到 mq 子模块） ──

mod mq;

// ── Graph（委托到 graph 子模块） ──

mod graph;

// ── GEO（委托到 geo 子模块） ──

mod geo;

// ── FTS（委托到 fts 子模块） ──

mod fts;

// ── Vector ──

fn exec_vector(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    let name = str_p(params, "name");
    match action {
        "insert" => {
            let id = u64_p(params, "id");
            let vec: Vec<f32> = params
                .get("vector")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            match db.vector(name).and_then(|ve| ve.insert(id, &vec)) {
                Ok(()) => {
                    let vd: Vec<u8> = vec.iter().flat_map(|f| f.to_le_bytes()).collect();
                    let _ = db.append_oplog(crate::Operation::VecInsert {
                        collection: name.to_string(),
                        id,
                        vector_data: vd,
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "search" => {
            let vec: Vec<f32> = params
                .get("vector")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let k = u64_p(params, "k").max(1) as usize;
            let metric = str_p(params, "metric");
            let metric = if metric.is_empty() { "cosine" } else { metric };
            match db.vector(name).and_then(|ve| ve.search(&vec, k, metric)) {
                Ok(results) => {
                    let items: Vec<serde_json::Value> = results
                        .iter()
                        .map(|(id, dist)| serde_json::json!({"id": id, "distance": dist}))
                        .collect();
                    ok_json(serde_json::json!({"results": items}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "delete" => {
            let id = u64_p(params, "id");
            match db.vector(name).and_then(|ve| ve.delete(id)) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::VecDelete {
                        collection: name.to_string(),
                        id,
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "count" => match db.vector(name).and_then(|ve| ve.count()) {
            Ok(n) => ok_json(serde_json::json!({"count": n})),
            Err(e) => err_json(&e.to_string()),
        },
        "batch_insert" => {
            let items: Vec<serde_json::Value> = params
                .get("items")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let parsed: Vec<(u64, Vec<f32>)> = items
                .iter()
                .filter_map(|item| {
                    let id = item.get("id").and_then(|v| v.as_u64())?;
                    let vec: Vec<f32> = serde_json::from_value(item.get("vector")?.clone()).ok()?;
                    Some((id, vec))
                })
                .collect();
            let batch: Vec<(u64, &[f32])> =
                parsed.iter().map(|(id, v)| (*id, v.as_slice())).collect();
            match db.vector(name).and_then(|ve| ve.insert_batch(&batch)) {
                Ok(()) => {
                    for (id, v) in &parsed {
                        let vd: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
                        let _ = db.append_oplog(crate::Operation::VecInsert {
                            collection: name.to_string(),
                            id: *id,
                            vector_data: vd,
                        });
                    }
                    ok_json(serde_json::json!({"inserted": batch.len()}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "batch_search" => {
            let vecs_raw: Vec<Vec<f32>> = params
                .get("vectors")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let vecs: Vec<&[f32]> = vecs_raw.iter().map(|v| v.as_slice()).collect();
            let k = u64_p(params, "k").max(1) as usize;
            let metric = str_p(params, "metric");
            let metric = if metric.is_empty() { "cosine" } else { metric };
            match db
                .vector(name)
                .and_then(|ve| ve.batch_search(&vecs, k, metric))
            {
                Ok(results) => {
                    let items: Vec<serde_json::Value> = results
                        .iter()
                        .map(|group| {
                            let hits: Vec<serde_json::Value> = group
                                .iter()
                                .map(|(id, dist)| serde_json::json!({"id": id, "distance": dist}))
                                .collect();
                            serde_json::json!(hits)
                        })
                        .collect();
                    ok_json(serde_json::json!({"results": items}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "set_ef_search" => {
            let ef = u64_p(params, "ef_search").max(1) as usize;
            match db.vector(name).and_then(|ve| ve.set_ef_search(ef)) {
                Ok(()) => ok_json(serde_json::json!({})),
                Err(e) => err_json(&e.to_string()),
            }
        }
        _ => err_json(&format!("未知 Vector 操作: {}", action)),
    }
}

// ── Backup 路由 ──

fn exec_backup(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    match action {
        "export" => {
            let dir = str_p(params, "dir");
            let names: Vec<String> = params
                .get("keyspaces")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            match db.export(dir, &refs) {
                Ok(c) => ok_json(serde_json::json!({"exported": c})),
                Err(e) => err_json(&e.to_string()),
            }
        }
        "import" => match db.import(str_p(params, "dir")) {
            Ok(c) => ok_json(serde_json::json!({"imported": c})),
            Err(e) => err_json(&e.to_string()),
        },
        _ => err_json(&format!("未知 backup 操作: {}", action)),
    }
}

// ── Cluster ──

fn exec_cluster(db: &Talon, action: &str) -> String {
    match action {
        "status" => {
            let status = db.cluster_status();
            match serde_json::to_value(&status) {
                Ok(v) => ok_json(v),
                Err(e) => err_json(&e.to_string()),
            }
        }
        "role" => ok_json(serde_json::json!({"role": db.cluster_role()})),
        "promote" => match db.promote() {
            Ok(()) => ok_json(serde_json::json!({"promoted": true, "role": "Primary"})),
            Err(e) => err_json(&e.to_string()),
        },
        "replicas" => {
            let status = db.cluster_status();
            match serde_json::to_value(&status.replicas) {
                Ok(v) => ok_json(v),
                Err(e) => err_json(&e.to_string()),
            }
        }
        _ => err_json(&format!("未知 cluster 操作: {}", action)),
    }
}

#[cfg(test)]
mod tests;
