/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! HTTP 路由处理函数：SQL / KV / TS / MQ / Vector。

use std::io::Write;
use std::net::TcpStream;

use crate::error::Error;
use crate::Talon;

use super::protocol::{Request, Response};

pub(super) fn parse_request(body: &[u8]) -> Result<Request, Error> {
    serde_json::from_slice(body).map_err(|e| Error::Protocol(format!("请求解析失败: {}", e)))
}

pub(super) fn write_response(
    stream: &mut TcpStream,
    status: u16,
    resp: &Response,
) -> Result<(), Error> {
    let body = serde_json::to_vec(resp).map_err(|e| Error::Protocol(e.to_string()))?;
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    // 预分配精确容量，避免 format! 的额外分配
    let header_prefix = b"HTTP/1.1 ";
    let content_type = b"\r\nContent-Type: application/json\r\nContent-Length: ";
    let header_end = b"\r\nConnection: close\r\n\r\n";
    let status_bytes = status.to_string();
    let body_len_bytes = body.len().to_string();
    let total_len = header_prefix.len()
        + status_bytes.len()
        + 1
        + status_text.len()
        + content_type.len()
        + body_len_bytes.len()
        + header_end.len()
        + body.len();
    let mut buf = Vec::with_capacity(total_len);
    buf.extend_from_slice(header_prefix);
    buf.extend_from_slice(status_bytes.as_bytes());
    buf.push(b' ');
    buf.extend_from_slice(status_text.as_bytes());
    buf.extend_from_slice(content_type);
    buf.extend_from_slice(body_len_bytes.as_bytes());
    buf.extend_from_slice(header_end);
    buf.extend_from_slice(&body);
    stream.write_all(&buf)?;
    stream.flush()?;
    Ok(())
}

/// SQL 路由：执行 SQL 语句。
///
/// 支持参数化查询：`params.bind` 为 Value 数组时调用 `run_sql_param`。
/// ```json
/// {"module":"sql","params":{"sql":"SELECT * FROM t WHERE id = ?","bind":[{"Integer":1}]}}
/// ```
pub(super) fn handle_sql(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let sql = req.params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
    let result = if let Some(bind_arr) = req.params.get("bind").and_then(|v| v.as_array()) {
        let params: Result<Vec<crate::types::Value>, _> = bind_arr
            .iter()
            .map(|v| serde_json::from_value(v.clone()).map_err(|e| Error::SqlExec(e.to_string())))
            .collect();
        match params {
            Ok(p) => db.run_sql_param(sql, &p),
            Err(e) => Err(e),
        }
    } else {
        db.run_sql(sql)
    };
    match result {
        Ok(rows) => write_response(
            stream,
            200,
            &Response::ok(serde_json::json!({"rows": rows})),
        ),
        Err(e) => write_response(stream, 200, &Response::err(e.to_string())),
    }
}

/// KV 路由：SET/GET/DEL/MSET/MGET/EXISTS/INCR/EXPIRE/TTL/KEYS。
pub(super) fn handle_kv(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    // M96：读操作用 kv_read()（并发），写操作用 kv()（独占）
    let resp = match req.action.as_str() {
        "set" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let value = req
                .params
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ttl = req.params.get("ttl").and_then(|v| v.as_u64());
            let kv = db.kv()?;
            match kv.set(key.as_bytes(), value.as_bytes(), ttl) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "get" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let kv = db.kv_read()?;
            match kv.get(key.as_bytes()) {
                Ok(Some(v)) => Response::ok(serde_json::json!({
                    "value": String::from_utf8_lossy(&v)
                })),
                Ok(None) => Response::ok(serde_json::json!({"value": null})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "del" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let kv = db.kv()?;
            let existed = kv.exists(key.as_bytes())?;
            match kv.del(key.as_bytes()) {
                Ok(()) => Response::ok(serde_json::json!({"deleted": existed})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "exists" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let kv = db.kv_read()?;
            match kv.exists(key.as_bytes()) {
                Ok(b) => Response::ok(serde_json::json!({"exists": b})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "incr" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let kv = db.kv()?;
            match kv.incr(key.as_bytes()) {
                Ok(n) => Response::ok(serde_json::json!({"value": n})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "incrby" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let delta = req
                .params
                .get("delta")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            let kv = db.kv()?;
            match kv.incrby(key.as_bytes(), delta) {
                Ok(n) => Response::ok(serde_json::json!({"value": n})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "decrby" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let delta = req
                .params
                .get("delta")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            let kv = db.kv()?;
            match kv.decrby(key.as_bytes(), delta) {
                Ok(n) => Response::ok(serde_json::json!({"value": n})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "setnx" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let value = req
                .params
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ttl = req.params.get("ttl").and_then(|v| v.as_u64());
            let kv = db.kv()?;
            match kv.setnx(key.as_bytes(), value.as_bytes(), ttl) {
                Ok(was_set) => Response::ok(serde_json::json!({"set": was_set})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "keys" => {
            let prefix = req
                .params
                .get("prefix")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kv = db.kv_read()?;
            match kv.keys_prefix(prefix.as_bytes()) {
                Ok(keys) => {
                    let strs: Vec<String> = keys
                        .iter()
                        .map(|k| String::from_utf8_lossy(k).to_string())
                        .collect();
                    Response::ok(serde_json::json!({"keys": strs}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "keys_match" => {
            let pattern = req
                .params
                .get("pattern")
                .and_then(|v| v.as_str())
                .unwrap_or("*");
            let kv = db.kv_read()?;
            match kv.keys_match(pattern.as_bytes()) {
                Ok(keys) => {
                    let strs: Vec<String> = keys
                        .iter()
                        .map(|k| String::from_utf8_lossy(k).to_string())
                        .collect();
                    Response::ok(serde_json::json!({"keys": strs}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "expire" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let seconds = req
                .params
                .get("seconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let kv = db.kv()?;
            match kv.expire(key.as_bytes(), seconds) {
                Ok(b) => Response::ok(serde_json::json!({"updated": b})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "ttl" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let kv = db.kv_read()?;
            match kv.ttl(key.as_bytes()) {
                Ok(t) => Response::ok(serde_json::json!({"ttl": t})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "mset" => {
            let pairs: Vec<(String, String)> = req
                .params
                .get("pairs")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_bytes()).collect();
            let vals: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_bytes()).collect();
            let kv = db.kv()?;
            match kv.mset(&keys, &vals) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "mget" => {
            let keys: Vec<String> = req
                .params
                .get("keys")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            let kv = db.kv_read()?;
            match kv.mget(&key_refs) {
                Ok(vals) => {
                    let items: Vec<serde_json::Value> = vals
                        .iter()
                        .map(|v| match v {
                            Some(b) => serde_json::json!(String::from_utf8_lossy(b)),
                            None => serde_json::Value::Null,
                        })
                        .collect();
                    Response::ok(serde_json::json!({"values": items}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        _ => Response::err(format!("未知 KV 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}

/// 消息队列路由：create/publish/poll/ack/len/drop。
/// P0：MQ 引擎通过 Talon::mq() 获取 Mutex 保护的单例。
pub(super) fn handle_mq(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let mq = db.mq()?;
    let resp = match req.action.as_str() {
        "create" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let max_len = req
                .params
                .get("max_len")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            match mq.create_topic(topic, max_len) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "publish" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let payload = req
                .params
                .get("payload")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // 支持延迟发布和 TTL
            let delay_ms = req
                .params
                .get("delay_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let ttl_ms = req
                .params
                .get("ttl_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let result = if delay_ms > 0 || ttl_ms > 0 {
                mq.publish_advanced(topic, payload.as_bytes(), delay_ms, ttl_ms)
            } else {
                mq.publish(topic, payload.as_bytes())
            };
            match result {
                Ok(id) => Response::ok(serde_json::json!({"id": id})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "publish_batch" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let payloads: Vec<&[u8]> = req
                .params
                .get("payloads")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.as_bytes()))
                        .collect()
                })
                .unwrap_or_default();
            match mq.publish_batch(topic, &payloads) {
                Ok(ids) => Response::ok(serde_json::json!({"ids": ids})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "poll" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let group = req
                .params
                .get("group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let consumer = req
                .params
                .get("consumer")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let count = req
                .params
                .get("count")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            let block_ms = req
                .params
                .get("block_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            // BLOCK 模式：在 handler 层循环，每次迭代释放 Mutex 后 sleep
            if block_ms > 0 {
                drop(mq); // 释放 Mutex，避免阻塞其他操作
                let deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(block_ms);
                loop {
                    let mq = db.mq()?;
                    match mq.poll(topic, group, consumer, count) {
                        Ok(msgs) if !msgs.is_empty() => {
                            let items = format_mq_messages(&msgs);
                            let resp = Response::ok(serde_json::json!({"messages": items}));
                            return write_response(stream, 200, &resp);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            let resp = Response::err(e.to_string());
                            return write_response(stream, 200, &resp);
                        }
                    }
                    drop(mq);
                    if std::time::Instant::now() >= deadline {
                        let resp = Response::ok(serde_json::json!({"messages": []}));
                        return write_response(stream, 200, &resp);
                    }
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    let sleep_dur = remaining.min(std::time::Duration::from_millis(50));
                    std::thread::sleep(sleep_dur);
                }
            }
            match mq.poll(topic, group, consumer, count) {
                Ok(msgs) => {
                    let items = format_mq_messages(&msgs);
                    Response::ok(serde_json::json!({"messages": items}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "ack" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let group = req
                .params
                .get("group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let consumer = req
                .params
                .get("consumer")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let message_id = req
                .params
                .get("message_id")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            match mq.ack(topic, group, consumer, message_id) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "len" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match mq.len(topic) {
                Ok(n) => Response::ok(serde_json::json!({"len": n})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "drop" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match mq.drop_topic(topic) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "subscribe" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let group = req
                .params
                .get("group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match mq.subscribe(topic, group) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "unsubscribe" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let group = req
                .params
                .get("group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match mq.unsubscribe(topic, group) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "list_subscriptions" => {
            let topic = req
                .params
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match mq.list_subscriptions(topic) {
                Ok(groups) => Response::ok(serde_json::json!({"groups": groups})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        _ => Response::err(format!("未知 MQ 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}

/// 格式化 MQ 消息列表为 JSON 数组。
fn format_mq_messages(msgs: &[crate::Message]) -> Vec<serde_json::Value> {
    msgs.iter()
        .map(|m| {
            serde_json::json!({
                "id": m.id,
                "payload": String::from_utf8_lossy(&m.payload),
                "timestamp": m.timestamp,
            })
        })
        .collect()
}

/// 备份路由：export/import。
pub(super) fn handle_backup(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let resp = match req.action.as_str() {
        "export" => {
            let dir = req.params.get("dir").and_then(|v| v.as_str()).unwrap_or("");
            let names: Vec<String> = req
                .params
                .get("keyspaces")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            match db.export(dir, &refs) {
                Ok(count) => Response::ok(serde_json::json!({"exported": count})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "import" => {
            let dir = req.params.get("dir").and_then(|v| v.as_str()).unwrap_or("");
            match db.import(dir) {
                Ok(count) => Response::ok(serde_json::json!({"imported": count})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        _ => Response::err(format!("未知 backup 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}
