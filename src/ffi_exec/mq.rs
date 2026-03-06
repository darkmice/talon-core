/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FFI MQ 命令路由（从 mod.rs 拆分，满足 500 行约束）。

use super::{err_json, ok_json, str_p, u64_p};
use crate::Talon;

pub(super) fn exec_mq(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    let topic = str_p(params, "topic");
    match action {
        // ── 写操作（mq() 加只读保护）──
        "create" | "publish" | "ack" | "drop" | "subscribe" | "unsubscribe" => {
            let mq = match db.mq() {
                Ok(m) => m,
                Err(e) => return err_json(&e.to_string()),
            };
            exec_mq_write(db, &mq, action, topic, params)
        }
        // ── 阻塞拉取（特殊处理：需多次获取锁）──
        "poll" => exec_mq_poll(db, topic, params),
        // ── 读操作（mq_read()，Replica 可用）──
        _ => {
            let mq = match db.mq_read() {
                Ok(m) => m,
                Err(e) => return err_json(&e.to_string()),
            };
            exec_mq_read(&mq, action, topic)
        }
    }
}

fn exec_mq_write(
    db: &Talon,
    mq: &crate::MqEngine,
    action: &str,
    topic: &str,
    params: &serde_json::Value,
) -> String {
    match action {
        "create" => {
            let max_len = u64_p(params, "max_len");
            match mq.create_topic(topic, max_len) {
                Ok(()) => {
                    let ml = if max_len > 0 { Some(max_len) } else { None };
                    let _ = db.append_oplog(crate::Operation::MqCreate {
                        topic: topic.to_string(),
                        max_len: ml,
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "publish" => {
            let payload = str_p(params, "payload");
            let delay_ms = u64_p(params, "delay_ms");
            let ttl_ms = u64_p(params, "ttl_ms");
            let result = if delay_ms > 0 || ttl_ms > 0 {
                mq.publish_advanced(topic, payload.as_bytes(), delay_ms, ttl_ms)
            } else {
                mq.publish(topic, payload.as_bytes())
            };
            match result {
                Ok(id) => {
                    let _ = db.append_oplog(crate::Operation::MqPublish {
                        topic: topic.to_string(),
                        payload: payload.as_bytes().to_vec(),
                    });
                    ok_json(serde_json::json!({"id": id}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "ack" => {
            let group = str_p(params, "group");
            let consumer = str_p(params, "consumer");
            let msg_id = u64_p(params, "message_id");
            match mq.ack(topic, group, consumer, msg_id) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::MqAck {
                        topic: topic.to_string(),
                        group: group.to_string(),
                        msg_id: msg_id.to_string(),
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "drop" => match mq.drop_topic(topic) {
            Ok(()) => {
                let _ = db.append_oplog(crate::Operation::MqDrop {
                    topic: topic.to_string(),
                });
                ok_json(serde_json::json!({}))
            }
            Err(e) => err_json(&e.to_string()),
        },
        "subscribe" => {
            let group = str_p(params, "group");
            match mq.subscribe(topic, group) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::MqSubscribe {
                        topic: topic.to_string(),
                        group: group.to_string(),
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "unsubscribe" => {
            let group = str_p(params, "group");
            match mq.unsubscribe(topic, group) {
                Ok(()) => {
                    let _ = db.append_oplog(crate::Operation::MqUnsubscribe {
                        topic: topic.to_string(),
                        group: group.to_string(),
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        _ => err_json(&format!("未知 MQ 写操作: {}", action)),
    }
}

fn exec_mq_poll(db: &Talon, topic: &str, params: &serde_json::Value) -> String {
    let group = str_p(params, "group");
    let consumer = str_p(params, "consumer");
    let count = u64_p(params, "count").max(1) as usize;
    let block_ms = u64_p(params, "block_ms");
    // block_ms > 0 时使用阻塞拉取；需多次获取锁
    if block_ms > 0 {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(block_ms);
        loop {
            let mq = match db.mq_read() {
                Ok(m) => m,
                Err(e) => return err_json(&e.to_string()),
            };
            match mq.poll(topic, group, consumer, count) {
                Ok(msgs) if !msgs.is_empty() => {
                    return ok_json(serde_json::json!({"messages":
                        format_mq_msgs(&msgs)}));
                }
                Ok(_) => {}
                Err(e) => return err_json(&e.to_string()),
            }
            drop(mq);
            if std::time::Instant::now() >= deadline {
                return ok_json(serde_json::json!({"messages": []}));
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    let mq = match db.mq_read() {
        Ok(m) => m,
        Err(e) => return err_json(&e.to_string()),
    };
    match mq.poll(topic, group, consumer, count) {
        Ok(msgs) => ok_json(serde_json::json!({"messages": format_mq_msgs(&msgs)})),
        Err(e) => err_json(&e.to_string()),
    }
}

fn exec_mq_read(mq: &crate::MqEngine, action: &str, topic: &str) -> String {
    match action {
        "len" => match mq.len(topic) {
            Ok(n) => ok_json(serde_json::json!({"len": n})),
            Err(e) => err_json(&e.to_string()),
        },
        "list_subscriptions" => match mq.list_subscriptions(topic) {
            Ok(groups) => ok_json(serde_json::json!({"groups": groups})),
            Err(e) => err_json(&e.to_string()),
        },
        "topics" => match mq.list_topics() {
            Ok(topics) => ok_json(serde_json::json!({"topics": topics})),
            Err(e) => err_json(&e.to_string()),
        },
        _ => err_json(&format!("未知 MQ 操作: {}", action)),
    }
}

/// 格式化 MQ 消息列表为 JSON 数组。
fn format_mq_msgs(msgs: &[crate::Message]) -> Vec<serde_json::Value> {
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
