/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! MQ 发布扩展：延迟消息、TTL 消息、组合发布。

use crate::error::Error;

use super::{
    mq_keyspace_name, now_ms, Message, MqEngine, TopicMeta, MAX_DELAY_MS, topic_meta_key,
};

impl MqEngine {
    /// 发布延迟消息到 topic；消息在 `delay_ms` 毫秒后才对 consumer 可见。
    ///
    /// - `delay_ms = 0`：等同于 `publish`（立即投递）
    /// - 最大延迟 7 天（604,800,000 ms），超出返回错误
    ///
    /// AI 场景：Agent 重试退避、RAG 管道延迟触发、对话超时提醒。
    pub fn publish_delayed(
        &self,
        topic: &str,
        payload: &[u8],
        delay_ms: u64,
    ) -> Result<u64, Error> {
        if delay_ms > MAX_DELAY_MS {
            return Err(Error::MessageQueue(format!(
                "延迟时间超出上限: {}ms > {}ms (7天)",
                delay_ms, MAX_DELAY_MS
            )));
        }
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        let mut meta = TopicMeta::decode(&raw)?;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let ts = now_ms();
        let deliver_at = if delay_ms > 0 {
            ts + delay_ms as i64
        } else {
            0
        };
        let msg_id = meta.next_id;
        meta.next_id += 1;
        meta.count += 1;
        let msg = Message {
            id: msg_id,
            payload: payload.to_vec(),
            timestamp: ts,
            retry_count: 0,
            deliver_at,
            expire_at: 0,
            key: None,
            priority: 5,
        };
        let mut batch = self.store.batch();
        batch.insert(&ks, msg_id.to_be_bytes().to_vec(), msg.encode())?;
        batch.insert(
            &self.store_meta,
            meta_key.into_bytes(),
            meta.encode().to_vec(),
        )?;
        batch.commit()?;
        if meta.max_len > 0 {
            self.trim_topic(topic, meta.max_len)?;
        }
        Ok(msg_id)
    }

    /// 发布带 TTL 的消息到 topic；消息在 `ttl_ms` 毫秒后自动过期不可见。
    ///
    /// - `ttl_ms = 0`：使用 topic 默认 TTL（若未设置则永不过期）
    /// - 过期消息在 poll 时惰性删除
    ///
    /// AI 场景：对话上下文自动过期、临时缓存消息、Agent 任务超时清理。
    pub fn publish_with_ttl(&self, topic: &str, payload: &[u8], ttl_ms: u64) -> Result<u64, Error> {
        self.publish_advanced(topic, payload, 0, ttl_ms)
    }

    /// 发布同时支持延迟投递和 TTL 的消息。
    ///
    /// - `delay_ms`：延迟投递时间（0 = 立即）
    /// - `ttl_ms`：消息存活时间（0 = 使用 topic 默认 TTL，topic 默认也为 0 则永不过期）
    /// - `expire_at` 优先于 `deliver_at`：过期消息即使到了投递时间也不会被消费
    pub fn publish_advanced(
        &self,
        topic: &str,
        payload: &[u8],
        delay_ms: u64,
        ttl_ms: u64,
    ) -> Result<u64, Error> {
        if delay_ms > MAX_DELAY_MS {
            return Err(Error::MessageQueue(format!(
                "延迟时间超出上限: {}ms > {}ms (7天)",
                delay_ms, MAX_DELAY_MS
            )));
        }
        if ttl_ms > MAX_DELAY_MS {
            return Err(Error::MessageQueue(format!(
                "TTL 超出上限: {}ms > {}ms (7天)",
                ttl_ms, MAX_DELAY_MS
            )));
        }
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        let mut meta = TopicMeta::decode(&raw)?;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let ts = now_ms();
        let deliver_at = if delay_ms > 0 {
            ts + delay_ms as i64
        } else {
            0
        };
        // 确定 expire_at：per-message TTL > topic 默认 TTL > 0（永不过期）
        let effective_ttl = if ttl_ms > 0 {
            ttl_ms
        } else {
            self.get_topic_ttl(topic)?
        };
        let expire_at = if effective_ttl > 0 {
            ts + effective_ttl as i64
        } else {
            0
        };
        let msg_id = meta.next_id;
        meta.next_id += 1;
        meta.count += 1;
        let msg = Message {
            id: msg_id,
            payload: payload.to_vec(),
            timestamp: ts,
            retry_count: 0,
            deliver_at,
            expire_at,
            key: None,
            priority: 5,
        };
        let mut batch = self.store.batch();
        batch.insert(&ks, msg_id.to_be_bytes().to_vec(), msg.encode())?;
        batch.insert(
            &self.store_meta,
            meta_key.into_bytes(),
            meta.encode().to_vec(),
        )?;
        batch.commit()?;
        if meta.max_len > 0 {
            self.trim_topic(topic, meta.max_len)?;
        }
        Ok(msg_id)
    }

    /// 发布带优先级的消息到 topic。
    ///
    /// - `priority`：0-9，0 最高优先级，9 最低；默认 5
    /// - poll 时同一批候选消息按 `(priority, id)` 排序，优先级高的先消费
    /// - 超出 0-9 范围返回错误
    ///
    /// AI 场景：Agent 紧急任务优先处理、RAG 管道关键查询插队、对话管理 VIP 用户优先。
    pub fn publish_with_priority(
        &self,
        topic: &str,
        payload: &[u8],
        priority: u8,
    ) -> Result<u64, Error> {
        if priority > 9 {
            return Err(Error::MessageQueue(format!(
                "优先级超出范围: {} (有效范围 0-9)",
                priority
            )));
        }
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        let mut meta = TopicMeta::decode(&raw)?;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let ts = now_ms();
        let msg_id = meta.next_id;
        meta.next_id += 1;
        meta.count += 1;
        let msg = Message {
            id: msg_id,
            payload: payload.to_vec(),
            timestamp: ts,
            retry_count: 0,
            deliver_at: 0,
            expire_at: 0,
            key: None,
            priority,
        };
        let mut batch = self.store.batch();
        batch.insert(&ks, msg_id.to_be_bytes().to_vec(), msg.encode())?;
        batch.insert(
            &self.store_meta,
            meta_key.into_bytes(),
            meta.encode().to_vec(),
        )?;
        batch.commit()?;
        if meta.max_len > 0 {
            self.trim_topic(topic, meta.max_len)?;
        }
        Ok(msg_id)
    }
}
