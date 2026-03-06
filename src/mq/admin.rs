/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! MQ 管理操作：订阅、topic 详情、消费位移重置、清空、删除。

use crate::error::Error;

use super::{
    mq_keyspace_name, now_ms, ConsumerGroupInfo, ConsumerInfo, ConsumerState, MqEngine,
    SubscriptionMeta, TopicInfo, TopicMeta, DLQ_SUFFIX, MAX_DELAY_MS, MQ_CONSUMER_PREFIX,
    MQ_MAX_RETRIES_PREFIX, MQ_SUBSCRIBE_PREFIX, MQ_TOPIC_META_PREFIX, MQ_TOPIC_TTL_PREFIX,
    topic_meta_key,
};

impl MqEngine {
    /// 获取 topic 队列长度（O(1)，从 meta 读取）。
    pub fn len(&self, topic: &str) -> Result<u64, Error> {
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        Ok(TopicMeta::decode(&raw)?.count)
    }

    /// 注册消费者组订阅到 topic（持久化绑定关系）。
    ///
    /// 类似 Redis `XGROUP CREATE`：建立 group→topic 的持久化关系，
    /// 后续通过 `poll` / `poll_block` 消费消息。
    /// 重复订阅同一 group 幂等（不报错）。
    pub fn subscribe(&self, topic: &str, group: &str) -> Result<(), Error> {
        let meta_key = topic_meta_key(topic);
        if self.store_meta.get(meta_key.as_bytes())?.is_none() {
            return Err(Error::MessageQueue(format!("topic 不存在: {}", topic)));
        }
        let sub_key = format!("{}{}:{}", MQ_SUBSCRIBE_PREFIX, topic, group);
        let sub = SubscriptionMeta {
            created_at: now_ms(),
        };
        self.store_meta.set(sub_key.as_bytes(), sub.encode())?;
        Ok(())
    }

    /// 取消消费者组对 topic 的订阅。
    ///
    /// 同时清理该 group 下所有 consumer 的状态。
    pub fn unsubscribe(&self, topic: &str, group: &str) -> Result<(), Error> {
        let sub_key = format!("{}{}:{}", MQ_SUBSCRIBE_PREFIX, topic, group);
        self.store_meta.delete(sub_key.as_bytes())?;
        let prefix = format!("{}{}:{}:", MQ_CONSUMER_PREFIX, topic, group);
        let keys = self.store_meta.keys_with_prefix(prefix.as_bytes())?;
        for key in &keys {
            self.store_meta.delete(key)?;
        }
        Ok(())
    }

    /// 列出 topic 的所有订阅消费者组。
    pub fn list_subscriptions(&self, topic: &str) -> Result<Vec<String>, Error> {
        let prefix = format!("{}{}:", MQ_SUBSCRIBE_PREFIX, topic);
        let keys = self.store_meta.keys_with_prefix(prefix.as_bytes())?;
        let groups: Vec<String> = keys
            .iter()
            .filter_map(|k| {
                let s = std::str::from_utf8(k).ok()?;
                s.strip_prefix(&prefix).map(|g| g.to_string())
            })
            .collect();
        Ok(groups)
    }

    /// 列出所有已创建的 topic 名称。
    pub fn list_topics(&self) -> Result<Vec<String>, Error> {
        let keys = self
            .store_meta
            .keys_with_prefix(MQ_TOPIC_META_PREFIX.as_bytes())?;
        Ok(keys
            .iter()
            .filter_map(|k| {
                std::str::from_utf8(k)
                    .ok()?
                    .strip_prefix(MQ_TOPIC_META_PREFIX)
                    .map(|s| s.to_string())
            })
            .collect())
    }

    /// 获取 topic 详情：消息数、订阅组数、最大长度。
    ///
    /// topic 不存在返回错误。对标 Kafka `describeTopics()`。
    pub fn describe_topic(&self, topic: &str) -> Result<TopicInfo, Error> {
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        let meta = TopicMeta::decode(&raw)?;
        let subs = self.list_subscriptions(topic)?;
        Ok(TopicInfo {
            name: topic.to_string(),
            message_count: meta.count,
            subscriber_count: subs.len(),
            max_len: meta.max_len,
        })
    }

    /// 获取消费组详情：组内所有消费者的 acked_id 和 pending 数量。
    ///
    /// 对标 Kafka `describeConsumerGroups()`。
    /// 通过前缀扫描 `consumer:{topic}:{group}:` 获取所有消费者状态。
    pub fn describe_consumer_group(
        &self,
        topic: &str,
        group: &str,
    ) -> Result<ConsumerGroupInfo, Error> {
        let prefix = format!("{}{}:{}:", MQ_CONSUMER_PREFIX, topic, group);
        let mut consumers = Vec::new();
        self.store_meta
            .for_each_kv_prefix(prefix.as_bytes(), |key, raw| {
                let name = match std::str::from_utf8(key) {
                    Ok(s) => match s.strip_prefix(&prefix) {
                        Some(n) => n.to_string(),
                        None => return true,
                    },
                    Err(_) => return true,
                };
                if let Ok(state) = ConsumerState::decode(raw) {
                    consumers.push(ConsumerInfo {
                        consumer: name,
                        acked_id: state.acked_id,
                        pending_count: state.pending.len(),
                    });
                }
                true
            })?;
        Ok(ConsumerGroupInfo {
            group: group.to_string(),
            consumers,
        })
    }

    /// 重置消费组中指定消费者的消费位移。
    ///
    /// `new_offset` 为新的 acked_id，pending 列表清空。
    /// 对标 Kafka `alterConsumerGroupOffsets()`。
    pub fn reset_consumer_offset(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        new_offset: u64,
    ) -> Result<(), Error> {
        let state = ConsumerState {
            acked_id: new_offset,
            pending: Vec::new(),
            max_acked_id: new_offset,
        };
        self.put_consumer_state(topic, group, consumer, &state)
    }

    /// 清空 topic 中的所有消息，保留 topic 元数据和订阅关系。
    ///
    /// - 删除 keyspace 中所有消息
    /// - 重置 meta.count = 0（next_id 不变，避免 id 冲突）
    /// - 重置所有 consumer 的 cursor 和 pending 列表
    /// - 返回被清除的消息数量
    pub fn purge_topic(&self, topic: &str) -> Result<u64, Error> {
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        let mut meta = TopicMeta::decode(&raw)?;
        let purged = meta.count;

        // 1. 删除 keyspace 中所有消息
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        ks.for_each_key_prefix(b"", |key| {
            keys.push(key.to_vec());
            true
        })?;
        for key in &keys {
            ks.delete(key)?;
        }

        // 2. 重置 meta count（next_id 保持不变）
        meta.count = 0;
        self.store_meta.set(meta_key.as_bytes(), meta.encode())?;

        // 3. 重置所有 consumer 状态：cursor = next_id, pending = []
        let consumer_prefix = format!("{}{}:", MQ_CONSUMER_PREFIX, topic);
        let consumer_keys = self
            .store_meta
            .keys_with_prefix(consumer_prefix.as_bytes())?;
        let reset_state = ConsumerState {
            acked_id: meta.next_id.saturating_sub(1),
            pending: Vec::new(),
            max_acked_id: meta.next_id.saturating_sub(1),
        };
        for key in &consumer_keys {
            self.store_meta.set(key, reset_state.encode())?;
        }

        Ok(purged)
    }

    /// 删除 topic。
    /// M4：同时清理 SegmentManager 中该 topic 的所有缓存。
    /// M30：同时清理所有订阅关系。
    /// M79：同时清理 max_retries 设置和 DLQ topic。
    /// M124：同时清理 topic 默认 TTL 设置。
    pub fn drop_topic(&self, topic: &str) -> Result<(), Error> {
        let meta_key = topic_meta_key(topic);
        self.store_meta.delete(meta_key.as_bytes())?;
        // 清理所有订阅关系
        let sub_prefix = format!("{}{}:", MQ_SUBSCRIBE_PREFIX, topic);
        let sub_keys = self.store_meta.keys_with_prefix(sub_prefix.as_bytes())?;
        for key in &sub_keys {
            self.store_meta.delete(key)?;
        }
        // M79：清理 max_retries 设置
        let retries_key = format!("{}{}", MQ_MAX_RETRIES_PREFIX, topic);
        let _ = self.store_meta.delete(retries_key.as_bytes());
        // M124：清理 topic 默认 TTL 设置
        let ttl_key = format!("{}{}", MQ_TOPIC_TTL_PREFIX, topic);
        let _ = self.store_meta.delete(ttl_key.as_bytes());
        // M79：清理 DLQ topic（如果存在）
        let dlq_topic = format!("{}{}", topic, DLQ_SUFFIX);
        let dlq_meta_key = topic_meta_key(&dlq_topic);
        let _ = self.store_meta.delete(dlq_meta_key.as_bytes());
        self.segments.remove_prefix(&format!("mq:{}:", topic));
        Ok(())
    }

    /// 设置 topic 的默认消息 TTL（毫秒）。
    ///
    /// 发布消息时若未指定 per-message TTL，则使用此默认值。
    /// `ttl_ms = 0` 表示清除默认 TTL（消息永不过期）。
    pub fn set_topic_ttl(&self, topic: &str, ttl_ms: u64) -> Result<(), Error> {
        if ttl_ms > MAX_DELAY_MS {
            return Err(Error::MessageQueue(format!(
                "TTL 超出上限: {}ms > {}ms (7天)",
                ttl_ms, MAX_DELAY_MS
            )));
        }
        let meta_key = topic_meta_key(topic);
        if self.store_meta.get(meta_key.as_bytes())?.is_none() {
            return Err(Error::MessageQueue(format!("topic 不存在: {}", topic)));
        }
        let key = format!("{}{}", MQ_TOPIC_TTL_PREFIX, topic);
        if ttl_ms == 0 {
            let _ = self.store_meta.delete(key.as_bytes());
        } else {
            self.store_meta.set(key.as_bytes(), ttl_ms.to_le_bytes())?;
        }
        Ok(())
    }

    /// 获取 topic 的默认消息 TTL（毫秒）；未设置返回 0（永不过期）。
    pub fn get_topic_ttl(&self, topic: &str) -> Result<u64, Error> {
        let key = format!("{}{}", MQ_TOPIC_TTL_PREFIX, topic);
        match self.store_meta.get(key.as_bytes())? {
            Some(raw) if raw.len() >= 8 => Ok(u64::from_le_bytes(raw[..8].try_into().unwrap())),
            _ => Ok(0),
        }
    }
}
