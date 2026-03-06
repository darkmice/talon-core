/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M79：NACK + Dead Letter Queue（DLQ）。
//!
//! 消息消费失败时通过 `nack()` 重新入队或移入死信队列。
//! DLQ topic 命名：`{topic}_dlq`，自动创建。

use crate::error::Error;

use super::{
    mq_keyspace_name, now_ms, Message, MqEngine, TopicMeta, DEFAULT_MAX_RETRIES, DLQ_SUFFIX,
    MQ_MAX_RETRIES_PREFIX, topic_meta_key,
};

impl MqEngine {
    /// 设置 topic 的最大重试次数。
    ///
    /// 消息被 nack(requeue=true) 时，retry_count 达到此阈值后自动移入 DLQ。
    /// 默认值为 3。
    pub fn set_max_retries(&self, topic: &str, max_retries: u32) -> Result<(), Error> {
        // 验证 topic 存在
        let meta_key = topic_meta_key(topic);
        if self.store_meta.get(meta_key.as_bytes())?.is_none() {
            return Err(Error::MessageQueue(format!("topic 不存在: {}", topic)));
        }
        let key = format!("{}{}", MQ_MAX_RETRIES_PREFIX, topic);
        self.store_meta
            .set(key.as_bytes(), max_retries.to_le_bytes())?;
        Ok(())
    }

    /// 获取 topic 的最大重试次数（未设置则返回默认值 3）。
    fn get_max_retries(&self, topic: &str) -> Result<u32, Error> {
        let key = format!("{}{}", MQ_MAX_RETRIES_PREFIX, topic);
        match self.store_meta.get(key.as_bytes())? {
            Some(raw) if raw.len() >= 4 => Ok(u32::from_le_bytes(raw[..4].try_into().unwrap())),
            _ => Ok(DEFAULT_MAX_RETRIES),
        }
    }

    /// 否定确认消息：消费失败后重新入队或移入死信队列。
    ///
    /// - `requeue=true`：递增 retry_count 后重新发布到原 topic；
    ///   若 retry_count >= max_retries，自动移入 DLQ。
    /// - `requeue=false`：直接移入 DLQ。
    ///
    /// 无论哪种路径，消息都会从 consumer 的 pending 列表中移除。
    pub fn nack(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        message_id: u64,
        requeue: bool,
    ) -> Result<(), Error> {
        // 1. 读取原始消息
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let raw = ks
            .get(&message_id.to_be_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("消息不存在: {}", message_id)))?;
        let mut msg = Message::decode(&raw)?;

        // 2. 准备 consumer state 更新（移除 pending）
        let mut state = self.get_consumer_state(topic, group, consumer)?;
        state.pending.retain(|&id| id != message_id);
        let consumer_key = Self::consumer_key(topic, group, consumer);

        // 3. 原子 WriteBatch：consumer state + 消息操作一次提交
        let mut batch = self.store.batch();
        batch.insert(&self.store_meta, consumer_key.into_bytes(), state.encode())?;

        if requeue {
            msg.retry_count = msg.retry_count.saturating_add(1);
            let max = self.get_max_retries(topic)?;
            if msg.retry_count < max {
                // 重新写入原 topic（覆盖原消息，保留 id）
                batch.insert(&ks, message_id.to_be_bytes().to_vec(), msg.encode())?;
            } else {
                // 超过重试上限，移入 DLQ 并从原 topic 删除
                self.move_to_dlq_batch(&mut batch, topic, &msg)?;
                batch.remove(&ks, message_id.to_be_bytes().to_vec());
                self.decrement_topic_count_batch(&mut batch, topic)?;
            }
        } else {
            // 直接移入 DLQ 并从原 topic 删除
            self.move_to_dlq_batch(&mut batch, topic, &msg)?;
            batch.remove(&ks, message_id.to_be_bytes().to_vec());
            self.decrement_topic_count_batch(&mut batch, topic)?;
        }

        batch.commit()
    }

    /// 从死信队列拉取一条消息。
    ///
    /// DLQ 使用独立 consumer 状态，与原 topic 隔离。
    /// 返回 `None` 表示 DLQ 为空。
    pub fn poll_dlq(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
    ) -> Result<Option<Message>, Error> {
        let dlq_topic = format!("{}{}", topic, DLQ_SUFFIX);
        // DLQ topic 可能尚未创建（无消息进入过 DLQ）
        let meta_key = topic_meta_key(&dlq_topic);
        if self.store_meta.get(meta_key.as_bytes())?.is_none() {
            return Ok(None);
        }
        let msgs = self.poll(&dlq_topic, group, consumer, 1)?;
        Ok(msgs.into_iter().next())
    }

    /// 将消息移入死信队列（通过 WriteBatch，崩溃安全）。
    ///
    /// DLQ topic 自动创建（无 MAXLEN 限制）。
    /// 消息在 DLQ 中获得新的 id 和时间戳，保留原始 payload 和 retry_count。
    fn move_to_dlq_batch(
        &self,
        batch: &mut crate::storage::Batch,
        topic: &str,
        msg: &Message,
    ) -> Result<(), Error> {
        let dlq_topic = format!("{}{}", topic, DLQ_SUFFIX);
        self.ensure_dlq_topic(&dlq_topic)?;
        let meta_key = topic_meta_key(&dlq_topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue("DLQ meta 丢失".into()))?;
        let mut meta = TopicMeta::decode(&raw)?;
        let dlq_id = meta.next_id;
        meta.next_id += 1;
        meta.count += 1;
        let dlq_msg = Message {
            id: dlq_id,
            payload: msg.payload.clone(),
            timestamp: now_ms(),
            retry_count: msg.retry_count,
            deliver_at: 0,
            expire_at: 0,
            key: msg.key.clone(),
            priority: msg.priority,
        };
        let ks = self.store.open_keyspace(&mq_keyspace_name(&dlq_topic))?;
        batch.insert(&ks, dlq_id.to_be_bytes().to_vec(), dlq_msg.encode())?;
        batch.insert(
            &self.store_meta,
            meta_key.into_bytes(),
            meta.encode().to_vec(),
        )?;
        Ok(())
    }

    /// 递减原 topic 的 meta.count（通过 WriteBatch，崩溃安全）。
    fn decrement_topic_count_batch(
        &self,
        batch: &mut crate::storage::Batch,
        topic: &str,
    ) -> Result<(), Error> {
        let meta_key = topic_meta_key(topic);
        if let Some(raw) = self.store_meta.get(meta_key.as_bytes())? {
            let mut meta = TopicMeta::decode(&raw)?;
            meta.count = meta.count.saturating_sub(1);
            batch.insert(
                &self.store_meta,
                meta_key.into_bytes(),
                meta.encode().to_vec(),
            )?;
        }
        Ok(())
    }

    /// 确保 DLQ topic 存在（幂等）。
    fn ensure_dlq_topic(&self, dlq_topic: &str) -> Result<(), Error> {
        let meta_key = topic_meta_key(&dlq_topic);
        if self.store_meta.get(meta_key.as_bytes())?.is_some() {
            return Ok(());
        }
        let meta = TopicMeta {
            next_id: 1,
            max_len: 0,
            count: 0,
        };
        self.store_meta.set(meta_key.as_bytes(), meta.encode())?;
        let _ = self.store.open_keyspace(&mq_keyspace_name(dlq_topic))?;
        Ok(())
    }
}
