/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 消息队列引擎：topic 创建/发布/订阅/拉取/确认。
//! 数据按 keyspace `mq_{topic}` 存储，消费者组状态存储在 `mq_meta` keyspace。

use crate::error::Error;
use crate::storage::{Keyspace, SegmentManager, Store};

const MQ_META_KEYSPACE: &str = "mq_meta";
pub(super) const MQ_TOPIC_META_PREFIX: &str = "topic:";
pub(super) const MQ_CONSUMER_PREFIX: &str = "consumer:";
pub(super) const MQ_SUBSCRIBE_PREFIX: &str = "sub:";
pub(super) const DLQ_SUFFIX: &str = "_dlq"; // DLQ topic 后缀
pub(super) const DEFAULT_MAX_RETRIES: u32 = 3; // 默认最大重试次数
pub(super) const MQ_MAX_RETRIES_PREFIX: &str = "max_retries:"; // max_retries 元数据 key 前缀
pub(super) const MQ_TOPIC_TTL_PREFIX: &str = "ttl:"; // topic 默认 TTL 前缀
/// 阻塞拉取轮询间隔（毫秒）。
const BLOCK_POLL_INTERVAL_MS: u64 = 50;
/// 最大延迟/TTL 时间（7 天，毫秒）。
pub(super) const MAX_DELAY_MS: u64 = 604_800_000;

pub(super) fn mq_keyspace_name(topic: &str) -> String {
    let mut s = String::with_capacity(3 + topic.len());
    s.push_str("mq_");
    s.push_str(topic);
    s
}

/// 构建 topic 元数据 key（减少 format! 分配；对标 NATS 高吞吐路径）。
pub(super) fn topic_meta_key(topic: &str) -> String {
    let mut key = String::with_capacity(MQ_TOPIC_META_PREFIX.len() + topic.len());
    key.push_str(MQ_TOPIC_META_PREFIX);
    key.push_str(topic);
    key
}

/// Topic 元数据（M103：二进制编码，24 字节固定）。
pub(super) struct TopicMeta {
    next_id: u64,
    max_len: u64,
    count: u64,
}
impl TopicMeta {
    fn encode(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.next_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.max_len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.count.to_le_bytes());
        buf
    }
    fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() < 24 {
            return Err(Error::MessageQueue("TopicMeta 数据不足".into()));
        }
        Ok(TopicMeta {
            next_id: u64::from_le_bytes(raw[0..8].try_into().unwrap()),
            max_len: u64::from_le_bytes(raw[8..16].try_into().unwrap()),
            count: u64::from_le_bytes(raw[16..24].try_into().unwrap()),
        })
    }
}

/// 消费者组状态（M103：二进制编码）。
pub(super) struct ConsumerState {
    pub(super) acked_id: u64,
    pub(super) pending: Vec<u64>,
    /// 曾经 ack 过的最高消息 ID（乱序 ack 时 acked_id 不前进，此字段记录真实上界）。
    pub(super) max_acked_id: u64,
}
impl ConsumerState {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4 + self.pending.len() * 8 + 8);
        buf.extend_from_slice(&self.acked_id.to_le_bytes());
        buf.extend_from_slice(&(self.pending.len() as u32).to_le_bytes());
        for &id in &self.pending {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        buf.extend_from_slice(&self.max_acked_id.to_le_bytes());
        buf
    }
    fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() < 12 {
            return Err(Error::MessageQueue("ConsumerState 数据不足".into()));
        }
        let acked_id = u64::from_le_bytes(raw[0..8].try_into().unwrap());
        let plen = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
        let mut pending = Vec::with_capacity(plen);
        for i in 0..plen {
            let off = 12 + i * 8;
            if off + 8 > raw.len() {
                break;
            }
            pending.push(u64::from_le_bytes(raw[off..off + 8].try_into().unwrap()));
        }
        // 向后兼容：旧格式无 max_acked_id，默认等于 acked_id
        let tail_off = 12 + plen * 8;
        let max_acked_id = if raw.len() >= tail_off + 8 {
            u64::from_le_bytes(raw[tail_off..tail_off + 8].try_into().unwrap())
        } else {
            acked_id
        };
        Ok(ConsumerState {
            acked_id,
            pending,
            max_acked_id,
        })
    }
}

/// 订阅元数据（M103：二进制编码，8 字节）。
pub(super) struct SubscriptionMeta {
    created_at: i64,
}
impl SubscriptionMeta {
    fn encode(&self) -> [u8; 8] {
        self.created_at.to_le_bytes()
    }
}

/// 消息（M103：二进制编码，20 字节头 + payload；M79：+4 字节 retry_count；M132：+key）。
#[derive(Debug, Clone)]
pub struct Message {
    /// 消息 ID（topic 内唯一，单调递增）。
    pub id: u64,
    /// 消息体（字节）。
    pub payload: Vec<u8>,
    /// 发布时间戳（毫秒）。
    pub timestamp: i64,
    /// 重试次数（nack 时递增；默认 0）。
    pub retry_count: u32,
    /// 投递时间戳（毫秒）；0 = 立即投递，>0 = 延迟到该时刻。
    pub deliver_at: i64,
    /// 过期时间戳（毫秒）；0 = 永不过期，>0 = 超过该时刻后不可见。
    pub expire_at: i64,
    /// 消息 Key（可选）；用于 poll 端按 key 过滤。
    pub key: Option<String>,
    /// 消息优先级（0-9，0 最高，默认 5）。
    pub priority: u8,
}
impl Message {
    fn encode(&self) -> Vec<u8> {
        // v5: id(8) + timestamp(8) + payload_len(4) + payload + retry_count(4) + deliver_at(8) + expire_at(8) + key_len(2) + key_bytes + priority(1)
        let key_bytes = self.key.as_deref().unwrap_or("");
        let mut buf = Vec::with_capacity(43 + self.payload.len() + key_bytes.len());
        buf.extend_from_slice(&self.id.to_le_bytes());
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf.extend_from_slice(&self.retry_count.to_le_bytes());
        buf.extend_from_slice(&self.deliver_at.to_le_bytes());
        buf.extend_from_slice(&self.expire_at.to_le_bytes());
        buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(key_bytes.as_bytes());
        buf.push(self.priority);
        buf
    }
    fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() < 20 {
            return Err(Error::MessageQueue("Message 数据不足".into()));
        }
        let id = u64::from_le_bytes(raw[0..8].try_into().unwrap());
        let timestamp = i64::from_le_bytes(raw[8..16].try_into().unwrap());
        let plen = u32::from_le_bytes(raw[16..20].try_into().unwrap()) as usize;
        let payload = if raw.len() >= 20 + plen {
            raw[20..20 + plen].to_vec()
        } else {
            raw[20..].to_vec()
        };
        // 向后兼容：旧格式无 retry_count（20 + plen 字节），默认 0
        let retry_count = if raw.len() >= 20 + plen + 4 {
            u32::from_le_bytes(raw[20 + plen..24 + plen].try_into().unwrap_or([0; 4]))
        } else {
            0
        };
        // 向后兼容：旧格式无 deliver_at，默认 0（立即投递）
        let deliver_at = if raw.len() >= 24 + plen + 8 {
            i64::from_le_bytes(raw[24 + plen..32 + plen].try_into().unwrap_or([0; 8]))
        } else {
            0
        };
        Ok(Message {
            id,
            payload,
            timestamp,
            retry_count,
            deliver_at,
            expire_at: if raw.len() >= 32 + plen + 8 {
                i64::from_le_bytes(raw[32 + plen..40 + plen].try_into().unwrap_or([0; 8]))
            } else {
                0
            },
            // v4：向后兼容，旧格式无 key 字段
            key: if raw.len() >= 40 + plen + 2 {
                let klen =
                    u16::from_le_bytes(raw[40 + plen..42 + plen].try_into().unwrap_or([0; 2]))
                        as usize;
                if klen > 0 && raw.len() >= 42 + plen + klen {
                    std::str::from_utf8(&raw[42 + plen..42 + plen + klen])
                        .ok()
                        .map(|s| s.to_string())
                } else {
                    None
                }
            } else {
                None
            },
            // v5：向后兼容，旧格式无 priority 字段，默认 5
            priority: {
                let key_end = if raw.len() >= 40 + plen + 2 {
                    let klen =
                        u16::from_le_bytes(raw[40 + plen..42 + plen].try_into().unwrap_or([0; 2]))
                            as usize;
                    42 + plen + klen
                } else {
                    40 + plen
                };
                if raw.len() > key_end {
                    raw[key_end]
                } else {
                    5
                }
            },
        })
    }
}

/// Topic 详情信息（`describe_topic` 返回值）。
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic 名称。
    pub name: String,
    /// 当前消息数量。
    pub message_count: u64,
    /// 订阅组数量。
    pub subscriber_count: usize,
    /// 最大消息容量（0 = 无限制）。
    pub max_len: u64,
}

/// 消费组详情（`describe_consumer_group` 返回值）。
#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    /// 消费组名称。
    pub group: String,
    /// 组内消费者列表。
    pub consumers: Vec<ConsumerInfo>,
}

/// 单个消费者详情。
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    /// 消费者标识。
    pub consumer: String,
    /// 已确认的最大消息 ID。
    pub acked_id: u64,
    /// 待处理（pending）消息数量。
    pub pending_count: usize,
}

/// 消息队列引擎；内部持有 Store 引用，所有操作无需外部传入 store。
/// M4：通过 SegmentManager 追踪热 topic 消费者状态。
pub struct MqEngine {
    pub(super) store: Store,
    pub(super) store_meta: Keyspace,
    /// 统一段管理器。
    segments: SegmentManager,
}

impl MqEngine {
    /// 打开消息队列引擎。
    pub fn open(store: &Store) -> Result<Self, Error> {
        let store_meta = store.open_keyspace(MQ_META_KEYSPACE)?;
        let segments = store.segment_manager().clone();
        Ok(MqEngine {
            store: store.clone(),
            store_meta,
            segments,
        })
    }

    /// 创建 topic；max_len=0 表示无限制。
    pub fn create_topic(&self, topic: &str, max_len: u64) -> Result<(), Error> {
        let meta_key = topic_meta_key(topic);
        let meta = TopicMeta {
            next_id: 1,
            max_len,
            count: 0,
        };
        self.store_meta.set(meta_key.as_bytes(), meta.encode())?;
        let _ = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        Ok(())
    }

    /// 发布消息到 topic；返回 message_id。
    /// 读取-修改-写入 next_id 在 Talon 层 Mutex 保护下原子执行。
    pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<u64, Error> {
        let ids = self.publish_batch(topic, &[payload])?;
        Ok(ids[0])
    }

    /// M103：批量发布消息到 topic；返回分配的 message_id 列表。
    /// N 条消息合入 1 个 WriteBatch commit，消除 N-1 次 journal fsync。
    pub fn publish_batch(&self, topic: &str, payloads: &[&[u8]]) -> Result<Vec<u64>, Error> {
        if payloads.is_empty() {
            return Ok(vec![]);
        }
        let meta_key = topic_meta_key(topic);
        let raw = self
            .store_meta
            .get(meta_key.as_bytes())?
            .ok_or_else(|| Error::MessageQueue(format!("topic 不存在: {}", topic)))?;
        let mut meta = TopicMeta::decode(&raw)?;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let ts = now_ms();
        let mut batch = self.store.batch();
        let mut ids = Vec::with_capacity(payloads.len());
        for payload in payloads {
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
                priority: 5,
            };
            batch.insert(&ks, msg_id.to_be_bytes().to_vec(), msg.encode())?;
            ids.push(msg_id);
        }
        batch.insert(
            &self.store_meta,
            meta_key.into_bytes(),
            meta.encode().to_vec(),
        )?;
        batch.commit()?;
        if meta.max_len > 0 {
            self.trim_topic(topic, meta.max_len)?;
        }
        Ok(ids)
    }

    /// 拉取消息：返回 consumer 尚未确认的消息，最多 count 条。
    /// 流式 range scan（key+value 一次读取），O(count) 时间+内存，消除全量 key 加载和 N+1 查询。
    pub fn poll(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<Vec<Message>, Error> {
        let state = self.get_consumer_state(topic, group, consumer)?;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;

        let start_id = state.acked_id + 1;
        let start_key = start_id.to_be_bytes();
        let mut candidates = Vec::new();
        let mut scan_err: Option<Error> = None;
        let mut expired_keys: Vec<[u8; 8]> = Vec::new();

        // 使用 HashSet 加速 pending 检查（从 O(n) 变 O(1)）
        let pending_set: std::collections::HashSet<u64> = state.pending.iter().copied().collect();

        // 多收集候选消息以支持优先级排序（扫描 count*4 条候选）
        let scan_limit = count.saturating_mul(4).max(count);
        let now = now_ms();
        ks.for_each_kv_range(&start_key, &u64::MAX.to_be_bytes(), |key, raw| {
            if key.len() != 8 {
                return true;
            }
            let id = u64::from_be_bytes(key.try_into().unwrap());
            if id < start_id || pending_set.contains(&id) {
                return true;
            }
            match Message::decode(raw) {
                Ok(msg) => {
                    // M124：跳过已过期的消息（标记惰性删除，单次上限 128 条）
                    if msg.expire_at > 0 && msg.expire_at <= now {
                        if expired_keys.len() < 128 {
                            expired_keys.push(id.to_be_bytes());
                        }
                        return true;
                    }
                    // 跳过尚未到投递时间的延迟消息
                    if msg.deliver_at > 0 && msg.deliver_at > now {
                        return true;
                    }
                    candidates.push(msg);
                    candidates.len() < scan_limit
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }

        // M124：惰性删除过期消息并更新 meta count
        if !expired_keys.is_empty() {
            for key in &expired_keys {
                let _ = ks.delete(key);
            }
            let meta_key = topic_meta_key(topic);
            if let Ok(Some(raw)) = self.store_meta.get(meta_key.as_bytes()) {
                if let Ok(mut meta) = TopicMeta::decode(&raw) {
                    meta.count = meta.count.saturating_sub(expired_keys.len() as u64);
                    let _ = self.store_meta.set(meta_key.as_bytes(), meta.encode());
                }
            }
        }

        // M138：按 (priority, id) 排序，优先级小的先消费，同优先级按 id 顺序
        candidates.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.id.cmp(&b.id)));
        candidates.truncate(count);

        let mut new_pending = state.pending.clone();
        for msg in &candidates {
            new_pending.push(msg.id);
        }

        let new_state = ConsumerState {
            acked_id: state.acked_id,
            pending: new_pending,
            max_acked_id: state.max_acked_id,
        };
        self.put_consumer_state(topic, group, consumer, &new_state)?;

        Ok(candidates)
    }

    /// 阻塞拉取消息：无消息时 sleep/retry 直到超时或拿到消息。
    ///
    /// `block_ms` 为最大阻塞时间（毫秒），0 表示不阻塞（等同于 `poll`）。
    /// 内部以 50ms 间隔轮询，每次迭代独立调用 `poll`。
    pub fn poll_block(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        count: usize,
        block_ms: u64,
    ) -> Result<Vec<Message>, Error> {
        if block_ms == 0 {
            return self.poll(topic, group, consumer, count);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(block_ms);
        loop {
            let msgs = self.poll(topic, group, consumer, count)?;
            if !msgs.is_empty() {
                return Ok(msgs);
            }
            if std::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let sleep_dur = remaining.min(std::time::Duration::from_millis(BLOCK_POLL_INTERVAL_MS));
            std::thread::sleep(sleep_dur);
        }
    }

    /// 发布带 Key 的消息到 topic；返回 message_id。
    ///
    /// Key 用于消费端按 key 过滤（`poll_with_filter`）。
    /// AI 场景：Agent 间按目标 ID 过滤、RAG 管道按文档类型过滤。
    pub fn publish_with_key(&self, topic: &str, payload: &[u8], key: &str) -> Result<u64, Error> {
        if key.len() > u16::MAX as usize {
            return Err(Error::MessageQueue("消息 key 长度超出上限 (65535)".into()));
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
            key: if key.is_empty() {
                None
            } else {
                Some(key.to_string())
            },
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

    /// 按 Key 过滤拉取消息：只返回 key 匹配的消息，最多 count 条。
    ///
    /// `key_filter` 为精确匹配；不匹配的消息被跳过（不加入 pending）。
    /// 为防止长时间扫描，内部最多扫描 `count * 10` 条消息。
    pub fn poll_with_filter(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        count: usize,
        key_filter: &str,
    ) -> Result<Vec<Message>, Error> {
        let state = self.get_consumer_state(topic, group, consumer)?;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;

        let start_id = state.acked_id + 1;
        let start_key = start_id.to_be_bytes();
        let mut candidates = Vec::new();
        let mut scan_err: Option<Error> = None;
        let mut expired_keys: Vec<[u8; 8]> = Vec::new();
        let max_scan = count.saturating_mul(10).max(10);
        let mut scanned = 0usize;

        let pending_set: std::collections::HashSet<u64> = state.pending.iter().copied().collect();

        let now = now_ms();
        ks.for_each_kv_range(&start_key, &u64::MAX.to_be_bytes(), |key, raw| {
            if key.len() != 8 {
                return true;
            }
            let id = u64::from_be_bytes(key.try_into().unwrap());
            if id < start_id || pending_set.contains(&id) {
                return true;
            }
            scanned += 1;
            if scanned > max_scan {
                return false;
            }
            match Message::decode(raw) {
                Ok(msg) => {
                    // 跳过已过期消息
                    if msg.expire_at > 0 && msg.expire_at <= now {
                        if expired_keys.len() < 128 {
                            expired_keys.push(id.to_be_bytes());
                        }
                        return true;
                    }
                    // 跳过延迟消息
                    if msg.deliver_at > 0 && msg.deliver_at > now {
                        return true;
                    }
                    // Key 过滤：不匹配则跳过（不加入 pending）
                    let msg_key = msg.key.as_deref().unwrap_or("");
                    if msg_key != key_filter {
                        return true;
                    }
                    candidates.push(msg);
                    candidates.len() < count.saturating_mul(4).max(count)
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }

        // 惰性删除过期消息
        if !expired_keys.is_empty() {
            for k in &expired_keys {
                let _ = ks.delete(k);
            }
            let meta_key = topic_meta_key(topic);
            if let Ok(Some(raw)) = self.store_meta.get(meta_key.as_bytes()) {
                if let Ok(mut meta) = TopicMeta::decode(&raw) {
                    meta.count = meta.count.saturating_sub(expired_keys.len() as u64);
                    let _ = self.store_meta.set(meta_key.as_bytes(), meta.encode());
                }
            }
        }

        // M138：按 (priority, id) 排序
        candidates.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.id.cmp(&b.id)));
        candidates.truncate(count);

        let mut new_pending = state.pending.clone();
        for msg in &candidates {
            new_pending.push(msg.id);
        }

        let new_state = ConsumerState {
            acked_id: state.acked_id,
            pending: new_pending,
            max_acked_id: state.max_acked_id,
        };
        self.put_consumer_state(topic, group, consumer, &new_state)?;

        Ok(candidates)
    }

    /// 确认消息已消费。
    ///
    /// 支持乱序 ack：`acked_id`（扫描起点）只在安全时推进，
    /// 避免跳过尚未 ack 的低位消息。
    pub fn ack(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        message_id: u64,
    ) -> Result<(), Error> {
        let mut state = self.get_consumer_state(topic, group, consumer)?;
        state.pending.retain(|&id| id != message_id);
        state.max_acked_id = state.max_acked_id.max(message_id);
        if state.pending.is_empty() {
            // 所有已投递消息均已 ack，安全推进到最高 ack 位
            state.acked_id = state.max_acked_id;
        } else {
            // 仅推进到第一个未 ack 消息之前（不跳过 pending 中的消息）
            let min_pending = state.pending.iter().copied().min().unwrap();
            state.acked_id = state.acked_id.max(min_pending.saturating_sub(1));
        }
        self.put_consumer_state(topic, group, consumer, &state)
    }

    /// MAXLEN 淘汰：保留最新的 max_len 条消息，更新 meta count。
    /// M86：使用 for_each_key_prefix + meta.count 替代全量 key 加载。
    /// Bug 37：消息删除 + meta count 更新合入同一 WriteBatch 原子提交。
    fn trim_topic(&self, topic: &str, max_len: u64) -> Result<(), Error> {
        let meta_key = topic_meta_key(topic);
        let raw = match self.store_meta.get(meta_key.as_bytes())? {
            Some(r) => r,
            None => return Ok(()),
        };
        let mut meta = TopicMeta::decode(&raw)?;
        if meta.count <= max_len {
            return Ok(());
        }
        let to_remove = (meta.count - max_len) as usize;
        let ks = self.store.open_keyspace(&mq_keyspace_name(topic))?;
        let mut oldest: Vec<Vec<u8>> = Vec::with_capacity(to_remove);
        ks.for_each_key_prefix(b"", |key| {
            if oldest.len() < to_remove {
                oldest.push(key.to_vec());
                true
            } else {
                false // 提前终止
            }
        })?;
        if !oldest.is_empty() {
            meta.count = meta.count.saturating_sub(oldest.len() as u64);
            let mut batch = self.store.batch();
            for key in &oldest {
                batch.remove(&ks, key.clone());
            }
            batch.insert(
                &self.store_meta,
                meta_key.into_bytes(),
                meta.encode().to_vec(),
            )?;
            batch.commit()?;
        }
        Ok(())
    }

    fn consumer_key(topic: &str, group: &str, consumer: &str) -> String {
        format!("{}{}:{}:{}", MQ_CONSUMER_PREFIX, topic, group, consumer)
    }

    pub(super) fn get_consumer_state(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
    ) -> Result<ConsumerState, Error> {
        let key = Self::consumer_key(topic, group, consumer);
        let raw = self.store_meta.get(key.as_bytes())?;
        match raw {
            Some(data) => ConsumerState::decode(&data),
            None => Ok(ConsumerState {
                acked_id: 0,
                pending: Vec::new(),
                max_acked_id: 0,
            }),
        }
    }

    pub(super) fn put_consumer_state(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        state: &ConsumerState,
    ) -> Result<(), Error> {
        let key = Self::consumer_key(topic, group, consumer);
        self.store_meta.set(key.as_bytes(), state.encode())
    }
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

mod admin;
mod nack;
mod publish;

#[cfg(test)]
mod tests;
