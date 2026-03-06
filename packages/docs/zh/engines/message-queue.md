# 消息队列引擎

内置消息队列，支持消费者组、死信队列、优先级、延迟消息，1.6M msg/s 吞吐。

## 概述

MQ 引擎提供持久化消息队列，支持 Topic 管理、消费者组、至少一次投递、阻塞拉取、消息 Key 路由、优先级调度、延迟消息和 TTL 过期。

## 快速开始

```rust
let db = Talon::open("./data")?;

db.mq()?.create_topic("events", 0)?;
let msg_id = db.mq()?.publish("events", b"user_login")?;

db.mq()?.subscribe("events", "analytics")?;
let msgs = db.mq()?.poll("events", "analytics", "worker1", 10)?;

for msg in &msgs {
    db.mq()?.ack("events", "analytics", "worker1", msg.id)?;
}
```

## API 参考

### Topic 管理

```rust
pub fn create_topic(&self, topic: &str, max_len: u64) -> Result<(), Error>  // 0 = 无限制
pub fn delete_topic(&self, topic: &str) -> Result<(), Error>
pub fn drop_topic(&self, topic: &str) -> Result<(), Error>  // 删除 topic 及所有数据
pub fn list_topics(&self) -> Result<Vec<String>, Error>
pub fn describe_topic(&self, topic: &str) -> Result<TopicInfo, Error>
pub fn set_topic_ttl(&self, topic: &str, ttl_ms: u64) -> Result<(), Error>
pub fn get_topic_ttl(&self, topic: &str) -> Result<u64, Error>
pub fn len(&self, topic: &str) -> Result<u64, Error>  // 当前消息数
pub fn purge_topic(&self, topic: &str) -> Result<u64, Error>  // 清空消息
```

### 发布

```rust
pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<u64, Error>
pub fn publish_batch(&self, topic: &str, payloads: &[&[u8]]) -> Result<Vec<u64>, Error>
pub fn publish_with_key(&self, topic: &str, payload: &[u8], key: &str) -> Result<u64, Error>
pub fn publish_delayed(&self, topic: &str, payload: &[u8], delay_ms: u64) -> Result<u64, Error>
pub fn publish_with_priority(&self, topic: &str, payload: &[u8], priority: u8) -> Result<u64, Error>
pub fn publish_with_ttl(&self, topic: &str, payload: &[u8], ttl_ms: u64) -> Result<u64, Error>
pub fn publish_advanced(&self, topic: &str, payload: &[u8], key: Option<&str>, delay_ms: Option<u64>, ttl_ms: Option<u64>, priority: Option<u8>) -> Result<u64, Error>
```

| 函数 | 说明 |
|------|------|
| `publish` | 基本发布 |
| `publish_batch` | 批量发布（单次 WriteBatch） |
| `publish_with_key` | 带路由 Key 的发布 |
| `publish_delayed` | 延迟投递（最大 7 天） |
| `publish_with_priority` | 优先级（0-9，0 最高） |
| `publish_with_ttl` | 带消息级 TTL |
| `publish_advanced` | 全功能发布 |

### 消费

```rust
pub fn subscribe(&self, topic: &str, group: &str) -> Result<(), Error>
pub fn unsubscribe(&self, topic: &str, group: &str) -> Result<(), Error>
pub fn list_subscriptions(&self, topic: &str) -> Result<Vec<String>, Error>
pub fn poll(&self, topic: &str, group: &str, consumer: &str, count: usize) -> Result<Vec<Message>, Error>
pub fn poll_block(&self, topic: &str, group: &str, consumer: &str, count: usize, block_ms: u64) -> Result<Vec<Message>, Error>
pub fn poll_with_filter(&self, topic: &str, group: &str, consumer: &str, count: usize, key_filter: &str) -> Result<Vec<Message>, Error>
pub fn ack(&self, topic: &str, group: &str, consumer: &str, message_id: u64) -> Result<(), Error>
pub fn nack(&self, topic: &str, group: &str, consumer: &str, message_id: u64) -> Result<(), Error>
```

### 死信队列

```rust
pub fn set_max_retries(&self, topic: &str, max_retries: u32) -> Result<(), Error>  // 默认 3 次
pub fn poll_dlq(&self, topic: &str, group: &str, consumer: &str, count: usize) -> Result<Vec<Message>, Error>
```
超过最大重试次数的消息自动进入死信队列（`{topic}_dlq`）。

### 消费者组信息

```rust
pub fn describe_consumer_group(&self, topic: &str, group: &str) -> Result<ConsumerGroupInfo, Error>
pub fn reset_consumer_offset(&self, topic: &str, group: &str, consumer: &str, offset: u64) -> Result<(), Error>
```

### 消息结构

```rust
pub struct Message {
    pub id: u64,              // 唯一 ID（单调递增）
    pub payload: Vec<u8>,     // 消息体
    pub timestamp: i64,       // 发布时间（毫秒）
    pub retry_count: u32,     // Nack 重试次数
    pub deliver_at: i64,      // 延迟投递时间戳
    pub expire_at: i64,       // 过期时间戳
    pub key: Option<String>,  // 路由 Key
    pub priority: u8,         // 0-9（0 最高）
}
```

## Kafka / RabbitMQ / Redis Streams 兼容性

### 功能对比

| 功能 | Kafka | RabbitMQ | Redis Streams | Talon MQ |
|------|-------|----------|---------------|----------|
| 发布/订阅 | ✅ | ✅ | ✅ | ✅ |
| 消费者组 | ✅ | ✅ | ✅ | ✅ |
| 消息确认 | ✅ | ✅ | ✅（`XACK`） | ✅ |
| 死信队列（DLQ） | ✅ | ✅ | ❌ | ✅ |
| 消息 TTL | ❌ | ✅ | ❌ | ✅ |
| 延迟投递 | ❌ | ✅ 插件 | ❌ | ✅ 原生 |
| 优先级队列 | ❌ | ✅ | ❌ | ✅（0-9） |
| 消息路由 Key | ✅ | ✅ | ❌ | ✅ |
| 偏移追踪 | ✅ | ❌ | ✅ | ✅ |
| 保留策略 | ✅ | ✅ | ✅（`MAXLEN`） | ✅ |
| 分区 | ✅ | ❌ | ❌ | ❌ |
| 复制 | ✅ | ✅ | ✅ | ⚠️ Primary-Replica |
| 精确一次语义 | ✅ | ❌ | ❌ | ❌ |
| 嵌入式模式 | ❌ | ❌ | ❌ | ✅ |
| 单二进制 | ❌（JVM） | ❌（Erlang） | ❌（Redis） | ✅ |

### Talon 独有特性

- **嵌入式 MQ** — 进程内消息传递，无网络开销
- **多引擎融合** — MQ + SQL 联合、MQ + Vector 语义路由
- **优先级 + 延迟 + TTL** — 三合一，无需插件
- **DLQ 内置** — 最大重试后自动转入死信队列

## 性能

| 基准测试 | 结果 |
|----------|------|
| 发布（1M 消息） | 1,611K msg/s |
| 拉取 + 确认 | ~500K msg/s |
