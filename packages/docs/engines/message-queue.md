# MessageQueue Engine

Built-in message queue with consumer groups, dead letter queues, priority, and 1.6M msg/s throughput.

## Overview

The MQ Engine provides a persistent message queue with topic management, consumer groups, at-least-once delivery, blocking polls, message keys, priority scheduling, delayed messages, and TTL-based expiration.

## Quick Start

```rust
let db = Talon::open("./data")?;

// Create topic
db.mq()?.create_topic("events", 0)?; // 0 = unlimited

// Publish
let msg_id = db.mq()?.publish("events", b"user_login")?;

// Subscribe & consume
db.mq()?.subscribe("events", "analytics")?;
let msgs = db.mq()?.poll("events", "analytics", "worker1", 10)?;

// Acknowledge
for msg in &msgs {
    db.mq()?.ack("events", "analytics", "worker1", msg.id)?;
}
```

## API Reference

### Topic Management

#### `create_topic`
```rust
pub fn create_topic(&self, topic: &str, max_len: u64) -> Result<(), Error>
```
Create a topic. `max_len = 0` means unlimited. Older messages are auto-trimmed when exceeded.

#### `delete_topic`
```rust
pub fn delete_topic(&self, topic: &str) -> Result<(), Error>
```

#### `list_topics`
```rust
pub fn list_topics(&self) -> Result<Vec<String>, Error>
```

#### `describe_topic`
```rust
pub fn describe_topic(&self, topic: &str) -> Result<TopicInfo, Error>
```
Returns `TopicInfo { name, message_count, subscriber_count, max_len }`.

#### `set_topic_ttl`
```rust
pub fn set_topic_ttl(&self, topic: &str, ttl_ms: u64) -> Result<(), Error>
```
Set default TTL for new messages in this topic.

### Publishing

#### `publish`
```rust
pub fn publish(&self, topic: &str, payload: &[u8]) -> Result<u64, Error>
```
Publish a message. Returns assigned message ID.

#### `publish_batch`
```rust
pub fn publish_batch(&self, topic: &str, payloads: &[&[u8]]) -> Result<Vec<u64>, Error>
```
Batch publish. Single WriteBatch commit for N messages.

#### `publish_with_key`
```rust
pub fn publish_with_key(&self, topic: &str, payload: &[u8], key: &str) -> Result<u64, Error>
```
Publish with a routing key for filtered consumption.

#### `publish_delayed`
```rust
pub fn publish_delayed(&self, topic: &str, payload: &[u8], delay_ms: u64) -> Result<u64, Error>
```
Publish with a delay. Message becomes visible after `delay_ms` milliseconds. Max: 7 days.

#### `publish_with_priority`
```rust
pub fn publish_with_priority(&self, topic: &str, payload: &[u8], priority: u8) -> Result<u64, Error>
```
Publish with priority (0-9, 0 = highest, default = 5).

### Consuming

#### `subscribe`
```rust
pub fn subscribe(&self, topic: &str, group: &str) -> Result<(), Error>
```
Register a consumer group for a topic.

#### `poll`
```rust
pub fn poll(&self, topic: &str, group: &str, consumer: &str, count: usize) -> Result<Vec<Message>, Error>
```
Pull up to `count` unacknowledged messages. Messages are added to the consumer's pending list.

#### `poll_block`
```rust
pub fn poll_block(&self, topic: &str, group: &str, consumer: &str, count: usize, block_ms: u64) -> Result<Vec<Message>, Error>
```
Blocking poll. Retries every 50ms until timeout or messages available.

#### `poll_with_filter`
```rust
pub fn poll_with_filter(&self, topic: &str, group: &str, consumer: &str, count: usize, key_filter: &str) -> Result<Vec<Message>, Error>
```
Pull only messages matching the key filter.

#### `ack`
```rust
pub fn ack(&self, topic: &str, group: &str, consumer: &str, message_id: u64) -> Result<(), Error>
```
Acknowledge a message as consumed.

#### `nack`
```rust
pub fn nack(&self, topic: &str, group: &str, consumer: &str, message_id: u64) -> Result<(), Error>
```
Negative acknowledge — re-queue for retry. Increments `retry_count`. Exceeding `max_retries` sends to DLQ.

### Additional Publishing

#### `publish_with_ttl`
```rust
pub fn publish_with_ttl(&self, topic: &str, payload: &[u8], ttl_ms: u64) -> Result<u64, Error>
```
Publish a message with per-message TTL. Expired messages are skipped during poll.

#### `publish_advanced`
```rust
pub fn publish_advanced(&self, topic: &str, payload: &[u8], key: Option<&str>, delay_ms: Option<u64>, ttl_ms: Option<u64>, priority: Option<u8>) -> Result<u64, Error>
```
Full-featured publish with all options combined.

### Subscription Management

#### `unsubscribe`
```rust
pub fn unsubscribe(&self, topic: &str, group: &str) -> Result<(), Error>
```
Remove a consumer group subscription.

#### `list_subscriptions`
```rust
pub fn list_subscriptions(&self, topic: &str) -> Result<Vec<String>, Error>
```
List all consumer groups subscribed to a topic.

### Dead Letter Queue

#### `set_max_retries`
```rust
pub fn set_max_retries(&self, topic: &str, max_retries: u32) -> Result<(), Error>
```
Set max retry count before DLQ (default: 3).

#### `poll_dlq`
```rust
pub fn poll_dlq(&self, topic: &str, group: &str, consumer: &str, count: usize) -> Result<Vec<Message>, Error>
```
Pull messages from the dead letter queue (topic name: `{topic}_dlq`).

### Topic Administration

#### `len`
```rust
pub fn len(&self, topic: &str) -> Result<u64, Error>
```
Get current message count in a topic.

#### `purge_topic`
```rust
pub fn purge_topic(&self, topic: &str) -> Result<u64, Error>
```
Delete all messages in a topic. Returns count purged.

#### `drop_topic`
```rust
pub fn drop_topic(&self, topic: &str) -> Result<(), Error>
```
Delete a topic and all its data (messages, subscriptions, consumer state).

#### `get_topic_ttl`
```rust
pub fn get_topic_ttl(&self, topic: &str) -> Result<u64, Error>
```
Get the default TTL for a topic (milliseconds). 0 = no TTL.

#### `reset_consumer_offset`
```rust
pub fn reset_consumer_offset(&self, topic: &str, group: &str, consumer: &str, offset: u64) -> Result<(), Error>
```
Reset a consumer's acknowledged offset. Used for replaying messages.

### Consumer Group Info

#### `describe_consumer_group`
```rust
pub fn describe_consumer_group(&self, topic: &str, group: &str) -> Result<ConsumerGroupInfo, Error>
```

### Message Structure

```rust
pub struct Message {
    pub id: u64,           // Unique ID (monotonic)
    pub payload: Vec<u8>,  // Message body
    pub timestamp: i64,    // Publish time (ms)
    pub retry_count: u32,  // Nack retry count
    pub deliver_at: i64,   // Delayed delivery timestamp
    pub expire_at: i64,    // Expiration timestamp
    pub key: Option<String>, // Routing key
    pub priority: u8,      // 0-9 (0 = highest)
}
```

## Kafka / RabbitMQ / Redis Streams Compatibility

### Feature Comparison

| Feature | Kafka | RabbitMQ | Redis Streams | Talon MQ |
|---------|-------|----------|---------------|----------|
| Publish / subscribe | ✅ | ✅ | ✅ | ✅ |
| Consumer groups | ✅ | ✅ | ✅ | ✅ |
| Message ACK | ✅ | ✅ | ✅ (`XACK`) | ✅ |
| Dead letter queue (DLQ) | ✅ | ✅ | ❌ | ✅ |
| Message TTL | ❌ | ✅ | ❌ | ✅ |
| Delayed delivery | ❌ | ✅ plugin | ❌ | ✅ native |
| Priority queues | ❌ | ✅ | ❌ | ✅ (0-9) |
| Message keys / routing | ✅ | ✅ | ❌ | ✅ |
| Offset tracking | ✅ | ❌ | ✅ | ✅ |
| Retention policies | ✅ | ✅ | ✅ (`MAXLEN`) | ✅ |
| Partitions | ✅ | ❌ | ❌ | ❌ |
| Replication | ✅ | ✅ | ✅ | ⚠️ Primary-Replica |
| Exactly-once semantics | ✅ | ❌ | ❌ | ❌ |
| Embedded mode | ❌ | ❌ | ❌ | ✅ |
| Single binary | ❌ (JVM) | ❌ (Erlang) | ❌ (Redis) | ✅ |

### Talon-Only Features

- **Embedded MQ** — in-process message passing, no network overhead
- **Multi-engine fusion** — MQ + SQL joins, MQ + Vector semantic routing
- **Priority + Delay + TTL** — all three in one engine, no plugins needed
- **DLQ built-in** — automatic dead letter after max retries

## Performance

| Benchmark | Result |
|-----------|--------|
| Publish (1M messages) | 1,611K msg/s |
| Poll + Ack | ~500K msg/s |
