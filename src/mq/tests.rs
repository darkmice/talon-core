/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
use super::*;
use crate::storage::Store;

#[test]
fn mq_create_publish_poll_ack() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    let id1 = mq.publish("tasks", b"task1").unwrap();
    let id2 = mq.publish("tasks", b"task2").unwrap();
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    let msgs = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].payload, b"task1");
    assert_eq!(msgs[1].payload, b"task2");

    mq.ack("tasks", "g1", "c1", id1).unwrap();
    let msgs2 = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert!(msgs2.is_empty());

    mq.ack("tasks", "g1", "c1", id2).unwrap();
    let msgs3 = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert!(msgs3.is_empty());
}

#[test]
fn mq_maxlen_trim() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("log", 3).unwrap();
    for i in 0..5 {
        mq.publish("log", format!("msg{}", i).as_bytes()).unwrap();
    }
    let len = mq.len("log").unwrap();
    assert_eq!(len, 3);
}

#[test]
fn mq_len_and_drop() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t1", 0).unwrap();
    mq.publish("t1", b"a").unwrap();
    assert_eq!(mq.len("t1").unwrap(), 1);

    mq.drop_topic("t1").unwrap();
    assert!(mq.publish("t1", b"b").is_err());
}

#[test]
fn mq_multiple_consumer_groups() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    mq.publish("events", b"e1").unwrap();

    let msgs_g1 = mq.poll("events", "group1", "c1", 10).unwrap();
    let msgs_g2 = mq.poll("events", "group2", "c1", 10).unwrap();
    assert_eq!(msgs_g1.len(), 1);
    assert_eq!(msgs_g2.len(), 1);
}

#[test]
fn mq_poll_block_returns_immediately_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.publish("t", b"msg1").unwrap();

    let start = std::time::Instant::now();
    let msgs = mq.poll_block("t", "g", "c", 10, 1000).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"msg1");
    assert!(elapsed.as_millis() < 200, "应立即返回，耗时 {:?}", elapsed);
}

#[test]
fn mq_poll_block_timeout_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();

    let start = std::time::Instant::now();
    let msgs = mq.poll_block("t", "g", "c", 10, 100).unwrap();
    let elapsed = start.elapsed();
    assert!(msgs.is_empty());
    assert!(
        elapsed.as_millis() >= 80,
        "应阻塞约 100ms，实际 {:?}",
        elapsed
    );
}

#[test]
fn mq_poll_block_zero_equals_poll() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();

    let start = std::time::Instant::now();
    let msgs = mq.poll_block("t", "g", "c", 10, 0).unwrap();
    let elapsed = start.elapsed();
    assert!(msgs.is_empty());
    assert!(elapsed.as_millis() < 50, "block_ms=0 应立即返回");
}

// ── M30: SUBSCRIBE 测试 ──

#[test]
fn mq_subscribe_and_list() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();

    // 订阅两个消费者组
    mq.subscribe("events", "analytics").unwrap();
    mq.subscribe("events", "logging").unwrap();

    let subs = mq.list_subscriptions("events").unwrap();
    assert_eq!(subs.len(), 2);
    assert!(subs.contains(&"analytics".to_string()));
    assert!(subs.contains(&"logging".to_string()));

    // 幂等：重复订阅不报错
    mq.subscribe("events", "analytics").unwrap();
    let subs2 = mq.list_subscriptions("events").unwrap();
    assert_eq!(subs2.len(), 2);
}

#[test]
fn mq_subscribe_nonexistent_topic_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    let result = mq.subscribe("no_such_topic", "g1");
    assert!(result.is_err());
}

#[test]
fn mq_unsubscribe_cleans_consumer_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    mq.subscribe("tasks", "workers").unwrap();
    mq.publish("tasks", b"job1").unwrap();

    // 消费一条消息，建立 consumer 状态
    let msgs = mq.poll("tasks", "workers", "w1", 10).unwrap();
    assert_eq!(msgs.len(), 1);

    // 取消订阅：清理 consumer 状态
    mq.unsubscribe("tasks", "workers").unwrap();

    let subs = mq.list_subscriptions("tasks").unwrap();
    assert!(subs.is_empty());

    // 重新订阅后，consumer 状态已重置，应能重新消费
    mq.subscribe("tasks", "workers").unwrap();
    let msgs2 = mq.poll("tasks", "workers", "w1", 10).unwrap();
    assert_eq!(msgs2.len(), 1);
}

#[test]
fn mq_drop_topic_cleans_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();
    mq.subscribe("t", "g2").unwrap();

    mq.drop_topic("t").unwrap();

    // 重新创建同名 topic，订阅应为空
    mq.create_topic("t", 0).unwrap();
    let subs = mq.list_subscriptions("t").unwrap();
    assert!(subs.is_empty());
}

// ── M79: NACK + DLQ 测试 ──

#[test]
fn mq_nack_requeue_within_retries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("jobs", 0).unwrap();
    let id = mq.publish("jobs", b"task1").unwrap();

    // 消费
    let msgs = mq.poll("jobs", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].retry_count, 0);

    // nack requeue=true，应重新入队
    mq.nack("jobs", "g1", "c1", id, true).unwrap();

    // 再次消费，retry_count 应为 1
    let msgs2 = mq.poll("jobs", "g1", "c1", 10).unwrap();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].id, id);
    assert_eq!(msgs2[0].retry_count, 1);
    assert_eq!(msgs2[0].payload, b"task1");
}

#[test]
fn mq_nack_exceeds_max_retries_moves_to_dlq() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("jobs", 0).unwrap();
    mq.set_max_retries("jobs", 2).unwrap();
    let id = mq.publish("jobs", b"fail_task").unwrap();

    // 第 1 次消费 + nack
    let _ = mq.poll("jobs", "g1", "c1", 10).unwrap();
    mq.nack("jobs", "g1", "c1", id, true).unwrap();

    // 第 2 次消费 + nack（retry_count=1 → 2，达到 max_retries=2，移入 DLQ）
    let _ = mq.poll("jobs", "g1", "c1", 10).unwrap();
    mq.nack("jobs", "g1", "c1", id, true).unwrap();

    // 原 topic 应无消息可消费
    let msgs = mq.poll("jobs", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty());

    // DLQ 应有一条消息
    let dlq_msg = mq.poll_dlq("jobs", "g1", "c1").unwrap();
    assert!(dlq_msg.is_some());
    let m = dlq_msg.unwrap();
    assert_eq!(m.payload, b"fail_task");
    assert_eq!(m.retry_count, 2);
}

#[test]
fn mq_nack_no_requeue_direct_dlq() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    let id = mq.publish("events", b"bad_event").unwrap();

    let _ = mq.poll("events", "g1", "c1", 10).unwrap();
    // requeue=false，直接进 DLQ
    mq.nack("events", "g1", "c1", id, false).unwrap();

    // 原 topic 无消息
    let msgs = mq.poll("events", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty());

    // DLQ 有消息，retry_count 保持 0
    let dlq_msg = mq.poll_dlq("events", "g1", "c1").unwrap();
    assert!(dlq_msg.is_some());
    assert_eq!(dlq_msg.unwrap().retry_count, 0);
}

#[test]
fn mq_poll_dlq_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // DLQ 尚未创建，应返回 None
    let result = mq.poll_dlq("t", "g1", "c1").unwrap();
    assert!(result.is_none());
}

#[test]
fn mq_set_max_retries_nonexistent_topic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    let result = mq.set_max_retries("no_such", 5);
    assert!(result.is_err());
}

#[test]
fn mq_nack_nonexistent_message() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    let result = mq.nack("t", "g1", "c1", 999, true);
    assert!(result.is_err());
}

#[test]
fn mq_default_max_retries_is_3() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    let id = mq.publish("t", b"msg").unwrap();

    // 默认 max_retries=3，nack 3 次后进 DLQ
    for _ in 0..3 {
        let _ = mq.poll("t", "g1", "c1", 10).unwrap();
        mq.nack("t", "g1", "c1", id, true).unwrap();
    }

    // 第 3 次 nack 后 retry_count=3 >= max_retries=3，应进 DLQ
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty());

    let dlq = mq.poll_dlq("t", "g1", "c1").unwrap();
    assert!(dlq.is_some());
    assert_eq!(dlq.unwrap().retry_count, 3);
}

// ── M82: purge_topic 测试 ──

#[test]
fn mq_purge_topic_clears_messages() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.publish("t", b"a").unwrap();
    mq.publish("t", b"b").unwrap();
    mq.publish("t", b"c").unwrap();
    assert_eq!(mq.len("t").unwrap(), 3);

    let purged = mq.purge_topic("t").unwrap();
    assert_eq!(purged, 3);
    assert_eq!(mq.len("t").unwrap(), 0);

    // purge 后无消息可消费
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty());
}

#[test]
fn mq_purge_preserves_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();
    mq.subscribe("t", "g2").unwrap();
    mq.publish("t", b"msg").unwrap();

    mq.purge_topic("t").unwrap();

    // 订阅关系保留
    let subs = mq.list_subscriptions("t").unwrap();
    assert_eq!(subs.len(), 2);
}

#[test]
fn mq_purge_resets_consumer_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.publish("t", b"old1").unwrap();
    mq.publish("t", b"old2").unwrap();

    // 消费建立 consumer 状态
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 2);
    mq.ack("t", "g1", "c1", msgs[0].id).unwrap();

    // purge 清空
    mq.purge_topic("t").unwrap();

    // 发布新消息，id 应该从 next_id 继续（不冲突）
    let new_id = mq.publish("t", b"new1").unwrap();
    assert!(new_id > 2); // next_id 保持递增

    // consumer 应能消费新消息（状态已重置）
    let msgs2 = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].payload, b"new1");
}

#[test]
fn mq_purge_empty_topic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    let purged = mq.purge_topic("t").unwrap();
    assert_eq!(purged, 0);
}

#[test]
fn mq_purge_nonexistent_topic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    let result = mq.purge_topic("no_such");
    assert!(result.is_err());
}

#[test]
fn mq_describe_topic_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("info", 1000).unwrap();
    mq.publish("info", b"msg1").unwrap();
    mq.publish("info", b"msg2").unwrap();
    mq.subscribe("info", "g1").unwrap();
    mq.subscribe("info", "g2").unwrap();

    let info = mq.describe_topic("info").unwrap();
    assert_eq!(info.name, "info");
    assert_eq!(info.message_count, 2);
    assert_eq!(info.subscriber_count, 2);
    assert_eq!(info.max_len, 1000);
}

#[test]
fn mq_describe_topic_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("empty", 0).unwrap();
    let info = mq.describe_topic("empty").unwrap();
    assert_eq!(info.message_count, 0);
    assert_eq!(info.subscriber_count, 0);
    assert_eq!(info.max_len, 0);
}

#[test]
fn mq_describe_topic_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    assert!(mq.describe_topic("nope").is_err());
}

// ── M99: describeConsumerGroup ──

#[test]
fn mq_describe_consumer_group_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    mq.subscribe("events", "grp1").unwrap();
    mq.publish("events", b"msg1").unwrap();
    mq.publish("events", b"msg2").unwrap();

    // consumer-a poll 2 条，ack 1 条
    let msgs = mq.poll("events", "grp1", "consumer-a", 2).unwrap();
    assert_eq!(msgs.len(), 2);
    mq.ack("events", "grp1", "consumer-a", msgs[0].id).unwrap();

    // consumer-b poll 1 条，不 ack
    let _msgs_b = mq.poll("events", "grp1", "consumer-b", 1).unwrap();

    let info = mq.describe_consumer_group("events", "grp1").unwrap();
    assert_eq!(info.group, "grp1");
    assert_eq!(info.consumers.len(), 2);

    let ca = info
        .consumers
        .iter()
        .find(|c| c.consumer == "consumer-a")
        .unwrap();
    assert_eq!(ca.acked_id, msgs[0].id);
    assert_eq!(ca.pending_count, 1); // msg2 still pending

    let cb = info
        .consumers
        .iter()
        .find(|c| c.consumer == "consumer-b")
        .unwrap();
    assert_eq!(cb.pending_count, 1);
}

#[test]
fn mq_describe_consumer_group_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    mq.subscribe("events", "grp1").unwrap();

    let info = mq.describe_consumer_group("events", "grp1").unwrap();
    assert_eq!(info.group, "grp1");
    assert!(info.consumers.is_empty()); // 无消费者活动过
}

// ── M100: resetConsumerOffset ──

#[test]
fn mq_reset_consumer_offset() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    mq.subscribe("events", "grp1").unwrap();
    let id1 = mq.publish("events", b"msg1").unwrap();
    let id2 = mq.publish("events", b"msg2").unwrap();
    let id3 = mq.publish("events", b"msg3").unwrap();

    // 消费全部并 ack
    let msgs = mq.poll("events", "grp1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 3);
    for m in &msgs {
        mq.ack("events", "grp1", "c1", m.id).unwrap();
    }

    // 重置 offset 到 id1（重新消费 id2, id3）
    mq.reset_consumer_offset("events", "grp1", "c1", id1)
        .unwrap();

    let msgs2 = mq.poll("events", "grp1", "c1", 10).unwrap();
    assert_eq!(msgs2.len(), 2);
    assert_eq!(msgs2[0].id, id2);
    assert_eq!(msgs2[1].id, id3);
    let _ = (id1, id2, id3); // suppress unused warnings
}

#[test]
fn mq_reset_consumer_offset_to_zero() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    mq.subscribe("events", "grp1").unwrap();
    mq.publish("events", b"msg1").unwrap();
    mq.publish("events", b"msg2").unwrap();

    // 消费并 ack 全部
    let msgs = mq.poll("events", "grp1", "c1", 10).unwrap();
    for m in &msgs {
        mq.ack("events", "grp1", "c1", m.id).unwrap();
    }

    // 重置到 0 → 重新消费全部
    mq.reset_consumer_offset("events", "grp1", "c1", 0).unwrap();
    let msgs2 = mq.poll("events", "grp1", "c1", 10).unwrap();
    assert_eq!(msgs2.len(), 2);
}

// ── M119: 延迟消息 (Delayed Message) 测试 ──

#[test]
fn mq_publish_delayed_not_visible_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    // 延迟 10 秒（远大于测试执行时间）
    let id = mq
        .publish_delayed("tasks", b"delayed_task", 10_000)
        .unwrap();
    assert!(id > 0);

    // 立即 poll 应拿不到延迟消息
    let msgs = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty(), "延迟消息不应立即可见");
}

#[test]
fn mq_publish_delayed_zero_is_immediate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    // delay_ms=0 等同于立即发布
    let id = mq.publish_delayed("tasks", b"immediate", 0).unwrap();
    assert!(id > 0);

    let msgs = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"immediate");
    assert_eq!(msgs[0].deliver_at, 0);
}

#[test]
fn mq_publish_delayed_becomes_visible_after_time() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    // 延迟 100ms
    let _id = mq.publish_delayed("tasks", b"soon", 100).unwrap();

    // 立即 poll 应为空
    let msgs = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty());

    // 等待 150ms 后应可见
    std::thread::sleep(std::time::Duration::from_millis(150));
    let msgs2 = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].payload, b"soon");
}

#[test]
fn mq_delayed_mixed_with_immediate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    // 先发一条立即消息
    mq.publish("tasks", b"now1").unwrap();
    // 再发一条延迟 10s 消息
    mq.publish_delayed("tasks", b"later", 10_000).unwrap();
    // 再发一条立即消息
    mq.publish("tasks", b"now2").unwrap();

    // poll 应只拿到 2 条立即消息，跳过延迟消息
    let msgs = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].payload, b"now1");
    assert_eq!(msgs[1].payload, b"now2");
}

#[test]
fn mq_publish_delayed_exceeds_max_delay() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    // 超过 7 天上限
    let result = mq.publish_delayed("tasks", b"too_late", 604_800_001);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("延迟时间超出上限"),
        "错误信息: {}",
        err_msg
    );
}

#[test]
fn mq_publish_delayed_nonexistent_topic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    let result = mq.publish_delayed("no_such", b"msg", 1000);
    assert!(result.is_err());
}

#[test]
fn mq_delayed_message_deliver_at_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    let _id = mq.publish_delayed("tasks", b"check_field", 50).unwrap();

    // 等待消息到期
    std::thread::sleep(std::time::Duration::from_millis(80));
    let msgs = mq.poll("tasks", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    // deliver_at 应大于 0（非立即投递）
    assert!(msgs[0].deliver_at > 0, "deliver_at 应为正数");
    // deliver_at 应大于 timestamp（延迟后投递）
    assert!(
        msgs[0].deliver_at > msgs[0].timestamp,
        "deliver_at({}) 应大于 timestamp({})",
        msgs[0].deliver_at,
        msgs[0].timestamp
    );
}

#[test]
fn mq_delayed_with_poll_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("tasks", 0).unwrap();
    // 延迟 100ms
    mq.publish_delayed("tasks", b"delayed_block", 100).unwrap();

    // poll_block 200ms 应在延迟到期后拿到消息
    let start = std::time::Instant::now();
    let msgs = mq.poll_block("tasks", "g1", "c1", 10, 300).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"delayed_block");
    // 应在 100ms 左右拿到（不是立即）
    assert!(
        elapsed.as_millis() >= 80,
        "应等待延迟到期，实际 {:?}",
        elapsed
    );
}

#[test]
fn mq_delayed_backward_compat_old_messages() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // 普通 publish 的消息 deliver_at 应为 0
    mq.publish("t", b"old_style").unwrap();

    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].deliver_at, 0, "普通消息 deliver_at 应为 0");
}

// ── M124: 消息 TTL（自动过期）测试 ──

#[test]
fn mq_publish_with_ttl_expires() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // TTL 100ms
    let _id = mq.publish_with_ttl("t", b"short_lived", 100).unwrap();

    // 立即 poll 应能拿到
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"short_lived");
    assert!(msgs[0].expire_at > 0);

    // ack 后重置 consumer，等待过期
    mq.ack("t", "g1", "c1", msgs[0].id).unwrap();
    mq.reset_consumer_offset("t", "g1", "c1", 0).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(150));

    // 过期后 poll 应拿不到
    let msgs2 = mq.poll("t", "g1", "c2", 10).unwrap();
    assert!(msgs2.is_empty(), "过期消息不应被消费");
}

#[test]
fn mq_publish_with_ttl_zero_uses_topic_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // 设置 topic 默认 TTL 100ms
    mq.set_topic_ttl("t", 100).unwrap();
    assert_eq!(mq.get_topic_ttl("t").unwrap(), 100);

    // publish_with_ttl(0) 应使用 topic 默认 TTL
    let _id = mq.publish_with_ttl("t", b"default_ttl", 0).unwrap();

    // 立即 poll 应能拿到
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert!(msgs[0].expire_at > 0, "应使用 topic 默认 TTL");
}

#[test]
fn mq_publish_with_ttl_overrides_topic_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.set_topic_ttl("t", 60_000).unwrap(); // topic 默认 60s

    // per-message TTL 100ms 覆盖 topic 默认
    let _id = mq.publish_with_ttl("t", b"custom_ttl", 100).unwrap();

    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    // expire_at 应接近 now + 100ms，而非 now + 60s
    let diff = msgs[0].expire_at - msgs[0].timestamp;
    assert!(
        diff <= 200,
        "per-message TTL 应覆盖 topic 默认，diff={}",
        diff
    );
}

#[test]
fn mq_publish_advanced_delay_and_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // delay 50ms + TTL 200ms
    let _id = mq.publish_advanced("t", b"combo", 50, 200).unwrap();

    // 立即 poll：延迟未到，拿不到
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty(), "延迟消息不应立即可见");

    // 等 80ms，延迟到期但 TTL 未过期
    std::thread::sleep(std::time::Duration::from_millis(80));
    let msgs2 = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].payload, b"combo");
    assert!(msgs2[0].deliver_at > 0);
    assert!(msgs2[0].expire_at > 0);
}

#[test]
fn mq_ttl_expired_before_delay_delivers() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // delay 200ms + TTL 50ms → 消息在延迟到期前就过期了
    let _id = mq
        .publish_advanced("t", b"dead_on_arrival", 200, 50)
        .unwrap();

    // 等 250ms，延迟到期但 TTL 已过期
    std::thread::sleep(std::time::Duration::from_millis(250));
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert!(msgs.is_empty(), "TTL 过期的消息即使延迟到期也不应被消费");
}

#[test]
fn mq_set_topic_ttl_nonexistent_topic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    let result = mq.set_topic_ttl("no_such", 1000);
    assert!(result.is_err());
}

#[test]
fn mq_set_topic_ttl_zero_clears() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.set_topic_ttl("t", 5000).unwrap();
    assert_eq!(mq.get_topic_ttl("t").unwrap(), 5000);

    // 设置为 0 清除默认 TTL
    mq.set_topic_ttl("t", 0).unwrap();
    assert_eq!(mq.get_topic_ttl("t").unwrap(), 0);
}

#[test]
fn mq_no_ttl_messages_never_expire() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // 普通 publish，无 TTL
    mq.publish("t", b"forever").unwrap();

    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].expire_at, 0, "无 TTL 消息 expire_at 应为 0");
}

#[test]
fn mq_drop_topic_cleans_ttl_setting() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.set_topic_ttl("t", 5000).unwrap();
    mq.drop_topic("t").unwrap();

    // 重新创建，TTL 应为 0
    mq.create_topic("t", 0).unwrap();
    assert_eq!(mq.get_topic_ttl("t").unwrap(), 0);
}

#[test]
fn mq_dlq_messages_no_expire() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    // 发布带 TTL 的消息
    let id = mq.publish_with_ttl("t", b"will_fail", 60_000).unwrap();

    let _ = mq.poll("t", "g1", "c1", 10).unwrap();
    // 直接 nack 到 DLQ
    mq.nack("t", "g1", "c1", id, false).unwrap();

    // DLQ 消息的 expire_at 应为 0（永不过期）
    let dlq = mq.poll_dlq("t", "g1", "c1").unwrap();
    assert!(dlq.is_some());
    assert_eq!(dlq.unwrap().expire_at, 0, "DLQ 消息不应有过期时间");
}

// ── M132：消息 Key 过滤 ──────────────────────────────

#[test]
fn mq_publish_with_key_and_poll_filter() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("events", 0).unwrap();
    mq.subscribe("events", "g1").unwrap();

    // 发布带不同 key 的消息
    let id1 = mq
        .publish_with_key("events", b"for-agent-1", "agent-1")
        .unwrap();
    let _id2 = mq
        .publish_with_key("events", b"for-agent-2", "agent-2")
        .unwrap();
    let id3 = mq
        .publish_with_key("events", b"also-agent-1", "agent-1")
        .unwrap();

    // 按 key 过滤：只拿 agent-1 的消息
    let msgs = mq
        .poll_with_filter("events", "g1", "c1", 10, "agent-1")
        .unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].id, id1);
    assert_eq!(msgs[1].id, id3);
    assert_eq!(msgs[0].key.as_deref(), Some("agent-1"));
    assert_eq!(msgs[1].key.as_deref(), Some("agent-1"));
}

#[test]
fn mq_poll_filter_skips_non_matching() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 发布无 key 的消息和有 key 的消息
    mq.publish("t", b"no-key").unwrap();
    mq.publish_with_key("t", b"has-key", "mykey").unwrap();
    mq.publish("t", b"no-key-2").unwrap();

    // 按 key 过滤：只拿 mykey
    let msgs = mq.poll_with_filter("t", "g1", "c1", 10, "mykey").unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].payload, b"has-key");
    assert_eq!(msgs[0].key.as_deref(), Some("mykey"));

    // 不匹配的消息不在 pending 中，普通 poll 仍可拿到
    let msgs2 = mq.poll("t", "g1", "c2", 10).unwrap();
    // c2 从头开始，能拿到所有 3 条
    assert_eq!(msgs2.len(), 3);
}

#[test]
fn mq_publish_with_key_empty_key_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 空 key 等同于无 key
    let id = mq.publish_with_key("t", b"data", "").unwrap();
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].id, id);
    assert!(msgs[0].key.is_none());
}

#[test]
fn mq_key_preserved_in_dlq() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    let id = mq.publish_with_key("t", b"payload", "my-key").unwrap();
    let _ = mq.poll("t", "g1", "c1", 10).unwrap();
    // nack 直接到 DLQ
    mq.nack("t", "g1", "c1", id, false).unwrap();

    let dlq = mq.poll_dlq("t", "g1", "c1").unwrap();
    assert!(dlq.is_some());
    let dlq_msg = dlq.unwrap();
    assert_eq!(dlq_msg.key.as_deref(), Some("my-key"), "DLQ 应保留原始 key");
    assert_eq!(dlq_msg.payload, b"payload");
}

#[test]
fn mq_backward_compat_old_messages_no_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 普通 publish 不带 key
    mq.publish("t", b"old-format").unwrap();
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert!(msgs[0].key.is_none(), "旧消息应无 key");
}

#[test]
fn mq_publish_with_priority_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 发布不同优先级的消息：先低优先级，再高优先级
    mq.publish_with_priority("t", b"low", 9).unwrap();
    mq.publish_with_priority("t", b"high", 0).unwrap();
    mq.publish_with_priority("t", b"mid", 5).unwrap();

    // poll 应按 priority 排序：0 → 5 → 9
    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].payload, b"high");
    assert_eq!(msgs[0].priority, 0);
    assert_eq!(msgs[1].payload, b"mid");
    assert_eq!(msgs[1].priority, 5);
    assert_eq!(msgs[2].payload, b"low");
    assert_eq!(msgs[2].priority, 9);
}

#[test]
fn mq_priority_default_is_5() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 普通 publish 默认优先级 5
    mq.publish("t", b"default").unwrap();
    let msgs = mq.poll("t", "g1", "c1", 1).unwrap();
    assert_eq!(msgs[0].priority, 5);
}

#[test]
fn mq_priority_invalid_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();

    // 优先级 10 超出范围
    let err = mq.publish_with_priority("t", b"bad", 10).unwrap_err();
    assert!(err.to_string().contains("优先级超出范围"));

    // 优先级 255 超出范围
    let err = mq.publish_with_priority("t", b"bad", 255).unwrap_err();
    assert!(err.to_string().contains("优先级超出范围"));

    // 边界值 0 和 9 应成功
    mq.publish_with_priority("t", b"ok0", 0).unwrap();
    mq.publish_with_priority("t", b"ok9", 9).unwrap();
}

#[test]
fn mq_priority_same_priority_fifo() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 同优先级按 id 顺序（FIFO）
    mq.publish_with_priority("t", b"a", 3).unwrap();
    mq.publish_with_priority("t", b"b", 3).unwrap();
    mq.publish_with_priority("t", b"c", 3).unwrap();

    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].payload, b"a");
    assert_eq!(msgs[1].payload, b"b");
    assert_eq!(msgs[2].payload, b"c");
}

#[test]
fn mq_priority_mixed_with_normal() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 混合普通消息（priority=5）和优先级消息
    mq.publish("t", b"normal1").unwrap();
    mq.publish_with_priority("t", b"urgent", 1).unwrap();
    mq.publish("t", b"normal2").unwrap();
    mq.publish_with_priority("t", b"critical", 0).unwrap();

    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 4);
    assert_eq!(msgs[0].payload, b"critical"); // priority 0
    assert_eq!(msgs[1].payload, b"urgent"); // priority 1
    assert_eq!(msgs[2].payload, b"normal1"); // priority 5
    assert_eq!(msgs[3].payload, b"normal2"); // priority 5
}

#[test]
fn mq_priority_preserved_in_dlq() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 发布高优先级消息
    let id = mq.publish_with_priority("t", b"important", 1).unwrap();
    let _ = mq.poll("t", "g1", "c1", 1).unwrap();

    // nack 直接进 DLQ
    mq.nack("t", "g1", "c1", id, false).unwrap();

    // DLQ 中应保留原始优先级
    let dlq_msg = mq.poll_dlq("t", "g1", "c1").unwrap().unwrap();
    assert_eq!(dlq_msg.priority, 1);
    assert_eq!(dlq_msg.payload, b"important");
}

#[test]
fn mq_priority_encode_decode_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();

    mq.create_topic("t", 0).unwrap();
    mq.subscribe("t", "g1").unwrap();

    // 测试各种优先级的编码/解码
    for p in [0u8, 1, 3, 5, 7, 9] {
        mq.publish_with_priority("t", &[p], p).unwrap();
    }

    let msgs = mq.poll("t", "g1", "c1", 10).unwrap();
    assert_eq!(msgs.len(), 6);
    // 按优先级排序
    for msg in &msgs {
        assert_eq!(msg.priority, msg.payload[0]);
    }
}
