/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序引擎单元测试。

use super::*;
use crate::storage::Store;
use std::collections::BTreeMap;

fn make_point(ts: i64, session: &str, role: &str, content: &str, tokens: i32) -> DataPoint {
    let mut tags = BTreeMap::new();
    tags.insert("session_id".to_string(), session.to_string());
    tags.insert("role".to_string(), role.to_string());
    let mut fields = BTreeMap::new();
    fields.insert("content".to_string(), content.to_string());
    fields.insert("token_count".to_string(), tokens.to_string());
    DataPoint {
        timestamp: ts,
        tags,
        fields,
    }
}

#[test]
fn ts_create_insert_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["session_id".into(), "role".into()],
        fields: vec!["content".into(), "token_count".into()],
    };
    let ts = TsEngine::create(&store, "conversations", schema).unwrap();
    ts.insert(&make_point(1000, "s1", "user", "hello", 5))
        .unwrap();
    ts.insert(&make_point(2000, "s1", "assistant", "hi", 3))
        .unwrap();
    ts.insert(&make_point(3000, "s2", "user", "bye", 4))
        .unwrap();

    let q = TsQuery {
        tag_filters: vec![("session_id".into(), "s1".into())],
        desc: false,
        ..Default::default()
    };
    let results = ts.query(&q).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].timestamp, 1000);
    assert_eq!(results[1].timestamp, 2000);
}

#[test]
fn ts_time_range_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["session_id".into()],
        fields: vec!["content".into()],
    };
    let ts = TsEngine::create(&store, "log", schema).unwrap();
    for i in 0..10 {
        let mut tags = BTreeMap::new();
        tags.insert("session_id".to_string(), "s1".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("content".to_string(), format!("msg{}", i));
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags,
            fields,
        })
        .unwrap();
    }

    let q = TsQuery {
        time_start: Some(3000),
        time_end: Some(7000),
        desc: true,
        ..Default::default()
    };
    let results = ts.query(&q).unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].timestamp, 6000);
}

#[test]
fn ts_aggregate_sum_count() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["model".into()],
        fields: vec!["token_count".into()],
    };
    let ts = TsEngine::create(&store, "usage", schema).unwrap();
    for i in 0..6 {
        let mut tags = BTreeMap::new();
        tags.insert("model".to_string(), "gpt-4".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("token_count".to_string(), "10".to_string());
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags,
            fields,
        })
        .unwrap();
    }

    let q = TsAggQuery {
        tag_filters: vec![("model".into(), "gpt-4".into())],
        time_start: None,
        time_end: None,
        field: "token_count".into(),
        func: AggFunc::Sum,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 1);
    assert!((buckets[0].value - 60.0).abs() < 0.01);
    assert_eq!(buckets[0].count, 6);

    let q2 = TsAggQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        field: "token_count".into(),
        func: AggFunc::Count,
        interval_ms: Some(3000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let buckets2 = ts.aggregate(&q2).unwrap();
    assert_eq!(buckets2.len(), 2);
}

#[test]
fn ts_open_existing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    TsEngine::create(&store, "t1", schema).unwrap();
    let ts2 = TsEngine::open(&store, "t1").unwrap();
    assert_eq!(ts2.schema().tags, vec!["k".to_string()]);
}

#[test]
fn ts_retention_policy() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["src".into()],
        fields: vec!["msg".into()],
    };
    let ts = TsEngine::create(&store, "logs", schema).unwrap();

    // 默认无保留策略
    assert_eq!(ts.get_retention().unwrap(), None);

    // 插入数据：ts=1000, 2000, 3000, 4000, 5000
    for i in 1..=5 {
        let mut tags = BTreeMap::new();
        tags.insert("src".to_string(), "app".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("msg".to_string(), format!("log{}", i));
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags,
            fields,
        })
        .unwrap();
    }

    // purge_before：删除 ts < 3000 的数据
    let deleted = ts.purge_before(3000).unwrap();
    assert_eq!(deleted, 2); // ts=1000, 2000

    let q = TsQuery {
        desc: false,
        ..Default::default()
    };
    let results = ts.query(&q).unwrap();
    assert_eq!(results.len(), 3); // ts=3000, 4000, 5000
    assert_eq!(results[0].timestamp, 3000);

    // 设置保留策略
    ts.set_retention(2000).unwrap();
    assert_eq!(ts.get_retention().unwrap(), Some(2000));

    // 清除保留策略
    ts.set_retention(0).unwrap();
    assert_eq!(ts.get_retention().unwrap(), None);

    // 重新打开后保留策略持久化
    ts.set_retention(5000).unwrap();
    let ts2 = TsEngine::open(&store, "logs").unwrap();
    assert_eq!(ts2.get_retention().unwrap(), Some(5000));
}

// ── M30: list_timeseries + TsRetentionCleaner 测试 ──

#[test]
fn ts_list_timeseries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();

    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    TsEngine::create(&store, "metrics", schema.clone()).unwrap();
    TsEngine::create(&store, "logs", schema).unwrap();

    let names = super::list_timeseries(&store).unwrap();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"metrics".to_string()));
    assert!(names.contains(&"logs".to_string()));
}

#[test]
fn ts_retention_cleaner_purges_expired() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();

    let schema = TsSchema {
        tags: vec!["src".into()],
        fields: vec!["val".into()],
    };
    let ts = TsEngine::create(&store, "data", schema).unwrap();

    // 写入一条很旧的数据（timestamp=1000，远早于当前时间）
    let mut tags = std::collections::BTreeMap::new();
    tags.insert("src".to_string(), "a".to_string());
    let mut fields = std::collections::BTreeMap::new();
    fields.insert("val".to_string(), "42".to_string());
    ts.insert(&DataPoint {
        timestamp: 1000,
        tags: tags.clone(),
        fields: fields.clone(),
    })
    .unwrap();

    // 写入一条当前时间的数据
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    ts.insert(&DataPoint {
        timestamp: now,
        tags,
        fields,
    })
    .unwrap();

    // 设置 retention = 60 秒（timestamp=1000 远超过期）
    ts.set_retention(60_000).unwrap();

    // 启动 retention cleaner，间隔 1 秒
    let _cleaner = super::start_ts_retention_cleaner(&store, 1);

    // 等待清理线程执行一次
    std::thread::sleep(std::time::Duration::from_millis(1500));

    // 验证旧数据已被清理，新数据保留
    let all = ts
        .query(&TsQuery {
            ..Default::default()
        })
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].timestamp, now);
}

// ── M74: First / Last / Stddev 聚合函数 ──

#[test]
fn ts_aggregate_first_last() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["agent".into()],
        fields: vec!["latency".into()],
    };
    let ts = TsEngine::create(&store, "calls", schema).unwrap();
    // 插入 5 个数据点，值分别为 10, 20, 30, 40, 50
    for i in 1..=5 {
        let mut tags = BTreeMap::new();
        tags.insert("agent".to_string(), "bot1".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("latency".to_string(), (i * 10).to_string());
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags,
            fields,
        })
        .unwrap();
    }

    // First: 应返回第一个值 10
    let q = TsAggQuery {
        tag_filters: vec![("agent".into(), "bot1".into())],
        time_start: None,
        time_end: None,
        field: "latency".into(),
        func: AggFunc::First,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 1);
    assert!((buckets[0].value - 10.0).abs() < 0.01);

    // Last: 应返回最后一个值 50
    let q2 = TsAggQuery {
        func: AggFunc::Last,
        ..q.clone()
    };
    let buckets2 = ts.aggregate(&q2).unwrap();
    assert_eq!(buckets2.len(), 1);
    assert!((buckets2[0].value - 50.0).abs() < 0.01);
}

#[test]
fn ts_aggregate_first_last_with_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["src".into()],
        fields: vec!["val".into()],
    };
    let ts = TsEngine::create(&store, "data", schema).unwrap();
    // 桶1: ts=0,1000,2000 值=1,2,3  桶2: ts=3000,4000 值=4,5
    for i in 0..5 {
        let mut tags = BTreeMap::new();
        tags.insert("src".to_string(), "a".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("val".to_string(), (i + 1).to_string());
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags,
            fields,
        })
        .unwrap();
    }

    let q = TsAggQuery {
        tag_filters: vec![("src".into(), "a".into())],
        time_start: None,
        time_end: None,
        field: "val".into(),
        func: AggFunc::First,
        interval_ms: Some(3000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 2);
    assert!((buckets[0].value - 1.0).abs() < 0.01); // 桶1 first=1
    assert!((buckets[1].value - 4.0).abs() < 0.01); // 桶2 first=4

    let q2 = TsAggQuery {
        func: AggFunc::Last,
        ..q.clone()
    };
    let buckets2 = ts.aggregate(&q2).unwrap();
    assert_eq!(buckets2.len(), 2);
    assert!((buckets2[0].value - 3.0).abs() < 0.01); // 桶1 last=3
    assert!((buckets2[1].value - 5.0).abs() < 0.01); // 桶2 last=5
}

#[test]
fn ts_aggregate_stddev() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "metrics", schema).unwrap();
    // 插入值: 2, 4, 4, 4, 5, 5, 7, 9 → 总体标准差 = 2.0
    let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    for (i, v) in values.iter().enumerate() {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), "srv1".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("cpu".to_string(), v.to_string());
        ts.insert(&DataPoint {
            timestamp: i as i64 * 1000,
            tags,
            fields,
        })
        .unwrap();
    }

    let q = TsAggQuery {
        tag_filters: vec![("host".into(), "srv1".into())],
        time_start: None,
        time_end: None,
        field: "cpu".into(),
        func: AggFunc::Stddev,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 1);
    // 总体标准差 = sqrt(((2-5)^2+(4-5)^2+(4-5)^2+(4-5)^2+(5-5)^2+(5-5)^2+(7-5)^2+(9-5)^2)/8)
    // = sqrt((9+1+1+1+0+0+4+16)/8) = sqrt(32/8) = sqrt(4) = 2.0
    assert!((buckets[0].value - 2.0).abs() < 0.01);
    assert_eq!(buckets[0].count, 8);
}

#[test]
fn ts_aggregate_stddev_single_point() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "s", schema).unwrap();
    let mut tags = BTreeMap::new();
    tags.insert("k".to_string(), "a".to_string());
    let mut fields = BTreeMap::new();
    fields.insert("v".to_string(), "42".to_string());
    ts.insert(&DataPoint {
        timestamp: 1000,
        tags,
        fields,
    })
    .unwrap();

    let q = TsAggQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        field: "v".into(),
        func: AggFunc::Stddev,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 1);
    // 单点标准差 = 0
    assert!((buckets[0].value - 0.0).abs() < 0.01);
}

// ── M75: Fill 策略测试 ──

use super::FillStrategy;

#[test]
fn ts_fill_none_skips_empty_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "f", schema).unwrap();
    // 桶0: ts=0 值=1, 桶2: ts=2000 值=3 (桶1: ts=1000 空)
    for (t, v) in [(0, 1), (2000, 3)] {
        let mut tags = BTreeMap::new();
        tags.insert("k".to_string(), "a".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("v".to_string(), v.to_string());
        ts.insert(&DataPoint {
            timestamp: t,
            tags,
            fields,
        })
        .unwrap();
    }
    let q = TsAggQuery {
        tag_filters: vec![("k".into(), "a".into())],
        time_start: Some(0),
        time_end: Some(3000),
        field: "v".into(),
        func: AggFunc::Sum,
        interval_ms: Some(1000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: Some(FillStrategy::None),
    };
    // FillStrategy::None 不填充，只返回有数据的桶
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 2);
}

#[test]
fn ts_fill_null_inserts_nan() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "f", schema).unwrap();
    for (t, v) in [(0, 1), (2000, 3)] {
        let mut tags = BTreeMap::new();
        tags.insert("k".to_string(), "a".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("v".to_string(), v.to_string());
        ts.insert(&DataPoint {
            timestamp: t,
            tags,
            fields,
        })
        .unwrap();
    }
    let q = TsAggQuery {
        tag_filters: vec![("k".into(), "a".into())],
        time_start: Some(0),
        time_end: Some(3000),
        field: "v".into(),
        func: AggFunc::Sum,
        interval_ms: Some(1000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: Some(FillStrategy::Null),
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 3); // 0, 1000, 2000
    assert!((buckets[0].value - 1.0).abs() < 0.01);
    assert!(buckets[1].value.is_nan()); // 空桶填 NaN
    assert_eq!(buckets[1].count, 0);
    assert!((buckets[2].value - 3.0).abs() < 0.01);
}

#[test]
fn ts_fill_value_inserts_constant() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "f", schema).unwrap();
    for (t, v) in [(0, 10), (2000, 30)] {
        let mut tags = BTreeMap::new();
        tags.insert("k".to_string(), "a".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("v".to_string(), v.to_string());
        ts.insert(&DataPoint {
            timestamp: t,
            tags,
            fields,
        })
        .unwrap();
    }
    let q = TsAggQuery {
        tag_filters: vec![("k".into(), "a".into())],
        time_start: Some(0),
        time_end: Some(3000),
        field: "v".into(),
        func: AggFunc::Sum,
        interval_ms: Some(1000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: Some(FillStrategy::Value(0.0)),
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 3);
    assert!((buckets[1].value - 0.0).abs() < 0.01); // 空桶填 0
    assert_eq!(buckets[1].count, 0);
}

#[test]
fn ts_fill_previous() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "f", schema).unwrap();
    for (t, v) in [(0, 10), (3000, 40)] {
        let mut tags = BTreeMap::new();
        tags.insert("k".to_string(), "a".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("v".to_string(), v.to_string());
        ts.insert(&DataPoint {
            timestamp: t,
            tags,
            fields,
        })
        .unwrap();
    }
    let q = TsAggQuery {
        tag_filters: vec![("k".into(), "a".into())],
        time_start: Some(0),
        time_end: Some(4000),
        field: "v".into(),
        func: AggFunc::Sum,
        interval_ms: Some(1000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: Some(FillStrategy::Previous),
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 4); // 0, 1000, 2000, 3000
    assert!((buckets[0].value - 10.0).abs() < 0.01);
    assert!((buckets[1].value - 10.0).abs() < 0.01); // previous=10
    assert!((buckets[2].value - 10.0).abs() < 0.01); // previous=10
    assert!((buckets[3].value - 40.0).abs() < 0.01);
}

#[test]
fn ts_fill_linear_interpolation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "f", schema).unwrap();
    for (t, v) in [(0, 10), (3000, 40)] {
        let mut tags = BTreeMap::new();
        tags.insert("k".to_string(), "a".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("v".to_string(), v.to_string());
        ts.insert(&DataPoint {
            timestamp: t,
            tags,
            fields,
        })
        .unwrap();
    }
    let q = TsAggQuery {
        tag_filters: vec![("k".into(), "a".into())],
        time_start: Some(0),
        time_end: Some(4000),
        field: "v".into(),
        func: AggFunc::Sum,
        interval_ms: Some(1000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: Some(FillStrategy::Linear),
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 4);
    assert!((buckets[0].value - 10.0).abs() < 0.01);
    // 线性插值: 10 + (1000-0)/(3000-0) * (40-10) = 10 + 10 = 20
    assert!((buckets[1].value - 20.0).abs() < 0.01);
    // 线性插值: 10 + (2000-0)/(3000-0) * (40-10) = 10 + 20 = 30
    assert!((buckets[2].value - 30.0).abs() < 0.01);
    assert!((buckets[3].value - 40.0).abs() < 0.01);
}

#[test]
fn ts_drop_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["session_id".into()],
        fields: vec!["content".into(), "token_count".into()],
    };
    let ts = TsEngine::create(&store, "chat_log", schema).unwrap();
    ts.insert(&make_point(1000, "s1", "user", "hello", 1))
        .unwrap();
    ts.insert(&make_point(2000, "s1", "assistant", "hi", 2))
        .unwrap();
    // 确认存在
    let names = retention::list_timeseries(&store).unwrap();
    assert!(names.contains(&"chat_log".to_string()));
    // 删除
    drop(ts);
    retention::drop_timeseries(&store, "chat_log").unwrap();
    // 确认不在列表中
    let names = retention::list_timeseries(&store).unwrap();
    assert!(!names.contains(&"chat_log".to_string()));
}

#[test]
fn ts_drop_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let err = retention::drop_timeseries(&store, "no_such_table").unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn ts_drop_and_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    TsEngine::create(&store, "tmp", schema.clone()).unwrap();
    retention::drop_timeseries(&store, "tmp").unwrap();
    // 重新创建同名表
    let ts = TsEngine::create(&store, "tmp", schema).unwrap();
    let mut tags = BTreeMap::new();
    tags.insert("k".to_string(), "a".to_string());
    let mut fields = BTreeMap::new();
    fields.insert("v".to_string(), "42".to_string());
    ts.insert(&DataPoint {
        timestamp: 100,
        tags,
        fields,
    })
    .unwrap();
    let q = TsQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        desc: false,
        limit: None,
    };
    let results = ts.query(&q).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].timestamp, 100);
}

#[test]
fn ts_drop_with_retention() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "ret_table", schema).unwrap();
    ts.set_retention(3600_000).unwrap();
    assert!(ts.get_retention().unwrap().is_some());
    drop(ts);
    retention::drop_timeseries(&store, "ret_table").unwrap();
    let names = retention::list_timeseries(&store).unwrap();
    assert!(!names.contains(&"ret_table".to_string()));
}

#[test]
fn ts_describe_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["session_id".into(), "role".into()],
        fields: vec!["content".into(), "token_count".into()],
    };
    let ts = TsEngine::create(&store, "chat_log", schema).unwrap();
    ts.insert(&make_point(1000, "s1", "user", "hello", 1))
        .unwrap();
    ts.insert(&make_point(2000, "s1", "assistant", "hi", 2))
        .unwrap();
    let info = retention::describe_timeseries(&store, "chat_log").unwrap();
    assert_eq!(info.name, "chat_log");
    assert_eq!(info.schema.tags, vec!["session_id", "role"]);
    assert_eq!(info.schema.fields, vec!["content", "token_count"]);
    assert_eq!(info.retention_ms, None);
    assert_eq!(info.point_count, 2);
}

#[test]
fn ts_describe_with_retention() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "metrics", schema).unwrap();
    ts.set_retention(86400_000).unwrap();
    let info = retention::describe_timeseries(&store, "metrics").unwrap();
    assert_eq!(info.retention_ms, Some(86400_000));
    assert_eq!(info.point_count, 0);
}

#[test]
fn ts_describe_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let err = retention::describe_timeseries(&store, "no_such").unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

// ── M133：TAG VALUES ──────────────────────────────

#[test]
fn ts_tag_values_single_tag() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "metrics", schema).unwrap();

    let mut tags1 = BTreeMap::new();
    tags1.insert("host".into(), "server-1".into());
    tags1.insert("region".into(), "us".into());
    let mut f1 = BTreeMap::new();
    f1.insert("cpu".into(), "50".into());
    ts.insert(&DataPoint {
        timestamp: 1000,
        tags: tags1,
        fields: f1,
    })
    .unwrap();

    let mut tags2 = BTreeMap::new();
    tags2.insert("host".into(), "server-2".into());
    tags2.insert("region".into(), "eu".into());
    let mut f2 = BTreeMap::new();
    f2.insert("cpu".into(), "60".into());
    ts.insert(&DataPoint {
        timestamp: 2000,
        tags: tags2,
        fields: f2,
    })
    .unwrap();

    let mut tags3 = BTreeMap::new();
    tags3.insert("host".into(), "server-1".into());
    tags3.insert("region".into(), "eu".into());
    let mut f3 = BTreeMap::new();
    f3.insert("cpu".into(), "70".into());
    ts.insert(&DataPoint {
        timestamp: 3000,
        tags: tags3,
        fields: f3,
    })
    .unwrap();

    let hosts = ts.tag_values("host").unwrap();
    assert_eq!(hosts, vec!["server-1", "server-2"]);

    let regions = ts.tag_values("region").unwrap();
    assert_eq!(regions, vec!["eu", "us"]);
}

#[test]
fn ts_tag_values_nonexistent_tag() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "m", schema).unwrap();

    let mut tags = BTreeMap::new();
    tags.insert("host".into(), "s1".into());
    let mut fields = BTreeMap::new();
    fields.insert("cpu".into(), "50".into());
    ts.insert(&DataPoint {
        timestamp: 1000,
        tags,
        fields,
    })
    .unwrap();

    let vals = ts.tag_values("nonexistent").unwrap();
    assert!(vals.is_empty());
}

#[test]
fn ts_all_tag_values() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "metrics", schema).unwrap();

    let mut tags1 = BTreeMap::new();
    tags1.insert("host".into(), "s1".into());
    tags1.insert("region".into(), "us".into());
    let mut f1 = BTreeMap::new();
    f1.insert("cpu".into(), "50".into());
    ts.insert(&DataPoint {
        timestamp: 1000,
        tags: tags1,
        fields: f1,
    })
    .unwrap();

    let mut tags2 = BTreeMap::new();
    tags2.insert("host".into(), "s2".into());
    tags2.insert("region".into(), "eu".into());
    let mut f2 = BTreeMap::new();
    f2.insert("cpu".into(), "60".into());
    ts.insert(&DataPoint {
        timestamp: 2000,
        tags: tags2,
        fields: f2,
    })
    .unwrap();

    let mut tags3 = BTreeMap::new();
    tags3.insert("host".into(), "s1".into());
    tags3.insert("region".into(), "eu".into());
    let mut f3 = BTreeMap::new();
    f3.insert("cpu".into(), "70".into());
    ts.insert(&DataPoint {
        timestamp: 3000,
        tags: tags3,
        fields: f3,
    })
    .unwrap();

    let all = ts.all_tag_values().unwrap();
    assert_eq!(
        all.get("host").unwrap(),
        &vec!["s1".to_string(), "s2".to_string()]
    );
    assert_eq!(
        all.get("region").unwrap(),
        &vec!["eu".to_string(), "us".to_string()]
    );
}

#[test]
fn ts_tag_values_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "empty", schema).unwrap();

    let vals = ts.tag_values("host").unwrap();
    assert!(vals.is_empty());

    let all = ts.all_tag_values().unwrap();
    assert!(all.is_empty());
}

#[test]
fn ts_downsample_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let src = TsEngine::create(&store, "src_sec", schema.clone()).unwrap();
    let dst = TsEngine::create(&store, "dst_min", schema).unwrap();

    // 插入 6 个秒级数据点，分布在 2 个 1 分钟桶内
    for i in 0..3 {
        src.insert(&DataPoint {
            timestamp: 60_000 + i * 1000, // 第 1 分钟: 60s, 61s, 62s
            tags: [("host".into(), "a".into())].into(),
            fields: [("cpu".into(), format!("{}", 10.0 + i as f64))].into(),
        })
        .unwrap();
    }
    for i in 0..3 {
        src.insert(&DataPoint {
            timestamp: 120_000 + i * 1000, // 第 2 分钟: 120s, 121s, 122s
            tags: [("host".into(), "a".into())].into(),
            fields: [("cpu".into(), format!("{}", 20.0 + i as f64))].into(),
        })
        .unwrap();
    }

    let count = src
        .downsample(
            &dst,
            "cpu",
            AggFunc::Avg,
            60_000,
            &[("host".into(), "a".into())],
            None,
            None,
        )
        .unwrap();
    assert_eq!(count, 2);

    // 验证目标表数据
    let points = dst
        .query(&TsQuery {
            tag_filters: vec![],
            time_start: None,
            time_end: None,
            desc: false,
            limit: None,
        })
        .unwrap();
    assert_eq!(points.len(), 2);
    // 第 1 桶: avg(10, 11, 12) = 11.0
    let v0: f64 = points[0].fields["cpu"].parse().unwrap();
    assert!((v0 - 11.0).abs() < 0.01);
    // 第 2 桶: avg(20, 21, 22) = 21.0
    let v1: f64 = points[1].fields["cpu"].parse().unwrap();
    assert!((v1 - 21.0).abs() < 0.01);
}

#[test]
fn ts_downsample_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let src = TsEngine::create(&store, "empty_src", schema.clone()).unwrap();
    let dst = TsEngine::create(&store, "empty_dst", schema).unwrap();

    let count = src
        .downsample(&dst, "cpu", AggFunc::Sum, 60_000, &[], None, None)
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn ts_downsample_invalid_interval() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec![],
        fields: vec!["val".into()],
    };
    let src = TsEngine::create(&store, "inv_src", schema.clone()).unwrap();
    let dst = TsEngine::create(&store, "inv_dst", schema).unwrap();

    let err = src
        .downsample(&dst, "val", AggFunc::Avg, 0, &[], None, None)
        .unwrap_err();
    assert!(err.to_string().contains("间隔"));
}

#[test]
fn ts_downsample_with_time_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec![],
        fields: vec!["val".into()],
    };
    let src = TsEngine::create(&store, "range_src", schema.clone()).unwrap();
    let dst = TsEngine::create(&store, "range_dst", schema).unwrap();

    for i in 0..10 {
        src.insert(&DataPoint {
            timestamp: i * 10_000,
            tags: BTreeMap::new(),
            fields: [("val".into(), format!("{}", i as f64))].into(),
        })
        .unwrap();
    }

    // 只降采样 [20000, 60000) 范围
    let count = src
        .downsample(
            &dst,
            "val",
            AggFunc::Sum,
            20_000,
            &[],
            Some(20_000),
            Some(60_000),
        )
        .unwrap();
    assert_eq!(count, 2); // 桶 [20000,40000) 和 [40000,60000)
}

#[test]
fn ts_query_regex_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "regex_test", schema).unwrap();

    for (host, val) in &[
        ("agent-001", "10"),
        ("agent-002", "20"),
        ("server-001", "30"),
    ] {
        ts.insert(&DataPoint {
            timestamp: 1000,
            tags: [("host".into(), host.to_string())].into(),
            fields: [("cpu".into(), val.to_string())].into(),
        })
        .unwrap();
    }

    // 正则匹配 agent-*
    let points = ts
        .query_regex(
            &[("host".into(), "agent-.*".into())],
            None,
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(points.len(), 2);
    assert!(points.iter().all(|p| p.tags["host"].starts_with("agent-")));
}

#[test]
fn ts_query_regex_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "regex_no", schema).unwrap();

    ts.insert(&DataPoint {
        timestamp: 1000,
        tags: [("host".into(), "server-001".into())].into(),
        fields: [("cpu".into(), "10".into())].into(),
    })
    .unwrap();

    let points = ts
        .query_regex(&[("host".into(), "^agent".into())], None, None, false, None)
        .unwrap();
    assert!(points.is_empty());
}

#[test]
fn ts_query_regex_invalid_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "regex_inv", schema).unwrap();

    let err = ts
        .query_regex(
            &[("host".into(), "[invalid".into())],
            None,
            None,
            false,
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("正则表达式无效"));
}

#[test]
fn ts_query_regex_empty_filters() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "regex_empty", schema).unwrap();

    ts.insert(&DataPoint {
        timestamp: 1000,
        tags: [("host".into(), "a".into())].into(),
        fields: [("cpu".into(), "10".into())].into(),
    })
    .unwrap();

    // 空正则条件 → 返回所有
    let points = ts.query_regex(&[], None, None, false, None).unwrap();
    assert_eq!(points.len(), 1);
}

#[test]
fn ts_query_regex_with_time_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["val".into()],
    };
    let ts = TsEngine::create(&store, "regex_time", schema).unwrap();

    for i in 0..5 {
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags: [("host".into(), "agent-01".into())].into(),
            fields: [("val".into(), format!("{}", i))].into(),
        })
        .unwrap();
    }

    let points = ts
        .query_regex(
            &[("host".into(), "agent.*".into())],
            Some(1000),
            Some(3000),
            false,
            None,
        )
        .unwrap();
    assert_eq!(points.len(), 2); // ts=1000, ts=2000
}

#[test]
fn ts_sliding_window_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["cpu".into()],
    };
    let ts = TsEngine::create(&store, "slide_test", schema).unwrap();

    // 插入 5 个点：ts=0,1000,2000,3000,4000
    for i in 0..5 {
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags: [("host".into(), "a".into())].into(),
            fields: [("cpu".into(), format!("{}", (i + 1) * 10))].into(),
        })
        .unwrap();
    }

    // interval=2000, sliding=1000 → 滑动窗口
    // 桶 [0,2000): ts=0,1000 → sum=10+20=30
    // 桶 [1000,3000): ts=1000,2000 → sum=20+30=50
    // 桶 [2000,4000): ts=2000,3000 → sum=30+40=70
    // 桶 [3000,5000): ts=3000,4000 → sum=40+50=90
    // 桶 [4000,6000): ts=4000 → sum=50
    let q = TsAggQuery {
        tag_filters: vec![],
        time_start: Some(0),
        time_end: Some(5000),
        field: "cpu".into(),
        func: AggFunc::Sum,
        interval_ms: Some(2000),
        sliding_ms: Some(1000),
        session_gap_ms: None,
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 5);
    assert_eq!(buckets[0].bucket_start, 0);
    assert!((buckets[0].value - 30.0).abs() < 0.01);
    assert_eq!(buckets[1].bucket_start, 1000);
    assert!((buckets[1].value - 50.0).abs() < 0.01);
    assert_eq!(buckets[2].bucket_start, 2000);
    assert!((buckets[2].value - 70.0).abs() < 0.01);
    assert_eq!(buckets[3].bucket_start, 3000);
    assert!((buckets[3].value - 90.0).abs() < 0.01);
    assert_eq!(buckets[4].bucket_start, 4000);
    assert!((buckets[4].value - 50.0).abs() < 0.01);
}

#[test]
fn ts_sliding_window_none_equals_interval() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["h".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "slide_none", schema).unwrap();

    for i in 0..4 {
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags: [("h".into(), "x".into())].into(),
            fields: [("v".into(), "1".into())].into(),
        })
        .unwrap();
    }

    // sliding_ms=None → 等同于 interval_ms（无滑动）
    let q1 = TsAggQuery {
        tag_filters: vec![],
        time_start: Some(0),
        time_end: Some(4000),
        field: "v".into(),
        func: AggFunc::Count,
        interval_ms: Some(2000),
        sliding_ms: None,
        session_gap_ms: None,
        fill: None,
    };
    let b1 = ts.aggregate(&q1).unwrap();

    let q2 = TsAggQuery {
        sliding_ms: Some(2000),
        ..q1.clone()
    };
    let b2 = ts.aggregate(&q2).unwrap();

    assert_eq!(b1.len(), b2.len());
    for (a, b) in b1.iter().zip(b2.iter()) {
        assert_eq!(a.bucket_start, b.bucket_start);
        assert_eq!(a.count, b.count);
    }
}

#[test]
fn ts_session_window_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["user".into()],
        fields: vec!["tokens".into()],
    };
    let ts = TsEngine::create(&store, "chat", schema).unwrap();

    // 模拟对话：3 条消息间隔 < 5s，然后沉默 10s，再 2 条消息
    // 会话1: ts=1000,2000,3000  会话2: ts=13000,14000
    for (t, v) in [(1000, 10), (2000, 20), (3000, 30), (13000, 40), (14000, 50)] {
        ts.insert(&DataPoint {
            timestamp: t,
            tags: [("user".into(), "alice".into())].into(),
            fields: [("tokens".into(), format!("{}", v))].into(),
        })
        .unwrap();
    }

    let q = TsAggQuery {
        tag_filters: vec![("user".into(), "alice".into())],
        time_start: None,
        time_end: None,
        field: "tokens".into(),
        func: AggFunc::Sum,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: Some(5000),
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 2);
    // 会话1: sum(10+20+30) = 60, 起始 ts=1000
    assert_eq!(buckets[0].bucket_start, 1000);
    assert!((buckets[0].value - 60.0).abs() < 0.01);
    assert_eq!(buckets[0].count, 3);
    // 会话2: sum(40+50) = 90, 起始 ts=13000
    assert_eq!(buckets[1].bucket_start, 13000);
    assert!((buckets[1].value - 90.0).abs() < 0.01);
    assert_eq!(buckets[1].count, 2);
}

#[test]
fn ts_session_window_single_session() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "s", schema).unwrap();

    // 所有点间隔 < gap → 单个会话
    for i in 0..5 {
        ts.insert(&DataPoint {
            timestamp: i * 100,
            tags: [("k".into(), "a".into())].into(),
            fields: [("v".into(), "1".into())].into(),
        })
        .unwrap();
    }

    let q = TsAggQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        field: "v".into(),
        func: AggFunc::Count,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: Some(500),
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].count, 5);
    assert_eq!(buckets[0].bucket_start, 0);
}

#[test]
fn ts_session_window_each_point_separate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["k".into()],
        fields: vec!["v".into()],
    };
    let ts = TsEngine::create(&store, "s", schema).unwrap();

    // 每个点间隔 > gap → 每个点独立会话
    for i in 0..3 {
        ts.insert(&DataPoint {
            timestamp: i * 10000,
            tags: [("k".into(), "a".into())].into(),
            fields: [("v".into(), format!("{}", (i + 1) * 10))].into(),
        })
        .unwrap();
    }

    let q = TsAggQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        field: "v".into(),
        func: AggFunc::Avg,
        interval_ms: None,
        sliding_ms: None,
        session_gap_ms: Some(1000),
        fill: None,
    };
    let buckets = ts.aggregate(&q).unwrap();
    assert_eq!(buckets.len(), 3);
    assert!((buckets[0].value - 10.0).abs() < 0.01);
    assert!((buckets[1].value - 20.0).abs() < 0.01);
    assert!((buckets[2].value - 30.0).abs() < 0.01);
}

#[test]
fn ts_state_window_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["agent".into()],
        fields: vec!["latency".into(), "status".into()],
    };
    let ts = TsEngine::create(&store, "state_test", schema).unwrap();

    // status: running, running, idle, idle, running
    let data = [
        (1000, "10", "running"),
        (2000, "20", "running"),
        (3000, "30", "idle"),
        (4000, "40", "idle"),
        (5000, "50", "running"),
    ];
    for (t, lat, st) in &data {
        ts.insert(&DataPoint {
            timestamp: *t,
            tags: [("agent".into(), "bot1".into())].into(),
            fields: [
                ("latency".into(), lat.to_string()),
                ("status".into(), st.to_string()),
            ]
            .into(),
        })
        .unwrap();
    }

    let buckets = ts
        .aggregate_state_window(&[], None, None, "latency", AggFunc::Sum, "status")
        .unwrap();
    assert_eq!(buckets.len(), 3);
    // 段1: running ts=1000,2000 → sum=30
    assert_eq!(buckets[0].bucket_start, 1000);
    assert!((buckets[0].value - 30.0).abs() < 0.01);
    assert_eq!(buckets[0].count, 2);
    // 段2: idle ts=3000,4000 → sum=70
    assert_eq!(buckets[1].bucket_start, 3000);
    assert!((buckets[1].value - 70.0).abs() < 0.01);
    assert_eq!(buckets[1].count, 2);
    // 段3: running ts=5000 → sum=50
    assert_eq!(buckets[2].bucket_start, 5000);
    assert!((buckets[2].value - 50.0).abs() < 0.01);
    assert_eq!(buckets[2].count, 1);
}

#[test]
fn ts_state_window_single_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["h".into()],
        fields: vec!["v".into(), "s".into()],
    };
    let ts = TsEngine::create(&store, "state_single", schema).unwrap();

    for i in 0..3 {
        ts.insert(&DataPoint {
            timestamp: i * 1000,
            tags: [("h".into(), "x".into())].into(),
            fields: [("v".into(), "1".into()), ("s".into(), "ok".into())].into(),
        })
        .unwrap();
    }

    let buckets = ts
        .aggregate_state_window(&[], None, None, "v", AggFunc::Count, "s")
        .unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].count, 3);
}
