/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M96: OpLog 端到端集成测试 — 验证 Primary 写操作 → OpLog 记录 → Replayer 回放 → 数据一致。
//!
//! 测试策略：
//! 1. 创建 Primary Talon 实例（启用 OpLog）
//! 2. 通过 Talon 公开 API 执行写操作
//! 3. 从 OpLog 读取条目
//! 4. 在独立 Talon 实例上用 Replayer 回放
//! 5. 验证两个实例数据一致

use std::collections::BTreeMap;

use talon::{
    ClusterConfig, ClusterRole, DataPoint, OpLogConfig, Replayer, StorageConfig, Talon, TsSchema,
};

/// 创建 Primary 模式的 Talon 实例。
fn open_primary(dir: &std::path::Path) -> Talon {
    let cluster = ClusterConfig {
        role: ClusterRole::Primary,
        oplog: OpLogConfig {
            max_entries: 100_000,
            max_age_secs: 3600,
        },
        ..Default::default()
    };
    Talon::open_with_cluster(dir, StorageConfig::default(), cluster).unwrap()
}

/// 从 Primary 读取全部 OpLog 条目并在 Replica 上回放。
fn replay_all(primary: &Talon, replica: &Talon) {
    let max_lsn = primary.oplog_current_lsn();
    if max_lsn == 0 {
        return;
    }
    let entries = primary.oplog_range(0, max_lsn, max_lsn as usize).unwrap();
    assert!(!entries.is_empty(), "OpLog 应有条目");
    let replayer = Replayer::new(0);
    let count = replayer.replay_batch(replica, &entries).unwrap();
    assert_eq!(count, entries.len() as u64);
}

// ── KV 端到端 ──
#[test]
fn e2e_kv_set_del_incr() {
    let dir_p = tempfile::tempdir().unwrap();
    let dir_r = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());
    let replica = Talon::open(dir_r.path()).unwrap();

    // KV SET
    {
        let kv = primary.kv().unwrap();
        kv.set(b"k1", b"v1", None).unwrap();
        let _ = primary.append_oplog(talon::Operation::KvSet {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_secs: None,
        });
    }
    // KV SET with TTL
    {
        let kv = primary.kv().unwrap();
        kv.set(b"k2", b"v2", Some(3600)).unwrap();
        let _ = primary.append_oplog(talon::Operation::KvSet {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            ttl_secs: Some(3600),
        });
    }
    // KV DEL
    {
        let kv = primary.kv().unwrap();
        kv.del(b"k2").unwrap();
        let _ = primary.append_oplog(talon::Operation::KvDel {
            key: b"k2".to_vec(),
        });
    }
    // KV INCR（从不存在的 key 开始，incr 返回 1；再 incr 返回 2）
    {
        let kv = primary.kv().unwrap();
        let n1 = kv.incr(b"counter").unwrap();
        assert_eq!(n1, 1);
        let _ = primary.append_oplog(talon::Operation::KvIncr {
            key: b"counter".to_vec(),
            new_value: n1,
        });
        let n2 = kv.incr(b"counter").unwrap();
        assert_eq!(n2, 2);
        let _ = primary.append_oplog(talon::Operation::KvIncr {
            key: b"counter".to_vec(),
            new_value: n2,
        });
    }

    // 验证 OpLog 有条目
    let lsn = primary.oplog_current_lsn();
    assert!(lsn >= 5, "应至少有 5 条 OpLog");

    // 回放到 Replica
    replay_all(&primary, &replica);

    // 验证 Replica 数据
    let kv_r = replica.kv_read().unwrap();
    assert_eq!(kv_r.get(b"k1").unwrap().as_deref(), Some(b"v1" as &[u8]));
    assert!(kv_r.get(b"k2").unwrap().is_none(), "k2 应已被删除");
    // incr 存储为 8 字节大端 i64
    let counter_bytes = kv_r.get(b"counter").unwrap().unwrap();
    let counter_val = i64::from_be_bytes(counter_bytes[..8].try_into().unwrap());
    assert_eq!(counter_val, 2);
}

// ── SQL 端到端 ──
#[test]
fn e2e_sql_ddl_dml() {
    let dir_p = tempfile::tempdir().unwrap();
    let dir_r = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());
    let replica = Talon::open(dir_r.path()).unwrap();

    // DDL + DML 通过 run_sql（自动记录 OpLog）
    primary
        .run_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    primary
        .run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    primary
        .run_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    primary
        .run_sql("UPDATE users SET age = 31 WHERE id = 1")
        .unwrap();
    primary.run_sql("DELETE FROM users WHERE id = 2").unwrap();

    // 验证 OpLog
    let lsn = primary.oplog_current_lsn();
    assert!(lsn >= 5, "应至少有 5 条 SQL OpLog");

    // 回放
    replay_all(&primary, &replica);

    // 验证 Replica
    let rows = replica.run_sql("SELECT * FROM users").unwrap();
    assert_eq!(rows.len(), 1, "应只剩 1 行");
    // 第一行应为 id=1, age=31
    assert_eq!(rows[0][0], talon::Value::Integer(1));
    assert_eq!(rows[0][2], talon::Value::Integer(31));
}

// ── TS 端到端 ──
#[test]
fn e2e_ts_create_insert() {
    let dir_p = tempfile::tempdir().unwrap();
    let dir_r = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());
    let replica = Talon::open(dir_r.path()).unwrap();

    // TS CREATE — 需要手动记录 OpLog（create_timeseries 不经过 ffi_exec）
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["usage".into()],
    };
    let schema_data = serde_json::to_vec(&schema).unwrap();
    primary.create_timeseries("cpu", schema.clone()).unwrap();
    let _ = primary.append_oplog(talon::Operation::TsCreate {
        series: "cpu".into(),
        schema_data,
    });

    // TS INSERT
    let p1 = DataPoint {
        timestamp: 1000,
        tags: vec![("host".into(), "srv1".into())].into_iter().collect(),
        fields: vec![("usage".into(), "75.5".into())].into_iter().collect(),
    };
    let p2 = DataPoint {
        timestamp: 2000,
        tags: vec![("host".into(), "srv1".into())].into_iter().collect(),
        fields: vec![("usage".into(), "82.3".into())].into_iter().collect(),
    };
    let ts = primary.open_timeseries("cpu").unwrap();
    ts.insert(&p1).unwrap();
    let _ = primary.append_oplog(talon::Operation::TsInsert {
        series: "cpu".into(),
        point_data: serde_json::to_vec(&p1).unwrap(),
    });
    ts.insert(&p2).unwrap();
    let _ = primary.append_oplog(talon::Operation::TsInsert {
        series: "cpu".into(),
        point_data: serde_json::to_vec(&p2).unwrap(),
    });

    // 回放
    replay_all(&primary, &replica);

    // 验证 Replica — 查询时序数据
    let ts_r = replica.open_timeseries("cpu").unwrap();
    let query = talon::TsQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        desc: false,
        limit: None,
    };
    let points = ts_r.query(&query).unwrap();
    assert_eq!(points.len(), 2, "应有 2 个数据点");
}

// ── MQ 端到端 ──
#[test]
fn e2e_mq_create_publish_subscribe() {
    let dir_p = tempfile::tempdir().unwrap();
    let dir_r = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());
    let replica = Talon::open(dir_r.path()).unwrap();

    // MQ CREATE
    {
        let mq = primary.mq().unwrap();
        mq.create_topic("events", 1000).unwrap();
        let _ = primary.append_oplog(talon::Operation::MqCreate {
            topic: "events".into(),
            max_len: Some(1000),
        });
    }
    // MQ SUBSCRIBE
    {
        let mq = primary.mq().unwrap();
        mq.subscribe("events", "g1").unwrap();
        let _ = primary.append_oplog(talon::Operation::MqSubscribe {
            topic: "events".into(),
            group: "g1".into(),
        });
    }
    // MQ PUBLISH
    {
        let mq = primary.mq().unwrap();
        mq.publish("events", b"hello").unwrap();
        let _ = primary.append_oplog(talon::Operation::MqPublish {
            topic: "events".into(),
            payload: b"hello".to_vec(),
        });
        mq.publish("events", b"world").unwrap();
        let _ = primary.append_oplog(talon::Operation::MqPublish {
            topic: "events".into(),
            payload: b"world".to_vec(),
        });
    }

    // 回放
    replay_all(&primary, &replica);

    // 验证 Replica
    let mq_r = replica.mq_read().unwrap();
    let topics = mq_r.list_topics().unwrap();
    assert!(topics.contains(&"events".to_string()));
    assert_eq!(mq_r.len("events").unwrap(), 2);
    let groups = mq_r.list_subscriptions("events").unwrap();
    assert!(groups.contains(&"g1".to_string()));
}

// ── Vector 端到端 ──
#[test]
fn e2e_vector_insert_delete() {
    let dir_p = tempfile::tempdir().unwrap();
    let dir_r = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());
    let replica = Talon::open(dir_r.path()).unwrap();

    // Vector INSERT
    {
        let ve = primary.vector("emb").unwrap();
        ve.insert(1, &[0.1, 0.2, 0.3]).unwrap();
        let vd: Vec<u8> = [0.1f32, 0.2, 0.3]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let _ = primary.append_oplog(talon::Operation::VecInsert {
            collection: "emb".into(),
            id: 1,
            vector_data: vd,
        });
    }
    {
        let ve = primary.vector("emb").unwrap();
        ve.insert(2, &[0.4, 0.5, 0.6]).unwrap();
        let vd: Vec<u8> = [0.4f32, 0.5, 0.6]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let _ = primary.append_oplog(talon::Operation::VecInsert {
            collection: "emb".into(),
            id: 2,
            vector_data: vd,
        });
    }
    // Vector DELETE
    {
        let ve = primary.vector("emb").unwrap();
        ve.delete(2).unwrap();
        let _ = primary.append_oplog(talon::Operation::VecDelete {
            collection: "emb".into(),
            id: 2,
        });
    }

    // 回放
    replay_all(&primary, &replica);

    // 验证 Replica — count 应为 1（id=1；id=2 已删除）
    let ve_r = talon::vector::VectorEngine::open(replica.store(), "emb").unwrap();
    assert_eq!(ve_r.count().unwrap(), 1);
}

// ── 混合操作端到端 ──
#[test]
fn e2e_mixed_cross_engine() {
    let dir_p = tempfile::tempdir().unwrap();
    let dir_r = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());
    let replica = Talon::open(dir_r.path()).unwrap();

    // KV
    {
        let kv = primary.kv().unwrap();
        kv.set(b"session:1", b"active", None).unwrap();
        let _ = primary.append_oplog(talon::Operation::KvSet {
            key: b"session:1".to_vec(),
            value: b"active".to_vec(),
            ttl_secs: None,
        });
    }

    // SQL
    primary
        .run_sql("CREATE TABLE logs (id INT PRIMARY KEY, msg TEXT)")
        .unwrap();
    primary
        .run_sql("INSERT INTO logs VALUES (1, 'started')")
        .unwrap();

    // TS
    let schema = TsSchema {
        tags: vec![],
        fields: vec!["val".into()],
    };
    let schema_data = serde_json::to_vec(&schema).unwrap();
    primary.create_timeseries("metrics", schema).unwrap();
    let _ = primary.append_oplog(talon::Operation::TsCreate {
        series: "metrics".into(),
        schema_data,
    });
    let p = DataPoint {
        timestamp: 100,
        tags: BTreeMap::new(),
        fields: vec![("val".into(), "42.0".into())].into_iter().collect(),
    };
    let ts = primary.open_timeseries("metrics").unwrap();
    ts.insert(&p).unwrap();
    let _ = primary.append_oplog(talon::Operation::TsInsert {
        series: "metrics".into(),
        point_data: serde_json::to_vec(&p).unwrap(),
    });

    // MQ
    {
        let mq = primary.mq().unwrap();
        mq.create_topic("notify", 0).unwrap();
        let _ = primary.append_oplog(talon::Operation::MqCreate {
            topic: "notify".into(),
            max_len: None,
        });
        mq.publish("notify", b"ping").unwrap();
        let _ = primary.append_oplog(talon::Operation::MqPublish {
            topic: "notify".into(),
            payload: b"ping".to_vec(),
        });
    }

    // Vector
    {
        let ve = primary.vector("mix").unwrap();
        ve.insert(1, &[1.0, 2.0, 3.0]).unwrap();
        let vd: Vec<u8> = [1.0f32, 2.0, 3.0]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let _ = primary.append_oplog(talon::Operation::VecInsert {
            collection: "mix".into(),
            id: 1,
            vector_data: vd,
        });
    }

    // 验证 OpLog 覆盖多模
    let lsn = primary.oplog_current_lsn();
    assert!(lsn >= 8, "混合操作应至少 8 条 OpLog");

    // 回放
    replay_all(&primary, &replica);

    // 验证 KV
    let kv_r = replica.kv_read().unwrap();
    assert_eq!(
        kv_r.get(b"session:1").unwrap().as_deref(),
        Some(b"active" as &[u8])
    );

    // 验证 SQL
    let rows = replica.run_sql("SELECT * FROM logs").unwrap();
    assert_eq!(rows.len(), 1);

    // 验证 TS
    let ts_r = replica.open_timeseries("metrics").unwrap();
    let query = talon::TsQuery {
        tag_filters: vec![],
        time_start: None,
        time_end: None,
        desc: false,
        limit: None,
    };
    assert_eq!(ts_r.query(&query).unwrap().len(), 1);

    // 验证 MQ
    let mq_r = replica.mq_read().unwrap();
    assert_eq!(mq_r.len("notify").unwrap(), 1);

    // 验证 Vector
    let ve_r = talon::vector::VectorEngine::open(replica.store(), "mix").unwrap();
    assert_eq!(ve_r.count().unwrap(), 1);
}

// ── OpLog 条目内容验证 ──
#[test]
fn e2e_oplog_entry_content_correct() {
    let dir_p = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());

    {
        let kv = primary.kv().unwrap();
        kv.set(b"test", b"data", None).unwrap();
        let _ = primary.append_oplog(talon::Operation::KvSet {
            key: b"test".to_vec(),
            value: b"data".to_vec(),
            ttl_secs: None,
        });
    }

    let entry = primary.oplog_get(1).unwrap().unwrap();
    assert_eq!(entry.lsn, 1);
    assert!(entry.timestamp_ms > 0);
    match &entry.op {
        talon::Operation::KvSet {
            key,
            value,
            ttl_secs,
        } => {
            assert_eq!(key, b"test");
            assert_eq!(value, b"data");
            assert!(ttl_secs.is_none());
        }
        _ => panic!("期望 KvSet，实际 {:?}", entry.op),
    }
}

// ── OpLog range 读取验证 ──
#[test]
fn e2e_oplog_range_reads() {
    let dir_p = tempfile::tempdir().unwrap();
    let primary = open_primary(dir_p.path());

    // 写入 10 条
    for i in 0..10 {
        let kv = primary.kv().unwrap();
        kv.set(format!("r{}", i).as_bytes(), b"v", None).unwrap();
        let _ = primary.append_oplog(talon::Operation::KvSet {
            key: format!("r{}", i).into_bytes(),
            value: b"v".to_vec(),
            ttl_secs: None,
        });
    }

    assert_eq!(primary.oplog_current_lsn(), 10);

    // 读取 (0, 10] 全部
    let all = primary.oplog_range(0, 10, 100).unwrap();
    assert_eq!(all.len(), 10);

    // 读取 (5, 10] 后半段
    let half = primary.oplog_range(5, 10, 100).unwrap();
    assert_eq!(half.len(), 5);
    assert_eq!(half[0].lsn, 6);
    assert_eq!(half[4].lsn, 10);

    // 读取 (0, 3] limit=2
    let limited = primary.oplog_range(0, 3, 2).unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].lsn, 1);
    assert_eq!(limited[1].lsn, 2);
}
