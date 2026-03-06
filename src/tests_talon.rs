/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Talon 集成测试：统一 API、数据库诊断、KV 分页扫描。
//!
//! 从 lib.rs 拆分，保持单文件 ≤500 行。

use super::*;
use std::collections::BTreeMap;

#[test]
fn talon_unified_api_kv_sql_vector() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    kv.set(b"k", b"v", None).unwrap();
    assert_eq!(kv.get(b"k").unwrap().as_deref(), Some(b"v" as &[u8]));
    drop(kv); // 释放锁后再调用 sql

    db.run_sql("CREATE TABLE t (id INT, x TEXT)").unwrap();
    db.run_sql("INSERT INTO t (id, x) VALUES (1, 'a')").unwrap();
    let rows = db.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);

    let vec_engine = db.vector("emb").unwrap();
    vec_engine.insert(1, &[0.1, 0.2]).unwrap();
    let out = vec_engine.search(&[0.1, 0.2], 1, "cosine").unwrap();
    assert_eq!(out.len(), 1);
}

#[test]
fn talon_timeseries_api() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["session".into()],
        fields: vec!["msg".into(), "tokens".into()],
    };
    let ts = db.create_timeseries("chat", schema).unwrap();
    let mut tags = BTreeMap::new();
    tags.insert("session".to_string(), "s1".to_string());
    let mut fields = BTreeMap::new();
    fields.insert("msg".to_string(), "hello".to_string());
    fields.insert("tokens".to_string(), "5".to_string());
    ts.insert(&DataPoint {
        timestamp: 1000,
        tags,
        fields,
    })
    .unwrap();
    let results = ts
        .query(&TsQuery {
            tag_filters: vec![("session".into(), "s1".into())],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn talon_mq_api() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    {
        let mq = db.mq().unwrap();
        mq.create_topic("tasks", 0).unwrap();
        let id = mq.publish("tasks", b"do_something").unwrap();
        assert_eq!(id, 1);
        let msgs = mq.poll("tasks", "workers", "w1", 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, b"do_something");
        mq.ack("tasks", "workers", "w1", id).unwrap();
    }
}

#[test]
fn talon_concurrent_kv_access() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(Talon::open(dir.path()).unwrap());
    let mut handles = vec![];
    for i in 0..4 {
        let db2 = db.clone();
        handles.push(std::thread::spawn(move || {
            let kv = db2.kv().unwrap();
            let key = format!("key_{}", i);
            let val = format!("val_{}", i);
            kv.set(key.as_bytes(), val.as_bytes(), None).unwrap();
            drop(kv);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let kv = db.kv().unwrap();
    for i in 0..4 {
        let key = format!("key_{}", i);
        assert!(kv.get(key.as_bytes()).unwrap().is_some());
    }
}

// ── M65: Database Stats & Health Check ──

#[test]
fn database_stats_reflects_all_engines() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();

    // KV
    {
        let kv = db.kv().unwrap();
        kv.set(b"a", b"1", None).unwrap();
        kv.set(b"b", b"2", None).unwrap();
    }
    // SQL
    db.run_sql("CREATE TABLE t1 (id INT, x TEXT)").unwrap();
    db.run_sql("CREATE TABLE t2 (id INT)").unwrap();
    // TS
    db.create_timeseries(
        "ts1",
        TsSchema {
            tags: vec!["t".into()],
            fields: vec!["f".into()],
        },
    )
    .unwrap();
    // MQ
    {
        let mq = db.mq().unwrap();
        mq.create_topic("q1", 0).unwrap();
    }

    let stats = db.database_stats().unwrap();
    assert_eq!(stats["kv"]["key_count"], 2);
    assert_eq!(stats["sql"]["table_count"], 2);
    assert_eq!(stats["timeseries"]["series_count"], 1);
    assert_eq!(stats["mq"]["topic_count"], 1);
    assert!(stats["version"].is_string());
    // M71: 磁盘占用统计
    assert!(stats["storage"]["total_disk_bytes"].as_u64().unwrap() > 0);
}

#[test]
fn storage_disk_usage_grows_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let stats0 = db.database_stats().unwrap();
    let disk0 = stats0["storage"]["total_disk_bytes"].as_u64().unwrap();
    // 写入数据
    {
        let kv = db.kv().unwrap();
        for i in 0..100u32 {
            kv.set(format!("dk:{:04}", i).as_bytes(), b"payload_data", None)
                .unwrap();
        }
    }
    let stats1 = db.database_stats().unwrap();
    let disk1 = stats1["storage"]["total_disk_bytes"].as_u64().unwrap();
    assert!(disk1 >= disk0, "磁盘占用应不减少: {} vs {}", disk1, disk0);
}

#[test]
fn health_check_all_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let health = db.health_check();
    assert_eq!(health["status"], "healthy");
    assert_eq!(health["checks"]["kv"]["status"], "ok");
    assert_eq!(health["checks"]["sql"]["status"], "ok");
    assert_eq!(health["checks"]["timeseries"]["status"], "ok");
    assert_eq!(health["checks"]["mq"]["status"], "ok");
    assert_eq!(health["checks"]["storage"]["status"], "ok");
}

#[test]
fn database_stats_empty_db() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let stats = db.database_stats().unwrap();
    assert_eq!(stats["kv"]["key_count"], 0);
    assert_eq!(stats["sql"]["table_count"], 0);
    assert_eq!(stats["timeseries"]["series_count"], 0);
    assert_eq!(stats["mq"]["topic_count"], 0);
}

#[test]
fn mq_list_topics() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    assert!(mq.list_topics().unwrap().is_empty());
    mq.create_topic("alpha", 0).unwrap();
    mq.create_topic("beta", 0).unwrap();
    let topics = mq.list_topics().unwrap();
    assert_eq!(topics.len(), 2);
    assert!(topics.contains(&"alpha".to_string()));
    assert!(topics.contains(&"beta".to_string()));
}

// ── M67: KV Paginated Scan ──

#[test]
fn kv_keys_prefix_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    {
        let kv = db.kv().unwrap();
        for i in 0..20u32 {
            kv.set(format!("k:{:03}", i).as_bytes(), b"v", None)
                .unwrap();
        }
        let page1 = kv.keys_prefix_limit(b"k:", 0, 5).unwrap();
        assert_eq!(page1.len(), 5);
        let page2 = kv.keys_prefix_limit(b"k:", 5, 5).unwrap();
        assert_eq!(page2.len(), 5);
        assert_ne!(page1[0], page2[0]);
        let beyond = kv.keys_prefix_limit(b"k:", 18, 10).unwrap();
        assert_eq!(beyond.len(), 2);
        assert_eq!(kv.key_count().unwrap(), 20);
    }
}

#[test]
fn kv_scan_prefix_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    for i in 0..10u32 {
        kv.set(
            format!("s:{}", i).as_bytes(),
            format!("val{}", i).as_bytes(),
            None,
        )
        .unwrap();
    }
    let pairs = kv.scan_prefix_limit(b"s:", 0, 3).unwrap();
    assert_eq!(pairs.len(), 3);
    assert!(!pairs[0].1.is_empty());
}

// ── Cluster: Replica Readonly Guard ──

#[test]
fn replica_rejects_kv_write() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    assert!(db.cluster_role().is_readonly());
    assert!(matches!(db.kv(), Err(Error::ReadOnly(_))));
}

#[test]
fn replica_allows_kv_read() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    // kv_read should work on Replica
    let kv_r = db.kv_read().unwrap();
    assert!(kv_r.get(b"nonexistent").unwrap().is_none());
}

#[test]
fn replica_rejects_write_sql() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    let err = db.run_sql("CREATE TABLE t (id INT)").unwrap_err();
    assert!(matches!(err, Error::ReadOnly(_)));
    let err = db.run_sql("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(matches!(err, Error::ReadOnly(_)));
}

#[test]
fn replica_allows_read_sql() {
    let dir = tempfile::tempdir().unwrap();
    // First create table in standalone mode
    {
        let db = Talon::open(dir.path()).unwrap();
        db.run_sql("CREATE TABLE t (id INT, x TEXT)").unwrap();
        db.run_sql("INSERT INTO t (id, x) VALUES (1, 'a')").unwrap();
    }
    // Reopen as Replica
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    // SELECT should work
    let rows = db.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    // SHOW TABLES should work
    let tables = db.run_sql("SHOW TABLES").unwrap();
    assert!(!tables.is_empty());
}

#[test]
fn replica_rejects_batch_insert() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    let err = db.batch_insert_rows("t", &[], vec![]).unwrap_err();
    assert!(matches!(err, Error::ReadOnly(_)));
}

#[test]
fn replica_rejects_import() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    let err = db.import("/nonexistent").unwrap_err();
    assert!(matches!(err, Error::ReadOnly(_)));
}

#[test]
fn standalone_allows_all_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    assert!(!db.cluster_role().is_readonly());
    assert!(!db.has_oplog()); // Standalone has no OpLog
                              // All writes should work
    let kv = db.kv().unwrap();
    kv.set(b"k", b"v", None).unwrap();
    drop(kv);
    db.run_sql("CREATE TABLE t (id INT)").unwrap();
    db.run_sql("INSERT INTO t (id) VALUES (1)").unwrap();
}

#[test]
fn primary_has_oplog() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Primary,
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    assert!(!db.cluster_role().is_readonly());
    assert!(db.has_oplog());
    // Writes should work
    let kv = db.kv().unwrap();
    kv.set(b"k", b"v", None).unwrap();
    drop(kv);
    db.run_sql("CREATE TABLE t (id INT)").unwrap();
}

#[test]
fn primary_sql_ddl_auto_appends_oplog() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Primary,
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    assert_eq!(db.oplog_current_lsn(), 0);

    db.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    assert_eq!(db.oplog_current_lsn(), 1);

    db.run_sql("INSERT INTO t (id, name) VALUES (1, 'alice')")
        .unwrap();
    assert_eq!(db.oplog_current_lsn(), 2);

    // SELECT should NOT append OpLog
    db.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(db.oplog_current_lsn(), 2);

    // Verify OpLog entry content
    let entry = db.oplog_get(1).unwrap().unwrap();
    if let Operation::SqlDdl { sql } = &entry.op {
        assert!(sql.contains("CREATE TABLE"));
    } else {
        panic!("expected SqlDdl, got {:?}", entry.op);
    }
}

#[test]
fn standalone_no_oplog_overhead() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    // append_oplog should silently return 0 in Standalone mode
    let lsn = db
        .append_oplog(Operation::KvSet {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_secs: None,
        })
        .unwrap();
    assert_eq!(lsn, 0);
}

#[test]
fn promote_replica_to_primary() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    assert!(db.cluster_role().is_readonly());
    assert!(!db.has_oplog());

    // Promote
    db.promote().unwrap();
    assert!(!db.cluster_role().is_readonly());
    assert_eq!(db.cluster_role(), ClusterRole::Primary);
    assert!(db.has_oplog());

    // Now writes should work
    let kv = db.kv().unwrap();
    kv.set(b"pk", b"pv", None).unwrap();
    drop(kv);
    db.run_sql("CREATE TABLE promoted_t (id INT)").unwrap();
    // OpLog should record the DDL
    assert!(db.oplog_current_lsn() > 0);
}

#[test]
fn promote_primary_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Primary,
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    let err = db.promote().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

#[test]
fn promote_standalone_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let err = db.promote().unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

#[test]
fn replica_health_check_passes() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_cfg = ClusterConfig {
        role: ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        },
        ..Default::default()
    };
    let db = Talon::open_with_cluster(dir.path(), StorageConfig::default(), cluster_cfg).unwrap();
    let health = db.health_check();
    // Should not fail due to readonly
    assert_eq!(health["status"], "healthy");
}
