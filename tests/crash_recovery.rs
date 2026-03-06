/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M101 崩溃恢复测试 — 验证 kill -9 后数据完整性。
//!
//! 策略：写入已提交数据 → drop Talon（模拟崩溃，无 graceful close）→ 重新打开 → 验证数据存活。
//! 运行：cargo test --test crash_recovery --release -- --nocapture

use std::collections::BTreeMap;
use talon::Talon;

// ══════════════════════════════════════════════════════
// 1. KV 引擎崩溃恢复
// ══════════════════════════════════════════════════════

#[test]
fn crash_kv_committed_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 写入数据后直接 drop（模拟崩溃）
    {
        let db = Talon::open(&path).unwrap();
        {
            let kv = db.kv().unwrap();
            kv.set(b"k1", b"v1", None).unwrap();
            kv.set(b"k2", b"v2", None).unwrap();
            let mut batch = db.batch();
            for i in 0..100 {
                let key = format!("batch:{:04}", i);
                kv.set_batch(&mut batch, key.as_bytes(), b"batch_val", None)
                    .unwrap();
            }
            batch.commit().unwrap();
        }
        // kv guard 已 drop，db 离开作用域（模拟崩溃，无 graceful close）
    }

    // Phase 2: 重新打开，验证数据完整
    {
        let db = Talon::open(&path).unwrap();
        let kv = db.kv().unwrap();
        assert_eq!(kv.get(b"k1").unwrap(), Some(b"v1".to_vec()), "k1 丢失");
        assert_eq!(kv.get(b"k2").unwrap(), Some(b"v2".to_vec()), "k2 丢失");
        for i in 0..100 {
            let key = format!("batch:{:04}", i);
            assert!(
                kv.get(key.as_bytes()).unwrap().is_some(),
                "batch key {} 丢失",
                i
            );
        }
        println!("✅ KV 崩溃恢复：102 个 key 全部存活");
    }
}

#[test]
fn crash_kv_overwrite_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 写入 → 覆盖 → crash
    {
        let db = Talon::open(&path).unwrap();
        {
            let kv = db.kv().unwrap();
            kv.set(b"key", b"old_value", None).unwrap();
            kv.set(b"key", b"new_value", None).unwrap();
        }
    }

    // Phase 2: 验证最新值
    {
        let db = Talon::open(&path).unwrap();
        let kv = db.kv().unwrap();
        assert_eq!(kv.get(b"key").unwrap(), Some(b"new_value".to_vec()));
        println!("✅ KV 崩溃恢复：覆盖值正确");
    }
}

#[test]
fn crash_kv_delete_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 写入 → 删除 → crash
    {
        let db = Talon::open(&path).unwrap();
        {
            let kv = db.kv().unwrap();
            kv.set(b"alive", b"yes", None).unwrap();
            kv.set(b"dead", b"yes", None).unwrap();
            kv.del(b"dead").unwrap();
        }
    }

    // Phase 2: 验证删除生效
    {
        let db = Talon::open(&path).unwrap();
        let kv = db.kv().unwrap();
        assert_eq!(kv.get(b"alive").unwrap(), Some(b"yes".to_vec()));
        assert_eq!(kv.get(b"dead").unwrap(), None, "已删除的 key 不应存在");
        println!("✅ KV 崩溃恢复：删除操作持久化");
    }
}

// ══════════════════════════════════════════════════════
// 2. SQL 引擎崩溃恢复
// ══════════════════════════════════════════════════════

#[test]
fn crash_sql_table_and_data_survive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 建表 + 插入数据 → crash
    {
        let db = Talon::open(&path).unwrap();
        db.run_sql("CREATE TABLE users (id INT, name TEXT, age INT)")
            .unwrap();
        db.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.run_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        db.run_sql("INSERT INTO users VALUES (3, 'Charlie', 35)")
            .unwrap();
        // UPDATE + DELETE
        db.run_sql("UPDATE users SET age = 31 WHERE id = 1")
            .unwrap();
        db.run_sql("DELETE FROM users WHERE id = 3").unwrap();
        drop(db);
    }

    // Phase 2: 验证表结构和数据
    {
        let db = Talon::open(&path).unwrap();
        let rows = db.run_sql("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 2, "应有 2 行（删除了 Charlie）");

        let rows = db.run_sql("SELECT age FROM users WHERE id = 1").unwrap();
        assert_eq!(rows.len(), 1);
        // age 应该是更新后的 31
        let age = &rows[0][0];
        match age {
            talon::Value::Integer(v) => assert_eq!(*v, 31, "Alice 年龄应为 31"),
            _ => panic!("age 类型错误: {:?}", age),
        }
        println!("✅ SQL 崩溃恢复：表结构 + 数据 + UPDATE/DELETE 持久化");
    }
}

#[test]
fn crash_sql_committed_transaction_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 事务提交 → crash
    {
        let db = Talon::open(&path).unwrap();
        db.run_sql("CREATE TABLE tx_test (id INT, val TEXT)")
            .unwrap();
        db.run_sql("BEGIN").unwrap();
        db.run_sql("INSERT INTO tx_test VALUES (1, 'committed')")
            .unwrap();
        db.run_sql("INSERT INTO tx_test VALUES (2, 'committed')")
            .unwrap();
        db.run_sql("COMMIT").unwrap();
        drop(db);
    }

    // Phase 2: 已提交事务数据应存活
    {
        let db = Talon::open(&path).unwrap();
        let rows = db.run_sql("SELECT * FROM tx_test").unwrap();
        assert_eq!(rows.len(), 2, "已提交事务的 2 行应存活");
        println!("✅ SQL 崩溃恢复：已提交事务数据存活");
    }
}

#[test]
fn crash_sql_index_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 建表 + 建索引 + 插入 → crash
    {
        let db = Talon::open(&path).unwrap();
        db.run_sql("CREATE TABLE indexed (id INT, email TEXT)")
            .unwrap();
        db.run_sql("CREATE INDEX idx_email ON indexed (email)")
            .unwrap();
        db.run_sql("INSERT INTO indexed VALUES (1, 'a@test.com')")
            .unwrap();
        db.run_sql("INSERT INTO indexed VALUES (2, 'b@test.com')")
            .unwrap();
        drop(db);
    }

    // Phase 2: 索引查询应正常工作
    {
        let db = Talon::open(&path).unwrap();
        let rows = db
            .run_sql("SELECT * FROM indexed WHERE email = 'a@test.com'")
            .unwrap();
        assert_eq!(rows.len(), 1, "索引查询应返回 1 行");
        println!("✅ SQL 崩溃恢复：二级索引持久化 + 查询正常");
    }
}

// ══════════════════════════════════════════════════════
// 3. TS 引擎崩溃恢复
// ══════════════════════════════════════════════════════

#[test]
fn crash_ts_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 创建时序表 + 插入数据 → crash
    {
        let db = Talon::open(&path).unwrap();
        let schema = talon::TsSchema {
            tags: vec!["host".into()],
            fields: vec!["cpu".into(), "mem".into()],
        };
        db.create_timeseries("metrics", schema).unwrap();
        let ts = db.open_timeseries("metrics").unwrap();
        for i in 0..50 {
            let mut tags = BTreeMap::new();
            tags.insert("host".to_string(), "srv1".to_string());
            let mut fields = BTreeMap::new();
            fields.insert("cpu".to_string(), format!("{}", 50 + i));
            fields.insert("mem".to_string(), "80".to_string());
            let point = talon::DataPoint {
                timestamp: 1000 + i as i64,
                tags,
                fields,
            };
            ts.insert(&point).unwrap();
        }
        drop(db);
    }

    // Phase 2: 验证时序数据存活
    {
        let db = Talon::open(&path).unwrap();
        let ts = db.open_timeseries("metrics").unwrap();
        let q = talon::TsQuery {
            tag_filters: vec![("host".to_string(), "srv1".to_string())],
            ..Default::default()
        };
        let points = ts.query(&q).unwrap();
        assert_eq!(points.len(), 50, "50 个数据点应全部存活");
        println!("✅ TS 崩溃恢复：50 个数据点全部存活");
    }
}

// ══════════════════════════════════════════════════════
// 4. MQ 引擎崩溃恢复
// ══════════════════════════════════════════════════════

#[test]
fn crash_mq_messages_survive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 创建 topic + 发布消息 → crash
    {
        let db = Talon::open(&path).unwrap();
        {
            let mq = db.mq().unwrap();
            mq.create_topic("tasks", 0).unwrap();
            for i in 0..20 {
                mq.publish("tasks", format!("job_{}", i).as_bytes())
                    .unwrap();
            }
        }
    }

    // Phase 2: 验证消息存活
    {
        let db = Talon::open(&path).unwrap();
        let mq = db.mq().unwrap();
        assert_eq!(mq.len("tasks").unwrap(), 20, "20 条消息应全部存活");
        // 消费验证
        mq.subscribe("tasks", "workers").unwrap();
        let msgs = mq.poll("tasks", "workers", "w1", 20).unwrap();
        assert_eq!(msgs.len(), 20, "应能消费 20 条消息");
        println!("✅ MQ 崩溃恢复：20 条消息全部存活 + 可消费");
    }
}

// ══════════════════════════════════════════════════════
// 5. Vector 引擎崩溃恢复
// ══════════════════════════════════════════════════════

#[test]
fn crash_vector_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 插入向量 → crash
    {
        let db = Talon::open(&path).unwrap();
        let vec_engine = db.vector("emb").unwrap();
        for i in 0..30u64 {
            let v = vec![i as f32 / 30.0, 1.0 - i as f32 / 30.0, 0.5];
            vec_engine.insert(i, &v).unwrap();
        }
        drop(db);
    }

    // Phase 2: 验证向量存活 + 搜索正常
    {
        let db = Talon::open(&path).unwrap();
        let vec_engine = db.vector("emb").unwrap();
        assert_eq!(vec_engine.count().unwrap(), 30, "30 个向量应全部存活");
        let results = vec_engine.search(&[0.5, 0.5, 0.5], 5, "cosine").unwrap();
        assert_eq!(results.len(), 5, "搜索应返回 5 个结果");
        println!("✅ Vector 崩溃恢复：30 个向量存活 + KNN 搜索正常");
    }
}

// ══════════════════════════════════════════════════════
// 6. 多引擎联合崩溃恢复
// ══════════════════════════════════════════════════════

#[test]
fn crash_multi_engine_joint_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: 同时操作多个引擎 → crash
    {
        let db = Talon::open(&path).unwrap();
        db.run_sql("CREATE TABLE docs (id INT, title TEXT)")
            .unwrap();
        db.run_sql("INSERT INTO docs VALUES (1, 'Hello World')")
            .unwrap();
        {
            let kv = db.kv().unwrap();
            kv.set(b"session:1", b"active", None).unwrap();
        }
        {
            let mq = db.mq().unwrap();
            mq.create_topic("events", 0).unwrap();
            mq.publish("events", b"doc_created").unwrap();
        }
    }

    // Phase 2: 全部引擎数据应存活
    {
        let db = Talon::open(&path).unwrap();
        // SQL
        let rows = db.run_sql("SELECT * FROM docs").unwrap();
        assert_eq!(rows.len(), 1, "SQL 数据应存活");
        // KV
        let kv = db.kv().unwrap();
        assert_eq!(
            kv.get(b"session:1").unwrap(),
            Some(b"active".to_vec()),
            "KV 数据应存活"
        );
        // MQ
        let mq = db.mq().unwrap();
        assert_eq!(mq.len("events").unwrap(), 1, "MQ 消息应存活");
        println!("✅ 多引擎联合崩溃恢复：SQL + KV + MQ 全部存活");
    }
}

// ══════════════════════════════════════════════════════
// 7. 多次崩溃恢复（连续 crash-recover 循环）
// ══════════════════════════════════════════════════════

#[test]
fn crash_repeated_crash_recover_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    for cycle in 0..5 {
        // 写入本轮数据 → crash
        {
            let db = Talon::open(&path).unwrap();
            {
                let kv = db.kv().unwrap();
                let key = format!("cycle:{}", cycle);
                kv.set(key.as_bytes(), format!("val_{}", cycle).as_bytes(), None)
                    .unwrap();
            }
        }
        // 验证所有历史数据 + 本轮数据存活
        {
            let db = Talon::open(&path).unwrap();
            let kv = db.kv_read().unwrap();
            for c in 0..=cycle {
                let key = format!("cycle:{}", c);
                let expected = format!("val_{}", c);
                let actual = kv.get(key.as_bytes()).unwrap();
                assert_eq!(actual, Some(expected.into_bytes()), "cycle {} 数据丢失", c);
            }
        }
    }
    println!("✅ 连续 5 次崩溃恢复循环：所有数据完整");
}
