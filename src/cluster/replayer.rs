/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 回放引擎 — 将 OpLog 条目按 LSN 顺序串行回放到本地引擎。
//!
//! 职责：
//! 1. 接收 OpLogEntry，按 LSN 严格递增顺序执行
//! 2. 将 Operation 分发到对应引擎（KV/SQL/TS/MQ/Vector/AI）
//! 3. 跳过已回放的条目（幂等）
//! 4. LSN 间隙检测

use std::sync::atomic::{AtomicU64, Ordering};

use crate::cluster::operation::Operation;
use crate::cluster::oplog::OpLogEntry;
use crate::error::Error;
use crate::Talon;

/// 回放引擎 — 在 Replica 节点上将 OpLog 条目回放到本地存储。
///
/// 线程安全：内部使用 AtomicU64 跟踪已回放 LSN。
/// 回放操作绕过 Talon 的只读检查，直接操作底层引擎。
pub struct Replayer {
    /// 已回放的最大 LSN。
    last_lsn: AtomicU64,
}

impl Replayer {
    /// 创建回放引擎，指定初始 LSN（已回放到的位置）。
    pub fn new(initial_lsn: u64) -> Self {
        Self {
            last_lsn: AtomicU64::new(initial_lsn),
        }
    }

    /// 已回放的最大 LSN。
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(Ordering::SeqCst)
    }

    /// 回放一批条目（必须按 LSN 升序排列）。
    ///
    /// 跳过 LSN <= last_lsn 的条目（幂等）。
    /// 检测 LSN 间隙并返回错误。
    pub fn replay_batch(&self, db: &Talon, entries: &[OpLogEntry]) -> Result<u64, Error> {
        let mut replayed = 0u64;
        for entry in entries {
            let current = self.last_lsn.load(Ordering::SeqCst);
            // 跳过已回放
            if entry.lsn <= current {
                continue;
            }
            // LSN 间隙检测（允许 current=0 时从任意 LSN 开始）
            if current > 0 && entry.lsn != current + 1 {
                return Err(Error::Replication(format!(
                    "LSN 间隙：期望 {}，实际 {}",
                    current + 1,
                    entry.lsn
                )));
            }
            replay_operation(db, &entry.op, entry.timestamp_ms)?;
            self.last_lsn.store(entry.lsn, Ordering::SeqCst);
            replayed += 1;
        }
        Ok(replayed)
    }

    /// 回放单条条目。
    pub fn replay_one(&self, db: &Talon, entry: &OpLogEntry) -> Result<bool, Error> {
        let n = self.replay_batch(db, std::slice::from_ref(entry))?;
        Ok(n > 0)
    }
}

/// 将单个 Operation 回放到本地引擎。
///
/// 绕过 Talon 的只读检查，直接操作底层 Store/Engine。
/// `entry_ts_ms` 为 OpLogEntry 的写入时间戳，用于 TTL 等时间相关计算。
fn replay_operation(db: &Talon, op: &Operation, _entry_ts_ms: u64) -> Result<(), Error> {
    match op {
        // ── KV（通过 KvEngine 回放，保证 TTL header 编码一致）──
        Operation::KvSet {
            key,
            value,
            ttl_secs,
        } => {
            let kv = crate::kv::KvEngine::open(db.store())?;
            kv.set(key, value, *ttl_secs)?;
            Ok(())
        }
        Operation::KvDel { key } => {
            let kv = crate::kv::KvEngine::open(db.store())?;
            kv.del(key)?;
            Ok(())
        }
        Operation::KvIncr { key, new_value } => {
            // incr 记录的是结果值，直接写入 i64 大端字节（与 KvEngine::incr 格式一致）
            let kv = crate::kv::KvEngine::open(db.store())?;
            kv.set(key, &new_value.to_be_bytes(), None)?;
            Ok(())
        }
        Operation::KvExpire { key, secs } => {
            let kv = crate::kv::KvEngine::open(db.store())?;
            kv.expire(key, *secs)?;
            Ok(())
        }

        // ── SQL ──
        Operation::SqlDdl { sql } => {
            // DDL 直接执行（绕过只读检查，使用底层 SqlEngine）
            let store = db.store();
            let mut eng = crate::sql::SqlEngine::new(store)?;
            eng.run_sql(sql)?;
            Ok(())
        }
        Operation::SqlInsert { table, row } => {
            let store = db.store();
            let mut eng = crate::sql::SqlEngine::new(store)?;
            // 构造 INSERT SQL
            let cols: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
            let vals: Vec<crate::types::Value> = row.iter().map(|(_, v)| v.clone()).collect();
            eng.batch_insert_rows(table, &cols, vec![vals])?;
            Ok(())
        }
        Operation::SqlUpdate {
            table,
            pk_column,
            pk,
            changes,
        } => {
            let mut set_parts = Vec::new();
            for (col, val) in changes {
                set_parts.push(format!("{} = {}", col, value_to_sql_literal(val)));
            }
            let sql = format!(
                "UPDATE {} SET {} WHERE {} = {}",
                table,
                set_parts.join(", "),
                pk_column,
                value_to_sql_literal(pk)
            );
            let store = db.store();
            let mut eng = crate::sql::SqlEngine::new(store)?;
            eng.run_sql(&sql)?;
            Ok(())
        }
        Operation::SqlDelete {
            table,
            pk_column,
            pk,
        } => {
            let sql = format!(
                "DELETE FROM {} WHERE {} = {}",
                table,
                pk_column,
                value_to_sql_literal(pk)
            );
            let store = db.store();
            let mut eng = crate::sql::SqlEngine::new(store)?;
            eng.run_sql(&sql)?;
            Ok(())
        }

        // ── TimeSeries ──
        Operation::TsCreate {
            series,
            schema_data,
        } => {
            let schema: crate::ts::TsSchema = serde_json::from_slice(schema_data)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            let store = db.store();
            crate::ts::TsEngine::create(store, series, schema)?;
            Ok(())
        }
        Operation::TsInsert { series, point_data } => {
            let store = db.store();
            let ts = crate::ts::TsEngine::open(store, series)?;
            let point: crate::ts::DataPoint = serde_json::from_slice(point_data)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            ts.insert(&point)?;
            Ok(())
        }
        Operation::TsDrop { series } => {
            let store = db.store();
            // 清理所有数据点
            let ts = crate::ts::TsEngine::open(store, series)?;
            ts.purge_before(i64::MAX)?;
            // 删除元数据
            let meta_ks = store.open_keyspace("__ts_meta__")?;
            meta_ks.delete(series.as_bytes())?;
            Ok(())
        }

        // ── MQ ──
        Operation::MqCreate { topic, max_len } => {
            let store = db.store();
            let mq = crate::mq::MqEngine::open(store)?;
            mq.create_topic(topic, max_len.unwrap_or(0))?;
            Ok(())
        }
        Operation::MqPublish { topic, payload } => {
            let store = db.store();
            let mq = crate::mq::MqEngine::open(store)?;
            mq.publish(topic, payload)?;
            Ok(())
        }
        Operation::MqAck {
            topic,
            group,
            msg_id,
        } => {
            let store = db.store();
            let mq = crate::mq::MqEngine::open(store)?;
            let id: u64 = msg_id
                .parse()
                .map_err(|_| Error::Serialization(format!("无效 msg_id: {}", msg_id)))?;
            mq.ack(topic, group, "", id)?;
            Ok(())
        }
        Operation::MqDrop { topic } => {
            let store = db.store();
            let mq = crate::mq::MqEngine::open(store)?;
            mq.drop_topic(topic)?;
            Ok(())
        }
        Operation::MqSubscribe { topic, group } => {
            let store = db.store();
            let mq = crate::mq::MqEngine::open(store)?;
            mq.subscribe(topic, group)?;
            Ok(())
        }
        Operation::MqUnsubscribe { topic, group } => {
            let store = db.store();
            let mq = crate::mq::MqEngine::open(store)?;
            mq.unsubscribe(topic, group)?;
            Ok(())
        }

        // ── Vector ──
        Operation::VecInsert {
            collection,
            id,
            vector_data,
        } => {
            let ve = crate::vector::VectorEngine::open(db.store(), collection)?;
            // 从字节恢复 f32 向量
            let floats: Vec<f32> = vector_data
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            ve.insert(*id, &floats)?;
            Ok(())
        }
        Operation::VecDelete { collection, id } => {
            let ve = crate::vector::VectorEngine::open(db.store(), collection)?;
            ve.delete(*id)?;
            Ok(())
        }

        // ── AI ──
        Operation::AiOp { sub_type, payload } => {
            // AI 操作回放：根据 sub_type 分发
            // 当前为占位实现，后续按需扩展
            let _ = (sub_type, payload);
            Ok(())
        }
    }
}

/// 将 Value 转为 SQL 字面量。
fn value_to_sql_literal(v: &crate::types::Value) -> String {
    match v {
        crate::types::Value::Null => "NULL".into(),
        crate::types::Value::Integer(n) => n.to_string(),
        crate::types::Value::Float(f) => f.to_string(),
        crate::types::Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        crate::types::Value::Boolean(b) => {
            if *b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        crate::types::Value::Timestamp(t) => t.to_string(),
        _ => "NULL".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::operation::Operation;
    use crate::types::Value;

    #[test]
    fn replayer_skips_already_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let replayer = Replayer::new(5);

        let entry = OpLogEntry {
            lsn: 3,
            timestamp_ms: 1000,
            op: Operation::KvSet {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_secs: None,
            },
        };
        // LSN 3 <= last_lsn 5, should be skipped
        let replayed = replayer.replay_one(&db, &entry).unwrap();
        assert!(!replayed);
        assert_eq!(replayer.last_lsn(), 5);
    }

    #[test]
    fn replayer_detects_lsn_gap() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let replayer = Replayer::new(1);

        let entry = OpLogEntry {
            lsn: 5, // gap: expected 2
            timestamp_ms: 1000,
            op: Operation::KvDel { key: b"k".to_vec() },
        };
        let err = replayer.replay_batch(&db, &[entry]).unwrap_err();
        assert!(err.to_string().contains("LSN 间隙"));
    }

    #[test]
    fn replayer_kv_set_and_del() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let replayer = Replayer::new(0);

        // Replay KV SET
        let set_entry = OpLogEntry {
            lsn: 1,
            timestamp_ms: 1000,
            op: Operation::KvSet {
                key: b"rk1".to_vec(),
                value: b"rv1".to_vec(),
                ttl_secs: None,
            },
        };
        replayer.replay_one(&db, &set_entry).unwrap();
        assert_eq!(replayer.last_lsn(), 1);

        // Verify via KvEngine（replayer 现在通过 KvEngine 写入，含 TTL header）
        let kv = crate::kv::KvEngine::open(db.store()).unwrap();
        assert_eq!(kv.get(b"rk1").unwrap().as_deref(), Some(b"rv1" as &[u8]));

        // Replay KV DEL
        let del_entry = OpLogEntry {
            lsn: 2,
            timestamp_ms: 1001,
            op: Operation::KvDel {
                key: b"rk1".to_vec(),
            },
        };
        replayer.replay_one(&db, &del_entry).unwrap();
        assert_eq!(replayer.last_lsn(), 2);
        assert!(kv.get(b"rk1").unwrap().is_none());
    }

    #[test]
    fn replayer_batch_sequential() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let replayer = Replayer::new(0);

        let entries: Vec<OpLogEntry> = (1..=5)
            .map(|i| OpLogEntry {
                lsn: i,
                timestamp_ms: 1000 + i,
                op: Operation::KvSet {
                    key: format!("bk{}", i).into_bytes(),
                    value: b"v".to_vec(),
                    ttl_secs: None,
                },
            })
            .collect();

        let count = replayer.replay_batch(&db, &entries).unwrap();
        assert_eq!(count, 5);
        assert_eq!(replayer.last_lsn(), 5);

        // Verify all keys exist via KvEngine
        let kv = crate::kv::KvEngine::open(db.store()).unwrap();
        for i in 1..=5 {
            assert!(kv.get(format!("bk{}", i).as_bytes()).unwrap().is_some());
        }
    }

    #[test]
    fn replayer_from_zero_accepts_any_first_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let replayer = Replayer::new(0);

        // When last_lsn=0, any first LSN is accepted (no gap check)
        let entry = OpLogEntry {
            lsn: 42,
            timestamp_ms: 1000,
            op: Operation::KvSet {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_secs: None,
            },
        };
        let replayed = replayer.replay_one(&db, &entry).unwrap();
        assert!(replayed);
        assert_eq!(replayer.last_lsn(), 42);
    }

    #[test]
    fn value_to_sql_literal_cases() {
        assert_eq!(value_to_sql_literal(&Value::Null), "NULL");
        assert_eq!(value_to_sql_literal(&Value::Integer(42)), "42");
        assert_eq!(value_to_sql_literal(&Value::Float(3.15)), "3.15");
        assert_eq!(
            value_to_sql_literal(&Value::Text("hello".into())),
            "'hello'"
        );
        assert_eq!(value_to_sql_literal(&Value::Text("it's".into())), "'it''s'");
        assert_eq!(value_to_sql_literal(&Value::Boolean(true)), "TRUE");
    }
}
