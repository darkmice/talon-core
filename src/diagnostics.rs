/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 数据库诊断能力：统计信息、健康检查、运行时状态。
//!
//! 从 lib.rs 拆分，满足 500 行约束。

use crate::error::Error;
use crate::{health_status, list_timeseries, lock_or_err, Talon};

impl Talon {
    /// 数据库全局存储统计：各引擎的键数/表数/队列数等。
    ///
    /// M65：数据库核心诊断能力。
    pub fn database_stats(&self) -> Result<serde_json::Value, Error> {
        // KV 键数 + 磁盘占用（O(1) 内存，流式计数，亿级安全）
        let kv = self.kv_read()?;
        let kv_key_count = kv.key_count().unwrap_or(0);
        let kv_disk = kv.disk_space();
        drop(kv);

        // SQL 表数 + 磁盘占用（SHOW TABLES 仅扫描 meta keyspace，表数有限）
        let mut sql = lock_or_err(&self.sql_engine, "sql")?;
        let sql_tables = sql.run_sql("SHOW TABLES").map(|r| r.len()).unwrap_or(0);
        let sql_disk = sql.disk_space();
        drop(sql);

        // TimeSeries 表数
        let ts_names = list_timeseries(&self.store).unwrap_or_default();
        let ts_count = ts_names.len();

        // MQ topic 数
        let mq = self.mq_read()?;
        let mq_topics = mq.list_topics().unwrap_or_default();
        let mq_topic_count = mq_topics.len();
        drop(mq);

        // 缓存统计
        let cache = self.store.segment_manager().stats();

        // M71: 磁盘空间统计
        let total_disk = self.store.disk_usage();

        Ok(serde_json::json!({
            "kv": { "key_count": kv_key_count, "disk_bytes": kv_disk },
            "sql": { "table_count": sql_tables, "disk_bytes": sql_disk },
            "timeseries": { "series_count": ts_count, "series_names": ts_names },
            "mq": { "topic_count": mq_topic_count, "topics": mq_topics },
            "storage": {
                "total_disk_bytes": total_disk,
            },
            "cache": {
                "entry_count": cache.entry_count,
                "total_size_bytes": cache.total_size,
                "hits": cache.hits,
                "misses": cache.misses,
            },
            "version": env!("CARGO_PKG_VERSION"),
        }))
    }

    /// 数据库健康检查：验证各引擎可读写。
    ///
    /// 返回各引擎的健康状态（ok/err）和整体状态。
    /// M65：数据库核心诊断能力。
    pub fn health_check(&self) -> serde_json::Value {
        let mut checks = serde_json::Map::new();
        let mut all_ok = true;

        // KV 引擎：Replica 仅做读探测，Primary/Standalone 做读写探测
        let kv_ok = (|| -> Result<(), Error> {
            let probe = b"__health_probe__";
            if self.cluster_role().is_readonly() {
                let kv_r = self.kv_read()?;
                let _ = kv_r.get(probe)?;
            } else {
                let kv = self.kv()?;
                kv.set(probe, b"1", None)?;
                drop(kv);
                let kv_r = self.kv_read()?;
                let _ = kv_r.get(probe)?;
                drop(kv_r);
                let kv = self.kv()?;
                kv.del(probe)?;
            }
            Ok(())
        })();
        checks.insert("kv".into(), health_status(&kv_ok, &mut all_ok));

        // SQL 引擎：尝试 SHOW TABLES
        let sql_ok = (|| -> Result<(), Error> {
            let mut sql = lock_or_err(&self.sql_engine, "sql")?;
            sql.run_sql("SHOW TABLES")?;
            Ok(())
        })();
        checks.insert("sql".into(), health_status(&sql_ok, &mut all_ok));

        // TimeSeries 引擎：尝试列出表
        let ts_ok = list_timeseries(&self.store).map(|_| ());
        checks.insert("timeseries".into(), health_status(&ts_ok, &mut all_ok));

        // MQ 引擎：尝试列出 topics
        let mq_ok = (|| -> Result<(), Error> {
            let mq = self.mq_read()?;
            mq.list_topics()?;
            Ok(())
        })();
        checks.insert("mq".into(), health_status(&mq_ok, &mut all_ok));

        // Store 引擎：尝试 persist
        let store_ok = self.store.persist();
        checks.insert("storage".into(), health_status(&store_ok, &mut all_ok));

        serde_json::json!({
            "status": if all_ok { "healthy" } else { "degraded" },
            "checks": checks,
        })
    }

    /// 获取引擎运行时统计信息（缓存、版本等）。
    pub fn stats(&self) -> serde_json::Value {
        let cache = self.store.segment_manager().stats();
        let hit_rate = if cache.hits + cache.misses > 0 {
            cache.hits as f64 / (cache.hits + cache.misses) as f64
        } else {
            0.0
        };
        serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "engine": "talon",
            "cache": {
                "entry_count": cache.entry_count,
                "total_size_bytes": cache.total_size,
                "hits": cache.hits,
                "misses": cache.misses,
                "hit_rate": (hit_rate * 10000.0).round() / 10000.0,
            }
        })
    }
}
