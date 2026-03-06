/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M95：时序引擎快照读 API。
//!
//! 提供 `TsEngine::snapshot_query` — 从快照读取时序数据，
//! 保证在并发写入场景下读一致性。

use super::encoding;
use super::{DataPoint, TsEngine, TsQuery};
use crate::error::Error;
use crate::storage::Snapshot;

impl TsEngine {
    /// M95：快照查询 — 从快照时刻读取数据，后续写入不可见。
    /// 接口与 `query` 完全一致，仅底层读取源为快照。
    pub fn snapshot_query(&self, snap: &Snapshot, q: &TsQuery) -> Result<Vec<DataPoint>, Error> {
        let prefix = self.tag_prefix(&q.tag_filters);
        let mut results = Vec::new();
        let mut scan_err: Option<Error> = None;
        let schema = &self.schema;
        let has_range = prefix.len() == 8 && (q.time_start.is_some() || q.time_end.is_some());

        let scan_cb = |key: &[u8], raw: &[u8]| -> bool {
            if key.len() != 16 {
                return true;
            }
            let ts = i64::from_be_bytes(key[8..16].try_into().unwrap());
            if let Some(start) = q.time_start {
                if ts < start {
                    return true;
                }
            }
            if let Some(end) = q.time_end {
                if ts >= end {
                    return !has_range;
                }
            }
            if !q.tag_filters.is_empty() && !encoding::tags_match(schema, raw, &q.tag_filters) {
                return true;
            }
            match encoding::decode_point(schema, ts, raw) {
                Ok(point) => {
                    results.push(point);
                    true
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        };

        if has_range {
            let mut sk = Vec::with_capacity(16);
            sk.extend_from_slice(&prefix);
            sk.extend_from_slice(&q.time_start.unwrap_or(i64::MIN).to_be_bytes());
            let mut ek = Vec::with_capacity(16);
            ek.extend_from_slice(&prefix);
            ek.extend_from_slice(&q.time_end.unwrap_or(i64::MAX).to_be_bytes());
            snap.for_each_kv_range(&self.keyspace, &sk, &ek, scan_cb)?;
        } else {
            snap.for_each_kv_prefix(&self.keyspace, &prefix, scan_cb)?;
        }

        if let Some(e) = scan_err {
            return Err(e);
        }
        if q.desc {
            results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        } else {
            results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }
        if let Some(limit) = q.limit {
            results.truncate(limit);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DataPoint, TsEngine, TsQuery, TsSchema};
    use crate::storage::Store;
    use std::collections::BTreeMap;

    #[test]
    fn ts_snapshot_query_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let schema = TsSchema {
            tags: vec!["host".into()],
            fields: vec!["cpu".into()],
        };
        let ts = TsEngine::create(&store, "snap_test", schema).unwrap();

        // 写入初始数据
        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "srv1".into());
        let mut fields = BTreeMap::new();
        fields.insert("cpu".into(), "50".into());
        ts.insert(&DataPoint {
            timestamp: 1000,
            tags: tags.clone(),
            fields: fields.clone(),
        })
        .unwrap();

        // 获取快照
        let snap = store.snapshot();

        // 快照后写入
        fields.insert("cpu".into(), "99".into());
        ts.insert(&DataPoint {
            timestamp: 2000,
            tags: tags.clone(),
            fields: fields.clone(),
        })
        .unwrap();

        // 快照查询：只看到 1 条
        let q = TsQuery::default();
        let snap_results = ts.snapshot_query(&snap, &q).unwrap();
        assert_eq!(snap_results.len(), 1, "快照应只看到 1 条数据");
        assert_eq!(snap_results[0].timestamp, 1000);

        // 当前查询：看到 2 条
        let current_results = ts.query(&q).unwrap();
        assert_eq!(current_results.len(), 2);
    }

    #[test]
    fn ts_snapshot_query_with_time_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let schema = TsSchema {
            tags: vec!["host".into()],
            fields: vec!["cpu".into()],
        };
        let ts = TsEngine::create(&store, "snap_range", schema).unwrap();

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "srv1".into());
        let mut fields = BTreeMap::new();
        fields.insert("cpu".into(), "10".into());

        for i in 0..5 {
            fields.insert("cpu".into(), format!("{}", i * 10));
            ts.insert(&DataPoint {
                timestamp: 1000 + i,
                tags: tags.clone(),
                fields: fields.clone(),
            })
            .unwrap();
        }

        let snap = store.snapshot();

        // 快照后写入更多
        for i in 5..10 {
            fields.insert("cpu".into(), format!("{}", i * 10));
            ts.insert(&DataPoint {
                timestamp: 1000 + i,
                tags: tags.clone(),
                fields: fields.clone(),
            })
            .unwrap();
        }

        // 快照范围查询
        let q = TsQuery {
            tag_filters: vec![("host".into(), "srv1".into())],
            time_start: Some(1002),
            time_end: Some(1008),
            ..Default::default()
        };
        let snap_results = ts.snapshot_query(&snap, &q).unwrap();
        // 快照中 ts=1002..1007，但快照只有 1000..1004
        assert_eq!(
            snap_results.len(),
            3,
            "快照范围查询 [1002,1005) 中有 1002,1003,1004"
        );
    }
}
