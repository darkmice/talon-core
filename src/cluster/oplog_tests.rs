/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! OpLog 单元测试（从 oplog.rs 拆分，满足 500 行约束）。

use super::*;
use crate::cluster::operation::Operation;
use crate::storage::Store;

fn open_test_oplog() -> (OpLog, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let oplog = OpLog::open(&store, OpLogConfig::default()).unwrap();
    (oplog, dir)
}

#[test]
fn append_and_get() {
    let (oplog, _dir) = open_test_oplog();
    let op = Operation::KvSet {
        key: b"k1".to_vec(),
        value: b"v1".to_vec(),
        ttl_secs: None,
    };
    let lsn = oplog.append(op.clone()).unwrap();
    assert_eq!(lsn, 1);
    assert_eq!(oplog.current_lsn(), 1);

    let entry = oplog.get(1).unwrap().unwrap();
    assert_eq!(entry.lsn, 1);
    assert_eq!(entry.op, op);
    assert!(entry.timestamp_ms > 0);
}

#[test]
fn append_sequential_lsn() {
    let (oplog, _dir) = open_test_oplog();
    for i in 1..=10 {
        let lsn = oplog
            .append(Operation::KvDel {
                key: format!("k{}", i).into_bytes(),
            })
            .unwrap();
        assert_eq!(lsn, i);
    }
    assert_eq!(oplog.current_lsn(), 10);
    assert_eq!(oplog.entry_count(), 10);
}

#[test]
fn range_query() {
    let (oplog, _dir) = open_test_oplog();
    for i in 0..5 {
        oplog
            .append(Operation::KvSet {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                ttl_secs: None,
            })
            .unwrap();
    }
    // 读取 (2, 4] → LSN 3, 4
    let entries = oplog.range(2, 4, 100).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].lsn, 3);
    assert_eq!(entries[1].lsn, 4);
}

#[test]
fn range_with_limit() {
    let (oplog, _dir) = open_test_oplog();
    for _ in 0..10 {
        oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
    }
    let entries = oplog.range(0, 10, 3).unwrap();
    assert_eq!(entries.len(), 3);
}

#[test]
fn truncate_removes_entries() {
    let (oplog, _dir) = open_test_oplog();
    for _ in 0..5 {
        oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
    }
    assert_eq!(oplog.entry_count(), 5);

    let removed = oplog.truncate(3).unwrap();
    assert_eq!(removed, 3);
    assert_eq!(oplog.min_lsn(), 4);

    // LSN 1-3 已删除
    assert!(oplog.get(1).unwrap().is_none());
    assert!(oplog.get(2).unwrap().is_none());
    assert!(oplog.get(3).unwrap().is_none());
    // LSN 4-5 仍在
    assert!(oplog.get(4).unwrap().is_some());
    assert!(oplog.get(5).unwrap().is_some());
}

#[test]
fn get_nonexistent_returns_none() {
    let (oplog, _dir) = open_test_oplog();
    assert!(oplog.get(999).unwrap().is_none());
}

#[test]
fn entry_roundtrip() {
    let entry = OpLogEntry {
        lsn: 42,
        timestamp_ms: 1708934400000,
        op: Operation::SqlInsert {
            table: "users".into(),
            row: vec![
                ("id".into(), crate::types::Value::Integer(1)),
                ("name".into(), crate::types::Value::Text("alice".into())),
            ],
        },
    };
    let bytes = entry.to_bytes().unwrap();
    let decoded = OpLogEntry::from_bytes(&bytes).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn empty_oplog_operations() {
    let (oplog, _dir) = open_test_oplog();
    assert_eq!(oplog.current_lsn(), 0);
    assert_eq!(oplog.min_lsn(), 0);
    assert_eq!(oplog.entry_count(), 0);
    // truncate on empty is safe
    assert_eq!(oplog.truncate(100).unwrap(), 0);
    // range on empty returns empty
    assert!(oplog.range(0, 10, 100).unwrap().is_empty());
    // get on empty returns None
    assert!(oplog.get(0).unwrap().is_none());
    assert!(oplog.get(1).unwrap().is_none());
}

#[test]
fn range_limit_zero_returns_empty() {
    let (oplog, _dir) = open_test_oplog();
    oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
    let entries = oplog.range(0, 1, 0).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn range_equal_bounds_returns_empty() {
    let (oplog, _dir) = open_test_oplog();
    oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
    // from == to → empty
    assert!(oplog.range(1, 1, 100).unwrap().is_empty());
    // from > to → empty
    assert!(oplog.range(5, 1, 100).unwrap().is_empty());
}

#[test]
fn truncate_then_append_continues_lsn() {
    let (oplog, _dir) = open_test_oplog();
    for _ in 0..5 {
        oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
    }
    oplog.truncate(3).unwrap();
    assert_eq!(oplog.min_lsn(), 4);
    // append continues from LSN 6
    let lsn = oplog.append(Operation::KvDel { key: vec![1] }).unwrap();
    assert_eq!(lsn, 6);
    assert_eq!(oplog.current_lsn(), 6);
}

#[test]
fn lsn_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path()).unwrap();
        let oplog = OpLog::open(&store, OpLogConfig::default()).unwrap();
        for _ in 0..5 {
            oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
        }
        assert_eq!(oplog.current_lsn(), 5);
    }
    // 重新打开
    {
        let store = Store::open(dir.path()).unwrap();
        let oplog = OpLog::open(&store, OpLogConfig::default()).unwrap();
        assert_eq!(oplog.current_lsn(), 5);
        assert_eq!(oplog.min_lsn(), 1);
        // 继续追加
        let lsn = oplog.append(Operation::KvDel { key: vec![1] }).unwrap();
        assert_eq!(lsn, 6);
    }
}
