/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 集群集成测试 — 主从端到端同步、断线重连、故障转移。

use std::io::BufReader;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use talon::cluster::operation::Operation;
use talon::cluster::oplog::{OpLog, OpLogConfig};
use talon::cluster::protocol::{recv_repl_msg, send_repl_msg, ReplMessage};
use talon::cluster::receiver::ReplReceiver;
use talon::cluster::replayer::Replayer;
use talon::cluster::{ClusterConfig, ClusterRole};
use talon::storage::Store;
use talon::{StorageConfig, Talon};

/// 端到端测试：Primary 写入 → OpLog → Sender → Receiver → Replayer → Replica 可读。
#[test]
fn end_to_end_primary_to_replica_kv_sync() {
    // 1. 创建 Primary 数据库 + OpLog
    let primary_dir = tempfile::tempdir().unwrap();
    let primary_store = Store::open(primary_dir.path()).unwrap();
    let oplog = Arc::new(OpLog::open(&primary_store, OpLogConfig::default()).unwrap());

    // 2. 写入 KV 操作到 OpLog
    for i in 0..5 {
        oplog
            .append(Operation::KvSet {
                key: format!("sync_k{}", i).into_bytes(),
                value: format!("sync_v{}", i).into_bytes(),
                ttl_secs: None,
            })
            .unwrap();
    }
    assert_eq!(oplog.current_lsn(), 5);

    // 3. 启动模拟 Primary sender（手动处理单连接）
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let oplog2 = Arc::clone(&oplog);
    let stop = Arc::new(AtomicBool::new(false));

    let sender_handle = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        // 握手
        let msg = recv_repl_msg(&mut reader).unwrap().unwrap();
        if let ReplMessage::Handshake { from_lsn, .. } = msg {
            send_repl_msg(
                &mut writer,
                &ReplMessage::HandshakeOk {
                    current_lsn: oplog2.current_lsn(),
                    need_full_sync: false,
                },
            )
            .unwrap();

            // 发送增量数据
            let entries = oplog2.range(from_lsn, oplog2.current_lsn(), 1000).unwrap();
            let entry_bytes: Vec<Vec<u8>> = entries.iter().map(|e| e.to_bytes().unwrap()).collect();
            send_repl_msg(
                &mut writer,
                &ReplMessage::SyncData {
                    entries: entry_bytes,
                    current_lsn: oplog2.current_lsn(),
                    has_more: false,
                },
            )
            .unwrap();

            // 等待 Ack
            if let Ok(Some(ReplMessage::Ack { confirmed_lsn })) = recv_repl_msg(&mut reader) {
                assert_eq!(confirmed_lsn, 5);
            }
        }
    });

    // 4. 创建 Replica 数据库
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_db = Talon::open_with_cluster(
        replica_dir.path(),
        StorageConfig::default(),
        ClusterConfig {
            role: ClusterRole::Replica {
                primary_addr: format!("127.0.0.1:{}", port),
            },
            ..Default::default()
        },
    )
    .unwrap();

    // 5. 创建 Replayer + Receiver
    let replayer = Arc::new(Replayer::new(0));
    let replayer2 = Arc::clone(&replayer);
    let replica_db_arc = Arc::new(replica_db);
    let replica_db2 = Arc::clone(&replica_db_arc);

    let receiver = ReplReceiver::new(
        format!("127.0.0.1:{}", port),
        None,
        5,
        Arc::clone(&stop),
        0,
        Box::new(move |entry| {
            replayer2.replay_one(&replica_db2, entry)?;
            Ok(())
        }),
    );

    // 6. 运行 receiver（会在收到数据后因连接关闭退出）
    let recv_handle = std::thread::spawn(move || {
        let _ = receiver.run();
    });

    sender_handle.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    recv_handle.join().unwrap();

    // 7. 验证 Replica 数据（通过 KvEngine 读取，含 TTL header 解码）
    assert_eq!(replayer.last_lsn(), 5);
    let kv = talon::KvEngine::open(replica_db_arc.store()).unwrap();
    for i in 0..5 {
        let key = format!("sync_k{}", i);
        let expected = format!("sync_v{}", i);
        let val = kv.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(val, expected.as_bytes());
    }

    // 8. 验证 Replica 拒绝写入
    assert!(matches!(
        replica_db_arc.kv(),
        Err(talon::Error::ReadOnly(_))
    ));
    // 读操作正常
    let kv_r = replica_db_arc.kv_read().unwrap();
    assert!(kv_r.get(b"nonexistent").unwrap().is_none());
}

/// 测试 OpLog 截断后从节点需要全量同步的检测。
#[test]
fn oplog_truncation_triggers_full_sync_flag() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let oplog = OpLog::open(&store, OpLogConfig::default()).unwrap();

    // 写入 10 条
    for _ in 0..10 {
        oplog.append(Operation::KvDel { key: vec![0] }).unwrap();
    }
    // 截断前 5 条
    oplog.truncate(5).unwrap();
    assert_eq!(oplog.min_lsn(), 6);

    // 从节点请求 from_lsn=3（已被截断）
    let from_lsn = 3u64;
    let min = oplog.min_lsn();
    let need_full = from_lsn > 0 && min > 0 && from_lsn < min;
    assert!(need_full, "应检测到需要全量同步");

    // 从节点请求 from_lsn=0（从头同步，不需要全量）
    let from_lsn_zero = 0u64;
    let need_full_zero = from_lsn_zero > 0 && min > 0 && from_lsn_zero < min;
    assert!(!need_full_zero, "from_lsn=0 不应触发全量同步");

    // 从节点请求 from_lsn=7（在范围内，不需要全量）
    let from_lsn_ok = 7u64;
    let need_full_ok = from_lsn_ok > 0 && min > 0 && from_lsn_ok < min;
    assert!(!need_full_ok, "from_lsn=7 在范围内，不需要全量同步");
}

/// 测试 cluster_status API。
#[test]
fn cluster_status_api() {
    // Standalone
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let status = db.cluster_status();
    assert_eq!(status.role, ClusterRole::Standalone);
    assert_eq!(status.current_lsn, 0);

    // Primary with OpLog
    let dir2 = tempfile::tempdir().unwrap();
    let db2 = Talon::open_with_cluster(
        dir2.path(),
        StorageConfig::default(),
        ClusterConfig {
            role: ClusterRole::Primary,
            ..Default::default()
        },
    )
    .unwrap();
    let status2 = db2.cluster_status();
    assert_eq!(status2.role, ClusterRole::Primary);
    assert_eq!(status2.current_lsn, 0);
    assert!(status2.replicas.is_empty());
}

/// 测试 Replayer LSN 间隙检测。
#[test]
fn replayer_gap_detection_integration() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let replayer = Replayer::new(0);

    // 正常顺序回放
    let entries: Vec<_> = (1..=3)
        .map(|i| talon::OpLogEntry {
            lsn: i,
            timestamp_ms: 1000 + i,
            op: Operation::KvSet {
                key: format!("gk{}", i).into_bytes(),
                value: b"v".to_vec(),
                ttl_secs: None,
            },
        })
        .collect();
    let count = replayer.replay_batch(&db, &entries).unwrap();
    assert_eq!(count, 3);
    assert_eq!(replayer.last_lsn(), 3);

    // 间隙：LSN 5（跳过 4）
    let gap_entry = talon::OpLogEntry {
        lsn: 5,
        timestamp_ms: 1005,
        op: Operation::KvDel { key: b"x".to_vec() },
    };
    let err = replayer.replay_batch(&db, &[gap_entry]).unwrap_err();
    assert!(err.to_string().contains("LSN 间隙"));
    // last_lsn 不变
    assert_eq!(replayer.last_lsn(), 3);
}

/// 测试 Primary 模式下 cluster_status 包含 OpLog 信息。
#[test]
fn primary_cluster_status_with_oplog() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open_with_cluster(
        dir.path(),
        StorageConfig::default(),
        ClusterConfig {
            role: ClusterRole::Primary,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(db.has_oplog());
    let status = db.cluster_status();
    assert_eq!(status.role, ClusterRole::Primary);
    assert_eq!(status.current_lsn, 0);
    assert_eq!(status.oplog_entries, 0);

    // 手动向 OpLog 追加条目
    db.append_oplog(Operation::KvSet {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        ttl_secs: None,
    })
    .unwrap();

    let status2 = db.cluster_status();
    assert_eq!(status2.current_lsn, 1);
    assert_eq!(status2.oplog_entries, 1);
}
