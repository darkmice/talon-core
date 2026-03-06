/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! ReplSender 单元测试（从 sender.rs 拆分，满足 500 行约束）。

use super::*;
use crate::cluster::operation::Operation;
use crate::cluster::oplog::OpLogConfig;
use crate::storage::Store;

#[test]
fn sender_handshake_auth_reject() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let oplog = Arc::new(OpLog::open(&store, OpLogConfig::default()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let oplog2 = Arc::clone(&oplog);
    let store2 = store.clone();

    let handle = std::thread::spawn(move || {
        let (stream, addr) = listener.accept().unwrap();
        let replicas = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        handle_replica_conn(
            stream,
            &addr.to_string(),
            &oplog2,
            &store2,
            Some("correct_token"),
            stop,
            replicas,
            5,
        )
    });

    let stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;

    send_repl_msg(
        &mut writer,
        &ReplMessage::Handshake {
            token: Some("wrong_token".into()),
            from_lsn: 0,
        },
    )
    .unwrap();

    let resp = recv_repl_msg(&mut reader).unwrap().unwrap();
    assert!(matches!(resp, ReplMessage::HandshakeErr { .. }));

    handle.join().unwrap().unwrap();
}

#[test]
fn sender_handshake_and_sync() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let oplog = Arc::new(OpLog::open(&store, OpLogConfig::default()).unwrap());

    for i in 0..5 {
        oplog
            .append(Operation::KvSet {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                ttl_secs: None,
            })
            .unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let oplog2 = Arc::clone(&oplog);
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let store2 = store.clone();

    let handle = std::thread::spawn(move || {
        let (stream, addr) = listener.accept().unwrap();
        let replicas = Arc::new(Mutex::new(Vec::new()));
        handle_replica_conn(
            stream,
            &addr.to_string(),
            &oplog2,
            &store2,
            None,
            stop2,
            replicas,
            5,
        )
    });

    let stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;

    send_repl_msg(
        &mut writer,
        &ReplMessage::Handshake {
            token: None,
            from_lsn: 0,
        },
    )
    .unwrap();

    let resp = recv_repl_msg(&mut reader).unwrap().unwrap();
    if let ReplMessage::HandshakeOk {
        current_lsn,
        need_full_sync,
    } = resp
    {
        assert_eq!(current_lsn, 5);
        assert!(!need_full_sync);
    } else {
        panic!("expected HandshakeOk, got {:?}", resp);
    }

    let data = recv_repl_msg(&mut reader).unwrap().unwrap();
    if let ReplMessage::SyncData {
        entries,
        current_lsn,
        has_more,
    } = data
    {
        assert_eq!(entries.len(), 5);
        let e0 = crate::cluster::oplog::OpLogEntry::from_bytes(&entries[0]).unwrap();
        let e4 = crate::cluster::oplog::OpLogEntry::from_bytes(&entries[4]).unwrap();
        assert_eq!(e0.lsn, 1);
        assert_eq!(e4.lsn, 5);
        assert_eq!(current_lsn, 5);
        assert!(!has_more);
    } else {
        panic!("expected SyncData, got {:?}", data);
    }

    stop.store(true, Ordering::Relaxed);
    drop(writer);
    drop(reader);
    let _ = handle.join();
}
