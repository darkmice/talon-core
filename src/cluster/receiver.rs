/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Replica 端复制接收器 — 连接主节点，拉取 OpLog 增量并回放。
//!
//! 职责：
//! 1. 连接主节点 `primary_addr`
//! 2. 握手认证 + 发送当前 LSN
//! 3. 接收增量 OpLog 并回放到本地引擎
//! 4. 发送 Ack 确认已回放 LSN
//! 5. 断线自动重连

use std::io::BufReader;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backup::base64_decode;
use crate::cluster::oplog::OpLogEntry;
use crate::cluster::protocol::{recv_repl_msg, send_repl_msg, ReplMessage};
use crate::error::Error;
use crate::storage::Store;

use super::is_timeout_error;

/// 重连间隔（秒）。
const RECONNECT_INTERVAL_SECS: u64 = 3;

/// 回放回调类型：接收 OpLogEntry，执行本地回放。
/// 返回 Ok(()) 表示回放成功。
pub type ReplayFn = Box<dyn Fn(&OpLogEntry) -> Result<(), Error> + Send + Sync>;

/// Replica 复制接收器。
pub struct ReplReceiver {
    /// 主节点地址。
    primary_addr: String,
    /// 认证 token。
    token: Option<String>,
    /// 连接超时（秒）。
    timeout_secs: u64,
    /// 停止信号。
    stop: Arc<AtomicBool>,
    /// 已回放的最大 LSN（原子，供外部查询）。
    confirmed_lsn: Arc<AtomicU64>,
    /// 回放回调。
    replay_fn: Arc<ReplayFn>,
    /// 本地 Store（全量同步时直接写入 keyspace）。
    store: Option<Store>,
}

impl ReplReceiver {
    /// 创建接收器。
    pub fn new(
        primary_addr: String,
        token: Option<String>,
        timeout_secs: u64,
        stop: Arc<AtomicBool>,
        initial_lsn: u64,
        replay_fn: ReplayFn,
    ) -> Self {
        Self {
            primary_addr,
            token,
            timeout_secs,
            stop,
            confirmed_lsn: Arc::new(AtomicU64::new(initial_lsn)),
            replay_fn: Arc::new(replay_fn),
            store: None,
        }
    }

    /// 设置 Store 引用（启用全量同步支持）。
    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    /// 已回放的最大 LSN。
    pub fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::SeqCst)
    }

    /// 启动接收循环（阻塞，应在独立线程中运行）。
    /// 断线后自动重连，直到 stop 信号。
    pub fn run(&self) -> Result<(), Error> {
        while !self.stop.load(Ordering::Relaxed) {
            match self.connect_and_sync() {
                Ok(()) => {
                    // 正常退出（stop 信号）
                    break;
                }
                Err(_e) => {
                    // 连接失败或断开，等待后重连
                    if self.stop.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(RECONNECT_INTERVAL_SECS));
                }
            }
        }
        Ok(())
    }

    /// 单次连接 + 同步循环。
    fn connect_and_sync(&self) -> Result<(), Error> {
        let stream = TcpStream::connect_timeout(
            &self
                .primary_addr
                .parse()
                .map_err(|e| Error::Config(format!("无效地址 {}: {}", self.primary_addr, e)))?,
            Duration::from_secs(self.timeout_secs),
        )?;
        stream.set_read_timeout(Some(Duration::from_secs(self.timeout_secs)))?;
        stream.set_write_timeout(Some(Duration::from_secs(self.timeout_secs)))?;

        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = stream;

        // 1. 握手
        let from_lsn = self.confirmed_lsn.load(Ordering::SeqCst);
        send_repl_msg(
            &mut writer,
            &ReplMessage::Handshake {
                token: self.token.clone(),
                from_lsn,
            },
        )?;

        match recv_repl_msg(&mut reader)? {
            Some(ReplMessage::HandshakeOk { need_full_sync, .. }) => {
                if need_full_sync {
                    self.receive_full_sync(&mut reader)?;
                }
            }
            Some(ReplMessage::HandshakeErr { reason }) => {
                return Err(Error::Replication(format!("握手被拒绝: {}", reason)));
            }
            other => {
                return Err(Error::Replication(format!(
                    "期望 HandshakeOk，收到: {:?}",
                    other
                )));
            }
        }

        // 2. 接收循环
        while !self.stop.load(Ordering::Relaxed) {
            match recv_repl_msg(&mut reader) {
                Ok(Some(ReplMessage::SyncData { entries, .. })) => {
                    let decoded: Vec<OpLogEntry> = entries
                        .iter()
                        .map(|b| OpLogEntry::from_bytes(b))
                        .collect::<Result<_, _>>()?;
                    self.replay_entries(&decoded)?;
                    if let Some(last) = decoded.last() {
                        self.confirmed_lsn.store(last.lsn, Ordering::SeqCst);
                        send_repl_msg(
                            &mut writer,
                            &ReplMessage::Ack {
                                confirmed_lsn: last.lsn,
                            },
                        )?;
                    }
                }
                Ok(Some(ReplMessage::Push { entry })) => {
                    let decoded = OpLogEntry::from_bytes(&entry)?;
                    self.replay_entries(std::slice::from_ref(&decoded))?;
                    self.confirmed_lsn.store(decoded.lsn, Ordering::SeqCst);
                    send_repl_msg(
                        &mut writer,
                        &ReplMessage::Ack {
                            confirmed_lsn: decoded.lsn,
                        },
                    )?;
                }
                Ok(Some(ReplMessage::Heartbeat { .. })) => {
                    // 主节点心跳，回复心跳
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    send_repl_msg(
                        &mut writer,
                        &ReplMessage::Heartbeat {
                            lsn: self.confirmed_lsn.load(Ordering::SeqCst),
                            role: "replica".into(),
                            timestamp_ms: now_ms,
                        },
                    )?;
                }
                Ok(None) => {
                    // 连接关闭
                    return Err(Error::Replication("主节点关闭连接".into()));
                }
                Err(ref e) if is_timeout_error(e) => {
                    // 读超时，继续等待
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// 接收全量同步数据：FullSyncBegin → FullSyncChunk* → FullSyncEnd。
    fn receive_full_sync(&self, reader: &mut impl std::io::Read) -> Result<(), Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Error::Replication("全量同步需要 Store 引用".into()))?;

        // 1. 接收 FullSyncBegin
        match recv_repl_msg(reader)? {
            Some(ReplMessage::FullSyncBegin { .. }) => {}
            other => {
                return Err(Error::Replication(format!(
                    "期望 FullSyncBegin，收到: {:?}",
                    other
                )));
            }
        }

        // 2. 接收 FullSyncChunk 循环
        let mut cleared_ks = std::collections::HashSet::new();
        loop {
            match recv_repl_msg(reader)? {
                Some(ReplMessage::FullSyncChunk {
                    keyspace,
                    pairs,
                    is_last,
                }) => {
                    let ks = store.open_keyspace(&keyspace)?;
                    // 首次收到该 keyspace 的 chunk 时，分批清空旧数据（Bug 40：防 OOM）
                    if cleared_ks.insert(keyspace.clone()) {
                        loop {
                            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
                            ks.for_each_key_prefix(b"", |key| {
                                keys.push(key.to_vec());
                                keys.len() < 1000
                            })?;
                            if keys.is_empty() {
                                break;
                            }
                            let mut batch = store.batch();
                            for k in &keys {
                                batch.remove(&ks, k.clone());
                            }
                            batch.commit()?;
                        }
                    }
                    for (k_b64, v_b64) in &pairs {
                        let key = base64_decode(k_b64)?;
                        let val = base64_decode(v_b64)?;
                        ks.set(&key, &val)?;
                    }
                    if is_last {
                        // 该 keyspace 传输完毕
                    }
                }
                Some(ReplMessage::FullSyncEnd { resume_lsn }) => {
                    self.confirmed_lsn.store(resume_lsn, Ordering::SeqCst);
                    break;
                }
                other => {
                    return Err(Error::Replication(format!(
                        "全量同步期间收到意外消息: {:?}",
                        other
                    )));
                }
            }
        }
        Ok(())
    }

    /// 按 LSN 顺序串行回放条目。
    fn replay_entries(&self, entries: &[OpLogEntry]) -> Result<(), Error> {
        let current = self.confirmed_lsn.load(Ordering::SeqCst);
        for entry in entries {
            // LSN 连续性校验
            if entry.lsn <= current && current > 0 {
                // 跳过已回放的条目（幂等）
                continue;
            }
            (self.replay_fn)(entry)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::operation::Operation;
    use crate::cluster::oplog::{OpLog, OpLogConfig};
    use crate::cluster::protocol::*;
    use crate::storage::Store;
    use std::sync::Mutex;

    #[test]
    fn receiver_connects_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let oplog = Arc::new(OpLog::open(&store, OpLogConfig::default()).unwrap());

        // 写入 OpLog
        for i in 0..3 {
            oplog
                .append(Operation::KvSet {
                    key: format!("k{}", i).into_bytes(),
                    value: b"v".to_vec(),
                    ttl_secs: None,
                })
                .unwrap();
        }

        // 模拟主节点
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let oplog2 = Arc::clone(&oplog);

        let server_handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut writer = stream;

            // 接收握手
            let msg = recv_repl_msg(&mut reader).unwrap().unwrap();
            if let ReplMessage::Handshake { from_lsn, .. } = msg {
                // 发送 HandshakeOk
                send_repl_msg(
                    &mut writer,
                    &ReplMessage::HandshakeOk {
                        current_lsn: oplog2.current_lsn(),
                        need_full_sync: false,
                    },
                )
                .unwrap();

                // 发送 SyncData
                let entries = oplog2.range(from_lsn, oplog2.current_lsn(), 1000).unwrap();
                let entry_bytes: Vec<Vec<u8>> =
                    entries.iter().map(|e| e.to_bytes().unwrap()).collect();
                send_repl_msg(
                    &mut writer,
                    &ReplMessage::SyncData {
                        entries: entry_bytes,
                        current_lsn: oplog2.current_lsn(),
                        has_more: false,
                    },
                )
                .unwrap();

                // 接收 Ack
                let ack = recv_repl_msg(&mut reader).unwrap().unwrap();
                if let ReplMessage::Ack { confirmed_lsn } = ack {
                    assert_eq!(confirmed_lsn, 3);
                }
            }
        });

        // 记录回放的条目
        let replayed: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let replayed2 = Arc::clone(&replayed);

        let stop = Arc::new(AtomicBool::new(false));

        let receiver = ReplReceiver::new(
            format!("127.0.0.1:{}", port),
            None,
            5,
            Arc::clone(&stop),
            0,
            Box::new(move |entry| {
                replayed2.lock().unwrap().push(entry.lsn);
                Ok(())
            }),
        );

        // 在另一个线程运行 receiver（它会在收到数据后因连接关闭而退出）
        let recv_handle = std::thread::spawn(move || {
            let _ = receiver.connect_and_sync();
            receiver.confirmed_lsn()
        });

        server_handle.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        let confirmed = recv_handle.join().unwrap();

        assert_eq!(confirmed, 3);
        let lsns = replayed.lock().unwrap();
        assert_eq!(*lsns, vec![1, 2, 3]);
    }
}
