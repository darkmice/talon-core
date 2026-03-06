/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Primary 端复制发送器 — 监听从节点连接，推送 OpLog 增量。
//!
//! 职责：
//! 1. 监听 `replication_addr`，接受从节点 TCP 连接
//! 2. 握手认证
//! 3. 增量同步：从 OpLog 读取 `(from_lsn, current_lsn]` 批量发送
//! 4. 实时推送：追上后进入推送模式，定期检查新 OpLog
//! 5. 心跳：定期发送，检测从节点存活

use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::backup::base64_encode;
use crate::cluster::oplog::OpLog;
use crate::cluster::protocol::{recv_repl_msg, send_repl_msg, ReplMessage};
use crate::cluster::{ClusterConfig, ReplicaInfo};
use crate::error::Error;
use crate::storage::Store;

use super::is_timeout_error;

/// 每批同步的默认条目数。
const DEFAULT_BATCH_SIZE: u32 = 1000;

/// 心跳间隔（秒）。
const HEARTBEAT_INTERVAL_SECS: u64 = 1;

/// 实时推送轮询间隔（毫秒）。
const PUSH_POLL_INTERVAL_MS: u64 = 50;

/// 全量同步每批 KV 对数。
const FULL_SYNC_CHUNK_SIZE: usize = 500;

/// 全量同步需要遍历的核心 keyspace 列表。
const SYNC_KEYSPACES: &[&str] = &["kv", "kv_ttl", "__sql_meta__", "__ts_meta__", "__mq_meta__"];

/// 从节点连接状态。
struct ReplicaConn {
    addr: String,
    confirmed_lsn: u64,
    #[allow(dead_code)]
    last_heartbeat: Instant,
}

/// Primary 复制发送器。
pub struct ReplSender {
    config: ClusterConfig,
    oplog: Arc<OpLog>,
    store: Store,
    stop: Arc<AtomicBool>,
    /// 活跃从节点列表（用于状态查询）。
    replicas: Arc<Mutex<Vec<ReplicaInfo>>>,
}

impl ReplSender {
    /// 创建发送器。
    pub fn new(
        config: ClusterConfig,
        oplog: Arc<OpLog>,
        store: Store,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            oplog,
            store,
            stop,
            replicas: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 注入外部共享的从节点状态列表（与 Talon 共享同一 Arc）。
    pub fn with_replicas(mut self, replicas: Arc<Mutex<Vec<ReplicaInfo>>>) -> Self {
        self.replicas = replicas;
        self
    }

    /// 获取当前从节点状态列表。
    pub fn replica_infos(&self) -> Vec<ReplicaInfo> {
        match self.replicas.lock() {
            Ok(list) => list.clone(),
            Err(_) => Vec::new(),
        }
    }

    /// 启动监听（阻塞，应在独立线程中运行）。
    pub fn run(&self) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.replication_addr)?;
        listener.set_nonblocking(true)?;

        while !self.stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    let addr_str = addr.to_string();
                    let oplog = Arc::clone(&self.oplog);
                    let token = self.config.replication_token.clone();
                    let stop = Arc::clone(&self.stop);
                    let replicas = Arc::clone(&self.replicas);
                    let timeout = self.config.replication_timeout_secs;
                    let store = self.store.clone();

                    std::thread::spawn(move || {
                        let _ = handle_replica_conn(
                            stream,
                            &addr_str,
                            &oplog,
                            &store,
                            token.as_deref(),
                            stop,
                            replicas,
                            timeout,
                        );
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => return Err(Error::Io(e)),
            }
        }
        Ok(())
    }
}

/// 处理单个从节点连接。
#[allow(clippy::too_many_arguments)]
fn handle_replica_conn(
    stream: TcpStream,
    addr: &str,
    oplog: &OpLog,
    store: &Store,
    expected_token: Option<&str>,
    stop: Arc<AtomicBool>,
    replicas: Arc<Mutex<Vec<ReplicaInfo>>>,
    timeout_secs: u64,
) -> Result<(), Error> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(timeout_secs)))?;
    stream.set_write_timeout(Some(Duration::from_secs(timeout_secs)))?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // 1. 握手
    let from_lsn = match recv_repl_msg(&mut reader)? {
        Some(ReplMessage::Handshake { token, from_lsn }) => {
            // 认证
            if let Some(expected) = expected_token {
                if token.as_deref() != Some(expected) {
                    send_repl_msg(
                        &mut writer,
                        &ReplMessage::HandshakeErr {
                            reason: "认证失败".into(),
                        },
                    )?;
                    return Ok(());
                }
            }
            // 检查是否需要全量同步：
            // from_lsn=0 表示从头同步，不需要全量同步
            // from_lsn>0 但 < min_lsn 表示 OpLog 已截断，需要全量同步
            let min = oplog.min_lsn();
            let need_full = from_lsn > 0 && min > 0 && from_lsn < min;
            send_repl_msg(
                &mut writer,
                &ReplMessage::HandshakeOk {
                    current_lsn: oplog.current_lsn(),
                    need_full_sync: need_full,
                },
            )?;
            if need_full {
                send_full_sync(&mut writer, store, oplog)?;
                // 全量同步后，从 current_lsn 开始增量
                oplog.current_lsn()
            } else {
                from_lsn
            }
        }
        _ => {
            send_repl_msg(
                &mut writer,
                &ReplMessage::HandshakeErr {
                    reason: "期望 Handshake 消息".into(),
                },
            )?;
            return Ok(());
        }
    };

    // 注册从节点
    let conn = ReplicaConn {
        addr: addr.to_string(),
        confirmed_lsn: from_lsn,
        last_heartbeat: Instant::now(),
    };
    update_replica_info(&replicas, &conn, oplog.current_lsn());

    // 2. 增量同步 + 实时推送循环
    let mut current_from = from_lsn;
    let mut last_heartbeat = Instant::now();

    // 设置读超时为短间隔，以便轮询新数据
    writer
        .set_read_timeout(Some(Duration::from_millis(PUSH_POLL_INTERVAL_MS * 2)))
        .ok();

    while !stop.load(Ordering::Relaxed) {
        let head_lsn = oplog.current_lsn();

        // 有新数据：发送增量
        if current_from < head_lsn {
            let batch_end = std::cmp::min(current_from + DEFAULT_BATCH_SIZE as u64, head_lsn);
            let entries = oplog.range(current_from, batch_end, DEFAULT_BATCH_SIZE as usize)?;
            if !entries.is_empty() {
                let last_lsn = entries.last().unwrap().lsn;
                let entry_bytes: Vec<Vec<u8>> = entries
                    .iter()
                    .map(|e| e.to_bytes())
                    .collect::<Result<_, _>>()?;
                send_repl_msg(
                    &mut writer,
                    &ReplMessage::SyncData {
                        entries: entry_bytes,
                        current_lsn: head_lsn,
                        has_more: last_lsn < head_lsn,
                    },
                )?;
                current_from = last_lsn;
            }
        }

        // 心跳
        if last_heartbeat.elapsed() >= Duration::from_secs(HEARTBEAT_INTERVAL_SECS) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            send_repl_msg(
                &mut writer,
                &ReplMessage::Heartbeat {
                    lsn: oplog.current_lsn(),
                    role: "primary".into(),
                    timestamp_ms: now_ms,
                },
            )?;
            last_heartbeat = Instant::now();
        }

        // 尝试读取从节点消息（非阻塞式，短超时）
        match recv_repl_msg(&mut reader) {
            Ok(Some(ReplMessage::Ack { confirmed_lsn })) => {
                let conn = ReplicaConn {
                    addr: addr.to_string(),
                    confirmed_lsn,
                    last_heartbeat: Instant::now(),
                };
                update_replica_info(&replicas, &conn, oplog.current_lsn());
            }
            Ok(Some(ReplMessage::Heartbeat { .. })) => {
                // 从节点心跳，更新时间
            }
            Ok(None) => {
                // 连接关闭
                remove_replica_info(&replicas, addr);
                return Ok(());
            }
            Err(ref e) if is_timeout_error(e) => {
                // 读超时，继续轮询
            }
            Err(_e) => {
                remove_replica_info(&replicas, addr);
                return Ok(());
            }
            _ => {}
        }

        // 无新数据时短暂休眠
        if current_from >= head_lsn {
            std::thread::sleep(Duration::from_millis(PUSH_POLL_INTERVAL_MS));
        }
    }

    remove_replica_info(&replicas, addr);
    Ok(())
}

/// 全量同步：遍历所有核心 keyspace，流式发送 KV 对到从节点。
fn send_full_sync(
    writer: &mut impl std::io::Write,
    store: &Store,
    oplog: &OpLog,
) -> Result<(), Error> {
    let snapshot_lsn = oplog.current_lsn();
    send_repl_msg(writer, &ReplMessage::FullSyncBegin { snapshot_lsn })?;

    // 收集所有需要同步的 keyspace 名称
    let mut all_ks: Vec<String> = SYNC_KEYSPACES.iter().map(|s| s.to_string()).collect();

    // 动态发现 SQL 表数据 keyspace（sql_{table}）和索引 keyspace（idx_{table}_{col}）
    if let Ok(meta_ks) = store.open_keyspace("sql_meta") {
        let _ = meta_ks.for_each_kv_prefix(b"", |key, _val| {
            if let Ok(table_name) = std::str::from_utf8(key) {
                all_ks.push(format!("sql_{}", table_name));
            }
            true
        });
    }
    // SQL 索引元数据 + 索引数据 keyspace
    all_ks.push("sql_index_meta".to_string());
    if let Ok(idx_meta) = store.open_keyspace("sql_index_meta") {
        let _ = idx_meta.for_each_kv_prefix(b"idx:", |key, _val| {
            // key 格式: "idx:{table}:{column}"
            if let Ok(s) = std::str::from_utf8(key) {
                let parts: Vec<&str> = s.splitn(3, ':').collect();
                if parts.len() == 3 {
                    all_ks.push(format!("idx_{}_{}", parts[1], parts[2]));
                }
            }
            true
        });
    }

    // 遍历所有 keyspace 流式发送
    for ks_name in &all_ks {
        if let Ok(ks) = store.open_keyspace(ks_name) {
            send_keyspace_chunks(writer, &ks, ks_name)?;
        }
    }

    send_repl_msg(
        writer,
        &ReplMessage::FullSyncEnd {
            resume_lsn: snapshot_lsn,
        },
    )?;
    Ok(())
}

/// 流式发送单个 keyspace 的所有 KV 对。
fn send_keyspace_chunks(
    writer: &mut impl std::io::Write,
    ks: &crate::storage::Keyspace,
    ks_name: &str,
) -> Result<(), Error> {
    let mut pairs = Vec::new();
    let mut send_err: Option<Error> = None;
    let _ = ks.for_each_kv_prefix(b"", |key, val| {
        pairs.push((base64_encode(key), base64_encode(val)));
        if pairs.len() >= FULL_SYNC_CHUNK_SIZE {
            let chunk = std::mem::take(&mut pairs);
            if let Err(e) = send_repl_msg(
                writer,
                &ReplMessage::FullSyncChunk {
                    keyspace: ks_name.to_string(),
                    pairs: chunk,
                    is_last: false,
                },
            ) {
                send_err = Some(e);
                return false;
            }
        }
        true
    });
    if let Some(e) = send_err {
        return Err(e);
    }
    send_repl_msg(
        writer,
        &ReplMessage::FullSyncChunk {
            keyspace: ks_name.to_string(),
            pairs,
            is_last: true,
        },
    )
}

/// 更新从节点状态信息。
fn update_replica_info(replicas: &Mutex<Vec<ReplicaInfo>>, conn: &ReplicaConn, current_lsn: u64) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let info = ReplicaInfo {
        addr: conn.addr.clone(),
        confirmed_lsn: conn.confirmed_lsn,
        lag: current_lsn.saturating_sub(conn.confirmed_lsn),
        lag_ms: 0, // 精确延迟需要时钟同步，暂用 0
        last_heartbeat_ms: now_ms,
    };
    if let Ok(mut list) = replicas.lock() {
        if let Some(existing) = list.iter_mut().find(|r| r.addr == conn.addr) {
            *existing = info;
        } else {
            list.push(info);
        }
    }
}

/// 移除从节点状态信息。
fn remove_replica_info(replicas: &Mutex<Vec<ReplicaInfo>>, addr: &str) {
    if let Ok(mut list) = replicas.lock() {
        list.retain(|r| r.addr != addr);
    }
}

#[cfg(test)]
#[path = "sender_tests.rs"]
mod tests;
