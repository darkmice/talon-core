/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 复制协议帧定义 — 主从节点间通信格式。
//!
//! 帧格式复用现有 TCP 协议：`[4 byte BE length][1 byte flags][payload]`
//! flags bit0 = compressed (LZ4)，其余保留。
//! payload 为 JSON 序列化的 `ReplMessage`。

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// 帧标志位。
pub const FLAG_COMPRESSED: u8 = 0x01;

/// 复制协议最大帧大小：64 MB（向量批量同步可能较大）。
pub const REPL_MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

/// 复制协议消息 — 主从节点间所有通信的统一类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ReplMessage {
    /// 从节点 → 主节点：握手请求。
    #[serde(rename = "handshake")]
    Handshake {
        /// 复制认证 token（可选）。
        token: Option<String>,
        /// 从节点当前最大 LSN（用于增量同步起点）。
        from_lsn: u64,
    },

    /// 主节点 → 从节点：握手响应。
    #[serde(rename = "handshake_ok")]
    HandshakeOk {
        /// 主节点当前 LSN。
        current_lsn: u64,
        /// 是否需要全量同步（从节点请求的 LSN 已被截断）。
        need_full_sync: bool,
    },

    /// 主节点 → 从节点：握手拒绝。
    #[serde(rename = "handshake_err")]
    HandshakeErr { reason: String },

    /// 从节点 → 主节点：请求增量同步。
    #[serde(rename = "sync_req")]
    SyncRequest {
        /// 从此 LSN 之后开始（不含）。
        from_lsn: u64,
        /// 每批最大条目数。
        batch_size: u32,
    },

    /// 主节点 → 从节点：增量同步数据。
    /// M111：entries 为 OpLogEntry 二进制序列化后的字节数组。
    #[serde(rename = "sync_data")]
    SyncData {
        entries: Vec<Vec<u8>>,
        current_lsn: u64,
        has_more: bool,
    },

    /// 从节点 → 主节点：确认已回放到指定 LSN。
    #[serde(rename = "ack")]
    Ack { confirmed_lsn: u64 },

    /// 双向：心跳。
    #[serde(rename = "heartbeat")]
    Heartbeat {
        /// 发送方当前 LSN。
        lsn: u64,
        /// 发送方角色。
        role: String,
        /// 时间戳（毫秒）。
        timestamp_ms: u64,
    },

    /// 主节点 → 从节点：实时推送新 OpLog 条目。
    /// M111：entry 为 OpLogEntry 二进制序列化后的字节。
    #[serde(rename = "push")]
    Push { entry: Vec<u8> },

    /// 主节点 → 从节点：全量同步开始标记。
    #[serde(rename = "full_sync_begin")]
    FullSyncBegin {
        /// 快照对应的 LSN。
        snapshot_lsn: u64,
    },

    /// 主节点 → 从节点：全量同步数据块。
    #[serde(rename = "full_sync_chunk")]
    FullSyncChunk {
        /// keyspace 名称。
        keyspace: String,
        /// key-value 对（base64 编码的字节）。
        pairs: Vec<(String, String)>,
        /// 是否为该 keyspace 的最后一块。
        is_last: bool,
    },

    /// 主节点 → 从节点：全量同步结束标记。
    #[serde(rename = "full_sync_end")]
    FullSyncEnd {
        /// 全量同步完成后的起始 LSN。
        resume_lsn: u64,
    },
}

impl ReplMessage {
    /// 序列化为字节（JSON）。
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// 从字节反序列化。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// 消息类型名（用于日志）。
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Handshake { .. } => "handshake",
            Self::HandshakeOk { .. } => "handshake_ok",
            Self::HandshakeErr { .. } => "handshake_err",
            Self::SyncRequest { .. } => "sync_req",
            Self::SyncData { .. } => "sync_data",
            Self::Ack { .. } => "ack",
            Self::Heartbeat { .. } => "heartbeat",
            Self::Push { .. } => "push",
            Self::FullSyncBegin { .. } => "full_sync_begin",
            Self::FullSyncChunk { .. } => "full_sync_chunk",
            Self::FullSyncEnd { .. } => "full_sync_end",
        }
    }
}

/// 读取一帧：`[4 byte BE length][1 byte flags][payload]`。
///
/// 返回 `(flags, payload)`。`None` 表示对端关闭连接。
pub fn read_repl_frame(r: &mut impl std::io::Read) -> Result<Option<(u8, Vec<u8>)>, Error> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Io(e)),
    }
    let total_len = u32::from_be_bytes(len_buf);
    if total_len > REPL_MAX_FRAME_SIZE {
        return Err(Error::Protocol(format!(
            "复制帧大小 {} 超过上限 {}",
            total_len, REPL_MAX_FRAME_SIZE
        )));
    }
    if total_len == 0 {
        return Err(Error::Protocol("复制帧长度为 0".into()));
    }
    // 读 flags
    let mut flags_buf = [0u8; 1];
    r.read_exact(&mut flags_buf)?;
    let flags = flags_buf[0];
    // 读 payload
    let payload_len = (total_len - 1) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        r.read_exact(&mut payload)?;
    }
    Ok(Some((flags, payload)))
}

/// 写入一帧：`[4 byte BE length][1 byte flags][payload]`。
pub fn write_repl_frame(
    w: &mut impl std::io::Write,
    flags: u8,
    payload: &[u8],
) -> Result<(), Error> {
    let total_len = (1 + payload.len()) as u32;
    w.write_all(&total_len.to_be_bytes())?;
    w.write_all(&[flags])?;
    w.write_all(payload)?;
    w.flush()?;
    Ok(())
}

/// 压缩阈值：超过此大小的 payload 自动 LZ4 压缩。
const COMPRESS_THRESHOLD: usize = 256;

/// 发送一条复制消息（自动序列化 + LZ4 压缩）。
pub fn send_repl_msg(w: &mut impl std::io::Write, msg: &ReplMessage) -> Result<(), Error> {
    let payload = msg.to_bytes()?;
    if payload.len() > COMPRESS_THRESHOLD {
        let compressed = lz4_flex::compress_prepend_size(&payload);
        write_repl_frame(w, FLAG_COMPRESSED, &compressed)
    } else {
        write_repl_frame(w, 0, &payload)
    }
}

/// 接收一条复制消息（自动解压 + 反序列化）。
///
/// 返回 `None` 表示对端关闭连接。
pub fn recv_repl_msg(r: &mut impl std::io::Read) -> Result<Option<ReplMessage>, Error> {
    match read_repl_frame(r)? {
        Some((flags, payload)) => {
            let data = if flags & FLAG_COMPRESSED != 0 {
                lz4_flex::decompress_size_prepended(&payload)
                    .map_err(|e| Error::Protocol(format!("LZ4 解压失败: {}", e)))?
            } else {
                payload
            };
            let msg = ReplMessage::from_bytes(&data)?;
            Ok(Some(msg))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::operation::Operation;
    use crate::cluster::oplog::OpLogEntry;

    #[test]
    fn repl_message_roundtrip_handshake() {
        let msg = ReplMessage::Handshake {
            token: Some("secret".into()),
            from_lsn: 42,
        };
        let bytes = msg.to_bytes().unwrap();
        let decoded = ReplMessage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.type_name(), "handshake");
        if let ReplMessage::Handshake { token, from_lsn } = decoded {
            assert_eq!(token.as_deref(), Some("secret"));
            assert_eq!(from_lsn, 42);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn repl_message_roundtrip_sync_data() {
        let entry = OpLogEntry {
            lsn: 1,
            timestamp_ms: 1000,
            op: Operation::KvSet {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_secs: None,
            },
        };
        let msg = ReplMessage::SyncData {
            entries: vec![entry.to_bytes().unwrap()],
            current_lsn: 10,
            has_more: false,
        };
        let bytes = msg.to_bytes().unwrap();
        let decoded = ReplMessage::from_bytes(&bytes).unwrap();
        if let ReplMessage::SyncData {
            entries,
            current_lsn,
            has_more,
        } = decoded
        {
            assert_eq!(entries.len(), 1);
            let e = OpLogEntry::from_bytes(&entries[0]).unwrap();
            assert_eq!(e.lsn, 1);
            assert_eq!(current_lsn, 10);
            assert!(!has_more);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn repl_frame_roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        write_repl_frame(&mut buf, 0, b"hello").unwrap();
        // 4 (len) + 1 (flags) + 5 (payload) = 10
        assert_eq!(buf.len(), 10);
        let mut cursor = std::io::Cursor::new(buf);
        let (flags, payload) = read_repl_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(flags, 0);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn repl_frame_eof_returns_none() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        assert!(read_repl_frame(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn repl_frame_oversized_rejected() {
        let len = REPL_MAX_FRAME_SIZE + 1;
        let mut buf = len.to_be_bytes().to_vec();
        buf.push(0); // flags
        let mut cursor = std::io::Cursor::new(buf);
        let err = read_repl_frame(&mut cursor).unwrap_err();
        assert!(err.to_string().contains("复制帧大小"));
    }

    #[test]
    fn send_recv_repl_msg() {
        let msg = ReplMessage::Heartbeat {
            lsn: 100,
            role: "primary".into(),
            timestamp_ms: 1708934400000,
        };
        let mut buf: Vec<u8> = Vec::new();
        send_repl_msg(&mut buf, &msg).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let decoded = recv_repl_msg(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.type_name(), "heartbeat");
    }

    #[test]
    fn all_message_type_names() {
        assert_eq!(
            ReplMessage::Handshake {
                token: None,
                from_lsn: 0
            }
            .type_name(),
            "handshake"
        );
        assert_eq!(
            ReplMessage::HandshakeOk {
                current_lsn: 0,
                need_full_sync: false
            }
            .type_name(),
            "handshake_ok"
        );
        assert_eq!(
            ReplMessage::HandshakeErr {
                reason: String::new()
            }
            .type_name(),
            "handshake_err"
        );
        assert_eq!(
            ReplMessage::SyncRequest {
                from_lsn: 0,
                batch_size: 0
            }
            .type_name(),
            "sync_req"
        );
        assert_eq!(ReplMessage::Ack { confirmed_lsn: 0 }.type_name(), "ack");
        assert_eq!(
            ReplMessage::Push {
                entry: OpLogEntry {
                    lsn: 0,
                    timestamp_ms: 0,
                    op: Operation::KvDel { key: vec![] },
                }
                .to_bytes()
                .unwrap()
            }
            .type_name(),
            "push"
        );
        assert_eq!(
            ReplMessage::FullSyncBegin { snapshot_lsn: 0 }.type_name(),
            "full_sync_begin"
        );
        assert_eq!(
            ReplMessage::FullSyncEnd { resume_lsn: 0 }.type_name(),
            "full_sync_end"
        );
    }
}
