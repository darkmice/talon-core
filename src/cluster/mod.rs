/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 集群模块 — 主从复制基础设施。
//!
//! 提供 OpLog（操作日志）、集群配置、角色管理等核心能力。
//! 依赖 storage 层，被 server / lib.rs 使用。

pub mod operation;
pub mod oplog;
pub mod protocol;
pub mod receiver;
pub mod replayer;
pub mod sender;

pub use operation::Operation;
pub use oplog::{OpLog, OpLogConfig, OpLogEntry};
pub use protocol::ReplMessage;
pub use receiver::ReplReceiver;
pub use replayer::Replayer;
pub use sender::ReplSender;

use serde::{Deserialize, Serialize};

/// 集群节点角色。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum ClusterRole {
    /// 独立模式（默认，当前行为）：单节点，不参与复制。
    #[default]
    Standalone,
    /// 主节点：接受读写，向从节点推送 OpLog。
    Primary,
    /// 从节点：只读，从主节点拉取 OpLog 回放。
    Replica {
        /// 主节点复制地址（如 "192.168.1.10:7721"）。
        primary_addr: String,
    },
}

impl ClusterRole {
    /// 是否为只读角色（Replica 不接受写入）。
    pub fn is_readonly(&self) -> bool {
        matches!(self, Self::Replica { .. })
    }

    /// 是否参与复制（Primary 或 Replica）。
    pub fn is_clustered(&self) -> bool {
        !matches!(self, Self::Standalone)
    }
}

/// 集群配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// 节点角色。
    pub role: ClusterRole,
    /// 集群复制通信地址（Primary 监听 / Replica 不使用）。
    /// 如 "0.0.0.0:7721"。
    pub replication_addr: String,
    /// 节点间复制认证 token；None 表示不启用认证。
    pub replication_token: Option<String>,
    /// 复制同步超时（秒）。
    pub replication_timeout_secs: u64,
    /// OpLog 配置。
    pub oplog: OpLogConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: ClusterRole::Standalone,
            replication_addr: "0.0.0.0:7721".into(),
            replication_token: None,
            replication_timeout_secs: 30,
            oplog: OpLogConfig::default(),
        }
    }
}

/// 从节点复制状态信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    /// 从节点地址。
    pub addr: String,
    /// 从节点已确认的 LSN。
    pub confirmed_lsn: u64,
    /// 复制延迟（LSN 差值）。
    pub lag: u64,
    /// 复制延迟（毫秒，基于时间戳差值估算）。
    pub lag_ms: u64,
    /// 最后心跳时间（毫秒时间戳）。
    pub last_heartbeat_ms: u64,
}

/// 集群状态快照（用于 `/cluster/status` API）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// 当前节点角色。
    pub role: ClusterRole,
    /// 当前 OpLog LSN。
    pub current_lsn: u64,
    /// OpLog 最小 LSN（截断后的起始点）。
    pub min_lsn: u64,
    /// OpLog 条目数（近似）。
    pub oplog_entries: u64,
    /// 从节点列表（仅 Primary 有值）。
    pub replicas: Vec<ReplicaInfo>,
}

/// 判断错误是否为 IO 超时（供 sender/receiver 共用）。
pub(crate) fn is_timeout_error(e: &crate::Error) -> bool {
    match e {
        crate::Error::Io(io_err) => {
            io_err.kind() == std::io::ErrorKind::TimedOut
                || io_err.kind() == std::io::ErrorKind::WouldBlock
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_role_defaults_to_standalone() {
        let role = ClusterRole::default();
        assert_eq!(role, ClusterRole::Standalone);
        assert!(!role.is_readonly());
        assert!(!role.is_clustered());
    }

    #[test]
    fn primary_is_clustered_not_readonly() {
        let role = ClusterRole::Primary;
        assert!(!role.is_readonly());
        assert!(role.is_clustered());
    }

    #[test]
    fn replica_is_readonly_and_clustered() {
        let role = ClusterRole::Replica {
            primary_addr: "127.0.0.1:7721".into(),
        };
        assert!(role.is_readonly());
        assert!(role.is_clustered());
    }

    #[test]
    fn cluster_config_default() {
        let cfg = ClusterConfig::default();
        assert_eq!(cfg.role, ClusterRole::Standalone);
        assert_eq!(cfg.replication_addr, "0.0.0.0:7721");
        assert!(cfg.replication_token.is_none());
        assert_eq!(cfg.replication_timeout_secs, 30);
    }

    #[test]
    fn cluster_status_serialization() {
        let status = ClusterStatus {
            role: ClusterRole::Primary,
            current_lsn: 100,
            min_lsn: 50,
            oplog_entries: 51,
            replicas: vec![ReplicaInfo {
                addr: "10.0.0.2:7721".into(),
                confirmed_lsn: 98,
                lag: 2,
                lag_ms: 150,
                last_heartbeat_ms: 1708934400000,
            }],
        };
        let json = serde_json::to_string(&status).unwrap();
        let decoded: ClusterStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.current_lsn, 100);
        assert_eq!(decoded.replicas.len(), 1);
        assert_eq!(decoded.replicas[0].lag, 2);
    }
}
