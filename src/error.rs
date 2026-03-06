/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 项目统一错误类型；公开 API 返回 `Result<T, Error>`。

use std::path::PathBuf;
use thiserror::Error;

/// Talon 公开 API 错误类型。
#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage engine error: {0}")]
    Storage(#[from] fjall::Error),

    #[error("invalid keyspace name: empty or exceeds 255 chars")]
    InvalidKeyspaceName,

    #[error("key length exceeds 65536 bytes: {0}")]
    KeyTooLong(usize),

    #[error("value size exceeds limit: {0} bytes (max {1})")]
    ValueTooLarge(usize, usize),

    #[error("invalid path: {0:?}")]
    InvalidPath(PathBuf),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("SQL exec error: {0}")]
    SqlExec(String),

    #[error("timeseries error: {0}")]
    TimeSeries(String),

    #[error("message queue error: {0}")]
    MessageQueue(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("vector dimension mismatch: expected {0}, got {1}")]
    VectorDimMismatch(usize, usize),

    #[error("vector error: {0}")]
    Vector(String),

    #[error("invalid connection string: {0}")]
    InvalidConnectionString(String),

    #[error("read-only node: {0}")]
    ReadOnly(String),

    #[error("replication error: {0}")]
    Replication(String),

    #[error("geo error: {0}")]
    Geo(String),

    #[error("graph error: {0}")]
    Graph(String),
    #[error("full-text search error: {0}")]
    FullTextSearch(String),
}
