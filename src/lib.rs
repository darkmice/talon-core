/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! # Talon
//!
//! 多模融合数据引擎：关系型 + KV + 时序 + 消息队列 + 向量 + GEO + 全文搜索 + 图。
//! 单二进制、零外部依赖，嵌入式 + Server 双模。
//! 对标 SQLite + Redis + InfluxDB + Elasticsearch + Qdrant + PostGIS 联合能力。
//!
//! AI-Native 扩展请使用 `talon-ai` crate（通过 `TalonAiExt` trait 注入）。
//!
//! P0：所有引擎单例化 + Mutex 保护，确保并发安全。
//! Mutex poison 使用 Error::LockPoisoned 安全处理，不 panic。

pub mod backup;
pub mod cluster;
pub mod cross;
pub mod error;
pub mod ffi;
pub(crate) mod ffi_bin;
mod ffi_exec;
pub mod fts;
pub mod geo;
pub mod graph;
pub mod import;
pub mod kv;
pub mod mq;
pub mod server;
pub mod sql;
pub mod storage;
pub mod ts;
pub mod types;
pub mod vector;

pub use backup::{export_db, export_keyspace, import_db, import_keyspace};
pub use cluster::{
    ClusterConfig, ClusterRole, ClusterStatus, OpLog, OpLogConfig, OpLogEntry, Operation,
    ReplReceiver, ReplSender, Replayer, ReplicaInfo,
};
pub use cross::{
    geo_box_vector_search, geo_vector_search, graph_fts_search, graph_vector_search, triple_search,
    GeoBoxVectorQuery, GeoVectorHit, GeoVectorQuery, GraphFtsHit, GraphFtsQuery, GraphVectorHit,
    GraphVectorQuery, TripleHit, TripleQuery,
};
pub use error::Error;
pub use fts::bool_query::BoolQuery;
pub use fts::es_bulk::{parse_es_bulk, EsBulkItem};
pub use fts::hybrid::{hybrid_search, HybridHit, HybridQuery};
pub use fts::multi_field::MultiFieldQuery;
pub use fts::{Analyzer, FtsConfig, FtsDoc, FtsEngine, SearchHit};
pub use geo::{GeoEngine, GeoMember, GeoPoint, GeoUnit};
pub use graph::{Direction, Edge, GraphEngine, Vertex};
pub use import::{CsvImportStats, JsonlImportStats};
pub use kv::KvEngine;
pub use kv::TtlCleaner;
pub use mq::{Message, MqEngine};
pub use server::{HttpServer, Protocol, ServerConfig, TalonUrl, TcpServer};
pub use sql::import::SqlImportStats;
pub use sql::SqlEngine;
pub use storage::{
    Batch, CacheStats, EvictionHandle, SegmentManager, Snapshot, StorageConfig, Store,
};
pub use ts::line_protocol::{parse_line_protocol, LineProtocolPoint};
pub use ts::{
    describe_timeseries, drop_timeseries, list_timeseries, start_ts_retention_cleaner, AggBucket,
    AggFunc, DataPoint, FillStrategy, TsAggQuery, TsEngine, TsInfo, TsQuery, TsRetentionCleaner,
    TsSchema,
};
pub use types::{ColumnType, Schema, Value};
pub use vector::metadata::{MetaFilter, MetaFilterOp, MetaValue};
pub use vector::VectorEngine;

// 嵌入式 JSON 命令入口（供 GUI 等嵌入场景使用）。
pub use ffi_exec::{execute_cmd, register_ai_handler, AiModuleHandler};

use std::sync::{Arc, Mutex, MutexGuard, RwLock};

/// 健康检查辅助：将 Result 转为 JSON 状态。
pub(crate) fn health_status(result: &Result<(), Error>, all_ok: &mut bool) -> serde_json::Value {
    match result {
        Ok(()) => serde_json::json!({"status": "ok"}),
        Err(e) => {
            *all_ok = false;
            serde_json::json!({"status": "error", "message": e.to_string()})
        }
    }
}

/// 安全获取 Mutex 锁；poison 时返回 Error::LockPoisoned 而非 panic。
pub(crate) fn lock_or_err<'a, T>(
    mutex: &'a Mutex<T>,
    name: &str,
) -> Result<MutexGuard<'a, T>, Error> {
    mutex
        .lock()
        .map_err(|_| Error::LockPoisoned(format!("{} mutex poisoned", name)))
}

/// 嵌入式公共 API：统一入口，暴露 KV / SQL / 时序 / 消息队列 / 向量。
/// P0：所有引擎单例化 + Mutex 保护，确保多线程并发安全。
pub struct Talon {
    store: Store,
    /// SQL 引擎（缓存 schema + keyspace 句柄）。
    sql_engine: Mutex<SqlEngine>,
    /// KV 引擎单例。M96：RwLock 支持并发读。
    kv_engine: RwLock<KvEngine>,
    /// MQ 引擎单例。
    mq_engine: Mutex<MqEngine>,
    /// 集群角色（Standalone / Primary / Replica）。
    /// RwLock 支持运行时 promote（Replica → Primary）。
    cluster_role: RwLock<ClusterRole>,
    /// OpLog（仅 Primary 模式启用；Standalone / Replica 为 None）。
    /// Arc 允许 ReplSender 共享访问；Mutex 支持运行时 promote 时创建。
    oplog: Mutex<Option<Arc<OpLog>>>,
    /// 从节点状态列表（由 ReplSender 共享写入，cluster_status 读取）。
    replica_infos: Arc<Mutex<Vec<ReplicaInfo>>>,
}

mod instance;

mod diagnostics;

#[cfg(test)]
mod tests_talon;
