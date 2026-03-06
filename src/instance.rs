/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Talon 实例方法实现。
//!
//! 拆分自 lib.rs，包含 Talon struct 的所有 impl 方法：
//! 构造、引擎访问、集群管理、SQL 执行、批量操作、备份导入导出。

use std::path::Path;
use std::sync::{Arc, MutexGuard, RwLockReadGuard, RwLockWriteGuard};

use crate::backup;
use crate::cluster::ReplicaInfo;
use crate::cluster::{
    ClusterConfig, ClusterRole, ClusterStatus, OpLog, OpLogConfig, OpLogEntry, Operation,
};
use crate::error::Error;
use crate::fts::FtsEngine;
use crate::geo::GeoEngine;
use crate::graph::GraphEngine;
use crate::kv::KvEngine;
use crate::mq::MqEngine;
use crate::sql::import::SqlImportStats;
use crate::sql::SqlEngine;
use crate::storage::{EvictionHandle, SegmentManager, StorageConfig, Store};
use crate::ts::{TsEngine, TsSchema};
use crate::types::Value;
use crate::vector::VectorEngine;
use crate::{lock_or_err, Talon};

impl Talon {
    /// 打开或创建数据库目录（使用默认配置）。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with_config(path, StorageConfig::default())
    }

    /// 打开或创建数据库目录，使用自定义存储配置。
    pub fn open_with_config(path: impl AsRef<Path>, config: StorageConfig) -> Result<Self, Error> {
        Self::open_full(path, config, ClusterConfig::default())
    }

    /// 打开数据库，指定集群配置。
    pub fn open_with_cluster(
        path: impl AsRef<Path>,
        storage_config: StorageConfig,
        cluster_config: ClusterConfig,
    ) -> Result<Self, Error> {
        Self::open_full(path, storage_config, cluster_config)
    }

    /// 内部统一构造方法。
    fn open_full(
        path: impl AsRef<Path>,
        storage_config: StorageConfig,
        cluster_config: ClusterConfig,
    ) -> Result<Self, Error> {
        let store = Store::open_with_config(path, storage_config)?;
        let sql_engine = SqlEngine::new(&store)?;
        let kv_engine = KvEngine::open(&store)?;
        let mq_engine = MqEngine::open(&store)?;

        // Primary 模式启用 OpLog
        let oplog = if matches!(cluster_config.role, ClusterRole::Primary) {
            Some(Arc::new(OpLog::open(&store, cluster_config.oplog.clone())?))
        } else {
            None
        };

        Ok(Talon {
            store,
            sql_engine: std::sync::Mutex::new(sql_engine),
            kv_engine: std::sync::RwLock::new(kv_engine),
            mq_engine: std::sync::Mutex::new(mq_engine),
            cluster_role: std::sync::RwLock::new(cluster_config.role),
            oplog: std::sync::Mutex::new(oplog),
            replica_infos: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    /// 判断 SQL 是否为写操作（INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE）。
    fn is_write_sql(sql: &str) -> bool {
        let t = sql.trim_start().as_bytes();
        fn prefix_ci(haystack: &[u8], needle: &[u8]) -> bool {
            haystack.len() >= needle.len()
                && haystack[..needle.len()]
                    .iter()
                    .zip(needle)
                    .all(|(a, b)| a.to_ascii_uppercase() == *b)
        }
        prefix_ci(t, b"INSERT")
            || prefix_ci(t, b"UPDATE")
            || prefix_ci(t, b"DELETE")
            || prefix_ci(t, b"CREATE")
            || prefix_ci(t, b"DROP")
            || prefix_ci(t, b"ALTER")
            || prefix_ci(t, b"TRUNCATE")
    }

    /// 只读检查：Replica 节点拒绝写操作。
    fn guard_readonly(&self, op: &str) -> Result<(), Error> {
        let role = self
            .cluster_role
            .read()
            .map_err(|_| Error::LockPoisoned("cluster_role rwlock".into()))?;
        if role.is_readonly() {
            return Err(Error::ReadOnly(format!(
                "写操作 {} 被拒绝：当前节点为 Replica 只读模式",
                op
            )));
        }
        Ok(())
    }

    /// 获取底层存储引擎引用（供 FTS / GEO 等引擎直接访问）。
    pub fn store_ref(&self) -> &Store {
        &self.store
    }

    /// 当前集群角色（克隆返回，线程安全）。
    pub fn cluster_role(&self) -> ClusterRole {
        self.cluster_role
            .read()
            .map(|r| r.clone())
            .unwrap_or(ClusterRole::Standalone)
    }

    /// 获取集群状态快照（用于 `/cluster/status` API）。
    pub fn cluster_status(&self) -> ClusterStatus {
        let (current_lsn, min_lsn, oplog_entries) = self
            .oplog
            .lock()
            .ok()
            .and_then(|g| {
                g.as_ref()
                    .map(|o| (o.current_lsn(), o.min_lsn(), o.entry_count()))
            })
            .unwrap_or((0, 0, 0));
        ClusterStatus {
            role: self.cluster_role(),
            current_lsn,
            min_lsn,
            oplog_entries,
            replicas: self
                .replica_infos
                .lock()
                .map(|g| g.clone())
                .unwrap_or_default(),
        }
    }

    /// 追加一条操作到 OpLog（仅 Primary 模式生效，其他模式静默忽略）。
    pub fn append_oplog(&self, op: Operation) -> Result<u64, Error> {
        let guard = lock_or_err(&self.oplog, "oplog")?;
        match guard.as_ref() {
            Some(oplog) => oplog.append(op),
            None => Ok(0),
        }
    }

    /// OpLog 是否已启用（Primary 模式）。
    pub fn has_oplog(&self) -> bool {
        self.oplog.lock().ok().map(|g| g.is_some()).unwrap_or(false)
    }

    /// 获取 OpLog 当前 LSN（线程安全）。
    pub fn oplog_current_lsn(&self) -> u64 {
        self.oplog
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|o| o.current_lsn()))
            .unwrap_or(0)
    }

    /// 读取指定 LSN 的 OpLog 条目（线程安全）。
    pub fn oplog_get(&self, lsn: u64) -> Result<Option<OpLogEntry>, Error> {
        let guard = lock_or_err(&self.oplog, "oplog")?;
        match guard.as_ref() {
            Some(oplog) => oplog.get(lsn),
            None => Ok(None),
        }
    }

    /// 读取 OpLog 范围条目 `(from_lsn, to_lsn]`（线程安全）。
    pub fn oplog_range(
        &self,
        from_lsn: u64,
        to_lsn: u64,
        limit: usize,
    ) -> Result<Vec<OpLogEntry>, Error> {
        let guard = lock_or_err(&self.oplog, "oplog")?;
        match guard.as_ref() {
            Some(oplog) => oplog.range(from_lsn, to_lsn, limit),
            None => Ok(Vec::new()),
        }
    }

    /// 手动故障转移：将当前节点提升为 Primary。
    pub fn promote(&self) -> Result<(), Error> {
        let mut role = self
            .cluster_role
            .write()
            .map_err(|_| Error::LockPoisoned("cluster_role rwlock".into()))?;
        if !role.is_readonly() {
            return Err(Error::Config("仅 Replica 节点可执行 promote".into()));
        }
        let mut oplog_guard = lock_or_err(&self.oplog, "oplog")?;
        if oplog_guard.is_none() {
            *oplog_guard = Some(Arc::new(OpLog::open(&self.store, OpLogConfig::default())?));
        }
        *role = ClusterRole::Primary;
        Ok(())
    }

    /// 获取 OpLog 的 Arc 引用（供 ReplSender 共享访问）。
    pub fn oplog_arc(&self) -> Option<Arc<OpLog>> {
        self.oplog.lock().ok().and_then(|g| g.as_ref().cloned())
    }

    /// 获取从节点状态列表的 Arc 引用（供 ReplSender 共享写入）。
    pub fn replica_infos_arc(&self) -> Arc<std::sync::Mutex<Vec<ReplicaInfo>>> {
        Arc::clone(&self.replica_infos)
    }

    /// 获取 KV 引擎写锁。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn kv(&self) -> Result<RwLockWriteGuard<'_, KvEngine>, Error> {
        self.guard_readonly("kv_write")?;
        self.kv_engine
            .write()
            .map_err(|_| Error::LockPoisoned("kv rwlock poisoned".into()))
    }

    /// M96：获取 KV 引擎读锁（多个读操作可并发执行）。
    pub fn kv_read(&self) -> Result<RwLockReadGuard<'_, KvEngine>, Error> {
        self.kv_engine
            .read()
            .map_err(|_| Error::LockPoisoned("kv rwlock poisoned".into()))
    }

    /// 执行一条 SQL。Replica 节点仅允许读操作。
    /// 快速路径（事务命令、PK 点查、简单 INSERT）在锁内直接执行；
    /// 非快速路径查询在锁外解析 SQL，减少锁持有时间。
    pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
        let is_write = Self::is_write_sql(sql);
        if self.cluster_role().is_readonly() && is_write {
            return Err(Error::ReadOnly(
                "写 SQL 被拒绝：当前节点为 Replica 只读模式".into(),
            ));
        }
        // Phase 1: 尝试快速路径（锁内短暂持有）
        {
            let mut eng = lock_or_err(&self.sql_engine, "sql")?;
            match eng.try_fast_exec(sql) {
                Ok(Some(result)) => {
                    if is_write {
                        let _ = self.append_oplog(Operation::SqlDdl {
                            sql: sql.to_string(),
                        });
                    }
                    return Ok(result);
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        } // 锁释放

        // Phase 2: 快速路径未命中 — 在锁外解析 SQL
        let stmt = crate::sql::parser::parse(sql)?;

        // Phase 3: 锁内执行已解析的语句
        let mut eng = lock_or_err(&self.sql_engine, "sql")?;
        let result = eng.exec_stmt(stmt)?;
        if is_write {
            let _ = self.append_oplog(Operation::SqlDdl {
                sql: sql.to_string(),
            });
        }
        Ok(result)
    }

    /// 参数化 SQL 查询：`?` 占位符绑定实际值后执行。
    /// SQL 解析和参数绑定在锁外完成，减少锁持有时间。
    pub fn run_sql_param(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, Error> {
        let is_write = Self::is_write_sql(sql);
        if self.cluster_role().is_readonly() && is_write {
            return Err(Error::ReadOnly(
                "写 SQL 被拒绝：当前节点为 Replica 只读模式".into(),
            ));
        }
        // 锁外解析 + 参数绑定
        let normalized = crate::sql::engine_utils::normalize_pg_placeholders(sql);
        let mut stmt = crate::sql::parser::parse(&normalized)?;
        crate::sql::bind::bind_params(&mut stmt, params)?;
        // 锁内执行
        let mut eng = lock_or_err(&self.sql_engine, "sql")?;
        let result = eng.exec_stmt(stmt)?;
        if is_write {
            let _ = self.append_oplog(Operation::SqlDdl {
                sql: sql.to_string(),
            });
        }
        Ok(result)
    }

    /// 批量执行多条 SQL — 单次获取锁，所有语句共享同一引擎实例。
    #[allow(clippy::type_complexity)]
    pub fn run_sql_batch(
        &self,
        sqls: &[&str],
    ) -> Result<Vec<Result<Vec<Vec<Value>>, Error>>, Error> {
        self.guard_readonly("run_sql_batch")?;
        let mut eng = lock_or_err(&self.sql_engine, "sql")?;
        let mut results = Vec::with_capacity(sqls.len());
        for sql in sqls {
            results.push(eng.run_sql(sql));
        }
        Ok(results)
    }

    /// M93：原生批量插入 — 跳过 SQL 解析，直接 encode + WriteBatch。
    pub fn batch_insert_rows(
        &self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<Value>>,
    ) -> Result<(), Error> {
        self.guard_readonly("batch_insert_rows")?;
        let mut eng = lock_or_err(&self.sql_engine, "sql")?;
        eng.batch_insert_rows(table, columns, rows)
    }

    /// 从 SQL dump 流导入数据（支持 SQLite `.dump` 格式）。
    pub fn import_sql(&self, reader: impl std::io::BufRead) -> Result<SqlImportStats, Error> {
        self.guard_readonly("import_sql")?;
        let mut eng = lock_or_err(&self.sql_engine, "sql")?;
        eng.import_sql_stream(reader)
    }

    /// 从文件路径导入 SQL dump。
    pub fn import_sql_file(&self, path: impl AsRef<Path>) -> Result<SqlImportStats, Error> {
        let file = std::fs::File::open(path.as_ref())?;
        let reader = std::io::BufReader::new(file);
        self.import_sql(reader)
    }

    /// 获取向量引擎（写操作）。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn vector(&self, name: &str) -> Result<VectorEngine, Error> {
        self.guard_readonly("vector")?;
        VectorEngine::open(&self.store, name)
    }

    /// 获取向量引擎（只读）。Replica 节点可用。
    pub fn vector_read(&self, name: &str) -> Result<VectorEngine, Error> {
        VectorEngine::open(&self.store, name)
    }

    /// 设置向量索引的运行时搜索宽度 ef_search。
    pub fn vector_set_ef_search(&self, name: &str, ef_search: usize) -> Result<(), Error> {
        let ve = VectorEngine::open(&self.store, name)?;
        ve.set_ef_search(ef_search)
    }

    /// 创建时序表。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn create_timeseries(&self, name: &str, schema: TsSchema) -> Result<TsEngine, Error> {
        self.guard_readonly("create_timeseries")?;
        TsEngine::create(&self.store, name, schema)
    }

    /// 打开已有时序表。
    pub fn open_timeseries(&self, name: &str) -> Result<TsEngine, Error> {
        TsEngine::open(&self.store, name)
    }

    /// 获取 MQ 引擎锁保护引用（写操作）。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn mq(&self) -> Result<MutexGuard<'_, MqEngine>, Error> {
        self.guard_readonly("mq")?;
        lock_or_err(&self.mq_engine, "mq")
    }

    /// 获取 MQ 引擎锁保护引用（只读）。Replica 节点可用。
    pub fn mq_read(&self) -> Result<MutexGuard<'_, MqEngine>, Error> {
        lock_or_err(&self.mq_engine, "mq")
    }

    /// 获取全文搜索引擎（写操作）。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn fts(&self) -> Result<FtsEngine, Error> {
        self.guard_readonly("fts")?;
        FtsEngine::open(&self.store)
    }

    /// 获取全文搜索引擎（只读）。Replica 节点可用。
    pub fn fts_read(&self) -> Result<FtsEngine, Error> {
        FtsEngine::open(&self.store)
    }

    /// 获取 GEO 地理引擎（写操作）。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn geo(&self) -> Result<GeoEngine, Error> {
        self.guard_readonly("geo")?;
        GeoEngine::open(&self.store)
    }

    /// 获取 GEO 地理引擎（只读）。Replica 节点可用。
    pub fn geo_read(&self) -> Result<GeoEngine, Error> {
        GeoEngine::open(&self.store)
    }

    /// 获取 Graph 图引擎（写操作）。Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn graph(&self) -> Result<GraphEngine, Error> {
        self.guard_readonly("graph")?;
        GraphEngine::open(&self.store)
    }

    /// 获取 Graph 图引擎（只读）。Replica 节点可用。
    pub fn graph_read(&self) -> Result<GraphEngine, Error> {
        GraphEngine::open(&self.store)
    }

    /// 获取底层 Store 引用（等同于 `store_ref()`）。
    pub fn store(&self) -> &Store {
        self.store_ref()
    }

    /// 创建原子批量写入句柄；可跨引擎写入，commit 时一次性提交。
    pub fn batch(&self) -> crate::Batch {
        self.store.batch()
    }

    /// 刷盘，保证此前写入持久化。
    pub fn persist(&self) -> Result<(), Error> {
        self.store.persist()
    }

    /// 获取统一段管理器引用（用于查看缓存统计或手动淘汰）。
    pub fn segment_manager(&self) -> &SegmentManager {
        self.store.segment_manager()
    }

    /// 启动后台 LRU 淘汰线程；返回句柄，drop 时自动停止。
    pub fn start_eviction(&self) -> EvictionHandle {
        self.store.segment_manager().start_eviction()
    }

    /// 导出指定 keyspace 列表到目录。
    pub fn export(
        &self,
        dir: impl AsRef<std::path::Path>,
        keyspace_names: &[&str],
    ) -> Result<u64, Error> {
        backup::export_db(&self.store, dir, keyspace_names)
    }

    /// 从目录导入所有 .jsonl 文件到对应 keyspace。
    /// Replica 节点调用会返回 `Error::ReadOnly`。
    pub fn import(&self, dir: impl AsRef<std::path::Path>) -> Result<u64, Error> {
        self.guard_readonly("import")?;
        backup::import_db(&self.store, dir)
    }
}
