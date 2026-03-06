/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine：缓存 schema + keyspace 的 SQL 执行引擎（构造/run_sql/事务/缓存）。

use std::collections::HashMap;
use std::sync::Arc;

use super::parser::{parse, unquote_ident, Stmt};
use crate::storage::{Keyspace, SegmentManager, Snapshot, Store};
use crate::types::{Schema, Value};
use crate::Error;

/// 行条目类型：(主键字节, 行数据)。
pub(super) type RowEntry = (Vec<u8>, Vec<Value>);

/// 事务写缓冲。
/// M82：新增 index_writes 缓冲索引写入，COMMIT 时统一刷出。
/// M95：新增 snapshot 快照读视图，BEGIN 时获取，事务内 SELECT 使用快照读。
pub(super) struct TxState {
    pub(super) writes: HashMap<(String, Vec<u8>), Option<Vec<u8>>>,
    /// (table, index_col, key) → Some(value) for insert, None for delete
    pub(super) index_writes: Vec<(String, String, Vec<u8>, bool)>,
    /// M95：事务开始时的跨 keyspace 快照，保证事务内读一致性。
    pub(super) snapshot: Snapshot,
    /// M110：SAVEPOINT 栈 — (name, writes_snapshot, index_writes_len)。
    /// ROLLBACK TO 时恢复 writes 并截断 index_writes。
    pub(super) savepoints: Vec<(String, HashMap<(String, Vec<u8>), Option<Vec<u8>>>, usize)>,
}

/// 表缓存条目。
pub(super) struct TableCache {
    pub(super) schema: Arc<Schema>,
    pub(super) data_ks: Keyspace,
    pub(super) index_keyspaces: HashMap<String, Keyspace>,
    /// M89：缓存该表是否有向量索引，避免每次 DML 都 open_keyspace 查询。
    pub(super) has_vec_indexes: bool,
    /// M111：唯一索引列名集合，INSERT/UPDATE 时检查唯一性。
    pub(super) unique_indexes: std::collections::HashSet<String>,
    /// M118：缓存解析后的 CHECK 约束表达式，避免每次 DML 重复解析。
    pub(super) parsed_checks: Vec<super::parser::WhereExpr>,
}

/// 列运行统计（SUM + COUNT），用于 O(1) 全表聚合。
#[derive(Debug, Clone, Default)]
pub(super) struct ColumnStats {
    pub(super) sum: f64,
    pub(super) count: i64,
    pub(super) is_int: bool,
}

/// SQL 引擎：缓存 schema/keyspace + SegmentManager LRU + BEGIN/COMMIT/ROLLBACK 事务。
pub struct SqlEngine {
    pub(super) store: Store,
    pub(super) meta_ks: Keyspace,
    pub(super) index_meta_ks: Keyspace,
    pub(super) cache: HashMap<String, TableCache>,
    pub(super) segments: SegmentManager,
    pub(super) tx: Option<TxState>,
    /// M93 方案B：表级列运行统计 (table → column → stats)。
    /// INSERT 时更新，SUM/AVG 无 WHERE 时 O(1) 读取。
    pub(super) column_stats: HashMap<String, HashMap<usize, ColumnStats>>,
    /// M125：视图嵌套深度计数器，防止循环引用导致栈溢出。
    pub(super) view_depth: usize,
    /// M126：临时表名称集合，引擎 drop 时自动清理。
    pub(super) temp_tables: std::collections::HashSet<String>,
}

impl SqlEngine {
    /// 创建 SQL 引擎（打开 meta keyspace 并缓存）。
    pub fn new(store: &Store) -> Result<Self, Error> {
        let meta_ks = store.open_keyspace("sql_meta")?;
        let index_meta_ks = store.open_keyspace("sql_index_meta")?;
        let segments = store.segment_manager().clone();
        Ok(SqlEngine {
            store: store.clone(),
            meta_ks,
            index_meta_ks,
            cache: HashMap::new(),
            segments,
            tx: None,
            column_stats: HashMap::new(),
            view_depth: 0,
            temp_tables: std::collections::HashSet::new(),
        })
    }

    /// 尝试所有预解析快速路径（事务命令、PK 点查、简单 INSERT 等）。
    /// 返回 Ok(Some(result)) 表示快速路径命中；Ok(None) 表示需要完整解析。
    pub(crate) fn try_fast_exec(&mut self, sql: &str) -> Result<Option<Vec<Vec<Value>>>, Error> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let tlen = trimmed.len();
        if tlen <= 20 {
            let upper = trimmed.to_uppercase();
            if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
                return self.exec_begin().map(Some);
            }
            if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
                return self.exec_commit().map(Some);
            }
            if upper == "ROLLBACK" || upper == "ABORT" {
                return self.exec_rollback().map(Some);
            }
            if upper == "SHOW TABLES" {
                return self.exec_show_tables().map(Some);
            }
        }
        let prefix24 = if tlen > 24 { &trimmed[..24] } else { trimmed };
        let prefix_upper = prefix24.to_uppercase();
        if prefix_upper.starts_with("SAVEPOINT ") {
            let name = unquote_ident(trimmed[9..].trim());
            if !name.is_empty() {
                return self.exec_savepoint(&name).map(Some);
            }
        }
        if prefix_upper.starts_with("RELEASE ") {
            let rest = trimmed[7..].trim();
            let rest = if rest.len() > 10 && rest[..10].eq_ignore_ascii_case("SAVEPOINT ") {
                rest[9..].trim()
            } else {
                rest
            };
            let name = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
            if !name.is_empty() {
                return self.exec_release(&name).map(Some);
            }
        }
        if prefix_upper.starts_with("ROLLBACK TO ") {
            let rest = trimmed[11..].trim();
            let rest = if rest.len() > 10 && rest[..10].eq_ignore_ascii_case("SAVEPOINT ") {
                rest[9..].trim()
            } else {
                rest
            };
            let name = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
            if !name.is_empty() {
                return self.exec_rollback_to(&name).map(Some);
            }
        }
        if prefix_upper.starts_with("DESCRIBE ") || prefix_upper.starts_with("DESC ") {
            let s = if prefix_upper.starts_with("DESC ") {
                4
            } else {
                8
            };
            let t = trimmed[s..].split_whitespace().next().unwrap_or("");
            if !t.is_empty() {
                let t = unquote_ident(t);
                return self.exec_describe(&t).map(Some);
            }
        }
        if prefix_upper.starts_with("EXPLAIN ") {
            let inner_sql = trimmed[7..].trim();
            return self.exec_explain(inner_sql).map(Some);
        }
        if let Some(result) = self.try_fast_pk_select(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_fast_insert(sql)? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    /// 执行一条 SQL（走缓存路径）。
    pub fn run_sql(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
        if let Some(result) = self.try_fast_exec(sql)? {
            return Ok(result);
        }
        let stmt = parse(sql)?;
        self.exec_stmt(stmt)
    }

    /// 参数化查询：解析 SQL 中的 `?` 占位符，绑定 `params` 后执行。
    ///
    /// 支持 INSERT / SELECT / UPDATE / DELETE 中的 `?` 占位符。
    /// 参数数量必须与 `?` 数量完全匹配，否则返回错误。
    ///
    /// ```text
    /// engine.run_sql_param("SELECT * FROM t WHERE id = ?", &[Value::Integer(1)])?;
    /// engine.run_sql_param("INSERT INTO t VALUES (?, ?)", &[Value::Integer(1), Value::Text("a".into())])?;
    /// ```
    pub fn run_sql_param(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, Error> {
        // PostgreSQL 兼容：将 $1, $2, ... 替换为 ?
        let sql = normalize_pg_placeholders(sql);
        let mut stmt = parse(&sql)?;
        super::bind::bind_params(&mut stmt, params)?;
        self.exec_stmt(stmt)
    }

    /// 执行已解析的 Stmt（run_sql 和 run_sql_param 共用）。
    pub(crate) fn exec_stmt(&mut self, stmt: Stmt) -> Result<Vec<Vec<Value>>, Error> {
        match stmt {
            Stmt::Union {
                left,
                right,
                all,
                op,
            } => {
                let mut left_rows = self.exec_stmt(*left)?;
                let right_rows = self.exec_stmt(*right)?;
                // 校验列数一致
                if let (Some(l), Some(r)) = (left_rows.first(), right_rows.first()) {
                    if l.len() != r.len() {
                        return Err(Error::SqlExec(format!(
                            "集合操作两侧列数不匹配: {} vs {}",
                            l.len(),
                            r.len()
                        )));
                    }
                }
                use super::parser::SetOpKind;
                match op {
                    SetOpKind::Union => {
                        if all {
                            left_rows.extend(right_rows);
                        } else {
                            let mut seen = std::collections::HashSet::new();
                            for row in &left_rows {
                                seen.insert(super::engine_groupby::values_to_bytes(row));
                            }
                            for row in right_rows {
                                if seen.insert(super::engine_groupby::values_to_bytes(&row)) {
                                    left_rows.push(row);
                                }
                            }
                        }
                        Ok(left_rows)
                    }
                    SetOpKind::Intersect => {
                        // 构建右表 HashSet
                        let mut right_set = std::collections::HashMap::<Vec<u8>, usize>::new();
                        for row in &right_rows {
                            *right_set
                                .entry(super::engine_groupby::values_to_bytes(row))
                                .or_insert(0) += 1;
                        }
                        let mut result = Vec::new();
                        if all {
                            // INTERSECT ALL：保留重复，每个值最多出现 min(left_count, right_count) 次
                            for row in left_rows {
                                let key = super::engine_groupby::values_to_bytes(&row);
                                if let Some(cnt) = right_set.get_mut(&key) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                        result.push(row);
                                    }
                                }
                            }
                        } else {
                            // INTERSECT：去重交集
                            let mut seen = std::collections::HashSet::new();
                            for row in left_rows {
                                let key = super::engine_groupby::values_to_bytes(&row);
                                if right_set.contains_key(&key) && seen.insert(key) {
                                    result.push(row);
                                }
                            }
                        }
                        Ok(result)
                    }
                    SetOpKind::Except => {
                        let mut right_set = std::collections::HashMap::<Vec<u8>, usize>::new();
                        for row in &right_rows {
                            *right_set
                                .entry(super::engine_groupby::values_to_bytes(row))
                                .or_insert(0) += 1;
                        }
                        let mut result = Vec::new();
                        if all {
                            // EXCEPT ALL：每个右表值消耗一个左表匹配
                            for row in left_rows {
                                let key = super::engine_groupby::values_to_bytes(&row);
                                if let Some(cnt) = right_set.get_mut(&key) {
                                    if *cnt > 0 {
                                        *cnt -= 1;
                                        continue;
                                    }
                                }
                                result.push(row);
                            }
                        } else {
                            // EXCEPT：去重差集
                            let mut seen = std::collections::HashSet::new();
                            for row in left_rows {
                                let key = super::engine_groupby::values_to_bytes(&row);
                                if !right_set.contains_key(&key) && seen.insert(key) {
                                    result.push(row);
                                }
                            }
                        }
                        Ok(result)
                    }
                }
            }
            Stmt::Begin => self.exec_begin(),
            Stmt::Commit => self.exec_commit(),
            Stmt::Rollback => self.exec_rollback(),
            Stmt::Savepoint { ref name } => self.exec_savepoint(name),
            Stmt::Release { ref name } => self.exec_release(name),
            Stmt::RollbackTo { ref name } => self.exec_rollback_to(name),
            Stmt::CreateTable {
                name,
                columns,
                if_not_exists,
                unique_constraints,
                check_constraints,
                temporary,
                foreign_keys,
            } => self.exec_create_table(
                name,
                columns,
                if_not_exists,
                unique_constraints,
                check_constraints,
                temporary,
                foreign_keys,
            ),
            Stmt::DropTable { name, if_exists } => self.exec_drop_table(&name, if_exists),
            Stmt::Insert {
                table,
                columns,
                values,
                or_replace,
                or_ignore,
                on_conflict,
                returning,
                source_select,
            } => {
                // M102: INSERT INTO ... SELECT — 先执行 SELECT 获取行数据
                let values = if let Some(sel) = source_select {
                    let rows = self.exec_stmt(*sel)?;
                    if !columns.is_empty() {
                        self.map_insert_columns(&table, &columns, rows)?
                    } else {
                        rows
                    }
                } else {
                    self.map_insert_columns(&table, &columns, values)?
                };
                self.exec_insert(
                    &table,
                    values.clone(),
                    or_replace,
                    or_ignore,
                    on_conflict.as_ref(),
                )?;
                if let Some(ref ret_cols) = returning {
                    Ok(apply_returning(&table, &values, ret_cols, &self.cache))
                } else {
                    Ok(vec![])
                }
            }
            Stmt::Select {
                table,
                columns,
                where_clause,
                order_by,
                limit,
                offset,
                distinct,
                distinct_on,
                vec_search,
                geo_search,
                join,
                group_by,
                having,
                ctes,
                window_functions,
            } => {
                // M125：视图解析 — 直接执行视图 SQL，在结果上应用外层条件
                if let Some(view_sql) = self.resolve_view(&table, self.view_depth)? {
                    return self.exec_view_select(
                        &view_sql,
                        &columns,
                        where_clause.as_ref(),
                        order_by.as_deref(),
                        limit,
                        offset,
                        distinct,
                    );
                }
                // M113：CTE 预执行 — 将 CTE 查询结果物化为临时表
                if !ctes.is_empty() {
                    return self.exec_with_ctes(
                        ctes,
                        Stmt::Select {
                            table,
                            columns,
                            where_clause,
                            order_by,
                            limit,
                            offset,
                            distinct,
                            distinct_on,
                            vec_search,
                            geo_search,
                            join,
                            group_by,
                            having,
                            ctes: vec![],
                            window_functions: vec![],
                        },
                    );
                }
                if let Some(ref jc) = join {
                    return self.exec_select_join(
                        &table,
                        &columns,
                        jc,
                        where_clause.as_ref(),
                        order_by.as_deref(),
                        limit,
                        offset,
                    );
                }
                if let Some(ref gs) = geo_search {
                    let star = vec!["*".to_string()];
                    let all_rows = self.exec_select(
                        &table,
                        &star,
                        where_clause.as_ref(),
                        None,
                        None,
                        None,
                        distinct,
                        vec_search.as_ref(),
                        None,
                    )?;
                    return self.apply_geo_search(
                        all_rows,
                        &table,
                        gs,
                        &columns,
                        order_by.as_deref(),
                        limit,
                        offset,
                    );
                }
                // 子查询预解析：WHERE x IN (SELECT ...) → 执行子查询并替换为值列表
                let resolved_where;
                let where_ref = if let Some(ref wc) = where_clause {
                    if has_subquery(wc) {
                        resolved_where = Some(self.resolve_subqueries(wc)?);
                        resolved_where.as_ref()
                    } else {
                        where_clause.as_ref()
                    }
                } else {
                    None
                };
                // GROUP BY 路径
                if let Some(ref gb_cols) = group_by {
                    let result = self.exec_group_by(
                        &table,
                        &columns,
                        where_ref,
                        gb_cols,
                        having.as_ref(),
                        order_by.as_deref(),
                        limit,
                    )?;
                    if !window_functions.is_empty() {
                        if !self.ensure_cached(&table)? {
                            return Err(Error::SqlExec(format!("表不存在: {}", table)));
                        }
                        let schema = self.cache.get(&table).unwrap().schema.clone();
                        return super::engine_window::apply_window_functions(
                            result,
                            &schema,
                            &window_functions,
                        );
                    }
                    return Ok(result);
                }
                // M177：窗口函数路径 — 先取全部列，再计算窗口函数，最后投影
                if !window_functions.is_empty() {
                    let star = vec!["*".to_string()];
                    let all_rows = self.exec_select(
                        &table,
                        &star,
                        where_ref,
                        order_by.as_deref(),
                        None, // 窗口函数需要全部行，LIMIT 在后面应用
                        None,
                        distinct,
                        vec_search.as_ref(),
                        distinct_on.as_deref(),
                    )?;
                    if !self.ensure_cached(&table)? {
                        return Err(Error::SqlExec(format!("表不存在: {}", table)));
                    }
                    let schema = self.cache.get(&table).unwrap().schema.clone();
                    let mut expanded = super::engine_window::apply_window_functions(
                        all_rows,
                        &schema,
                        &window_functions,
                    )?;
                    // 应用 OFFSET + LIMIT
                    if let Some(off) = offset {
                        let off = off as usize;
                        if off >= expanded.len() {
                            expanded.clear();
                        } else {
                            expanded = expanded.split_off(off);
                        }
                    }
                    if let Some(n) = limit {
                        expanded.truncate(n as usize);
                    }
                    // 投影：只保留用户请求的列（含窗口别名）
                    return super::engine_window::project_with_window(
                        expanded,
                        &columns,
                        &schema,
                        &window_functions,
                    );
                }
                self.exec_select(
                    &table,
                    &columns,
                    where_ref,
                    order_by.as_deref(),
                    limit,
                    offset,
                    distinct,
                    vec_search.as_ref(),
                    distinct_on.as_deref(),
                )
            }
            Stmt::Delete {
                table,
                where_clause,
                returning,
                using_table,
            } => {
                // M152：子查询预解析
                let resolved_where;
                let wc_ref = if let Some(ref wc) = where_clause {
                    if has_subquery(wc) {
                        resolved_where = Some(self.resolve_subqueries(wc)?);
                        resolved_where.as_ref()
                    } else {
                        where_clause.as_ref()
                    }
                } else {
                    None
                };
                // M163: DELETE ... USING 多表删除
                if let Some(ref src) = using_table {
                    self.exec_delete_using(&table, src, wc_ref)
                } else {
                    let result = self.exec_delete(&table, wc_ref, returning.as_deref())?;
                    Ok(result)
                }
            }
            Stmt::Update {
                table,
                assignments,
                where_clause,
                returning,
                from_table,
                order_by,
                limit,
            } => {
                // M152：子查询预解析
                let resolved_where;
                let wc_ref = if let Some(ref wc) = where_clause {
                    if has_subquery(wc) {
                        resolved_where = Some(self.resolve_subqueries(wc)?);
                        resolved_where.as_ref()
                    } else {
                        where_clause.as_ref()
                    }
                } else {
                    None
                };
                let result = if let Some(ref src) = from_table {
                    self.exec_update_from(&table, src, &assignments, wc_ref)?
                } else {
                    self.exec_update(&table, &assignments, wc_ref, order_by.as_deref(), limit)?
                };
                if let Some(ref ret_cols) = returning {
                    return self.exec_select(
                        &table, ret_cols, wc_ref, None, None, None, false, None, None,
                    );
                }
                Ok(result)
            }
            Stmt::CreateIndex {
                index_name,
                table,
                columns,
                unique,
            } => self.exec_create_index(index_name, &table, &columns, unique),
            Stmt::AlterTable { table, action } => self.exec_alter_table(&table, action),
            Stmt::ShowTables => self.exec_show_tables(),
            Stmt::ShowIndexes { table } => self.exec_show_indexes(table.as_deref()),
            Stmt::Describe { table } => self.exec_describe(&table),
            Stmt::CreateVectorIndex {
                index_name,
                table,
                column,
                metric,
                m,
                ef_construction,
            } => self.exec_create_vector_index(
                &index_name,
                &table,
                &column,
                &metric,
                m,
                ef_construction,
            ),
            Stmt::DropVectorIndex {
                index_name,
                if_exists,
            } => self.exec_drop_vector_index(&index_name, if_exists),
            Stmt::DropIndex {
                index_name,
                if_exists,
            } => self.exec_drop_index(&index_name, if_exists),
            Stmt::Truncate { table } => self.exec_truncate(&table),
            Stmt::Explain { inner } => self.exec_explain_stmt(*inner),
            Stmt::CreateView {
                name,
                if_not_exists,
                sql,
            } => self.exec_create_view(&name, if_not_exists, &sql),
            Stmt::DropView { name, if_exists } => self.exec_drop_view(&name, if_exists),
            Stmt::Comment {
                table,
                column,
                text,
            } => self.exec_comment(&table, column.as_deref(), &text),
        }
    }

    /// 查询是否在事务中。
    pub fn in_transaction(&self) -> bool {
        self.tx.is_some()
    }

    /// SQL 引擎磁盘空间占用（字节）。
    /// 包含 meta keyspace + 所有已缓存表数据 keyspace + 索引 keyspace。
    pub fn disk_space(&self) -> u64 {
        let mut total = self.meta_ks.disk_space() + self.index_meta_ks.disk_space();
        for tc in self.cache.values() {
            total += tc.data_ks.disk_space();
            for idx_ks in tc.index_keyspaces.values() {
                total += idx_ks.disk_space();
            }
        }
        total
    }

    /// 确保表缓存已加载；返回是否存在。
    pub(super) fn ensure_cached(&mut self, table: &str) -> Result<bool, Error> {
        if self.cache.contains_key(table) {
            let seg_key = format!("sql:{}:schema", table);
            let _ = self.segments.get(&seg_key);
            return Ok(true);
        }
        let raw = self.meta_ks.get(table.as_bytes())?;
        let Some(raw) = raw else { return Ok(false) };
        let mut schema: Schema =
            serde_json::from_slice(&raw).map_err(|e| Error::Serialization(e.to_string()))?;
        schema.ensure_defaults();
        let data_ks = self.store.open_keyspace(&format!("sql_{}", table))?;
        let prefix = format!("idx:{}:", table);
        let mut index_keyspaces = HashMap::new();
        let mut unique_indexes = std::collections::HashSet::new();
        for key in &self.index_meta_ks.keys_with_prefix(prefix.as_bytes())? {
            if let Some(col) = String::from_utf8_lossy(key)
                .strip_prefix(&prefix)
                .map(|s| s.to_string())
            {
                // M111：检查元数据 value 是否以 "u:" 前缀标记唯一索引
                if let Some(val) = self.index_meta_ks.get(key)? {
                    if val.starts_with(b"u:") {
                        unique_indexes.insert(col.clone());
                    }
                }
                let ks = self.store.open_keyspace(&format!(
                    "idx_{}_{}",
                    table,
                    col.replace(',', "_")
                ))?;
                index_keyspaces.insert(col, ks);
            }
        }
        let has_vec = !super::vec_idx::list_vec_indexes(&self.store, table)?.is_empty();
        // M118：解析 CHECK 约束字符串为 WhereExpr 并缓存
        let mut parsed_checks = Vec::with_capacity(schema.check_constraints.len());
        for chk_sql in &schema.check_constraints {
            let expr = super::parser::where_clause::parse_where(chk_sql)
                .map_err(|e| Error::SqlExec(format!("CHECK 约束解析失败: {}: {}", chk_sql, e)))?;
            parsed_checks.push(expr);
        }
        let seg_key = format!("sql:{}:schema", table);
        self.segments.put(seg_key, raw.to_vec());
        self.cache.insert(
            table.to_string(),
            TableCache {
                schema: Arc::new(schema),
                data_ks,
                index_keyspaces,
                has_vec_indexes: has_vec,
                unique_indexes,
                parsed_checks,
            },
        );
        Ok(true)
    }
    /// R-STATS-1: DELETE/UPDATE 后使指定表的列统计失效。
    /// 下次 SUM/AVG 查询将回退到全扫描重新计算。
    pub(super) fn invalidate_stats(&mut self, table: &str) {
        self.column_stats.remove(table);
    }
}

pub(super) use super::engine_agg_stats::accumulate_stats;
use super::engine_utils::{apply_returning, has_subquery, normalize_pg_placeholders};

/// M126：引擎销毁时自动清理临时表数据和 schema。
impl Drop for SqlEngine {
    fn drop(&mut self) {
        for name in self.temp_tables.drain() {
            let _ = self.meta_ks.delete(name.as_bytes());
            let counter_key = format!("autoincr:{}", name);
            let _ = self.meta_ks.delete(counter_key.as_bytes());
            // 清空数据 keyspace
            let ks_name = format!("sql_{}", name);
            if let Ok(ks) = self.store.open_keyspace(&ks_name) {
                let mut keys = Vec::new();
                let _ = ks.for_each_key_prefix(b"", |k| {
                    keys.push(k.to_vec());
                    true
                });
                if !keys.is_empty() {
                    let mut batch = self.store.batch();
                    for k in &keys {
                        batch.remove(&ks, k.clone());
                    }
                    let _ = batch.commit();
                }
            }
            self.cache.remove(&name);
        }
    }
}
