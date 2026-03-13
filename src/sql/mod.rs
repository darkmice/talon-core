/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 关系型 SQL 引擎：解析（DDL/DML 子集）、Planner、Executor、CRUD。
//!
//! M1.4 实现；依赖 storage + types。
//! M8 新增 SqlEngine：缓存 schema + keyspace 句柄，避免每次查询重复加载。
//! M8.1 兼容主流 SQL 写法。
//! M8.2 快速路径：对 SELECT * FROM t WHERE pk=val 和简单 INSERT 跳过完整解析。

pub(crate) mod bind;
mod engine;
mod engine_agg;
mod engine_agg_acc;
mod engine_agg_stats;
mod engine_analyze;
mod engine_batch;
mod engine_cte;
mod engine_ddl;
mod engine_delete;
mod engine_delete_using;
mod engine_exec;
mod engine_explain;
mod engine_fast;
mod engine_fk;
mod engine_geo;
mod engine_groupby;
mod engine_join;
mod engine_scan;
mod engine_select;
mod engine_tx;
mod engine_update;
mod engine_update_from;
mod engine_upsert;
pub(crate) mod engine_utils;
mod engine_view;
mod engine_window;
mod executor;
mod executor_ddl;
mod executor_select;
mod expr_eval;
mod geo;
mod helpers;
pub mod import;
mod index_key;
mod optimizer;
pub(crate) mod parser;
mod planner;
mod sql_funcs;
mod sql_funcs_dt;
mod sql_funcs_hash;
mod sql_funcs_json;
mod topn;
mod vec_idx;
mod vec_search;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_advanced;
#[cfg(test)]
mod tests_bench_mem;
#[cfg(test)]
mod tests_bench_rss;
#[cfg(test)]
mod tests_case_when;
#[cfg(test)]
mod tests_constraints;
#[cfg(test)]
mod tests_ddl_m37;
#[cfg(test)]
mod tests_fk;
#[cfg(test)]
mod tests_ignore;
#[cfg(test)]
mod tests_join;
#[cfg(test)]
mod tests_jsonb;
#[cfg(test)]
mod tests_migrate_compat;
#[cfg(test)]
mod tests_perf;
#[cfg(test)]
mod tests_query_opt;
#[cfg(test)]
mod tests_replace;
#[cfg(test)]
mod tests_snapshot;
#[cfg(test)]
mod tests_sql_funcs;
#[cfg(test)]
mod tests_subquery;
#[cfg(test)]
mod tests_temp_table;
#[cfg(test)]
mod tests_upsert;
#[cfg(test)]
mod tests_vec_idx;
#[cfg(test)]
mod tests_view;
#[cfg(test)]
mod tests_window;
#[cfg(test)]
mod tests_date_time;

pub use engine::SqlEngine;
pub use executor::execute;
pub use parser::{
    parse, AlterAction, ColumnDef, OnConflict, OnConflictValue, Stmt, VecSearchExpr,
    WhereCondition, WhereExpr, WhereOp,
};
pub use planner::{plan, Plan};

use crate::storage::Store;
use crate::types::Value;
use crate::Error;

/// 解析 SQL 并执行（无缓存路径）；返回结果行（每行为 `Vec<Value>`）。
pub fn run(store: &Store, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
    let stmt = parse(sql)?;
    let plan = plan(stmt);
    execute(store, plan)
}
