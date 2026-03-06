/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! EXPLAIN 查询计划分析器。
//!
//! M73：分析 SELECT 查询的执行计划，返回可读的计划行。
//! 不执行查询，仅静态分析。

use super::engine::SqlEngine;
use super::helpers::single_eq_condition;
use super::parser::{parse, Stmt, WhereExpr, WhereOp};
use crate::types::Value;
use crate::Error;

impl SqlEngine {
    /// EXPLAIN SQL — 从原始 SQL 字符串解析后分析。
    pub(super) fn exec_explain(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, Error> {
        let stmt = parse(sql)?;
        self.exec_explain_stmt(stmt)
    }

    /// EXPLAIN 已解析语句。
    pub(super) fn exec_explain_stmt(&mut self, stmt: Stmt) -> Result<Vec<Vec<Value>>, Error> {
        match stmt {
            Stmt::Select {
                table,
                columns,
                where_clause,
                order_by,
                limit,
                offset,
                distinct,
                vec_search,
                geo_search: _,
                join: _,
                group_by: _,
                having: _,
                ctes: _,
                window_functions: _,
                distinct_on: _,
            } => self.explain_select(
                &table,
                &columns,
                where_clause.as_ref(),
                order_by.as_deref(),
                limit,
                offset,
                distinct,
                vec_search.is_some(),
            ),
            other => {
                let desc = format!("{:?}", other);
                let kind = desc.split(['{', '(']).next().unwrap_or("Unknown");
                Ok(vec![vec![Value::Text(format!("Plan: {}", kind.trim()))]])
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn explain_select(
        &mut self,
        table: &str,
        columns: &[String],
        where_clause: Option<&WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
        distinct: bool,
        has_vec_search: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let mut plan = Vec::new();
        let row = |s: String| vec![Value::Text(s)];

        plan.push(row(format!("Table: {}", table)));

        // 检查表是否存在
        if !self.ensure_cached(table)? {
            plan.push(row("Error: table not found".to_string()));
            return Ok(plan);
        }
        let tc = self.cache.get(table).unwrap();
        let schema = &tc.schema;
        let pk_col = &schema.columns[0].0;
        let has_indexes: Vec<String> = tc.index_keyspaces.keys().cloned().collect();

        // 列投影
        let is_count_star = columns.len() == 1 && columns[0].eq_ignore_ascii_case("COUNT(*)");
        if is_count_star {
            plan.push(row(
                "Access: COUNT(*) fast path (streaming count_prefix, O(1) memory)".to_string(),
            ));
            return Ok(plan);
        }

        // 向量搜索
        if has_vec_search {
            plan.push(row("Access: Vector HNSW index scan".to_string()));
            if let Some(n) = limit {
                plan.push(row(format!("Limit: {} (top-K in HNSW)", n)));
            }
            return Ok(plan);
        }

        // WHERE 分析
        if let Some(expr) = where_clause {
            if let Some((col, _val)) = single_eq_condition(expr) {
                if col == *pk_col {
                    plan.push(row(format!("Access: PK point lookup (column: {})", col)));
                } else if has_indexes.iter().any(|c| c.as_str() == col) {
                    plan.push(row(format!(
                        "Access: Index scan (column: {}, index: idx_{}_{})",
                        col, table, col
                    )));
                } else {
                    plan.push(row(format!(
                        "Access: Full table scan + filter (column: {})",
                        col
                    )));
                    plan.push(row(
                        "Warning: no index on filter column, consider CREATE INDEX".to_string(),
                    ));
                }
            } else {
                // M76：检测 AND 多条件中是否有索引列
                let idx_matched = if let WhereExpr::And(children) = expr {
                    children.iter().any(|child| {
                        if let WhereExpr::Leaf(c) = child {
                            c.op == WhereOp::Eq
                                && c.jsonb_path.is_none()
                                && (c.column == *pk_col || has_indexes.contains(&c.column))
                        } else {
                            false
                        }
                    })
                } else {
                    false
                };
                if idx_matched {
                    plan.push(row(
                        "Access: AND index acceleration (index scan + in-memory filter)"
                            .to_string(),
                    ));
                } else {
                    plan.push(row("Access: Full table scan + complex filter".to_string()));
                }
            }
        } else {
            // 无 WHERE
            if limit.is_some() && order_by.is_none() {
                plan.push(row(
                    "Access: Streaming scan with LIMIT pushdown (O(limit) memory)".to_string(),
                ));
            } else {
                plan.push(row("Access: Full table scan".to_string()));
            }
        }

        // ORDER BY
        if let Some(ob) = order_by {
            let cols: Vec<String> = ob
                .iter()
                .map(|(c, desc, _nf)| {
                    if *desc {
                        format!("{} DESC", c)
                    } else {
                        format!("{} ASC", c)
                    }
                })
                .collect();
            // M76：Top-N 堆排序检测
            if let Some(lim) = limit {
                if !distinct {
                    let cap = lim + offset.unwrap_or(0);
                    plan.push(row(format!(
                        "Sort: {} (Top-N heap, capacity={}, O({}) memory)",
                        cols.join(", "),
                        cap,
                        cap
                    )));
                }
            } else {
                plan.push(row(format!("Sort: {} (full sort)", cols.join(", "))));
            }
        }

        // M76：WHERE + LIMIT 提前终止检测
        if let Some(lim) = limit {
            if where_clause.is_some() && order_by.is_none() && !distinct {
                let cap = lim + offset.unwrap_or(0);
                plan.push(row(format!(
                    "Early termination: stop after {} matching rows",
                    cap
                )));
            }
        }

        // OFFSET + LIMIT
        if let Some(off) = offset {
            plan.push(row(format!("Offset: {}", off)));
        }
        if let Some(n) = limit {
            if order_by.is_some() || where_clause.is_some() || offset.is_some() {
                plan.push(row(format!("Limit: {} (post-scan)", n)));
            } else {
                plan.push(row(format!("Limit: {} (pushdown)", n)));
            }
        }

        // DISTINCT
        if distinct {
            plan.push(row("Distinct: hash dedup".to_string()));
        }

        // 表统计
        let approx = tc.data_ks.approximate_len();
        plan.push(row(format!("Estimated rows: ~{}", approx)));
        if !has_indexes.is_empty() {
            plan.push(row(format!("Indexes: {}", has_indexes.join(", "))));
        }

        Ok(plan)
    }
}
