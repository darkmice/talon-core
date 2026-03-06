/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 查询规划：WHERE / ORDER BY / LIMIT 等。
//!
//! M1.4 实现透传 Stmt。
//! M2xx：接入 WHERE 谓词优化器，对 SELECT / UPDATE / DELETE / INSERT ... SELECT
//! 语句的 WHERE / HAVING 子句应用以下 Pass（参照 Apache DataFusion 优化器框架）：
//! - `flatten_conjunctions`：打平嵌套 AND-of-AND / OR-of-OR
//! - `reorder_predicates`：按代价升序重排 AND/OR 谓词，加速短路求值

use super::optimizer;
use super::parser::{Stmt, WhereExpr};

#[derive(Debug)]
pub struct Plan {
    pub stmt: Stmt,
}

/// 解析 SQL AST 并应用优化 Pass，生成可执行计划。
pub fn plan(stmt: Stmt) -> Plan {
    let mut stmt = stmt;
    optimize_stmt_where(&mut stmt);
    Plan { stmt }
}

/// 对语句中所有 WHERE / HAVING 子句递归应用优化 Pass。
fn optimize_stmt_where(stmt: &mut Stmt) {
    match stmt {
        Stmt::Select {
            where_clause,
            having,
            ctes,
            ..
        } => {
            optimize_opt_where(where_clause);
            optimize_opt_where(having);
            // 递归优化 CTE 子查询
            for cte in ctes.iter_mut() {
                optimize_stmt_where(cte.query.as_mut());
            }
        }
        Stmt::Delete { where_clause, .. } => {
            optimize_opt_where(where_clause);
        }
        Stmt::Update { where_clause, .. } => {
            optimize_opt_where(where_clause);
        }
        Stmt::Insert {
            source_select: Some(ref mut sel),
            ..
        } => {
            optimize_stmt_where(sel.as_mut());
        }
        Stmt::Union { left, right, .. } => {
            optimize_stmt_where(left.as_mut());
            optimize_stmt_where(right.as_mut());
        }
        Stmt::Explain { inner } => {
            optimize_stmt_where(inner.as_mut());
        }
        _ => {}
    }
}

/// 对可选 WHERE 表达式应用优化 Pass（原地替换）。
fn optimize_opt_where(expr: &mut Option<WhereExpr>) {
    *expr = expr.take().map(optimizer::optimize_where);
}
