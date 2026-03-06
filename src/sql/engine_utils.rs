/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SqlEngine 辅助函数：RETURNING 处理、PG 占位符、子查询解析。
//!
//! 从 engine.rs 拆分，减少单文件行数。

use std::collections::HashMap;

use crate::types::Value;
use crate::Error;

/// RETURNING 子句处理：对插入的行做列投影返回。
pub(super) fn apply_returning(
    table: &str,
    inserted_values: &[Vec<Value>],
    ret_cols: &[String],
    cache: &HashMap<String, super::engine::TableCache>,
) -> Vec<Vec<Value>> {
    if ret_cols.len() == 1 && ret_cols[0] == "*" {
        return inserted_values.to_vec();
    }
    let schema = match cache.get(table) {
        Some(tc) => &tc.schema,
        None => return inserted_values.to_vec(),
    };
    let indices: Vec<usize> = ret_cols
        .iter()
        .filter_map(|c| schema.column_index_by_name(c))
        .collect();
    inserted_values
        .iter()
        .map(|row| {
            indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

/// PostgreSQL 兼容：将 `$1`, `$2`, ... 占位符替换为 `?`。
/// 跳过引号内的 `$N`，保证字符串常量不被误替换。
/// 如果 SQL 中没有 `$` 字符，直接返回原字符串（零分配）。
pub(crate) fn normalize_pg_placeholders(sql: &str) -> std::borrow::Cow<'_, str> {
    if !sql.contains('$') {
        return std::borrow::Cow::Borrowed(sql);
    }
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut in_quote = false;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_quote = !in_quote;
            result.push('\'');
            i += 1;
        } else if !in_quote
            && bytes[i] == b'$'
            && i + 1 < bytes.len()
            && bytes[i + 1].is_ascii_digit()
        {
            result.push('?');
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    std::borrow::Cow::Owned(result)
}

impl super::engine::SqlEngine {
    /// 解析 WHERE 树中的子查询，执行并替换为值列表。
    /// 子查询结果集限制 10000 行，超出报错。
    /// M153：EXISTS/NOT EXISTS 子查询执行后替换为恒真/恒假条件。
    pub(super) fn resolve_subqueries(
        &mut self,
        expr: &super::parser::WhereExpr,
    ) -> Result<super::parser::WhereExpr, Error> {
        use super::parser::{WhereExpr, WhereOp};
        match expr {
            WhereExpr::Leaf(cond) => {
                if let Some(ref sub) = cond.subquery {
                    // M153: EXISTS / NOT EXISTS — 只需判断子查询是否有结果
                    if cond.op == WhereOp::Exists || cond.op == WhereOp::NotExists {
                        // 性能优化：注入 LIMIT 1，只需判断是否有行
                        let limited_sub = match sub.as_ref() {
                            super::parser::Stmt::Select {
                                table,
                                columns,
                                where_clause,
                                order_by,
                                limit: _,
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
                            } => super::parser::Stmt::Select {
                                table: table.clone(),
                                columns: columns.clone(),
                                where_clause: where_clause.clone(),
                                order_by: order_by.clone(),
                                limit: Some(1),
                                offset: *offset,
                                distinct: *distinct,
                                distinct_on: distinct_on.clone(),
                                vec_search: vec_search.clone(),
                                geo_search: geo_search.clone(),
                                join: join.clone(),
                                group_by: group_by.clone(),
                                having: having.clone(),
                                ctes: ctes.clone(),
                                window_functions: window_functions.clone(),
                            },
                            other => other.clone(),
                        };
                        let rows = self.exec_stmt_ref(&limited_sub)?;
                        let has_rows = !rows.is_empty();
                        let matches = if cond.op == WhereOp::Exists {
                            has_rows
                        } else {
                            !has_rows
                        };
                        // 替换为恒真（1=1）或恒假（1=0）条件
                        let new_cond = super::parser::WhereCondition {
                            column: String::new(),
                            op: if matches {
                                WhereOp::Exists
                            } else {
                                WhereOp::NotExists
                            },
                            value: Value::Integer(if matches { 1 } else { 0 }),
                            in_values: vec![],
                            value_high: None,
                            jsonb_path: None,
                            subquery: None,
                            escape_char: None,
                            value_column: None,
                        };
                        return Ok(WhereExpr::Leaf(new_cond));
                    }
                    // IN / NOT IN 子查询
                    let rows = self.exec_stmt_ref(sub)?;
                    if rows.len() > 10_000 {
                        return Err(Error::SqlExec("子查询结果集超过 10000 行限制".into()));
                    }
                    if let Some(first) = rows.first() {
                        if first.len() > 1 {
                            return Err(Error::SqlExec("子查询必须只返回一列".into()));
                        }
                    }
                    let vals: Vec<Value> = rows
                        .into_iter()
                        .filter_map(|row| row.into_iter().next())
                        .collect();
                    let mut new_cond = cond.clone();
                    new_cond.subquery = None;
                    new_cond.in_values = vals;
                    Ok(WhereExpr::Leaf(new_cond))
                } else {
                    Ok(expr.clone())
                }
            }
            WhereExpr::And(children) => {
                let resolved: Result<Vec<_>, _> = children
                    .iter()
                    .map(|c| self.resolve_subqueries(c))
                    .collect();
                Ok(WhereExpr::And(resolved?))
            }
            WhereExpr::Or(children) => {
                let resolved: Result<Vec<_>, _> = children
                    .iter()
                    .map(|c| self.resolve_subqueries(c))
                    .collect();
                Ok(WhereExpr::Or(resolved?))
            }
        }
    }

    /// 执行已解析的 Stmt 引用（子查询用，不消耗 Stmt）。
    pub(super) fn exec_stmt_ref(
        &mut self,
        stmt: &super::parser::Stmt,
    ) -> Result<Vec<Vec<Value>>, Error> {
        use super::parser::Stmt;
        match stmt {
            Stmt::Select {
                table,
                columns,
                where_clause,
                order_by,
                limit,
                offset,
                distinct,
                distinct_on: _,
                vec_search,
                geo_search: _,
                join: _,
                group_by,
                having,
                ctes: _,
                window_functions: _,
            } => {
                if let Some(ref gb_cols) = group_by {
                    return self.exec_group_by(
                        table,
                        columns,
                        where_clause.as_ref(),
                        gb_cols,
                        having.as_ref(),
                        order_by.as_deref(),
                        *limit,
                    );
                }
                self.exec_select(
                    table,
                    columns,
                    where_clause.as_ref(),
                    order_by.as_deref(),
                    *limit,
                    *offset,
                    *distinct,
                    vec_search.as_ref(),
                    None,
                )
            }
            _ => Err(Error::SqlExec("子查询仅支持 SELECT".into())),
        }
    }
}

/// 检查 WHERE 树中是否包含子查询。
pub(super) fn has_subquery(expr: &super::parser::WhereExpr) -> bool {
    use super::parser::WhereExpr;
    match expr {
        WhereExpr::Leaf(cond) => cond.subquery.is_some(),
        WhereExpr::And(children) | WhereExpr::Or(children) => children.iter().any(has_subquery),
    }
}
