/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M113：CTE (WITH 子句) 执行逻辑。
//!
//! 将 CTE 查询结果物化为临时表，执行主查询后清理。
//! 非递归 CTE，纯内存物化。
//! 临时表使用 `_cte_{name}` 前缀避免与真实表冲突。

use super::engine::SqlEngine;
use super::parser::{CteClause, Stmt};
use crate::types::Value;
use crate::Error;

/// CTE 临时表名前缀，避免与用户表冲突。
const CTE_PREFIX: &str = "_cte_";

impl SqlEngine {
    /// 执行带 CTE 的 SELECT：物化 CTE → 执行主查询 → 清理临时表。
    ///
    /// CTE 按声明顺序执行，后续 CTE 可引用前面的 CTE。
    /// 临时表使用 `_cte_{name}` 前缀，主查询中的表名自动替换。
    pub(super) fn exec_with_ctes(
        &mut self,
        ctes: Vec<CteClause>,
        main_stmt: Stmt,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 收集 CTE 名称映射：原名 → 临时表名
        let name_map: Vec<(String, String)> = ctes
            .iter()
            .map(|c| (c.name.clone(), format!("{}{}", CTE_PREFIX, c.name)))
            .collect();
        let mut created_tables: Vec<String> = Vec::new();
        for (idx, cte) in ctes.iter().enumerate() {
            let tmp_name = format!("{}{}", CTE_PREFIX, cte.name);
            // 只替换前面已声明的 CTE 名称（当前 CTE 子查询引用原始表）
            let prev_map = &name_map[..idx];
            let resolved_query = rewrite_table_refs(&cte.query, prev_map);
            let rows = match self.exec_stmt(resolved_query) {
                Ok(r) => r,
                Err(e) => {
                    // CTE 执行失败，清理已创建的临时表
                    for name in created_tables.iter().rev() {
                        let _ = self.run_sql(&format!("DROP TABLE IF EXISTS {}", name));
                    }
                    return Err(e);
                }
            };
            let col_names = self.infer_cte_columns(&cte.query, rows.first());
            let create_sql = build_create_table_sql(&tmp_name, &col_names, rows.first());
            self.run_sql(&create_sql)?;
            created_tables.push(tmp_name.clone());
            if !rows.is_empty() {
                insert_cte_rows(self, &tmp_name, &col_names, &rows)?;
            }
        }
        // 替换主查询中的 CTE 引用
        let rewritten = rewrite_table_refs(&main_stmt, &name_map);
        let result = self.exec_stmt(rewritten);
        // 清理临时表（逆序）
        for name in created_tables.iter().rev() {
            let _ = self.run_sql(&format!("DROP TABLE IF EXISTS {}", name));
        }
        result
    }

    /// 从 CTE 子查询推断列名。
    fn infer_cte_columns(&mut self, query: &Stmt, sample_row: Option<&Vec<Value>>) -> Vec<String> {
        if let Stmt::Select { columns, table, .. } = query {
            if columns.len() == 1 && columns[0] == "*" {
                // 尝试从源表（可能是另一个 CTE 临时表）获取列名
                let lookup = format!("{}{}", CTE_PREFIX, table);
                let tbl = if self.ensure_cached(&lookup).unwrap_or(false) {
                    &lookup
                } else if self.ensure_cached(table).unwrap_or(false) {
                    table
                } else {
                    return fallback_cols(sample_row);
                };
                if let Some(tc) = self.cache.get(tbl) {
                    return tc.schema.columns.iter().map(|(n, _)| n.clone()).collect();
                }
                return fallback_cols(sample_row);
            }
            return columns
                .iter()
                .map(|c| {
                    let upper = c.to_uppercase();
                    if let Some(pos) = upper.rfind(" AS ") {
                        return c[pos + 4..].trim().to_string();
                    }
                    if c.contains('(') {
                        return c
                            .replace('(', "_")
                            .replace(')', "")
                            .replace('*', "star")
                            .to_lowercase();
                    }
                    c.clone()
                })
                .collect();
        }
        fallback_cols(sample_row)
    }
}

/// 回退列名：c1, c2, ...
fn fallback_cols(sample: Option<&Vec<Value>>) -> Vec<String> {
    if let Some(row) = sample {
        (0..row.len()).map(|i| format!("c{}", i + 1)).collect()
    } else {
        vec!["c1".to_string()]
    }
}

/// 重写 Stmt 中的表名引用：将 CTE 名称替换为临时表名。
fn rewrite_table_refs(stmt: &Stmt, name_map: &[(String, String)]) -> Stmt {
    match stmt {
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
            let new_table = map_name(table, name_map);
            Stmt::Select {
                table: new_table,
                columns: columns.clone(),
                where_clause: where_clause.clone(),
                order_by: order_by.clone(),
                limit: *limit,
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
            }
        }
        other => other.clone(),
    }
}

/// 查找名称映射，命中则返回临时表名，否则原样返回。
fn map_name(name: &str, name_map: &[(String, String)]) -> String {
    for (orig, tmp) in name_map {
        if name == orig {
            return tmp.clone();
        }
    }
    name.to_string()
}

/// 构建 CTE 临时表的 CREATE TABLE SQL。
fn build_create_table_sql(name: &str, columns: &[String], sample: Option<&Vec<Value>>) -> String {
    let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", name);
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(col);
        sql.push(' ');
        let typ = sample
            .and_then(|row| row.get(i))
            .map(|v| match v {
                Value::Integer(_) => "INTEGER",
                Value::Float(_) => "REAL",
                Value::Boolean(_) => "INTEGER",
                Value::Blob(_) => "BLOB",
                _ => "TEXT",
            })
            .unwrap_or("TEXT");
        sql.push_str(typ);
    }
    sql.push(')');
    sql
}

/// 将 CTE 查询结果批量插入临时表。
fn insert_cte_rows(
    eng: &mut SqlEngine,
    table: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<(), Error> {
    let cols_str: String = columns.join(", ");
    for (i, row) in rows.iter().enumerate() {
        let mut values = Vec::with_capacity(row.len());
        for (ci, val) in row.iter().enumerate() {
            if ci == 0 && *val == Value::Null {
                values.push(Value::Text(format!("_cte_row_{}", i)));
            } else {
                values.push(val.clone());
            }
        }
        let vals_str: String = values
            .iter()
            .map(value_to_sql_literal)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
            table, cols_str, vals_str
        );
        eng.run_sql(&sql)?;
    }
    Ok(())
}

/// 将 Value 转换为 SQL 字面量字符串。
fn value_to_sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Blob(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("X'");
            for byte in b {
                hex.push_str(&format!("{:02x}", byte));
            }
            hex.push('\'');
            hex
        }
        _ => format!("'{}'", format!("{:?}", v).replace('\'', "''")),
    }
}
