/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M125：SQL VIEW 视图管理 — CREATE VIEW / DROP VIEW / 视图解析。
//!
//! 视图定义存储在 `sql_meta` keyspace 中，key = `view:{name}`，value = SQL 文本。
//! 查询视图时，将视图名替换为子查询 `(view_sql) AS view_name` 执行。
//! 只读视图，不支持 INSERT/UPDATE/DELETE 到视图。

use super::engine::SqlEngine;
use super::executor::META_KEYSPACE;
use crate::types::Value;
use crate::Error;

/// 视图 key 前缀。
const VIEW_PREFIX: &str = "view:";

/// 最大视图嵌套深度（防止循环引用）。
const MAX_VIEW_DEPTH: usize = 8;

impl SqlEngine {
    /// CREATE VIEW [IF NOT EXISTS] name AS SELECT ...
    ///
    /// 存储视图定义到 meta keyspace。检查同名表/视图是否已存在。
    pub(super) fn exec_create_view(
        &mut self,
        name: &str,
        if_not_exists: bool,
        sql: &str,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 检查同名表是否存在
        if self.meta_ks.contains_key(name.as_bytes())? {
            return Err(Error::SqlExec(format!(
                "同名表已存在，无法创建视图: {}",
                name
            )));
        }
        let view_key = format!("{}{}", VIEW_PREFIX, name);
        // 检查同名视图是否存在
        if self.meta_ks.contains_key(view_key.as_bytes())? {
            if if_not_exists {
                return Ok(vec![]);
            }
            return Err(Error::SqlExec(format!("视图已存在: {}", name)));
        }
        self.meta_ks.set(view_key.as_bytes(), sql.as_bytes())?;
        Ok(vec![])
    }

    /// DROP VIEW [IF EXISTS] name
    ///
    /// 从 meta keyspace 删除视图定义。
    pub(super) fn exec_drop_view(
        &mut self,
        name: &str,
        if_exists: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let view_key = format!("{}{}", VIEW_PREFIX, name);
        if !self.meta_ks.contains_key(view_key.as_bytes())? {
            if if_exists {
                return Ok(vec![]);
            }
            return Err(Error::SqlExec(format!("视图不存在: {}", name)));
        }
        self.meta_ks.delete(view_key.as_bytes())?;
        Ok(vec![])
    }

    /// 获取视图定义 SQL。返回 None 表示不是视图。
    pub(super) fn get_view_sql(&self, name: &str) -> Result<Option<String>, Error> {
        let view_key = format!("{}{}", VIEW_PREFIX, name);
        match self.meta_ks.get(view_key.as_bytes())? {
            Some(raw) => Ok(Some(
                String::from_utf8(raw.to_vec()).map_err(|e| Error::Serialization(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    /// 解析视图引用：如果 table 是视图，递归展开为子查询 SQL。
    /// 返回展开后的完整 SQL，或 None 表示不是视图。
    ///
    /// 深度限制防止循环引用（最大 8 层）。
    pub(super) fn resolve_view(&self, table: &str, depth: usize) -> Result<Option<String>, Error> {
        if depth > MAX_VIEW_DEPTH {
            return Err(Error::SqlExec(format!(
                "视图嵌套深度超过限制({}): {}",
                MAX_VIEW_DEPTH, table
            )));
        }
        self.get_view_sql(table)
    }

    /// 检查名称是否为视图。
    pub(super) fn is_view(&self, name: &str) -> Result<bool, Error> {
        let view_key = format!("{}{}", VIEW_PREFIX, name);
        self.meta_ks.contains_key(view_key.as_bytes())
    }

    /// 列出所有视图名称。
    pub(super) fn list_views(&self) -> Result<Vec<String>, Error> {
        let keys = self.meta_ks.keys_with_prefix(VIEW_PREFIX.as_bytes())?;
        Ok(keys
            .iter()
            .filter_map(|k| {
                String::from_utf8(k.to_vec())
                    .ok()
                    .and_then(|s| s.strip_prefix(VIEW_PREFIX).map(|n| n.to_string()))
            })
            .collect())
    }

    /// 从视图 SQL 推断列名列表（公共辅助，describe_view 和 exec_view_select 共用）。
    ///
    /// SELECT * 时从基表 schema 获取真实列名；显式列名时提取 AS 别名。
    /// `fallback_count` 用于 SELECT * 无法获取基表 schema 时生成 col0..colN。
    fn infer_view_columns(
        &mut self,
        view_sql: &str,
        fallback_count: usize,
    ) -> Result<Vec<String>, Error> {
        let stmt = super::parser::parse(view_sql)?;
        match stmt {
            super::parser::Stmt::Select {
                ref columns,
                ref table,
                ..
            } => {
                if columns.len() == 1 && columns[0] == "*" {
                    if self.ensure_cached(table).unwrap_or(false) {
                        if let Some(tc) = self.cache.get(table) {
                            return Ok(tc
                                .schema
                                .visible_columns()
                                .iter()
                                .map(|(n, _)| n.clone())
                                .collect());
                        }
                    }
                    Ok(fallback_col_names(fallback_count))
                } else {
                    Ok(extract_alias_names(columns))
                }
            }
            _ => Ok(fallback_col_names(fallback_count)),
        }
    }

    /// DESCRIBE VIEW：执行视图 SQL 推断列信息。
    /// 返回格式与 DESCRIBE TABLE 一致（列名、类型、主键、NULL、默认值）。
    pub(super) fn describe_view(
        &mut self,
        _name: &str,
        view_sql: &str,
    ) -> Result<Vec<Vec<Value>>, Error> {
        // 执行视图 SQL 获取样本行（空表时返回空 vec，执行失败时降级为空）
        let sample = match self.run_sql(view_sql) {
            Ok(rows) => rows,
            Err(_) => vec![],
        };
        let col_names = self.infer_view_columns(view_sql, sample.first().map_or(1, |r| r.len()))?;
        let mut rows = Vec::new();
        for (i, col_name) in col_names.iter().enumerate() {
            let type_str = if let Some(row) = sample.first() {
                if let Some(val) = row.get(i) {
                    value_type_name(val)
                } else {
                    "Text"
                }
            } else {
                "Text"
            };
            rows.push(vec![
                Value::Text(col_name.clone()),
                Value::Text(type_str.to_string()),
                Value::Text("NO".to_string()),
                Value::Text("YES".to_string()),
                Value::Null,
            ]);
        }
        Ok(rows)
    }

    /// M125：执行视图 SELECT — 直接执行视图 SQL，在结果上应用外层条件。
    ///
    /// 不使用 CTE 物化，避免临时表数据残留问题。
    /// 视图每次查询都实时执行基表 SQL，保证数据一致性。
    pub(super) fn exec_view_select(
        &mut self,
        view_sql: &str,
        columns: &[String],
        where_clause: Option<&super::parser::WhereExpr>,
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
        distinct: bool,
    ) -> Result<Vec<Vec<Value>>, Error> {
        use super::helpers::{row_matches, value_cmp};
        // 1. 递增视图嵌套深度，执行完毕后恢复
        self.view_depth += 1;
        let all_rows = match self.run_sql(view_sql) {
            Ok(rows) => {
                self.view_depth -= 1;
                rows
            }
            Err(e) => {
                self.view_depth -= 1;
                return Err(e);
            }
        };
        if all_rows.is_empty() {
            return Ok(vec![]);
        }
        // 2. 推断视图列名（用于 WHERE/ORDER BY 列名解析）
        let col_names = self.infer_view_columns(view_sql, all_rows[0].len())?;
        // 构建伪 Schema 用于 WHERE 匹配
        let view_schema = crate::types::Schema::from_column_names(&col_names);
        // 3. 应用 WHERE 过滤
        let filtered: Vec<Vec<Value>> = if let Some(expr) = where_clause {
            all_rows
                .into_iter()
                .filter(|row| row_matches(row, &view_schema, expr).unwrap_or(false))
                .collect()
        } else {
            all_rows
        };
        // 4. 应用 DISTINCT
        let deduped = if distinct {
            let mut seen = std::collections::HashSet::new();
            filtered
                .into_iter()
                .filter(|row| seen.insert(super::engine_groupby::values_to_bytes(row)))
                .collect()
        } else {
            filtered
        };
        // 5. 应用 ORDER BY
        let mut sorted = deduped;
        if let Some(ob) = order_by {
            let col_indices: Vec<(usize, bool, bool)> = ob
                .iter()
                .filter_map(|(col, desc, nf)| {
                    col_names
                        .iter()
                        .position(|n| n.eq_ignore_ascii_case(col))
                        .map(|i| (i, *desc, nf.unwrap_or(*desc)))
                })
                .collect();
            if !col_indices.is_empty() {
                sorted.sort_by(|a, b| {
                    for &(idx, desc, nf) in &col_indices {
                        let av = a.get(idx).unwrap_or(&Value::Null);
                        let bv = b.get(idx).unwrap_or(&Value::Null);
                        match (matches!(av, Value::Null), matches!(bv, Value::Null)) {
                            (true, true) => continue,
                            (true, false) => {
                                return if nf {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            }
                            (false, true) => {
                                return if nf {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            }
                            _ => {}
                        }
                        let cmp = value_cmp(av, bv).unwrap_or(std::cmp::Ordering::Equal);
                        let cmp = if desc { cmp.reverse() } else { cmp };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }
        // 6. 应用 OFFSET + LIMIT
        let start = offset.unwrap_or(0) as usize;
        let result: Vec<Vec<Value>> = if let Some(n) = limit {
            sorted.into_iter().skip(start).take(n as usize).collect()
        } else {
            sorted.into_iter().skip(start).collect()
        };
        // 7. 列投影（SELECT 指定列 vs SELECT *）
        if columns.len() == 1 && columns[0] == "*" {
            return Ok(result);
        }
        let proj_indices: Vec<usize> = columns
            .iter()
            .filter_map(|c| col_names.iter().position(|n| n.eq_ignore_ascii_case(c)))
            .collect();
        if proj_indices.is_empty() || proj_indices.len() != columns.len() {
            return Ok(result);
        }
        Ok(result
            .into_iter()
            .map(|row| {
                proj_indices
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect())
    }
}

/// 无状态路径：获取视图定义 SQL。
#[allow(dead_code)]
pub(super) fn get_view_sql_stateless(
    store: &crate::storage::Store,
    name: &str,
) -> Result<Option<String>, Error> {
    let meta = store.open_keyspace(META_KEYSPACE)?;
    let view_key = format!("{}{}", VIEW_PREFIX, name);
    match meta.get(view_key.as_bytes())? {
        Some(raw) => Ok(Some(
            String::from_utf8(raw.to_vec()).map_err(|e| Error::Serialization(e.to_string()))?,
        )),
        None => Ok(None),
    }
}

/// 从列表达式中提取别名（`expr AS alias` → `alias`，否则原样返回）。
fn extract_alias_names(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|c| {
            let upper = c.to_uppercase();
            if let Some(pos) = upper.rfind(" AS ") {
                c[pos + 4..].trim().to_string()
            } else {
                c.clone()
            }
        })
        .collect()
}

/// 回退列名：col0, col1, ...
fn fallback_col_names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("col{}", i)).collect()
}

/// 从 Value 推断类型名称字符串。
fn value_type_name(val: &Value) -> &'static str {
    match val {
        Value::Integer(_) => "Integer",
        Value::Float(_) => "Float",
        Value::Text(_) => "Text",
        Value::Boolean(_) => "Boolean",
        Value::Blob(_) => "Blob",
        Value::Null => "Text",
        Value::Timestamp(_) => "Timestamp",
        Value::GeoPoint(_, _) => "GeoPoint",
        Value::Date(_) => "Date",
        Value::Time(_) => "Time",
        Value::Jsonb(_) => "Jsonb",
        Value::Vector(_) => "Vector",
        Value::Placeholder(_) => "Text",
    }
}
