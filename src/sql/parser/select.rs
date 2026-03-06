/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SELECT 语句解析（含 JOIN / GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET）。
//! 从 parser/mod.rs 拆分，保持单文件 ≤500 行。

use super::geo_search::extract_geo_search;
use super::join;
use super::types::*;
use super::utils::*;
use super::vec_search::extract_vec_search;
use super::where_clause::parse_where;
use super::window::extract_window_functions;

/// 解析 SELECT 语句。
pub(super) fn parse_select(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[6..].trim_start();
    // DISTINCT ON (col1, col2, ...) 或普通 DISTINCT
    let (distinct, distinct_on, rest) = {
        let upper = rest.to_uppercase();
        if upper.starts_with("DISTINCT ") {
            let after_distinct = rest[9..].trim_start();
            let after_upper = after_distinct.to_uppercase();
            if after_upper.starts_with("ON") {
                // DISTINCT ON (col1, col2, ...)
                let on_rest = after_distinct[2..].trim_start();
                if on_rest.starts_with('(') {
                    let close = on_rest
                        .find(')')
                        .ok_or_else(|| crate::Error::SqlParse("DISTINCT ON 缺少右括号".into()))?;
                    let cols_str = &on_rest[1..close];
                    let cols: Vec<String> = cols_str
                        .split(',')
                        .map(|s| unquote_ident(s.trim()))
                        .filter(|s| !s.is_empty())
                        .collect();
                    if cols.is_empty() {
                        return Err(crate::Error::SqlParse("DISTINCT ON 列列表不能为空".into()));
                    }
                    (true, Some(cols), on_rest[close + 1..].trim_start())
                } else {
                    (true, None, after_distinct)
                }
            } else {
                (true, None, after_distinct)
            }
        } else {
            (false, None, rest)
        }
    };
    let from_pos = find_keyword(rest, "FROM")
        .ok_or_else(|| crate::Error::SqlParse("SELECT missing FROM".to_string()))?;
    let cols_part = rest[..from_pos].trim();
    let after_from = rest[from_pos + 4..].trim_start();
    let columns: Vec<String> = if cols_part == "*" || cols_part.is_empty() {
        vec!["*".to_string()]
    } else {
        split_respecting_quotes(cols_part)
            .iter()
            .map(|s| {
                let t = s.trim();
                // CASE WHEN 表达式保留原文（内部自行处理大小写）
                if t.len() >= 5 && t[..5].eq_ignore_ascii_case("CASE ") {
                    t.to_string()
                } else if t.contains('(') {
                    // 函数调用保留原文（UPPER/CAST/COALESCE 等内部自行处理）
                    t.to_string()
                } else {
                    unquote_ident(t)
                }
            })
            .filter(|s| !s.is_empty())
            .collect()
    };
    let (table, remainder) = extract_table_name(after_from);
    let table = unquote_ident(&table);
    // M121：跳过 FROM 表的可选别名（AS alias 或隐式别名）
    let remainder = skip_from_alias(remainder.trim());
    let mut where_clause = None;
    let mut order_by = None;
    let mut limit = None;
    let mut rest = remainder.trim().to_string();

    // M92/M121: parse optional JOIN clause
    let join = match join::parse_join_clause(&rest)? {
        Some((jc, consumed)) => {
            rest = rest[consumed..].to_string();
            Some(jc)
        }
        None => None,
    };

    if let Some(pos) = find_keyword(&rest, "WHERE") {
        let after_where = &rest[pos + 5..];
        let end = min_keyword_pos(after_where, &["GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH"]);
        let where_str = after_where[..end].trim();
        if !where_str.is_empty() {
            where_clause = Some(parse_where(where_str)?);
        }
        rest = after_where[end..].to_string();
    }
    // GROUP BY col1, col2 [HAVING ...]
    let mut group_by = None;
    let mut having = None;
    if let Some(pos) = find_keyword(&rest, "GROUP") {
        let after_group = rest[pos + 5..].trim_start();
        let after_by = if after_group.to_uppercase().starts_with("BY") {
            after_group[2..].trim_start()
        } else {
            after_group
        };
        let end = min_keyword_pos(after_by, &["HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH"]);
        let gb_str = after_by[..end].trim();
        if !gb_str.is_empty() {
            let cols: Vec<String> = split_respecting_quotes(gb_str)
                .iter()
                .map(|s| unquote_ident(s.trim()))
                .filter(|s| !s.is_empty())
                .collect();
            if !cols.is_empty() {
                group_by = Some(cols);
            }
        }
        rest = after_by[end..].to_string();
    }
    if let Some(pos) = find_keyword(&rest, "HAVING") {
        let after_having = &rest[pos + 6..];
        let end = min_keyword_pos(after_having, &["ORDER", "LIMIT", "OFFSET", "FETCH"]);
        let having_str = after_having[..end].trim();
        if !having_str.is_empty() {
            having = Some(parse_where(having_str)?);
        }
        rest = after_having[end..].to_string();
    }
    if let Some(pos) = find_keyword(&rest, "ORDER") {
        let after_order = rest[pos + 5..].trim_start();
        let after_by = if after_order.to_uppercase().starts_with("BY") {
            after_order[2..].trim_start()
        } else {
            after_order
        };
        let end = min_keyword_pos(after_by, &["LIMIT", "FETCH", "OFFSET"]);
        let ob_str = after_by[..end].trim();
        if !ob_str.is_empty() {
            let mut cols = Vec::new();
            for part in split_respecting_quotes(ob_str) {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                // 解析 NULLS FIRST / NULLS LAST 后缀
                let (part_no_nulls, nulls_first) = {
                    let u = part.to_uppercase();
                    if u.ends_with("NULLS FIRST") {
                        (&part[..part.len() - 11], Some(true))
                    } else if u.ends_with("NULLS LAST") {
                        (&part[..part.len() - 10], Some(false))
                    } else {
                        (part, None)
                    }
                };
                let part_trimmed = part_no_nulls.trim();
                let (col, desc) = if let Some(stripped) = part_trimmed
                    .strip_suffix("DESC")
                    .or_else(|| part_trimmed.strip_suffix("desc"))
                {
                    (unquote_ident(stripped.trim()), true)
                } else if let Some(stripped) = part_trimmed
                    .strip_suffix("ASC")
                    .or_else(|| part_trimmed.strip_suffix("asc"))
                {
                    (unquote_ident(stripped.trim()), false)
                } else {
                    (unquote_ident(part_trimmed), false)
                };
                if !col.is_empty() {
                    cols.push((col, desc, nulls_first));
                }
            }
            if !cols.is_empty() {
                order_by = Some(cols);
            }
        }
        rest = after_by[end..].to_string();
    }
    let mut offset = None;
    if let Some(pos) = find_keyword(&rest, "LIMIT") {
        let after_limit = rest[pos + 5..].trim_start();
        let token = after_limit.split_whitespace().next().unwrap_or("");
        limit = if token == "?" {
            Some(u64::MAX) // 占位符哨兵，由 bind_params 替换
        } else {
            token.parse().ok()
        };
        rest = rest[pos + 5..].to_string();
    }
    if let Some(pos) = find_keyword(&rest, "OFFSET") {
        let after_off = rest[pos + 6..].trim_start();
        let token = after_off.split_whitespace().next().unwrap_or("");
        offset = if token == "?" {
            Some(u64::MAX) // 占位符哨兵，由 bind_params 替换
        } else {
            token.parse().ok()
        };
        rest = rest[pos + 6..].to_string();
    }
    // M158: FETCH FIRST/NEXT n ROWS ONLY — SQL 标准 LIMIT 别名
    if let Some(pos) = find_keyword(&rest, "FETCH") {
        let after_fetch = rest[pos + 5..].trim_start().to_uppercase();
        // 跳过 FIRST 或 NEXT
        let num_start = if after_fetch.starts_with("FIRST") {
            &rest[pos + 5..].trim_start()[5..]
        } else if after_fetch.starts_with("NEXT") {
            &rest[pos + 5..].trim_start()[4..]
        } else {
            ""
        };
        if !num_start.is_empty() {
            let n = num_start
                .trim_start()
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok());
            if let Some(n) = n {
                limit = Some(n);
            }
        }
    }
    // M177：提取窗口函数表达式
    let (columns_no_win, window_functions) = extract_window_functions(&columns);
    // 检测 vec_* 向量搜索函数
    let (final_columns, vec_search) = extract_vec_search(&columns_no_win);
    // M94：检测 ST_DISTANCE 地理空间搜索函数
    let (final_columns, geo_search) = extract_geo_search(&final_columns);
    Ok(Stmt::Select {
        table,
        columns: final_columns,
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
        window_functions,
    })
}

/// M121：跳过 FROM 表名后的可选别名（`AS alias` 或隐式别名）。
fn skip_from_alias(s: &str) -> &str {
    // AS alias
    if s.len() >= 3 && s[..3].eq_ignore_ascii_case("AS ") {
        let rest = s[3..].trim_start();
        // 跳过别名词
        match rest.find(|c: char| c.is_whitespace()) {
            Some(pos) => return &rest[pos..],
            None => return "",
        }
    }
    // 隐式别名：第一个词如果不是关键字
    let word_end = s.find(|c: char| c.is_whitespace()).unwrap_or(s.len());
    if word_end == 0 {
        return s;
    }
    let word = &s[..word_end];
    let wu = word.to_uppercase();
    if !matches!(
        wu.as_str(),
        "WHERE"
            | "ORDER"
            | "LIMIT"
            | "FETCH"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "NATURAL"
            | "ON"
            | "SET"
            | "GROUP"
            | "HAVING"
    ) {
        return &s[word_end..];
    }
    s
}
