/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M92/M121: JOIN 子句解析 — 支持 INNER/LEFT/RIGHT JOIN + 表别名 + 多条件 ON。

use super::types::*;
use super::utils::*;

/// M121：解析 JOIN 子句。
/// 支持：`[INNER|LEFT|RIGHT] JOIN t [AS alias] ON a.c1 = b.c2 [AND ...]`
pub(super) fn parse_join_clause(s: &str) -> Result<Option<(JoinClause, usize)>, crate::Error> {
    let upper = s.to_uppercase();
    let (join_type, join_pos) = if let Some(pos) = find_keyword(&upper, "NATURAL JOIN") {
        (JoinType::Natural, pos + 12)
    } else if let Some(pos) = find_keyword(&upper, "CROSS JOIN") {
        (JoinType::Cross, pos + 10)
    } else if let Some(pos) = find_keyword(&upper, "FULL OUTER JOIN") {
        (JoinType::Full, pos + 15)
    } else if let Some(pos) = find_keyword(&upper, "FULL JOIN") {
        (JoinType::Full, pos + 9)
    } else if let Some(pos) = find_keyword(&upper, "RIGHT JOIN") {
        (JoinType::Right, pos + 10)
    } else if let Some(pos) = find_keyword(&upper, "INNER JOIN") {
        (JoinType::Inner, pos + 10)
    } else if let Some(pos) = find_keyword(&upper, "LEFT JOIN") {
        (JoinType::Left, pos + 9)
    } else if let Some(pos) = find_keyword(&upper, "JOIN") {
        (JoinType::Inner, pos + 4)
    } else {
        return Ok(None);
    };
    let after_join = s[join_pos..].trim_start();
    let (join_table, after_table) = extract_table_name(after_join);
    let join_table = unquote_ident(&join_table);
    if join_table.is_empty() {
        return Err(crate::Error::SqlParse("JOIN missing table name".into()));
    }
    // M121：解析可选表别名（AS alias 或隐式别名）
    let (table_alias, after_alias) = parse_optional_alias(after_table.trim());
    let after_alias = after_alias.trim();

    // M106/M107: CROSS JOIN / NATURAL JOIN 无 ON 条件，直接返回
    if join_type == JoinType::Cross || join_type == JoinType::Natural {
        let consumed = s.len() - after_alias.len();
        // 递归解析链式 JOIN
        let (next, total) = if !after_alias.is_empty() {
            match parse_join_clause(after_alias)? {
                Some((next_jc, nc)) => (Some(Box::new(next_jc)), consumed + nc),
                None => (None, consumed),
            }
        } else {
            (None, consumed)
        };
        return Ok(Some((
            JoinClause {
                join_type,
                table: join_table,
                table_alias,
                left_col: String::new(),
                right_col: String::new(),
                next,
            },
            total,
        )));
    }

    if after_alias.len() < 2 || !after_alias[..2].eq_ignore_ascii_case("ON") {
        return Err(crate::Error::SqlParse("JOIN missing ON".into()));
    }
    let on_str = after_alias[2..].trim_start();
    let offset_on = s.len() - on_str.len();
    // M121：支持 AND 多条件，但只取第一个等值条件作为 join key
    let end = find_keyword(on_str, "WHERE")
        .or_else(|| find_keyword(on_str, "ORDER"))
        .or_else(|| find_keyword(on_str, "LIMIT"))
        .unwrap_or(on_str.len());
    let cond = on_str[..end].trim();
    // 取第一个等值条件（AND 之前的部分）
    let first_cond = if let Some(and_pos) = find_keyword(cond, "AND") {
        cond[..and_pos].trim()
    } else {
        cond
    };
    let eq_pos = first_cond
        .find('=')
        .ok_or_else(|| crate::Error::SqlParse("JOIN ON condition requires '='".into()))?;
    let left_part = first_cond[..eq_pos].trim();
    let right_part = first_cond[eq_pos + 1..].trim();
    let left_col = strip_table_prefix(left_part);
    let right_col = strip_table_prefix(right_part);
    if left_col.is_empty() || right_col.is_empty() {
        return Err(crate::Error::SqlParse(
            "JOIN ON condition column name is empty".into(),
        ));
    }
    let consumed = offset_on + end;
    // 递归解析链式 JOIN（A JOIN B ON ... JOIN C ON ...）
    let remaining = s[consumed..].trim_start();
    let (next, extra_consumed) = if !remaining.is_empty() {
        match parse_join_clause(remaining)? {
            Some((next_jc, nc)) => (
                Some(Box::new(next_jc)),
                consumed + (s[consumed..].len() - remaining.len()) + nc,
            ),
            None => (None, consumed),
        }
    } else {
        (None, consumed)
    };
    let total_consumed = if next.is_some() {
        extra_consumed
    } else {
        consumed
    };
    Ok(Some((
        JoinClause {
            join_type,
            table: join_table,
            table_alias,
            left_col: unquote_ident(&left_col),
            right_col: unquote_ident(&right_col),
            next,
        },
        total_consumed,
    )))
}

/// M121：解析可选别名（`AS alias` 或隐式 `table alias`）。
/// 返回 (Option<alias>, 剩余字符串)。
fn parse_optional_alias(s: &str) -> (Option<String>, &str) {
    if s.len() >= 3 && s[..3].eq_ignore_ascii_case("AS ") {
        let rest = s[3..].trim_start();
        let (alias, remainder) = next_word(rest);
        if !alias.is_empty() {
            return (Some(unquote_ident(&alias)), remainder);
        }
    }
    // 隐式别名：下一个词如果不是关键字（ON/WHERE/ORDER/LIMIT/JOIN 等）
    let (word, remainder) = next_word(s);
    let word_upper = word.to_uppercase();
    if !word.is_empty()
        && !matches!(
            word_upper.as_str(),
            "ON" | "WHERE"
                | "ORDER"
                | "LIMIT"
                | "JOIN"
                | "INNER"
                | "LEFT"
                | "RIGHT"
                | "FULL"
                | "CROSS"
                | "NATURAL"
                | "SET"
                | "VALUES"
        )
    {
        return (Some(unquote_ident(&word)), remainder);
    }
    (None, s)
}

fn next_word(s: &str) -> (String, &str) {
    let s = s.trim_start();
    match s.find(|c: char| c.is_whitespace() || c == '(' || c == ')') {
        Some(pos) => (s[..pos].to_string(), &s[pos..]),
        None => (s.to_string(), ""),
    }
}

fn strip_table_prefix(s: &str) -> String {
    if let Some(pos) = s.rfind('.') {
        s[pos + 1..].trim().to_string()
    } else {
        s.trim().to_string()
    }
}
