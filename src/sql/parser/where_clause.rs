/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! WHERE 子句解析：支持 AND / OR / 括号嵌套、LIKE、IN、BETWEEN、JSONB path。
//!
//! 运算符优先级：AND > OR（标准 SQL 语义），括号 `()` 提升优先级。
//! 解析结果为 `WhereExpr` 表达式树。

use super::types::*;
use super::utils::*;
use crate::types::Value;

/// 解析 WHERE 子句为表达式树。
pub(crate) fn parse_where(s: &str) -> Result<WhereExpr, crate::Error> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::Error::SqlParse("WHERE clause is empty".to_string()));
    }
    parse_or_expr(s)
}

/// 解析 OR 层级（最低优先级）。
fn parse_or_expr(s: &str) -> Result<WhereExpr, crate::Error> {
    let parts = split_by_keyword(s, "OR")?;
    if parts.len() == 1 {
        return parse_and_expr(parts[0].trim());
    }
    let mut children = Vec::with_capacity(parts.len());
    for part in &parts {
        children.push(parse_and_expr(part.trim())?);
    }
    Ok(WhereExpr::Or(children))
}

/// 解析 AND 层级（高于 OR）。
fn parse_and_expr(s: &str) -> Result<WhereExpr, crate::Error> {
    let parts = split_by_keyword(s, "AND")?;
    if parts.len() == 1 {
        return parse_atom(parts[0].trim());
    }
    let mut children = Vec::with_capacity(parts.len());
    for part in &parts {
        children.push(parse_atom(part.trim())?);
    }
    Ok(WhereExpr::And(children))
}

/// 解析原子表达式：括号分组 `(...)` 或单个条件。
fn parse_atom(s: &str) -> Result<WhereExpr, crate::Error> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::Error::SqlParse(
            "WHERE condition is empty".to_string(),
        ));
    }

    // M153: NOT EXISTS (SELECT ...)
    let upper = s.to_uppercase();
    if upper.starts_with("NOT EXISTS ") || upper.starts_with("NOT EXISTS(") {
        let rest = s[10..].trim();
        if rest.starts_with('(') {
            let close = find_matching_close_paren(rest, 0)
                .ok_or_else(|| crate::Error::SqlParse("NOT EXISTS missing ')'".into()))?;
            let inner = rest[1..close].trim();
            if inner.to_uppercase().starts_with("SELECT") {
                let sub_stmt = super::parse(inner)?;
                let mut c = cond(String::new(), WhereOp::NotExists, Value::Null, vec![], None);
                c.subquery = Some(Box::new(sub_stmt));
                return Ok(WhereExpr::Leaf(c));
            }
        }
    }

    // M153: EXISTS (SELECT ...)
    if upper.starts_with("EXISTS ") || upper.starts_with("EXISTS(") {
        let rest = s[6..].trim();
        if rest.starts_with('(') {
            let close = find_matching_close_paren(rest, 0)
                .ok_or_else(|| crate::Error::SqlParse("EXISTS missing ')'".into()))?;
            let inner = rest[1..close].trim();
            if inner.to_uppercase().starts_with("SELECT") {
                let sub_stmt = super::parse(inner)?;
                let mut c = cond(String::new(), WhereOp::Exists, Value::Null, vec![], None);
                c.subquery = Some(Box::new(sub_stmt));
                return Ok(WhereExpr::Leaf(c));
            }
        }
    }

    // 括号分组：整个表达式被括号包裹
    if s.starts_with('(') {
        if let Some(close) = find_matching_close_paren(s, 0) {
            let after = s[close + 1..].trim();
            if after.is_empty() {
                // 整个表达式是 (...)，递归解析内部
                return parse_or_expr(&s[1..close]);
            }
        }
    }
    // 单个条件
    let cond = parse_single_condition(s)?;
    Ok(WhereExpr::Leaf(cond))
}

/// 按顶层关键字分割，跳过引号内和括号内的内容。
/// 对 AND 分割时，还需跳过 BETWEEN ... AND ... 中的 AND。
fn split_by_keyword(s: &str, keyword: &str) -> Result<Vec<String>, crate::Error> {
    let upper = s.to_uppercase();
    let bytes = s.as_bytes();
    let ubytes = upper.as_bytes();
    let kw_bytes = format!(" {} ", keyword).into_bytes();
    let kw_len = kw_bytes.len();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        // 跳过引号内
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            continue;
        }
        // 跳过括号内
        if bytes[i] == b'(' {
            let mut depth = 1i32;
            i += 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i] == b'\'' {
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == b'\'' {
                            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i += 1;
                    }
                } else if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                }
                i += 1;
            }
            continue;
        }
        // 检查关键字匹配（前后须有空格）
        if i + kw_len <= ubytes.len() && &ubytes[i..i + kw_len] == kw_bytes.as_slice() {
            // AND 分割时跳过 BETWEEN ... AND ...
            if keyword == "AND" {
                let before = &upper[start..i];
                if is_between_waiting_for_and(before.trim()) {
                    i += kw_len;
                    continue;
                }
            }
            parts.push(s[start..i].to_string());
            start = i + kw_len;
            i = start;
            continue;
        }
        i += 1;
    }
    parts.push(s[start..].to_string());
    Ok(parts)
}

/// 检查表达式是否是 "col [NOT] BETWEEN val" 形式（等待后续 AND）。
fn is_between_waiting_for_and(expr: &str) -> bool {
    let upper = expr.to_uppercase();
    if let Some(pos) = upper.rfind("BETWEEN") {
        let after = upper[pos + 7..].trim();
        !after.is_empty() && !after.contains(" AND ")
    } else {
        false
    }
}

/// 找到从 start 位置开始的 '(' 对应的 ')' 位置。
fn find_matching_close_paren(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.get(start) != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    let mut in_q = false;
    for i in start..bytes.len() {
        if bytes[i] == b'\'' {
            if in_q && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                continue;
            }
            in_q = !in_q;
        } else if !in_q {
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// 构造无 JSONB path 的 WhereCondition。
fn cond(
    column: String,
    op: WhereOp,
    value: Value,
    in_values: Vec<Value>,
    value_high: Option<Value>,
) -> WhereCondition {
    WhereCondition {
        column,
        op,
        value,
        in_values,
        value_high,
        jsonb_path: None,
        subquery: None,
        escape_char: None,
        value_column: None,
    }
}

/// 从列表达式中提取 JSONB path：`col->>'key'` → (col, Some(key))。
fn extract_jsonb_path(col_expr: &str) -> (String, Option<String>) {
    if let Some(pos) = col_expr.find("->>") {
        let col = unquote_ident(col_expr[..pos].trim());
        let key_part = col_expr[pos + 3..].trim();
        let key = if (key_part.starts_with('\'') && key_part.ends_with('\''))
            || (key_part.starts_with('"') && key_part.ends_with('"'))
        {
            key_part[1..key_part.len() - 1].to_string()
        } else {
            key_part.to_string()
        };
        (col, Some(key))
    } else {
        (unquote_ident(col_expr), None)
    }
}

/// 解析单个 WHERE 条件。
fn parse_single_condition(s: &str) -> Result<WhereCondition, crate::Error> {
    let s = s.trim();
    let upper = s.to_uppercase();

    // M94: ST_WITHIN(col, lat, lng, radius_m)
    if let Some(inner) = strip_func_call(s, "ST_WITHIN") {
        let parts = split_respecting_quotes(inner);
        if parts.len() == 4 {
            let col = unquote_ident(parts[0].trim());
            let lat: f64 = parts[1]
                .trim()
                .parse()
                .map_err(|_| crate::Error::SqlParse("ST_WITHIN lat 解析失败".into()))?;
            let lng: f64 = parts[2]
                .trim()
                .parse()
                .map_err(|_| crate::Error::SqlParse("ST_WITHIN lng 解析失败".into()))?;
            let radius: f64 = parts[3]
                .trim()
                .parse()
                .map_err(|_| crate::Error::SqlParse("ST_WITHIN radius 解析失败".into()))?;
            return Ok(cond(
                col,
                WhereOp::StWithin,
                Value::GeoPoint(lat, lng),
                vec![],
                Some(Value::Float(radius)),
            ));
        }
        return Err(crate::Error::SqlParse(
            "ST_WITHIN 需要 4 个参数: col, lat, lng, radius_m".into(),
        ));
    }

    // IS NOT NULL（必须在 IS NULL 之前检测）
    if let Some(pos) = find_keyword_in_expr(&upper, "IS NOT NULL") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let mut c = cond(col, WhereOp::IsNotNull, Value::Null, vec![], None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // IS NULL
    if let Some(pos) = find_keyword_in_expr(&upper, "IS NULL") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let mut c = cond(col, WhereOp::IsNull, Value::Null, vec![], None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // NOT BETWEEN
    if let Some(pos) = find_keyword_in_expr(&upper, "NOT BETWEEN") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let rest = s[pos + 11..].trim();
        let and_pos = find_keyword_in_expr(&rest.to_uppercase(), "AND")
            .ok_or_else(|| crate::Error::SqlParse("NOT BETWEEN missing AND".into()))?;
        let low = parse_value(rest[..and_pos].trim())
            .ok_or_else(|| crate::Error::SqlParse("NOT BETWEEN 下界解析失败".into()))?;
        let high = parse_value(rest[and_pos + 3..].trim())
            .ok_or_else(|| crate::Error::SqlParse("NOT BETWEEN 上界解析失败".into()))?;
        let mut c = cond(col, WhereOp::NotBetween, low, vec![], Some(high));
        c.jsonb_path = jp;
        return Ok(c);
    }

    // BETWEEN
    if let Some(pos) = find_keyword_in_expr(&upper, "BETWEEN") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let rest = s[pos + 7..].trim();
        let and_pos = find_keyword_in_expr(&rest.to_uppercase(), "AND")
            .ok_or_else(|| crate::Error::SqlParse("BETWEEN missing AND".into()))?;
        let low = parse_value(rest[..and_pos].trim())
            .ok_or_else(|| crate::Error::SqlParse("BETWEEN 下界解析失败".into()))?;
        let high = parse_value(rest[and_pos + 3..].trim())
            .ok_or_else(|| crate::Error::SqlParse("BETWEEN 上界解析失败".into()))?;
        let mut c = cond(col, WhereOp::Between, low, vec![], Some(high));
        c.jsonb_path = jp;
        return Ok(c);
    }

    // NOT LIKE
    if let Some(pos) = find_keyword_in_expr(&upper, "NOT LIKE") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let rest = s[pos + 8..].trim();
        let (pattern_str, esc) = split_escape(rest)?;
        let pattern = parse_value(pattern_str.trim())
            .ok_or_else(|| crate::Error::SqlParse("NOT LIKE 模式解析失败".into()))?;
        let mut c = cond(col, WhereOp::NotLike, pattern, vec![], None);
        c.jsonb_path = jp;
        c.escape_char = esc;
        return Ok(c);
    }

    // LIKE
    if let Some(pos) = find_keyword_in_expr(&upper, "LIKE") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let rest = s[pos + 4..].trim();
        let (pattern_str, esc) = split_escape(rest)?;
        let pattern = parse_value(pattern_str.trim())
            .ok_or_else(|| crate::Error::SqlParse("LIKE 模式解析失败".into()))?;
        let mut c = cond(col, WhereOp::Like, pattern, vec![], None);
        c.jsonb_path = jp;
        c.escape_char = esc;
        return Ok(c);
    }

    // NOT REGEXP（必须在 REGEXP 之前检测）
    if let Some(pos) = find_keyword_in_expr(&upper, "NOT REGEXP") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let pattern = parse_value(s[pos + 10..].trim())
            .ok_or_else(|| crate::Error::SqlParse("NOT REGEXP 模式解析失败".into()))?;
        let mut c = cond(col, WhereOp::NotRegexp, pattern, vec![], None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // REGEXP
    if let Some(pos) = find_keyword_in_expr(&upper, "REGEXP") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let pattern = parse_value(s[pos + 6..].trim())
            .ok_or_else(|| crate::Error::SqlParse("REGEXP 模式解析失败".into()))?;
        let mut c = cond(col, WhereOp::Regexp, pattern, vec![], None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // NOT GLOB
    if let Some(pos) = find_keyword_in_expr(&upper, "NOT GLOB") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let pattern = parse_value(s[pos + 8..].trim())
            .ok_or_else(|| crate::Error::SqlParse("NOT GLOB 模式解析失败".into()))?;
        let mut c = cond(col, WhereOp::NotGlob, pattern, vec![], None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // GLOB
    if let Some(pos) = find_keyword_in_expr(&upper, "GLOB") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let pattern = parse_value(s[pos + 4..].trim())
            .ok_or_else(|| crate::Error::SqlParse("GLOB 模式解析失败".into()))?;
        let mut c = cond(col, WhereOp::Glob, pattern, vec![], None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // NOT IN (支持子查询: NOT IN (SELECT ...))
    if let Some(pos) = find_keyword_in_expr(&upper, "NOT IN") {
        let (col, jp) = extract_jsonb_path(s[..pos].trim());
        let rest = s[pos + 6..].trim();
        // 检测子查询: NOT IN (SELECT ...)
        if rest.starts_with('(') {
            let inner_start = rest.find('(').unwrap() + 1;
            let inner_end = rest
                .rfind(')')
                .ok_or_else(|| crate::Error::SqlParse("NOT IN subquery missing ')'".into()))?;
            let inner = rest[inner_start..inner_end].trim();
            if inner.to_uppercase().starts_with("SELECT") {
                let sub_stmt = super::parse(inner)?;
                let mut c = cond(col, WhereOp::NotIn, Value::Null, vec![], None);
                c.jsonb_path = jp;
                c.subquery = Some(Box::new(sub_stmt));
                return Ok(c);
            }
        }
        let vals = parse_in_list(rest)?;
        let mut c = cond(col, WhereOp::NotIn, Value::Null, vals, None);
        c.jsonb_path = jp;
        return Ok(c);
    }

    // IN (支持子查询: IN (SELECT ...))
    if let Some(pos) = find_keyword_in_expr(&upper, "IN") {
        let before = s[..pos].trim();
        if !before.is_empty() {
            let (col, jp) = extract_jsonb_path(before);
            let rest = s[pos + 2..].trim();
            // 检测子查询: IN (SELECT ...)
            if rest.starts_with('(') {
                let inner_start = rest.find('(').unwrap() + 1;
                let inner_end = rest
                    .rfind(')')
                    .ok_or_else(|| crate::Error::SqlParse("IN subquery missing ')'".into()))?;
                let inner = rest[inner_start..inner_end].trim();
                if inner.to_uppercase().starts_with("SELECT") {
                    let sub_stmt = super::parse(inner)?;
                    let mut c = cond(col, WhereOp::In, Value::Null, vec![], None);
                    c.jsonb_path = jp;
                    c.subquery = Some(Box::new(sub_stmt));
                    return Ok(c);
                }
            }
            let vals = parse_in_list(rest)?;
            let mut c = cond(col, WhereOp::In, Value::Null, vals, None);
            c.jsonb_path = jp;
            return Ok(c);
        }
    }

    // 标准比较操作符
    let (col_expr, op, val_str) = parse_comparison(s)?;
    let trimmed_val = val_str.trim();
    // M118：先尝试解析为字面值，失败则尝试作为列引用（CHECK 约束中 `lo <= hi`）
    if let Some(val) = parse_value(trimmed_val) {
        let (col, jp) = extract_jsonb_path(&col_expr);
        let mut c = cond(col, op, val, vec![], None);
        c.jsonb_path = jp;
        Ok(c)
    } else if is_column_ident(trimmed_val) {
        let (col, jp) = extract_jsonb_path(&col_expr);
        let mut c = cond(col, op, Value::Null, vec![], None);
        c.jsonb_path = jp;
        c.value_column = Some(unquote_ident(trimmed_val));
        Ok(c)
    } else {
        Err(crate::Error::SqlParse(format!(
            "WHERE value parse failed: {}",
            val_str
        )))
    }
}

/// 从 LIKE/NOT LIKE 后的剩余字符串中分离模式和 ESCAPE 子句。
/// 例如 `'%x\%%' ESCAPE '\'` → (`'%x\%%'`, Some('\\'))。
fn split_escape(rest: &str) -> Result<(&str, Option<char>), crate::Error> {
    let upper = rest.to_uppercase();
    if let Some(pos) = find_keyword_in_expr(&upper, "ESCAPE") {
        let pattern_str = rest[..pos].trim();
        let esc_val = rest[pos + 6..].trim();
        // ESCAPE 值必须是单字符字符串 'x'
        if esc_val.starts_with('\'') && esc_val.ends_with('\'') && esc_val.len() == 3 {
            let ch = esc_val.as_bytes()[1] as char;
            Ok((pattern_str, Some(ch)))
        } else {
            Err(crate::Error::SqlParse(
                "ESCAPE 必须是单字符，如 ESCAPE '\\'".into(),
            ))
        }
    } else {
        Ok((rest, None))
    }
}

/// 在表达式中查找关键字（前后须为空白或边界）。
fn find_keyword_in_expr(upper: &str, keyword: &str) -> Option<usize> {
    let kw_len = keyword.len();
    let mut search_from = 0;
    while search_from + kw_len <= upper.len() {
        if let Some(pos) = upper[search_from..].find(keyword) {
            let abs = search_from + pos;
            let before_ok = abs == 0 || upper.as_bytes()[abs - 1].is_ascii_whitespace();
            let after = abs + kw_len;
            let after_ok = after >= upper.len()
                || upper.as_bytes()[after].is_ascii_whitespace()
                || upper.as_bytes()[after] == b'(';
            if before_ok && after_ok {
                return Some(abs);
            }
            search_from = abs + 1;
        } else {
            break;
        }
    }
    None
}

/// 解析 IN 列表：(val1, val2, ...)。
fn parse_in_list(s: &str) -> Result<Vec<Value>, crate::Error> {
    let s = s.trim();
    if !s.starts_with('(') {
        return Err(crate::Error::SqlParse("IN missing '('".into()));
    }
    let close = s
        .rfind(')')
        .ok_or_else(|| crate::Error::SqlParse("IN missing ')'".into()))?;
    let inner = &s[1..close];
    let parts = split_respecting_quotes(inner);
    let mut vals = Vec::with_capacity(parts.len());
    for p in &parts {
        let v = parse_value(p.trim()).ok_or_else(|| {
            crate::Error::SqlParse(format!("IN value parse failed: {}", p.trim()))
        })?;
        vals.push(v);
    }
    if vals.is_empty() {
        return Err(crate::Error::SqlParse("IN list is empty".into()));
    }
    Ok(vals)
}

/// 解析比较表达式：col op val。
/// 注意：需要跳过 `->>` 中的 `>` 字符，不误判为 Gt 操作符。
fn parse_comparison(s: &str) -> Result<(String, WhereOp, String), crate::Error> {
    let s = s.trim();
    let arrow_pos = find_outside_quotes(s, "->>");
    for (pat, op) in &[
        ("!=", WhereOp::Ne),
        ("<>", WhereOp::Ne),
        ("<=", WhereOp::Le),
        (">=", WhereOp::Ge),
        ("<", WhereOp::Lt),
        (">", WhereOp::Gt),
        ("=", WhereOp::Eq),
    ] {
        if let Some(pos) = find_outside_quotes(s, pat) {
            if let Some(ap) = arrow_pos {
                if pos >= ap && pos <= ap + 2 {
                    continue;
                }
            }
            let col = s[..pos].trim().to_string();
            let val = s[pos + pat.len()..].trim().to_string();
            return Ok((col, *op, val));
        }
    }
    Err(crate::Error::SqlParse(format!("无法解析比较表达式: {}", s)))
}

/// 检查字符串是否为合法的列标识符（字母/下划线开头，后跟字母/数字/下划线/点）。
fn is_column_ident(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let first = bytes[0];
    if !(first.is_ascii_alphabetic() || first == b'_' || first == b'"' || first == b'`') {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'"' || b == b'`')
}

/// 在非引号区域查找子串。
fn find_outside_quotes(s: &str, pat: &str) -> Option<usize> {
    let mut in_q = false;
    let bytes = s.as_bytes();
    let pat_bytes = pat.as_bytes();
    if pat_bytes.is_empty() || bytes.len() < pat_bytes.len() {
        return None;
    }
    let mut i = 0;
    while i <= bytes.len() - pat_bytes.len() {
        if bytes[i] == b'\'' {
            in_q = !in_q;
            i += 1;
            continue;
        }
        if !in_q && &bytes[i..i + pat_bytes.len()] == pat_bytes {
            return Some(i);
        }
        i += 1;
    }
    None
}
