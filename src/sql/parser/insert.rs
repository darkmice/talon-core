/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! INSERT 语句解析：快速路径 + 慢路径 + RETURNING。
//! 从 parser/mod.rs 拆分，保持单文件 ≤500 行。

use super::ddl;
use super::types::*;
use super::utils::*;

/// 解析 INSERT 语句（快速路径优先，回退慢路径）。
pub(super) fn parse_insert(sql: &str) -> Result<Stmt, crate::Error> {
    // M102：检测 INSERT INTO ... SELECT 模式
    if let Some(stmt) = try_insert_select(sql)? {
        return Ok(stmt);
    }
    // M123-C：快速路径 — 对于常见的 INSERT INTO table (...) VALUES (...) 格式，
    // 用字节扫描直接定位 VALUES 位置，避免逐步解析关键字/表名/列名。
    if let Some(stmt) = try_fast_insert(sql)? {
        return Ok(stmt);
    }
    // 慢路径：处理 INSERT OR REPLACE、无列名等非标准写法
    parse_insert_slow(sql)
}

/// M102：检测并解析 `INSERT [OR REPLACE|IGNORE] INTO table [(cols)] SELECT ...`。
fn try_insert_select(sql: &str) -> Result<Option<Stmt>, crate::Error> {
    let upper_prefix = if sql.len() > 30 { &sql[..30] } else { sql };
    let up = upper_prefix.to_uppercase();
    // 提取 OR REPLACE / OR IGNORE 标志和 INTO 之后的部分
    let (or_replace, or_ignore, rest) = if up.starts_with("INSERT OR") {
        let after_or = sql[9..].trim_start();
        let skip = after_or
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after_or.len());
        let kw = after_or[..skip].to_uppercase();
        let after_kw = after_or[skip..].trim_start();
        if after_kw.len() >= 4 && after_kw[..4].eq_ignore_ascii_case("INTO") {
            (kw == "REPLACE", kw == "IGNORE", after_kw[4..].trim_start())
        } else {
            return Ok(None);
        }
    } else if up.starts_with("INSERT INTO") {
        (false, false, sql[11..].trim_start())
    } else {
        return Ok(None);
    };
    // 提取表名
    let (table, rest) = match rest.split_once(|c: char| c.is_whitespace() || c == '(') {
        Some(pair) => pair,
        None => return Ok(None),
    };
    let table = unquote_ident(table);
    let rest = rest.trim_start();
    // 可选列名括号
    let (columns, after_cols) = if rest.starts_with('(') {
        match find_matching_paren(rest, 0) {
            Ok(close) => {
                let col_inner = &rest[1..close];
                let cols: Vec<String> = col_inner
                    .split(',')
                    .map(|s| unquote_ident(s.trim()))
                    .filter(|s| !s.is_empty())
                    .collect();
                (cols, rest[close + 1..].trim_start())
            }
            Err(_) => return Ok(None),
        }
    } else {
        (vec![], rest)
    };
    // 检测 SELECT 关键字
    if after_cols.len() < 6 || !after_cols[..6].eq_ignore_ascii_case("SELECT") {
        return Ok(None); // 不是 INSERT...SELECT，回退到 VALUES 路径
    }
    // 递归解析 SELECT 子句
    let select_stmt = super::select::parse_select(after_cols)?;
    Ok(Some(Stmt::Insert {
        table,
        columns,
        values: vec![],
        or_replace,
        or_ignore,
        on_conflict: None,
        returning: None,
        source_select: Some(Box::new(select_stmt)),
    }))
}

/// M123-C：快速路径 INSERT 解析 — 字节级定位 VALUES，跳过关键字/表名/列名重新解析。
/// 适用于 `INSERT INTO table (col1, ...) VALUES (...)` 标准格式。
fn try_fast_insert(sql: &str) -> Result<Option<Stmt>, crate::Error> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    // 必须以 INSERT INTO 开头（大小写不敏感）
    if len < 12 || !bytes[..11].eq_ignore_ascii_case(b"INSERT INTO") {
        return Ok(None); // 非标准格式，走慢路径
    }
    // 跳过 "INSERT INTO " + 表名
    let after_into = &sql[11..];
    let rest = after_into.trim_start();
    let (table, rest) = match rest.split_once(|c: char| c.is_whitespace() || c == '(') {
        Some(pair) => pair,
        None => return Ok(None),
    };
    let table = unquote_ident(table);
    let rest = rest.trim_start();
    // 查找列名括号和 VALUES 关键字
    if !rest.starts_with('(') {
        return Ok(None); // 无列名括号，走慢路径
    }
    let close = match find_matching_paren(rest, 0) {
        Ok(c) => c,
        Err(_) => return Ok(None),
    };
    let col_inner = &rest[1..close];
    let columns: Vec<String> = col_inner
        .split(',')
        .map(|s| unquote_ident(s.trim()))
        .filter(|s| !s.is_empty())
        .collect();
    let after_cols = rest[close + 1..].trim_start();
    // 定位 VALUES 关键字
    let values_part = if after_cols.len() >= 6 && after_cols[..6].eq_ignore_ascii_case("VALUES") {
        after_cols[6..].trim_start()
    } else if after_cols.len() >= 5 && after_cols[..5].eq_ignore_ascii_case("VALUE") {
        after_cols[5..].trim_start()
    } else {
        return Ok(None); // 无 VALUES，走慢路径
    };
    // M123-A：单 pass 解析值
    let row_strs = split_value_rows(ddl::truncate_on_conflict(values_part))?;
    if row_strs.is_empty() {
        return Err(crate::Error::SqlParse("INSERT VALUES is empty".to_string()));
    }
    let mut values = Vec::with_capacity(row_strs.len());
    for row_str in &row_strs {
        values.push(parse_row_single_pass(row_str)?);
    }
    Ok(Some(Stmt::Insert {
        table,
        columns,
        values,
        or_replace: false,
        or_ignore: false,
        on_conflict: ddl::parse_on_conflict(sql)?,
        returning: parse_returning(sql),
        source_select: None,
    }))
}

/// INSERT 慢路径：处理 INSERT OR REPLACE、无列名等非标准写法。
fn parse_insert_slow(sql: &str) -> Result<Stmt, crate::Error> {
    let prefix = if sql.len() > 20 { &sql[..20] } else { sql };
    let prefix_upper = prefix.to_uppercase();
    let (or_replace, or_ignore, rest) = if prefix_upper.starts_with("INSERT OR") {
        let after_or = sql[9..].trim_start();
        let skip = after_or
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after_or.len());
        let keyword = after_or[..skip].to_uppercase();
        let is_replace = keyword == "REPLACE";
        let is_ignore = keyword == "IGNORE";
        if !is_replace && !is_ignore {
            return Err(crate::Error::SqlParse(format!(
                "INSERT OR {}: unsupported keyword (expected REPLACE or IGNORE)",
                keyword
            )));
        }
        let after_keyword = after_or[skip..].trim_start();
        if after_keyword.len() >= 4 && after_keyword[..4].eq_ignore_ascii_case("INTO") {
            (is_replace, is_ignore, after_keyword[4..].trim_start())
        } else {
            return Err(crate::Error::SqlParse(
                "INSERT OR ... missing INTO".to_string(),
            ));
        }
    } else if prefix_upper.starts_with("INSERT INTO") {
        (false, false, sql[11..].trim_start())
    } else {
        return Err(crate::Error::SqlParse("invalid INSERT syntax".to_string()));
    };
    let (table, rest) = rest
        .split_once(|c: char| c.is_whitespace() || c == '(')
        .ok_or_else(|| crate::Error::SqlParse("INSERT missing table name".to_string()))?;
    let table = unquote_ident(table);
    let rest = rest.trim_start();
    let starts_values = rest.len() >= 6 && rest[..6].eq_ignore_ascii_case("VALUES");
    let starts_value = !starts_values && rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("VALUE");
    let (columns, values_part) = if starts_values || starts_value {
        let kw_len = if starts_values { 6 } else { 5 };
        (vec![], rest[kw_len..].trim_start())
    } else {
        let cols_str = if rest.starts_with('(') {
            rest
        } else {
            let paren_start = rest.find('(').ok_or_else(|| {
                crate::Error::SqlParse("invalid INSERT: missing columns or VALUES".to_string())
            })?;
            &rest[paren_start..]
        };
        let close = find_matching_paren(cols_str, 0)?;
        let col_inner = &cols_str[1..close];
        let columns: Vec<String> = col_inner
            .split(',')
            .map(|s| unquote_ident(s.trim()))
            .filter(|s| !s.is_empty())
            .collect();
        let after_cols = cols_str[close + 1..].trim_start();
        let values_part = if after_cols.len() >= 6 && after_cols[..6].eq_ignore_ascii_case("VALUES")
        {
            after_cols[6..].trim_start()
        } else if after_cols.len() >= 5 && after_cols[..5].eq_ignore_ascii_case("VALUE") {
            after_cols[5..].trim_start()
        } else {
            return Err(crate::Error::SqlParse("INSERT missing VALUES".to_string()));
        };
        (columns, values_part)
    };
    // 截断 ON CONFLICT 和 RETURNING 子句，只保留 VALUES 部分
    let values_clean = truncate_returning(ddl::truncate_on_conflict(values_part));
    let row_strs = split_value_rows(values_clean)?;
    if row_strs.is_empty() {
        return Err(crate::Error::SqlParse("INSERT VALUES is empty".to_string()));
    }
    let mut values = Vec::with_capacity(row_strs.len());
    for row_str in &row_strs {
        values.push(parse_row_single_pass(row_str)?);
    }
    Ok(Stmt::Insert {
        table,
        columns,
        values,
        or_replace,
        or_ignore,
        on_conflict: ddl::parse_on_conflict(sql)?,
        returning: parse_returning(sql),
        source_select: None,
    })
}

/// 截断 RETURNING 子句，只保留前面部分。
/// 在引号外查找 `RETURNING` 关键字并截断。
pub(super) fn truncate_returning(s: &str) -> &str {
    let upper = s.to_uppercase();
    if let Some(pos) = find_keyword(&upper, "RETURNING") {
        s[..pos].trim_end()
    } else {
        s
    }
}

/// 解析 RETURNING 子句：`... RETURNING col1, col2` 或 `... RETURNING *`。
pub(super) fn parse_returning(sql: &str) -> Option<Vec<String>> {
    let upper = sql.to_uppercase();
    let pos = upper.rfind("RETURNING ")?;
    let after = sql[pos + 10..].trim();
    if after.is_empty() {
        return None;
    }
    let cols: Vec<String> = after
        .split(',')
        .map(|s| unquote_ident(s.trim()))
        .filter(|s| !s.is_empty())
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}
