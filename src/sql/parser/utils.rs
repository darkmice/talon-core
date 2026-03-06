/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 解析辅助函数：标识符处理、类型解析、值解析、字符串分割。
//! 从 parser/types.rs 拆分，保持单文件 ≤500 行。

use crate::types::{ColumnType, Value};

/// 去除标识符的引号；未引用的标识符折叠为小写（SQL 标准行为）。
///
/// - 双引号 `"Foo"` → `Foo`（保持原始大小写）
/// - 反引号 `` `Foo` `` → `Foo`（保持原始大小写，MySQL 兼容）
/// - 无引号 `Foo` → `foo`（折叠为小写，对齐 PostgreSQL 默认行为）
pub(crate) fn unquote_ident(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('`') && s.ends_with('`')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_ascii_lowercase()
    }
}
pub(crate) fn parse_column_type(s: &str) -> Option<ColumnType> {
    let s = s.trim().to_uppercase();
    match s.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "NUMERIC" => {
            Some(ColumnType::Integer)
        }
        "FLOAT" | "DOUBLE" | "REAL" | "NUMBER" | "DECIMAL" => Some(ColumnType::Float),
        "TEXT" | "VARCHAR" | "STRING" | "CHAR" | "NVARCHAR" | "NCHAR" | "CLOB" => {
            Some(ColumnType::Text)
        }
        "BLOB" | "BYTES" | "BYTEA" => Some(ColumnType::Blob),
        "BOOLEAN" | "BOOL" => Some(ColumnType::Boolean),
        "JSONB" | "JSON" | "JSONB[]" => Some(ColumnType::Jsonb),
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => Some(ColumnType::Timestamp),
        "DATE" => Some(ColumnType::Date),
        "TIME" | "TIMETZ" => Some(ColumnType::Time),
        "GEOPOINT" | "GEO" | "POINT" | "GEOMETRY" => Some(ColumnType::GeoPoint),
        // PostgreSQL specific
        "SERIAL" | "SMALLSERIAL" | "BIGSERIAL" => Some(ColumnType::Integer),
        "UUID" | "CITEXT" | "INET" | "CIDR" | "MACADDR" | "INTERVAL" | "XML" => {
            Some(ColumnType::Text)
        }
        _ => {
            if s.starts_with("VECTOR(") && s.ends_with(')') {
                let dim: usize = s[7..s.len() - 1].trim().parse().ok()?;
                Some(ColumnType::Vector(dim))
            } else if (s.starts_with("VARCHAR(")
                || s.starts_with("CHAR(")
                || s.starts_with("NVARCHAR(")
                || s.starts_with("CHARACTER("))
                && s.ends_with(')')
            {
                Some(ColumnType::Text)
            } else if s.starts_with("ENUM(") && s.ends_with(')') {
                Some(ColumnType::Text)
            } else if s.starts_with("DECIMAL(")
                || s.starts_with("NUMERIC(")
                || s.starts_with("NUMBER(")
            {
                Some(ColumnType::Float)
            } else if s.starts_with("INT(")
                || s.starts_with("INTEGER(")
                || s.starts_with("BIGINT(")
                || s.starts_with("SMALLINT(")
                || s.starts_with("TINYINT(")
                || s.starts_with("MEDIUMINT(")
            {
                Some(ColumnType::Integer)
            } else if s.starts_with("TIMESTAMP(") {
                Some(ColumnType::Timestamp)
            } else {
                None
            }
        }
    }
}
/// M123-B：首字符快速分派 + 类型探测顺序优化。
pub(crate) fn parse_value(s: &str) -> Option<Value> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let first = s.as_bytes()[0];
    // 参数化占位符 `?`
    if first == b'?' && s.len() == 1 {
        return Some(Value::Placeholder(0));
    }
    // 快速分派：首字符判断
    match first {
        // 单引号开头 → 字符串（71列中 ~40 个 Text）
        b'\'' => {
            let inner = s.strip_prefix('\'').and_then(|t| t.strip_suffix('\''))?;
            return Some(Value::Text(if inner.contains("''") {
                inner.replace("''", "'")
            } else {
                inner.to_string()
            }));
        }
        // 数字或负号 → Integer 或 Float（71列中 ~30 个数字）
        b'0'..=b'9' | b'-' => {
            if let Ok(n) = s.parse::<i64>() {
                return Some(Value::Integer(n));
            }
            if let Ok(n) = s.parse::<f64>() {
                return Some(Value::Float(n));
            }
        }
        // '[' → 向量字面量
        b'[' => {
            if s.ends_with(']') {
                let inner = &s[1..s.len() - 1];
                let mut vec_data = Vec::new();
                for part in inner.split(',') {
                    let trimmed = part.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let val: f32 = trimmed.parse().ok()?;
                    vec_data.push(val);
                }
                if !vec_data.is_empty() {
                    return Some(Value::Vector(vec_data));
                }
            }
        }
        _ => {}
    }
    // 关键字匹配（大小写不敏感）
    if s.eq_ignore_ascii_case("NULL") {
        return Some(Value::Null);
    }
    if s.eq_ignore_ascii_case("TRUE") {
        return Some(Value::Boolean(true));
    }
    if s.eq_ignore_ascii_case("FALSE") {
        return Some(Value::Boolean(false));
    }
    if s.eq_ignore_ascii_case("NOW()") || s.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        return Some(Value::Timestamp(ts));
    }
    if s.eq_ignore_ascii_case("CURRENT_DATE") {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32
            / 86400;
        return Some(Value::Date(ts));
    }
    if s.eq_ignore_ascii_case("CURRENT_TIME") {
        let dur = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let day_secs = dur.as_secs() % 86400;
        let nanos = day_secs as i64 * 1_000_000_000 + dur.subsec_nanos() as i64;
        return Some(Value::Time(nanos));
    }
    // DATE 'YYYY-MM-DD' 字面量
    if let Some(inner) = strip_typed_literal(s, "DATE") {
        if let Some(d) = crate::types::parse_date_string(inner) {
            return Some(Value::Date(d));
        }
    }
    // TIME 'HH:MM:SS' 字面量
    if let Some(inner) = strip_typed_literal(s, "TIME") {
        if let Some(t) = crate::types::parse_time_string(inner) {
            return Some(Value::Time(t));
        }
    }
    // GEOPOINT(lat, lng) 字面量
    if let Some(inner) = strip_func_call(s, "GEOPOINT") {
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() == 2 {
            if let (Ok(lat), Ok(lng)) = (
                parts[0].trim().parse::<f64>(),
                parts[1].trim().parse::<f64>(),
            ) {
                return Some(Value::GeoPoint(lat, lng));
            }
        }
    }
    // M116: table.col 列引用（UPDATE ... FROM 的 WHERE 条件中使用）
    // 保存为 Text 以便执行层识别
    if s.contains('.')
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
    {
        return Some(Value::Text(s.to_string()));
    }
    None
}

/// M123：单 pass VALUES 行解析 — 合并 split + parse_value 为一次遍历。
/// 避免中间 Vec<&str> 分配和重复扫描。
pub(crate) fn parse_row_single_pass(s: &str) -> Result<Vec<Value>, crate::Error> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut row = Vec::with_capacity((len / 8).min(128) + 1);
    let mut start = 0;
    let mut in_quote = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut i = 0;
    while i < len {
        if bytes[i] == b'\'' {
            if in_quote && i + 1 < len && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_quote = !in_quote;
        } else if !in_quote {
            match bytes[i] {
                b'(' => paren_depth += 1,
                b')' => paren_depth -= 1,
                b'[' => bracket_depth += 1,
                b']' => bracket_depth -= 1,
                b',' if paren_depth == 0 && bracket_depth == 0 => {
                    let val_str = &s[start..i];
                    row.push(parse_value(val_str.trim()).ok_or_else(|| {
                        crate::Error::SqlParse(format!("cannot parse value: {}", val_str.trim()))
                    })?);
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    // 最后一个值
    let val_str = &s[start..];
    let trimmed = val_str.trim();
    if !trimmed.is_empty() {
        row.push(
            parse_value(trimmed).ok_or_else(|| {
                crate::Error::SqlParse(format!("cannot parse value: {}", trimmed))
            })?,
        );
    }
    Ok(row)
}

/// 在尊重单引号字符串和方括号的前提下，按顶层逗号分割。
pub(crate) fn split_respecting_quotes(s: &str) -> Vec<&str> {
    let mut parts = Vec::with_capacity((s.len() / 8).min(128) + 1);
    let mut start = 0;
    let mut in_quote = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if in_quote && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_quote = !in_quote;
        } else if !in_quote {
            match bytes[i] {
                b'(' => paren_depth += 1,
                b')' => paren_depth -= 1,
                b'[' => bracket_depth += 1,
                b']' => bracket_depth -= 1,
                b',' if paren_depth == 0 && bracket_depth == 0 => {
                    parts.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

/// 多行 VALUES 分割：`(1, 'a'), (2, 'b')` → ["1, 'a'", "2, 'b'"]
pub(crate) fn split_value_rows(s: &str) -> Result<Vec<&str>, crate::Error> {
    let s = s.trim();
    let mut rows = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len()
            && (bytes[i] == b' '
                || bytes[i] == b','
                || bytes[i] == b'\n'
                || bytes[i] == b'\r'
                || bytes[i] == b'\t')
        {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if bytes[i] != b'(' {
            return Err(crate::Error::SqlParse(format!(
                "VALUES syntax error, expected '(' at position {}",
                i
            )));
        }
        i += 1;
        let start = i;
        let mut depth = 1i32;
        let mut in_quote = false;
        while i < bytes.len() && depth > 0 {
            if bytes[i] == b'\'' {
                if in_quote && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_quote = !in_quote;
            } else if !in_quote {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                }
            }
            if depth > 0 {
                i += 1;
            }
        }
        if depth != 0 {
            return Err(crate::Error::SqlParse(
                "VALUES unmatched parentheses".to_string(),
            ));
        }
        rows.push(&s[start..i]);
        i += 1;
    }
    Ok(rows)
}

/// 检查位置 pos 是否在单引号字符串内。
/// 正确处理转义引号 `''`（SQL 标准转义）。
pub(crate) fn in_quote(s: &str, pos: usize) -> bool {
    let bytes = s.as_bytes();
    let mut q = false;
    let mut i = 0;
    while i < bytes.len() {
        if i == pos {
            return q;
        }
        if bytes[i] == b'\'' {
            if q && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                // 转义引号 '' — 跳过两个字符，不改变引号状态
                if i + 1 == pos {
                    return true; // pos 在转义引号内部，仍在引号内
                }
                i += 2;
                continue;
            }
            q = !q;
        }
        i += 1;
    }
    false
}

/// 在多个终止关键字中找到最早出现的位置。
/// 解决 `or_else` 链只取第一个 `Some` 而非最小位置的 bug。
pub(crate) fn min_keyword_pos(s: &str, keywords: &[&str]) -> usize {
    keywords
        .iter()
        .filter_map(|kw| find_keyword(s, kw))
        .min()
        .unwrap_or(s.len())
}

/// 在非引号区域查找关键字（前后须为空白或字符串边界）。
/// 使用 ASCII 大小写不敏感匹配，避免 to_uppercase() 的 UTF-8 偏移问题。
pub(crate) fn find_keyword(s: &str, keyword: &str) -> Option<usize> {
    let kw_bytes = keyword.as_bytes();
    let s_bytes = s.as_bytes();
    let kw_len = kw_bytes.len();
    if kw_len == 0 || s_bytes.len() < kw_len {
        return None;
    }
    let mut i = 0;
    while i + kw_len <= s_bytes.len() {
        // ASCII 大小写不敏感比较
        if s_bytes[i..i + kw_len].eq_ignore_ascii_case(kw_bytes) {
            let before_ok =
                i == 0 || s_bytes[i - 1].is_ascii_whitespace() || s_bytes[i - 1] == b'(';
            let after = i + kw_len;
            let after_ok = after >= s_bytes.len()
                || s_bytes[after].is_ascii_whitespace()
                || s_bytes[after] == b'('
                || s_bytes[after] == b')';
            if before_ok && after_ok && !in_quote(s, i) {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// M152：在顶层（括号外）查找关键字，跳过括号和引号内的内容。
/// 用于 UPDATE 解析器避免匹配子查询内的 FROM。
pub(crate) fn find_keyword_top_level(s: &str, keyword: &str) -> Option<usize> {
    let kw_bytes = keyword.as_bytes();
    let s_bytes = s.as_bytes();
    let kw_len = kw_bytes.len();
    if kw_len == 0 || s_bytes.len() < kw_len {
        return None;
    }
    let mut i = 0;
    let mut depth = 0i32;
    let mut in_q = false;
    while i < s_bytes.len() {
        if s_bytes[i] == b'\'' {
            if in_q && i + 1 < s_bytes.len() && s_bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_q = !in_q;
        } else if !in_q {
            if s_bytes[i] == b'(' {
                depth += 1;
            } else if s_bytes[i] == b')' {
                depth -= 1;
            }
        }
        if depth == 0 && !in_q && i + kw_len <= s_bytes.len() {
            if s_bytes[i..i + kw_len].eq_ignore_ascii_case(kw_bytes) {
                let before_ok =
                    i == 0 || s_bytes[i - 1].is_ascii_whitespace() || s_bytes[i - 1] == b'(';
                let after = i + kw_len;
                let after_ok = after >= s_bytes.len()
                    || s_bytes[after].is_ascii_whitespace()
                    || s_bytes[after] == b'('
                    || s_bytes[after] == b')';
                if before_ok && after_ok {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

/// 找到从 start 位置开始的 '(' 对应的 ')' 位置。
/// 正确处理转义引号 `''`（SQL 标准转义）。
pub(crate) fn find_matching_paren(s: &str, start: usize) -> Result<usize, crate::Error> {
    let bytes = s.as_bytes();
    if bytes.get(start) != Some(&b'(') {
        return Err(crate::Error::SqlParse("expected '('".to_string()));
    }
    let mut depth = 0i32;
    let mut in_q = false;
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if in_q && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2; // 跳过转义引号 ''
                continue;
            }
            in_q = !in_q;
        } else if !in_q {
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
        }
        i += 1;
    }
    Err(crate::Error::SqlParse("unmatched parentheses".to_string()))
}

/// M94：从函数调用 `FUNC(args)` 中提取 args 部分（不区分大小写）。
pub(crate) fn strip_func_call<'a>(s: &'a str, func: &str) -> Option<&'a str> {
    let s = s.trim();
    if s.len() < func.len() + 2 {
        return None;
    }
    if !s[..func.len()].eq_ignore_ascii_case(func) {
        return None;
    }
    let rest = s[func.len()..].trim_start();
    rest.strip_prefix('(')?.strip_suffix(')')
}

/// 提取 SQL 类型化字面量：`TYPE 'value'` → `value`（不区分大小写）。
pub(crate) fn strip_typed_literal<'a>(s: &'a str, type_name: &str) -> Option<&'a str> {
    let s = s.trim();
    if s.len() < type_name.len() + 3 {
        return None;
    }
    if !s[..type_name.len()].eq_ignore_ascii_case(type_name) {
        return None;
    }
    let rest = s[type_name.len()..].trim_start();
    rest.strip_prefix('\'')?.strip_suffix('\'')
}

/// 从字符串开头提取表名（到空白或结尾），返回 (table, remainder)。
pub(crate) fn extract_table_name(s: &str) -> (String, &str) {
    let s = s.trim();
    if s.starts_with('`') || s.starts_with('"') {
        let quote = s.as_bytes()[0];
        if let Some(end) = s[1..].find(|c: char| c as u8 == quote) {
            return (s[..end + 2].to_string(), s[end + 2..].trim_start());
        }
    }
    match s.find(|c: char| c.is_whitespace()) {
        Some(pos) => (s[..pos].to_string(), &s[pos..]),
        None => (s.to_string(), ""),
    }
}
