/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 内置函数实现：字符串、空值处理、数学、类型转换。
//!
//! 所有函数接收 `&[Value]` 参数列表，返回 `Value`。
//! 由 `expr_eval::eval_project_op` 分派调用。
//!
//! 部分函数移植自上游开源项目（已注明来源）：
//! - apache/datafusion：REGEXP_REPLACE、REGEXP_LIKE、SPLIT_PART、REPEAT、TRANSLATE
//! - apache/datafusion-sqlparser-rs：DATE_TRUNC（语法参考）

use crate::types::Value;

/// Value 转字符串（Value 未实现 Display，手动转换）。
pub(super) fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Date(d) => crate::types::date_to_string(*d),
        Value::Time(t) => crate::types::time_to_string(*t),
        Value::Blob(b) => format!("{:?}", b),
        Value::Jsonb(j) => j.to_string(),
        Value::Vector(v) => format!("{:?}", v),
        Value::GeoPoint(lat, lng) => format!("GEOPOINT({},{})", lat, lng),
        Value::Placeholder(_) => String::new(),
    }
}

/// SQL 内置函数求值入口：根据函数名分派到具体实现。
pub(super) fn eval_sql_func(name: &str, args: &[Value]) -> Value {
    match name {
        // 字符串函数
        "UPPER" | "UCASE" => func_upper(args),
        "LOWER" | "LCASE" => func_lower(args),
        "LENGTH" | "LEN" => func_length(args),
        "SUBSTR" | "SUBSTRING" => func_substr(args),
        "TRIM" => func_trim(args),
        "LTRIM" => func_ltrim(args),
        "RTRIM" => func_rtrim(args),
        "REPLACE" => func_replace(args),
        "CONCAT" => func_concat(args),
        "LEFT" => func_left(args),
        "RIGHT" => func_right(args),
        "REVERSE" => func_reverse(args),
        "LPAD" => func_lpad(args),
        "RPAD" => func_rpad(args),
        "CHARINDEX" => func_charindex(args),
        "INSTR" => func_instr(args),
        "CHAR" => func_char(args),
        "ASCII" => func_ascii(args),
        // 移植自 apache/datafusion 的字符串扩展函数
        "REGEXP_REPLACE" => func_regexp_replace(args),
        "REGEXP_LIKE" => func_regexp_like(args),
        "SPLIT_PART" => func_split_part(args),
        "REPEAT" => func_repeat(args),
        "TRANSLATE" => func_translate(args),
        // 空值处理 / 条件
        "COALESCE" => func_coalesce(args),
        "IFNULL" | "ISNULL" => func_ifnull(args),
        "NULLIF" => func_nullif(args),
        "IF" | "IIF" => func_if(args),
        // 数学函数
        "ABS" => func_abs(args),
        "ROUND" => func_round(args),
        "CEIL" | "CEILING" => func_ceil(args),
        "FLOOR" => func_floor(args),
        "MOD" => func_mod(args),
        "POWER" | "POW" => func_power(args),
        "SQRT" => func_sqrt(args),
        "SIGN" => func_sign(args),
        "TRUNCATE" | "TRUNC" => func_truncate(args),
        "RAND" | "RANDOM" => func_rand(),
        "PI" => Value::Float(std::f64::consts::PI),
        "EXP" => func_exp(args),
        "LOG" | "LN" => func_log(args),
        "LOG10" => func_log10(args),
        // 类型转换
        "CAST" => func_cast(args),
        "CONVERT" => func_convert(args),
        // 日期时间函数（委托子模块）
        "NOW" | "GETDATE" | "CURRENT_TIMESTAMP" => super::sql_funcs_dt::func_now(),
        "YEAR" => super::sql_funcs_dt::func_year(args),
        "MONTH" => super::sql_funcs_dt::func_month(args),
        "DAY" | "DAYOFMONTH" => super::sql_funcs_dt::func_day(args),
        "HOUR" => super::sql_funcs_dt::func_hour(args),
        "MINUTE" => super::sql_funcs_dt::func_minute(args),
        "SECOND" => super::sql_funcs_dt::func_second(args),
        "DATEDIFF" => super::sql_funcs_dt::func_datediff(args),
        "DATEADD" => super::sql_funcs_dt::func_dateadd(args),
        "DATE_ADD" => super::sql_funcs_dt::func_date_add(args),
        "DATE_SUB" => super::sql_funcs_dt::func_date_sub(args),
        // P1 日期时间扩展
        "DATEPART" => super::sql_funcs_dt::func_datepart(args),
        "DATE_FORMAT" => super::sql_funcs_dt::func_date_format(args),
        "TIME_BUCKET" => super::sql_funcs_dt::func_time_bucket(args),
        "WEEKDAY" => super::sql_funcs_dt::func_weekday(args),
        "DAYOFWEEK" => super::sql_funcs_dt::func_dayofweek(args),
        "QUARTER" => super::sql_funcs_dt::func_quarter(args),
        "WEEK" => super::sql_funcs_dt::func_week(args),
        // P2 日期时间扩展
        "LAST_DAY" => super::sql_funcs_dt::func_last_day(args),
        "TIMESTAMPDIFF" => super::sql_funcs_dt::func_timestampdiff(args),
        "TIMESTAMPADD" => super::sql_funcs_dt::func_timestampadd(args),
        // 移植自 apache/datafusion 的日期时间扩展函数（语法参考 sqlparser-rs）
        "DATE_TRUNC" => super::sql_funcs_dt::func_date_trunc(args),
        // P1 哈希函数（委托子模块）
        "MD5" => super::sql_funcs_hash::func_md5(args),
        "SHA1" => super::sql_funcs_hash::func_sha1(args),
        "SHA2" => super::sql_funcs_hash::func_sha2(args),
        // JSON 函数（委托子模块）
        "JSON_EXTRACT" => super::sql_funcs_json::func_json_extract(args),
        "JSON_EXTRACT_TEXT" => super::sql_funcs_json::func_json_extract_text(args),
        "JSON_SET" => super::sql_funcs_json::func_json_set(args),
        "JSON_REMOVE" => super::sql_funcs_json::func_json_remove(args),
        "JSON_TYPE" => super::sql_funcs_json::func_json_type(args),
        "JSON_ARRAY_LENGTH" => super::sql_funcs_json::func_json_array_length(args),
        "JSON_KEYS" => super::sql_funcs_json::func_json_keys(args),
        "JSON_VALID" => super::sql_funcs_json::func_json_valid(args),
        "JSON_CONTAINS" => super::sql_funcs_json::func_json_contains(args),
        // 系统函数
        "DATABASE" | "CURRENT_DATABASE" => Value::Text("talon".into()),
        "VERSION" => Value::Text(env!("CARGO_PKG_VERSION").into()),
        "ROW_COUNT" => Value::Integer(0), // 占位：需引擎状态支持
        "LAST_INSERT_ID" => Value::Integer(0), // 占位：需引擎状态支持
        "USER" | "CURRENT_USER" => Value::Text("talon".into()),
        "CONNECTION_ID" => Value::Integer(0),
        _ => Value::Null,
    }
}

/// 判断函数名是否为已知内置函数。
pub(super) fn is_known_func(name: &str) -> bool {
    matches!(
        name,
        "UPPER"
            | "UCASE"
            | "LOWER"
            | "LCASE"
            | "LENGTH"
            | "LEN"
            | "SUBSTR"
            | "SUBSTRING"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "REPLACE"
            | "CONCAT"
            | "LEFT"
            | "RIGHT"
            | "REVERSE"
            | "LPAD"
            | "RPAD"
            | "CHARINDEX"
            | "INSTR"
            | "CHAR"
            | "ASCII"
            | "REGEXP_REPLACE"
            | "REGEXP_LIKE"
            | "SPLIT_PART"
            | "REPEAT"
            | "TRANSLATE"
            | "COALESCE"
            | "IFNULL"
            | "ISNULL"
            | "NULLIF"
            | "IF"
            | "IIF"
            | "ABS"
            | "ROUND"
            | "CEIL"
            | "CEILING"
            | "FLOOR"
            | "MOD"
            | "POWER"
            | "POW"
            | "SQRT"
            | "SIGN"
            | "TRUNCATE"
            | "TRUNC"
            | "RAND"
            | "RANDOM"
            | "PI"
            | "EXP"
            | "LOG"
            | "LN"
            | "LOG10"
            | "CAST"
            | "CONVERT"
            | "NOW"
            | "GETDATE"
            | "CURRENT_TIMESTAMP"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "DAYOFMONTH"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "DATEDIFF"
            | "DATEADD"
            | "DATE_ADD"
            | "DATE_SUB"
            | "DATEPART"
            | "DATE_FORMAT"
            | "TIME_BUCKET"
            | "WEEKDAY"
            | "DAYOFWEEK"
            | "QUARTER"
            | "WEEK"
            | "LAST_DAY"
            | "TIMESTAMPDIFF"
            | "TIMESTAMPADD"
            | "DATE_TRUNC"
            | "MD5"
            | "SHA1"
            | "SHA2"
            | "DATABASE"
            | "CURRENT_DATABASE"
            | "VERSION"
            | "ROW_COUNT"
            | "LAST_INSERT_ID"
            | "USER"
            | "CURRENT_USER"
            | "CONNECTION_ID"
            | "JSON_EXTRACT"
            | "JSON_EXTRACT_TEXT"
            | "JSON_SET"
            | "JSON_REMOVE"
            | "JSON_TYPE"
            | "JSON_ARRAY_LENGTH"
            | "JSON_KEYS"
            | "JSON_VALID"
            | "JSON_CONTAINS"
    )
}

// ── 字符串函数 ────────────────────────────────────────────

/// UPPER(x) — 转大写。NULL 输入返回 NULL。
fn func_upper(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.to_uppercase()),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Text(value_to_string(v).to_uppercase()),
    }
}

/// LOWER(x) — 转小写。NULL 输入返回 NULL。
fn func_lower(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.to_lowercase()),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Text(value_to_string(v).to_lowercase()),
    }
}

/// LENGTH(x) — 字符串长度（字符数）。NULL 返回 NULL。
fn func_length(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Integer(s.chars().count() as i64),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Integer(value_to_string(v).chars().count() as i64),
    }
}

/// SUBSTR(x, start[, len]) — 子串提取（1-based 索引，SQL 标准）。
/// start < 0 时从末尾倒数。len 省略时取到末尾。
fn func_substr(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        Some(v) => return Value::Text(substr_impl(&value_to_string(v), args)),
    };
    Value::Text(substr_impl(s, args))
}

/// SUBSTR 内部实现。
fn substr_impl(s: &str, args: &[Value]) -> String {
    let start = match args.get(1) {
        Some(Value::Integer(n)) => *n,
        Some(Value::Float(f)) => *f as i64,
        _ => return s.to_string(),
    };
    let chars: Vec<char> = s.chars().collect();
    let total = chars.len() as i64;
    // SQL SUBSTR 是 1-based；负数从末尾倒数
    let zero_idx = if start > 0 {
        (start - 1).min(total) as usize
    } else if start < 0 {
        (total + start).max(0) as usize
    } else {
        0 // start == 0 按 SQLite 行为等同于 1
    };
    let len = match args.get(2) {
        Some(Value::Integer(n)) => Some(*n as usize),
        Some(Value::Float(f)) => Some(*f as usize),
        _ => None,
    };
    match len {
        Some(l) => chars[zero_idx..].iter().take(l).collect(),
        None => chars[zero_idx..].iter().collect(),
    }
}

/// TRIM(x) — 去除首尾空白。
fn func_trim(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.trim().to_string()),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Text(value_to_string(v).trim().to_string()),
    }
}

/// LTRIM(x) — 去除左侧空白。
fn func_ltrim(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.trim_start().to_string()),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Text(value_to_string(v).trim_start().to_string()),
    }
}

/// RTRIM(x) — 去除右侧空白。
fn func_rtrim(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.trim_end().to_string()),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Text(value_to_string(v).trim_end().to_string()),
    }
}

/// REPLACE(x, from, to) — 字符串替换。
fn func_replace(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Null) | None => return Value::Null,
        Some(v) => value_to_string(v),
    };
    let from = match args.get(1) {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Text(s),
    };
    let to = match args.get(2) {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => "",
    };
    Value::Text(s.replace(from, to))
}

/// CONCAT(a, b, ...) — 字符串拼接。NULL 参数视为空串（MySQL 兼容）。
fn func_concat(args: &[Value]) -> Value {
    let mut result = String::new();
    for arg in args {
        match arg {
            Value::Null => {} // MySQL: CONCAT 中 NULL 视为空串
            Value::Text(s) => result.push_str(s),
            v => result.push_str(&value_to_string(v)),
        }
    }
    Value::Text(result)
}

/// LEFT(s, n) — 左截取 n 个字符。
fn func_left(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        Some(v) => {
            return Value::Text(
                value_to_string(v)
                    .chars()
                    .take(to_usize(args.get(1)))
                    .collect(),
            )
        }
    };
    Value::Text(s.chars().take(to_usize(args.get(1))).collect())
}

/// RIGHT(s, n) — 右截取 n 个字符。
fn func_right(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        Some(v) => {
            let t = value_to_string(v);
            let n = to_usize(args.get(1));
            let chars: Vec<char> = t.chars().collect();
            return Value::Text(chars[chars.len().saturating_sub(n)..].iter().collect());
        }
    };
    let n = to_usize(args.get(1));
    let chars: Vec<char> = s.chars().collect();
    Value::Text(chars[chars.len().saturating_sub(n)..].iter().collect())
}

/// REVERSE(s) — 字符串反转。
fn func_reverse(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) => Value::Text(s.chars().rev().collect()),
        Some(Value::Null) | None => Value::Null,
        Some(v) => Value::Text(value_to_string(v).chars().rev().collect()),
    }
}

/// LPAD(s, len, pad) — 左填充到指定长度。
fn func_lpad(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Null) | None => return Value::Null,
        Some(v) => value_to_string(v),
    };
    let target_len = to_usize(args.get(1));
    let pad = match args.get(2) {
        Some(Value::Text(p)) => p.clone(),
        _ => " ".to_string(),
    };
    let s_char_len = s.chars().count();
    if s_char_len >= target_len || pad.is_empty() {
        return Value::Text(s.chars().take(target_len).collect());
    }
    let need = target_len - s_char_len;
    let mut prefix = String::with_capacity(need + s.len());
    for c in pad.chars().cycle().take(need) {
        prefix.push(c);
    }
    prefix.push_str(&s);
    Value::Text(prefix)
}

/// RPAD(s, len, pad) — 右填充到指定长度。
fn func_rpad(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Null) | None => return Value::Null,
        Some(v) => value_to_string(v),
    };
    let target_len = to_usize(args.get(1));
    let pad = match args.get(2) {
        Some(Value::Text(p)) => p.clone(),
        _ => " ".to_string(),
    };
    let s_char_len = s.chars().count();
    if s_char_len >= target_len || pad.is_empty() {
        return Value::Text(s.chars().take(target_len).collect());
    }
    let need = target_len - s_char_len;
    let mut result = s;
    result.reserve(need);
    for c in pad.chars().cycle().take(need) {
        result.push(c);
    }
    Value::Text(result)
}

/// CHARINDEX(substr, s[, start]) — 子串位置（1-based，0=不存在）。SQL Server 风格。
/// start 为字符偏移（非字节偏移），多字节 UTF-8 安全。
fn func_charindex(args: &[Value]) -> Value {
    let substr = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let s = match args.get(1) {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    let start_char = to_usize(args.get(2)).max(1) - 1; // 1-based → 0-based 字符偏移
                                                       // 将字符偏移转为字节偏移
    let byte_offset = s
        .char_indices()
        .nth(start_char)
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    match s[byte_offset..].find(substr) {
        Some(byte_pos) => {
            // 将字节位置转回字符位置
            let char_pos = s[..byte_offset + byte_pos].chars().count();
            Value::Integer((char_pos + 1) as i64)
        }
        None => Value::Integer(0),
    }
}

/// INSTR(s, substr) — 子串位置（1-based 字符偏移，0=不存在）。MySQL 风格。
/// 使用字符偏移而非字节偏移，多字节 UTF-8 安全。
fn func_instr(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let substr = match args.get(1) {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    match s.find(substr) {
        Some(byte_pos) => {
            // 将字节位置转为字符位置（UTF-8 安全）
            let char_pos = s[..byte_pos].chars().count();
            Value::Integer((char_pos + 1) as i64)
        }
        None => Value::Integer(0),
    }
}

/// CHAR(n) — ASCII/Unicode 码转字符。
fn func_char(args: &[Value]) -> Value {
    let n = match args.first() {
        Some(Value::Integer(n)) => *n as u32,
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    match char::from_u32(n) {
        Some(c) => Value::Text(c.to_string()),
        None => Value::Null,
    }
}

/// ASCII(s) — 首字符 ASCII 码。
fn func_ascii(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Text(s)) if !s.is_empty() => Value::Integer(s.as_bytes()[0] as i64),
        Some(Value::Null) | None => Value::Null,
        _ => Value::Null,
    }
}

// ── 移植自 apache/datafusion 的字符串扩展函数 ────────────────

/// REGEXP_REPLACE(str, pattern, replacement[, flags]) — 正则替换。
///
/// 移植来源：apache/datafusion `string_expressions::regexp_replace`。
/// 用途：AI 场景下清洗文本、标准化格式（如去除 Markdown 标记、提取实体）。
/// - flags 可选，`g` 替换所有匹配（默认仅替换第一个）。
/// - 任意参数为 NULL 返回 NULL。
fn func_regexp_replace(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let pattern = match args.get(1) {
        Some(Value::Text(p)) => p.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    let replacement = match args.get(2) {
        Some(Value::Text(r)) => r.as_str(),
        Some(Value::Null) => return Value::Null,
        None => "",
        _ => return Value::Null,
    };
    let global = matches!(args.get(3), Some(Value::Text(f)) if f.contains('g'));
    match regex::Regex::new(pattern) {
        Ok(re) => {
            let result = if global {
                re.replace_all(s, replacement).into_owned()
            } else {
                re.replace(s, replacement).into_owned()
            };
            Value::Text(result)
        }
        Err(_) => Value::Null,
    }
}

/// REGEXP_LIKE(str, pattern) — 正则匹配检查，返回布尔值。
///
/// 移植来源：apache/datafusion `string_expressions::regexp_like`。
/// 用途：AI 场景下过滤符合特定模式的文本（如验证 URL、邮箱格式）。
/// - 任意参数为 NULL 返回 NULL。
fn func_regexp_like(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let pattern = match args.get(1) {
        Some(Value::Text(p)) => p.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    match regex::Regex::new(pattern) {
        Ok(re) => Value::Boolean(re.is_match(s)),
        Err(_) => Value::Null,
    }
}

/// SPLIT_PART(str, delimiter, index) — 按分隔符分割，返回第 N 个部分（1-based）。
///
/// 移植来源：apache/datafusion `string_expressions::split_part`。
/// 用途：AI 场景下解析结构化文本输出（如 CSV、路径、tag 列表）。
/// - index 从 1 开始；超出范围返回空字符串。
/// - 任意参数为 NULL 返回 NULL。
fn func_split_part(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let delimiter = match args.get(1) {
        Some(Value::Text(d)) => d.as_str(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    let index = match args.get(2) {
        Some(Value::Integer(n)) => *n,
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    if delimiter.is_empty() {
        return if index == 1 {
            Value::Text(s.to_string())
        } else {
            Value::Text(String::new())
        };
    }
    let parts: Vec<&str> = s.split(delimiter).collect();
    let idx = if index > 0 {
        (index - 1) as usize
    } else {
        return Value::Text(String::new());
    };
    Value::Text(parts.get(idx).unwrap_or(&"").to_string())
}

/// REPEAT(str, n) — 将字符串重复 n 次。
///
/// 移植来源：apache/datafusion `string_expressions::repeat`。
/// 用途：AI 场景下生成测试数据、填充固定格式模板。
/// - n <= 0 返回空字符串；任意参数为 NULL 返回 NULL。
fn func_repeat(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let n = match args.get(1) {
        Some(Value::Integer(n)) => *n,
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    if n <= 0 {
        return Value::Text(String::new());
    }
    Value::Text(s.repeat(n as usize))
}

/// TRANSLATE(str, from_chars, to_chars) — 字符逐一替换（类似 Unix `tr`）。
///
/// 移植来源：apache/datafusion `string_expressions::translate`。
/// 用途：AI 场景下字符集转换、去除特定字符（如清理 prompt 输入中的特殊符号）。
/// - from_chars 中每个字符被替换为 to_chars 对应位置的字符。
/// - to_chars 比 from_chars 短时，超出部分的字符被删除。
/// - 任意参数为 NULL 返回 NULL。
fn func_translate(args: &[Value]) -> Value {
    let s = match args.first() {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let from = match args.get(1) {
        Some(Value::Text(f)) => f.clone(),
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    let to = match args.get(2) {
        Some(Value::Text(t)) => t.clone(),
        Some(Value::Null) => return Value::Null,
        None => String::new(),
        _ => return Value::Null,
    };
    let from_chars: Vec<char> = from.chars().collect();
    let to_chars: Vec<char> = to.chars().collect();
    let result: String = s
        .chars()
        .filter_map(|c| {
            if let Some(i) = from_chars.iter().position(|&fc| fc == c) {
                to_chars.get(i).copied().map(Some).unwrap_or(None)
            } else {
                Some(c)
            }
        })
        .collect();
    Value::Text(result)
}

/// 辅助：从 Value 提取 usize（默认 0）。
fn to_usize(v: Option<&Value>) -> usize {
    match v {
        Some(Value::Integer(n)) => (*n).max(0) as usize,
        Some(Value::Float(f)) => (*f).max(0.0) as usize,
        _ => 0,
    }
}

// ── 空值处理函数 ──────────────────────────────────────────

/// COALESCE(a, b, ...) — 返回第一个非 NULL 参数。
fn func_coalesce(args: &[Value]) -> Value {
    for arg in args {
        if !matches!(arg, Value::Null) {
            return arg.clone();
        }
    }
    Value::Null
}

/// IFNULL(a, b) — 如果 a 为 NULL 返回 b，否则返回 a（MySQL/SQLite 兼容）。
fn func_ifnull(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Null) | None => args.get(1).cloned().unwrap_or(Value::Null),
        Some(v) => v.clone(),
    }
}

/// NULLIF(a, b) — 如果 a == b 返回 NULL，否则返回 a。
fn func_nullif(args: &[Value]) -> Value {
    let a = args.first().cloned().unwrap_or(Value::Null);
    let b = args.get(1).cloned().unwrap_or(Value::Null);
    if a == b {
        Value::Null
    } else {
        a
    }
}

/// IF(cond, a, b) / IIF(cond, a, b) — 三元条件。cond 为真返回 a，否则返回 b。
fn func_if(args: &[Value]) -> Value {
    let cond = match args.first() {
        Some(Value::Boolean(b)) => *b,
        Some(Value::Integer(n)) => *n != 0,
        Some(Value::Null) | None => false,
        _ => false,
    };
    if cond {
        args.get(1).cloned().unwrap_or(Value::Null)
    } else {
        args.get(2).cloned().unwrap_or(Value::Null)
    }
}

// ── 数学函数 ──────────────────────────────────────────────

/// ABS(x) — 绝对值。
fn func_abs(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Integer(n)) => Value::Integer(n.abs()),
        Some(Value::Float(f)) => Value::Float(f.abs()),
        Some(Value::Null) | None => Value::Null,
        _ => Value::Null,
    }
}

/// ROUND(x[, n]) — 四舍五入到 n 位小数（默认 0）。
/// decimals 限制在 [-18, 18] 范围内防止 10^n 溢出。
fn func_round(args: &[Value]) -> Value {
    let val = match args.first() {
        Some(Value::Integer(n)) => *n as f64,
        Some(Value::Float(f)) => *f,
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let decimals = match args.get(1) {
        Some(Value::Integer(n)) => (*n as i32).clamp(-18, 18),
        Some(Value::Float(f)) => (*f as i32).clamp(-18, 18),
        _ => 0,
    };
    let factor = 10f64.powi(decimals);
    let rounded = (val * factor).round() / factor;
    if decimals <= 0 && rounded.abs() < 9.007_199_254_740_992e15 {
        Value::Integer(rounded as i64)
    } else {
        Value::Float(rounded)
    }
}

/// CEIL(x) / CEILING(x) — 向上取整。
fn func_ceil(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Integer(n)) => Value::Integer(*n),
        Some(Value::Float(f)) => Value::Integer(f.ceil() as i64),
        Some(Value::Null) | None => Value::Null,
        _ => Value::Null,
    }
}

/// FLOOR(x) — 向下取整。
fn func_floor(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Integer(n)) => Value::Integer(*n),
        Some(Value::Float(f)) => Value::Integer(f.floor() as i64),
        Some(Value::Null) | None => Value::Null,
        _ => Value::Null,
    }
}

/// MOD(a, b) — 取模。b=0 返回 NULL。
fn func_mod(args: &[Value]) -> Value {
    let a = to_f64(args.first());
    let b = to_f64(args.get(1));
    match (a, b) {
        (Some(_), Some(b)) if b == 0.0 => Value::Null,
        (Some(a), Some(b)) => {
            let r = a % b;
            if r == r.trunc() && r.abs() < 9.007_199_254_740_992e15 {
                Value::Integer(r as i64)
            } else {
                Value::Float(r)
            }
        }
        _ => Value::Null,
    }
}

/// POWER(base, exp) / POW(base, exp) — 幂运算。
fn func_power(args: &[Value]) -> Value {
    match (to_f64(args.first()), to_f64(args.get(1))) {
        (Some(base), Some(exp)) => Value::Float(base.powf(exp)),
        _ => Value::Null,
    }
}

/// SQRT(n) — 平方根。n<0 返回 NULL。
fn func_sqrt(args: &[Value]) -> Value {
    match to_f64(args.first()) {
        Some(n) if n >= 0.0 => Value::Float(n.sqrt()),
        _ => Value::Null,
    }
}

/// SIGN(n) — 符号：-1/0/1。
fn func_sign(args: &[Value]) -> Value {
    match to_f64(args.first()) {
        Some(n) if n > 0.0 => Value::Integer(1),
        Some(n) if n < 0.0 => Value::Integer(-1),
        Some(_) => Value::Integer(0),
        None => Value::Null,
    }
}

/// TRUNCATE(n, decimals) / TRUNC(n, decimals) — 截断（不四舍五入）。
/// decimals 限制在 [-18, 18] 范围内防止 10^n 溢出。
fn func_truncate(args: &[Value]) -> Value {
    let val = match to_f64(args.first()) {
        Some(v) => v,
        None => return Value::Null,
    };
    let decimals = match args.get(1) {
        Some(Value::Integer(n)) => (*n as i32).clamp(-18, 18),
        Some(Value::Float(f)) => (*f as i32).clamp(-18, 18),
        _ => 0,
    };
    let factor = 10f64.powi(decimals);
    let truncated = (val * factor).trunc() / factor;
    if decimals <= 0 && truncated.abs() < 9.007_199_254_740_992e15 {
        Value::Integer(truncated as i64)
    } else {
        Value::Float(truncated)
    }
}

/// RAND() / RANDOM() — 随机数 [0.0, 1.0)。
/// 注意：基于时间纳秒的简单伪随机，不适用于密码学场景。
fn func_rand() -> Value {
    // 简单伪随机：基于时间纳秒
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Value::Float((ns as f64) / 4_294_967_296.0)
}

/// EXP(n) — e 的 n 次方。
fn func_exp(args: &[Value]) -> Value {
    match to_f64(args.first()) {
        Some(n) => Value::Float(n.exp()),
        None => Value::Null,
    }
}

/// LOG(n) / LN(n) — 自然对数。n<=0 返回 NULL。
fn func_log(args: &[Value]) -> Value {
    match to_f64(args.first()) {
        Some(n) if n > 0.0 => Value::Float(n.ln()),
        _ => Value::Null,
    }
}

/// LOG10(n) — 以 10 为底对数。n<=0 返回 NULL。
fn func_log10(args: &[Value]) -> Value {
    match to_f64(args.first()) {
        Some(n) if n > 0.0 => Value::Float(n.log10()),
        _ => Value::Null,
    }
}

/// 辅助：从 Value 提取 f64。
fn to_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Integer(n)) => Some(*n as f64),
        Some(Value::Float(f)) => Some(*f),
        _ => None,
    }
}

// ── 类型转换 ──────────────────────────────────────────────

/// CAST(x AS type) — 类型转换。
/// 特殊处理：参数列表中 args[0] = 值，args[1] = Text("目标类型名")。
fn func_cast(args: &[Value]) -> Value {
    let val = match args.first() {
        Some(v) => v,
        None => return Value::Null,
    };
    if matches!(val, Value::Null) {
        return Value::Null;
    }
    let target = match args.get(1) {
        Some(Value::Text(t)) => t.to_uppercase(),
        _ => return val.clone(),
    };
    match target.as_str() {
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" => match val {
            Value::Text(s) => Value::Text(s.clone()),
            v => Value::Text(value_to_string(v)),
        },
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => match val {
            Value::Integer(n) => Value::Integer(*n),
            Value::Float(f) => Value::Integer(*f as i64),
            Value::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Integer)
                .unwrap_or(Value::Null),
            Value::Boolean(b) => Value::Integer(if *b { 1 } else { 0 }),
            _ => Value::Null,
        },
        "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "NUMERIC" => match val {
            Value::Float(f) => Value::Float(*f),
            Value::Integer(n) => Value::Float(*n as f64),
            Value::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        "BOOLEAN" | "BOOL" => match val {
            Value::Boolean(b) => Value::Boolean(*b),
            Value::Integer(n) => Value::Boolean(*n != 0),
            Value::Text(s) => {
                let u = s.to_uppercase();
                Value::Boolean(u == "TRUE" || u == "1" || u == "YES")
            }
            _ => Value::Null,
        },
        "DATE" => match val {
            Value::Date(d) => Value::Date(*d),
            Value::Integer(n) => Value::Date(*n as i32),
            Value::Text(s) => {
                if let Some(d) = crate::types::parse_date_string(s) {
                    Value::Date(d)
                } else if let Ok(n) = s.parse::<i32>() {
                    Value::Date(n)
                } else {
                    Value::Null
                }
            }
            Value::Timestamp(ts) => Value::Date((*ts / 86_400_000) as i32),
            _ => Value::Null,
        },
        "TIME" | "TIMETZ" => match val {
            Value::Time(t) => Value::Time(*t),
            Value::Integer(n) => Value::Time(*n),
            Value::Text(s) => {
                if let Some(t) = crate::types::parse_time_string(s) {
                    Value::Time(t)
                } else if let Ok(n) = s.parse::<i64>() {
                    Value::Time(n)
                } else {
                    Value::Null
                }
            }
            Value::Timestamp(ts) => {
                let day_ms = *ts % 86_400_000;
                Value::Time(day_ms * 1_000_000)
            }
            _ => Value::Null,
        },
        _ => val.clone(), // 未知类型保持原值
    }
}

/// CONVERT(type, x) — SQL Server 风格类型转换。
/// 参数：args[0] = Text("TYPE"), args[1] = 值。
fn func_convert(args: &[Value]) -> Value {
    // CONVERT(type, x) → 内部转为 CAST 语义：[值, 类型]
    if args.len() >= 2 {
        func_cast(&[args[1].clone(), args[0].clone()])
    } else {
        Value::Null
    }
}
