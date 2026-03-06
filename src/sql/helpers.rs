/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 模块级辅助函数：WHERE 条件匹配、LIKE、值解析等。
//!
//! 索引 key 构造已移到 index_key.rs（有序字节编码）。
use super::parser::{WhereCondition, WhereExpr, WhereOp};
use crate::types::{Schema, Value};
use crate::Error;

/// 从 WHERE 表达式中提取单 Eq 条件（快速路径）。
pub(super) fn single_eq_condition(expr: &WhereExpr) -> Option<(&str, &Value)> {
    match expr {
        WhereExpr::Leaf(c) if c.op == WhereOp::Eq && c.jsonb_path.is_none() => {
            Some((&c.column, &c.value))
        }
        WhereExpr::And(children) if children.len() == 1 => single_eq_condition(&children[0]),
        _ => None,
    }
}
/// 比较两个 Value 的大小。
pub(super) fn value_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        _ => None,
    }
}
/// 检查一行是否满足 WHERE 表达式树（AND/OR/嵌套 + JSONB path）。
pub(super) fn row_matches(row: &[Value], schema: &Schema, expr: &WhereExpr) -> Result<bool, Error> {
    match expr {
        WhereExpr::Leaf(cond) => {
            // M153: EXISTS/NOT EXISTS — 无列引用，直接评估
            if cond.op == WhereOp::Exists || cond.op == WhereOp::NotExists {
                return Ok(eval_condition(&Value::Null, cond));
            }
            let col_idx = schema
                .column_index_by_name(&cond.column)
                .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", cond.column)))?;
            let lhs = if let Some(ref path_key) = cond.jsonb_path {
                extract_jsonb_text(&row[col_idx], path_key)
            } else {
                row[col_idx].clone()
            };
            // M118：右侧列引用 — 从行数据中取列值作为比较右侧
            if let Some(ref rhs_col) = cond.value_column {
                let rhs_idx = schema
                    .column_index_by_name(rhs_col)
                    .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", rhs_col)))?;
                let rhs = &row[rhs_idx];
                return Ok(eval_cmp(&lhs, cond.op, rhs));
            }
            Ok(eval_condition(&lhs, cond))
        }
        WhereExpr::And(children) => {
            for child in children {
                if !row_matches(row, schema, child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        WhereExpr::Or(children) => {
            for child in children {
                if row_matches(row, schema, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

/// 从 JSONB 值中提取指定 key 的文本值（`->>` 语义：返回 Text）。
fn extract_jsonb_text(value: &Value, key: &str) -> Value {
    let json = match value {
        Value::Jsonb(j) => Some(j.clone()),
        Value::Text(s) => serde_json::from_str::<serde_json::Value>(s).ok(),
        _ => None,
    };
    match json.as_ref().and_then(|j| j.get(key)) {
        Some(serde_json::Value::String(s)) => Value::Text(s.clone()),
        Some(serde_json::Value::Number(n)) => Value::Text(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Value::Text(b.to_string()),
        _ => Value::Null,
    }
}

/// M118：列对列比较（CHECK 约束中 `lo <= hi`）。
fn eval_cmp(lhs: &Value, op: WhereOp, rhs: &Value) -> bool {
    match op {
        WhereOp::Eq => lhs == rhs,
        WhereOp::Ne => lhs != rhs,
        WhereOp::Lt => value_cmp(lhs, rhs).map(|o| o.is_lt()).unwrap_or(false),
        WhereOp::Le => value_cmp(lhs, rhs).map(|o| !o.is_gt()).unwrap_or(false),
        WhereOp::Gt => value_cmp(lhs, rhs).map(|o| o.is_gt()).unwrap_or(false),
        WhereOp::Ge => value_cmp(lhs, rhs).map(|o| !o.is_lt()).unwrap_or(false),
        _ => false,
    }
}

/// 评估单个 WHERE 条件（支持 LIKE/IN/BETWEEN）。
fn eval_condition(lhs: &Value, cond: &WhereCondition) -> bool {
    match cond.op {
        WhereOp::Eq => lhs == &cond.value,
        WhereOp::Ne => lhs != &cond.value,
        WhereOp::Lt => value_cmp(lhs, &cond.value)
            .map(|o| o.is_lt())
            .unwrap_or(false),
        WhereOp::Le => value_cmp(lhs, &cond.value)
            .map(|o| !o.is_gt())
            .unwrap_or(false),
        WhereOp::Gt => value_cmp(lhs, &cond.value)
            .map(|o| o.is_gt())
            .unwrap_or(false),
        WhereOp::Ge => value_cmp(lhs, &cond.value)
            .map(|o| !o.is_lt())
            .unwrap_or(false),
        WhereOp::Like => like_match(lhs, &cond.value, cond.escape_char),
        WhereOp::NotLike => !like_match(lhs, &cond.value, cond.escape_char),
        WhereOp::Glob => glob_match(lhs, &cond.value),
        WhereOp::NotGlob => !glob_match(lhs, &cond.value),
        WhereOp::Regexp => regexp_match(lhs, &cond.value),
        WhereOp::NotRegexp => !regexp_match(lhs, &cond.value),
        WhereOp::In => cond.in_values.iter().any(|v| lhs == v),
        WhereOp::NotIn => !cond.in_values.iter().any(|v| lhs == v),
        WhereOp::Between | WhereOp::NotBetween => {
            let in_range = cond.value_high.as_ref().is_some_and(|high| {
                value_cmp(lhs, &cond.value)
                    .map(|o| !o.is_lt())
                    .unwrap_or(false)
                    && value_cmp(lhs, high).map(|o| !o.is_gt()).unwrap_or(false)
            });
            if cond.op == WhereOp::NotBetween {
                !in_range
            } else {
                in_range
            }
        }
        WhereOp::IsNull => matches!(lhs, Value::Null),
        WhereOp::IsNotNull => !matches!(lhs, Value::Null),
        WhereOp::StWithin => match (lhs, &cond.value, &cond.value_high) {
            (Value::GeoPoint(lat, lng), Value::GeoPoint(tlat, tlng), Some(Value::Float(r))) => {
                super::geo::haversine(*lat, *lng, *tlat, *tlng) <= *r
            }
            (Value::GeoPoint(lat, lng), Value::GeoPoint(tlat, tlng), Some(Value::Integer(r))) => {
                super::geo::haversine(*lat, *lng, *tlat, *tlng) <= *r as f64
            }
            _ => false,
        },
        // M153: EXISTS/NOT EXISTS — resolve 后 value=1 恒真，value=0 恒假
        WhereOp::Exists => matches!(&cond.value, Value::Integer(1)),
        WhereOp::NotExists => matches!(&cond.value, Value::Integer(1)),
    }
}

/// SQL LIKE 通配符匹配：% 匹配任意字符序列，_ 匹配单个字符。
/// 支持 ESCAPE 子句：转义字符后的 % / _ 作为字面匹配。
fn like_match(value: &Value, pattern: &Value, escape: Option<char>) -> bool {
    let (Value::Text(text), Value::Text(pat)) = (value, pattern) else {
        return false;
    };
    like_pattern_match(text.as_bytes(), pat.as_bytes(), escape.map(|c| c as u8))
}

/// GLOB 模式匹配（大小写敏感，`*` 任意字符串，`?` 单字符）。
fn glob_match(value: &Value, pattern: &Value) -> bool {
    let (Value::Text(text), Value::Text(pat)) = (value, pattern) else {
        return false;
    };
    glob_pattern_match(text.as_bytes(), pat.as_bytes())
}

/// REGEXP 正则匹配（`col REGEXP 'pattern'`）。
/// 非 Text 值返回 false，无效正则返回 false。
/// 使用 thread_local 缓存最近一次编译的正则，避免全表扫描时重复编译。
fn regexp_match(value: &Value, pattern: &Value) -> bool {
    use std::cell::RefCell;
    let (Value::Text(text), Value::Text(pat)) = (value, pattern) else {
        return false;
    };
    thread_local! {
        static CACHE: RefCell<Option<(String, regex::Regex)>> = const { RefCell::new(None) };
    }
    CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        // 命中缓存：pattern 相同则复用已编译的 Regex
        if let Some((ref cached_pat, ref re)) = *c {
            if cached_pat == pat {
                return re.is_match(text);
            }
        }
        // 编译新正则并缓存
        match regex::Regex::new(pat) {
            Ok(re) => {
                let result = re.is_match(text);
                *c = Some((pat.clone(), re));
                result
            }
            Err(_) => false,
        }
    })
}

/// GLOB 匹配算法（* 和 ?），大小写敏感。
/// 双指针迭代实现，避免递归栈溢出。
fn glob_pattern_match(text: &[u8], pattern: &[u8]) -> bool {
    let mut ti = 0;
    let mut pi = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;
    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// LIKE 匹配算法（% 和 _），大小写不敏感。
/// 支持 ESCAPE：转义字符后的 % / _ / 转义字符自身作为字面匹配。
fn like_pattern_match(text: &[u8], pattern: &[u8], escape: Option<u8>) -> bool {
    // 有 ESCAPE 时使用递归实现（模式通常很短，不会栈溢出）
    if escape.is_some() {
        return like_match_escape(text, 0, pattern, 0, escape.unwrap());
    }
    // 无 ESCAPE 时使用高性能双指针迭代
    let mut ti = 0;
    let mut pi = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;
    while ti < text.len() {
        if pi < pattern.len()
            && (pattern[pi] == b'_' || pattern[pi].eq_ignore_ascii_case(&text[ti]))
        {
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'%' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'%' {
        pi += 1;
    }
    pi == pattern.len()
}

/// LIKE + ESCAPE 递归匹配。
fn like_match_escape(text: &[u8], ti: usize, pat: &[u8], pi: usize, esc: u8) -> bool {
    // 模式消耗完毕
    if pi >= pat.len() {
        return ti >= text.len();
    }
    // 转义字符：下一个字符作为字面匹配
    if pat[pi] == esc && pi + 1 < pat.len() {
        if ti < text.len() && pat[pi + 1].eq_ignore_ascii_case(&text[ti]) {
            return like_match_escape(text, ti + 1, pat, pi + 2, esc);
        }
        return false;
    }
    if pat[pi] == b'%' {
        // % 匹配零个或多个字符
        // 跳过连续 %
        let mut np = pi;
        while np < pat.len() && pat[np] == b'%' {
            np += 1;
        }
        if np >= pat.len() {
            return true;
        }
        for start in ti..=text.len() {
            if like_match_escape(text, start, pat, np, esc) {
                return true;
            }
        }
        return false;
    }
    if pat[pi] == b'_' {
        // _ 匹配单个字符
        if ti < text.len() {
            return like_match_escape(text, ti + 1, pat, pi + 1, esc);
        }
        return false;
    }
    // 普通字符
    if ti < text.len() && pat[pi].eq_ignore_ascii_case(&text[ti]) {
        return like_match_escape(text, ti + 1, pat, pi + 1, esc);
    }
    false
}

/// DISTINCT 去重（M88：hash 替代 Debug 格式）。
pub(super) fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen = std::collections::HashSet::new();
    rows.retain(|row| {
        use std::hash::Hasher;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for v in row {
            hash_value(v, &mut h);
        }
        seen.insert(h.finish())
    });
}

/// 对单个 Value 做 hash（供 DISTINCT / DISTINCT ON 共用）。
pub(super) fn hash_value(v: &Value, h: &mut impl std::hash::Hasher) {
    use std::hash::Hash;
    std::mem::discriminant(v).hash(h);
    match v {
        Value::Null => {}
        Value::Integer(n) => n.hash(h),
        Value::Float(f) => f.to_bits().hash(h),
        Value::Text(s) => s.hash(h),
        Value::Blob(b) => b.hash(h),
        Value::Boolean(b) => b.hash(h),
        Value::Jsonb(j) => j.to_string().hash(h),
        Value::Vector(v) => {
            for f in v {
                f.to_bits().hash(h);
            }
        }
        Value::Timestamp(ts) => ts.hash(h),
        Value::GeoPoint(lat, lng) => {
            lat.to_bits().hash(h);
            lng.to_bits().hash(h);
        }
        Value::Date(d) => d.hash(h),
        Value::Time(t) => t.hash(h),
        Value::Placeholder(_) => unreachable!("Placeholder must be bound before execution"),
    }
}
// ── 列投影 ────────────────────────────────────────────────
/// 对结果行做列投影：按 SELECT 列名列表提取目标列，返回投影后的行。
/// `columns` 为 `["*"]` 时返回原始行不做投影。
/// 聚合列（含 COUNT/SUM/AVG/MIN/MAX）不做投影。
/// 支持算术表达式列：`a + b`、`price * quantity`、`score / 100`。
pub(super) fn project_columns(
    rows: Vec<Vec<Value>>,
    columns: &[String],
    schema: &Schema,
) -> Result<Vec<Vec<Value>>, crate::Error> {
    if columns.is_empty() || (columns.len() == 1 && columns[0] == "*") {
        return Ok(rows);
    }
    // 聚合函数不做投影（结果已经是聚合值）
    if columns.iter().any(|c| {
        let u = c.to_uppercase().replace(' ', "");
        u.contains("COUNT(")
            || u.contains("SUM(")
            || u.contains("AVG(")
            || u.contains("MIN(")
            || u.contains("MAX(")
    }) {
        return Ok(rows);
    }
    // 编译投影指令（委托 expr_eval 模块）
    use super::expr_eval::{compile_project_op, eval_project_op};
    let ops: Vec<_> = columns
        .iter()
        .map(|col| compile_project_op(col, schema))
        .collect::<Result<_, _>>()?;
    Ok(rows
        .into_iter()
        .map(|row| ops.iter().map(|op| eval_project_op(&row, op)).collect())
        .collect())
}

// ── 聚合函数 ──────────────────────────────────────────────

/// 解析聚合列：返回 (func, col_name) 列表。如 "SUM(x)" → (Sum, "x")。
/// 非聚合列返回 None。
pub(super) fn parse_agg_columns(columns: &[String]) -> Option<Vec<(AggType, String)>> {
    let mut aggs = Vec::new();
    for col in columns {
        let trimmed = col.trim();
        let u = trimmed.to_uppercase().replace(' ', "");
        if let Some(inner) = extract_agg_inner(&u, "COUNT(", trimmed) {
            aggs.push((AggType::Count, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "SUM(", trimmed) {
            aggs.push((AggType::Sum, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "AVG(", trimmed) {
            aggs.push((AggType::Avg, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "MIN(", trimmed) {
            aggs.push((AggType::Min, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "MAX(", trimmed) {
            aggs.push((AggType::Max, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "GROUP_CONCAT(", trimmed) {
            let (col_name, sep) = parse_group_concat_args(&inner);
            aggs.push((AggType::GroupConcat(sep), col_name));
        } else if let Some(inner) = extract_agg_inner(&u, "STRING_AGG(", trimmed) {
            let (col_name, sep) = parse_group_concat_args(&inner);
            aggs.push((AggType::GroupConcat(sep), col_name));
        } else if let Some(inner) = extract_agg_inner(&u, "STDDEV(", trimmed) {
            aggs.push((AggType::Stddev, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "VARIANCE(", trimmed) {
            aggs.push((AggType::Variance, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "JSON_ARRAYAGG(", trimmed) {
            aggs.push((AggType::JsonArrayAgg, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "JSON_OBJECTAGG(", trimmed) {
            let (key_col, val_col) = parse_json_objectagg_args(&inner);
            aggs.push((AggType::JsonObjectAgg(val_col), key_col));
        } else if let Some(inner) = extract_agg_inner(&u, "BOOL_AND(", trimmed) {
            aggs.push((AggType::BoolAnd, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "BOOL_OR(", trimmed) {
            aggs.push((AggType::BoolOr, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "ARRAY_AGG(", trimmed) {
            aggs.push((AggType::ArrayAgg, inner));
        } else if let Some(inner) = extract_agg_inner(&u, "PERCENTILE_CONT(", trimmed) {
            let (frac, col) = parse_percentile_args(&inner);
            aggs.push((AggType::PercentileCont(frac), col));
        } else if let Some(inner) = extract_agg_inner(&u, "PERCENTILE_DISC(", trimmed) {
            let (frac, col) = parse_percentile_args(&inner);
            aggs.push((AggType::PercentileDisc(frac), col));
        } else {
            return None; // 混合聚合和普通列暂不支持
        }
    }
    if aggs.is_empty() {
        None
    } else {
        Some(aggs)
    }
}

/// 解析 GROUP_CONCAT 参数：`col` 或 `col,'sep'`。
fn parse_group_concat_args(inner: &str) -> (String, String) {
    if let Some(comma_pos) = inner.find(',') {
        let col = inner[..comma_pos].trim().to_string();
        let sep_raw = inner[comma_pos + 1..].trim();
        // 去除引号
        let sep = if (sep_raw.starts_with('\'') && sep_raw.ends_with('\''))
            || (sep_raw.starts_with('"') && sep_raw.ends_with('"'))
        {
            sep_raw[1..sep_raw.len() - 1].to_string()
        } else {
            sep_raw.to_string()
        };
        (col, sep)
    } else {
        (inner.to_string(), ",".to_string())
    }
}

/// 解析 JSON_OBJECTAGG 参数：`key_col, val_col`。
fn parse_json_objectagg_args(inner: &str) -> (String, String) {
    if let Some(comma_pos) = inner.find(',') {
        let key_col = inner[..comma_pos].trim().to_string();
        let val_col = inner[comma_pos + 1..].trim().to_string();
        (key_col, val_col)
    } else {
        // 缺少 val_col 时，默认用 key_col 自身
        (inner.to_string(), inner.to_string())
    }
}

/// 解析 PERCENTILE_CONT/DISC 参数：`fraction, col` → (fraction, col_name)。
fn parse_percentile_args(inner: &str) -> (f64, String) {
    if let Some(comma_pos) = inner.find(',') {
        let frac_str = inner[..comma_pos].trim();
        let col = inner[comma_pos + 1..].trim().to_string();
        let frac = frac_str.parse::<f64>().unwrap_or(0.5);
        (frac.clamp(0.0, 1.0), col)
    } else {
        // 单参数时默认 0.5（中位数）
        (0.5, inner.to_string())
    }
}

/// 解析单个聚合函数列名：如 "COUNT(*)" → Some(("COUNT", "*"))，非聚合返回 None。
pub(super) fn parse_agg_func(col: &str) -> Option<(String, String)> {
    let u = col.trim().to_uppercase().replace(' ', "");
    let orig = col.trim().replace(' ', "");
    for prefix in &[
        "COUNT(",
        "SUM(",
        "AVG(",
        "MIN(",
        "MAX(",
        "GROUP_CONCAT(",
        "STRING_AGG(",
        "STDDEV(",
        "VARIANCE(",
        "JSON_ARRAYAGG(",
        "JSON_OBJECTAGG(",
        "BOOL_AND(",
        "BOOL_OR(",
        "ARRAY_AGG(",
        "PERCENTILE_CONT(",
        "PERCENTILE_DISC(",
    ] {
        if u.starts_with(prefix) && u.ends_with(')') {
            let func = prefix[..prefix.len() - 1].to_string();
            // STRING_AGG 映射为 GROUP_CONCAT
            let func = if func == "STRING_AGG" {
                "GROUP_CONCAT".to_string()
            } else {
                func
            };
            let inner = &orig[prefix.len()..orig.len() - 1];
            return Some((func, inner.to_string()));
        }
    }
    None
}

/// 从 "SUM(col)" 提取原始大小写的 "col"。
/// `upper` 是大写版本用于匹配前缀，`original` 是原始字符串用于提取列名。
/// 列名转小写以匹配 schema（column_index_by_name 大小写不敏感）。
/// 注意：引号内的空格必须保留（如分隔符 `', '`），仅去除引号外的空格。
fn extract_agg_inner(upper: &str, prefix: &str, original: &str) -> Option<String> {
    if upper.starts_with(prefix) && upper.ends_with(')') {
        // 从原始字符串中提取括号内的内容
        // 仅去除引号外的空格，保留引号内的空格（分隔符可能含空格）
        let orig_trimmed = original.trim();
        let raw_inner = &orig_trimmed[prefix.len()..orig_trimmed.len() - 1];
        let inner = strip_spaces_outside_quotes(raw_inner);
        Some(inner.to_ascii_lowercase())
    } else {
        None
    }
}

/// 去除引号外的空格，保留单引号/双引号内的空格。
pub(super) fn strip_spaces_outside_quotes(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut in_quote: Option<char> = None;
    for ch in s.chars() {
        match in_quote {
            Some(q) => {
                result.push(ch);
                if ch == q {
                    in_quote = None;
                }
            }
            None => {
                if ch == '\'' || ch == '"' {
                    in_quote = Some(ch);
                    result.push(ch);
                } else if ch != ' ' {
                    result.push(ch);
                }
            }
        }
    }
    result
}

/// 聚合类型。
#[derive(Debug, Clone)]
pub(super) enum AggType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// GROUP_CONCAT(col [, separator])，默认分隔符为逗号。
    GroupConcat(String),
    /// 总体标准差（Welford 在线算法）。
    Stddev,
    /// 总体方差（Welford 在线算法）。
    Variance,
    /// JSON_ARRAYAGG(col) — 将列值聚合为 JSON 数组。
    JsonArrayAgg,
    /// JSON_OBJECTAGG(key_col, val_col) — 将键值对聚合为 JSON 对象。
    /// 存储 val_col 名称。
    JsonObjectAgg(String),
    /// BOOL_AND(col) — 逻辑与聚合，全 true 返回 true。
    BoolAnd,
    /// BOOL_OR(col) — 逻辑或聚合，任一 true 返回 true。
    BoolOr,
    /// ARRAY_AGG(col) — 将列值聚合为 JSON 数组（跳过 NULL，PostgreSQL 兼容）。
    ArrayAgg,
    /// PERCENTILE_CONT(fraction, col) — 连续百分位（线性插值）。
    PercentileCont(f64),
    /// PERCENTILE_DISC(fraction, col) — 离散百分位（取最近值）。
    PercentileDisc(f64),
}

/// 对行集执行聚合计算，返回单行结果。
pub(super) fn compute_aggregates(
    rows: &[Vec<Value>],
    aggs: &[(AggType, String)],
    schema: &Schema,
) -> Result<Vec<Value>, crate::Error> {
    let mut result = Vec::with_capacity(aggs.len());
    for (agg_type, col_name) in aggs {
        if col_name == "*" {
            // COUNT(*) 特殊处理
            result.push(Value::Integer(rows.len() as i64));
            continue;
        }
        let col_idx = schema
            .column_index_by_name(col_name)
            .ok_or_else(|| crate::Error::SqlExec(format!("聚合列不存在: {}", col_name)))?;
        match agg_type {
            AggType::Count => {
                let count = rows
                    .iter()
                    .filter(|r| !matches!(r[col_idx], Value::Null))
                    .count();
                result.push(Value::Integer(count as i64));
            }
            AggType::Sum => {
                let mut sum = 0.0f64;
                let mut is_int = true;
                for row in rows {
                    match &row[col_idx] {
                        Value::Integer(n) => sum += *n as f64,
                        Value::Float(n) => {
                            sum += n;
                            is_int = false;
                        }
                        Value::Null => {}
                        _ => {}
                    }
                }
                if is_int {
                    result.push(Value::Integer(sum as i64));
                } else {
                    result.push(Value::Float(sum));
                }
            }
            AggType::Avg => {
                let mut sum = 0.0f64;
                let mut count = 0u64;
                for row in rows {
                    match &row[col_idx] {
                        Value::Integer(n) => {
                            sum += *n as f64;
                            count += 1;
                        }
                        Value::Float(n) => {
                            sum += n;
                            count += 1;
                        }
                        _ => {}
                    }
                }
                if count > 0 {
                    result.push(Value::Float(sum / count as f64));
                } else {
                    result.push(Value::Null);
                }
            }
            AggType::Min => {
                let mut min_val: Option<Value> = None;
                for row in rows {
                    let v = &row[col_idx];
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    min_val = Some(match min_val {
                        None => v.clone(),
                        Some(ref cur) => {
                            if value_cmp(v, cur).map(|o| o.is_lt()).unwrap_or(false) {
                                v.clone()
                            } else {
                                cur.clone()
                            }
                        }
                    });
                }
                result.push(min_val.unwrap_or(Value::Null));
            }
            AggType::Max => {
                let mut max_val: Option<Value> = None;
                for row in rows {
                    let v = &row[col_idx];
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    max_val = Some(match max_val {
                        None => v.clone(),
                        Some(ref cur) => {
                            if value_cmp(v, cur).map(|o| o.is_gt()).unwrap_or(false) {
                                v.clone()
                            } else {
                                cur.clone()
                            }
                        }
                    });
                }
                result.push(max_val.unwrap_or(Value::Null));
            }
            AggType::GroupConcat(ref sep) => {
                let mut parts: Vec<String> = Vec::new();
                for row in rows {
                    let v = &row[col_idx];
                    if matches!(v, Value::Null) {
                        continue;
                    }
                    parts.push(match v {
                        Value::Text(s) => s.clone(),
                        Value::Integer(n) => n.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        _ => continue,
                    });
                }
                if parts.is_empty() {
                    result.push(Value::Null);
                } else {
                    result.push(Value::Text(parts.join(sep)));
                }
            }
            AggType::Stddev | AggType::Variance => {
                // Welford 在线算法
                let mut count = 0u64;
                let mut mean = 0.0f64;
                let mut m2 = 0.0f64;
                for row in rows {
                    match &row[col_idx] {
                        Value::Integer(n) => {
                            count += 1;
                            let delta = *n as f64 - mean;
                            mean += delta / count as f64;
                            let delta2 = *n as f64 - mean;
                            m2 += delta * delta2;
                        }
                        Value::Float(n) => {
                            count += 1;
                            let delta = n - mean;
                            mean += delta / count as f64;
                            let delta2 = n - mean;
                            m2 += delta * delta2;
                        }
                        _ => {}
                    }
                }
                if count == 0 {
                    result.push(Value::Null);
                } else {
                    let variance = m2 / count as f64;
                    result.push(Value::Float(if matches!(agg_type, AggType::Stddev) {
                        variance.sqrt()
                    } else {
                        variance
                    }));
                }
            }
            AggType::JsonArrayAgg => {
                let mut arr: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
                for row in rows {
                    arr.push(value_to_json(&row[col_idx]));
                }
                result.push(Value::Text(
                    serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into()),
                ));
            }
            AggType::JsonObjectAgg(ref val_col) => {
                let val_idx = schema.column_index_by_name(val_col).ok_or_else(|| {
                    crate::Error::SqlExec(format!("JSON_OBJECTAGG 值列不存在: {}", val_col))
                })?;
                let mut map = serde_json::Map::new();
                for row in rows {
                    let key = match &row[col_idx] {
                        Value::Text(s) => s.clone(),
                        Value::Integer(n) => n.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        Value::Null => continue,
                        other => format!("{:?}", other),
                    };
                    map.insert(key, value_to_json(&row[val_idx]));
                }
                result.push(Value::Text(
                    serde_json::to_string(&serde_json::Value::Object(map))
                        .unwrap_or_else(|_| "{}".into()),
                ));
            }
            AggType::BoolAnd => {
                let mut acc: Option<bool> = None;
                for row in rows {
                    match &row[col_idx] {
                        Value::Boolean(b) => acc = Some(acc.unwrap_or(true) && *b),
                        Value::Integer(n) => acc = Some(acc.unwrap_or(true) && (*n != 0)),
                        Value::Null => {}
                        _ => {}
                    }
                }
                result.push(match acc {
                    Some(v) => Value::Boolean(v),
                    None => Value::Null,
                });
            }
            AggType::BoolOr => {
                let mut acc: Option<bool> = None;
                for row in rows {
                    match &row[col_idx] {
                        Value::Boolean(b) => acc = Some(acc.unwrap_or(false) || *b),
                        Value::Integer(n) => acc = Some(acc.unwrap_or(false) || (*n != 0)),
                        Value::Null => {}
                        _ => {}
                    }
                }
                result.push(match acc {
                    Some(v) => Value::Boolean(v),
                    None => Value::Null,
                });
            }
            AggType::ArrayAgg => {
                let mut arr: Vec<serde_json::Value> = Vec::new();
                for row in rows {
                    if matches!(row[col_idx], Value::Null) {
                        continue;
                    }
                    arr.push(value_to_json(&row[col_idx]));
                }
                if arr.is_empty() {
                    result.push(Value::Null);
                } else {
                    result.push(Value::Text(
                        serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into()),
                    ));
                }
            }
            AggType::PercentileCont(frac) | AggType::PercentileDisc(frac) => {
                let mut vals: Vec<f64> = Vec::new();
                for row in rows {
                    match &row[col_idx] {
                        Value::Integer(n) => vals.push(*n as f64),
                        Value::Float(f) => vals.push(*f),
                        _ => {}
                    }
                }
                if vals.is_empty() {
                    result.push(Value::Null);
                } else {
                    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let n = vals.len();
                    if matches!(agg_type, AggType::PercentileCont(_)) {
                        // 连续百分位：线性插值
                        let idx = frac * (n - 1) as f64;
                        let lo = idx.floor() as usize;
                        let hi = idx.ceil() as usize;
                        let v = if lo == hi {
                            vals[lo]
                        } else {
                            vals[lo] + (vals[hi] - vals[lo]) * (idx - lo as f64)
                        };
                        result.push(Value::Float(v));
                    } else {
                        // 离散百分位：取 ceil 位置
                        let idx = (frac * n as f64).ceil() as usize;
                        let idx = idx.clamp(1, n) - 1;
                        result.push(Value::Float(vals[idx]));
                    }
                }
            }
        }
    }
    Ok(result)
}

/// Value → serde_json::Value 转换（聚合 JSON 序列化用）。
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(n) => serde_json::json!(*n),
        Value::Float(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::json!(s),
        Value::Boolean(b) => serde_json::json!(*b),
        other => serde_json::json!(format!("{:?}", other)),
    }
}

// ── 快速路径辅助函数 ───────────────────────────────────────

/// M123-F：首字符快速分派 + 避免无效 replace 的快速值解析。
pub(super) fn fast_parse_value(s: &str) -> Option<Value> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let first = s.as_bytes()[0];
    match first {
        b'\'' => {
            let inner = s.strip_prefix('\'').and_then(|t| t.strip_suffix('\''))?;
            return Some(Value::Text(if inner.contains("''") {
                inner.replace("''", "'")
            } else {
                inner.to_string()
            }));
        }
        b'0'..=b'9' | b'-' => {
            if let Ok(n) = s.parse::<i64>() {
                return Some(Value::Integer(n));
            }
            if let Ok(n) = s.parse::<f64>() {
                return Some(Value::Float(n));
            }
        }
        _ => {}
    }
    if s.eq_ignore_ascii_case("NULL") {
        return Some(Value::Null);
    }
    if s.eq_ignore_ascii_case("TRUE") {
        return Some(Value::Boolean(true));
    }
    if s.eq_ignore_ascii_case("FALSE") {
        return Some(Value::Boolean(false));
    }
    if s.eq_ignore_ascii_case("NOW()") {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        return Some(Value::Timestamp(ts));
    }
    None
}

/// M123-F：单 pass 快速行解析 — 合并 fast_split_values + fast_parse_value。
pub(super) fn fast_parse_row(s: &str) -> Option<Vec<Value>> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut row = Vec::with_capacity((len / 8).min(128) + 1);
    let mut start = 0;
    let mut in_quote = false;
    let mut i = 0;
    while i < len {
        if bytes[i] == b'\'' {
            if in_quote && i + 1 < len && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_quote = !in_quote;
        } else if bytes[i] == b',' && !in_quote {
            row.push(fast_parse_value(&s[start..i])?);
            start = i + 1;
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        row.push(fast_parse_value(last)?);
    }
    Some(row)
}

/// 找到从位置 0 开始的 '(' 对应的 ')' 位置。
pub(super) fn find_close_paren(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    let mut in_q = false;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' {
            if in_q && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                continue;
            }
            in_q = !in_q;
        } else if !in_q {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
    }
    None
}
/// M118：校验一行数据是否满足所有 CHECK 约束。
/// `checks` 为预解析的 WhereExpr 列表，与 `schema.check_constraints` 一一对应。
/// 任一 CHECK 不满足则返回 `Error::SqlExec`。
pub(super) fn validate_check_constraints(
    row: &[Value],
    schema: &Schema,
    checks: &[WhereExpr],
    check_sqls: &[String],
) -> Result<(), Error> {
    for (i, expr) in checks.iter().enumerate() {
        if !row_matches(row, schema, expr)? {
            let desc = check_sqls.get(i).map(|s| s.as_str()).unwrap_or("?");
            return Err(Error::SqlExec(format!("CHECK 约束失败: {}", desc)));
        }
    }
    Ok(())
}
