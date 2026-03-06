/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL JSONB 操作函数：json_extract / json_set / json_remove / json_type /
//! json_array_length / json_keys / json_valid / json_contains。
//!
//! 路径语法（简化 JSONPath）：`$.key`、`$.key.subkey`、`$.arr[0]`。
//! 对标 SQLite json_extract / PG jsonb_extract_path_text / MySQL JSON_EXTRACT。

use crate::types::Value;

/// `json_extract(jsonb_col, path)` — 提取 JSON 路径值，返回对应 Talon Value。
///
/// 路径格式：`$.key`、`$.key.subkey`、`$.arr[0]`。
/// 路径不存在时返回 NULL。
pub(super) fn func_json_extract(args: &[Value]) -> Value {
    let (json_val, path) = match parse_json_and_path(args) {
        Some(v) => v,
        None => return Value::Null,
    };
    match navigate_path(&json_val, &path) {
        Some(v) => serde_to_talon(v),
        None => Value::Null,
    }
}

/// `json_extract_text(jsonb_col, path)` — 提取为文本（`->>` 语义）。
pub(super) fn func_json_extract_text(args: &[Value]) -> Value {
    let (json_val, path) = match parse_json_and_path(args) {
        Some(v) => v,
        None => return Value::Null,
    };
    match navigate_path(&json_val, &path) {
        Some(serde_json::Value::String(s)) => Value::Text(s.clone()),
        Some(serde_json::Value::Null) => Value::Null,
        Some(v) => Value::Text(v.to_string()),
        None => Value::Null,
    }
}

/// `json_set(jsonb_col, path, new_value)` — 设置/更新 JSON 路径值，返回新 JSONB。
pub(super) fn func_json_set(args: &[Value]) -> Value {
    if args.len() < 3 {
        return Value::Null;
    }
    let mut json_val = match to_serde_json(&args[0]) {
        Some(v) => v,
        None => return Value::Null,
    };
    let path = match to_text(&args[1]) {
        Some(p) => p,
        None => return Value::Null,
    };
    let segments = parse_path_segments(&path);
    if segments.is_empty() {
        return Value::Null;
    }
    let new_val = talon_to_serde(&args[2]);
    set_path(&mut json_val, &segments, new_val);
    Value::Jsonb(json_val)
}

/// `json_remove(jsonb_col, path)` — 删除 JSON 路径，返回新 JSONB。
pub(super) fn func_json_remove(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }
    let mut json_val = match to_serde_json(&args[0]) {
        Some(v) => v,
        None => return Value::Null,
    };
    let path = match to_text(&args[1]) {
        Some(p) => p,
        None => return Value::Null,
    };
    let segments = parse_path_segments(&path);
    if segments.is_empty() {
        return Value::Null;
    }
    remove_path(&mut json_val, &segments);
    Value::Jsonb(json_val)
}

/// `json_type(jsonb_col)` — 返回 JSON 值类型字符串。
///
/// 返回值：`"object"` / `"array"` / `"string"` / `"number"` / `"boolean"` / `"null"`。
pub(super) fn func_json_type(args: &[Value]) -> Value {
    let json_val = match args.first().and_then(to_serde_json) {
        Some(v) => v,
        None => return Value::Null,
    };
    let type_str = match &json_val {
        serde_json::Value::Object(_) => "object",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Null => "null",
    };
    Value::Text(type_str.into())
}

/// `json_array_length(jsonb_col [, path])` — 返回 JSON 数组长度。
///
/// 非数组返回 NULL。可选 path 参数指定嵌套数组。
pub(super) fn func_json_array_length(args: &[Value]) -> Value {
    let json_val = match args.first().and_then(to_serde_json) {
        Some(v) => v,
        None => return Value::Null,
    };
    let target: &serde_json::Value = if args.len() >= 2 {
        let path = match to_text(&args[1]) {
            Some(p) => p,
            None => return Value::Null,
        };
        let segments = parse_path_segments(&path);
        match navigate_path(&json_val, &segments) {
            Some(v) => v,
            None => return Value::Null,
        }
    } else {
        &json_val
    };
    match target {
        serde_json::Value::Array(arr) => Value::Integer(arr.len() as i64),
        _ => Value::Null,
    }
}

/// `json_keys(jsonb_col)` — 返回对象所有 key（逗号分隔文本）。
///
/// 非对象返回 NULL。
pub(super) fn func_json_keys(args: &[Value]) -> Value {
    let json_val = match args.first().and_then(to_serde_json) {
        Some(v) => v,
        None => return Value::Null,
    };
    match json_val {
        serde_json::Value::Object(map) => {
            let keys: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
            Value::Text(keys.join(","))
        }
        _ => Value::Null,
    }
}

/// `json_valid(text_col)` — 检查文本是否为有效 JSON。
pub(super) fn func_json_valid(args: &[Value]) -> Value {
    let text = match args.first() {
        Some(Value::Text(s)) => s.as_str(),
        Some(Value::Jsonb(_)) => return Value::Boolean(true),
        _ => return Value::Boolean(false),
    };
    Value::Boolean(serde_json::from_str::<serde_json::Value>(text).is_ok())
}

/// `json_contains(jsonb_col, search_value)` — 检查 JSON 是否包含指定值。
///
/// 对象：检查是否包含指定 key。
/// 数组：检查是否包含指定元素。
pub(super) fn func_json_contains(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }
    let json_val = match to_serde_json(&args[0]) {
        Some(v) => v,
        None => return Value::Null,
    };
    let search = match to_text(&args[1]) {
        Some(s) => s,
        None => return Value::Null,
    };
    match &json_val {
        serde_json::Value::Object(map) => Value::Boolean(map.contains_key(&search)),
        serde_json::Value::Array(arr) => {
            // 尝试解析 search 为 JSON 值进行比较
            let search_val = serde_json::from_str::<serde_json::Value>(&search)
                .unwrap_or(serde_json::Value::String(search));
            Value::Boolean(arr.contains(&search_val))
        }
        _ => Value::Boolean(false),
    }
}

// ── 内部辅助函数 ──────────────────────────────────────────

/// 路径段：key 名或数组索引。
#[derive(Debug)]
enum PathSegment {
    Key(String),
    Index(usize),
}

/// 解析 JSON 路径字符串为段列表。
///
/// 支持格式：`$.key.subkey`、`$.arr[0]`、`$.arr[0].name`。
/// `$` 前缀可选。
fn parse_path_segments(path: &str) -> Vec<PathSegment> {
    let path = path.strip_prefix('$').unwrap_or(path);
    let path = path.strip_prefix('.').unwrap_or(path);
    if path.is_empty() {
        return Vec::new();
    }
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Key(std::mem::take(&mut current)));
                }
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Key(std::mem::take(&mut current)));
                }
                let mut idx_str = String::new();
                for c in chars.by_ref() {
                    if c == ']' {
                        break;
                    }
                    idx_str.push(c);
                }
                if let Ok(idx) = idx_str.parse::<usize>() {
                    segments.push(PathSegment::Index(idx));
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.is_empty() {
        segments.push(PathSegment::Key(current));
    }
    segments
}

/// 沿路径导航 JSON 值，返回目标节点的引用。
fn navigate_path<'a>(
    val: &'a serde_json::Value,
    segments: &[PathSegment],
) -> Option<&'a serde_json::Value> {
    let mut current = val;
    for seg in segments {
        match seg {
            PathSegment::Key(key) => {
                current = current.get(key.as_str())?;
            }
            PathSegment::Index(idx) => {
                current = current.get(*idx)?;
            }
        }
    }
    Some(current)
}

/// 在 JSON 值中设置路径对应的值（原地修改）。
fn set_path(val: &mut serde_json::Value, segments: &[PathSegment], new_val: serde_json::Value) {
    if segments.is_empty() {
        return;
    }
    if segments.len() == 1 {
        match &segments[0] {
            PathSegment::Key(key) => {
                if let serde_json::Value::Object(map) = val {
                    map.insert(key.clone(), new_val);
                }
            }
            PathSegment::Index(idx) => {
                if let serde_json::Value::Array(arr) = val {
                    if *idx < arr.len() {
                        arr[*idx] = new_val;
                    }
                }
            }
        }
        return;
    }
    let child = match &segments[0] {
        PathSegment::Key(key) => {
            if let serde_json::Value::Object(map) = val {
                map.entry(key.clone())
                    .or_insert_with(|| serde_json::Value::Object(Default::default()))
            } else {
                return;
            }
        }
        PathSegment::Index(idx) => {
            if let serde_json::Value::Array(arr) = val {
                if *idx < arr.len() {
                    &mut arr[*idx]
                } else {
                    return;
                }
            } else {
                return;
            }
        }
    };
    set_path(child, &segments[1..], new_val);
}

/// 从 JSON 值中删除路径对应的节点（原地修改）。
fn remove_path(val: &mut serde_json::Value, segments: &[PathSegment]) {
    if segments.is_empty() {
        return;
    }
    if segments.len() == 1 {
        match &segments[0] {
            PathSegment::Key(key) => {
                if let serde_json::Value::Object(map) = val {
                    map.remove(key.as_str());
                }
            }
            PathSegment::Index(idx) => {
                if let serde_json::Value::Array(arr) = val {
                    if *idx < arr.len() {
                        arr.remove(*idx);
                    }
                }
            }
        }
        return;
    }
    let child = match &segments[0] {
        PathSegment::Key(key) => val.get_mut(key.as_str()),
        PathSegment::Index(idx) => val.get_mut(*idx),
    };
    if let Some(child) = child {
        remove_path(child, &segments[1..]);
    }
}

/// 解析前两个参数为 (serde_json::Value, path_segments)。
fn parse_json_and_path(args: &[Value]) -> Option<(serde_json::Value, Vec<PathSegment>)> {
    if args.len() < 2 {
        return None;
    }
    let json_val = to_serde_json(&args[0])?;
    let path = to_text(&args[1])?;
    let segments = parse_path_segments(&path);
    Some((json_val, segments))
}

/// 将 Talon Value 转为 serde_json::Value。
fn to_serde_json(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Jsonb(j) => Some(j.clone()),
        Value::Text(s) => serde_json::from_str(s).ok(),
        Value::Null => None,
        _ => None,
    }
}

/// 将 Talon Value 提取为 String。
fn to_text(v: &Value) -> Option<String> {
    match v {
        Value::Text(s) => Some(s.clone()),
        Value::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// 将 serde_json::Value 转为 Talon Value。
fn serde_to_talon(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Jsonb(v.clone()),
    }
}

/// 将 Talon Value 转为 serde_json::Value（用于 json_set 的新值参数）。
fn talon_to_serde(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Jsonb(j) => j.clone(),
        _ => serde_json::Value::Null,
    }
}
