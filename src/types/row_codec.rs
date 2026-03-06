/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M81：二进制行编码器 — 替代 serde_json，聚合/扫描性能提升 5-20x。
//!
//! ## 格式
//! ```text
//! [u16 LE schema_version][value₀][value₁]...[valueₙ]
//! ```
//! 每个 value 编码：
//! ```text
//! [u8 type_tag][payload]
//!   0x00 Null       — 无 payload
//!   0x01 Integer    — 8 字节 i64 LE
//!   0x02 Float      — 8 字节 f64 LE
//!   0x03 Text       — u32 LE 长度 + UTF-8 字节
//!   0x04 Blob       — u32 LE 长度 + 原始字节
//!   0x05 Boolean    — 1 字节 (0/1)
//!   0x06 Jsonb      — u32 LE 长度 + JSON 字节
//!   0x07 Vector     — u32 LE 元素数 + f32 LE × N
//!   0x08 Timestamp  — 8 字节 i64 LE
//! ```
//!
//! ## 向后兼容
//! `decode_row` 检测 `raw[2]`：若为 `0x5B` (`[`) 则走旧 JSON 路径，否则走二进制路径。

use super::Value;
use crate::Error;

// ── 类型标签常量 ──────────────────────────────────────────
const TAG_NULL: u8 = 0x00;
const TAG_INTEGER: u8 = 0x01;
const TAG_FLOAT: u8 = 0x02;
const TAG_TEXT: u8 = 0x03;
const TAG_BLOB: u8 = 0x04;
const TAG_BOOLEAN: u8 = 0x05;
const TAG_JSONB: u8 = 0x06;
const TAG_VECTOR: u8 = 0x07;
const TAG_TIMESTAMP: u8 = 0x08;
const TAG_GEOPOINT: u8 = 0x09;
const TAG_DATE: u8 = 0x0A;
const TAG_TIME: u8 = 0x0B;

/// JSON 数组起始字节 `[`，用于向后兼容检测。
const JSON_ARRAY_START: u8 = 0x5B;

// ── 编码 ──────────────────────────────────────────────────

/// 将单个 Value 追加到 buf。
pub(crate) fn encode_value(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(TAG_NULL),
        Value::Integer(n) => {
            buf.push(TAG_INTEGER);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(TAG_TEXT);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Blob(b) => {
            buf.push(TAG_BLOB);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Boolean(b) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Jsonb(j) => {
            buf.push(TAG_JSONB);
            let bytes = serde_json::to_vec(j).unwrap_or_default();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&bytes);
        }
        Value::Vector(v) => {
            buf.push(TAG_VECTOR);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Timestamp(ts) => {
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&ts.to_le_bytes());
        }
        Value::GeoPoint(lat, lng) => {
            buf.push(TAG_GEOPOINT);
            buf.extend_from_slice(&lat.to_le_bytes());
            buf.extend_from_slice(&lng.to_le_bytes());
        }
        Value::Date(d) => {
            buf.push(TAG_DATE);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Time(t) => {
            buf.push(TAG_TIME);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Placeholder(_) => unreachable!("Placeholder must be bound before encoding"),
    }
}

/// 二进制编码行（不含 version 头 — 由 Schema::encode_row 负责添加）。
/// M102：精确预估容量，消除宽表 realloc（71列表从 710→实际 ~1000 字节）。
pub fn encode_row_binary(row: &[Value]) -> Vec<u8> {
    let cap: usize = row.iter().map(estimate_value_size).sum();
    let mut buf = Vec::with_capacity(cap);
    for val in row {
        encode_value(&mut buf, val);
    }
    buf
}

/// 精确估算单个 Value 编码后的字节数。
#[inline]
fn estimate_value_size(val: &Value) -> usize {
    match val {
        Value::Null => 1,
        Value::Integer(_) | Value::Float(_) | Value::Timestamp(_) | Value::Time(_) => 1 + 8,
        Value::Boolean(_) => 1 + 1,
        Value::Date(_) => 1 + 4,
        Value::GeoPoint(_, _) => 1 + 16,
        Value::Text(s) => 1 + 4 + s.len(),
        Value::Blob(b) => 1 + 4 + b.len(),
        Value::Jsonb(_) => 1 + 4 + 64, // JSON 大小不确定，给保守估计
        Value::Vector(v) => 1 + 4 + v.len() * 4,
        Value::Placeholder(_) => unreachable!("Placeholder must be bound before encoding"),
    }
}

// ── 解码 ──────────────────────────────────────────────────

/// 从字节切片中解码单个 Value，返回 (Value, 消耗字节数)。
pub(crate) fn decode_value(data: &[u8]) -> Result<(Value, usize), Error> {
    if data.is_empty() {
        return Err(Error::Serialization("行数据截断：缺少 type tag".into()));
    }
    let tag = data[0];
    let rest = &data[1..];
    match tag {
        TAG_NULL => Ok((Value::Null, 1)),
        TAG_INTEGER => {
            if rest.len() < 8 {
                return Err(Error::Serialization("Integer 数据不足".into()));
            }
            let n = i64::from_le_bytes(rest[..8].try_into().unwrap());
            Ok((Value::Integer(n), 1 + 8))
        }
        TAG_FLOAT => {
            if rest.len() < 8 {
                return Err(Error::Serialization("Float 数据不足".into()));
            }
            let f = f64::from_le_bytes(rest[..8].try_into().unwrap());
            Ok((Value::Float(f), 1 + 8))
        }
        TAG_TEXT => {
            if rest.len() < 4 {
                return Err(Error::Serialization("Text 长度不足".into()));
            }
            let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if rest.len() < 4 + len {
                return Err(Error::Serialization("Text 数据不足".into()));
            }
            let s = std::str::from_utf8(&rest[4..4 + len])
                .map_err(|e| Error::Serialization(e.to_string()))?;
            Ok((Value::Text(s.to_string()), 1 + 4 + len))
        }
        TAG_BLOB => {
            if rest.len() < 4 {
                return Err(Error::Serialization("Blob 长度不足".into()));
            }
            let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if rest.len() < 4 + len {
                return Err(Error::Serialization("Blob 数据不足".into()));
            }
            Ok((Value::Blob(rest[4..4 + len].to_vec()), 1 + 4 + len))
        }
        TAG_BOOLEAN => {
            if rest.is_empty() {
                return Err(Error::Serialization("Boolean 数据不足".into()));
            }
            Ok((Value::Boolean(rest[0] != 0), 1 + 1))
        }
        TAG_JSONB => {
            if rest.len() < 4 {
                return Err(Error::Serialization("Jsonb 长度不足".into()));
            }
            let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if rest.len() < 4 + len {
                return Err(Error::Serialization("Jsonb 数据不足".into()));
            }
            let j: serde_json::Value = serde_json::from_slice(&rest[4..4 + len])
                .map_err(|e| Error::Serialization(e.to_string()))?;
            Ok((Value::Jsonb(j), 1 + 4 + len))
        }
        TAG_VECTOR => {
            if rest.len() < 4 {
                return Err(Error::Serialization("Vector 元素数不足".into()));
            }
            let count = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            let bytes_needed = count * 4;
            if rest.len() < 4 + bytes_needed {
                return Err(Error::Serialization("Vector 数据不足".into()));
            }
            let mut v = Vec::with_capacity(count);
            for i in 0..count {
                let off = 4 + i * 4;
                let f = f32::from_le_bytes(rest[off..off + 4].try_into().unwrap());
                v.push(f);
            }
            Ok((Value::Vector(v), 1 + 4 + bytes_needed))
        }
        TAG_TIMESTAMP => {
            if rest.len() < 8 {
                return Err(Error::Serialization("Timestamp 数据不足".into()));
            }
            let ts = i64::from_le_bytes(rest[..8].try_into().unwrap());
            Ok((Value::Timestamp(ts), 1 + 8))
        }
        TAG_GEOPOINT => {
            if rest.len() < 16 {
                return Err(Error::Serialization("GeoPoint 数据不足".into()));
            }
            let lat = f64::from_le_bytes(rest[..8].try_into().unwrap());
            let lng = f64::from_le_bytes(rest[8..16].try_into().unwrap());
            Ok((Value::GeoPoint(lat, lng), 1 + 16))
        }
        TAG_DATE => {
            if rest.len() < 4 {
                return Err(Error::Serialization("Date 数据不足".into()));
            }
            let d = i32::from_le_bytes(rest[..4].try_into().unwrap());
            Ok((Value::Date(d), 1 + 4))
        }
        TAG_TIME => {
            if rest.len() < 8 {
                return Err(Error::Serialization("Time 数据不足".into()));
            }
            let t = i64::from_le_bytes(rest[..8].try_into().unwrap());
            Ok((Value::Time(t), 1 + 8))
        }
        _ => Err(Error::Serialization(format!(
            "未知 type tag: 0x{:02X}",
            tag
        ))),
    }
}

/// 二进制解码行（不含 version 头 — 由 Schema::decode_row 负责剥离）。
/// M83：预估容量减少 realloc。
pub fn decode_row_binary(data: &[u8]) -> Result<Vec<Value>, Error> {
    // 估算：平均每列 ~9 字节 (1 tag + 8 payload)
    let mut row = Vec::with_capacity((data.len() / 9).max(4));
    let mut offset = 0;
    while offset < data.len() {
        let (val, consumed) = decode_value(&data[offset..])?;
        row.push(val);
        offset += consumed;
    }
    Ok(row)
}

/// M93：跳过单个 value（不分配内存），返回消耗字节数。
/// 用于列裁剪——快速跳过不需要的列。
fn skip_value(data: &[u8]) -> Result<usize, Error> {
    if data.is_empty() {
        return Err(Error::Serialization("skip: 缺少 type tag".into()));
    }
    match data[0] {
        TAG_NULL => Ok(1),
        TAG_INTEGER | TAG_FLOAT | TAG_TIMESTAMP | TAG_TIME => Ok(1 + 8),
        TAG_GEOPOINT => Ok(1 + 16),
        TAG_BOOLEAN => Ok(1 + 1),
        TAG_DATE => Ok(1 + 4),
        TAG_TEXT | TAG_BLOB | TAG_JSONB => {
            if data.len() < 5 {
                return Err(Error::Serialization("skip: 长度不足".into()));
            }
            let len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            Ok(1 + 4 + len)
        }
        TAG_VECTOR => {
            if data.len() < 5 {
                return Err(Error::Serialization("skip: Vector 长度不足".into()));
            }
            let count = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            Ok(1 + 4 + count * 4)
        }
        _ => Err(Error::Serialization(format!(
            "skip: 未知 tag 0x{:02X}",
            data[0]
        ))),
    }
}

/// M93：稀疏列解码——只解码 `targets` 中指定的列索引，其余跳过。
/// 返回 Vec 长度 = targets.len()，按 targets 顺序排列。
/// 用于聚合优化：71 列宽表的 SUM(col) 只需解码 1 列。
pub fn decode_columns_sparse(data: &[u8], targets: &[usize]) -> Result<Vec<Value>, Error> {
    if targets.is_empty() {
        return Ok(vec![]);
    }
    let max_target = *targets.iter().max().unwrap();
    let mut result = vec![Value::Null; targets.len()];
    // 构建 col_idx -> result_position 映射
    let mut target_map = std::collections::HashMap::with_capacity(targets.len());
    for (pos, &col_idx) in targets.iter().enumerate() {
        target_map.insert(col_idx, pos);
    }
    let mut offset = 0;
    let mut col = 0usize;
    while offset < data.len() && col <= max_target {
        if let Some(&pos) = target_map.get(&col) {
            let (val, consumed) = decode_value(&data[offset..])?;
            result[pos] = val;
            offset += consumed;
        } else {
            offset += skip_value(&data[offset..])?;
        }
        col += 1;
    }
    Ok(result)
}

/// M93 方案A：零分配读取指定列的 f64 值（聚合专用）。
/// 跳过前 col_idx 列，直接从 raw bytes 读取 Integer/Float/Timestamp 为 f64。
/// 无任何堆分配，1M 行聚合消除 1M 次 `Vec<Value>` 分配。
/// 返回 None 表示该列为 NULL 或非数值类型。
#[inline]
pub fn read_column_f64(data: &[u8], col_idx: usize) -> Result<Option<f64>, Error> {
    let mut offset = 0;
    let mut col = 0usize;
    while offset < data.len() && col < col_idx {
        offset += skip_value(&data[offset..])?;
        col += 1;
    }
    if offset >= data.len() {
        return Ok(None);
    }
    let tag = data[offset];
    let rest = &data[offset + 1..];
    match tag {
        TAG_NULL => Ok(None),
        TAG_INTEGER | TAG_TIMESTAMP => {
            if rest.len() < 8 {
                return Ok(None);
            }
            Ok(Some(
                i64::from_le_bytes(rest[..8].try_into().unwrap()) as f64
            ))
        }
        TAG_FLOAT => {
            if rest.len() < 8 {
                return Ok(None);
            }
            Ok(Some(f64::from_le_bytes(rest[..8].try_into().unwrap())))
        }
        _ => Ok(None),
    }
}

/// M93 方案A：零分配判断指定列是否为 Integer 类型（用于区分 SUM 返回类型）。
#[inline]
pub fn is_column_integer(data: &[u8], col_idx: usize) -> bool {
    let mut offset = 0;
    let mut col = 0usize;
    while offset < data.len() && col < col_idx {
        if let Ok(n) = skip_value(&data[offset..]) {
            offset += n;
        } else {
            return false;
        }
        col += 1;
    }
    offset < data.len() && data[offset] == TAG_INTEGER
}

/// 检测 payload（version 头之后的字节）是否为旧 JSON 格式。
#[inline]
pub fn is_json_payload(payload: &[u8]) -> bool {
    !payload.is_empty() && payload[0] == JSON_ARRAY_START
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_roundtrip_all_types() {
        let row = vec![
            Value::Null,
            Value::Integer(42),
            Value::Float(3.15),
            Value::Text("hello 你好".to_string()),
            Value::Blob(vec![0xDE, 0xAD]),
            Value::Boolean(true),
            Value::Jsonb(serde_json::json!({"a": 1})),
            Value::Vector(vec![0.1, 0.2, 0.3]),
            Value::Timestamp(1708934400000_i64),
        ];
        let encoded = encode_row_binary(&row);
        let decoded = decode_row_binary(&encoded).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn binary_roundtrip_empty_row() {
        let row: Vec<Value> = vec![];
        let encoded = encode_row_binary(&row);
        assert!(encoded.is_empty());
        let decoded = decode_row_binary(&encoded).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn binary_integer_exact_bytes() {
        let row = vec![Value::Integer(12345)];
        let encoded = encode_row_binary(&row);
        // 1 tag + 8 bytes = 9
        assert_eq!(encoded.len(), 9);
        assert_eq!(encoded[0], TAG_INTEGER);
    }

    #[test]
    fn json_detection() {
        // JSON payload starts with [
        assert!(is_json_payload(b"[1,2,3]"));
        // Binary payload starts with type tag
        assert!(!is_json_payload(&[TAG_NULL]));
        assert!(!is_json_payload(&[TAG_INTEGER]));
        // Empty
        assert!(!is_json_payload(b""));
    }

    #[test]
    fn binary_roundtrip_date_time() {
        let row = vec![
            Value::Date(19783),              // 2024-03-01
            Value::Time(43200_000_000_000),  // 12:00:00
        ];
        let encoded = encode_row_binary(&row);
        let decoded = decode_row_binary(&encoded).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn binary_date_exact_bytes() {
        let row = vec![Value::Date(19783)];
        let encoded = encode_row_binary(&row);
        // 1 tag + 4 bytes = 5
        assert_eq!(encoded.len(), 5);
        assert_eq!(encoded[0], TAG_DATE);
    }

    #[test]
    fn binary_time_exact_bytes() {
        let row = vec![Value::Time(43200_000_000_000)];
        let encoded = encode_row_binary(&row);
        // 1 tag + 8 bytes = 9
        assert_eq!(encoded.len(), 9);
        assert_eq!(encoded[0], TAG_TIME);
    }
}
