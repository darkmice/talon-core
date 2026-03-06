/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 二进制 FFI 编码/解码：替代 JSON 序列化，消除 SQL/Vector 路径的序列化开销。
//!
//! 编码格式（小端序 TLV）：
//! ```text
//! SQL 结果:
//!   row_count: u32, col_count: u32
//!   per cell: type_tag: u8 + payload
//!
//! Vector 搜索结果:
//!   count: u32
//!   per item: id: u64, distance: f32
//!
//! 参数编码:
//!   param_count: u32
//!   per param: type_tag: u8 + payload
//!
//! Type tags:
//!   0=Null, 1=Integer(i64), 2=Float(f64), 3=Text(u32 len + bytes),
//!   4=Blob(u32 len + bytes), 5=Boolean(u8), 6=Jsonb(u32 len + bytes),
//!   7=Vector(u32 dim + f32*dim), 8=Timestamp(i64), 9=GeoPoint(f64,f64)
//! ```

use crate::types::Value;

// ── 编码：Value → bytes ────────────────────────────────────────────────

/// 将 SQL 结果编码为二进制。
pub(crate) fn encode_rows(rows: &[Vec<Value>]) -> Vec<u8> {
    let row_count = rows.len() as u32;
    let col_count = rows.first().map(|r| r.len()).unwrap_or(0) as u32;
    // 预估容量：header(8) + 每 cell 平均 20 bytes
    let mut buf = Vec::with_capacity(8 + (row_count as usize) * (col_count as usize) * 20);
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf.extend_from_slice(&col_count.to_le_bytes());
    for row in rows {
        for val in row {
            encode_value(&mut buf, val);
        }
    }
    buf
}

/// 将向量搜索结果编码为二进制。
pub(crate) fn encode_vector_results(results: &[(u64, f32)]) -> Vec<u8> {
    let count = results.len() as u32;
    let mut buf = Vec::with_capacity(4 + results.len() * 12);
    buf.extend_from_slice(&count.to_le_bytes());
    for (id, dist) in results {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&dist.to_le_bytes());
    }
    buf
}

/// 编码单个 Value 到缓冲区。
fn encode_value(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(0),
        Value::Integer(i) => {
            buf.push(1);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(2);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(3);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Blob(b) => {
            buf.push(4);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Boolean(b) => {
            buf.push(5);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Jsonb(j) => {
            buf.push(6);
            let s = j.to_string();
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Vector(v) => {
            buf.push(7);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Timestamp(t) => {
            buf.push(8);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::GeoPoint(lat, lon) => {
            buf.push(9);
            buf.extend_from_slice(&lat.to_le_bytes());
            buf.extend_from_slice(&lon.to_le_bytes());
        }
        Value::Date(d) => {
            buf.push(10);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Time(t) => {
            buf.push(11);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Placeholder(_) => buf.push(0), // 不应出现在结果中
    }
}

// ── 解码：bytes → Vec<Value>（用于参数解码）──────────────────────────

/// 从二进制解码参数列表。
pub(crate) fn decode_params(data: &[u8]) -> Result<Vec<Value>, &'static str> {
    if data.len() < 4 {
        return Err("params too short");
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut pos = 4;
    let mut params = Vec::with_capacity(count);
    for _ in 0..count {
        let (val, consumed) = decode_value(data, pos)?;
        params.push(val);
        pos += consumed;
    }
    Ok(params)
}

/// 解码单个 Value，返回 (value, consumed_bytes)。
fn decode_value(data: &[u8], pos: usize) -> Result<(Value, usize), &'static str> {
    if pos >= data.len() {
        return Err("unexpected end of data");
    }
    let tag = data[pos];
    let mut off = pos + 1;
    match tag {
        0 => Ok((Value::Null, 1)),
        1 => {
            // Integer: i64
            if off + 8 > data.len() { return Err("truncated i64"); }
            let v = i64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Integer(v), 9))
        }
        2 => {
            // Float: f64
            if off + 8 > data.len() { return Err("truncated f64"); }
            let v = f64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Float(v), 9))
        }
        3 => {
            // Text: u32 len + bytes
            if off + 4 > data.len() { return Err("truncated text len"); }
            let len = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            off += 4;
            if off + len > data.len() { return Err("truncated text data"); }
            let s = std::str::from_utf8(&data[off..off+len]).map_err(|_| "invalid utf8")?;
            Ok((Value::Text(s.to_string()), 5 + len))
        }
        4 => {
            // Blob: u32 len + bytes
            if off + 4 > data.len() { return Err("truncated blob len"); }
            let len = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            off += 4;
            if off + len > data.len() { return Err("truncated blob data"); }
            Ok((Value::Blob(data[off..off+len].to_vec()), 5 + len))
        }
        5 => {
            // Boolean: u8
            if off >= data.len() { return Err("truncated bool"); }
            Ok((Value::Boolean(data[off] != 0), 2))
        }
        6 => {
            // Jsonb: u32 len + JSON bytes
            if off + 4 > data.len() { return Err("truncated jsonb len"); }
            let len = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            off += 4;
            if off + len > data.len() { return Err("truncated jsonb data"); }
            let s = std::str::from_utf8(&data[off..off+len]).map_err(|_| "invalid utf8")?;
            let j: serde_json::Value = serde_json::from_str(s).map_err(|_| "invalid json")?;
            Ok((Value::Jsonb(j), 5 + len))
        }
        7 => {
            // Vector: u32 dim + f32*dim
            if off + 4 > data.len() { return Err("truncated vec dim"); }
            let dim = u32::from_le_bytes(data[off..off+4].try_into().unwrap()) as usize;
            off += 4;
            let byte_len = dim * 4;
            if off + byte_len > data.len() { return Err("truncated vec data"); }
            let mut v = Vec::with_capacity(dim);
            for i in 0..dim {
                let start = off + i * 4;
                v.push(f32::from_le_bytes(data[start..start+4].try_into().unwrap()));
            }
            Ok((Value::Vector(v), 5 + byte_len))
        }
        8 => {
            // Timestamp: i64
            if off + 8 > data.len() { return Err("truncated timestamp"); }
            let v = i64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Timestamp(v), 9))
        }
        9 => {
            // GeoPoint: f64 + f64
            if off + 16 > data.len() { return Err("truncated geopoint"); }
            let lat = f64::from_le_bytes(data[off..off+8].try_into().unwrap());
            let lon = f64::from_le_bytes(data[off+8..off+16].try_into().unwrap());
            Ok((Value::GeoPoint(lat, lon), 17))
        }
        10 => {
            // Date: i32
            if off + 4 > data.len() { return Err("truncated date"); }
            let d = i32::from_le_bytes(data[off..off+4].try_into().unwrap());
            Ok((Value::Date(d), 5))
        }
        11 => {
            // Time: i64
            if off + 8 > data.len() { return Err("truncated time"); }
            let t = i64::from_le_bytes(data[off..off+8].try_into().unwrap());
            Ok((Value::Time(t), 9))
        }
        _ => Err("unknown type tag"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_rows() {
        let rows = vec![
            vec![Value::Integer(42), Value::Text("hello".into()), Value::Null],
            vec![Value::Float(3.14), Value::Boolean(true), Value::Timestamp(1700000000)],
        ];
        let encoded = encode_rows(&rows);
        // 手动解码验证
        let row_count = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
        let col_count = u32::from_le_bytes(encoded[4..8].try_into().unwrap());
        assert_eq!(row_count, 2);
        assert_eq!(col_count, 3);

        // 完整解码
        let decoded = decode_rows_full(&encoded).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn roundtrip_params() {
        let params = vec![
            Value::Text("test".into()),
            Value::Integer(100),
            Value::Null,
            Value::Float(2.718),
        ];
        let mut buf = Vec::new();
        buf.extend_from_slice(&(params.len() as u32).to_le_bytes());
        for p in &params {
            encode_value(&mut buf, p);
        }
        let decoded = decode_params(&buf).unwrap();
        assert_eq!(decoded, params);
    }

    #[test]
    fn roundtrip_vector_results() {
        let results = vec![(1u64, 0.5f32), (2, 0.8), (3, 1.2)];
        let encoded = encode_vector_results(&results);
        let count = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;
        assert_eq!(count, 3);
        let mut decoded = Vec::with_capacity(count);
        for i in 0..count {
            let off = 4 + i * 12;
            let id = u64::from_le_bytes(encoded[off..off+8].try_into().unwrap());
            let dist = f32::from_le_bytes(encoded[off+8..off+12].try_into().unwrap());
            decoded.push((id, dist));
        }
        assert_eq!(decoded, results);
    }

    /// 辅助：完整解码二进制行数据。
    fn decode_rows_full(data: &[u8]) -> Result<Vec<Vec<Value>>, &'static str> {
        if data.len() < 8 { return Err("too short"); }
        let row_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let col_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let mut pos = 8;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut row = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let (val, consumed) = decode_value(data, pos)?;
                row.push(val);
                pos += consumed;
            }
            rows.push(row);
        }
        Ok(rows)
    }
}
