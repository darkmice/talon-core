/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 有序字节编码（Order-Preserving Encoding）：索引 key 的正确性基础。
//!
//! 保证：对于同类型值 a < b，`ordered_encode(a) < ordered_encode(b)`（字节序）。
//!
//! 编码格式：
//! - Null:    `0x00`
//! - Boolean: `0x01` + `0x00`(false) / `0x01`(true)
//! - Integer: `0x02` + big-endian i64（XOR 符号位，使负数 < 正数）
//! - Float:   `0x03` + IEEE 754 变换（正数 XOR 符号位；负数全部取反）
//! - Text:    `0x04` + UTF-8 bytes（`0x00` → `0x00 0xFF` 转义）+ `0x00 0x00` 终结符
//!
//! 索引条目 key = `ordered_encode(col_val) + ordered_encode(pk)`。
//! 范围扫描利用字节序有序性，直接在 LSM-Tree 上做前缀/范围迭代。

use crate::types::Value;
use crate::Error;

// ── 类型标记 ──────────────────────────────────────────────

const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_INT: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_TEXT: u8 = 0x04;

// ── 编码 ──────────────────────────────────────────────────

/// 将 Value 编码为保序字节序列。
pub(super) fn ordered_encode(val: &Value) -> Result<Vec<u8>, Error> {
    match val {
        Value::Null => Ok(vec![TAG_NULL]),
        Value::Boolean(b) => Ok(vec![TAG_BOOL, if *b { 0x01 } else { 0x00 }]),
        Value::Integer(n) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_INT);
            // XOR 符号位：使 i64 的字节序等于数值序
            let encoded = (*n as u64) ^ (1u64 << 63);
            buf.extend_from_slice(&encoded.to_be_bytes());
            Ok(buf)
        }
        Value::Float(f) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_FLOAT);
            let bits = f.to_bits();
            // 符号位判断：bits 最高位为 0 → 正数/+0；为 1 → 负数/-0
            // 正数（含 +0）：XOR 符号位
            // 负数（含 -0）：全部取反
            // 特殊：-0.0 归一化为 +0.0（二者数学相等，编码也应相等）
            let normalized_bits = if bits == 0x8000_0000_0000_0000 {
                0u64
            } else {
                bits
            };
            let encoded = if normalized_bits >> 63 == 0 {
                normalized_bits ^ (1u64 << 63)
            } else {
                !normalized_bits
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
            Ok(buf)
        }
        Value::Text(s) => {
            // 预分配：tag + bytes + 转义余量 + 终结符
            let mut buf = Vec::with_capacity(1 + s.len() + 2);
            buf.push(TAG_TEXT);
            for &b in s.as_bytes() {
                if b == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF); // 转义 0x00
                } else {
                    buf.push(b);
                }
            }
            buf.push(0x00);
            buf.push(0x00); // 终结符
            Ok(buf)
        }
        Value::Blob(b) => {
            // Blob 与 Text 同理，使用 TAG_TEXT + 1 = 0x05
            let mut buf = Vec::with_capacity(1 + b.len() + 2);
            buf.push(TAG_TEXT + 1);
            for &byte in b.iter() {
                if byte == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF);
                } else {
                    buf.push(byte);
                }
            }
            buf.push(0x00);
            buf.push(0x00);
            Ok(buf)
        }
        _ => Err(Error::SqlExec(format!("不支持索引编码的类型: {:?}", val))),
    }
}

/// 从保序字节序列解码回 Value。返回 (Value, 消耗的字节数)。
pub(super) fn ordered_decode(buf: &[u8]) -> Result<(Value, usize), Error> {
    if buf.is_empty() {
        return Err(Error::SqlExec("索引 key 为空".into()));
    }
    match buf[0] {
        TAG_NULL => Ok((Value::Null, 1)),
        TAG_BOOL => {
            if buf.len() < 2 {
                return Err(Error::SqlExec("Boolean 索引 key 不完整".into()));
            }
            Ok((Value::Boolean(buf[1] != 0), 2))
        }
        TAG_INT => {
            if buf.len() < 9 {
                return Err(Error::SqlExec("Integer 索引 key 不完整".into()));
            }
            let encoded = u64::from_be_bytes(buf[1..9].try_into().unwrap());
            let n = (encoded ^ (1u64 << 63)) as i64;
            Ok((Value::Integer(n), 9))
        }
        TAG_FLOAT => {
            if buf.len() < 9 {
                return Err(Error::SqlExec("Float 索引 key 不完整".into()));
            }
            let encoded = u64::from_be_bytes(buf[1..9].try_into().unwrap());
            // 逆变换：如果最高位为 1（正数），XOR 符号位；否则（负数）全部取反
            let bits = if encoded & (1u64 << 63) != 0 {
                encoded ^ (1u64 << 63)
            } else {
                !encoded
            };
            Ok((Value::Float(f64::from_bits(bits)), 9))
        }
        TAG_TEXT => {
            let mut s = Vec::new();
            let mut i = 1;
            while i < buf.len() {
                if buf[i] == 0x00 {
                    if i + 1 < buf.len() && buf[i + 1] == 0xFF {
                        s.push(0x00); // 转义还原
                        i += 2;
                    } else {
                        // 0x00 0x00 终结符
                        i += 2;
                        break;
                    }
                } else {
                    s.push(buf[i]);
                    i += 1;
                }
            }
            let text = String::from_utf8(s)
                .map_err(|e| Error::SqlExec(format!("索引 key UTF-8 解码失败: {}", e)))?;
            Ok((Value::Text(text), i))
        }
        0x05 => {
            // Blob 解码
            let mut data = Vec::new();
            let mut i = 1;
            while i < buf.len() {
                if buf[i] == 0x00 {
                    if i + 1 < buf.len() && buf[i + 1] == 0xFF {
                        data.push(0x00);
                        i += 2;
                    } else {
                        i += 2;
                        break;
                    }
                } else {
                    data.push(buf[i]);
                    i += 1;
                }
            }
            Ok((Value::Blob(data), i))
        }
        _ => Err(Error::SqlExec(format!(
            "未知索引 key 类型标记: 0x{:02X}",
            buf[0]
        ))),
    }
}

// ── 索引 key 构造 ─────────────────────────────────────────

/// 构造索引条目 key：`ordered_encode(col_val) + ordered_encode(pk)`。
pub(super) fn index_entry_key(col_val: &Value, pk: &Value) -> Result<Vec<u8>, Error> {
    let mut key = ordered_encode(col_val)?;
    key.extend(ordered_encode(pk)?);
    Ok(key)
}

/// M112：构造复合索引条目 key：`ordered_encode(v1) + ordered_encode(v2) + ... + ordered_encode(pk)`。
/// 单列索引是 `col_vals.len() == 1` 的特例。
pub(super) fn composite_index_entry_key(col_vals: &[&Value], pk: &Value) -> Result<Vec<u8>, Error> {
    let mut key = Vec::new();
    for v in col_vals {
        key.extend(ordered_encode(v)?);
    }
    key.extend(ordered_encode(pk)?);
    Ok(key)
}

/// M112：构造复合索引扫描前缀：`ordered_encode(v1) + ordered_encode(v2) + ...`。
/// 用于等值扫描：前缀匹配同一组合值的所有行。
pub(super) fn composite_index_scan_prefix(col_vals: &[&Value]) -> Result<Vec<u8>, Error> {
    let mut prefix = Vec::new();
    for v in col_vals {
        prefix.extend(ordered_encode(v)?);
    }
    Ok(prefix)
}

/// 构造索引扫描前缀：`ordered_encode(col_val)`。
/// 用于等值扫描：前缀匹配同一列值的所有行。
pub(super) fn index_scan_prefix(col_val: &Value) -> Result<Vec<u8>, Error> {
    ordered_encode(col_val)
}

/// 从索引条目 key 中提取 PK 字节（数据层格式：`Value::to_bytes()`）。
///
/// 索引 key = `ordered_encode(col) + ordered_encode(pk)`。
/// 此函数解码 PK 部分后，用 `Value::to_bytes()` 转为数据层可用的 key。
pub(super) fn parse_index_pk(index_key: &[u8]) -> Option<Vec<u8>> {
    let (_col_val, col_len) = ordered_decode(index_key).ok()?;
    if col_len >= index_key.len() {
        return None;
    }
    let (pk_val, _) = ordered_decode(&index_key[col_len..]).ok()?;
    pk_val.to_bytes().ok()
}
/// M112：复合索引唯一约束检查。
pub(super) fn check_unique_violation_composite(
    idx_ks: &crate::storage::Keyspace,
    col_vals: &[&Value],
    exclude_pk: Option<&[u8]>,
) -> Result<(), Error> {
    // NULL 值不参与唯一约束（SQL 标准：NULL != NULL）
    if col_vals.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(());
    }
    let prefix = composite_index_scan_prefix(col_vals)?;
    let mut found = false;
    idx_ks.for_each_key_prefix(&prefix, |key| {
        if let Some(epk) = exclude_pk {
            if let Some(pk_bytes) = parse_index_pk_at(key, col_vals.len()) {
                if pk_bytes == epk {
                    return true; // 继续扫描
                }
            }
        }
        found = true;
        false // 找到一条即停止
    })?;
    if found {
        Err(Error::SqlExec(format!(
            "UNIQUE constraint failed: duplicate value {:?}",
            col_vals
        )))
    } else {
        Ok(())
    }
}
/// M112：事务感知的复合索引唯一约束检查。
pub(super) fn check_unique_violation_tx_composite(
    idx_ks: &crate::storage::Keyspace,
    tx_index_writes: &[(String, String, Vec<u8>, bool)],
    table: &str,
    cols_key: &str,
    col_vals: &[&Value],
    exclude_pk: Option<&[u8]>,
) -> Result<(), Error> {
    if col_vals.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(());
    }
    let prefix = composite_index_scan_prefix(col_vals)?;
    let num_cols = col_vals.len();
    // 1. 检查底层 keyspace
    let mut found = false;
    idx_ks.for_each_key_prefix(&prefix, |key| {
        if let Some(epk) = exclude_pk {
            if let Some(pk_bytes) = parse_index_pk_at(key, num_cols) {
                if pk_bytes == epk {
                    return true;
                }
            }
        }
        found = true;
        false
    })?;
    if found {
        return Err(Error::SqlExec(format!(
            "UNIQUE constraint failed: duplicate value {:?}",
            col_vals
        )));
    }
    // 2. 检查事务缓冲区中的 set 条目
    for (t, c, key, is_set) in tx_index_writes {
        if !is_set || t != table || c != cols_key {
            continue;
        }
        if key.starts_with(&prefix) {
            if let Some(epk) = exclude_pk {
                if let Some(pk_bytes) = parse_index_pk_at(key, num_cols) {
                    if pk_bytes == epk {
                        continue;
                    }
                }
            }
            return Err(Error::SqlExec(format!(
                "UNIQUE constraint failed: duplicate value {:?}",
                col_vals
            )));
        }
    }
    Ok(())
}

/// 从索引条目 key 中解码 PK Value（类型化版本，与 `parse_index_pk` 互补）。
#[allow(dead_code)]
pub(crate) fn decode_index_pk(index_key: &[u8]) -> Result<Value, Error> {
    let (_col_val, col_len) = ordered_decode(index_key)?;
    if col_len >= index_key.len() {
        return Err(Error::SqlExec("索引 key 中无 PK 部分".into()));
    }
    let (pk, _) = ordered_decode(&index_key[col_len..])?;
    Ok(pk)
}

/// M112：从复合索引 key 中提取 PK 字节，跳过 `num_cols` 个编码值后解码 PK。
fn parse_index_pk_at(index_key: &[u8], num_cols: usize) -> Option<Vec<u8>> {
    let mut offset = 0;
    for _ in 0..num_cols {
        let (_, len) = ordered_decode(&index_key[offset..]).ok()?;
        offset += len;
    }
    if offset >= index_key.len() {
        return None;
    }
    let (pk_val, _) = ordered_decode(&index_key[offset..]).ok()?;
    pk_val.to_bytes().ok()
}

// ── 范围扫描辅助 ──────────────────────────────────────────

/// 构造范围扫描的起始/结束 key。
///
/// 对于 `col > val`：起始 = encode(val) 的后继字节（append 0x00）
/// 对于 `col >= val`：起始 = encode(val)
/// 对于 `col < val`：结束 = encode(val)
/// 对于 `col <= val`：结束 = encode(val) 的后继字节
///
/// 返回 (start_inclusive, end_exclusive) 前缀范围。
pub(super) fn range_bounds(col_val: &Value, op: RangeOp) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let encoded = ordered_encode(col_val)?;
    match op {
        RangeOp::Gt => {
            // start = encoded + 0x00（后继），end = type_max
            let mut start = encoded;
            start.push(0xFF); // 超过任何 PK 后缀
            start.push(0xFF);
            let end = vec![col_val.type_tag() + 1]; // 下一个类型标记
            Ok((start, end))
        }
        RangeOp::Ge => {
            let start = encoded;
            let end = vec![col_val.type_tag() + 1];
            Ok((start, end))
        }
        RangeOp::Lt => {
            let start = vec![col_val.type_tag()]; // 该类型的最小前缀
            let end = encoded;
            Ok((start, end))
        }
        RangeOp::Le => {
            let start = vec![col_val.type_tag()]; // 该类型的最小前缀
            let mut end = encoded;
            end.push(0xFF); // 包含所有 PK 后缀
            end.push(0xFF);
            Ok((start, end))
        }
    }
}

/// 构造 BETWEEN 范围的起始/结束 key。
pub(super) fn between_bounds(low: &Value, high: &Value) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let start = ordered_encode(low)?;
    let mut end = ordered_encode(high)?;
    end.push(0xFF); // 包含 high 值的所有 PK 后缀
    end.push(0xFF);
    Ok((start, end))
}

/// 范围操作类型。
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RangeOp {
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `<`
    Lt,
    /// `<=`
    Le,
}

// ── Value 辅助 trait ──────────────────────────────────────

trait ValueTypeTag {
    fn type_tag(&self) -> u8;
}

impl ValueTypeTag for Value {
    fn type_tag(&self) -> u8 {
        match self {
            Value::Null => TAG_NULL,
            Value::Boolean(_) => TAG_BOOL,
            Value::Integer(_) => TAG_INT,
            Value::Float(_) => TAG_FLOAT,
            Value::Text(_) => TAG_TEXT,
            Value::Blob(_) => TAG_TEXT + 1,
            _ => 0xFF,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let cases = vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(-1),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Float(0.0),
            Value::Float(-0.0),
            Value::Float(1.5),
            Value::Float(-1.5),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Text("".into()),
            Value::Text("hello".into()),
            Value::Text("hello\0world".into()),
            Value::Text("\0\0\0".into()),
        ];
        for val in &cases {
            let encoded = ordered_encode(val).unwrap();
            let (decoded, len) = ordered_decode(&encoded).unwrap();
            assert_eq!(len, encoded.len(), "len mismatch for {:?}", val);
            match (val, &decoded) {
                (Value::Float(a), Value::Float(b)) => {
                    if a.is_nan() {
                        assert!(b.is_nan());
                    } else {
                        // -0.0 归一化为 +0.0，所以用数学相等比较
                        assert!(a == b, "float mismatch for {:?}: got {:?}", val, decoded);
                    }
                }
                _ => assert_eq!(val, &decoded, "roundtrip failed for {:?}", val),
            }
        }
    }

    #[test]
    fn integer_ordering() {
        let vals = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let encoded: Vec<Vec<u8>> = vals
            .iter()
            .map(|n| ordered_encode(&Value::Integer(*n)).unwrap())
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "ordering failed: {} vs {}",
                vals[i],
                vals[i + 1]
            );
        }
    }

    #[test]
    fn float_ordering() {
        let vals = [
            f64::NEG_INFINITY,
            -1e100,
            -1.5,
            -0.0,
            0.0,
            1.5,
            1e100,
            f64::INFINITY,
        ];
        let encoded: Vec<Vec<u8>> = vals
            .iter()
            .map(|f| ordered_encode(&Value::Float(*f)).unwrap())
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "ordering failed: {} vs {}",
                vals[i],
                vals[i + 1]
            );
        }
    }

    #[test]
    fn text_ordering() {
        let vals = ["", "a", "aa", "ab", "b", "hello", "hello\0world"];
        let encoded: Vec<Vec<u8>> = vals
            .iter()
            .map(|s| ordered_encode(&Value::Text(s.to_string())).unwrap())
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "ordering failed: {:?} vs {:?}",
                vals[i],
                vals[i + 1]
            );
        }
    }

    #[test]
    fn type_ordering() {
        // Null < Boolean < Integer < Float < Text
        let vals = vec![
            Value::Null,
            Value::Boolean(false),
            Value::Integer(0),
            Value::Float(0.0),
            Value::Text("".into()),
        ];
        let encoded: Vec<Vec<u8>> = vals.iter().map(|v| ordered_encode(v).unwrap()).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "type ordering failed: {:?} vs {:?}",
                vals[i],
                vals[i + 1]
            );
        }
    }

    #[test]
    fn index_entry_key_roundtrip() {
        let col = Value::Integer(42);
        let pk = Value::Integer(1001);
        let key = index_entry_key(&col, &pk).unwrap();
        let decoded_pk = decode_index_pk(&key).unwrap();
        assert_eq!(decoded_pk, pk);
    }

    #[test]
    fn index_entry_key_ordering() {
        // 同一列值下，按 PK 排序
        let col = Value::Text("alice".into());
        let key1 = index_entry_key(&col, &Value::Integer(1)).unwrap();
        let key2 = index_entry_key(&col, &Value::Integer(2)).unwrap();
        let key3 = index_entry_key(&col, &Value::Integer(100)).unwrap();
        assert!(key1 < key2);
        assert!(key2 < key3);

        // 不同列值之间的排序
        let ka = index_entry_key(&Value::Integer(10), &Value::Integer(1)).unwrap();
        let kb = index_entry_key(&Value::Integer(20), &Value::Integer(1)).unwrap();
        assert!(ka < kb);
    }

    #[test]
    fn scan_prefix_matches_entry() {
        let col = Value::Integer(42);
        let pk = Value::Integer(99);
        let prefix = index_scan_prefix(&col).unwrap();
        let key = index_entry_key(&col, &pk).unwrap();
        assert!(key.starts_with(&prefix));

        // 不同列值的前缀不匹配
        let other_prefix = index_scan_prefix(&Value::Integer(43)).unwrap();
        assert!(!key.starts_with(&other_prefix));
    }
}
