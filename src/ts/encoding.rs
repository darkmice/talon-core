/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序数据点二进制编解码：替代 JSON，提升 3-10x 写入/查询性能。
//!
//! **V2 格式**（当前写入格式，支持混合数值/文本 field）：
//! ```text
//! [version: u8 = 2]
//! [num_fields: u16 LE]
//! For each field:
//!   [type_tag: u8]  // 0 = f64, 1 = string
//!   f64: [value: f64 LE]  |  string: [len: u16 LE][bytes]
//! [num_tags: u16 LE]
//! [tag_val_len: u16 LE][tag_val_bytes] × num_tags
//! ```
//!
//! V1 格式（旧，仅数值 field）和 JSON 格式均可解码（向后兼容）。
use super::{DataPoint, TsSchema};
use crate::error::Error;
use std::collections::BTreeMap;

const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;
/// V2 field type tags.
const FIELD_F64: u8 = 0;
const FIELD_STR: u8 = 1;
/// 将 DataPoint 编码为 v2 二进制格式（支持混合数值/文本 field）。
pub(super) fn encode_point(schema: &TsSchema, point: &DataPoint) -> Result<Vec<u8>, Error> {
    let num_fields = schema.fields.len();
    let num_tags = schema.tags.len();
    let mut buf = Vec::with_capacity(1 + 2 + num_fields * 10 + 2 + num_tags * 12);
    buf.push(VERSION_V2);
    buf.extend_from_slice(&(num_fields as u16).to_le_bytes());
    for field_name in &schema.fields {
        let raw = point
            .fields
            .get(field_name)
            .map(|s| s.as_str())
            .unwrap_or("");
        if let Ok(val) = raw.parse::<f64>() {
            buf.push(FIELD_F64);
            buf.extend_from_slice(&val.to_le_bytes());
        } else {
            let bytes = raw.as_bytes();
            if bytes.len() > u16::MAX as usize {
                return Err(Error::TimeSeries(format!(
                    "field '{}' 超过 65535 字节限制 ({})",
                    field_name,
                    bytes.len()
                )));
            }
            buf.push(FIELD_STR);
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    buf.extend_from_slice(&(num_tags as u16).to_le_bytes());
    for tag_name in &schema.tags {
        let val = point
            .tags
            .get(tag_name)
            .map(|s| s.as_bytes())
            .unwrap_or(b"");
        buf.extend_from_slice(&(val.len() as u16).to_le_bytes());
        buf.extend_from_slice(val);
    }
    Ok(buf)
}
/// 将二进制格式解码为 DataPoint。兼容 v1/v2/JSON 三种格式。
pub(super) fn decode_point(
    schema: &TsSchema,
    timestamp: i64,
    data: &[u8],
) -> Result<DataPoint, Error> {
    if data.is_empty() {
        return Err(Error::TimeSeries("空数据".into()));
    }
    match data[0] {
        VERSION_V2 => decode_v2(schema, timestamp, &data[1..]),
        VERSION_V1 => decode_v1(schema, timestamp, &data[1..]),
        _ => decode_json(data),
    }
}
/// 从二进制数据中直接读取指定 field 的 f64 值（零拷贝）。
/// 支持 v1（固定 f64）和 v2（混合类型）格式。
#[allow(dead_code)]
pub(super) fn read_field_f64(data: &[u8], field_index: usize) -> Option<f64> {
    if data.len() < 2 {
        return None;
    }
    let version = data[0];
    let d = &data[1..];
    if d.len() < 2 {
        return None;
    }
    let num_fields = u16::from_le_bytes([d[0], d[1]]) as usize;
    if field_index >= num_fields {
        return None;
    }
    if version == VERSION_V1 {
        let offset = 2 + field_index * 8;
        if d.len() < offset + 8 {
            return None;
        }
        return Some(f64::from_le_bytes(
            d[offset..offset + 8].try_into().unwrap(),
        ));
    }
    if version == VERSION_V2 {
        let mut pos = 2;
        for i in 0..num_fields {
            if pos >= d.len() {
                return None;
            }
            let type_tag = d[pos];
            pos += 1;
            if type_tag == FIELD_F64 {
                if pos + 8 > d.len() {
                    return None;
                }
                if i == field_index {
                    return Some(f64::from_le_bytes(d[pos..pos + 8].try_into().unwrap()));
                }
                pos += 8;
            } else {
                if pos + 2 > d.len() {
                    return None;
                }
                let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
                pos += 2;
                if i == field_index {
                    let s = std::str::from_utf8(d.get(pos..pos + len)?).ok()?;
                    return s.parse::<f64>().ok();
                }
                pos += len;
            }
        }
    }
    None
}
/// 合并 tags_match + read_field_f64：单次遍历同时读取 field 值和检查 tag。
/// 聚合查询专用，避免两次遍历二进制数据。返回 None 表示 tag 不匹配或读取失败。
pub(super) fn read_field_if_tags_match(
    schema: &TsSchema,
    data: &[u8],
    field_index: usize,
    tag_filters: &[(String, String)],
) -> Option<f64> {
    if data.len() < 2 {
        return None;
    }
    let version = data[0];
    if version != VERSION_V1 && version != VERSION_V2 {
        return None;
    }
    let d = &data[1..];
    if d.len() < 2 {
        return None;
    }
    let num_fields = u16::from_le_bytes([d[0], d[1]]) as usize;
    if field_index >= num_fields {
        return None;
    }
    // 读取目标 field 值
    let mut field_val: Option<f64> = None;
    let mut pos = 2usize;
    if version == VERSION_V1 {
        let offset = 2 + field_index * 8;
        if d.len() < offset + 8 {
            return None;
        }
        field_val = Some(f64::from_le_bytes(
            d[offset..offset + 8].try_into().unwrap(),
        ));
        pos = 2 + num_fields * 8;
    } else {
        for i in 0..num_fields {
            if pos >= d.len() {
                return None;
            }
            let type_tag = d[pos];
            pos += 1;
            if type_tag == FIELD_F64 {
                if pos + 8 > d.len() {
                    return None;
                }
                if i == field_index {
                    field_val = Some(f64::from_le_bytes(d[pos..pos + 8].try_into().unwrap()));
                }
                pos += 8;
            } else if type_tag == FIELD_STR {
                if pos + 2 > d.len() {
                    return None;
                }
                let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
                pos += 2;
                if i == field_index {
                    let s = std::str::from_utf8(d.get(pos..pos + len)?).ok()?;
                    field_val = Some(s.parse::<f64>().ok()?);
                }
                pos += len;
            } else {
                return None;
            }
        }
    }
    let val = field_val?;
    if val.is_nan() {
        return None;
    }
    // 检查 tags
    if tag_filters.is_empty() {
        return Some(val);
    }
    if check_tags_at(d, pos, schema, tag_filters) {
        Some(val)
    } else {
        None
    }
}

/// 检查二进制数据中的 tag 值是否匹配过滤条件。支持 v1/v2。
pub(super) fn tags_match(schema: &TsSchema, data: &[u8], tag_filters: &[(String, String)]) -> bool {
    if tag_filters.is_empty() {
        return true;
    }
    if data.len() < 2 {
        return false;
    }
    let version = data[0];
    if version != VERSION_V1 && version != VERSION_V2 {
        return false;
    }
    let d = &data[1..];
    if d.len() < 2 {
        return false;
    }
    let num_fields = u16::from_le_bytes([d[0], d[1]]) as usize;
    let pos = skip_fields(version, d, num_fields);
    if pos == 0 {
        return false;
    } // skip failed
    check_tags_at(d, pos, schema, tag_filters)
}
// ── 内部函数 ──
fn decode_v1(schema: &TsSchema, timestamp: i64, d: &[u8]) -> Result<DataPoint, Error> {
    if d.len() < 2 {
        return Err(Error::TimeSeries("数据太短".into()));
    }
    let num_fields = u16::from_le_bytes([d[0], d[1]]) as usize;
    let mut pos = 2;
    // 读取 fields
    let mut fields = BTreeMap::new();
    for (i, field_name) in schema.fields.iter().enumerate() {
        if i < num_fields && d.len() >= pos + 8 {
            let val = f64::from_le_bytes(d[pos..pos + 8].try_into().unwrap());
            // 保持与旧 API 兼容：field 值存为字符串
            if val.is_nan() {
                fields.insert(field_name.clone(), String::new());
            } else if val == val.trunc() && val.abs() < 1e15 {
                fields.insert(field_name.clone(), format!("{}", val as i64));
            } else {
                fields.insert(field_name.clone(), val.to_string());
            }
            pos += 8;
        }
    }

    // 读取 tags
    if d.len() < pos + 2 {
        return Err(Error::TimeSeries("tag 数据截断".into()));
    }
    let num_tags = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
    pos += 2;

    let mut tags = BTreeMap::new();
    for (i, tag_name) in schema.tags.iter().enumerate() {
        if i < num_tags {
            if d.len() < pos + 2 {
                return Err(Error::TimeSeries("tag 长度截断".into()));
            }
            let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
            pos += 2;
            if d.len() < pos + len {
                return Err(Error::TimeSeries("tag 值截断".into()));
            }
            let val = std::str::from_utf8(&d[pos..pos + len])
                .map_err(|e| Error::TimeSeries(e.to_string()))?;
            tags.insert(tag_name.clone(), val.to_string());
            pos += len;
        }
    }

    Ok(DataPoint {
        timestamp,
        tags,
        fields,
    })
}

/// V2 解码：支持混合 f64/string field。
fn decode_v2(schema: &TsSchema, timestamp: i64, d: &[u8]) -> Result<DataPoint, Error> {
    if d.len() < 2 {
        return Err(Error::TimeSeries("v2 数据太短".into()));
    }
    let num_fields = u16::from_le_bytes([d[0], d[1]]) as usize;
    let mut pos = 2;
    let mut fields = BTreeMap::new();
    for (i, field_name) in schema.fields.iter().enumerate() {
        if i >= num_fields || pos >= d.len() {
            break;
        }
        let type_tag = d[pos];
        pos += 1;
        if type_tag == FIELD_F64 {
            if pos + 8 > d.len() {
                return Err(Error::TimeSeries("v2 f64 截断".into()));
            }
            let val = f64::from_le_bytes(d[pos..pos + 8].try_into().unwrap());
            if val == val.trunc() && val.abs() < 1e15 && !val.is_nan() {
                fields.insert(field_name.clone(), format!("{}", val as i64));
            } else if val.is_nan() {
                fields.insert(field_name.clone(), String::new());
            } else {
                fields.insert(field_name.clone(), val.to_string());
            }
            pos += 8;
        } else if type_tag == FIELD_STR {
            if pos + 2 > d.len() {
                return Err(Error::TimeSeries("v2 str len 截断".into()));
            }
            let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
            pos += 2;
            if pos + len > d.len() {
                return Err(Error::TimeSeries("v2 str 截断".into()));
            }
            let val = std::str::from_utf8(&d[pos..pos + len])
                .map_err(|e| Error::TimeSeries(e.to_string()))?;
            fields.insert(field_name.clone(), val.to_string());
            pos += len;
        } else {
            return Err(Error::TimeSeries(format!(
                "v2 未知 field type_tag: {}",
                type_tag
            )));
        }
    }
    // tags 部分与 v1 格式相同
    if d.len() < pos + 2 {
        return Err(Error::TimeSeries("v2 tag 截断".into()));
    }
    let num_tags = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
    pos += 2;
    let mut tags = BTreeMap::new();
    for (i, tag_name) in schema.tags.iter().enumerate() {
        if i >= num_tags {
            break;
        }
        if d.len() < pos + 2 {
            return Err(Error::TimeSeries("v2 tag len 截断".into()));
        }
        let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
        pos += 2;
        if d.len() < pos + len {
            return Err(Error::TimeSeries("v2 tag val 截断".into()));
        }
        let val = std::str::from_utf8(&d[pos..pos + len])
            .map_err(|e| Error::TimeSeries(e.to_string()))?;
        tags.insert(tag_name.clone(), val.to_string());
        pos += len;
    }
    Ok(DataPoint {
        timestamp,
        tags,
        fields,
    })
}

/// 跳过 fields 区域，返回 tags 起始位置。失败返回 0。
fn skip_fields(version: u8, d: &[u8], num_fields: usize) -> usize {
    if version == VERSION_V1 {
        return 2 + num_fields * 8;
    }
    // v2: 逐个跳过
    let mut pos = 2;
    for _ in 0..num_fields {
        if pos >= d.len() {
            return 0;
        }
        let type_tag = d[pos];
        pos += 1;
        if type_tag == FIELD_F64 {
            if pos + 8 > d.len() {
                return 0;
            }
            pos += 8;
        } else if type_tag == FIELD_STR {
            if pos + 2 > d.len() {
                return 0;
            }
            let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
            pos += 2 + len;
        } else {
            return 0; // 未知 type_tag，数据损坏
        }
    }
    pos
}

/// 从给定位置读取 tags 并检查过滤条件。
fn check_tags_at(
    d: &[u8],
    mut pos: usize,
    schema: &TsSchema,
    filters: &[(String, String)],
) -> bool {
    if d.len() < pos + 2 {
        return false;
    }
    let num_tags = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
    pos += 2;
    for (i, tag_name) in schema.tags.iter().enumerate() {
        if i >= num_tags || d.len() < pos + 2 {
            return false;
        }
        let len = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
        pos += 2;
        if d.len() < pos + len {
            return false;
        }
        for (fk, fv) in filters {
            if fk == tag_name && (len != fv.len() || &d[pos..pos + len] != fv.as_bytes()) {
                return false;
            }
        }
        pos += len;
    }
    true
}

fn decode_json(data: &[u8]) -> Result<DataPoint, Error> {
    serde_json::from_slice(data).map_err(|e| Error::TimeSeries(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_encode_decode() {
        let schema = TsSchema {
            tags: vec!["host".into(), "region".into()],
            fields: vec!["cpu".into(), "mem".into()],
        };
        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "srv1".into());
        tags.insert("region".into(), "us-east".into());
        let mut fields = BTreeMap::new();
        fields.insert("cpu".into(), "85.5".into());
        fields.insert("mem".into(), "1024".into());
        let point = DataPoint {
            timestamp: 1234567890,
            tags,
            fields,
        };

        let encoded = encode_point(&schema, &point).unwrap();
        assert!(encoded.len() < 60); // much smaller than JSON

        let decoded = decode_point(&schema, 1234567890, &encoded).unwrap();
        assert_eq!(decoded.timestamp, 1234567890);
        assert_eq!(decoded.tags.get("host").unwrap(), "srv1");
        assert_eq!(decoded.tags.get("region").unwrap(), "us-east");
        assert_eq!(decoded.fields.get("cpu").unwrap(), "85.5");
        assert_eq!(decoded.fields.get("mem").unwrap(), "1024");
    }

    #[test]
    fn read_field_direct() {
        let schema = TsSchema {
            tags: vec!["t".into()],
            fields: vec!["a".into(), "b".into()],
        };
        let mut tags = BTreeMap::new();
        tags.insert("t".into(), "x".into());
        let mut fields = BTreeMap::new();
        fields.insert("a".into(), "42".into());
        fields.insert("b".into(), "99.9".into());
        let point = DataPoint {
            timestamp: 0,
            tags,
            fields,
        };
        let encoded = encode_point(&schema, &point).unwrap();
        assert!((read_field_f64(&encoded, 0).unwrap() - 42.0).abs() < 0.01);
        assert!((read_field_f64(&encoded, 1).unwrap() - 99.9).abs() < 0.01);
        assert!(read_field_f64(&encoded, 2).is_none());
    }

    #[test]
    fn tags_match_filter() {
        let schema = TsSchema {
            tags: vec!["host".into(), "dc".into()],
            fields: vec!["cpu".into()],
        };
        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "srv1".into());
        tags.insert("dc".into(), "us".into());
        let mut fields = BTreeMap::new();
        fields.insert("cpu".into(), "50".into());
        let point = DataPoint {
            timestamp: 0,
            tags,
            fields,
        };
        let encoded = encode_point(&schema, &point).unwrap();

        assert!(tags_match(&schema, &encoded, &[]));
        assert!(tags_match(
            &schema,
            &encoded,
            &[("host".into(), "srv1".into())]
        ));
        assert!(!tags_match(
            &schema,
            &encoded,
            &[("host".into(), "srv2".into())]
        ));
        assert!(tags_match(
            &schema,
            &encoded,
            &[("host".into(), "srv1".into()), ("dc".into(), "us".into())]
        ));
    }

    #[test]
    fn roundtrip_mixed_fields() {
        let schema = TsSchema {
            tags: vec!["session_id".into(), "role".into()],
            fields: vec!["content".into(), "token_count".into()],
        };
        let mut tags = BTreeMap::new();
        tags.insert("session_id".into(), "s1".into());
        tags.insert("role".into(), "user".into());
        let mut fields = BTreeMap::new();
        fields.insert("content".into(), "Hello, world!".into());
        fields.insert("token_count".into(), "5".into());
        let point = DataPoint {
            timestamp: 100,
            tags,
            fields,
        };
        let encoded = encode_point(&schema, &point).unwrap();
        assert_eq!(encoded[0], 2); // v2
        let decoded = decode_point(&schema, 100, &encoded).unwrap();
        assert_eq!(decoded.fields.get("content").unwrap(), "Hello, world!");
        assert_eq!(decoded.fields.get("token_count").unwrap(), "5");
        assert_eq!(decoded.tags.get("session_id").unwrap(), "s1");
        assert_eq!(decoded.tags.get("role").unwrap(), "user");
        // read_field_f64 should return None for string field, Some for numeric
        assert!(read_field_f64(&encoded, 0).is_none());
        assert!((read_field_f64(&encoded, 1).unwrap() - 5.0).abs() < 0.01);
    }

    #[test]
    fn json_fallback() {
        let schema = TsSchema {
            tags: vec!["k".into()],
            fields: vec!["v".into()],
        };
        let mut tags = BTreeMap::new();
        tags.insert("k".into(), "x".into());
        let mut fields = BTreeMap::new();
        fields.insert("v".into(), "42".into());
        let point = DataPoint {
            timestamp: 1000,
            tags,
            fields,
        };
        let json = serde_json::to_vec(&point).unwrap();
        let decoded = decode_point(&schema, 1000, &json).unwrap();
        assert_eq!(decoded.timestamp, 1000);
        assert_eq!(decoded.fields.get("v").unwrap(), "42");
    }
}
