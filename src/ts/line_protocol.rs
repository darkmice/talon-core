/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! InfluxDB Line Protocol 解析器。
//!
//! 格式：`measurement,tag1=v1,tag2=v2 field1=1.0,field2="str" timestamp_ns`
//! 参考：https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/
//!
//! 特性：
//! - 纳秒时间戳自动转毫秒（Talon TS 引擎精度）
//! - 支持 integer (`123i`)、float (`1.5`)、string (`"str"`)、boolean (`true/false/T/F`)
//! - 空行和 `#` 注释行自动跳过
//! - 无时间戳时使用当前系统时间

use std::collections::BTreeMap;

use crate::error::Error;
use crate::ts::DataPoint;

/// 解析结果：measurement 名 + 数据点。
#[derive(Debug, Clone)]
pub struct LineProtocolPoint {
    /// measurement 名（对应 Talon TS 表名）。
    pub measurement: String,
    /// 解析后的数据点。
    pub point: DataPoint,
}

/// 解析 InfluxDB Line Protocol 文本（可含多行）。
/// 返回 `(measurement, DataPoint)` 列表。
/// 空行和 `#` 注释行自动跳过。
pub fn parse_line_protocol(input: &str) -> Result<Vec<LineProtocolPoint>, Error> {
    let mut results = Vec::new();
    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        results.push(parse_single_line(trimmed)?);
    }
    Ok(results)
}

/// 解析单行 Line Protocol。
fn parse_single_line(line: &str) -> Result<LineProtocolPoint, Error> {
    // 格式: measurement[,tag=val...] field=val[,field=val...] [timestamp]
    // 第一个空格分隔 measurement+tags 与 fields+timestamp
    let (meas_tags, rest) =
        split_first_unescaped_space(line).ok_or_else(|| lp_err("missing field set"))?;

    // 解析 measurement 和 tags
    let (measurement, tags) = parse_measurement_tags(meas_tags)?;

    // 分离 fields 和 optional timestamp
    let (fields_str, ts_str) = match split_first_unescaped_space(rest) {
        Some((f, t)) => (f, Some(t.trim())),
        None => (rest, None),
    };

    // 解析 fields
    let fields = parse_fields(fields_str)?;
    if fields.is_empty() {
        return Err(lp_err("at least one field required"));
    }

    // 解析时间戳（纳秒 → 毫秒）
    let timestamp = match ts_str {
        Some(s) if !s.is_empty() => {
            let ns: i64 = s
                .parse()
                .map_err(|_| lp_err(&format!("invalid timestamp: {}", s)))?;
            ns / 1_000_000 // 纳秒 → 毫秒
        }
        _ => now_ms(),
    };

    Ok(LineProtocolPoint {
        measurement,
        point: DataPoint {
            timestamp,
            tags,
            fields,
        },
    })
}

/// 解析 measurement[,tag=val,tag=val] 部分。
fn parse_measurement_tags(s: &str) -> Result<(String, BTreeMap<String, String>), Error> {
    let mut tags = BTreeMap::new();
    // 第一个逗号前是 measurement
    let (measurement, tag_str) = match s.find(',') {
        Some(pos) => (&s[..pos], Some(&s[pos + 1..])),
        None => (s, None),
    };

    if measurement.is_empty() {
        return Err(lp_err("empty measurement name"));
    }

    if let Some(tag_str) = tag_str {
        for kv in tag_str.split(',') {
            let (k, v) = split_kv(kv, "tag")?;
            tags.insert(k, v);
        }
    }

    Ok((measurement.to_string(), tags))
}

/// 解析 field=val,field=val 部分。
/// 值类型：integer `123i`、float `1.5`、string `"str"`、boolean `true/false/T/F`。
/// 所有值统一存为字符串（Talon TS DataPoint.fields 为 BTreeMap<String, String>）。
fn parse_fields(s: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut fields = BTreeMap::new();
    for kv in split_fields(s) {
        let eq_pos = kv
            .find('=')
            .ok_or_else(|| lp_err(&format!("invalid field: {}", kv)))?;
        let key = &kv[..eq_pos];
        let raw_val = &kv[eq_pos + 1..];
        if key.is_empty() {
            return Err(lp_err("empty field key"));
        }
        let val = normalize_field_value(raw_val);
        fields.insert(key.to_string(), val);
    }
    Ok(fields)
}

/// 规范化字段值：去掉 integer 后缀 `i`、去掉字符串引号、保留 float 和 boolean。
fn normalize_field_value(raw: &str) -> String {
    // 字符串值: "..."
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        return raw[1..raw.len() - 1].to_string();
    }
    // integer: 123i
    if raw.ends_with('i') || raw.ends_with('I') {
        let num_part = &raw[..raw.len() - 1];
        if num_part.parse::<i64>().is_ok() {
            return num_part.to_string();
        }
    }
    // boolean: true/false/T/F/TRUE/FALSE
    match raw {
        "t" | "T" | "True" | "TRUE" => return "true".to_string(),
        "f" | "F" | "False" | "FALSE" => return "false".to_string(),
        _ => {}
    }
    // float 或其他：原样保留
    raw.to_string()
}

/// 按未转义的空格分割（Line Protocol 中空格可被 `\` 转义，引号内空格不分割）。
fn split_first_unescaped_space(s: &str) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_quotes = false;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => in_quotes = !in_quotes,
            b'\\' => {
                i += 2; // 跳过转义字符
                continue;
            }
            b' ' if !in_quotes => {
                return Some((&s[..i], &s[i + 1..]));
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// 按逗号分割 field set（注意字符串值 "..." 中的逗号不分割）。
fn split_fields(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => in_quotes = !in_quotes,
            b'\\' if in_quotes => {
                i += 1; // 跳过转义
            }
            b',' if !in_quotes => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// 分割 key=value 对。
fn split_kv(s: &str, ctx: &str) -> Result<(String, String), Error> {
    let pos = s
        .find('=')
        .ok_or_else(|| lp_err(&format!("invalid {} key=value: {}", ctx, s)))?;
    let k = &s[..pos];
    let v = &s[pos + 1..];
    if k.is_empty() {
        return Err(lp_err(&format!("empty {} key", ctx)));
    }
    Ok((k.to_string(), v.to_string()))
}

/// 当前系统时间（毫秒）。
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 构造 Line Protocol 解析错误。
fn lp_err(msg: &str) -> Error {
    Error::TimeSeries(format!("line protocol: {}", msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_line() {
        let input = "cpu,host=srv1,region=us value=0.64 1609459200000000000";
        let pts = parse_line_protocol(input).unwrap();
        assert_eq!(pts.len(), 1);
        let p = &pts[0];
        assert_eq!(p.measurement, "cpu");
        assert_eq!(p.point.tags["host"], "srv1");
        assert_eq!(p.point.tags["region"], "us");
        assert_eq!(p.point.fields["value"], "0.64");
        assert_eq!(p.point.timestamp, 1609459200000); // ns → ms
    }

    #[test]
    fn parse_multiple_fields() {
        let input = "mem,host=a used=1024i,free=2048i,pct=0.33 1000000000000";
        let pts = parse_line_protocol(input).unwrap();
        let p = &pts[0];
        assert_eq!(p.measurement, "mem");
        assert_eq!(p.point.fields["used"], "1024");
        assert_eq!(p.point.fields["free"], "2048");
        assert_eq!(p.point.fields["pct"], "0.33");
        assert_eq!(p.point.timestamp, 1_000_000); // 1000000000000 ns = 1000000 ms
    }

    #[test]
    fn parse_string_and_boolean_fields() {
        let input = r#"log,app=web msg="hello world",ok=true 5000000000000"#;
        let pts = parse_line_protocol(input).unwrap();
        let p = &pts[0];
        assert_eq!(p.point.fields["msg"], "hello world");
        assert_eq!(p.point.fields["ok"], "true");
    }

    #[test]
    fn parse_no_tags() {
        let input = "cpu value=42.0 1000000000";
        let pts = parse_line_protocol(input).unwrap();
        let p = &pts[0];
        assert_eq!(p.measurement, "cpu");
        assert!(p.point.tags.is_empty());
        assert_eq!(p.point.fields["value"], "42.0");
        assert_eq!(p.point.timestamp, 1000); // 1000000000 ns = 1000 ms
    }

    #[test]
    fn parse_no_timestamp() {
        let input = "cpu,host=a value=1.0";
        let pts = parse_line_protocol(input).unwrap();
        assert_eq!(pts.len(), 1);
        assert!(pts[0].point.timestamp > 0);
    }

    #[test]
    fn parse_multiline_with_comments() {
        let input =
            "# this is a comment\ncpu value=1.0 1000000000\n\nmem value=2.0 2000000000\n# end";
        let pts = parse_line_protocol(input).unwrap();
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0].measurement, "cpu");
        assert_eq!(pts[1].measurement, "mem");
    }

    #[test]
    fn parse_boolean_variants() {
        let input = "s v1=t,v2=F,v3=TRUE,v4=False 1000000000000";
        let pts = parse_line_protocol(input).unwrap();
        let f = &pts[0].point.fields;
        assert_eq!(f["v1"], "true");
        assert_eq!(f["v2"], "false");
        assert_eq!(f["v3"], "true");
        assert_eq!(f["v4"], "false");
    }

    #[test]
    fn parse_error_no_fields() {
        let result = parse_line_protocol("cpu,host=a");
        assert!(result.is_err());
    }

    #[test]
    fn parse_error_empty_measurement() {
        let result = parse_line_protocol(",host=a value=1.0");
        assert!(result.is_err());
    }
}
