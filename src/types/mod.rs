/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 类型系统：Value、Schema、列类型（INTEGER/FLOAT/TEXT/BLOB/BOOLEAN/JSONB/VECTOR/TIMESTAMP）。
//!
//! M81：行编码从 JSON 切换为紧凑二进制格式，聚合/扫描性能提升 5-20x。
//! 向后兼容：decode_row 自动检测 JSON vs 二进制。
pub mod row_codec;
use crate::Error;
use serde::{Deserialize, Serialize};

/// v2 规格约定的向量维度上限。
pub const VECTOR_DIM_MAX: usize = 4096;
/// `DEFAULT NOW()` 的 sentinel 值：INSERT 时动态替换为当前时间戳。
pub const TIMESTAMP_NOW_SENTINEL: i64 = i64::MIN;
/// 解析动态默认值：将 sentinel 替换为实际值（如 NOW() → 当前时间戳）。
pub fn resolve_default(value: &Value) -> Value {
    if let Value::Timestamp(ts) = value {
        if *ts == TIMESTAMP_NOW_SENTINEL {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            return Value::Timestamp(now);
        }
    }
    value.clone()
}

/// 单值类型（v2 规格：INTEGER/FLOAT/TEXT/BLOB/BOOLEAN/JSONB/VECTOR/TIMESTAMP/GEOPOINT）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Value {
    #[default]
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Jsonb(serde_json::Value),
    /// 向量；维度由 Schema 约束，单维上限见 `VECTOR_DIM_MAX`。
    Vector(Vec<f32>),
    /// 时间戳；语义由使用方约定（通常为自纪元以来的毫秒或微秒）。
    Timestamp(i64),
    /// M94：地理坐标 (latitude, longitude)，WGS-84。
    GeoPoint(f64, f64),
    /// 日期类型：自 Unix 纪元 (1970-01-01) 以来的天数（i32，可表示负值即纪元前）。
    /// 对标 Apache DataFusion Date32 / SQL DATE。
    Date(i32),
    /// 时间类型：自午夜以来的纳秒数（i64，0..86_399_999_999_999）。
    /// 对标 Apache DataFusion Time64(Nanosecond) / SQL TIME。
    Time(i64),
    /// 参数化查询占位符 `?`，存储 0-based 参数索引。
    /// 解析阶段生成，执行前必须通过 `bind_params` 替换为实际值。
    #[serde(skip)]
    Placeholder(usize),
}
impl Value {
    /// 序列化为字节（当前为 JSON，与 JSONB 互通；后续可增其他格式）。
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// 从字节反序列化；失败返回 `Error::Serialization`。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }
}
/// 列类型定义（与 v2 规格一一对应）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
    Boolean,
    Jsonb,
    /// 向量；元组为维度，须在 1..=VECTOR_DIM_MAX。
    Vector(usize),
    Timestamp,
    /// M94：地理坐标类型，存储 (lat, lng) 二元组。
    GeoPoint,
    /// 日期类型（天精度）：自 1970-01-01 以来的天数。
    Date,
    /// 时间类型（纳秒精度）：自午夜以来的纳秒数。
    Time,
}
/// 表/时序等 Schema：列名 + 列类型列表。
///
/// 支持 schema 版本化：每次 ALTER TABLE ADD COLUMN 递增 `version`，
/// 行数据带 `[u16 LE version]` 头，读取时按版本差补齐缺失列，
/// ALTER TABLE 为 O(1) 操作，不碰任何数据行。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<(String, ColumnType)>,
    /// Schema 版本号；CREATE TABLE 时为 0，每次 ALTER ADD COLUMN 递增。
    #[serde(default)]
    pub version: u16,
    /// 每列的默认值；与 columns 等长，None 表示默认 NULL。
    /// CREATE TABLE 时全部为 None，ALTER ADD COLUMN 时记录新列默认值。
    #[serde(default)]
    pub column_defaults: Vec<Option<Value>>,
    /// 每列是否允许 NULL；与 columns 等长，true 表示允许（默认）。
    /// CREATE TABLE 时由 NOT NULL 约束决定。
    #[serde(default)]
    pub column_nullable: Vec<bool>,
    /// 已删除列的下标集合（ALTER TABLE DROP COLUMN 标记删除，O(1) 操作）。
    /// decode_row 时跳过这些列，对外不可见。
    #[serde(default)]
    pub dropped_columns: Vec<usize>,
    /// 复合唯一约束列表：每个元素是一组列名，表示这些列的组合值必须唯一。
    /// 例如 `UNIQUE(session_id, dedup_key)` → `vec![vec!["session_id", "dedup_key"]]`。
    #[serde(default)]
    pub unique_constraints: Vec<Vec<String>>,
    /// 主键是否为 AUTOINCREMENT（仅 INTEGER 主键有效）。
    #[serde(default)]
    pub auto_increment: bool,
    /// CHECK 约束原始 SQL 文本列表（运行时解析为 WhereExpr 校验）。
    #[serde(default)]
    pub check_constraints: Vec<String>,
    /// M127：外键约束列表 (子表列名, 父表名, 父表列名)。
    /// INSERT/UPDATE 子表时检查父表值存在；DELETE/DROP 父表时检查无子表引用。
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// M164：表级注释（COMMENT ON TABLE）。
    #[serde(default)]
    pub table_comment: Option<String>,
    /// M164：列级注释（COMMENT ON COLUMN），与 columns 等长。
    #[serde(default)]
    pub column_comments: Vec<Option<String>>,
}

/// M127：外键约束定义。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyDef {
    /// 子表中的外键列名。
    pub column: String,
    /// 引用的父表名。
    pub ref_table: String,
    /// 引用的父表列名（通常为主键）。
    pub ref_column: String,
}
impl Schema {
    /// 从列名列表构建轻量 Schema（视图 WHERE 匹配用）。
    ///
    /// 所有列类型设为 Text，仅用于列名查找，不用于编解码。
    pub fn from_column_names(names: &[String]) -> Self {
        Schema {
            columns: names
                .iter()
                .map(|n| (n.clone(), ColumnType::Text))
                .collect(),
            ..Default::default()
        }
    }

    /// 按列名查找可见列下标；dropped 列返回 `None`。
    pub fn column_index_by_name(&self, name: &str) -> Option<usize> {
        let mut visible_idx = 0usize;
        for (phys_idx, (n, _)) in self.columns.iter().enumerate() {
            if self.dropped_columns.contains(&phys_idx) {
                continue;
            }
            if n.eq_ignore_ascii_case(name) {
                return Some(visible_idx);
            }
            visible_idx += 1;
        }
        None
    }
    /// 返回可见列列表（排除 dropped 列）。
    pub fn visible_columns(&self) -> Vec<&(String, ColumnType)> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(i, _)| !self.dropped_columns.contains(i))
            .map(|(_, c)| c)
            .collect()
    }
    /// 可见列数量。
    pub fn visible_column_count(&self) -> usize {
        self.columns.len() - self.dropped_columns.len()
    }
    /// 将可见列下标转换为物理列下标。
    pub fn visible_to_physical(&self, visible_idx: usize) -> Option<usize> {
        let mut count = 0usize;
        for (phys, _) in self.columns.iter().enumerate() {
            if self.dropped_columns.contains(&phys) {
                continue;
            }
            if count == visible_idx {
                return Some(phys);
            }
            count += 1;
        }
        None
    }
    /// 隐式类型转换：将 Text 值自动转换为目标列类型。
    ///
    /// row 按可见列顺序排列，内部映射到物理列类型。
    pub fn coerce_types(&self, row: &mut [Value]) {
        for (vis_idx, val) in row.iter_mut().enumerate() {
            let phys_idx = match self.visible_to_physical(vis_idx) {
                Some(p) => p,
                None => break,
            };
            let col_ty = &self.columns[phys_idx].1;
            // Integer → Date/Time 隐式转换
            if let Value::Integer(n) = val {
                match col_ty {
                    ColumnType::Date => {
                        *val = Value::Date(*n as i32);
                        continue;
                    }
                    ColumnType::Time => {
                        *val = Value::Time(*n);
                        continue;
                    }
                    ColumnType::Timestamp => {
                        *val = Value::Timestamp(*n);
                        continue;
                    }
                    _ => {}
                }
            }
            if let Value::Text(ref s) = val {
                match col_ty {
                    ColumnType::Jsonb => {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
                            *val = Value::Jsonb(json);
                        }
                    }
                    ColumnType::Integer => {
                        if let Ok(n) = s.parse::<i64>() {
                            *val = Value::Integer(n);
                        }
                    }
                    ColumnType::Float => {
                        if let Ok(n) = s.parse::<f64>() {
                            *val = Value::Float(n);
                        }
                    }
                    ColumnType::Boolean => {
                        if s.eq_ignore_ascii_case("true") {
                            *val = Value::Boolean(true);
                        } else if s.eq_ignore_ascii_case("false") {
                            *val = Value::Boolean(false);
                        }
                    }
                    ColumnType::Timestamp => {
                        if let Ok(n) = s.parse::<i64>() {
                            *val = Value::Timestamp(n);
                        }
                    }
                    ColumnType::Date => {
                        if let Ok(d) = s.parse::<i32>() {
                            *val = Value::Date(d);
                        } else if let Some(d) = parse_date_string(s) {
                            *val = Value::Date(d);
                        }
                    }
                    ColumnType::Time => {
                        if let Ok(t) = s.parse::<i64>() {
                            *val = Value::Time(t);
                        } else if let Some(t) = parse_time_string(s) {
                            *val = Value::Time(t);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// 校验一行值是否与 Schema 可见列一致：长度、类型及 VECTOR 维度。
    /// row 按可见列顺序排列。
    pub fn validate_row(&self, row: &[Value]) -> Result<(), Error> {
        let vis_count = self.visible_column_count();
        if row.len() != vis_count {
            return Err(Error::Serialization(format!(
                "row has {} columns but schema expects {}",
                row.len(),
                vis_count
            )));
        }
        for (vis_idx, v) in row.iter().enumerate() {
            let phys_idx = self.visible_to_physical(vis_idx).unwrap();
            let col_ty = &self.columns[phys_idx].1;
            // NOT NULL 约束检查
            if matches!(v, Value::Null) {
                if let Some(&false) = self.column_nullable.get(phys_idx) {
                    return Err(Error::Serialization(format!(
                        "column {} ({}) does not allow NULL",
                        vis_idx, self.columns[phys_idx].0
                    )));
                }
            }
            match (v, col_ty) {
                (Value::Null, _) => {}
                (Value::Integer(_), ColumnType::Integer) => {}
                (Value::Float(_), ColumnType::Float) => {}
                (Value::Text(_), ColumnType::Text) => {}
                (Value::Blob(_), ColumnType::Blob) => {}
                (Value::Boolean(_), ColumnType::Boolean) => {}
                (Value::Jsonb(_), ColumnType::Jsonb) => {}
                (Value::Vector(vec), ColumnType::Vector(dim)) => {
                    if vec.len() != *dim {
                        return Err(Error::Serialization(format!(
                            "column {} vector dim {} does not match schema dim {}",
                            vis_idx,
                            vec.len(),
                            dim
                        )));
                    }
                    if *dim == 0 || *dim > VECTOR_DIM_MAX {
                        return Err(Error::Serialization(format!(
                            "column {} vector dim {} not in 1..={}",
                            vis_idx, dim, VECTOR_DIM_MAX
                        )));
                    }
                }
                (Value::Timestamp(_), ColumnType::Timestamp) => {}
                (Value::GeoPoint(lat, lng), ColumnType::GeoPoint) => {
                    if !(-90.0..=90.0).contains(lat) || !(-180.0..=180.0).contains(lng) {
                        return Err(Error::Serialization(format!(
                            "column {} GeoPoint out of range: lat={}, lng={}",
                            vis_idx, lat, lng
                        )));
                    }
                }
                (Value::Date(_), ColumnType::Date) => {}
                (Value::Time(t), ColumnType::Time) => {
                    if *t < 0 || *t > 86_399_999_999_999 {
                        return Err(Error::Serialization(format!(
                            "column {} Time out of range: {}",
                            vis_idx, t
                        )));
                    }
                }
                _ => {
                    return Err(Error::Serialization(format!(
                        "列 {} 类型与 schema 不匹配",
                        vis_idx
                    )));
                }
            }
        }
        Ok(())
    }

    /// 确保 `column_defaults` 和 `column_nullable` 与 `columns` 等长
    /// （兼容旧 schema 无此字段的情况）。
    pub fn ensure_defaults(&mut self) {
        while self.column_defaults.len() < self.columns.len() {
            self.column_defaults.push(None);
        }
        while self.column_nullable.len() < self.columns.len() {
            self.column_nullable.push(true); // 旧列默认允许 NULL
        }
    }

    /// M81：编码行数据：`[schema_version: u16 LE][binary payload]`。
    /// M83：dropped_columns 为空时直接编码借用切片，避免克隆。
    pub fn encode_row(&self, row: &[Value]) -> Result<Vec<u8>, Error> {
        let payload = if self.dropped_columns.is_empty() {
            row_codec::encode_row_binary(row)
        } else {
            row_codec::encode_row_binary(&self.to_physical_row(row))
        };
        let mut buf = Vec::with_capacity(2 + payload.len());
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// 可见列 → 物理列（dropped 列位置填 Null）。
    fn to_physical_row(&self, row: &[Value]) -> Vec<Value> {
        let mut phys = Vec::with_capacity(self.columns.len());
        let mut vi = 0usize;
        for pi in 0..self.columns.len() {
            if self.dropped_columns.contains(&pi) {
                phys.push(Value::Null);
            } else {
                phys.push(row.get(vi).cloned().unwrap_or(Value::Null));
                vi += 1;
            }
        }
        phys
    }

    /// M93：稀疏列解码——只解码 targets 中指定的列，其余跳过。JSON 旧格式回退全解码。
    pub fn decode_columns_sparse(
        &self,
        raw: &[u8],
        targets: &[usize],
    ) -> Result<Vec<Value>, Error> {
        if raw.len() < 2 {
            return Err(Error::Serialization("行数据过短".into()));
        }
        let payload = &raw[2..];
        if row_codec::is_json_payload(payload) {
            let row = self.decode_row(raw)?;
            return Ok(targets
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect());
        }
        row_codec::decode_columns_sparse(payload, targets)
    }

    /// M81：解码行数据。自动检测 JSON vs 二进制格式，向后兼容。
    pub fn decode_row(&self, raw: &[u8]) -> Result<Vec<Value>, Error> {
        if raw.len() < 2 {
            return Err(Error::Serialization("行数据过短".into()));
        }
        let _row_version = u16::from_le_bytes([raw[0], raw[1]]);
        let payload = &raw[2..];
        let mut row = if row_codec::is_json_payload(payload) {
            // 向后兼容：旧 JSON 格式
            serde_json::from_slice(payload).map_err(|e| Error::Serialization(e.to_string()))?
        } else {
            row_codec::decode_row_binary(payload)?
        };
        // 按版本差补齐缺失列
        for i in row.len()..self.columns.len() {
            let d = self.column_defaults.get(i).and_then(|d| d.as_ref());
            row.push(d.map(resolve_default).unwrap_or(Value::Null));
        }
        // 过滤 dropped 列
        if self.dropped_columns.is_empty() {
            Ok(row)
        } else {
            Ok(row
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !self.dropped_columns.contains(i))
                .map(|(_, v)| v)
                .collect())
        }
    }
}

/// 解析 `YYYY-MM-DD` 格式的日期字符串为自 1970-01-01 以来的天数。
pub fn parse_date_string(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&m) || d == 0 {
        return None;
    }
    let max_day = days_in_month(y, m);
    if d > max_day {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

/// 解析 `HH:MM:SS` 或 `HH:MM:SS.nnn` 格式的时间字符串为自午夜以来的纳秒数。
pub fn parse_time_string(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }
    let h: i64 = parts[0].parse().ok()?;
    let min: i64 = parts[1].parse().ok()?;
    if !(0..24).contains(&h) || !(0..60).contains(&min) {
        return None;
    }
    let (sec, nanos) = if parts.len() == 3 {
        let sec_parts: Vec<&str> = parts[2].split('.').collect();
        let s: i64 = sec_parts[0].parse().ok()?;
        let ns = if sec_parts.len() == 2 {
            let frac = sec_parts[1];
            if !frac.bytes().all(|b| b.is_ascii_digit()) || frac.is_empty() {
                return None;
            }
            let truncated = if frac.len() > 9 { &frac[..9] } else { frac };
            let padded = format!("{:0<9}", truncated);
            padded.parse::<i64>().ok()?
        } else {
            0
        };
        (s, ns)
    } else {
        (0, 0)
    };
    if !(0..60).contains(&sec) {
        return None;
    }
    Some(h * 3_600_000_000_000 + min * 60_000_000_000 + sec * 1_000_000_000 + nanos)
}

/// 将日期 (year, month, day) 转换为自 1970-01-01 以来的天数。
/// 算法来源：Howard Hinnant 的 civil_from_days / days_from_civil。
fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m_adj = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * m_adj as u32 + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i64 - 719468) as i32
}

/// 返回指定年月的天数（考虑闰年）。
fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// 将自 1970-01-01 以来的天数转换为 `YYYY-MM-DD` 字符串。
pub fn date_to_string(days: i32) -> String {
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// 将自午夜以来的纳秒数转换为 `HH:MM:SS.nnnnnnnnn` 字符串。
pub fn time_to_string(nanos: i64) -> String {
    let h = nanos / 3_600_000_000_000;
    let rem = nanos % 3_600_000_000_000;
    let m = rem / 60_000_000_000;
    let rem = rem % 60_000_000_000;
    let s = rem / 1_000_000_000;
    let ns = rem % 1_000_000_000;
    if ns == 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}:{:02}.{:09}", h, m, s, ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_roundtrip_all_types() {
        for v in [
            Value::Null,
            Value::Integer(42),
            Value::Float(3.15),
            Value::Text("hello".into()),
            Value::Boolean(true),
            Value::Timestamp(1708934400000),
        ] {
            assert_eq!(Value::from_bytes(&v.to_bytes().unwrap()).unwrap(), v);
        }
        let blob = Value::Blob(vec![0u8, 1, 2]);
        assert_eq!(Value::from_bytes(&blob.to_bytes().unwrap()).unwrap(), blob);
        let j = Value::Jsonb(serde_json::json!({"a":1}));
        assert_eq!(Value::from_bytes(&j.to_bytes().unwrap()).unwrap(), j);
        let vec = Value::Vector(vec![0.1, 0.2]);
        assert_eq!(Value::from_bytes(&vec.to_bytes().unwrap()).unwrap(), vec);
    }

    #[test]
    fn schema_column_index_by_name() {
        let s = Schema {
            columns: vec![
                ("a".to_string(), ColumnType::Integer),
                ("b".to_string(), ColumnType::Text),
            ],
            ..Default::default()
        };
        assert_eq!(s.column_index_by_name("a"), Some(0));
        assert_eq!(s.column_index_by_name("b"), Some(1));
        assert_eq!(s.column_index_by_name("c"), None);
    }

    #[test]
    fn schema_validate_row_ok() {
        let s = Schema {
            columns: vec![
                ("id".to_string(), ColumnType::Integer),
                ("vec".to_string(), ColumnType::Vector(2)),
            ],
            ..Default::default()
        };
        let row = vec![Value::Integer(1), Value::Vector(vec![0.0, 1.0])];
        assert!(s.validate_row(&row).is_ok());
    }

    #[test]
    fn schema_validate_row_wrong_len() {
        let s = Schema {
            columns: vec![("a".to_string(), ColumnType::Integer)],
            ..Default::default()
        };
        let row = vec![Value::Integer(1), Value::Integer(2)];
        assert!(s.validate_row(&row).is_err());
    }

    #[test]
    fn schema_validate_row_wrong_vector_dim() {
        let s = Schema {
            columns: vec![("v".to_string(), ColumnType::Vector(2))],
            ..Default::default()
        };
        let row = vec![Value::Vector(vec![0.0, 1.0, 2.0])];
        assert!(s.validate_row(&row).is_err());
    }

    #[test]
    fn schema_validate_row_type_mismatch() {
        let s = Schema {
            columns: vec![("a".to_string(), ColumnType::Integer)],
            ..Default::default()
        };
        let row = vec![Value::Text("x".to_string())];
        assert!(s.validate_row(&row).is_err());
    }

    #[test]
    fn schema_encode_decode_roundtrip() {
        let s = Schema {
            columns: vec![
                ("id".to_string(), ColumnType::Integer),
                ("name".to_string(), ColumnType::Text),
            ],
            version: 0,
            column_defaults: vec![None, None],
            column_nullable: vec![true, true],
            dropped_columns: vec![],
            unique_constraints: vec![],
            auto_increment: false,
            check_constraints: vec![],
            foreign_keys: vec![],
            table_comment: None,
            column_comments: vec![],
        };
        let row = vec![Value::Integer(1), Value::Text("alice".into())];
        let raw = s.encode_row(&row).unwrap();
        // 前 2 字节是版本号
        assert_eq!(raw[0], 0);
        assert_eq!(raw[1], 0);
        let decoded = s.decode_row(&raw).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn schema_decode_pads_missing_columns() {
        // 模拟 ALTER TABLE ADD COLUMN 后读取旧行
        let v0 = Schema {
            columns: vec![
                ("id".to_string(), ColumnType::Integer),
                ("name".to_string(), ColumnType::Text),
            ],
            version: 0,
            column_defaults: vec![None, None],
            column_nullable: vec![true, true],
            dropped_columns: vec![],
            unique_constraints: vec![],
            auto_increment: false,
            check_constraints: vec![],
            foreign_keys: vec![],
            table_comment: None,
            column_comments: vec![],
        };
        let row = vec![Value::Integer(1), Value::Text("alice".into())];
        let raw = v0.encode_row(&row).unwrap();

        // v1 schema 多了一列 age，默认值 18
        let v1 = Schema {
            columns: vec![
                ("id".to_string(), ColumnType::Integer),
                ("name".to_string(), ColumnType::Text),
                ("age".to_string(), ColumnType::Integer),
            ],
            version: 1,
            column_defaults: vec![None, None, Some(Value::Integer(18))],
            column_nullable: vec![true, true, true],
            dropped_columns: vec![],
            unique_constraints: vec![],
            auto_increment: false,
            check_constraints: vec![],
            foreign_keys: vec![],
            table_comment: None,
            column_comments: vec![],
        };
        let decoded = v1.decode_row(&raw).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[2], Value::Integer(18));
    }

    #[test]
    fn schema_decode_pads_null_when_no_default() {
        let v0 = Schema {
            columns: vec![("id".to_string(), ColumnType::Integer)],
            version: 0,
            column_defaults: vec![None],
            column_nullable: vec![true],
            dropped_columns: vec![],
            unique_constraints: vec![],
            auto_increment: false,
            check_constraints: vec![],
            foreign_keys: vec![],
            table_comment: None,
            column_comments: vec![],
        };
        let raw = v0.encode_row(&[Value::Integer(1)]).unwrap();

        let v1 = Schema {
            columns: vec![
                ("id".to_string(), ColumnType::Integer),
                ("extra".to_string(), ColumnType::Text),
            ],
            version: 1,
            column_defaults: vec![None, None],
            column_nullable: vec![true, true],
            dropped_columns: vec![],
            unique_constraints: vec![],
            auto_increment: false,
            check_constraints: vec![],
            foreign_keys: vec![],
            table_comment: None,
            column_comments: vec![],
        };
        let decoded = v1.decode_row(&raw).unwrap();
        assert_eq!(decoded[1], Value::Null);
    }

    #[test]
    fn value_roundtrip_date_time() {
        let date = Value::Date(19783); // 2024-03-01
        assert_eq!(Value::from_bytes(&date.to_bytes().unwrap()).unwrap(), date);
        let time = Value::Time(43200_000_000_000); // 12:00:00
        assert_eq!(Value::from_bytes(&time.to_bytes().unwrap()).unwrap(), time);
    }

    #[test]
    fn schema_validate_date_time_ok() {
        let s = Schema {
            columns: vec![
                ("d".to_string(), ColumnType::Date),
                ("t".to_string(), ColumnType::Time),
            ],
            ..Default::default()
        };
        let row = vec![Value::Date(19783), Value::Time(43200_000_000_000)];
        assert!(s.validate_row(&row).is_ok());
    }

    #[test]
    fn schema_validate_time_out_of_range() {
        let s = Schema {
            columns: vec![("t".to_string(), ColumnType::Time)],
            ..Default::default()
        };
        // 超过一天的纳秒数
        let row = vec![Value::Time(86_400_000_000_000)];
        assert!(s.validate_row(&row).is_err());
        // 负值
        let row2 = vec![Value::Time(-1)];
        assert!(s.validate_row(&row2).is_err());
    }

    #[test]
    fn schema_encode_decode_date_time_roundtrip() {
        let s = Schema {
            columns: vec![
                ("d".to_string(), ColumnType::Date),
                ("t".to_string(), ColumnType::Time),
            ],
            version: 0,
            column_defaults: vec![None, None],
            column_nullable: vec![true, true],
            dropped_columns: vec![],
            unique_constraints: vec![],
            auto_increment: false,
            check_constraints: vec![],
            foreign_keys: vec![],
            table_comment: None,
            column_comments: vec![],
        };
        let row = vec![Value::Date(19783), Value::Time(43200_000_000_000)];
        let raw = s.encode_row(&row).unwrap();
        let decoded = s.decode_row(&raw).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn schema_coerce_date_time() {
        let mut s = Schema {
            columns: vec![
                ("d".to_string(), ColumnType::Date),
                ("t".to_string(), ColumnType::Time),
            ],
            ..Default::default()
        };
        s.ensure_defaults();
        let mut row = vec![
            Value::Text("2024-03-01".into()),
            Value::Text("12:30:45".into()),
        ];
        s.coerce_types(&mut row);
        assert!(matches!(row[0], Value::Date(_)));
        assert!(matches!(row[1], Value::Time(_)));
    }

    #[test]
    fn parse_date_string_works() {
        assert_eq!(parse_date_string("1970-01-01"), Some(0));
        assert_eq!(parse_date_string("2024-03-01"), Some(19783));
        assert!(parse_date_string("invalid").is_none());
        assert!(parse_date_string("2024-13-01").is_none());
        // 日历感知验证：无效日期应被拒绝
        assert!(parse_date_string("2024-02-30").is_none()); // 2月无30日
        assert!(parse_date_string("2023-02-29").is_none()); // 2023 非闰年
        assert_eq!(parse_date_string("2024-02-29").is_some(), true); // 2024 闰年
        assert!(parse_date_string("2024-04-31").is_none()); // 4月无31日
    }

    #[test]
    fn parse_time_string_works() {
        assert_eq!(parse_time_string("00:00:00"), Some(0));
        assert_eq!(
            parse_time_string("12:30:45"),
            Some(12 * 3_600_000_000_000 + 30 * 60_000_000_000 + 45 * 1_000_000_000)
        );
        assert!(parse_time_string("invalid").is_none());
        assert!(parse_time_string("24:00:00").is_none());
    }

    #[test]
    fn date_to_string_roundtrip() {
        assert_eq!(date_to_string(0), "1970-01-01");
        let days = parse_date_string("2024-03-01").unwrap();
        assert_eq!(date_to_string(days), "2024-03-01");
        // 纪元前日期
        let neg = parse_date_string("1969-12-31").unwrap();
        assert_eq!(neg, -1);
        assert_eq!(date_to_string(neg), "1969-12-31");
    }

    #[test]
    fn time_to_string_roundtrip() {
        assert_eq!(time_to_string(0), "00:00:00");
        let nanos = parse_time_string("12:30:45").unwrap();
        assert_eq!(time_to_string(nanos), "12:30:45");
        let nanos_frac = parse_time_string("01:02:03.456000000").unwrap();
        assert_eq!(time_to_string(nanos_frac), "01:02:03.456000000");
    }
}
