/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 统一操作类型 — 覆盖五模引擎所有写操作。
//!
//! OpLog 记录的最小单元，用于主从复制时在从节点回放。
//! SQL 写操作记录为行级变更（非原始 SQL），避免非确定性函数问题。

use crate::types::Value;

// ── M111：二进制编码辅助函数 ─────────────────────────────

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}
fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_bytes(buf, s.as_bytes());
}

fn read_bytes(d: &[u8]) -> Result<(Vec<u8>, &[u8]), crate::Error> {
    if d.len() < 4 {
        return Err(crate::Error::Serialization("Operation 数据不足".into()));
    }
    let len = u32::from_le_bytes(d[..4].try_into().unwrap()) as usize;
    if d.len() < 4 + len {
        return Err(crate::Error::Serialization("Operation 数据截断".into()));
    }
    Ok((d[4..4 + len].to_vec(), &d[4 + len..]))
}
fn read_string(d: &[u8]) -> Result<(String, &[u8]), crate::Error> {
    let (bytes, rest) = read_bytes(d)?;
    Ok((String::from_utf8_lossy(&bytes).into_owned(), rest))
}
fn read_u64(d: &[u8]) -> Result<(u64, &[u8]), crate::Error> {
    if d.len() < 8 {
        return Err(crate::Error::Serialization("Operation u64 数据不足".into()));
    }
    Ok((u64::from_le_bytes(d[..8].try_into().unwrap()), &d[8..]))
}
fn read_i64(d: &[u8]) -> Result<(i64, &[u8]), crate::Error> {
    if d.len() < 8 {
        return Err(crate::Error::Serialization("Operation i64 数据不足".into()));
    }
    Ok((i64::from_le_bytes(d[..8].try_into().unwrap()), &d[8..]))
}
fn read_u32(d: &[u8]) -> Result<(u32, &[u8]), crate::Error> {
    if d.len() < 4 {
        return Err(crate::Error::Serialization("Operation u32 数据不足".into()));
    }
    Ok((u32::from_le_bytes(d[..4].try_into().unwrap()), &d[4..]))
}

/// 统一操作类型 — 描述一次写操作，覆盖 KV / SQL / TS / MQ / Vector / AI 六模。
///
/// 设计原则：
/// - SQL 写操作记录为行级变更，不记录原始 SQL（避免 NOW() 等非确定性问题）
/// - DDL 操作记录原始 SQL（DDL 是确定性的）
/// - 二进制数据（向量、MQ payload）直接记录字节
#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    // ── KV ──────────────────────────────────────────────
    /// KV SET（含可选 TTL）。
    KvSet {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    },
    /// KV DEL。
    KvDel { key: Vec<u8> },
    /// KV INCR（记录增量后的结果值，保证幂等）。
    KvIncr { key: Vec<u8>, new_value: i64 },
    /// KV EXPIRE（设置 TTL）。
    KvExpire { key: Vec<u8>, secs: u64 },

    // ── SQL（行级变更）──────────────────────────────────
    /// SQL INSERT — 记录具体行数据。
    SqlInsert {
        table: String,
        row: Vec<(String, Value)>,
    },
    /// SQL UPDATE — 记录主键列名 + 主键值 + 变更列。
    SqlUpdate {
        table: String,
        pk_column: String,
        pk: Value,
        changes: Vec<(String, Value)>,
    },
    /// SQL DELETE — 记录主键列名 + 主键值。
    SqlDelete {
        table: String,
        pk_column: String,
        pk: Value,
    },
    /// SQL DDL（CREATE TABLE / DROP TABLE / ALTER TABLE / CREATE INDEX 等）。
    SqlDdl { sql: String },

    // ── TimeSeries ──────────────────────────────────────
    /// 创建时序表。
    TsCreate {
        series: String,
        /// 序列化后的 TsSchema 字节。
        schema_data: Vec<u8>,
    },
    /// 时序数据写入。
    TsInsert {
        series: String,
        /// 序列化后的 DataPoint 字节。
        point_data: Vec<u8>,
    },
    /// 删除时序表。
    TsDrop { series: String },

    // ── MQ ──────────────────────────────────────────────
    /// MQ 创建 topic。
    MqCreate { topic: String, max_len: Option<u64> },
    /// MQ 发布消息。
    MqPublish { topic: String, payload: Vec<u8> },
    /// MQ 确认消费。
    MqAck {
        topic: String,
        group: String,
        msg_id: String,
    },
    /// MQ 删除 topic。
    MqDrop { topic: String },
    /// MQ 订阅消费者组。
    MqSubscribe { topic: String, group: String },
    /// MQ 取消订阅消费者组。
    MqUnsubscribe { topic: String, group: String },

    // ── Vector ──────────────────────────────────────────
    /// 向量插入。
    VecInsert {
        collection: String,
        id: u64,
        /// 序列化后的向量字节（f32 数组）。
        vector_data: Vec<u8>,
    },
    /// 向量删除。
    VecDelete { collection: String, id: u64 },

    // ── AI ──────────────────────────────────────────────
    /// AI 引擎操作（Session/Context/Memory/Trace 等）。
    /// sub_type 标识具体操作，payload 为序列化后的参数。
    AiOp { sub_type: String, payload: Vec<u8> },
}

impl Operation {
    /// 返回操作所属的引擎模块名（用于日志和监控）。
    pub fn module(&self) -> &'static str {
        match self {
            Self::KvSet { .. }
            | Self::KvDel { .. }
            | Self::KvIncr { .. }
            | Self::KvExpire { .. } => "kv",
            Self::SqlInsert { .. }
            | Self::SqlUpdate { .. }
            | Self::SqlDelete { .. }
            | Self::SqlDdl { .. } => "sql",
            Self::TsCreate { .. } | Self::TsInsert { .. } | Self::TsDrop { .. } => "ts",
            Self::MqCreate { .. }
            | Self::MqPublish { .. }
            | Self::MqAck { .. }
            | Self::MqDrop { .. }
            | Self::MqSubscribe { .. }
            | Self::MqUnsubscribe { .. } => "mq",
            Self::VecInsert { .. } | Self::VecDelete { .. } => "vector",
            Self::AiOp { .. } => "ai",
        }
    }

    /// M111：序列化为二进制字节。
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::Error> {
        let mut buf = Vec::with_capacity(64);
        match self {
            Self::KvSet {
                key,
                value,
                ttl_secs,
            } => {
                buf.push(0x01);
                write_bytes(&mut buf, key);
                write_bytes(&mut buf, value);
                buf.push(ttl_secs.is_some() as u8);
                if let Some(t) = ttl_secs {
                    buf.extend_from_slice(&t.to_le_bytes());
                }
            }
            Self::KvDel { key } => {
                buf.push(0x02);
                write_bytes(&mut buf, key);
            }
            Self::KvIncr { key, new_value } => {
                buf.push(0x03);
                write_bytes(&mut buf, key);
                buf.extend_from_slice(&new_value.to_le_bytes());
            }
            Self::KvExpire { key, secs } => {
                buf.push(0x04);
                write_bytes(&mut buf, key);
                buf.extend_from_slice(&secs.to_le_bytes());
            }
            Self::SqlInsert { table, row } => {
                buf.push(0x10);
                write_str(&mut buf, table);
                buf.extend_from_slice(&(row.len() as u32).to_le_bytes());
                for (col, val) in row {
                    write_str(&mut buf, col);
                    crate::types::row_codec::encode_value(&mut buf, val);
                }
            }
            Self::SqlUpdate {
                table,
                pk_column,
                pk,
                changes,
            } => {
                buf.push(0x11);
                write_str(&mut buf, table);
                write_str(&mut buf, pk_column);
                crate::types::row_codec::encode_value(&mut buf, pk);
                buf.extend_from_slice(&(changes.len() as u32).to_le_bytes());
                for (col, val) in changes {
                    write_str(&mut buf, col);
                    crate::types::row_codec::encode_value(&mut buf, val);
                }
            }
            Self::SqlDelete {
                table,
                pk_column,
                pk,
            } => {
                buf.push(0x12);
                write_str(&mut buf, table);
                write_str(&mut buf, pk_column);
                crate::types::row_codec::encode_value(&mut buf, pk);
            }
            Self::SqlDdl { sql } => {
                buf.push(0x13);
                write_str(&mut buf, sql);
            }
            Self::TsCreate {
                series,
                schema_data,
            } => {
                buf.push(0x20);
                write_str(&mut buf, series);
                write_bytes(&mut buf, schema_data);
            }
            Self::TsInsert { series, point_data } => {
                buf.push(0x21);
                write_str(&mut buf, series);
                write_bytes(&mut buf, point_data);
            }
            Self::TsDrop { series } => {
                buf.push(0x22);
                write_str(&mut buf, series);
            }
            Self::MqCreate { topic, max_len } => {
                buf.push(0x30);
                write_str(&mut buf, topic);
                buf.push(max_len.is_some() as u8);
                if let Some(m) = max_len {
                    buf.extend_from_slice(&m.to_le_bytes());
                }
            }
            Self::MqPublish { topic, payload } => {
                buf.push(0x31);
                write_str(&mut buf, topic);
                write_bytes(&mut buf, payload);
            }
            Self::MqAck {
                topic,
                group,
                msg_id,
            } => {
                buf.push(0x32);
                write_str(&mut buf, topic);
                write_str(&mut buf, group);
                write_str(&mut buf, msg_id);
            }
            Self::MqDrop { topic } => {
                buf.push(0x33);
                write_str(&mut buf, topic);
            }
            Self::MqSubscribe { topic, group } => {
                buf.push(0x34);
                write_str(&mut buf, topic);
                write_str(&mut buf, group);
            }
            Self::MqUnsubscribe { topic, group } => {
                buf.push(0x35);
                write_str(&mut buf, topic);
                write_str(&mut buf, group);
            }
            Self::VecInsert {
                collection,
                id,
                vector_data,
            } => {
                buf.push(0x40);
                write_str(&mut buf, collection);
                buf.extend_from_slice(&id.to_le_bytes());
                write_bytes(&mut buf, vector_data);
            }
            Self::VecDelete { collection, id } => {
                buf.push(0x41);
                write_str(&mut buf, collection);
                buf.extend_from_slice(&id.to_le_bytes());
            }
            Self::AiOp { sub_type, payload } => {
                buf.push(0x50);
                write_str(&mut buf, sub_type);
                write_bytes(&mut buf, payload);
            }
        }
        Ok(buf)
    }

    /// M111：从二进制字节反序列化。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::Error> {
        if bytes.is_empty() {
            return Err(crate::Error::Serialization("Operation 数据为空".into()));
        }
        let tag = bytes[0];
        let d = &bytes[1..];
        match tag {
            0x01 => {
                let (key, d) = read_bytes(d)?;
                let (value, d) = read_bytes(d)?;
                let (has_ttl, d) = (d.first().copied().unwrap_or(0) != 0, &d[1..]);
                let ttl_secs = if has_ttl { Some(read_u64(d)?.0) } else { None };
                Ok(Self::KvSet {
                    key,
                    value,
                    ttl_secs,
                })
            }
            0x02 => {
                let (key, _) = read_bytes(d)?;
                Ok(Self::KvDel { key })
            }
            0x03 => {
                let (key, d) = read_bytes(d)?;
                let (nv, _) = read_i64(d)?;
                Ok(Self::KvIncr { key, new_value: nv })
            }
            0x04 => {
                let (key, d) = read_bytes(d)?;
                let (secs, _) = read_u64(d)?;
                Ok(Self::KvExpire { key, secs })
            }
            0x10 => {
                let (table, d) = read_string(d)?;
                let (cnt, mut d) = read_u32(d)?;
                let mut row = Vec::with_capacity(cnt as usize);
                for _ in 0..cnt {
                    let (col, rest) = read_string(d)?;
                    let (val, consumed) = crate::types::row_codec::decode_value(rest)?;
                    row.push((col, val));
                    d = &rest[consumed..];
                }
                Ok(Self::SqlInsert { table, row })
            }
            0x11 => {
                let (table, d) = read_string(d)?;
                let (pk_column, d) = read_string(d)?;
                let (pk, consumed) = crate::types::row_codec::decode_value(d)?;
                let d = &d[consumed..];
                let (cnt, mut d) = read_u32(d)?;
                let mut changes = Vec::with_capacity(cnt as usize);
                for _ in 0..cnt {
                    let (col, rest) = read_string(d)?;
                    let (val, consumed) = crate::types::row_codec::decode_value(rest)?;
                    changes.push((col, val));
                    d = &rest[consumed..];
                }
                Ok(Self::SqlUpdate {
                    table,
                    pk_column,
                    pk,
                    changes,
                })
            }
            0x12 => {
                let (table, d) = read_string(d)?;
                let (pk_column, d) = read_string(d)?;
                let (pk, _) = crate::types::row_codec::decode_value(d)?;
                Ok(Self::SqlDelete {
                    table,
                    pk_column,
                    pk,
                })
            }
            0x13 => {
                let (sql, _) = read_string(d)?;
                Ok(Self::SqlDdl { sql })
            }
            0x20 => {
                let (series, d) = read_string(d)?;
                let (schema_data, _) = read_bytes(d)?;
                Ok(Self::TsCreate {
                    series,
                    schema_data,
                })
            }
            0x21 => {
                let (series, d) = read_string(d)?;
                let (point_data, _) = read_bytes(d)?;
                Ok(Self::TsInsert { series, point_data })
            }
            0x22 => {
                let (series, _) = read_string(d)?;
                Ok(Self::TsDrop { series })
            }
            0x30 => {
                let (topic, d) = read_string(d)?;
                let has = d.first().copied().unwrap_or(0) != 0;
                let max_len = if has {
                    Some(read_u64(&d[1..])?.0)
                } else {
                    None
                };
                Ok(Self::MqCreate { topic, max_len })
            }
            0x31 => {
                let (topic, d) = read_string(d)?;
                let (payload, _) = read_bytes(d)?;
                Ok(Self::MqPublish { topic, payload })
            }
            0x32 => {
                let (topic, d) = read_string(d)?;
                let (group, d) = read_string(d)?;
                let (msg_id, _) = read_string(d)?;
                Ok(Self::MqAck {
                    topic,
                    group,
                    msg_id,
                })
            }
            0x33 => {
                let (topic, _) = read_string(d)?;
                Ok(Self::MqDrop { topic })
            }
            0x34 => {
                let (topic, d) = read_string(d)?;
                let (group, _) = read_string(d)?;
                Ok(Self::MqSubscribe { topic, group })
            }
            0x35 => {
                let (topic, d) = read_string(d)?;
                let (group, _) = read_string(d)?;
                Ok(Self::MqUnsubscribe { topic, group })
            }
            0x40 => {
                let (collection, d) = read_string(d)?;
                let (id, d) = read_u64(d)?;
                let (vector_data, _) = read_bytes(d)?;
                Ok(Self::VecInsert {
                    collection,
                    id,
                    vector_data,
                })
            }
            0x41 => {
                let (collection, d) = read_string(d)?;
                let (id, _) = read_u64(d)?;
                Ok(Self::VecDelete { collection, id })
            }
            0x50 => {
                let (sub_type, d) = read_string(d)?;
                let (payload, _) = read_bytes(d)?;
                Ok(Self::AiOp { sub_type, payload })
            }
            _ => Err(crate::Error::Serialization(format!(
                "Operation 未知标签: 0x{:02x}",
                tag
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_roundtrip_kv_set() {
        let op = Operation::KvSet {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            ttl_secs: Some(60),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = Operation::from_bytes(&bytes).unwrap();
        assert_eq!(op, decoded);
    }

    #[test]
    fn operation_roundtrip_sql_insert() {
        let op = Operation::SqlInsert {
            table: "users".into(),
            row: vec![
                ("id".into(), Value::Integer(1)),
                ("name".into(), Value::Text("alice".into())),
            ],
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = Operation::from_bytes(&bytes).unwrap();
        assert_eq!(op, decoded);
    }

    #[test]
    fn operation_roundtrip_vec_insert() {
        let vec_data: Vec<u8> = [0.1f32, 0.2, 0.3]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let op = Operation::VecInsert {
            collection: "embeddings".into(),
            id: 42,
            vector_data: vec_data,
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = Operation::from_bytes(&bytes).unwrap();
        assert_eq!(op, decoded);
    }

    #[test]
    fn operation_module_names() {
        assert_eq!(
            Operation::KvSet {
                key: vec![],
                value: vec![],
                ttl_secs: None
            }
            .module(),
            "kv"
        );
        assert_eq!(Operation::SqlDdl { sql: String::new() }.module(), "sql");
        assert_eq!(
            Operation::MqPublish {
                topic: String::new(),
                payload: vec![]
            }
            .module(),
            "mq"
        );
        assert_eq!(
            Operation::VecDelete {
                collection: String::new(),
                id: 0
            }
            .module(),
            "vector"
        );
        assert_eq!(
            Operation::AiOp {
                sub_type: String::new(),
                payload: vec![]
            }
            .module(),
            "ai"
        );
    }
}
