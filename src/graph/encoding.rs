/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph 引擎节点/边的二进制编解码。
//!
//! 编码格式紧凑，避免 JSON 开销。属性使用 length-prefixed string pairs。

use std::collections::BTreeMap;

/// 图节点。
#[derive(Debug, Clone, PartialEq)]
pub struct Vertex {
    /// 节点 ID（自增）。
    pub id: u64,
    /// 节点标签（类型）。
    pub label: String,
    /// 属性键值对。
    pub properties: BTreeMap<String, String>,
}

/// 图边。
#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    /// 边 ID（自增）。
    pub id: u64,
    /// 起点节点 ID。
    pub from: u64,
    /// 终点节点 ID。
    pub to: u64,
    /// 边标签（关系类型）。
    pub label: String,
    /// 属性键值对。
    pub properties: BTreeMap<String, String>,
}

/// 遍历方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// 出边方向。
    Out,
    /// 入边方向。
    In,
    /// 双向。
    Both,
}

// ---- 二进制编码 ----
// Vertex: [label_len:u16][label bytes][prop_count:u16][for each: key_len:u16][key][val_len:u16][val]]
// Edge:   [from:u64BE][to:u64BE][label_len:u16][label bytes][prop_count:u16][...props]

/// 将 Vertex 编码为二进制（不含 id，id 作为 key）。
pub(crate) fn encode_vertex(v: &Vertex) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_str(&mut buf, &v.label);
    write_props(&mut buf, &v.properties);
    buf
}

/// 从二进制解码 Vertex。
pub(crate) fn decode_vertex(id: u64, data: &[u8]) -> Option<Vertex> {
    let mut pos = 0;
    let label = read_str(data, &mut pos)?;
    let properties = read_props(data, &mut pos)?;
    Some(Vertex {
        id,
        label,
        properties,
    })
}

/// 将 Edge 编码为二进制（不含 id）。
pub(crate) fn encode_edge(e: &Edge) -> Vec<u8> {
    let mut buf = Vec::with_capacity(80);
    buf.extend_from_slice(&e.from.to_be_bytes());
    buf.extend_from_slice(&e.to.to_be_bytes());
    write_str(&mut buf, &e.label);
    write_props(&mut buf, &e.properties);
    buf
}

/// 从二进制解码 Edge。
pub(crate) fn decode_edge(id: u64, data: &[u8]) -> Option<Edge> {
    if data.len() < 16 {
        return None;
    }
    let from = u64::from_be_bytes(data[0..8].try_into().ok()?);
    let to = u64::from_be_bytes(data[8..16].try_into().ok()?);
    let mut pos = 16;
    let label = read_str(data, &mut pos)?;
    let properties = read_props(data, &mut pos)?;
    Some(Edge {
        id,
        from,
        to,
        label,
        properties,
    })
}

/// u64 → 8 字节大端序 key。
pub(crate) fn id_to_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

/// 8 字节大端序 key → u64。
pub(crate) fn key_to_id(key: &[u8]) -> Option<u64> {
    if key.len() < 8 {
        return None;
    }
    Some(u64::from_be_bytes(key[..8].try_into().ok()?))
}

/// 出边/入边索引 key: [vertex_id:8][edge_id:8]。
pub(crate) fn adj_key(vertex_id: u64, edge_id: u64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&vertex_id.to_be_bytes());
    buf[8..].copy_from_slice(&edge_id.to_be_bytes());
    buf
}

/// 标签索引 key: "v:{label}:{id BE8}" 或 "e:{label}:{id BE8}"。
pub(crate) fn label_key(prefix: &str, label: &str, id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 1 + label.len() + 1 + 8);
    key.extend_from_slice(prefix.as_bytes());
    key.push(b':');
    key.extend_from_slice(label.as_bytes());
    key.push(b':');
    key.extend_from_slice(&id.to_be_bytes());
    key
}

/// 标签索引前缀: "v:{label}:" 或 "e:{label}:"。
pub(crate) fn label_prefix(prefix: &str, label: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 1 + label.len() + 1);
    key.extend_from_slice(prefix.as_bytes());
    key.push(b':');
    key.extend_from_slice(label.as_bytes());
    key.push(b':');
    key
}

// ---- 内部辅助 ----

fn write_str(buf: &mut Vec<u8>, s: &str) {
    // 截断到 u16::MAX 防止 as u16 静默溢出
    let len = s.len().min(u16::MAX as usize) as u16;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&s.as_bytes()[..len as usize]);
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() {
        return None;
    }
    let len = u16::from_be_bytes(data[*pos..*pos + 2].try_into().ok()?) as usize;
    *pos += 2;
    if *pos + len > data.len() {
        return None;
    }
    let s = String::from_utf8(data[*pos..*pos + len].to_vec()).ok()?;
    *pos += len;
    Some(s)
}

fn write_props(buf: &mut Vec<u8>, props: &BTreeMap<String, String>) {
    let count = props.len() as u16;
    buf.extend_from_slice(&count.to_be_bytes());
    for (k, v) in props {
        write_str(buf, k);
        write_str(buf, v);
    }
}

fn read_props(data: &[u8], pos: &mut usize) -> Option<BTreeMap<String, String>> {
    if *pos + 2 > data.len() {
        return None;
    }
    let count = u16::from_be_bytes(data[*pos..*pos + 2].try_into().ok()?) as usize;
    *pos += 2;
    let mut map = BTreeMap::new();
    for _ in 0..count {
        let k = read_str(data, pos)?;
        let v = read_str(data, pos)?;
        map.insert(k, v);
    }
    Some(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vertex_roundtrip() {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), "Alice".to_string());
        props.insert("age".to_string(), "30".to_string());
        let v = Vertex {
            id: 42,
            label: "Person".to_string(),
            properties: props,
        };
        let encoded = encode_vertex(&v);
        let decoded = decode_vertex(42, &encoded).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn edge_roundtrip() {
        let mut props = BTreeMap::new();
        props.insert("weight".to_string(), "0.8".to_string());
        let e = Edge {
            id: 7,
            from: 1,
            to: 2,
            label: "knows".to_string(),
            properties: props,
        };
        let encoded = encode_edge(&e);
        let decoded = decode_edge(7, &encoded).unwrap();
        assert_eq!(e, decoded);
    }

    #[test]
    fn empty_props_roundtrip() {
        let v = Vertex {
            id: 1,
            label: "X".to_string(),
            properties: BTreeMap::new(),
        };
        let decoded = decode_vertex(1, &encode_vertex(&v)).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn id_key_roundtrip() {
        for id in [0u64, 1, 255, 65535, u64::MAX] {
            assert_eq!(key_to_id(&id_to_key(id)).unwrap(), id);
        }
    }

    #[test]
    fn adj_key_format() {
        let key = adj_key(100, 200);
        assert_eq!(key_to_id(&key[..8]).unwrap(), 100);
        assert_eq!(key_to_id(&key[8..]).unwrap(), 200);
    }
}
