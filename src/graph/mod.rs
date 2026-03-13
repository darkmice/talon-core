/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph 图引擎：属性图模型，支持节点/边 CRUD、出入边索引、标签索引。
//!
//! 存储基于 Talon Keyspace 封装层（底层 fjall LSM-Tree）。
//! 每个图使用 6 个 keyspace：节点、边、出边索引、入边索引、标签索引、元数据。

pub mod encoding;
pub mod traversal;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::Error;
use crate::storage::{Keyspace, Store};

pub use encoding::{Direction, Edge, Vertex};

/// 图引擎实例。
pub struct GraphEngine {
    store: Store,
    /// 图句柄缓存：避免每次操作重复打开 6 个 Keyspace（与 Store::ks_cache 模式一致）。
    handle_cache: RwLock<HashMap<String, GraphHandle>>,
}

/// 单个图的句柄（持有 6 个 Keyspace 引用）。
#[derive(Clone)]
struct GraphHandle {
    vertices: Keyspace,
    edges: Keyspace,
    out_idx: Keyspace,
    in_idx: Keyspace,
    label_idx: Keyspace,
    meta: Keyspace,
}

/// 图元数据。
#[derive(Debug)]
pub(super) struct GraphMeta {
    next_vertex_id: u64,
    next_edge_id: u64,
}

pub(crate) const META_KEY: &[u8] = b"meta";

impl GraphEngine {
    /// 打开图引擎。
    pub fn open(store: &Store) -> Result<Self, Error> {
        Ok(GraphEngine {
            store: store.clone(),
            handle_cache: RwLock::new(HashMap::new()),
        })
    }

    /// 创建一个新的命名图。若图已存在则跳过（幂等）。
    pub fn create(&self, name: &str) -> Result<(), Error> {
        let h = self.open_graph(name)?;
        // 幂等：已存在 meta 则跳过，防止重置 ID 导致数据覆盖
        if h.meta.get(META_KEY)?.is_some() {
            return Ok(());
        }
        let meta = GraphMeta {
            next_vertex_id: 1,
            next_edge_id: 1,
        };
        h.meta.set(META_KEY, encode_meta(&meta))?;
        Ok(())
    }

    /// 添加节点，返回自增 ID。
    pub fn add_vertex(
        &self,
        graph: &str,
        label: &str,
        props: &BTreeMap<String, String>,
    ) -> Result<u64, Error> {
        let h = self.open_graph(graph)?;
        let mut meta = self.load_meta(&h)?;
        let id = meta.next_vertex_id;
        meta.next_vertex_id += 1;

        let v = Vertex {
            id,
            label: label.to_string(),
            properties: props.clone(),
        };
        let mut batch = self.store.batch();
        batch.insert(&h.vertices, encoding::id_to_key(id), encoding::encode_vertex(&v))?;
        batch.insert(&h.label_idx, encoding::label_key("v", label, id), vec![])?;
        batch.insert(&h.meta, META_KEY.to_vec(), encode_meta(&meta).to_vec())?;
        batch.commit()?;
        Ok(id)
    }

    /// 获取节点。
    pub fn get_vertex(&self, graph: &str, id: u64) -> Result<Option<Vertex>, Error> {
        let h = self.open_graph(graph)?;
        match h.vertices.get(encoding::id_to_key(id))? {
            Some(data) => Ok(encoding::decode_vertex(id, &data)),
            None => Ok(None),
        }
    }

    /// 更新节点属性（保留 label）。
    pub fn update_vertex(
        &self,
        graph: &str,
        id: u64,
        props: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        let h = self.open_graph(graph)?;
        let data = h
            .vertices
            .get(encoding::id_to_key(id))?
            .ok_or_else(|| Error::Graph(format!("vertex {} not found", id)))?;
        let mut v = encoding::decode_vertex(id, &data)
            .ok_or_else(|| Error::Serialization("corrupt vertex".into()))?;
        v.properties = props.clone();
        h.vertices
            .set(encoding::id_to_key(id), encoding::encode_vertex(&v))?;
        Ok(())
    }

    /// 删除节点（级联删除关联边）。
    /// Bug 36：vertex+label_idx+级联边全部合入单个 WriteBatch 原子提交。
    pub fn delete_vertex(&self, graph: &str, id: u64) -> Result<(), Error> {
        let h = self.open_graph(graph)?;
        let key = encoding::id_to_key(id);
        let mut batch = self.store.batch();
        // 删除顶点 label 索引
        if let Some(data) = h.vertices.get(key)? {
            if let Some(v) = encoding::decode_vertex(id, &data) {
                batch.remove(&h.label_idx, encoding::label_key("v", &v.label, id));
            }
        }
        // 收集并级联删除出边
        let mut out_eids = Vec::new();
        h.out_idx.for_each_kv_prefix(&id.to_be_bytes(), |k, _v| {
            if k.len() >= 16 {
                if let Some(eid) = encoding::key_to_id(&k[8..]) {
                    out_eids.push(eid);
                }
            }
            true
        })?;
        for eid in &out_eids {
            self.remove_edge_into_batch(&h, *eid, &mut batch)?;
        }
        // 收集并级联删除入边
        let mut in_eids = Vec::new();
        h.in_idx.for_each_kv_prefix(&id.to_be_bytes(), |k, _v| {
            if k.len() >= 16 {
                if let Some(eid) = encoding::key_to_id(&k[8..]) {
                    in_eids.push(eid);
                }
            }
            true
        })?;
        for eid in &in_eids {
            self.remove_edge_into_batch(&h, *eid, &mut batch)?;
        }
        // 删除顶点数据
        batch.remove(&h.vertices, key.to_vec());
        batch.commit()?;
        Ok(())
    }

    /// 添加边，返回自增 ID。
    pub fn add_edge(
        &self,
        graph: &str,
        from: u64,
        to: u64,
        label: &str,
        props: &BTreeMap<String, String>,
    ) -> Result<u64, Error> {
        let h = self.open_graph(graph)?;
        if h.vertices.get(encoding::id_to_key(from))?.is_none() {
            return Err(Error::Graph(format!("source vertex {} not found", from)));
        }
        if h.vertices.get(encoding::id_to_key(to))?.is_none() {
            return Err(Error::Graph(format!("target vertex {} not found", to)));
        }
        let mut meta = self.load_meta(&h)?;
        let id = meta.next_edge_id;
        meta.next_edge_id += 1;

        let e = Edge {
            id,
            from,
            to,
            label: label.to_string(),
            properties: props.clone(),
        };
        let mut batch = self.store.batch();
        batch.insert(&h.edges, encoding::id_to_key(id), encoding::encode_edge(&e))?;
        batch.insert(&h.out_idx, encoding::adj_key(from, id), encoding::id_to_key(to).to_vec())?;
        batch.insert(&h.in_idx, encoding::adj_key(to, id), encoding::id_to_key(from).to_vec())?;
        batch.insert(&h.label_idx, encoding::label_key("e", label, id), vec![])?;
        batch.insert(&h.meta, META_KEY.to_vec(), encode_meta(&meta).to_vec())?;
        batch.commit()?;
        Ok(id)
    }

    /// 获取边。
    pub fn get_edge(&self, graph: &str, id: u64) -> Result<Option<Edge>, Error> {
        let h = self.open_graph(graph)?;
        match h.edges.get(encoding::id_to_key(id))? {
            Some(data) => Ok(encoding::decode_edge(id, &data)),
            None => Ok(None),
        }
    }

    /// 删除边。
    pub fn delete_edge(&self, graph: &str, edge_id: u64) -> Result<(), Error> {
        let h = self.open_graph(graph)?;
        self.remove_edge_internal(&h, edge_id)
    }

    /// 获取节点的出边。
    pub fn out_edges(&self, graph: &str, vertex_id: u64) -> Result<Vec<Edge>, Error> {
        let h = self.open_graph(graph)?;
        self.collect_adj_edges(&h, vertex_id, true)
    }

    /// 获取节点的入边。
    pub fn in_edges(&self, graph: &str, vertex_id: u64) -> Result<Vec<Edge>, Error> {
        let h = self.open_graph(graph)?;
        self.collect_adj_edges(&h, vertex_id, false)
    }

    /// 获取节点的邻居 ID 列表。
    pub fn neighbors(
        &self,
        graph: &str,
        vertex_id: u64,
        direction: Direction,
    ) -> Result<Vec<u64>, Error> {
        let h = self.open_graph(graph)?;
        // Both 模式使用 HashSet 去重，避免 O(N²) 的 Vec::contains
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        if direction == Direction::Out || direction == Direction::Both {
            h.out_idx
                .for_each_kv_prefix(&vertex_id.to_be_bytes(), |_k, v| {
                    if let Some(to) = encoding::key_to_id(v) {
                        if seen.insert(to) {
                            result.push(to);
                        }
                    }
                    true
                })?;
        }
        if direction == Direction::In || direction == Direction::Both {
            h.in_idx
                .for_each_kv_prefix(&vertex_id.to_be_bytes(), |_k, v| {
                    if let Some(from) = encoding::key_to_id(v) {
                        if seen.insert(from) {
                            result.push(from);
                        }
                    }
                    true
                })?;
        }
        Ok(result)
    }

    /// 按标签查询节点。
    pub fn vertices_by_label(&self, graph: &str, label: &str) -> Result<Vec<Vertex>, Error> {
        let h = self.open_graph(graph)?;
        let prefix = encoding::label_prefix("v", label);
        let mut ids = Vec::new();
        h.label_idx.for_each_key_prefix(&prefix, |key| {
            if key.len() >= 8 {
                if let Some(id) = encoding::key_to_id(&key[key.len() - 8..]) {
                    ids.push(id);
                }
            }
            true
        })?;
        let mut result = Vec::new();
        for id in ids {
            if let Some(data) = h.vertices.get(encoding::id_to_key(id))? {
                if let Some(v) = encoding::decode_vertex(id, &data) {
                    result.push(v);
                }
            }
        }
        Ok(result)
    }

    /// 按标签查询边。
    pub fn edges_by_label(&self, graph: &str, label: &str) -> Result<Vec<Edge>, Error> {
        let h = self.open_graph(graph)?;
        let prefix = encoding::label_prefix("e", label);
        let mut ids = Vec::new();
        h.label_idx.for_each_key_prefix(&prefix, |key| {
            if key.len() >= 8 {
                if let Some(id) = encoding::key_to_id(&key[key.len() - 8..]) {
                    ids.push(id);
                }
            }
            true
        })?;
        let mut result = Vec::new();
        for id in ids {
            if let Some(data) = h.edges.get(encoding::id_to_key(id))? {
                if let Some(e) = encoding::decode_edge(id, &data) {
                    result.push(e);
                }
            }
        }
        Ok(result)
    }

    /// 节点计数（存活节点数，O(N) 扫描）。
    pub fn vertex_count(&self, graph: &str) -> Result<u64, Error> {
        let h = self.open_graph(graph)?;
        h.vertices.count_prefix(&[])
    }

    /// 边计数（存活边数，O(N) 扫描）。
    pub fn edge_count(&self, graph: &str) -> Result<u64, Error> {
        let h = self.open_graph(graph)?;
        h.edges.count_prefix(&[])
    }

    // ---- 内部方法 ----

    fn open_graph(&self, name: &str) -> Result<GraphHandle, Error> {
        // 图名合法性校验：非空、不超 200 字符、仅允许字母数字下划线
        if name.is_empty() || name.len() > 200 {
            return Err(Error::Graph(format!(
                "graph name must be 1-200 chars, got {}",
                name.len()
            )));
        }
        if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(Error::Graph(format!(
                "graph name '{}' contains invalid chars, only [a-zA-Z0-9_] allowed",
                name
            )));
        }
        // 快路径：读缓存
        if let Ok(cache) = self.handle_cache.read() {
            if let Some(h) = cache.get(name) {
                return Ok(h.clone());
            }
        }
        // 慢路径：创建并缓存
        let handle = GraphHandle {
            vertices: self.store.open_keyspace(&format!("graph_{}_v", name))?,
            edges: self.store.open_keyspace(&format!("graph_{}_e", name))?,
            out_idx: self.store.open_keyspace(&format!("graph_{}_out", name))?,
            in_idx: self.store.open_keyspace(&format!("graph_{}_in", name))?,
            label_idx: self.store.open_keyspace(&format!("graph_{}_lbl", name))?,
            meta: self.store.open_keyspace(&format!("graph_{}_meta", name))?,
        };
        if let Ok(mut cache) = self.handle_cache.write() {
            cache.insert(name.to_string(), handle.clone());
        }
        Ok(handle)
    }

    fn load_meta(&self, h: &GraphHandle) -> Result<GraphMeta, Error> {
        match h.meta.get(META_KEY)? {
            Some(data) => {
                decode_meta(&data).ok_or_else(|| Error::Serialization("corrupt graph meta".into()))
            }
            None => Ok(GraphMeta {
                next_vertex_id: 1,
                next_edge_id: 1,
            }),
        }
    }

    fn remove_edge_internal(&self, h: &GraphHandle, edge_id: u64) -> Result<(), Error> {
        let mut batch = self.store.batch();
        self.remove_edge_into_batch(h, edge_id, &mut batch)?;
        batch.commit()?;
        Ok(())
    }

    /// 将边删除操作追加到已有 WriteBatch（不 commit），供 delete_vertex 级联使用。
    fn remove_edge_into_batch(
        &self,
        h: &GraphHandle,
        edge_id: u64,
        batch: &mut crate::storage::Batch,
    ) -> Result<(), Error> {
        let key = encoding::id_to_key(edge_id);
        if let Some(data) = h.edges.get(key)? {
            if let Some(e) = encoding::decode_edge(edge_id, &data) {
                batch.remove(&h.out_idx, encoding::adj_key(e.from, edge_id));
                batch.remove(&h.in_idx, encoding::adj_key(e.to, edge_id));
                batch.remove(&h.label_idx, encoding::label_key("e", &e.label, edge_id));
            }
        }
        batch.remove(&h.edges, key.to_vec());
        Ok(())
    }

    fn collect_adj_edges(
        &self,
        h: &GraphHandle,
        vertex_id: u64,
        is_out: bool,
    ) -> Result<Vec<Edge>, Error> {
        let prefix = vertex_id.to_be_bytes();
        let idx = if is_out { &h.out_idx } else { &h.in_idx };
        let mut eids = Vec::new();
        idx.for_each_kv_prefix(&prefix, |k, _v| {
            if k.len() >= 16 {
                if let Some(eid) = encoding::key_to_id(&k[8..]) {
                    eids.push(eid);
                }
            }
            true
        })?;
        let mut result = Vec::new();
        for eid in eids {
            if let Some(data) = h.edges.get(encoding::id_to_key(eid))? {
                if let Some(e) = encoding::decode_edge(eid, &data) {
                    result.push(e);
                }
            }
        }
        Ok(result)
    }
}

// ---- Meta 编解码 ----

fn encode_meta(m: &GraphMeta) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&m.next_vertex_id.to_be_bytes());
    buf[8..].copy_from_slice(&m.next_edge_id.to_be_bytes());
    buf
}

pub(super) fn decode_meta(data: &[u8]) -> Option<GraphMeta> {
    if data.len() < 16 {
        return None;
    }
    Some(GraphMeta {
        next_vertex_id: u64::from_be_bytes(data[..8].try_into().ok()?),
        next_edge_id: u64::from_be_bytes(data[8..16].try_into().ok()?),
    })
}
