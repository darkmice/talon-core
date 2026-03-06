/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量引擎：持久化 HNSW 索引 + KNN（cosine/L2/dot）。
//!
//! M5 实现：自研 HNSW（Hierarchical Navigable Small World）图索引，
//! 替换 M1.5 的暴力 KNN。图结构持久化到 fjall keyspace。
//!
//! 依赖 storage + types + error。

mod distance;
mod hnsw;
pub mod metadata;
mod quantization;
mod recommend;
mod snapshot;

#[cfg(test)]
mod tests;

use crate::error::Error;
use crate::storage::{Keyspace, SegmentManager, Store};

use distance::{deserialize_vec, dist_fn};
use hnsw::HnswMeta;
use quantization::{
    compute_quantization_params, compute_quantization_params_ref, quant_dist_fn, quant_key,
    quantize_vec, QuantizationParams,
};

const VEC_PREFIX: &str = "vector_";

fn keyspace_name(name: &str) -> String {
    format!("{}{}", VEC_PREFIX, name)
}

/// 向量索引与检索引擎；内部使用 HNSW 图索引。
/// M4：通过 SegmentManager 缓存热点 HNSW 节点，冷节点自动淘汰。
pub struct VectorEngine {
    keyspace: Keyspace,
    store: Store,
    /// 索引名称（用于 SegmentManager key 前缀）。
    name: String,
    /// 统一段管理器。
    segments: SegmentManager,
}

impl VectorEngine {
    /// 打开向量索引（使用 store 的 vector_{name} keyspace）。
    pub fn open(store: &Store, name: &str) -> Result<Self, Error> {
        let keyspace = store.open_keyspace(&keyspace_name(name))?;
        let segments = store.segment_manager().clone();
        Ok(VectorEngine {
            keyspace,
            store: store.clone(),
            name: name.to_string(),
            segments,
        })
    }

    /// 设置查询时搜索宽度 ef_search（运行时可调）。
    pub fn set_ef_search(&self, ef_search: usize) -> Result<(), Error> {
        let mut meta = self.load_or_init_meta()?;
        meta.ef_search = ef_search.max(1);
        self.save_meta(&meta)
    }

    /// 启用 SQ8 标量量化：从已有向量统计 min/max 参数，并将所有向量量化存储。
    /// 压缩比约 4:1（f32 → u8），精度损失 <2%。
    /// 要求索引中至少有 1 个向量用于统计参数。
    /// M86：使用 for_each_kv_prefix 消除 N+1 双重查找。
    pub fn enable_quantization(&self) -> Result<(), Error> {
        let meta = self.load_or_init_meta()?;
        if meta.quantization.is_some() {
            return Ok(());
        }
        // 第一遍：收集所有向量用于计算量化参数
        let mut items: Vec<(u64, Vec<f32>)> = Vec::new();
        let mut scan_err: Option<Error> = None;
        self.keyspace.for_each_kv_prefix(b"v:", |key, raw| {
            if key.len() != 10 {
                return true;
            }
            let id = u64::from_be_bytes(key[2..10].try_into().unwrap());
            match deserialize_vec(raw) {
                Ok(v) => {
                    items.push((id, v));
                    true
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }
        let vecs_refs: Vec<&[f32]> = items.iter().map(|(_, v)| v.as_slice()).collect();
        let params = compute_quantization_params_ref(&vecs_refs)
            .ok_or_else(|| Error::Serialization("无向量数据，无法计算量化参数".into()))?;
        // 第二遍：量化存储（直接使用已收集的数据，无需再次读取）
        for (id, vec) in &items {
            let quantized = quantize_vec(vec, &params);
            self.keyspace.set(quant_key(*id), &quantized)?;
        }
        let mut meta = self.load_or_init_meta()?;
        meta.quantization = Some(params);
        self.save_meta(&meta)?;
        Ok(())
    }

    /// 禁用量化：清除量化参数和量化向量数据。
    /// Bug 33：分批删除量化 key（每批 1000），避免 OOM。
    pub fn disable_quantization(&self) -> Result<(), Error> {
        let mut meta = self.load_or_init_meta()?;
        if meta.quantization.is_none() {
            return Ok(());
        }
        meta.quantization = None;
        self.save_meta(&meta)?;
        loop {
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
            self.keyspace.for_each_key_prefix(b"q:", |key| {
                keys.push(key.to_vec());
                keys.len() < 1000
            })?;
            if keys.is_empty() {
                break;
            }
            let mut batch = self.store.batch();
            for k in &keys {
                batch.remove(&self.keyspace, k.clone());
            }
            batch.commit()?;
        }
        Ok(())
    }

    /// 查询是否已启用量化。
    pub fn is_quantized(&self) -> Result<bool, Error> {
        let meta = self.load_or_init_meta()?;
        Ok(meta.quantization.is_some())
    }

    /// 插入向量（id 与 key 对应）。若 id 已存在则更新向量数据并重建图连接。
    /// 启用量化时自动生成量化向量。
    /// P0：维度校验 — 索引非空时，新向量维度必须与已有向量一致。
    pub fn insert(&self, id: u64, vec: &[f32]) -> Result<(), Error> {
        if vec.is_empty() {
            return Err(Error::Serialization("向量不能为空".into()));
        }
        // 维度一致性校验：与索引中第一个向量的维度比较
        let meta = self.load_or_init_meta()?;
        if let Some(entry_id) = meta.entry_point {
            if let Some(existing) = self.load_vec(entry_id)? {
                if existing.len() != vec.len() {
                    return Err(Error::VectorDimMismatch(existing.len(), vec.len()));
                }
            }
        }
        let existed = self.load_node(id)?.is_some();
        // M90：vec/quant 数据延迟到 hnsw_insert 的 WriteBatch 中一起提交
        let quant_data = meta.quantization.as_ref().map(|p| quantize_vec(vec, p));
        if existed {
            // 如果节点已存在（含标记删除），恢复 deleted 标记并更新 count
            // Bug 34：node+meta 改为 WriteBatch 原子提交
            if let Some(mut node) = self.load_node(id)? {
                if node.deleted {
                    node.deleted = false;
                    let mut m = self.load_or_init_meta()?;
                    m.count += 1;
                    let mut batch = self.store.batch();
                    let node_raw = node.encode();
                    batch.insert(&self.keyspace, hnsw::node_key(id), node_raw.clone())?;
                    batch.insert(&self.keyspace, hnsw::META_KEY.to_vec(), m.encode())?;
                    batch.commit()?;
                    let seg_key = format!("vec:{}:n:{}", self.name, id);
                    self.segments.put(seg_key, node_raw);
                }
            }
            return Ok(());
        }
        self.hnsw_insert(id, vec, quant_data.as_deref())?;
        Ok(())
    }

    /// 批量插入向量。
    pub fn insert_batch(&self, items: &[(u64, &[f32])]) -> Result<(), Error> {
        for (id, vec) in items {
            self.insert(*id, vec)?;
        }
        Ok(())
    }

    /// 删除向量（标记删除）。
    /// 向量数据保留用于图遍历，仅标记节点为已删除并从搜索结果中排除。
    /// M76：同时清理关联的 metadata。
    /// Bug 32：node+meta+metadata 三步改为 WriteBatch 原子提交。
    pub fn delete(&self, id: u64) -> Result<(), Error> {
        if let Some(mut node) = self.load_node(id)? {
            node.deleted = true;
            let mut meta = self.load_or_init_meta()?;
            meta.count = meta.count.saturating_sub(1);
            // WriteBatch 原子提交：node 标记删除 + meta count 更新 + metadata 清理
            let mut batch = self.store.batch();
            let node_raw = node.encode();
            batch.insert(&self.keyspace, hnsw::node_key(id), node_raw.clone())?;
            batch.insert(&self.keyspace, hnsw::META_KEY.to_vec(), meta.encode())?;
            batch.remove(&self.keyspace, metadata::meta_key(id));
            batch.commit()?;
            // 更新 SegmentManager 缓存（非关键路径，崩溃后自动 miss 重建）
            let seg_key = format!("vec:{}:n:{}", self.name, id);
            self.segments.put(seg_key, node_raw);
            let vec_seg_key = format!("vec:{}:v:{}", self.name, id);
            self.segments.remove(&vec_seg_key);
            if meta.quantization.is_some() {
                let quant_seg_key = format!("vec:{}:q:{}", self.name, id);
                self.segments.remove(&quant_seg_key);
            }
        }
        Ok(())
    }

    /// 获取指定 ID 的原始向量数据（不存在或已删除返回 None）。
    pub fn get_vector(&self, id: u64) -> Result<Option<Vec<f32>>, Error> {
        self.load_vec(id)
    }

    /// 获取向量数量（不含已删除）。
    pub fn count(&self) -> Result<u64, Error> {
        let meta = self.load_or_init_meta()?;
        Ok(meta.count)
    }

    /// K 近邻检索；metric: "cosine" | "l2" | "dot"。返回 (id, distance) 升序。
    /// 启用量化时使用量化域距离计算（更快、更省内存）。
    /// P0：查询向量维度必须与索引维度一致。
    pub fn search(&self, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, Error> {
        let meta = self.load_or_init_meta()?;
        let Some(entry_id) = meta.entry_point else {
            return Ok(vec![]);
        };
        // 维度校验
        if let Some(existing) = self.load_vec(entry_id)? {
            if existing.len() != query.len() {
                return Err(Error::VectorDimMismatch(existing.len(), query.len()));
            }
        }
        if let Some(ref params) = meta.quantization {
            self.search_quantized(query, k, metric, &meta, entry_id, params)
        } else {
            self.search_raw(query, k, metric, &meta, entry_id)
        }
    }

    /// 原始 f32 向量搜索路径。
    fn search_raw(
        &self,
        query: &[f32],
        k: usize,
        metric: &str,
        meta: &HnswMeta,
        entry_id: u64,
    ) -> Result<Vec<(u64, f32)>, Error> {
        let distance = dist_fn(metric)?;
        let entry_vec = match self.load_vec(entry_id)? {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut current_id = entry_id;
        let mut current_dist = distance(query, &entry_vec);
        for level in (1..=meta.max_level).rev() {
            while let Some(node) = self.load_node(current_id)? {
                let neighbors = node.neighbors.get(level).cloned().unwrap_or_default();
                let mut improved = false;
                for &nb_id in &neighbors {
                    if let Some(nb_vec) = self.load_vec(nb_id)? {
                        let d = distance(query, &nb_vec);
                        if d < current_dist {
                            current_dist = d;
                            current_id = nb_id;
                            improved = true;
                        }
                    }
                }
                if !improved {
                    break;
                }
            }
        }
        let ef = meta.ef_search.max(k);
        let results = self.search_layer(query, current_id, ef, 0, distance)?;
        let mut filtered: Vec<(u64, f32)> = results
            .into_iter()
            .filter(|(id, _)| {
                self.load_node(*id)
                    .ok()
                    .flatten()
                    .map(|n| !n.deleted)
                    .unwrap_or(false)
            })
            .collect();
        filtered.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        filtered.truncate(k);
        Ok(filtered)
    }

    /// 量化搜索路径：使用 u8 量化向量计算距离，减少内存和计算开销。
    fn search_quantized(
        &self,
        query: &[f32],
        k: usize,
        metric: &str,
        meta: &HnswMeta,
        entry_id: u64,
        params: &QuantizationParams,
    ) -> Result<Vec<(u64, f32)>, Error> {
        let qdist = quant_dist_fn(metric)?;
        let query_q = quantize_vec(query, params);
        let entry_q = match self.load_quantized_vec(entry_id)? {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut current_id = entry_id;
        let mut current_dist = qdist(&query_q, &entry_q, params);
        for level in (1..=meta.max_level).rev() {
            while let Some(node) = self.load_node(current_id)? {
                let neighbors = node.neighbors.get(level).cloned().unwrap_or_default();
                let mut improved = false;
                for &nb_id in &neighbors {
                    if let Some(nb_q) = self.load_quantized_vec(nb_id)? {
                        let d = qdist(&query_q, &nb_q, params);
                        if d < current_dist {
                            current_dist = d;
                            current_id = nb_id;
                            improved = true;
                        }
                    }
                }
                if !improved {
                    break;
                }
            }
        }
        let ef = meta.ef_search.max(k);
        let results = self.search_layer_quantized(&query_q, current_id, ef, 0, qdist, params)?;
        let mut filtered: Vec<(u64, f32)> = results
            .into_iter()
            .filter(|(id, _)| {
                self.load_node(*id)
                    .ok()
                    .flatten()
                    .map(|n| !n.deleted)
                    .unwrap_or(false)
            })
            .collect();
        filtered.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        filtered.truncate(k);
        Ok(filtered)
    }

    /// 批量 KNN 检索。
    pub fn batch_search(
        &self,
        vecs: &[&[f32]],
        k: usize,
        metric: &str,
    ) -> Result<Vec<Vec<(u64, f32)>>, Error> {
        vecs.iter().map(|v| self.search(v, k, metric)).collect()
    }

    /// 从暴力模式数据重建 HNSW 索引（用于升级迁移）。
    /// 若量化已启用，重建后重新量化所有向量。
    /// M86：使用 for_each_key_prefix / for_each_kv_prefix 消除 N+1。
    pub fn rebuild_index(&self) -> Result<u64, Error> {
        let old_meta = self.load_or_init_meta()?;
        let had_quantization = old_meta.quantization.is_some();

        // 清除旧的 HNSW 元数据和节点（流式收集 key 再删除）
        self.keyspace.delete(hnsw::META_KEY)?;
        let mut del_keys: Vec<Vec<u8>> = Vec::new();
        self.keyspace.for_each_key_prefix(b"n:", |k| {
            del_keys.push(k.to_vec());
            true
        })?;
        self.keyspace.for_each_key_prefix(b"q:", |k| {
            del_keys.push(k.to_vec());
            true
        })?;
        for k in &del_keys {
            self.keyspace.delete(k)?;
        }
        self.segments
            .remove_prefix(&format!("vec:{}:n:", self.name));
        self.segments
            .remove_prefix(&format!("vec:{}:q:", self.name));

        // 收集所有向量数据（一次遍历获取 key+value）
        let mut items: Vec<(u64, Vec<f32>)> = Vec::new();
        let mut scan_err: Option<Error> = None;
        self.keyspace.for_each_kv_prefix(b"v:", |key, raw| {
            if key.len() != 10 {
                return true;
            }
            let id = u64::from_be_bytes(key[2..10].try_into().unwrap());
            match deserialize_vec(raw) {
                Ok(v) => {
                    items.push((id, v));
                    true
                }
                Err(e) => {
                    scan_err = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = scan_err {
            return Err(e);
        }

        let count = items.len() as u64;
        for (id, vec) in &items {
            self.hnsw_insert(*id, vec, None)?;
        }

        if had_quantization && !items.is_empty() {
            let vecs: Vec<Vec<f32>> = items.iter().map(|(_, v)| v.clone()).collect();
            if let Some(params) = compute_quantization_params(&vecs) {
                for (id, vec) in &items {
                    let quantized = quantize_vec(vec, &params);
                    self.keyspace.set(quant_key(*id), &quantized)?;
                }
                let mut meta = self.load_or_init_meta()?;
                meta.quantization = Some(params);
                self.save_meta(&meta)?;
            }
        }

        Ok(count)
    }
}
