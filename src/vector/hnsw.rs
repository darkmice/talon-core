/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! HNSW 图索引内部算法：插入、搜索、裁剪、持久化。

use crate::error::Error;
use std::collections::{BinaryHeap, HashSet};

use super::distance::{deserialize_vec, dist_fn, DistFn, MaxItem, MinItem};
use super::quantization::{quant_key, QuantDistFn, QuantizationParams};
use super::VectorEngine;

/// HNSW 元数据 key。
pub(super) const META_KEY: &[u8] = b"hnsw_meta";

/// HNSW 索引元数据（M106：二进制编码）。
#[derive(Debug, Clone)]
pub(super) struct HnswMeta {
    pub entry_point: Option<u64>,
    pub max_level: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub metric: String,
    pub count: u64,
    pub seed_counter: u64,
    pub quantization: Option<QuantizationParams>,
}
impl HnswMeta {
    pub fn encode(&self) -> Vec<u8> {
        let qp_bytes = self.quantization.as_ref().map(|q| q.encode());
        let cap = 57 + self.metric.len() + 1 + qp_bytes.as_ref().map_or(0, |b| b.len());
        let mut buf = Vec::with_capacity(cap);
        buf.push(self.entry_point.is_some() as u8);
        buf.extend_from_slice(&self.entry_point.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&(self.max_level as u64).to_le_bytes());
        buf.extend_from_slice(&(self.m as u64).to_le_bytes());
        buf.extend_from_slice(&(self.ef_construction as u64).to_le_bytes());
        buf.extend_from_slice(&(self.ef_search as u64).to_le_bytes());
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&self.seed_counter.to_le_bytes());
        buf.extend_from_slice(&(self.metric.len() as u16).to_le_bytes());
        buf.extend_from_slice(self.metric.as_bytes());
        match &qp_bytes {
            Some(qb) => {
                buf.push(1);
                buf.extend_from_slice(qb);
            }
            None => buf.push(0),
        }
        buf
    }
    pub fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() < 59 {
            return Err(Error::Serialization("HnswMeta 数据不足".into()));
        }
        let has_ep = raw[0] != 0;
        let ep_val = u64::from_le_bytes(raw[1..9].try_into().unwrap());
        let entry_point = if has_ep { Some(ep_val) } else { None };
        let max_level = u64::from_le_bytes(raw[9..17].try_into().unwrap()) as usize;
        let m = u64::from_le_bytes(raw[17..25].try_into().unwrap()) as usize;
        let ef_construction = u64::from_le_bytes(raw[25..33].try_into().unwrap()) as usize;
        let ef_search = u64::from_le_bytes(raw[33..41].try_into().unwrap()) as usize;
        let count = u64::from_le_bytes(raw[41..49].try_into().unwrap());
        let seed_counter = u64::from_le_bytes(raw[49..57].try_into().unwrap());
        let metric_len = u16::from_le_bytes(raw[57..59].try_into().unwrap()) as usize;
        let off = 59 + metric_len;
        if raw.len() < off + 1 {
            return Err(Error::Serialization("HnswMeta metric 数据不足".into()));
        }
        let metric = String::from_utf8_lossy(&raw[59..off]).to_string();
        let quantization = if raw[off] != 0 {
            Some(QuantizationParams::decode(&raw[off + 1..])?)
        } else {
            None
        };
        Ok(HnswMeta {
            entry_point,
            max_level,
            m,
            ef_construction,
            ef_search,
            metric,
            count,
            seed_counter,
            quantization,
        })
    }
}

impl Default for HnswMeta {
    fn default() -> Self {
        HnswMeta {
            entry_point: None,
            max_level: 0,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            metric: "cosine".to_string(),
            count: 0,
            seed_counter: 0,
            quantization: None,
        }
    }
}

/// HNSW 节点（M106：二进制编码，紧凑格式）。
#[derive(Debug, Clone)]
pub(super) struct HnswNode {
    pub id: u64,
    pub level: usize,
    pub neighbors: Vec<Vec<u64>>,
    pub deleted: bool,
}
impl HnswNode {
    pub fn encode(&self) -> Vec<u8> {
        let nbr_size: usize = self.neighbors.iter().map(|l| 2 + l.len() * 8).sum();
        let mut buf = Vec::with_capacity(10 + nbr_size);
        buf.extend_from_slice(&self.id.to_le_bytes());
        buf.push(self.level as u8);
        buf.push(self.deleted as u8);
        for layer in &self.neighbors {
            buf.extend_from_slice(&(layer.len() as u16).to_le_bytes());
            for &nid in layer {
                buf.extend_from_slice(&nid.to_le_bytes());
            }
        }
        buf
    }
    pub fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() < 10 {
            return Err(Error::Serialization("HnswNode 数据不足".into()));
        }
        let id = u64::from_le_bytes(raw[0..8].try_into().unwrap());
        let level = raw[8] as usize;
        let deleted = raw[9] != 0;
        let mut off = 10;
        let mut neighbors = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            if off + 2 > raw.len() {
                break;
            }
            let cnt = u16::from_le_bytes(raw[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let byte_len = cnt * 8;
            if off + byte_len > raw.len() {
                break;
            }
            // LE 平台零拷贝：一次 memcpy 整个邻居块
            #[cfg(target_endian = "little")]
            {
                let mut layer = vec![0u64; cnt];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        raw[off..].as_ptr(),
                        layer.as_mut_ptr() as *mut u8,
                        byte_len,
                    );
                }
                neighbors.push(layer);
            }
            #[cfg(not(target_endian = "little"))]
            {
                let mut layer = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    layer.push(u64::from_le_bytes(raw[off..off + 8].try_into().unwrap()));
                    off += 8;
                }
                neighbors.push(layer);
            }
            #[cfg(target_endian = "little")]
            {
                off += byte_len;
            }
        }
        Ok(HnswNode {
            id,
            level,
            neighbors,
            deleted,
        })
    }
}

// ── keyspace key 约定 ────────────────────────────────────

/// 向量数据 key：`v:{id_be}`（栈上固定 10 字节，零堆分配；对标 Qdrant key 编码）。
pub(super) fn vec_key(id: u64) -> [u8; 10] {
    let mut k = [0u8; 10];
    k[0] = b'v';
    k[1] = b':';
    k[2..10].copy_from_slice(&id.to_be_bytes());
    k
}

/// HNSW 节点 key：`n:{id_be}`（栈上固定 10 字节，零堆分配；对标 Qdrant key 编码）。
pub(super) fn node_key(id: u64) -> [u8; 10] {
    let mut k = [0u8; 10];
    k[0] = b'n';
    k[1] = b':';
    k[2..10].copy_from_slice(&id.to_be_bytes());
    k
}

// ── VectorEngine HNSW 内部方法 ───────────────────────────

/// 构建 SegmentManager 缓存 key（减少 format! 堆分配；对标 Qdrant 缓存 key 路径）。
/// 格式：`vec:{name}:{tag}:{id}`，使用预分配 String 代替 format!。
#[inline]
fn seg_cache_key(name: &str, tag: &str, id: u64) -> String {
    use std::fmt::Write;
    // "vec:" + name + ":" + tag + ":" + max 20 digits
    let mut key = String::with_capacity(5 + name.len() + 1 + tag.len() + 1 + 20);
    key.push_str("vec:");
    key.push_str(name);
    key.push(':');
    key.push_str(tag);
    key.push(':');
    let _ = write!(key, "{}", id);
    key
}

impl VectorEngine {
    /// 加载或初始化 HNSW 元数据。
    pub(super) fn load_or_init_meta(&self) -> Result<HnswMeta, Error> {
        match self.keyspace.get(META_KEY)? {
            Some(raw) => HnswMeta::decode(&raw),
            None => {
                let meta = HnswMeta::default();
                self.save_meta(&meta)?;
                Ok(meta)
            }
        }
    }

    /// 保存 HNSW 元数据。
    pub(super) fn save_meta(&self, meta: &HnswMeta) -> Result<(), Error> {
        self.keyspace.set(META_KEY, meta.encode())
    }

    /// 加载 HNSW 节点。
    /// 优先从 SegmentManager 缓存读取，未命中时从 fjall 加载并缓存。
    pub(super) fn load_node(&self, id: u64) -> Result<Option<HnswNode>, Error> {
        let seg_key = seg_cache_key(&self.name, "n", id);
        if let Some(cached) = self.segments.get(&seg_key) {
            return Ok(Some(HnswNode::decode(&cached)?));
        }
        match self.keyspace.get(node_key(id))? {
            Some(raw) => {
                let node = HnswNode::decode(&raw)?;
                self.segments.put(seg_key, raw);
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    /// 与 `load_node` 对称的单节点持久化接口。
    /// 注意：内部热路径使用 WriteBatch 以获得原子性，此方法保留用于未来单节点更新场景。
    #[allow(dead_code)]
    pub(super) fn save_node(&self, node: &HnswNode) -> Result<(), Error> {
        let raw = node.encode();
        self.keyspace.set(node_key(node.id), &raw)?;
        let seg_key = seg_cache_key(&self.name, "n", node.id);
        self.segments.put(seg_key, raw);
        Ok(())
    }

    /// 加载向量数据。优先从 SegmentManager 缓存读取。
    pub(super) fn load_vec(&self, id: u64) -> Result<Option<Vec<f32>>, Error> {
        let seg_key = seg_cache_key(&self.name, "v", id);
        if let Some(cached) = self.segments.get(&seg_key) {
            return Ok(Some(deserialize_vec(&cached)?));
        }
        match self.keyspace.get(vec_key(id))? {
            Some(raw) => {
                self.segments.put(seg_key, raw.clone());
                Ok(Some(deserialize_vec(&raw)?))
            }
            None => Ok(None),
        }
    }

    /// 加载量化向量数据（u8 数组）。优先从 SegmentManager 缓存读取。
    pub(super) fn load_quantized_vec(&self, id: u64) -> Result<Option<Vec<u8>>, Error> {
        let seg_key = seg_cache_key(&self.name, "q", id);
        if let Some(cached) = self.segments.get(&seg_key) {
            return Ok(Some((*cached).clone()));
        }
        match self.keyspace.get(quant_key(id))? {
            Some(raw) => {
                self.segments.put(seg_key, raw.clone());
                Ok(Some(raw))
            }
            None => Ok(None),
        }
    }

    /// 随机层级：使用 -ln(uniform) * ml 公式，ml = 1/ln(M)。
    fn random_level(&self, meta: &mut HnswMeta) -> usize {
        meta.seed_counter += 1;
        let mut x = meta
            .seed_counter
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^= x >> 33;
        let uniform = ((x as f64) / (u64::MAX as f64)).clamp(f64::MIN_POSITIVE, 1.0);
        let ml = 1.0 / (meta.m as f64).ln();
        let level = (-uniform.ln() * ml) as usize;
        level.min(32)
    }

    /// HNSW 插入算法（使用内存节点缓存 + WriteBatch 批量提交，减少磁盘 IO）。
    /// M90: quant_data 和 vec 数据一起写入 WriteBatch。
    pub(super) fn hnsw_insert(
        &self,
        id: u64,
        vec: &[f32],
        quant_data: Option<&[u8]>,
    ) -> Result<(), Error> {
        let mut meta = self.load_or_init_meta()?;
        let distance = dist_fn(&meta.metric)?;

        let mut node_cache: std::collections::HashMap<u64, HnswNode> =
            std::collections::HashMap::new();

        // 第一个节点
        if meta.entry_point.is_none() {
            let node = HnswNode {
                id,
                level: 0,
                neighbors: vec![vec![]],
                deleted: false,
            };
            node_cache.insert(id, node);
            meta.entry_point = Some(id);
            meta.max_level = 0;
            meta.count = 1;
            self.flush_node_cache(&node_cache, &meta, Some((id, vec, quant_data)))?;
            return Ok(());
        }

        let new_level = self.random_level(&mut meta);
        let entry_id = meta.entry_point.unwrap();

        // M202：局部向量缓存 — 消除 hnsw_insert 热路径中重复 load_vec 的
        // Mutex 锁 + format! 堆分配。新向量预存入缓存，解决 recall bug。
        let mut vec_cache: std::collections::HashMap<u64, Vec<f32>> =
            std::collections::HashMap::with_capacity(256);
        vec_cache.insert(id, vec.to_vec());

        // 从顶层贪心下降到 new_level + 1
        let mut current_id = entry_id;
        {
            // 对标 Qdrant：缓存命中只借用引用计算距离，避免 clone 全量向量。
            if !vec_cache.contains_key(&entry_id) {
                if let Some(v) = self.load_vec(entry_id)? {
                    vec_cache.insert(entry_id, v);
                } else {
                    return Ok(());
                }
            }
            let mut current_dist = distance(vec, &vec_cache[&entry_id]);

            let start_level = meta.max_level;
            for level in (new_level + 1..=start_level).rev() {
                while let Some(node) = self.cached_load_node(&node_cache, current_id)? {
                    let empty: Vec<u64> = Vec::new();
                    let neighbors = node.neighbors.get(level).unwrap_or(&empty);
                    let mut improved = false;
                    for &nb_id in neighbors {
                        if !vec_cache.contains_key(&nb_id) {
                            match self.load_vec(nb_id)? {
                                Some(v) => {
                                    vec_cache.insert(nb_id, v);
                                }
                                None => continue,
                            }
                        }
                        let d = distance(vec, &vec_cache[&nb_id]);
                        if d < current_dist {
                            current_dist = d;
                            current_id = nb_id;
                            improved = true;
                        }
                    }
                    if !improved {
                        break;
                    }
                }
            }
        }

        let ef = meta.ef_construction;
        let m = meta.m;
        let m_max0 = m * 2;

        let mut new_node = HnswNode {
            id,
            level: new_level,
            neighbors: vec![vec![]; new_level + 1],
            deleted: false,
        };

        let insert_top = new_level.min(meta.max_level);
        for level in (0..=insert_top).rev() {
            let candidates =
                self.search_layer_cached(vec, current_id, ef, level, distance, &mut vec_cache)?;
            let m_level = if level == 0 { m_max0 } else { m };

            let selected: Vec<u64> = candidates
                .iter()
                .take(m_level)
                .map(|(nid, _)| *nid)
                .collect();

            new_node.neighbors[level] = selected.clone();

            for &nb_id in &selected {
                if let Some(mut nb_node) = self.cached_load_node(&node_cache, nb_id)? {
                    while nb_node.neighbors.len() <= level {
                        nb_node.neighbors.push(vec![]);
                    }
                    nb_node.neighbors[level].push(id);
                    if nb_node.neighbors[level].len() > m_level {
                        self.prune_neighbors(
                            &mut nb_node,
                            level,
                            m_level,
                            distance,
                            &mut vec_cache,
                        )?;
                    }
                    node_cache.insert(nb_id, nb_node);
                }
            }

            if let Some((closest_id, _)) = candidates.first() {
                current_id = *closest_id;
            }
        }

        node_cache.insert(id, new_node);

        if new_level > meta.max_level {
            meta.max_level = new_level;
            meta.entry_point = Some(id);
        }
        meta.count += 1;

        self.flush_node_cache(&node_cache, &meta, Some((id, vec, quant_data)))?;
        Ok(())
    }

    /// 从缓存或磁盘加载节点。
    fn cached_load_node(
        &self,
        cache: &std::collections::HashMap<u64, HnswNode>,
        id: u64,
    ) -> Result<Option<HnswNode>, Error> {
        if let Some(node) = cache.get(&id) {
            return Ok(Some(node.clone()));
        }
        self.load_node(id)
    }

    /// 将节点缓存、元数据、向量数据一次性写入 WriteBatch 并提交。
    /// M90: vec_data 含 (id, vec, quant_data)，合并到同一 batch 减少 journal writes。
    #[allow(clippy::type_complexity)]
    fn flush_node_cache(
        &self,
        cache: &std::collections::HashMap<u64, HnswNode>,
        meta: &HnswMeta,
        vec_data: Option<(u64, &[f32], Option<&[u8]>)>,
    ) -> Result<(), Error> {
        let mut batch = self.store.batch();
        for node in cache.values() {
            let raw = node.encode();
            let seg_key = seg_cache_key(&self.name, "n", node.id);
            batch.insert(&self.keyspace, node_key(node.id), raw.clone())?;
            self.segments.put(seg_key, raw);
        }
        if let Some((id, vec, quant)) = vec_data {
            batch.insert(
                &self.keyspace,
                vec_key(id),
                super::distance::serialize_vec(vec),
            )?;
            if let Some(qd) = quant {
                batch.insert(&self.keyspace, quant_key(id), qd.to_vec())?;
            }
        }
        batch.insert(&self.keyspace, META_KEY.to_vec(), meta.encode())?;
        batch.commit()
    }

    /// 在指定层做 beam search，返回最近的 ef 个 (id, distance)，按距离升序。
    #[allow(dead_code)]
    pub(super) fn search_layer(
        &self,
        query: &[f32],
        entry_id: u64,
        ef: usize,
        level: usize,
        distance: DistFn,
    ) -> Result<Vec<(u64, f32)>, Error> {
        self.search_layer_generic(entry_id, ef, level, |id| {
            self.load_vec(id)
                .map(|opt| opt.map(|v| distance(query, &v)))
        })
    }

    /// 带局部向量缓存的 beam search（插入/搜索热路径用，消除重复 load_vec）。
    pub(super) fn search_layer_cached(
        &self,
        query: &[f32],
        entry_id: u64,
        ef: usize,
        level: usize,
        distance: DistFn,
        vec_cache: &mut std::collections::HashMap<u64, Vec<f32>>,
    ) -> Result<Vec<(u64, f32)>, Error> {
        self.search_layer_generic(entry_id, ef, level, |id| {
            if let Some(v) = vec_cache.get(&id) {
                return Ok(Some(distance(query, v)));
            }
            match self.load_vec(id)? {
                Some(v) => {
                    let d = distance(query, &v);
                    vec_cache.insert(id, v);
                    Ok(Some(d))
                }
                None => Ok(None),
            }
        })
    }

    /// 在指定层做量化 beam search。
    #[allow(dead_code)]
    pub(super) fn search_layer_quantized(
        &self,
        query_q: &[u8],
        entry_id: u64,
        ef: usize,
        level: usize,
        qdist: QuantDistFn,
        params: &QuantizationParams,
    ) -> Result<Vec<(u64, f32)>, Error> {
        self.search_layer_generic(entry_id, ef, level, |id| {
            self.load_quantized_vec(id)
                .map(|opt| opt.map(|v| qdist(query_q, &v, params)))
        })
    }

    /// 带缓存的量化 beam search（搜索路径用，消除重复 load_quantized_vec）。
    pub(super) fn search_layer_quantized_cached(
        &self,
        query_q: &[u8],
        entry_id: u64,
        ef: usize,
        level: usize,
        qdist: QuantDistFn,
        params: &QuantizationParams,
        quant_cache: &mut std::collections::HashMap<u64, Vec<u8>>,
    ) -> Result<Vec<(u64, f32)>, Error> {
        self.search_layer_generic(entry_id, ef, level, |id| {
            if let Some(v) = quant_cache.get(&id) {
                return Ok(Some(qdist(query_q, v, params)));
            }
            match self.load_quantized_vec(id)? {
                Some(v) => {
                    let d = qdist(query_q, &v, params);
                    quant_cache.insert(id, v);
                    Ok(Some(d))
                }
                None => Ok(None),
            }
        })
    }

    /// M88：泛型 beam search — 消除 search_layer/search_layer_quantized 代码重复。
    /// `calc_dist` 闭包：给定节点 ID，返回 Ok(Some(distance)) 或 Ok(None)（向量不存在）。
    fn search_layer_generic<F>(
        &self,
        entry_id: u64,
        ef: usize,
        level: usize,
        mut calc_dist: F,
    ) -> Result<Vec<(u64, f32)>, Error>
    where
        F: FnMut(u64) -> Result<Option<f32>, Error>,
    {
        let entry_dist = match calc_dist(entry_id)? {
            Some(d) => d,
            None => return Ok(vec![]),
        };
        let mut candidates: BinaryHeap<MinItem> = BinaryHeap::new();
        let mut results: BinaryHeap<MaxItem> = BinaryHeap::new();
        // 预分配容量：beam search 通常访问 2-5x ef 个节点
        let mut visited: HashSet<u64> = HashSet::with_capacity(ef * 4);
        candidates.push(MinItem(entry_dist, entry_id));
        results.push(MaxItem(entry_dist, entry_id));
        visited.insert(entry_id);
        while let Some(MinItem(c_dist, c_id)) = candidates.pop() {
            if let Some(farthest) = results.peek() {
                if c_dist > farthest.0 {
                    break;
                }
            }
            let node = match self.load_node(c_id)? {
                Some(n) => n,
                None => continue,
            };
            // 借用邻居列表，避免 .cloned() 堆分配
            let empty: Vec<u64> = Vec::new();
            let neighbors = node.neighbors.get(level).unwrap_or(&empty);
            for &nb_id in neighbors {
                if !visited.insert(nb_id) {
                    continue;
                }
                let nb_dist = match calc_dist(nb_id)? {
                    Some(d) => d,
                    None => continue,
                };
                let should_add =
                    results.len() < ef || results.peek().map_or(true, |f| nb_dist < f.0);
                if should_add {
                    candidates.push(MinItem(nb_dist, nb_id));
                    results.push(MaxItem(nb_dist, nb_id));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut out: Vec<(u64, f32)> = results.into_iter().map(|MaxItem(d, id)| (id, d)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(out)
    }

    /// 裁剪邻居列表：保留距离最近的 m_level 个。使用向量缓存消除重复 load_vec。
    fn prune_neighbors(
        &self,
        node: &mut HnswNode,
        level: usize,
        m_level: usize,
        distance: DistFn,
        vec_cache: &mut std::collections::HashMap<u64, Vec<f32>>,
    ) -> Result<(), Error> {
        // 对标 Qdrant：仅借用缓存引用计算距离，避免 clone 全量向量。
        if !vec_cache.contains_key(&node.id) {
            match self.load_vec(node.id)? {
                Some(v) => {
                    vec_cache.insert(node.id, v);
                }
                None => return Ok(()),
            }
        }
        let neighbors = &node.neighbors[level];
        // 预加载所有邻居向量到缓存，使后续距离计算阶段无需可变借用。
        let mut valid_nbs: Vec<u64> = Vec::with_capacity(neighbors.len());
        for &nb_id in neighbors {
            if !vec_cache.contains_key(&nb_id) {
                match self.load_vec(nb_id)? {
                    Some(v) => {
                        vec_cache.insert(nb_id, v);
                    }
                    None => continue,
                }
            }
            valid_nbs.push(nb_id);
        }
        // 所有向量已在缓存中，安全借用计算距离（单次 HashMap lookup per id）。
        let node_vec = &vec_cache[&node.id];
        let mut scored: Vec<(u64, f32)> = Vec::with_capacity(valid_nbs.len());
        for nb_id in valid_nbs {
            let d = distance(node_vec, &vec_cache[&nb_id]);
            scored.push((nb_id, d));
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(m_level);
        node.neighbors[level] = scored.into_iter().map(|(id, _)| id).collect();
        Ok(())
    }
}
