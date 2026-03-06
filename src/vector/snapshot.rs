/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M95.3：向量引擎快照搜索 API。
//!
//! 提供 `VectorEngine::snapshot_search` — 从快照读取 HNSW 图和向量数据，
//! 保证在并发写入（insert/delete）场景下搜索结果一致。

use std::collections::{BinaryHeap, HashSet};

use super::distance::{deserialize_vec, dist_fn, MaxItem, MinItem};
use super::hnsw::{node_key, vec_key, HnswMeta, HnswNode, META_KEY};
use super::quantization::{quant_dist_fn, quant_key, quantize_vec, QuantizationParams};
use super::VectorEngine;
use crate::error::Error;
use crate::storage::Snapshot;

impl VectorEngine {
    /// M95：快照 KNN 搜索 — 读取快照时刻的 HNSW 图和向量数据。
    /// 后续 insert/delete 对搜索不可见，保证结果一致性。
    pub fn snapshot_search(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        metric: &str,
    ) -> Result<Vec<(u64, f32)>, Error> {
        let meta = self.snap_load_meta(snap)?;
        let Some(entry_id) = meta.entry_point else {
            return Ok(vec![]);
        };
        if let Some(existing) = self.snap_load_vec(snap, entry_id)? {
            if existing.len() != query.len() {
                return Err(Error::VectorDimMismatch(existing.len(), query.len()));
            }
        }
        if let Some(ref params) = meta.quantization {
            self.snap_search_quantized(snap, query, k, metric, &meta, entry_id, params)
        } else {
            self.snap_search_raw(snap, query, k, metric, &meta, entry_id)
        }
    }

    // ── 快照加载方法 ──────────────────────────────────────

    fn snap_load_meta(&self, snap: &Snapshot) -> Result<HnswMeta, Error> {
        match snap.get(&self.keyspace, META_KEY)? {
            Some(raw) => HnswMeta::decode(&raw),
            None => Ok(HnswMeta::default()),
        }
    }

    fn snap_load_node(&self, snap: &Snapshot, id: u64) -> Result<Option<HnswNode>, Error> {
        match snap.get(&self.keyspace, &node_key(id))? {
            Some(raw) => Ok(Some(HnswNode::decode(&raw)?)),
            None => Ok(None),
        }
    }

    fn snap_load_vec(&self, snap: &Snapshot, id: u64) -> Result<Option<Vec<f32>>, Error> {
        match snap.get(&self.keyspace, &vec_key(id))? {
            Some(raw) => Ok(Some(deserialize_vec(&raw)?)),
            None => Ok(None),
        }
    }

    fn snap_load_quantized_vec(&self, snap: &Snapshot, id: u64) -> Result<Option<Vec<u8>>, Error> {
        match snap.get(&self.keyspace, &quant_key(id))? {
            Some(raw) => Ok(Some(raw)),
            None => Ok(None),
        }
    }

    // ── 快照搜索路径 ──────────────────────────────────────

    fn snap_search_raw(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        metric: &str,
        meta: &HnswMeta,
        entry_id: u64,
    ) -> Result<Vec<(u64, f32)>, Error> {
        let distance = dist_fn(metric)?;
        let entry_vec = match self.snap_load_vec(snap, entry_id)? {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut current_id = entry_id;
        let mut current_dist = distance(query, &entry_vec);
        for level in (1..=meta.max_level).rev() {
            while let Some(node) = self.snap_load_node(snap, current_id)? {
                let neighbors = node.neighbors.get(level).cloned().unwrap_or_default();
                let mut improved = false;
                for &nb_id in &neighbors {
                    if let Some(nb_vec) = self.snap_load_vec(snap, nb_id)? {
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
        let results = self.snap_beam_search(snap, entry_id, ef, 0, |id| {
            self.snap_load_vec(snap, id)
                .map(|opt| opt.map(|v| distance(query, &v)))
        })?;
        self.snap_filter_results(snap, results, k)
    }

    #[allow(clippy::too_many_arguments)]
    fn snap_search_quantized(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        metric: &str,
        meta: &HnswMeta,
        entry_id: u64,
        params: &QuantizationParams,
    ) -> Result<Vec<(u64, f32)>, Error> {
        let qdist = quant_dist_fn(metric)?;
        let query_q = quantize_vec(query, params);
        let entry_q = match self.snap_load_quantized_vec(snap, entry_id)? {
            Some(v) => v,
            None => return Ok(vec![]),
        };
        let mut current_id = entry_id;
        let mut current_dist = qdist(&query_q, &entry_q, params);
        for level in (1..=meta.max_level).rev() {
            while let Some(node) = self.snap_load_node(snap, current_id)? {
                let neighbors = node.neighbors.get(level).cloned().unwrap_or_default();
                let mut improved = false;
                for &nb_id in &neighbors {
                    if let Some(nb_q) = self.snap_load_quantized_vec(snap, nb_id)? {
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
        let results = self.snap_beam_search(snap, entry_id, ef, 0, |id| {
            self.snap_load_quantized_vec(snap, id)
                .map(|opt| opt.map(|v| qdist(&query_q, &v, params)))
        })?;
        self.snap_filter_results(snap, results, k)
    }

    /// 快照 beam search — 与 search_layer_generic 逻辑一致，load_node 从快照读。
    fn snap_beam_search<F>(
        &self,
        snap: &Snapshot,
        entry_id: u64,
        ef: usize,
        level: usize,
        calc_dist: F,
    ) -> Result<Vec<(u64, f32)>, Error>
    where
        F: Fn(u64) -> Result<Option<f32>, Error>,
    {
        let entry_dist = match calc_dist(entry_id)? {
            Some(d) => d,
            None => return Ok(vec![]),
        };
        let mut candidates: BinaryHeap<MinItem> = BinaryHeap::new();
        let mut results: BinaryHeap<MaxItem> = BinaryHeap::new();
        let mut visited: HashSet<u64> = HashSet::new();
        candidates.push(MinItem(entry_dist, entry_id));
        results.push(MaxItem(entry_dist, entry_id));
        visited.insert(entry_id);
        while let Some(MinItem(c_dist, c_id)) = candidates.pop() {
            if let Some(farthest) = results.peek() {
                if c_dist > farthest.0 {
                    break;
                }
            }
            let node = match self.snap_load_node(snap, c_id)? {
                Some(n) => n,
                None => continue,
            };
            let neighbors = node.neighbors.get(level).cloned().unwrap_or_default();
            for &nb_id in &neighbors {
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

    /// 过滤已删除节点并截断结果。
    fn snap_filter_results(
        &self,
        snap: &Snapshot,
        results: Vec<(u64, f32)>,
        k: usize,
    ) -> Result<Vec<(u64, f32)>, Error> {
        let mut filtered: Vec<(u64, f32)> = results
            .into_iter()
            .filter(|(id, _)| {
                self.snap_load_node(snap, *id)
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
}

#[cfg(test)]
mod tests {
    use crate::storage::Store;
    use crate::vector::VectorEngine;

    #[test]
    fn vec_snapshot_search_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ve = VectorEngine::open(&store, "snap_test").unwrap();

        // 插入初始向量
        ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        ve.insert(2, &[0.0, 1.0, 0.0]).unwrap();
        ve.insert(3, &[0.0, 0.0, 1.0]).unwrap();

        // 获取快照
        let snap = store.snapshot();

        // 快照后插入更多向量
        ve.insert(4, &[1.0, 1.0, 0.0]).unwrap();
        ve.insert(5, &[0.0, 1.0, 1.0]).unwrap();

        // 快照搜索：只看到 3 个向量
        let snap_results = ve
            .snapshot_search(&snap, &[1.0, 0.0, 0.0], 10, "cosine")
            .unwrap();
        assert_eq!(snap_results.len(), 3, "快照搜索应只看到快照时刻的 3 个向量");

        // 当前搜索：看到 5 个
        let current = ve.search(&[1.0, 0.0, 0.0], 10, "cosine").unwrap();
        assert_eq!(current.len(), 5);
    }

    #[test]
    fn vec_snapshot_search_delete_invisible() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ve = VectorEngine::open(&store, "snap_del").unwrap();

        ve.insert(1, &[1.0, 0.0]).unwrap();
        ve.insert(2, &[0.0, 1.0]).unwrap();

        let snap = store.snapshot();

        // 快照后删除 id=2
        ve.delete(2).unwrap();

        // 快照搜索：仍看到 2 个（快照时 id=2 未删除）
        let snap_results = ve
            .snapshot_search(&snap, &[1.0, 0.0], 10, "cosine")
            .unwrap();
        assert_eq!(snap_results.len(), 2, "快照搜索：删除对快照不可见");

        // 当前搜索：只看到 1 个
        let current = ve.search(&[1.0, 0.0], 10, "cosine").unwrap();
        assert_eq!(current.len(), 1);
    }

    #[test]
    fn vec_snapshot_search_accuracy() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ve = VectorEngine::open(&store, "snap_acc").unwrap();

        ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        ve.insert(2, &[0.9, 0.1, 0.0]).unwrap();
        ve.insert(3, &[0.0, 0.0, 1.0]).unwrap();

        let snap = store.snapshot();

        // 快照搜索 k=1：应返回最近的 id=1
        let results = ve
            .snapshot_search(&snap, &[1.0, 0.0, 0.0], 1, "cosine")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1, "最近邻应为 id=1");
        assert!(results[0].1 < 0.01, "距离应接近 0");
    }
}
