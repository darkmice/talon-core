/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO + Graph + Vector 三引擎联合搜索。
//!
//! 终极 RAG 场景："附近的知识图谱相关内容"——
//! 从实体出发 BFS 找关联节点，按地理位置过滤，向量相似度排序。

use std::collections::HashMap;

use super::distance::resolve_dist_fn;
use crate::error::Error;
use crate::geo::{GeoEngine, GeoPoint, GeoUnit};
use crate::graph::{Direction, GraphEngine};
use crate::vector::VectorEngine;

/// 三引擎联合查询命中结果。
#[derive(Debug, Clone)]
pub struct TripleHit {
    /// 图节点 ID。
    pub vertex_id: u64,
    /// 图节点标签。
    pub label: String,
    /// BFS 深度。
    pub depth: usize,
    /// 地理坐标。
    pub point: GeoPoint,
    /// 距搜索中心距离（米）。
    pub dist_m: f64,
    /// 向量相似度得分（越小越相似）。
    pub vector_score: f32,
}

/// 三引擎联合搜索参数。
#[derive(Debug, Clone)]
pub struct TripleQuery<'a> {
    /// 图名。
    pub graph_name: &'a str,
    /// BFS 起始节点 ID。
    pub start_vertex: u64,
    /// 最大遍历深度。
    pub max_depth: usize,
    /// 遍历方向。
    pub direction: Direction,
    /// 可选标签过滤。
    pub label_filter: Option<&'a str>,
    /// GEO 空间名。
    pub geo_name: &'a str,
    /// 搜索中心经度。
    pub center_lng: f64,
    /// 搜索中心纬度。
    pub center_lat: f64,
    /// 搜索半径。
    pub radius: f64,
    /// 距离单位。
    pub unit: GeoUnit,
    /// 查询向量。
    pub query_vec: &'a [f32],
    /// 距离度量 ("cosine" | "l2" | "dot")。
    pub metric: &'a str,
    /// 图节点 ID → GEO key 映射。
    pub vertex_to_geo_key: &'a HashMap<u64, String>,
    /// 图节点 ID → Vector ID 映射。
    pub vertex_to_vec_id: &'a HashMap<u64, u64>,
    /// 最大返回数。
    pub limit: usize,
}

/// GEO + Graph + Vector 三引擎联合搜索。
///
/// 流程：
/// 1. Graph BFS 遍历获取关联节点（+ 标签过滤）
/// 2. GEO 圆形范围搜索获取候选集
/// 3. 取交集：同时在图关联中且在地理范围内的节点
/// 4. 加载向量计算相似度
/// 5. 按向量相似度升序排序
pub fn triple_search(
    graph: &GraphEngine,
    geo: &GeoEngine,
    vec_engine: &VectorEngine,
    q: &TripleQuery<'_>,
) -> Result<Vec<TripleHit>, Error> {
    // Step 1: Graph BFS
    let bfs_result = graph.bfs(q.graph_name, q.start_vertex, q.max_depth, q.direction)?;
    let mut graph_candidates: HashMap<u64, (usize, String)> = HashMap::new();

    for (vid, depth) in &bfs_result {
        if *vid == q.start_vertex {
            continue;
        }
        let vertex = match graph.get_vertex(q.graph_name, *vid)? {
            Some(v) => v,
            None => continue,
        };
        if let Some(filter) = q.label_filter {
            if vertex.label != filter {
                continue;
            }
        }
        graph_candidates.insert(*vid, (*depth, vertex.label));
    }

    if graph_candidates.is_empty() {
        return Ok(vec![]);
    }

    // Step 2: GEO 范围搜索
    let geo_results = geo.geo_search(
        q.geo_name,
        q.center_lng,
        q.center_lat,
        q.radius,
        q.unit,
        None,
    )?;
    // GEO key → (point, dist_m)
    let geo_map: HashMap<String, (GeoPoint, f64)> = geo_results
        .iter()
        .map(|m| (m.key.clone(), (m.point, m.dist.unwrap_or(0.0))))
        .collect();

    // Step 3: 取交集 — 节点必须同时在图关联中且在地理范围内
    let dist_fn = resolve_dist_fn(q.metric)?;
    let mut hits: Vec<TripleHit> = Vec::new();

    for (vid, (depth, label)) in &graph_candidates {
        // 查找 GEO key
        let Some(geo_key) = q.vertex_to_geo_key.get(vid) else {
            continue;
        };
        let Some(&(point, dist_m)) = geo_map.get(geo_key) else {
            continue;
        };

        // Step 4: 加载向量计算相似度
        let Some(&vec_id) = q.vertex_to_vec_id.get(vid) else {
            continue;
        };
        let Some(vec_data) = vec_engine.get_vector(vec_id)? else {
            continue;
        };
        if vec_data.len() != q.query_vec.len() {
            continue;
        }

        let score = dist_fn(q.query_vec, &vec_data);
        hits.push(TripleHit {
            vertex_id: *vid,
            label: label.clone(),
            depth: *depth,
            point,
            dist_m,
            vector_score: score,
        });
    }

    // Step 5: 按向量相似度排序
    hits.sort_by(|a, b| {
        a.vector_score
            .partial_cmp(&b.vector_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    hits.truncate(q.limit);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;
    use std::collections::BTreeMap;

    #[test]
    fn triple_search_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        // Graph: Entity → Shop_A, Entity → Shop_B, Entity → Shop_C
        let g = GraphEngine::open(&store).unwrap();
        g.create("kg").unwrap();
        let entity = g.add_vertex("kg", "Entity", &BTreeMap::new()).unwrap();
        let shop_a = g.add_vertex("kg", "Shop", &BTreeMap::new()).unwrap();
        let shop_b = g.add_vertex("kg", "Shop", &BTreeMap::new()).unwrap();
        let shop_c = g.add_vertex("kg", "Shop", &BTreeMap::new()).unwrap();
        g.add_edge("kg", entity, shop_a, "has", &BTreeMap::new())
            .unwrap();
        g.add_edge("kg", entity, shop_b, "has", &BTreeMap::new())
            .unwrap();
        g.add_edge("kg", entity, shop_c, "has", &BTreeMap::new())
            .unwrap();

        // GEO: Shop_A 和 Shop_B 在搜索范围内，Shop_C 在范围外
        let geo = GeoEngine::open(&store).unwrap();
        geo.create("shops").unwrap();
        geo.geo_add("shops", "sa", 116.40, 39.91).unwrap(); // 近
        geo.geo_add("shops", "sb", 116.41, 39.92).unwrap(); // 近
        geo.geo_add("shops", "sc", 120.00, 30.00).unwrap(); // 远（上海）

        // Vector: 每个 shop 的 embedding
        let vec_eng = VectorEngine::open(&store, "shop_vec").unwrap();
        vec_eng.insert(1, &[0.9, 0.1]).unwrap(); // shop_a: 高相关
        vec_eng.insert(2, &[0.1, 0.9]).unwrap(); // shop_b: 低相关
        vec_eng.insert(3, &[0.5, 0.5]).unwrap(); // shop_c: 不会出现

        // 映射
        let mut v2g = HashMap::new();
        v2g.insert(shop_a, "sa".to_string());
        v2g.insert(shop_b, "sb".to_string());
        v2g.insert(shop_c, "sc".to_string());

        let mut v2v = HashMap::new();
        v2v.insert(shop_a, 1u64);
        v2v.insert(shop_b, 2u64);
        v2v.insert(shop_c, 3u64);

        let results = triple_search(
            &g,
            &geo,
            &vec_eng,
            &TripleQuery {
                graph_name: "kg",
                start_vertex: entity,
                max_depth: 1,
                direction: Direction::Out,
                label_filter: Some("Shop"),
                geo_name: "shops",
                center_lng: 116.40,
                center_lat: 39.91,
                radius: 5.0,
                unit: GeoUnit::Kilometers,
                query_vec: &[1.0, 0.0],
                metric: "cosine",
                vertex_to_geo_key: &v2g,
                vertex_to_vec_id: &v2v,
                limit: 10,
            },
        )
        .unwrap();

        // 只有 shop_a 和 shop_b 在 GEO 范围内（shop_c 在上海）
        assert_eq!(results.len(), 2);
        // shop_a [0.9,0.1] 更接近查询 [1,0]
        assert_eq!(results[0].vertex_id, shop_a);
        assert_eq!(results[1].vertex_id, shop_b);
    }

    #[test]
    fn triple_search_no_geo_match() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let g = GraphEngine::open(&store).unwrap();
        g.create("g").unwrap();
        let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();

        let geo = GeoEngine::open(&store).unwrap();
        geo.create("g").unwrap();
        // b 的 GEO 位置远离搜索中心
        geo.geo_add("g", "bk", 0.0, 0.0).unwrap();

        let vec_eng = VectorEngine::open(&store, "v").unwrap();
        vec_eng.insert(1, &[1.0]).unwrap();

        let mut v2g = HashMap::new();
        v2g.insert(b, "bk".to_string());
        let mut v2v = HashMap::new();
        v2v.insert(b, 1u64);

        let results = triple_search(
            &g,
            &geo,
            &vec_eng,
            &TripleQuery {
                graph_name: "g",
                start_vertex: a,
                max_depth: 1,
                direction: Direction::Out,
                label_filter: None,
                geo_name: "g",
                center_lng: 116.40,
                center_lat: 39.91,
                radius: 1.0,
                unit: GeoUnit::Kilometers,
                query_vec: &[1.0],
                metric: "cosine",
                vertex_to_geo_key: &v2g,
                vertex_to_vec_id: &v2v,
                limit: 10,
            },
        )
        .unwrap();

        assert!(results.is_empty()); // b 不在 GEO 范围内
    }
}
