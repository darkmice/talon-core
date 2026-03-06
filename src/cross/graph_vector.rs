/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph + Vector 联合搜索 (GraphRAG)：图遍历获取关联节点，向量重排序。
//!
//! 典型场景：知识图谱 RAG — 从实体出发 BFS 找关联文档节点，再用 embedding 排序。

use std::collections::HashMap;

use super::distance::resolve_dist_fn;
use crate::error::Error;
use crate::graph::{Direction, GraphEngine};
use crate::vector::VectorEngine;

/// Graph + Vector 联合查询命中结果。
#[derive(Debug, Clone)]
pub struct GraphVectorHit {
    /// 图节点 ID。
    pub vertex_id: u64,
    /// 图节点标签。
    pub label: String,
    /// BFS 深度（距起点的跳数）。
    pub depth: usize,
    /// 向量相似度得分（越小越相似）。
    pub vector_score: f32,
    /// 节点属性（可选，按需返回）。
    pub properties: HashMap<String, String>,
}

/// Graph + Vector 联合搜索参数。
#[derive(Debug, Clone)]
pub struct GraphVectorQuery<'a> {
    /// 图名。
    pub graph_name: &'a str,
    /// BFS 起始节点 ID。
    pub start_vertex: u64,
    /// 最大遍历深度。
    pub max_depth: usize,
    /// 遍历方向。
    pub direction: Direction,
    /// 查询向量。
    pub query_vec: &'a [f32],
    /// 距离度量 ("cosine" | "l2" | "dot")。
    pub metric: &'a str,
    /// 图节点 ID → Vector ID 映射。
    pub vertex_to_vec_id: &'a HashMap<u64, u64>,
    /// 可选：仅包含指定标签的节点（空则不过滤）。
    pub label_filter: Option<&'a str>,
    /// 最大返回数。
    pub limit: usize,
}

/// GraphRAG 联合搜索：BFS 遍历图获取候选节点，向量相似度重排序。
///
/// 流程：
/// 1. `GraphEngine::bfs` 从起点遍历 max_depth 跳
/// 2. 按 label_filter 过滤（可选）
/// 3. 通过 `vertex_to_vec_id` 映射查找向量 ID
/// 4. 从 VectorEngine 加载向量，计算相似度
/// 5. 按向量相似度升序排序，截断到 limit
pub fn graph_vector_search(
    graph: &GraphEngine,
    vec_engine: &VectorEngine,
    q: &GraphVectorQuery<'_>,
) -> Result<Vec<GraphVectorHit>, Error> {
    // Step 1: BFS 遍历
    let bfs_result = graph.bfs(q.graph_name, q.start_vertex, q.max_depth, q.direction)?;

    if bfs_result.is_empty() {
        return Ok(vec![]);
    }

    let dist_fn = resolve_dist_fn(q.metric)?;
    let mut hits: Vec<GraphVectorHit> = Vec::new();

    for (vertex_id, depth) in &bfs_result {
        // 跳过起点自身
        if *vertex_id == q.start_vertex {
            continue;
        }

        // Step 2: 可选 label 过滤
        let vertex = match graph.get_vertex(q.graph_name, *vertex_id)? {
            Some(v) => v,
            None => continue,
        };
        if let Some(filter) = q.label_filter {
            if vertex.label != filter {
                continue;
            }
        }

        // Step 3: 查找向量映射
        let Some(&vec_id) = q.vertex_to_vec_id.get(vertex_id) else {
            continue;
        };

        // Step 4: 加载向量并计算相似度
        let Some(vec_data) = vec_engine.get_vector(vec_id)? else {
            continue;
        };
        if vec_data.len() != q.query_vec.len() {
            continue;
        }
        let score = dist_fn(q.query_vec, &vec_data);

        hits.push(GraphVectorHit {
            vertex_id: *vertex_id,
            label: vertex.label,
            depth: *depth,
            vector_score: score,
            properties: vertex.properties.into_iter().collect(),
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
    use crate::graph::GraphEngine;
    use crate::storage::Store;
    use std::collections::BTreeMap;

    fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn graph_vector_basic_rag() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        // 构建知识图谱: Entity -> Doc1, Entity -> Doc2, Entity -> Doc3
        let g = GraphEngine::open(&store).unwrap();
        g.create("kg").unwrap();

        let entity = g
            .add_vertex("kg", "Entity", &props(&[("name", "Rust")]))
            .unwrap();
        let doc1 = g
            .add_vertex("kg", "Document", &props(&[("title", "Rust Intro")]))
            .unwrap();
        let doc2 = g
            .add_vertex("kg", "Document", &props(&[("title", "Rust Perf")]))
            .unwrap();
        let doc3 = g
            .add_vertex("kg", "Document", &props(&[("title", "Go Intro")]))
            .unwrap();

        g.add_edge("kg", entity, doc1, "references", &BTreeMap::new())
            .unwrap();
        g.add_edge("kg", entity, doc2, "references", &BTreeMap::new())
            .unwrap();
        g.add_edge("kg", entity, doc3, "references", &BTreeMap::new())
            .unwrap();

        // 创建 embedding 向量（4维）
        let vec_eng = VectorEngine::open(&store, "doc_embed").unwrap();
        vec_eng.insert(100, &[0.9, 0.1, 0.0, 0.0]).unwrap(); // doc1: Rust Intro
        vec_eng.insert(200, &[0.8, 0.2, 0.1, 0.0]).unwrap(); // doc2: Rust Perf
        vec_eng.insert(300, &[0.1, 0.1, 0.9, 0.0]).unwrap(); // doc3: Go Intro

        // vertex_id → vec_id 映射
        let mut mapping = HashMap::new();
        mapping.insert(doc1, 100u64);
        mapping.insert(doc2, 200u64);
        mapping.insert(doc3, 300u64);

        // 查询: "Rust 性能" → 向量 [0.85, 0.15, 0.05, 0.0]
        let query = [0.85, 0.15, 0.05, 0.0];

        let results = graph_vector_search(
            &g,
            &vec_eng,
            &GraphVectorQuery {
                graph_name: "kg",
                start_vertex: entity,
                max_depth: 1,
                direction: Direction::Out,
                query_vec: &query,
                metric: "cosine",
                vertex_to_vec_id: &mapping,
                label_filter: Some("Document"),
                limit: 10,
            },
        )
        .unwrap();

        assert_eq!(results.len(), 3);
        // doc1 (Rust Intro) 和 doc2 (Rust Perf) 应排在 doc3 (Go Intro) 前面
        assert!(results[0].vertex_id == doc1 || results[0].vertex_id == doc2);
        assert_eq!(results[2].vertex_id, doc3);
    }

    #[test]
    fn graph_vector_label_filter() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let g = GraphEngine::open(&store).unwrap();
        g.create("kg").unwrap();

        let e = g.add_vertex("kg", "Entity", &BTreeMap::new()).unwrap();
        let d = g.add_vertex("kg", "Document", &BTreeMap::new()).unwrap();
        let p = g.add_vertex("kg", "Person", &BTreeMap::new()).unwrap();
        g.add_edge("kg", e, d, "ref", &BTreeMap::new()).unwrap();
        g.add_edge("kg", e, p, "author", &BTreeMap::new()).unwrap();

        let vec_eng = VectorEngine::open(&store, "v").unwrap();
        vec_eng.insert(1, &[1.0]).unwrap();
        vec_eng.insert(2, &[0.5]).unwrap();

        let mut m = HashMap::new();
        m.insert(d, 1u64);
        m.insert(p, 2u64);

        // 仅返回 Document 标签的节点
        let results = graph_vector_search(
            &g,
            &vec_eng,
            &GraphVectorQuery {
                graph_name: "kg",
                start_vertex: e,
                max_depth: 1,
                direction: Direction::Out,
                query_vec: &[0.9],
                metric: "l2",
                vertex_to_vec_id: &m,
                label_filter: Some("Document"),
                limit: 10,
            },
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].label, "Document");
    }

    #[test]
    fn graph_vector_multi_hop() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let g = GraphEngine::open(&store).unwrap();
        g.create("g").unwrap();

        // A -> B -> C
        let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();
        g.add_edge("g", b, c, "r", &BTreeMap::new()).unwrap();

        let vec_eng = VectorEngine::open(&store, "v").unwrap();
        vec_eng.insert(10, &[0.5, 0.5]).unwrap(); // b
        vec_eng.insert(20, &[1.0, 0.0]).unwrap(); // c

        let mut m = HashMap::new();
        m.insert(b, 10u64);
        m.insert(c, 20u64);

        // depth=1 只找到 b
        let r1 = graph_vector_search(
            &g,
            &vec_eng,
            &GraphVectorQuery {
                graph_name: "g",
                start_vertex: a,
                max_depth: 1,
                direction: Direction::Out,
                query_vec: &[1.0, 0.0],
                metric: "cosine",
                vertex_to_vec_id: &m,
                label_filter: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].vertex_id, b);

        // depth=2 找到 b 和 c
        let r2 = graph_vector_search(
            &g,
            &vec_eng,
            &GraphVectorQuery {
                graph_name: "g",
                start_vertex: a,
                max_depth: 2,
                direction: Direction::Out,
                query_vec: &[1.0, 0.0],
                metric: "cosine",
                vertex_to_vec_id: &m,
                label_filter: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(r2.len(), 2);
        // c 的向量 [1,0] 与查询 [1,0] 完全匹配，排第一
        assert_eq!(r2[0].vertex_id, c);
    }

    #[test]
    fn graph_vector_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let g = GraphEngine::open(&store).unwrap();
        g.create("g").unwrap();
        let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();

        let vec_eng = VectorEngine::open(&store, "v").unwrap();
        let m = HashMap::new();

        let results = graph_vector_search(
            &g,
            &vec_eng,
            &GraphVectorQuery {
                graph_name: "g",
                start_vertex: a,
                max_depth: 3,
                direction: Direction::Out,
                query_vec: &[1.0],
                metric: "cosine",
                vertex_to_vec_id: &m,
                label_filter: None,
                limit: 10,
            },
        )
        .unwrap();

        assert!(results.is_empty());
    }
}
