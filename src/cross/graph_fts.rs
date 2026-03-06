/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph + FTS 联合搜索：图遍历获取关联节点，全文搜索重排序。
//!
//! 典型场景：知识图谱中从实体出发，找关联文档，用全文搜索匹配查询。

use std::collections::HashMap;

use crate::error::Error;
use crate::fts::{FtsEngine, SearchHit};
use crate::graph::{Direction, GraphEngine};

/// Graph + FTS 联合查询命中结果。
#[derive(Debug, Clone)]
pub struct GraphFtsHit {
    /// 图节点 ID。
    pub vertex_id: u64,
    /// 图节点标签。
    pub label: String,
    /// BFS 深度（距起点的跳数）。
    pub depth: usize,
    /// FTS 搜索命中（包含 doc_id, score, fields, highlights）。
    pub search_hit: SearchHit,
}

/// Graph + FTS 联合搜索参数。
#[derive(Debug, Clone)]
pub struct GraphFtsQuery<'a> {
    /// 图名。
    pub graph_name: &'a str,
    /// BFS 起始节点 ID。
    pub start_vertex: u64,
    /// 最大遍历深度。
    pub max_depth: usize,
    /// 遍历方向。
    pub direction: Direction,
    /// FTS 索引名。
    pub fts_index: &'a str,
    /// 全文搜索查询词。
    pub query: &'a str,
    /// 图节点 ID → FTS doc_id 映射。
    pub vertex_to_doc_id: &'a HashMap<u64, String>,
    /// 可选：仅包含指定标签的节点。
    pub label_filter: Option<&'a str>,
    /// 最大返回数。
    pub limit: usize,
}

/// GraphRAG + FTS 联合搜索：BFS 遍历图获取候选节点，FTS 全文搜索重排序。
///
/// 流程：
/// 1. `GraphEngine::bfs` 从起点遍历 max_depth 跳
/// 2. 按 label_filter 过滤（可选）
/// 3. 通过 `vertex_to_doc_id` 映射获取 FTS doc_id
/// 4. `FtsEngine::search` 获取搜索结果，与候选集取交集
/// 5. 按 BM25 分数降序排序，截断到 limit
pub fn graph_fts_search(
    graph: &GraphEngine,
    fts: &FtsEngine,
    q: &GraphFtsQuery<'_>,
) -> Result<Vec<GraphFtsHit>, Error> {
    // Step 1: BFS 遍历
    let bfs_result = graph.bfs(q.graph_name, q.start_vertex, q.max_depth, q.direction)?;

    if bfs_result.len() <= 1 {
        return Ok(vec![]);
    }

    // Step 2+3: 收集候选 doc_id → (vertex_id, depth, label)
    let mut doc_to_vertex: HashMap<String, (u64, usize, String)> = HashMap::new();
    for (vertex_id, depth) in &bfs_result {
        if *vertex_id == q.start_vertex {
            continue;
        }
        let vertex = match graph.get_vertex(q.graph_name, *vertex_id)? {
            Some(v) => v,
            None => continue,
        };
        if let Some(filter) = q.label_filter {
            if vertex.label != filter {
                continue;
            }
        }
        if let Some(doc_id) = q.vertex_to_doc_id.get(vertex_id) {
            doc_to_vertex.insert(doc_id.clone(), (*vertex_id, *depth, vertex.label.clone()));
        }
    }

    if doc_to_vertex.is_empty() {
        return Ok(vec![]);
    }

    // Step 4: FTS 搜索（获取较大结果集，然后与候选集取交集）
    // 扩大 FTS 搜索范围（候选集的 10 倍），避免候选集中高分文档被 FTS 层提前截断
    let fts_limit = doc_to_vertex.len().saturating_mul(10).max(100);
    let fts_results = fts.search(q.fts_index, q.query, fts_limit)?;

    // Step 5: 取交集，构建结果
    let mut hits: Vec<GraphFtsHit> = Vec::new();
    for hit in fts_results {
        if let Some((vertex_id, depth, label)) = doc_to_vertex.get(&hit.doc_id) {
            hits.push(GraphFtsHit {
                vertex_id: *vertex_id,
                label: label.clone(),
                depth: *depth,
                search_hit: hit,
            });
        }
    }

    // FTS 结果已按 BM25 分数降序排序
    hits.truncate(q.limit);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::{FtsConfig, FtsDoc};
    use crate::storage::Store;
    use std::collections::BTreeMap;

    fn bt(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn graph_fts_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        // 构建知识图谱
        let g = GraphEngine::open(&store).unwrap();
        g.create("kg").unwrap();

        let entity = g
            .add_vertex("kg", "Entity", &bt(&[("name", "Rust")]))
            .unwrap();
        let doc1 = g
            .add_vertex("kg", "Document", &bt(&[("title", "Rust Intro")]))
            .unwrap();
        let doc2 = g
            .add_vertex("kg", "Document", &bt(&[("title", "Rust Performance")]))
            .unwrap();
        let doc3 = g
            .add_vertex("kg", "Document", &bt(&[("title", "Go Basics")]))
            .unwrap();

        g.add_edge("kg", entity, doc1, "references", &BTreeMap::new())
            .unwrap();
        g.add_edge("kg", entity, doc2, "references", &BTreeMap::new())
            .unwrap();
        g.add_edge("kg", entity, doc3, "references", &BTreeMap::new())
            .unwrap();

        // 创建 FTS 索引并添加文档
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("docs", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "d1".to_string(),
                fields: bt(&[("content", "Introduction to Rust programming language")]),
            },
        )
        .unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "d2".to_string(),
                fields: bt(&[("content", "Rust performance benchmarks and optimization")]),
            },
        )
        .unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "d3".to_string(),
                fields: bt(&[("content", "Go language basics and tutorial")]),
            },
        )
        .unwrap();

        // vertex → doc_id 映射
        let mut mapping = HashMap::new();
        mapping.insert(doc1, "d1".to_string());
        mapping.insert(doc2, "d2".to_string());
        mapping.insert(doc3, "d3".to_string());

        // 搜索 "Rust" — 应该 d1/d2 排在 d3 前面
        let results = graph_fts_search(
            &g,
            &fts,
            &GraphFtsQuery {
                graph_name: "kg",
                start_vertex: entity,
                max_depth: 1,
                direction: Direction::Out,
                fts_index: "docs",
                query: "Rust",
                vertex_to_doc_id: &mapping,
                label_filter: Some("Document"),
                limit: 10,
            },
        )
        .unwrap();

        assert_eq!(results.len(), 2); // 只有 d1, d2 匹配 "Rust"
        assert!(results[0].search_hit.score > 0.0);
    }

    #[test]
    fn graph_fts_no_match() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let g = GraphEngine::open(&store).unwrap();
        g.create("kg").unwrap();
        let e = g.add_vertex("kg", "E", &BTreeMap::new()).unwrap();
        let d = g.add_vertex("kg", "D", &BTreeMap::new()).unwrap();
        g.add_edge("kg", e, d, "r", &BTreeMap::new()).unwrap();

        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("idx", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "idx",
            &FtsDoc {
                doc_id: "x".to_string(),
                fields: bt(&[("text", "hello world")]),
            },
        )
        .unwrap();

        let mut m = HashMap::new();
        m.insert(d, "x".to_string());

        // 搜索 "nonexistent" — 无匹配
        let results = graph_fts_search(
            &g,
            &fts,
            &GraphFtsQuery {
                graph_name: "kg",
                start_vertex: e,
                max_depth: 1,
                direction: Direction::Out,
                fts_index: "idx",
                query: "nonexistent",
                vertex_to_doc_id: &m,
                label_filter: None,
                limit: 10,
            },
        )
        .unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn graph_fts_empty_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let g = GraphEngine::open(&store).unwrap();
        g.create("g").unwrap();
        let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
        g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();

        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("idx", &FtsConfig::default()).unwrap();

        let m = HashMap::new();
        let results = graph_fts_search(
            &g,
            &fts,
            &GraphFtsQuery {
                graph_name: "g",
                start_vertex: a,
                max_depth: 1,
                direction: Direction::Out,
                fts_index: "idx",
                query: "test",
                vertex_to_doc_id: &m,
                label_filter: None,
                limit: 10,
            },
        )
        .unwrap();

        assert!(results.is_empty());
    }
}
