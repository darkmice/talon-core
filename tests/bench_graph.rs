/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph 引擎百万级基准测试。
//! 用法：cargo test --release --test bench_graph -- --nocapture

use std::collections::BTreeMap;
use std::time::Instant;
use talon::*;

const NODE_COUNT: usize = 1_000_000;

fn empty_props() -> BTreeMap<String, String> {
    BTreeMap::new()
}

#[test]
fn bench_graph_write_1m_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    let t0 = Instant::now();
    for i in 0..NODE_COUNT {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), format!("node_{}", i));
        g.add_vertex("bench", "Entity", &props).unwrap();
    }
    let ms = t0.elapsed().as_millis();
    let ops = NODE_COUNT as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} nodes written in {}ms — {:.0} ops/s",
        NODE_COUNT, ms, ops
    );
    assert!(ops > 1000.0, "Graph node write should exceed 1K ops/s");
}

#[test]
fn bench_graph_write_2m_edges() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    // 先创建节点
    let node_count = 10_000usize;
    for _ in 0..node_count {
        g.add_vertex("bench", "N", &empty_props()).unwrap();
    }

    // 创建 200 万条边（每个节点 200 条出边）
    let t0 = Instant::now();
    let mut edge_written = 0usize;
    for from in 1..=node_count as u64 {
        for j in 0..200u64 {
            let to = (from + j) % node_count as u64 + 1;
            g.add_edge("bench", from, to, "link", &empty_props())
                .unwrap();
            edge_written += 1;
        }
    }
    let ms = t0.elapsed().as_millis();
    let ops = edge_written as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} edges written in {}ms — {:.0} ops/s",
        edge_written, ms, ops
    );
    assert!(ops > 500.0, "Graph edge write should exceed 500 ops/s");
}

#[test]
fn bench_graph_read_vertex() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    let count = 100_000usize;
    for i in 0..count {
        let mut props = BTreeMap::new();
        props.insert("idx".to_string(), i.to_string());
        g.add_vertex("bench", "N", &props).unwrap();
    }

    // 随机读
    let read_count = 100_000usize;
    let t0 = Instant::now();
    for i in 0..read_count {
        let id = (i % count) as u64 + 1;
        let _ = g.get_vertex("bench", id).unwrap();
    }
    let ms = t0.elapsed().as_millis();
    let ops = read_count as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} vertex reads in {}ms — {:.0} ops/s",
        read_count, ms, ops
    );
    assert!(ops > 5000.0, "Graph vertex read should exceed 5K ops/s");
}

#[test]
fn bench_graph_neighbors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    // 创建星形图：1000 个中心节点，每个 50 条出边
    let centers = 1000usize;
    let fan_out = 50usize;
    let total_nodes = centers + centers * fan_out;

    for _ in 0..total_nodes {
        g.add_vertex("bench", "N", &empty_props()).unwrap();
    }

    let mut leaf_start = centers as u64 + 1;
    for c in 1..=centers as u64 {
        for _ in 0..fan_out {
            g.add_edge("bench", c, leaf_start, "r", &empty_props())
                .unwrap();
            leaf_start += 1;
        }
    }

    // 查询每个中心节点的出边邻居
    let t0 = Instant::now();
    let queries = 10_000usize;
    for i in 0..queries {
        let center = (i % centers) as u64 + 1;
        let n = g.neighbors("bench", center, Direction::Out).unwrap();
        assert_eq!(n.len(), fan_out);
    }
    let ms = t0.elapsed().as_millis();
    let qps = queries as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} neighbor queries (fan_out={}) in {}ms — {:.0} qps",
        queries, fan_out, ms, qps
    );
    assert!(qps > 100.0, "Graph neighbor query should exceed 100 qps");
}

#[test]
fn bench_graph_bfs() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    // 创建链式图：10000 个节点，链式连接
    let chain_len = 10_000usize;
    for _ in 0..chain_len {
        g.add_vertex("bench", "N", &empty_props()).unwrap();
    }
    for i in 1..chain_len as u64 {
        g.add_edge("bench", i, i + 1, "next", &empty_props())
            .unwrap();
    }

    // BFS 深度限制测试
    let t0 = Instant::now();
    let bfs_queries = 1000usize;
    for _ in 0..bfs_queries {
        let result = g.bfs("bench", 1, 100, Direction::Out).unwrap();
        assert_eq!(result.len(), 101); // 起点 + 100 层
    }
    let ms = t0.elapsed().as_millis();
    let qps = bfs_queries as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} BFS queries (depth=100, chain=10K) in {}ms — {:.0} qps",
        bfs_queries, ms, qps
    );
    assert!(qps > 1.0, "Graph BFS should exceed 1 qps");
}

#[test]
fn bench_graph_shortest_path() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    // 网格图：100x100 节点
    let side = 100usize;
    let total = side * side;
    for _ in 0..total {
        g.add_vertex("bench", "N", &empty_props()).unwrap();
    }
    // 右向和下向连接
    for row in 0..side {
        for col in 0..side {
            let id = (row * side + col) as u64 + 1;
            if col + 1 < side {
                g.add_edge("bench", id, id + 1, "r", &empty_props())
                    .unwrap();
            }
            if row + 1 < side {
                g.add_edge("bench", id, id + side as u64, "d", &empty_props())
                    .unwrap();
            }
        }
    }

    // 最短路径查询
    let t0 = Instant::now();
    let queries = 100usize;
    for _ in 0..queries {
        let path = g.shortest_path("bench", 1, total as u64, side * 2).unwrap();
        assert!(path.is_some());
        let p = path.unwrap();
        // 最短路径长度 = (side-1)*2 + 1 = 199
        assert_eq!(p.len(), side * 2 - 1);
    }
    let ms = t0.elapsed().as_millis();
    let qps = queries as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} shortest_path queries (100x100 grid) in {}ms — {:.0} qps",
        queries, ms, qps
    );
}

#[test]
fn bench_graph_label_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("bench").unwrap();

    // 创建 3 种标签各 10K 节点
    let per_label = 10_000usize;
    for i in 0..per_label {
        let mut props = BTreeMap::new();
        props.insert("idx".to_string(), i.to_string());
        g.add_vertex("bench", "Person", &props).unwrap();
    }
    for i in 0..per_label {
        let mut props = BTreeMap::new();
        props.insert("idx".to_string(), i.to_string());
        g.add_vertex("bench", "Document", &props).unwrap();
    }
    for i in 0..per_label {
        let mut props = BTreeMap::new();
        props.insert("idx".to_string(), i.to_string());
        g.add_vertex("bench", "Topic", &props).unwrap();
    }

    // 标签查询
    let t0 = Instant::now();
    let queries = 100usize;
    for _ in 0..queries {
        let persons = g.vertices_by_label("bench", "Person").unwrap();
        assert_eq!(persons.len(), per_label);
    }
    let ms = t0.elapsed().as_millis();
    let qps = queries as f64 / (ms as f64 / 1000.0);

    println!(
        "[Graph] {} label queries (10K results each) in {}ms — {:.0} qps",
        queries, ms, qps
    );
}
