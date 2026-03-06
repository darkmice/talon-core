/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph 引擎单元测试。

use super::encoding::Direction;
use super::*;
use crate::storage::Store;
use std::collections::BTreeMap;

fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn vertex_crud() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("test").unwrap();

    let id = g
        .add_vertex("test", "Person", &props(&[("name", "Alice")]))
        .unwrap();
    assert_eq!(id, 1);

    let v = g.get_vertex("test", id).unwrap().unwrap();
    assert_eq!(v.label, "Person");
    assert_eq!(v.properties.get("name").unwrap(), "Alice");

    g.update_vertex("test", id, &props(&[("name", "Bob")]))
        .unwrap();
    let v2 = g.get_vertex("test", id).unwrap().unwrap();
    assert_eq!(v2.properties.get("name").unwrap(), "Bob");

    g.delete_vertex("test", id).unwrap();
    assert!(g.get_vertex("test", id).unwrap().is_none());
}

#[test]
fn edge_crud() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("test").unwrap();

    let a = g
        .add_vertex("test", "Person", &props(&[("name", "A")]))
        .unwrap();
    let b = g
        .add_vertex("test", "Person", &props(&[("name", "B")]))
        .unwrap();

    let eid = g
        .add_edge("test", a, b, "knows", &props(&[("since", "2024")]))
        .unwrap();
    assert_eq!(eid, 1);

    let e = g.get_edge("test", eid).unwrap().unwrap();
    assert_eq!(e.from, a);
    assert_eq!(e.to, b);
    assert_eq!(e.label, "knows");

    g.delete_edge("test", eid).unwrap();
    assert!(g.get_edge("test", eid).unwrap().is_none());
}

#[test]
fn edge_requires_valid_vertices() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("test").unwrap();

    let a = g.add_vertex("test", "X", &BTreeMap::new()).unwrap();
    let result = g.add_edge("test", a, 999, "rel", &BTreeMap::new());
    assert!(result.is_err());
}

#[test]
fn out_in_edges() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();

    g.add_edge("g", a, b, "r1", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, c, "r2", &BTreeMap::new()).unwrap();
    g.add_edge("g", b, c, "r3", &BTreeMap::new()).unwrap();

    let out_a = g.out_edges("g", a).unwrap();
    assert_eq!(out_a.len(), 2);

    let in_c = g.in_edges("g", c).unwrap();
    assert_eq!(in_c.len(), 2);

    let out_c = g.out_edges("g", c).unwrap();
    assert_eq!(out_c.len(), 0);
}

#[test]
fn neighbors_direction() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", c, a, "r", &BTreeMap::new()).unwrap();

    let out = g.neighbors("g", a, Direction::Out).unwrap();
    assert_eq!(out, vec![b]);

    let in_n = g.neighbors("g", a, Direction::In).unwrap();
    assert_eq!(in_n, vec![c]);

    let both = g.neighbors("g", a, Direction::Both).unwrap();
    assert_eq!(both.len(), 2);
    assert!(both.contains(&b));
    assert!(both.contains(&c));
}

#[test]
fn vertices_by_label() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    g.add_vertex("g", "Person", &props(&[("name", "A")]))
        .unwrap();
    g.add_vertex("g", "Person", &props(&[("name", "B")]))
        .unwrap();
    g.add_vertex("g", "Doc", &props(&[("title", "X")])).unwrap();

    let persons = g.vertices_by_label("g", "Person").unwrap();
    assert_eq!(persons.len(), 2);

    let docs = g.vertices_by_label("g", "Doc").unwrap();
    assert_eq!(docs.len(), 1);
}

#[test]
fn edges_by_label() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "knows", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "likes", &BTreeMap::new()).unwrap();
    g.add_edge("g", b, a, "knows", &BTreeMap::new()).unwrap();

    let knows = g.edges_by_label("g", "knows").unwrap();
    assert_eq!(knows.len(), 2);

    let likes = g.edges_by_label("g", "likes").unwrap();
    assert_eq!(likes.len(), 1);
}

#[test]
fn delete_vertex_cascades_edges() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let eid = g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();

    g.delete_vertex("g", a).unwrap();
    // 边应被级联删除
    assert!(g.get_edge("g", eid).unwrap().is_none());
    // b 的入边也应该被清理
    let in_b = g.in_edges("g", b).unwrap();
    assert_eq!(in_b.len(), 0);
}

#[test]
fn bfs_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // 链式图: 1 -> 2 -> 3 -> 4
    let n1 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n2 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n3 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n4 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", n1, n2, "next", &BTreeMap::new()).unwrap();
    g.add_edge("g", n2, n3, "next", &BTreeMap::new()).unwrap();
    g.add_edge("g", n3, n4, "next", &BTreeMap::new()).unwrap();

    let result = g.bfs("g", n1, 2, Direction::Out).unwrap();
    assert_eq!(result.len(), 3); // n1(0), n2(1), n3(2)
    assert_eq!(result[0], (n1, 0));
    assert_eq!(result[1], (n2, 1));
    assert_eq!(result[2], (n3, 2));

    // depth=1 只到 n2
    let result1 = g.bfs("g", n1, 1, Direction::Out).unwrap();
    assert_eq!(result1.len(), 2);
}

#[test]
fn shortest_path_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // 图: 1->2->4, 1->3->4
    let n1 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n2 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n3 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n4 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", n1, n2, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", n2, n4, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", n1, n3, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", n3, n4, "r", &BTreeMap::new()).unwrap();

    let path = g.shortest_path("g", n1, n4, 10).unwrap().unwrap();
    assert_eq!(path.len(), 3); // 两跳
    assert_eq!(path[0], n1);
    assert_eq!(path[2], n4);
}

#[test]
fn shortest_path_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let n1 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let n2 = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    // 无边连接

    let path = g.shortest_path("g", n1, n2, 10).unwrap();
    assert!(path.is_none());
}

#[test]
fn k_hop_neighbors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // 星形图: center -> a, center -> b, center -> c
    let center = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", center, a, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", center, b, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", center, c, "r", &BTreeMap::new()).unwrap();

    let n1 = g.k_hop_neighbors("g", center, 1, Direction::Out).unwrap();
    assert_eq!(n1.len(), 3);
}

#[test]
fn vertex_edge_count() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    assert_eq!(g.vertex_count("g").unwrap(), 0);
    assert_eq!(g.edge_count("g").unwrap(), 0);

    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();

    assert_eq!(g.vertex_count("g").unwrap(), 2);
    assert_eq!(g.edge_count("g").unwrap(), 1);
}

#[test]
fn bfs_filter_by_label() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g.add_vertex("g", "Start", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "Doc", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "Person", &BTreeMap::new()).unwrap();
    let d = g.add_vertex("g", "Doc", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, c, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", c, d, "r", &BTreeMap::new()).unwrap();

    // 只保留 Doc 标签的节点
    let result = g
        .bfs_filter("g", a, 2, Direction::Out, |_id, v| v.label == "Doc")
        .unwrap();
    // 起点 + b(Doc)，c(Person) 被过滤因此 d 不可达
    assert_eq!(result.len(), 2); // start + b
    assert!(result.iter().any(|(id, _)| *id == b));
    assert!(!result.iter().any(|(id, _)| *id == d));
}

#[test]
fn bfs_filter_by_property() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g
        .add_vertex("g", "N", &props(&[("active", "true")]))
        .unwrap();
    let b = g
        .add_vertex("g", "N", &props(&[("active", "true")]))
        .unwrap();
    let c = g
        .add_vertex("g", "N", &props(&[("active", "false")]))
        .unwrap();
    let d = g
        .add_vertex("g", "N", &props(&[("active", "true")]))
        .unwrap();
    g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, c, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", c, d, "r", &BTreeMap::new()).unwrap();

    let result = g
        .bfs_filter("g", a, 3, Direction::Out, |_id, v| {
            v.properties
                .get("active")
                .map(|s| s == "true")
                .unwrap_or(false)
        })
        .unwrap();
    // a(start) + b(active=true); c 被过滤，d 不可达
    assert_eq!(result.len(), 2);
}

#[test]
fn weighted_shortest_path_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // A --(1)--> B --(1)--> D  (total: 2)
    // A --(5)--> C --(0.1)--> D  (total: 5.1)
    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let d = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "r", &props(&[("w", "1.0")])).unwrap();
    g.add_edge("g", b, d, "r", &props(&[("w", "1.0")])).unwrap();
    g.add_edge("g", a, c, "r", &props(&[("w", "5.0")])).unwrap();
    g.add_edge("g", c, d, "r", &props(&[("w", "0.1")])).unwrap();

    let result = g
        .weighted_shortest_path("g", a, d, 10, "w")
        .unwrap()
        .unwrap();
    let (path, weight) = result;
    assert_eq!(path, vec![a, b, d]); // 权重 2.0 < 5.1
    assert!((weight - 2.0).abs() < 0.01);
}

#[test]
fn weighted_shortest_path_chooses_lighter() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // A --(10)--> B --(10)--> D  (total: 20)
    // A --(1)---> C --(1)---> D  (total: 2)
    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let d = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "r", &props(&[("cost", "10")]))
        .unwrap();
    g.add_edge("g", b, d, "r", &props(&[("cost", "10")]))
        .unwrap();
    g.add_edge("g", a, c, "r", &props(&[("cost", "1")]))
        .unwrap();
    g.add_edge("g", c, d, "r", &props(&[("cost", "1")]))
        .unwrap();

    let (path, weight) = g
        .weighted_shortest_path("g", a, d, 10, "cost")
        .unwrap()
        .unwrap();
    assert_eq!(path, vec![a, c, d]);
    assert!((weight - 2.0).abs() < 0.01);
}

#[test]
fn weighted_shortest_path_unreachable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();

    let result = g.weighted_shortest_path("g", a, b, 10, "w").unwrap();
    assert!(result.is_none());
}

#[test]
fn degree_centrality_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // 星形图: center → a, center → b, center → c, a → center
    let center = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", center, a, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", center, b, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", center, c, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, center, "r", &BTreeMap::new()).unwrap();

    let result = g.degree_centrality("g", 10).unwrap();
    // center: out=3, in=1, total=4 → 排第一
    assert_eq!(result[0].0, center);
    assert_eq!(result[0].1, 3); // out_degree
    assert_eq!(result[0].2, 1); // in_degree
}

#[test]
fn pagerank_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    // 简单图: A → B → C, A → C
    let a = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let b = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    let c = g.add_vertex("g", "N", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, b, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", b, c, "r", &BTreeMap::new()).unwrap();
    g.add_edge("g", a, c, "r", &BTreeMap::new()).unwrap();

    let result = g.pagerank("g", 0.85, 20, 10).unwrap();
    assert_eq!(result.len(), 3);
    // C 接收来自 A 和 B 的链接，应有最高 PageRank
    assert_eq!(result[0].0, c);
    assert!(result[0].1 > result[1].1);
}

#[test]
fn pagerank_empty_graph() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let g = GraphEngine::open(&store).unwrap();
    g.create("g").unwrap();

    let result = g.pagerank("g", 0.85, 10, 10).unwrap();
    assert!(result.is_empty());
}
