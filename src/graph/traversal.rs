/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Graph 图遍历算法：BFS、最短路径。

use std::collections::{HashMap, HashSet, VecDeque};

use super::encoding::Direction;
use super::GraphEngine;
use crate::error::Error;

impl GraphEngine {
    /// BFS 广度优先遍历，返回 (vertex_id, depth) 列表。
    ///
    /// `max_depth` 限制最大遍历深度，防止无界遍历。
    pub fn bfs(
        &self,
        graph: &str,
        start: u64,
        max_depth: usize,
        direction: Direction,
    ) -> Result<Vec<(u64, usize)>, Error> {
        let mut visited: HashSet<u64> = HashSet::new();
        let mut queue: VecDeque<(u64, usize)> = VecDeque::new();
        let mut result: Vec<(u64, usize)> = Vec::new();

        visited.insert(start);
        queue.push_back((start, 0));
        result.push((start, 0));

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let neighbors = self.neighbors(graph, current, direction)?;
            for n in neighbors {
                if visited.insert(n) {
                    let next_depth = depth + 1;
                    queue.push_back((n, next_depth));
                    result.push((n, next_depth));
                }
            }
        }
        Ok(result)
    }

    /// 最短路径（BFS），返回从 `from` 到 `to` 的节点路径。
    ///
    /// 若不可达或超过 `max_depth`，返回 None。
    pub fn shortest_path(
        &self,
        graph: &str,
        from: u64,
        to: u64,
        max_depth: usize,
    ) -> Result<Option<Vec<u64>>, Error> {
        if from == to {
            return Ok(Some(vec![from]));
        }

        let mut visited: HashSet<u64> = HashSet::new();
        let mut queue: VecDeque<(u64, usize)> = VecDeque::new();
        // parent 记录：child → parent，用于回溯路径
        let mut parent: HashMap<u64, u64> = HashMap::new();

        visited.insert(from);
        queue.push_back((from, 0));

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let neighbors = self.neighbors(graph, current, Direction::Out)?;
            for n in neighbors {
                if !visited.insert(n) {
                    continue;
                }
                parent.insert(n, current);
                if n == to {
                    // 回溯路径
                    let mut path = vec![to];
                    let mut cur = to;
                    while let Some(&p) = parent.get(&cur) {
                        path.push(p);
                        cur = p;
                        if cur == from {
                            break;
                        }
                    }
                    path.reverse();
                    return Ok(Some(path));
                }
                queue.push_back((n, depth + 1));
            }
        }
        Ok(None)
    }

    /// 多跳邻居查询：返回 n 跳内可达的所有节点 ID（不含起点）。
    pub fn k_hop_neighbors(
        &self,
        graph: &str,
        start: u64,
        k: usize,
        direction: Direction,
    ) -> Result<Vec<u64>, Error> {
        let nodes = self.bfs(graph, start, k, direction)?;
        Ok(nodes
            .into_iter()
            .filter(|(id, _)| *id != start)
            .map(|(id, _)| id)
            .collect())
    }

    /// 属性过滤 BFS：遍历时仅展开满足条件的节点。
    ///
    /// `filter` 回调接收 `(vertex_id, &Vertex)`，返回 true 表示保留该节点并继续展开。
    /// 不满足条件的节点不会被加入结果集，也不会从该节点继续遍历。
    pub fn bfs_filter<F>(
        &self,
        graph: &str,
        start: u64,
        max_depth: usize,
        direction: Direction,
        filter: F,
    ) -> Result<Vec<(u64, usize)>, Error>
    where
        F: Fn(u64, &super::encoding::Vertex) -> bool,
    {
        let mut visited: HashSet<u64> = HashSet::new();
        let mut queue: VecDeque<(u64, usize)> = VecDeque::new();
        let mut result: Vec<(u64, usize)> = Vec::new();

        visited.insert(start);
        queue.push_back((start, 0));
        result.push((start, 0));

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let neighbors = self.neighbors(graph, current, direction)?;
            for n in neighbors {
                if !visited.insert(n) {
                    continue;
                }
                // 加载节点并检查过滤条件
                let vertex = match self.get_vertex(graph, n)? {
                    Some(v) => v,
                    None => continue,
                };
                if !filter(n, &vertex) {
                    continue;
                }
                let next_depth = depth + 1;
                queue.push_back((n, next_depth));
                result.push((n, next_depth));
            }
        }
        Ok(result)
    }

    /// 度中心性：返回每个节点的 (出度, 入度, 总度)。
    ///
    /// 用于识别知识图谱中的关键实体（高度数 = 高关联性）。
    /// 返回 `Vec<(vertex_id, out_degree, in_degree)>`，按总度降序排序。
    /// 直接遍历存活节点 keyspace，不依赖 next_vertex_id（避免扫描已删除节点 ID）。
    pub fn degree_centrality(
        &self,
        graph: &str,
        limit: usize,
    ) -> Result<Vec<(u64, usize, usize)>, Error> {
        let h = self.open_graph(graph)?;
        let vertex_ids = self.collect_vertex_ids(graph)?;
        let mut degrees: Vec<(u64, usize, usize)> = Vec::new();
        for vid in vertex_ids {
            // 直接 prefix count，避免加载完整边数据
            let out_deg = h.out_idx.count_prefix(&vid.to_be_bytes())? as usize;
            let in_deg = h.in_idx.count_prefix(&vid.to_be_bytes())? as usize;
            degrees.push((vid, out_deg, in_deg));
        }
        degrees.sort_by(|a, b| (b.1 + b.2).cmp(&(a.1 + a.2)));
        degrees.truncate(limit);
        Ok(degrees)
    }

    /// PageRank 算法：迭代计算节点重要性。
    ///
    /// - `damping`: 阻尼因子（通常 0.85）
    /// - `iterations`: 迭代次数
    /// - `limit`: 返回 top-N 节点
    ///
    /// 返回 `Vec<(vertex_id, score)>`，按 score 降序排序。
    /// 直接遍历存活节点 keyspace，不依赖 next_vertex_id。
    pub fn pagerank(
        &self,
        graph: &str,
        damping: f64,
        iterations: usize,
        limit: usize,
    ) -> Result<Vec<(u64, f64)>, Error> {
        let vertices = self.collect_vertex_ids(graph)?;
        let n = vertices.len();
        if n == 0 {
            return Ok(vec![]);
        }

        // 初始化分数
        let init_score = 1.0 / n as f64;
        let mut scores: HashMap<u64, f64> = vertices.iter().map(|&v| (v, init_score)).collect();

        // 预计算所有节点的出度和入边关系（一次性 I/O，避免迭代中重复读盘）
        let mut out_degrees: HashMap<u64, usize> = HashMap::new();
        let mut in_neighbors: HashMap<u64, Vec<u64>> = HashMap::new();
        for &v in &vertices {
            out_degrees.insert(v, self.out_edges(graph, v)?.len());
            let ie = self.in_edges(graph, v)?;
            in_neighbors.insert(v, ie.into_iter().map(|e| e.from).collect());
        }

        // 迭代（纯内存计算，无 I/O）
        for _ in 0..iterations {
            let mut new_scores: HashMap<u64, f64> = HashMap::new();
            let base = (1.0 - damping) / n as f64;

            for &v in &vertices {
                let mut sum = 0.0;
                if let Some(sources) = in_neighbors.get(&v) {
                    for &from in sources {
                        let from_score = scores.get(&from).copied().unwrap_or(0.0);
                        let from_out = *out_degrees.get(&from).unwrap_or(&1);
                        sum += from_score / from_out.max(1) as f64;
                    }
                }
                new_scores.insert(v, base + damping * sum);
            }
            scores = new_scores;
        }

        let mut result: Vec<(u64, f64)> = scores.into_iter().collect();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        result.truncate(limit);
        Ok(result)
    }

    /// 辅助方法：直接遍历 vertices keyspace 收集所有存活节点 ID。
    /// 避免依赖 next_vertex_id（删除节点后会有空洞）。
    fn collect_vertex_ids(&self, graph: &str) -> Result<Vec<u64>, Error> {
        let h = self.open_graph(graph)?;
        let mut ids = Vec::new();
        h.vertices.for_each_key_prefix(&[], |key| {
            if let Some(id) = super::encoding::key_to_id(key) {
                ids.push(id);
            }
            true
        })?;
        Ok(ids)
    }

    /// 带权最短路径（Dijkstra，BinaryHeap 实现，O((V+E) log V)）。
    ///
    /// `weight_key` 指定边属性中表示权重的 key（如 "weight"），缺失时默认权重 1.0。
    /// 返回 `(path, total_weight)`，不可达时返回 None。
    ///
    /// **注意**：Dijkstra 不支持负权边。如果边属性中存在负数权重，结果可能不正确。
    pub fn weighted_shortest_path(
        &self,
        graph: &str,
        from: u64,
        to: u64,
        max_depth: usize,
        weight_key: &str,
    ) -> Result<Option<(Vec<u64>, f64)>, Error> {
        if from == to {
            return Ok(Some((vec![from], 0.0)));
        }

        // Dijkstra：dist[node] = 最短距离, parent[node] = 前驱
        let mut dist: HashMap<u64, f64> = HashMap::new();
        let mut parent: HashMap<u64, u64> = HashMap::new();
        let mut visited: HashSet<u64> = HashSet::new();
        // BinaryHeap 最小堆：每次 pop 返回最小距离节点，O(log N)
        let mut frontier: std::collections::BinaryHeap<DijkstraItem> =
            std::collections::BinaryHeap::new();

        dist.insert(from, 0.0);
        frontier.push(DijkstraItem(0.0, from, 0));

        while let Some(DijkstraItem(d, current, depth)) = frontier.pop() {
            if !visited.insert(current) {
                continue;
            }
            if current == to {
                // 回溯路径
                let mut path = vec![to];
                let mut cur = to;
                while let Some(&p) = parent.get(&cur) {
                    path.push(p);
                    cur = p;
                    if cur == from {
                        break;
                    }
                }
                path.reverse();
                return Ok(Some((path, d)));
            }
            if depth >= max_depth {
                continue;
            }

            // 展开出边
            let edges = self.out_edges(graph, current)?;
            for edge in &edges {
                if visited.contains(&edge.to) {
                    continue;
                }
                let w: f64 = edge
                    .properties
                    .get(weight_key)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1.0);
                let new_dist = d + w;
                let old_dist = dist.get(&edge.to).copied().unwrap_or(f64::INFINITY);
                if new_dist < old_dist {
                    dist.insert(edge.to, new_dist);
                    parent.insert(edge.to, current);
                    frontier.push(DijkstraItem(new_dist, edge.to, depth + 1));
                }
            }
        }
        Ok(None)
    }
}

// ── Dijkstra BinaryHeap 辅助结构 ────────────────────────────
// BinaryHeap 是大顶堆，通过反转 Ord 实现小顶堆（最小距离优先）。
// 对标 Rust 标准库推荐的 Dijkstra 实现模式。

/// Dijkstra 优先队列元素：(距离, 节点 ID, 深度)。
#[derive(Clone, PartialEq)]
struct DijkstraItem(f64, u64, usize);

// Safety: f64 本身不满足 Eq（NaN != NaN），但 Dijkstra 算法中边权始终通过
// `parse().ok().unwrap_or(1.0)` 获取，NaN 不可能出现。此外 Ord::cmp 中使用
// `unwrap_or(Ordering::Equal)` 作为额外防护。BinaryHeap 要求 Ord 而 Ord 要求 Eq。
impl Eq for DijkstraItem {}

impl PartialOrd for DijkstraItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DijkstraItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 反转比较：距离越小优先级越高（BinaryHeap 是大顶堆）
        other
            .0
            .partial_cmp(&self.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}
