/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 跨引擎联合查询模块。
//!
//! 依赖方向：cross（顶层）→ geo + vector + graph（引擎层），符合模块依赖规范。
//! 子模块：
//! - `geo_vector`：GEO + Vector 联合搜索
//! - `graph_vector`：Graph + Vector 联合搜索 (GraphRAG)
//! - `graph_fts`：Graph + FTS 联合搜索（知识图谱全文检索）
//! - `triple`：GEO + Graph + Vector 三引擎联合搜索
//! - `distance`：共享向量距离度量函数

pub(crate) mod distance;
pub mod geo_vector;
pub mod graph_fts;
pub mod graph_vector;
pub mod triple;

pub use geo_vector::{
    geo_box_vector_search, geo_vector_search, GeoBoxVectorQuery, GeoVectorHit, GeoVectorQuery,
};
pub use graph_fts::{graph_fts_search, GraphFtsHit, GraphFtsQuery};
pub use graph_vector::{graph_vector_search, GraphVectorHit, GraphVectorQuery};
pub use triple::{triple_search, TripleHit, TripleQuery};
