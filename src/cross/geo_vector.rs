/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO + Vector 联合搜索：按地理范围过滤，再用向量相似度重排序。

use std::collections::HashMap;

use super::distance::resolve_dist_fn;
use crate::error::Error;
use crate::geo::{GeoEngine, GeoMember, GeoPoint, GeoUnit};
use crate::vector::VectorEngine;

/// GEO + 向量联合查询命中结果。
#[derive(Debug, Clone)]
pub struct GeoVectorHit {
    /// GEO 成员标识。
    pub key: String,
    /// 地理坐标。
    pub point: GeoPoint,
    /// 距离搜索中心（米）。
    pub dist_m: f64,
    /// 向量相似度得分（越小越相似，L2/cosine distance）。
    pub vector_score: f32,
}

/// GEO + 向量联合搜索参数。
#[derive(Debug, Clone)]
pub struct GeoVectorQuery<'a> {
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
    /// GEO key → Vector ID 映射。
    pub key_to_vec_id: &'a HashMap<String, u64>,
    /// 最大返回数。
    pub limit: usize,
}

/// GEO + 向量联合搜索：先按地理范围过滤，再用向量相似度重排序。
pub fn geo_vector_search(
    geo: &GeoEngine,
    vec_engine: &VectorEngine,
    q: &GeoVectorQuery<'_>,
) -> Result<Vec<GeoVectorHit>, Error> {
    let candidates = geo.geo_search(
        q.geo_name,
        q.center_lng,
        q.center_lat,
        q.radius,
        q.unit,
        None,
    )?;
    rerank_candidates(
        vec_engine,
        &candidates,
        q.query_vec,
        q.metric,
        q.key_to_vec_id,
        q.limit,
    )
}

/// GEO 矩形 + 向量联合搜索参数。
#[derive(Debug, Clone)]
pub struct GeoBoxVectorQuery<'a> {
    /// GEO 空间名。
    pub geo_name: &'a str,
    /// 矩形最小经度。
    pub min_lng: f64,
    /// 矩形最小纬度。
    pub min_lat: f64,
    /// 矩形最大经度。
    pub max_lng: f64,
    /// 矩形最大纬度。
    pub max_lat: f64,
    /// 查询向量。
    pub query_vec: &'a [f32],
    /// 距离度量。
    pub metric: &'a str,
    /// GEO key → Vector ID 映射。
    pub key_to_vec_id: &'a HashMap<String, u64>,
    /// 最大返回数。
    pub limit: usize,
}

/// GEO 矩形 + 向量联合搜索：先按矩形范围过滤，再用向量相似度重排序。
pub fn geo_box_vector_search(
    geo: &GeoEngine,
    vec_engine: &VectorEngine,
    q: &GeoBoxVectorQuery<'_>,
) -> Result<Vec<GeoVectorHit>, Error> {
    let candidates =
        geo.geo_search_box(q.geo_name, q.min_lng, q.min_lat, q.max_lng, q.max_lat, None)?;
    rerank_candidates(
        vec_engine,
        &candidates,
        q.query_vec,
        q.metric,
        q.key_to_vec_id,
        q.limit,
    )
}

/// 内部共享：对 GEO 候选集做向量重排序。
fn rerank_candidates(
    vec_engine: &VectorEngine,
    candidates: &[GeoMember],
    query_vec: &[f32],
    metric: &str,
    key_to_vec_id: &HashMap<String, u64>,
    limit: usize,
) -> Result<Vec<GeoVectorHit>, Error> {
    if candidates.is_empty() {
        return Ok(vec![]);
    }
    let dist_fn = resolve_dist_fn(metric)?;
    let mut hits: Vec<GeoVectorHit> = Vec::new();
    for member in candidates {
        let Some(&vec_id) = key_to_vec_id.get(&member.key) else {
            continue;
        };
        let Some(vec_data) = vec_engine.get_vector(vec_id)? else {
            continue;
        };
        if vec_data.len() != query_vec.len() {
            continue;
        }
        let score = dist_fn(query_vec, &vec_data);
        hits.push(GeoVectorHit {
            key: member.key.clone(),
            point: member.point,
            dist_m: member.dist.unwrap_or(0.0),
            vector_score: score,
        });
    }
    hits.sort_by(|a, b| {
        a.vector_score
            .partial_cmp(&b.vector_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    hits.truncate(limit);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;

    #[test]
    fn geo_vector_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let geo = GeoEngine::open(&store).unwrap();
        geo.create("shops").unwrap();
        geo.geo_add("shops", "coffee_a", 116.40, 39.91).unwrap();
        geo.geo_add("shops", "coffee_b", 116.41, 39.92).unwrap();
        geo.geo_add("shops", "tea_c", 116.39, 39.90).unwrap();

        let vec_eng = VectorEngine::open(&store, "shop_embed").unwrap();
        vec_eng.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        vec_eng.insert(2, &[0.9, 0.1, 0.0]).unwrap();
        vec_eng.insert(3, &[0.0, 0.0, 1.0]).unwrap();

        let mut mapping = HashMap::new();
        mapping.insert("coffee_a".to_string(), 1u64);
        mapping.insert("coffee_b".to_string(), 2u64);
        mapping.insert("tea_c".to_string(), 3u64);

        let query = [0.95, 0.05, 0.0];
        let results = geo_vector_search(
            &geo,
            &vec_eng,
            &GeoVectorQuery {
                geo_name: "shops",
                center_lng: 116.40,
                center_lat: 39.91,
                radius: 5.0,
                unit: GeoUnit::Kilometers,
                query_vec: &query,
                metric: "cosine",
                key_to_vec_id: &mapping,
                limit: 10,
            },
        )
        .unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].key, "coffee_a");
    }

    #[test]
    fn geo_vector_no_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let geo = GeoEngine::open(&store).unwrap();
        geo.create("empty").unwrap();
        geo.geo_add("empty", "a", 116.40, 39.91).unwrap();
        let vec_eng = VectorEngine::open(&store, "v_empty").unwrap();
        let mapping = HashMap::new();
        let results = geo_vector_search(
            &geo,
            &vec_eng,
            &GeoVectorQuery {
                geo_name: "empty",
                center_lng: 116.40,
                center_lat: 39.91,
                radius: 10.0,
                unit: GeoUnit::Kilometers,
                query_vec: &[1.0, 0.0],
                metric: "cosine",
                key_to_vec_id: &mapping,
                limit: 10,
            },
        )
        .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn geo_box_vector_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let geo = GeoEngine::open(&store).unwrap();
        geo.create("pois").unwrap();
        geo.geo_add("pois", "p1", 116.40, 39.91).unwrap();
        geo.geo_add("pois", "p2", 116.41, 39.92).unwrap();
        let vec_eng = VectorEngine::open(&store, "poi_vec").unwrap();
        vec_eng.insert(10, &[1.0, 0.0]).unwrap();
        vec_eng.insert(20, &[0.0, 1.0]).unwrap();
        let mut mapping = HashMap::new();
        mapping.insert("p1".to_string(), 10u64);
        mapping.insert("p2".to_string(), 20u64);
        let results = geo_box_vector_search(
            &geo,
            &vec_eng,
            &GeoBoxVectorQuery {
                geo_name: "pois",
                min_lng: 116.38,
                min_lat: 39.89,
                max_lng: 116.42,
                max_lat: 39.93,
                query_vec: &[0.1, 0.9],
                metric: "l2",
                key_to_vec_id: &mapping,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "p2");
    }
}
