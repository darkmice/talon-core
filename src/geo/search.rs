/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO 搜索函数：圆形搜索、矩形搜索、围栏检测。
//! 从 mod.rs 拆分，保持单文件 ≤500 行。

use super::{decode_member, encode_member, geo_keyspace_name, geohash, haversine_distance};
use super::{make_geo_key, make_member_index_key, validate_coords};
use super::{GeoEngine, GeoMember, GeoPoint, GeoUnit};
use crate::error::Error;

impl GeoEngine {
    /// 圆形范围搜索：返回中心点 radius 范围内的成员，按距离排序。
    pub fn geo_search(
        &self,
        name: &str,
        center_lng: f64,
        center_lat: f64,
        radius: f64,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<Vec<GeoMember>, Error> {
        validate_coords(center_lng, center_lat)?;
        let radius_m = unit.to_meters(radius);
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let bits = geohash::optimal_precision(radius_m);
        let center_hash = geohash::encode(center_lng, center_lat, bits);
        let neighbors = geohash::neighbors(center_hash, bits);
        let mut results: Vec<GeoMember> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for prefix_hash in neighbors {
            let prefix_bytes = geohash::hash_to_prefix_bytes(prefix_hash, bits);
            ks.for_each_kv_prefix(&prefix_bytes, |key, raw| {
                if key.starts_with(b"idx:") {
                    return true;
                }
                if let Some((member_key, point)) = decode_member(raw) {
                    if seen.insert(member_key.clone()) {
                        let dist = haversine_distance(center_lat, center_lng, point.lat, point.lng);
                        if dist <= radius_m {
                            results.push(GeoMember {
                                key: member_key,
                                point,
                                dist: Some(unit.convert_meters(dist)),
                            });
                        }
                    }
                }
                true
            })?;
        }
        results.sort_by(|a, b| {
            a.dist
                .unwrap_or(f64::MAX)
                .partial_cmp(&b.dist.unwrap_or(f64::MAX))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if let Some(n) = count {
            results.truncate(n);
        }
        Ok(results)
    }

    /// 矩形范围搜索：返回经纬度矩形区域内的成员。
    /// 使用 geohash 前缀剪枝（外接圆 → neighbors）避免全量扫描。
    pub fn geo_search_box(
        &self,
        name: &str,
        min_lng: f64,
        min_lat: f64,
        max_lng: f64,
        max_lat: f64,
        count: Option<usize>,
    ) -> Result<Vec<GeoMember>, Error> {
        validate_coords(min_lng, min_lat)?;
        validate_coords(max_lng, max_lat)?;
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let center_lng = (min_lng + max_lng) / 2.0;
        let center_lat = (min_lat + max_lat) / 2.0;
        let radius_m = haversine_distance(center_lat, center_lng, max_lat, max_lng);
        let bits = geohash::optimal_precision(radius_m * 2.0);
        let center_hash = geohash::encode(center_lng, center_lat, bits);
        let prefixes = geohash::neighbors(center_hash, bits);
        let mut results: Vec<GeoMember> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for prefix_hash in prefixes {
            let prefix_bytes = geohash::hash_to_prefix_bytes(prefix_hash, bits);
            ks.for_each_kv_prefix(&prefix_bytes, |key, raw| {
                if key.starts_with(b"idx:") {
                    return true;
                }
                if let Some((member_key, point)) = decode_member(raw) {
                    if point.lng >= min_lng
                        && point.lng <= max_lng
                        && point.lat >= min_lat
                        && point.lat <= max_lat
                        && seen.insert(member_key.clone())
                    {
                        results.push(GeoMember {
                            key: member_key,
                            point,
                            dist: None,
                        });
                    }
                }
                true
            })?;
        }
        results.sort_by(|a, b| {
            a.point
                .lng
                .partial_cmp(&b.point.lng)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if let Some(n) = count {
            results.truncate(n);
        }
        Ok(results)
    }

    /// 围栏检测：判断坐标是否在指定圆形区域内。
    pub fn geo_fence(
        &self,
        name: &str,
        member_key: &str,
        center_lng: f64,
        center_lat: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> Result<Option<bool>, Error> {
        let point = match self.geo_pos(name, member_key)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let dist_m = haversine_distance(center_lat, center_lng, point.lat, point.lng);
        let radius_m = unit.to_meters(radius);
        Ok(Some(dist_m <= radius_m))
    }

    /// 圆形搜索结果存入新 GEO 集合，返回存入的成员数。
    ///
    /// 对标 Redis `GEOSEARCHSTORE`：搜索 `name` 中的成员，
    /// 结果写入 `dest`（已有数据会被清空）。
    pub fn geo_search_store(
        &self,
        name: &str,
        dest: &str,
        center_lng: f64,
        center_lat: f64,
        radius: f64,
        unit: GeoUnit,
        count: Option<usize>,
    ) -> Result<u64, Error> {
        let results = self.geo_search(name, center_lng, center_lat, radius, unit, count)?;
        self.store_members(dest, &results)
    }

    /// 矩形搜索结果存入新 GEO 集合，返回存入的成员数。
    ///
    /// 对标 Redis `GEOSEARCHSTORE` BYBOX 模式。
    pub fn geo_search_box_store(
        &self,
        name: &str,
        dest: &str,
        min_lng: f64,
        min_lat: f64,
        max_lng: f64,
        max_lat: f64,
        count: Option<usize>,
    ) -> Result<u64, Error> {
        let results = self.geo_search_box(name, min_lng, min_lat, max_lng, max_lat, count)?;
        self.store_members(dest, &results)
    }

    /// 将搜索结果写入目标 GEO 集合（内部辅助）。
    fn store_members(&self, dest: &str, members: &[GeoMember]) -> Result<u64, Error> {
        // 创建/打开 dest 集合
        self.create(dest)?;
        let ks = self.store.open_keyspace(&geo_keyspace_name(dest))?;
        // 清空已有数据：分批删除避免 OOM（每批 1000 key）
        loop {
            let mut old_keys: Vec<Vec<u8>> = Vec::with_capacity(1000);
            ks.for_each_kv_prefix(b"", |key, _| {
                old_keys.push(key.to_vec());
                old_keys.len() < 1000
            })?;
            if old_keys.is_empty() {
                break;
            }
            let mut del_batch = self.store.batch();
            for k in &old_keys {
                del_batch.remove(&ks, k.clone());
            }
            del_batch.commit()?;
        }
        // 批量写入
        let mut batch = self.store.batch();
        for m in members {
            let point = GeoPoint {
                lng: m.point.lng,
                lat: m.point.lat,
            };
            let geo_key = make_geo_key(&point, &m.key);
            let value = encode_member(&m.key, &point);
            let idx_key = make_member_index_key(&m.key);
            batch.insert(&ks, geo_key.clone(), value)?;
            batch.insert(&ks, idx_key, geo_key)?;
        }
        batch.commit()?;
        Ok(members.len() as u64)
    }
}
