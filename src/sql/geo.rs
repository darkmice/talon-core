/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M94：地理空间计算模块 — GeoHash 编码 + Haversine 距离。
//! 零外部依赖，纯 Rust 实现。
//! 注：geohash 系列函数待 ST_WITHIN 空间查询集成后启用。
#![allow(dead_code)]

/// 地球平均半径（米）。
const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// Haversine 距离（米）：两个 WGS-84 坐标间的大圆距离。
pub(super) fn haversine(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let (rlat1, rlat2) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + rlat1.cos() * rlat2.cos() * (dlng / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// GeoHash 编码：(lat, lng) → 12 字符 base32 字符串。
/// 精度 12 级 ≈ 3.7cm × 1.9cm，足以满足所有实际场景。
pub(super) fn encode_geohash(lat: f64, lng: f64) -> String {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let (mut lat_min, mut lat_max) = (-90.0_f64, 90.0_f64);
    let (mut lng_min, mut lng_max) = (-180.0_f64, 180.0_f64);
    let mut hash = String::with_capacity(12);
    let mut bits = 0u8;
    let mut bit_count = 0u8;
    let mut is_lng = true;
    while hash.len() < 12 {
        if is_lng {
            let mid = (lng_min + lng_max) / 2.0;
            if lng >= mid {
                bits = (bits << 1) | 1;
                lng_min = mid;
            } else {
                bits <<= 1;
                lng_max = mid;
            }
        } else {
            let mid = (lat_min + lat_max) / 2.0;
            if lat >= mid {
                bits = (bits << 1) | 1;
                lat_min = mid;
            } else {
                bits <<= 1;
                lat_max = mid;
            }
        }
        is_lng = !is_lng;
        bit_count += 1;
        if bit_count == 5 {
            hash.push(BASE32[bits as usize] as char);
            bits = 0;
            bit_count = 0;
        }
    }
    hash
}

/// GeoHash 邻域：返回目标 hash 及其 8 个相邻格子的前缀列表。
/// 用于 ST_WITHIN 的空间索引查询——对 9 个前缀做 B-Tree prefix scan。
pub(super) fn geohash_neighbors(lat: f64, lng: f64, precision: usize) -> Vec<String> {
    let center = encode_geohash(lat, lng);
    let prefix = &center[..precision.min(center.len())];
    // 计算每个 precision 级别的 lat/lng 跨度
    let (lat_bits, lng_bits) = bits_for_precision(precision);
    let lat_step = 180.0 / (1u64 << lat_bits) as f64;
    let lng_step = 360.0 / (1u64 << lng_bits) as f64;
    let mut result = Vec::with_capacity(9);
    result.push(prefix.to_string());
    for dlat in [-1.0_f64, 0.0, 1.0] {
        for dlng in [-1.0_f64, 0.0, 1.0] {
            if dlat == 0.0 && dlng == 0.0 {
                continue;
            }
            let nlat = (lat + dlat * lat_step).clamp(-90.0, 90.0);
            let nlng = wrap_lng(lng + dlng * lng_step);
            let nh = encode_geohash(nlat, nlng);
            let np = &nh[..precision.min(nh.len())];
            if np != prefix && !result.iter().any(|s| s == np) {
                result.push(np.to_string());
            }
        }
    }
    result
}

/// 根据 geohash precision 计算 lat/lng 各自的二进制位数。
fn bits_for_precision(precision: usize) -> (u32, u32) {
    let total_bits = (precision * 5) as u32;
    let lng_bits = total_bits.div_ceil(2);
    let lat_bits = total_bits / 2;
    (lat_bits, lng_bits)
}

/// 经度环绕：确保在 [-180, 180] 范围内。
fn wrap_lng(lng: f64) -> f64 {
    if lng > 180.0 {
        lng - 360.0
    } else if lng < -180.0 {
        lng + 360.0
    } else {
        lng
    }
}

/// 根据搜索半径（米）推荐 geohash 精度。
/// 精度越低覆盖面越大，减少邻域 scan 次数但增加误报。
pub(super) fn precision_for_radius(radius_m: f64) -> usize {
    if radius_m > 5000.0 {
        4
    } else if radius_m > 1000.0 {
        5
    } else if radius_m > 100.0 {
        6
    } else if radius_m > 10.0 {
        7
    } else {
        8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_known_distance() {
        // 北京天安门 → 上海东方明珠，约 1064km
        let d = haversine(39.9042, 116.4074, 31.2397, 121.4998);
        assert!((d - 1_064_000.0).abs() < 20_000.0, "distance: {}", d);
    }

    #[test]
    fn haversine_same_point() {
        let d = haversine(39.9, 116.4, 39.9, 116.4);
        assert!(d < 0.01);
    }

    #[test]
    fn geohash_encode_decode() {
        let h = encode_geohash(39.9042, 116.4074);
        assert_eq!(h.len(), 12);
        // 北京天安门的 geohash 应以 "wx4g" 开头
        assert!(h.starts_with("wx4g"), "hash: {}", h);
    }

    #[test]
    fn geohash_neighbors_count() {
        let ns = geohash_neighbors(39.9, 116.4, 5);
        assert!(ns.len() >= 5 && ns.len() <= 9, "neighbors: {:?}", ns);
        // 中心 hash 应在列表中
        let center = &encode_geohash(39.9, 116.4)[..5];
        assert!(ns.iter().any(|s| s == center));
    }

    #[test]
    fn precision_for_radius_sanity() {
        assert!(precision_for_radius(10_000.0) <= 4);
        assert!(precision_for_radius(500.0) >= 5);
        assert!(precision_for_radius(5.0) >= 7);
    }
}
