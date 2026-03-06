/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Geohash 编码/解码：52-bit 精度，支持邻居计算和前缀匹配。
//!
//! 算法：交替编码经度和纬度位，经度占偶数位，纬度占奇数位。
//! 52-bit 精度约 0.6m，满足街道级需求。

/// 将经纬度编码为 N-bit Geohash（存储在 u64 高位）。
pub(super) fn encode(lng: f64, lat: f64, bits: u32) -> u64 {
    let mut min_lng = -180.0f64;
    let mut max_lng = 180.0f64;
    let mut min_lat = -85.05112878f64;
    let mut max_lat = 85.05112878f64;
    let mut hash: u64 = 0;
    for i in 0..bits {
        hash <<= 1;
        if i % 2 == 0 {
            // 经度位
            let mid = (min_lng + max_lng) / 2.0;
            if lng >= mid {
                hash |= 1;
                min_lng = mid;
            } else {
                max_lng = mid;
            }
        } else {
            // 纬度位
            let mid = (min_lat + max_lat) / 2.0;
            if lat >= mid {
                hash |= 1;
                min_lat = mid;
            } else {
                max_lat = mid;
            }
        }
    }
    hash
}

/// 将 Geohash 解码为经纬度中心点。
#[allow(dead_code)]
pub(super) fn decode(hash: u64, bits: u32) -> (f64, f64) {
    let mut min_lng = -180.0f64;
    let mut max_lng = 180.0f64;
    let mut min_lat = -85.05112878f64;
    let mut max_lat = 85.05112878f64;
    for i in 0..bits {
        let bit = (hash >> (bits - 1 - i)) & 1;
        if i % 2 == 0 {
            let mid = (min_lng + max_lng) / 2.0;
            if bit == 1 {
                min_lng = mid;
            } else {
                max_lng = mid;
            }
        } else {
            let mid = (min_lat + max_lat) / 2.0;
            if bit == 1 {
                min_lat = mid;
            } else {
                max_lat = mid;
            }
        }
    }
    ((min_lng + max_lng) / 2.0, (min_lat + max_lat) / 2.0)
}

/// 计算给定半径（米）所需的最优 Geohash 位数。
/// 返回值在 [4, 52] 之间，确保 Geohash 单元格覆盖搜索半径。
pub(super) fn optimal_precision(radius_m: f64) -> u32 {
    // 每个精度级别的近似单元格大小（米）
    // bits: 4→5000km, 8→630km, 12→78km, 16→10km, 20→1.2km,
    //       24→150m, 28→19m, 32→2.4m, 36→0.3m
    let levels: &[(u32, f64)] = &[
        (4, 5_000_000.0),
        (8, 630_000.0),
        (12, 78_000.0),
        (16, 10_000.0),
        (20, 1_200.0),
        (24, 150.0),
        (28, 19.0),
        (32, 2.4),
        (36, 0.3),
    ];
    for &(bits, cell_size) in levels {
        if cell_size <= radius_m * 2.0 {
            return bits;
        }
    }
    26 // 默认 ~76m 精度
}

/// 获取 Geohash 的 9 个邻居（含自身），用于范围搜索。
/// 返回中心 + 8 方向邻居的 hash 值。
pub(super) fn neighbors(hash: u64, bits: u32) -> Vec<u64> {
    let mut result = Vec::with_capacity(9);
    result.push(hash);
    // 分离经度位和纬度位
    let lng_bits = bits.div_ceil(2);
    let lat_bits = bits / 2;
    let (lng_val, lat_val) = deinterleave(hash, bits);
    let deltas: &[(i64, i64)] = &[
        (-1, -1),
        (-1, 0),
        (-1, 1),
        (0, -1),
        (0, 1),
        (1, -1),
        (1, 0),
        (1, 1),
    ];
    let lng_max = (1u64 << lng_bits) as i64;
    let lat_max = (1u64 << lat_bits) as i64;
    for &(dlng, dlat) in deltas {
        let new_lng = (lng_val as i64 + dlng).rem_euclid(lng_max) as u64;
        let new_lat = lat_val as i64 + dlat;
        if new_lat < 0 || new_lat >= lat_max {
            continue;
        }
        let neighbor = interleave(new_lng, new_lat as u64, bits);
        result.push(neighbor);
    }
    result
}

/// 将 Geohash（指定位数）转为前缀字节数组，用于 keyspace prefix scan。
pub(super) fn hash_to_prefix_bytes(hash: u64, bits: u32) -> Vec<u8> {
    // 将 hash 左移到 64 位高位，然后取前 ceil(bits/8) 字节
    let shifted = hash << (64 - bits);
    let num_bytes = (bits as usize).div_ceil(8);
    let full_bytes = shifted.to_be_bytes();
    full_bytes[..num_bytes].to_vec()
}

/// 分离交错的经度位和纬度位。
fn deinterleave(hash: u64, bits: u32) -> (u64, u64) {
    let mut lng_val = 0u64;
    let mut lat_val = 0u64;
    let mut lng_bit = 0u32;
    let mut lat_bit = 0u32;
    for i in 0..bits {
        let bit = (hash >> (bits - 1 - i)) & 1;
        if i % 2 == 0 {
            lng_val |= bit << (bits.div_ceil(2) - 1 - lng_bit);
            lng_bit += 1;
        } else {
            lat_val |= bit << ((bits / 2) - 1 - lat_bit);
            lat_bit += 1;
        }
    }
    (lng_val, lat_val)
}

/// 交错经度位和纬度位为 Geohash。
fn interleave(lng_val: u64, lat_val: u64, bits: u32) -> u64 {
    let lng_bits = bits.div_ceil(2);
    let lat_bits = bits / 2;
    let mut hash = 0u64;
    let mut lng_bit = 0u32;
    let mut lat_bit = 0u32;
    for i in 0..bits {
        hash <<= 1;
        if i % 2 == 0 {
            hash |= (lng_val >> (lng_bits - 1 - lng_bit)) & 1;
            lng_bit += 1;
        } else {
            hash |= (lat_val >> (lat_bits - 1 - lat_bit)) & 1;
            lat_bit += 1;
        }
    }
    hash
}

/// 标准 geohash base32 字母表（去掉 a/i/l/o）。
const BASE32_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// 将经纬度编码为 11 字符 base32 geohash 字符串（52-bit 精度，与 Redis 一致）。
pub(super) fn encode_to_base32(lng: f64, lat: f64) -> String {
    let hash = encode(lng, lat, 52);
    // 52 bit → 左移到 55 bit（11 × 5），高位对齐
    let padded = hash << 3; // 55 - 52 = 3
    let mut buf = [0u8; 11];
    for i in 0..11 {
        let idx = ((padded >> (50 - i * 5)) & 0x1F) as usize;
        buf[i] = BASE32_ALPHABET[idx];
    }
    // SAFETY: BASE32_ALPHABET 全是 ASCII
    unsafe { String::from_utf8_unchecked(buf.to_vec()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let lng = 116.4074;
        let lat = 39.9042;
        let hash = encode(lng, lat, 52);
        let (dlng, dlat) = decode(hash, 52);
        assert!((dlng - lng).abs() < 0.001, "lng: {} vs {}", dlng, lng);
        assert!((dlat - lat).abs() < 0.001, "lat: {} vs {}", dlat, lat);
    }

    #[test]
    fn nearby_points_share_prefix() {
        let h1 = encode(116.3912, 39.9087, 32);
        let h2 = encode(116.3972, 39.9169, 32);
        // 20-bit prefix（~1.2km 精度）应该相同
        let mask = !0u64 << 12;
        assert_eq!(h1 & mask, h2 & mask);
    }

    #[test]
    fn neighbors_include_self() {
        let hash = encode(116.4074, 39.9042, 20);
        let ns = neighbors(hash, 20);
        assert!(ns.contains(&hash));
        assert!(ns.len() >= 5 && ns.len() <= 9);
    }

    #[test]
    fn hash_to_prefix_bytes_length() {
        let hash = encode(0.0, 0.0, 20);
        let prefix = hash_to_prefix_bytes(hash, 20);
        assert_eq!(prefix.len(), 3); // ceil(20/8) = 3
    }

    #[test]
    fn optimal_precision_ranges() {
        assert!(optimal_precision(50_000.0) <= 12);
        assert!(optimal_precision(1000.0) <= 24);
        assert!(optimal_precision(10.0) <= 32);
    }

    #[test]
    fn deinterleave_interleave_roundtrip() {
        let hash = encode(116.4074, 39.9042, 32);
        let (lng, lat) = deinterleave(hash, 32);
        let reconstructed = interleave(lng, lat, 32);
        assert_eq!(hash, reconstructed);
    }
}
