/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO 地理引擎：基于 Geohash + LSM 前缀扫描的空间索引。
//!
//! 对标 Redis GEO：GEOADD / GEOPOS / GEODIST / GEOSEARCH / GEODEL。
//! 编码：经纬度 → 52-bit Geohash → 8 字节 BE key，精度约 0.6m。
//! 存储：keyspace `geo_{name}`，支持圆形范围搜索（neighbors prefix scan + 距离过滤）。

mod geohash;
mod search;

use crate::error::Error;
use crate::storage::{Keyspace, Store};

const GEO_META_KEYSPACE: &str = "geo_meta";

fn geo_keyspace_name(name: &str) -> String {
    format!("geo_{}", name)
}

/// 地理坐标点。
#[derive(Debug, Clone, Copy)]
pub struct GeoPoint {
    /// 经度 [-180, 180]。
    pub lng: f64,
    /// 纬度 [-85.05112878, 85.05112878]（Web Mercator 范围）。
    pub lat: f64,
}

/// 搜索结果成员。
#[derive(Debug, Clone)]
pub struct GeoMember {
    /// 成员标识。
    pub key: String,
    /// 坐标。
    pub point: GeoPoint,
    /// 与搜索中心的距离（米）。搜索时填充，非搜索场景为 None。
    pub dist: Option<f64>,
}

/// 距离单位。
#[derive(Debug, Clone, Copy)]
pub enum GeoUnit {
    /// 米。
    Meters,
    /// 千米。
    Kilometers,
    /// 英里。
    Miles,
}

impl GeoUnit {
    /// 单位转换因子（转为米）。
    fn to_meters(self, value: f64) -> f64 {
        match self {
            GeoUnit::Meters => value,
            GeoUnit::Kilometers => value * 1000.0,
            GeoUnit::Miles => value * 1609.344,
        }
    }

    /// 米转换为指定单位。
    fn convert_meters(self, meters: f64) -> f64 {
        match self {
            GeoUnit::Meters => meters,
            GeoUnit::Kilometers => meters / 1000.0,
            GeoUnit::Miles => meters / 1609.344,
        }
    }
}

/// GEO 引擎；绑定 Store 的命名空间。
pub struct GeoEngine {
    store: Store,
    meta_ks: Keyspace,
}

/// 编码成员数据为 value：lng(f64 LE) + lat(f64 LE) + key_len(u16 LE) + key_bytes。
fn encode_member(key: &str, point: &GeoPoint) -> Vec<u8> {
    let key_bytes = key.as_bytes();
    let mut buf = Vec::with_capacity(18 + key_bytes.len());
    buf.extend_from_slice(&point.lng.to_le_bytes());
    buf.extend_from_slice(&point.lat.to_le_bytes());
    buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(key_bytes);
    buf
}

/// 解码成员数据。
fn decode_member(data: &[u8]) -> Option<(String, GeoPoint)> {
    if data.len() < 18 {
        return None;
    }
    let lng = f64::from_le_bytes(data[0..8].try_into().ok()?);
    let lat = f64::from_le_bytes(data[8..16].try_into().ok()?);
    let key_len = u16::from_le_bytes(data[16..18].try_into().ok()?) as usize;
    if data.len() < 18 + key_len {
        return None;
    }
    let key = std::str::from_utf8(&data[18..18 + key_len]).ok()?;
    Some((key.to_string(), GeoPoint { lng, lat }))
}

/// 生成存储 key：geohash(8B, 左移到高位) + member_key_hash(8B)。
fn make_geo_key(point: &GeoPoint, member_key: &str) -> Vec<u8> {
    let gh = geohash::encode(point.lng, point.lat, 52);
    // 左移到 64 位高位，确保 prefix scan 字节对齐
    let gh_shifted = gh << (64 - 52);
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&gh_shifted.to_be_bytes());
    // member key hash 用于唯一性
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    member_key.hash(&mut hasher);
    key.extend_from_slice(&hasher.finish().to_be_bytes());
    key
}

/// 成员名到存储 key 的反向索引 key（用于 GEOPOS/GEODEL 按名查找）。
fn make_member_index_key(member_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + member_key.len());
    k.extend_from_slice(b"idx:");
    k.extend_from_slice(member_key.as_bytes());
    k
}

impl GeoEngine {
    /// 打开 GEO 引擎。
    pub fn open(store: &Store) -> Result<Self, Error> {
        let meta_ks = store.open_keyspace(GEO_META_KEYSPACE)?;
        Ok(GeoEngine {
            store: store.clone(),
            meta_ks,
        })
    }

    /// 创建 GEO 命名空间。
    pub fn create(&self, name: &str) -> Result<(), Error> {
        self.meta_ks.set(name.as_bytes(), b"1")?;
        let _ = self.store.open_keyspace(&geo_keyspace_name(name))?;
        Ok(())
    }

    /// 添加或更新成员坐标。
    pub fn geo_add(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<(), Error> {
        validate_coords(lng, lat)?;
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let point = GeoPoint { lng, lat };
        // 先删旧数据（如果存在）
        self.remove_member(&ks, member_key)?;
        // 写入新数据
        let geo_key = make_geo_key(&point, member_key);
        let value = encode_member(member_key, &point);
        let idx_key = make_member_index_key(member_key);
        let mut batch = self.store.batch();
        batch.insert(&ks, geo_key.clone(), value)?;
        batch.insert(&ks, idx_key, geo_key)?;
        batch.commit()
    }

    /// 批量添加成员。
    pub fn geo_add_batch(&self, name: &str, members: &[(&str, f64, f64)]) -> Result<(), Error> {
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let mut batch = self.store.batch();
        for &(member_key, lng, lat) in members {
            validate_coords(lng, lat)?;
            let point = GeoPoint { lng, lat };
            // 先删旧数据
            self.remove_member_batch(&ks, &mut batch, member_key)?;
            let geo_key = make_geo_key(&point, member_key);
            let value = encode_member(member_key, &point);
            let idx_key = make_member_index_key(member_key);
            batch.insert(&ks, geo_key.clone(), value)?;
            batch.insert(&ks, idx_key, geo_key)?;
        }
        batch.commit()
    }

    /// 条件添加：仅当成员不存在时添加坐标（NX 模式）。
    ///
    /// 成员已存在返回 `false`（不修改），不存在则添加并返回 `true`。
    /// 对标 Redis `GEOADD NX`。
    pub fn geo_add_nx(
        &self,
        name: &str,
        member_key: &str,
        lng: f64,
        lat: f64,
    ) -> Result<bool, Error> {
        validate_coords(lng, lat)?;
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let idx_key = make_member_index_key(member_key);
        if ks.get(&idx_key)?.is_some() {
            return Ok(false); // 已存在，跳过
        }
        let point = GeoPoint { lng, lat };
        let geo_key = make_geo_key(&point, member_key);
        let value = encode_member(member_key, &point);
        let mut batch = self.store.batch();
        batch.insert(&ks, geo_key.clone(), value)?;
        batch.insert(&ks, idx_key, geo_key)?;
        batch.commit()?;
        Ok(true)
    }

    /// 条件更新：仅当成员已存在时更新坐标（XX 模式）。
    ///
    /// 成员不存在返回 `false`，存在则更新并返回 `true`。
    /// 对标 Redis `GEOADD XX`。
    pub fn geo_add_xx(
        &self,
        name: &str,
        member_key: &str,
        lng: f64,
        lat: f64,
    ) -> Result<bool, Error> {
        validate_coords(lng, lat)?;
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let idx_key = make_member_index_key(member_key);
        if ks.get(&idx_key)?.is_none() {
            return Ok(false); // 不存在，跳过
        }
        // 删旧数据 + 写新数据
        self.remove_member(&ks, member_key)?;
        let point = GeoPoint { lng, lat };
        let geo_key = make_geo_key(&point, member_key);
        let value = encode_member(member_key, &point);
        let mut batch = self.store.batch();
        batch.insert(&ks, geo_key.clone(), value)?;
        batch.insert(&ks, idx_key, geo_key)?;
        batch.commit()?;
        Ok(true)
    }

    /// CH 模式添加：返回 true 表示有变更（新增或坐标变化）。
    ///
    /// 对标 Redis `GEOADD CH`：成员不存在则新增（变更），
    /// 已存在且坐标不同则更新（变更），坐标相同则不变（非变更）。
    pub fn geo_add_ch(
        &self,
        name: &str,
        member_key: &str,
        lng: f64,
        lat: f64,
    ) -> Result<bool, Error> {
        validate_coords(lng, lat)?;
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        // 检查是否已存在且坐标相同
        if let Some(old) = self.get_member_point(&ks, member_key)? {
            if (old.lng - lng).abs() < 1e-10 && (old.lat - lat).abs() < 1e-10 {
                return Ok(false); // 坐标未变
            }
        }
        // 有变更：删旧写新
        self.remove_member(&ks, member_key)?;
        let point = GeoPoint { lng, lat };
        let geo_key = make_geo_key(&point, member_key);
        let value = encode_member(member_key, &point);
        let idx_key = make_member_index_key(member_key);
        let mut batch = self.store.batch();
        batch.insert(&ks, geo_key.clone(), value)?;
        batch.insert(&ks, idx_key, geo_key)?;
        batch.commit()?;
        Ok(true)
    }

    /// CH 模式批量添加：返回变更数量（新增 + 坐标变化的成员数）。
    pub fn geo_add_batch_ch(&self, name: &str, members: &[(&str, f64, f64)]) -> Result<u64, Error> {
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let mut changed = 0u64;
        let mut batch = self.store.batch();
        for &(member_key, lng, lat) in members {
            validate_coords(lng, lat)?;
            let point = GeoPoint { lng, lat };
            let is_change = match self.get_member_point(&ks, member_key)? {
                Some(old) => (old.lng - lng).abs() >= 1e-10 || (old.lat - lat).abs() >= 1e-10,
                None => true,
            };
            if !is_change {
                continue;
            }
            self.remove_member_batch(&ks, &mut batch, member_key)?;
            let geo_key = make_geo_key(&point, member_key);
            let value = encode_member(member_key, &point);
            let idx_key = make_member_index_key(member_key);
            batch.insert(&ks, geo_key.clone(), value)?;
            batch.insert(&ks, idx_key, geo_key)?;
            changed += 1;
        }
        batch.commit()?;
        Ok(changed)
    }

    /// 查询成员坐标。
    pub fn geo_pos(&self, name: &str, member_key: &str) -> Result<Option<GeoPoint>, Error> {
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let idx_key = make_member_index_key(member_key);
        let geo_key = match ks.get(&idx_key)? {
            Some(k) => k,
            None => return Ok(None),
        };
        let raw = match ks.get(&geo_key)? {
            Some(r) => r,
            None => return Ok(None),
        };
        Ok(decode_member(&raw).map(|(_, p)| p))
    }

    /// 删除成员。
    pub fn geo_del(&self, name: &str, member_key: &str) -> Result<bool, Error> {
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        self.remove_member(&ks, member_key)
    }

    /// 计算两个成员之间的距离。
    pub fn geo_dist(
        &self,
        name: &str,
        key1: &str,
        key2: &str,
        unit: GeoUnit,
    ) -> Result<Option<f64>, Error> {
        let p1 = match self.geo_pos(name, key1)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let p2 = match self.geo_pos(name, key2)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let dist_m = haversine_distance(p1.lat, p1.lng, p2.lat, p2.lng);
        Ok(Some(unit.convert_meters(dist_m)))
    }

    /// 列出所有成员。
    pub fn geo_members(&self, name: &str) -> Result<Vec<String>, Error> {
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let mut members = Vec::new();
        ks.for_each_kv_prefix(b"", |key, raw| {
            if key.starts_with(b"idx:") {
                return true;
            }
            if let Some((member_key, _)) = decode_member(raw) {
                members.push(member_key);
            }
            true
        })?;
        Ok(members)
    }

    /// 返回成员的 geohash 字符串（11 字符 base32，52-bit 精度）。
    ///
    /// 成员不存在返回 `None`。对标 Redis `GEOHASH`。
    pub fn geo_hash(&self, name: &str, member_key: &str) -> Result<Option<String>, Error> {
        let point = match self.geo_pos(name, member_key)? {
            Some(p) => p,
            None => return Ok(None),
        };
        Ok(Some(geohash::encode_to_base32(point.lng, point.lat)))
    }

    /// 获取 GEO 集合的成员数量（对标 Redis ZCARD）。
    ///
    /// 通过 `idx:` 反向索引前缀扫描计数，仅遍历 key 不解码 value。
    pub fn geo_count(&self, name: &str) -> Result<u64, Error> {
        let ks = self.store.open_keyspace(&geo_keyspace_name(name))?;
        let mut count: u64 = 0;
        ks.for_each_key_prefix(b"idx:", |_key| {
            count += 1;
            true
        })?;
        Ok(count)
    }

    /// 删除成员（内部）。
    fn remove_member(&self, ks: &Keyspace, member_key: &str) -> Result<bool, Error> {
        let idx_key = make_member_index_key(member_key);
        let geo_key = match ks.get(&idx_key)? {
            Some(k) => k,
            None => return Ok(false),
        };
        let mut batch = self.store.batch();
        batch.remove(ks, geo_key);
        batch.remove(ks, idx_key);
        batch.commit()?;
        Ok(true)
    }

    /// 批量删除成员（内部，不单独 commit）。
    fn remove_member_batch(
        &self,
        ks: &Keyspace,
        batch: &mut crate::storage::Batch,
        member_key: &str,
    ) -> Result<(), Error> {
        let idx_key = make_member_index_key(member_key);
        if let Some(geo_key) = ks.get(&idx_key)? {
            batch.remove(ks, geo_key);
            batch.remove(ks, idx_key);
        }
        Ok(())
    }

    /// 查询成员坐标（内部辅助，CH 模式用）。
    fn get_member_point(&self, ks: &Keyspace, member_key: &str) -> Result<Option<GeoPoint>, Error> {
        let idx_key = make_member_index_key(member_key);
        let geo_key = match ks.get(&idx_key)? {
            Some(k) => k,
            None => return Ok(None),
        };
        let raw = match ks.get(&geo_key)? {
            Some(r) => r,
            None => return Ok(None),
        };
        Ok(decode_member(&raw).map(|(_, p)| p))
    }
}

/// 校验经纬度范围。
fn validate_coords(lng: f64, lat: f64) -> Result<(), Error> {
    if !(-180.0..=180.0).contains(&lng) || !(-85.05112878..=85.05112878).contains(&lat) {
        return Err(Error::Geo(format!(
            "坐标超出范围: lng={}, lat={} (lng: [-180,180], lat: [-85.05,85.05])",
            lng, lat
        )));
    }
    Ok(())
}

/// Haversine 公式计算两点距离（米）。
fn haversine_distance(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const R: f64 = 6371008.8; // 地球平均半径（米）
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    R * c
}

#[cfg(test)]
mod tests;
