/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M94：地理空间搜索表达式解析 — 从 SELECT 列中提取 ST_DISTANCE。

use super::types::GeoSearchExpr;
use super::utils::strip_func_call;

/// 从 SELECT 列列表中提取 ST_DISTANCE 表达式。
/// 返回 (过滤后的普通列, Option<GeoSearchExpr>)。
pub(crate) fn extract_geo_search(columns: &[String]) -> (Vec<String>, Option<GeoSearchExpr>) {
    let mut normal_cols = Vec::new();
    let mut geo_expr: Option<GeoSearchExpr> = None;

    for col in columns {
        let trimmed = col.trim();
        if let Some(expr) = try_parse_st_distance(trimmed) {
            geo_expr = Some(expr);
        } else {
            normal_cols.push(col.clone());
        }
    }
    (normal_cols, geo_expr)
}

/// 尝试解析 `ST_DISTANCE(col, GEOPOINT(lat, lng)) AS alias`。
fn try_parse_st_distance(expr: &str) -> Option<GeoSearchExpr> {
    let (func_part, alias) = split_as_alias(expr);
    let inner = strip_func_call(func_part, "ST_DISTANCE")?;
    // inner = "col, GEOPOINT(lat, lng)"
    // 找到 GEOPOINT( 的位置
    let gp_pos = inner.to_uppercase().find("GEOPOINT(")?;
    let col = inner[..gp_pos].trim().trim_end_matches(',').trim();
    if col.is_empty() {
        return None;
    }
    let gp_str = &inner[gp_pos..];
    let gp_inner = strip_func_call(gp_str, "GEOPOINT")?;
    let parts: Vec<&str> = gp_inner.split(',').collect();
    if parts.len() != 2 {
        return None;
    }
    let lat: f64 = parts[0].trim().parse().ok()?;
    let lng: f64 = parts[1].trim().parse().ok()?;
    Some(GeoSearchExpr {
        column: col.to_string(),
        target_lat: lat,
        target_lng: lng,
        alias,
    })
}

/// 分离 `expr AS alias` → (expr, Some(alias))。
fn split_as_alias(expr: &str) -> (&str, Option<String>) {
    // 在字节层面查找 " AS "（ASCII 大小写不敏感），避免 to_uppercase() 的 UTF-8 偏移问题
    let bytes = expr.as_bytes();
    let mut last_as_pos = None;
    if bytes.len() >= 4 {
        for i in 0..=bytes.len() - 4 {
            if bytes[i] == b' '
                && (bytes[i + 1] == b'A' || bytes[i + 1] == b'a')
                && (bytes[i + 2] == b'S' || bytes[i + 2] == b's')
                && bytes[i + 3] == b' '
            {
                last_as_pos = Some(i);
            }
        }
    }
    if let Some(pos) = last_as_pos {
        let alias = expr[pos + 4..].trim().to_string();
        if !alias.is_empty() {
            return (&expr[..pos], Some(alias));
        }
    }
    (expr, None)
}
