/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M94: ST_DISTANCE 后处理——计算距离、投影、排序、截断。

use super::engine::SqlEngine;
use super::parser::GeoSearchExpr;
use crate::types::Value;
use crate::Error;

impl SqlEngine {
    /// M94: geo_search 后处理。
    /// 输入 rows 为 SELECT * 全量列。
    /// 1) 计算 ST_DISTANCE 追加到末尾
    /// 2) 投影到 requested_cols + dist 列
    /// 3) 按距离排序 + offset/limit
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_geo_search(
        &self,
        mut rows: Vec<Vec<Value>>,
        table: &str,
        gs: &GeoSearchExpr,
        requested_cols: &[String],
        order_by: Option<&[(String, bool, Option<bool>)]>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<Vec<Vec<Value>>, Error> {
        let schema = &self
            .cache
            .get(table)
            .ok_or_else(|| Error::SqlExec("表不存在".into()))?
            .schema;
        let geo_ci = schema
            .column_index_by_name(&gs.column)
            .ok_or_else(|| Error::SqlExec(format!("GeoPoint 列不存在: {}", gs.column)))?;
        // 1) 追加距离列
        for row in &mut rows {
            let dist = match row.get(geo_ci) {
                Some(Value::GeoPoint(lat, lng)) => {
                    super::geo::haversine(*lat, *lng, gs.target_lat, gs.target_lng)
                }
                _ => f64::MAX,
            };
            row.push(Value::Float(dist));
        }
        // 2) 排序
        let alias = gs.alias.as_deref().unwrap_or("dist");
        let dist_idx = schema.visible_column_count(); // 距离列在 schema 列之后
        let sort_by_dist =
            order_by.is_some_and(|ob| ob.iter().any(|(c, _, _)| c.eq_ignore_ascii_case(alias)));
        if sort_by_dist {
            let desc = order_by
                .and_then(|ob| ob.iter().find(|(c, _, _)| c.eq_ignore_ascii_case(alias)))
                .is_some_and(|(_, d, _)| *d);
            rows.sort_by(|a, b| {
                let va = match a.get(dist_idx) {
                    Some(Value::Float(f)) => *f,
                    _ => f64::MAX,
                };
                let vb = match b.get(dist_idx) {
                    Some(Value::Float(f)) => *f,
                    _ => f64::MAX,
                };
                if desc {
                    vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                }
            });
        }
        // 3) offset + limit
        if let Some(off) = offset {
            let off = off as usize;
            if off < rows.len() {
                rows = rows[off..].to_vec();
            } else {
                rows.clear();
            }
        }
        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }
        // 4) 列投影：requested_cols + 自动追加 dist 别名
        let is_star = requested_cols.len() == 1 && requested_cols[0] == "*";
        if !is_star {
            let mut proj_cols: Vec<&str> = requested_cols.iter().map(|s| s.as_str()).collect();
            if !proj_cols.iter().any(|c| c.eq_ignore_ascii_case(alias)) {
                proj_cols.push(alias);
            }
            let col_indices: Vec<Option<usize>> = proj_cols
                .iter()
                .map(|c| {
                    if c.eq_ignore_ascii_case(alias) {
                        Some(dist_idx)
                    } else {
                        schema.column_index_by_name(c)
                    }
                })
                .collect();
            rows = rows
                .into_iter()
                .map(|row| {
                    col_indices
                        .iter()
                        .map(|ci| ci.and_then(|i| row.get(i).cloned()).unwrap_or(Value::Null))
                        .collect()
                })
                .collect();
        }
        Ok(rows)
    }
}
