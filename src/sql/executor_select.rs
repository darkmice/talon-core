/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 无缓存路径 SELECT 执行器（从 executor.rs 拆分，保持 ≤500 行）。
//! M75：Top-N 堆排序 + WHERE+LIMIT 提前终止优化。

use super::executor::{get_schema, has_index, index_lookup, row_key, scan_rows, table_keyspace};
use super::helpers::{
    compute_aggregates, dedup_rows, parse_agg_columns, project_columns, row_matches,
    single_eq_condition, value_cmp,
};
use super::parser::WhereExpr;
use super::topn::extract_indexed_eq;
use super::topn::TopNHeap;
use crate::storage::Store;
use crate::types::Value;
use crate::Error;

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_select(
    store: &Store,
    table: &str,
    columns: &[String],
    where_clause: Option<&WhereExpr>,
    order_by: Option<&[(String, bool, Option<bool>)]>,
    limit: Option<u64>,
    offset: Option<u64>,
    distinct: bool,
    vec_search: Option<&super::parser::VecSearchExpr>,
) -> Result<Vec<Vec<Value>>, Error> {
    let Some(schema) = get_schema(store, table)? else {
        return Err(Error::SqlExec(format!("表不存在: {}", table)));
    };
    let ks = store.open_keyspace(&table_keyspace(table))?;

    // M75：解析 ORDER BY 列索引（vec_search 有自己的排序，跳过）
    let col_indices: Option<Vec<(usize, bool, Option<bool>)>> = if vec_search.is_none() {
        if let Some(ob) = order_by {
            Some(
                ob.iter()
                    .map(|(col, desc, nf)| {
                        schema
                            .column_index_by_name(col)
                            .ok_or_else(|| Error::SqlExec(format!("ORDER BY 列不存在: {}", col)))
                            .map(|idx| (idx, *desc, *nf))
                    })
                    .collect::<Result<_, _>>()?,
            )
        } else {
            None
        }
    } else {
        None
    };

    let is_vec = vec_search.is_some();
    let topn_cap = if !is_vec && col_indices.is_some() && !distinct {
        limit.map(|l| l.saturating_add(offset.unwrap_or(0)) as usize)
    } else {
        None
    };

    let mut rows = Vec::new();
    if let Some(expr) = where_clause {
        if let Some((col, val)) = single_eq_condition(expr) {
            let ci = schema
                .column_index_by_name(col)
                .ok_or_else(|| Error::SqlExec(format!("WHERE 列不存在: {}", col)))?;
            if ci == 0 {
                if let Some(raw) = ks.get(&row_key(val)?)? {
                    rows.push(schema.decode_row(&raw)?);
                }
            } else if has_index(store, table, col)? {
                for pk_bytes in index_lookup(store, table, col, val)? {
                    if let Some(raw) = ks.get(&pk_bytes)? {
                        rows.push(schema.decode_row(&raw)?);
                    }
                }
            } else {
                for (_, row) in scan_rows(&ks, &schema, Some(expr))? {
                    rows.push(row);
                }
            }
        } else {
            // M76：AND 多条件索引加速
            let pk_col = &schema.columns[0].0;
            let idx_cols: Vec<String> = schema
                .columns
                .iter()
                .filter(|(name, _)| has_index(store, table, name).unwrap_or(false))
                .map(|(name, _)| name.clone())
                .collect();
            let idx_refs: Vec<&str> = idx_cols.iter().map(|s| s.as_str()).collect();
            if let Some((icol, ival, rest)) = extract_indexed_eq(expr, pk_col, &idx_refs) {
                let ci = schema.column_index_by_name(icol).unwrap();
                let candidate_rows = if ci == 0 {
                    match ks.get(&row_key(ival)?)? {
                        Some(raw) => vec![schema.decode_row(&raw)?],
                        None => vec![],
                    }
                } else {
                    let mut tmp = Vec::new();
                    for pk_bytes in index_lookup(store, table, icol, ival)? {
                        if let Some(raw) = ks.get(&pk_bytes)? {
                            tmp.push(schema.decode_row(&raw)?);
                        }
                    }
                    tmp
                };
                for row in candidate_rows {
                    let pass = rest
                        .iter()
                        .all(|e| row_matches(&row, &schema, e).unwrap_or(false));
                    if pass {
                        rows.push(row);
                    }
                }
            } else {
                for (_, row) in scan_rows(&ks, &schema, Some(expr))? {
                    rows.push(row);
                }
            }
        }
    } else if let Some(cap) = topn_cap {
        // M75：Top-N 堆排序，O(cap) 内存
        let mut heap = TopNHeap::new(col_indices.clone().unwrap(), cap);
        for (_, row) in scan_rows(&ks, &schema, None)? {
            heap.push(row);
        }
        let sorted = heap.into_sorted();
        let off = offset.unwrap_or(0) as usize;
        rows = if off < sorted.len() {
            sorted.into_iter().skip(off).collect()
        } else {
            vec![]
        };
        return post_select(
            rows, columns, &schema, vec_search, store, table, order_by, limit,
        );
    } else {
        for (_, row) in scan_rows(&ks, &schema, None)? {
            rows.push(row);
        }
    }

    if !is_vec {
        if let Some(ref ci) = col_indices {
            rows.sort_by(|a, b| {
                for &(idx, desc, nulls_first) in ci {
                    let av = &a[idx];
                    let bv = &b[idx];
                    let nf = nulls_first.unwrap_or(desc);
                    match (matches!(av, Value::Null), matches!(bv, Value::Null)) {
                        (true, true) => continue,
                        (true, false) => {
                            return if nf {
                                std::cmp::Ordering::Less
                            } else {
                                std::cmp::Ordering::Greater
                            }
                        }
                        (false, true) => {
                            return if nf {
                                std::cmp::Ordering::Greater
                            } else {
                                std::cmp::Ordering::Less
                            }
                        }
                        _ => {}
                    }
                    let cmp = value_cmp(av, bv).unwrap_or(std::cmp::Ordering::Equal);
                    let cmp = if desc { cmp.reverse() } else { cmp };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
    }
    if distinct {
        dedup_rows(&mut rows);
    }
    if !is_vec {
        if let Some(off) = offset {
            let off = off as usize;
            if off >= rows.len() {
                rows.clear();
            } else {
                rows = rows.split_off(off);
            }
        }
        if let Some(n) = limit {
            rows.truncate(n as usize);
        }
    }
    post_select(
        rows, columns, &schema, vec_search, store, table, order_by, limit,
    )
}

#[allow(clippy::too_many_arguments)]
fn post_select(
    rows: Vec<Vec<Value>>,
    columns: &[String],
    schema: &crate::types::Schema,
    vec_search: Option<&super::parser::VecSearchExpr>,
    store: &Store,
    table: &str,
    order_by: Option<&[(String, bool, Option<bool>)]>,
    limit: Option<u64>,
) -> Result<Vec<Vec<Value>>, Error> {
    if let Some(aggs) = parse_agg_columns(columns) {
        let agg_row = compute_aggregates(&rows, &aggs, schema)?;
        return Ok(vec![agg_row]);
    }
    if let Some(vs) = vec_search {
        let mut resolved_vs = vs.clone();
        if resolved_vs.metric == "distance" {
            if let Ok(Some(m)) = super::vec_idx::get_vec_index_metric(store, table, &vs.column) {
                resolved_vs.metric = m;
            }
        }
        return super::vec_search::exec_vec_search(
            rows,
            columns,
            schema,
            &resolved_vs,
            order_by,
            limit,
        );
    }
    project_columns(rows, columns, schema)
}
