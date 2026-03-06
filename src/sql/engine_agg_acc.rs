/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 流式聚合累加器（从 engine_agg.rs 拆分）。

use super::helpers::{value_cmp, AggType};
use crate::types::{Schema, Value};

/// 流式聚合累加器：多数聚合 O(1) 内存，PERCENTILE 等需收集全部值。
pub(super) struct AggAccumulator {
    accumulators: Vec<AggState>,
}

enum AggState {
    Count(i64),
    Sum {
        sum: f64,
        is_int: bool,
    },
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    GroupConcat {
        parts: Vec<String>,
        sep: String,
    },
    /// Welford 在线算法：总体标准差 / 方差。
    Stddev {
        count: u64,
        mean: f64,
        m2: f64,
        is_variance: bool,
    },
    /// JSON_ARRAYAGG：收集值为 JSON 数组。
    JsonArrayAgg {
        values: Vec<serde_json::Value>,
    },
    /// JSON_OBJECTAGG：收集键值对为 JSON 对象（val_col_idx 在 feed 时通过外部传入）。
    JsonObjectAgg {
        map: serde_json::Map<String, serde_json::Value>,
        val_col_idx: Option<usize>,
    },
    /// BOOL_AND：逻辑与聚合。
    BoolAnd(Option<bool>),
    /// BOOL_OR：逻辑或聚合。
    BoolOr(Option<bool>),
    /// ARRAY_AGG：收集非 NULL 值为 JSON 数组。
    ArrayAgg {
        values: Vec<serde_json::Value>,
    },
    /// PERCENTILE_CONT：连续百分位（线性插值），需收集全部值。
    PercentileCont {
        frac: f64,
        vals: Vec<f64>,
    },
    /// PERCENTILE_DISC：离散百分位（取最近值），需收集全部值。
    PercentileDisc {
        frac: f64,
        vals: Vec<f64>,
    },
}

impl AggAccumulator {
    pub(super) fn new(aggs: &[(AggType, String)], schema: Option<&Schema>) -> Self {
        let accumulators = aggs
            .iter()
            .map(|(agg_type, _)| match agg_type {
                AggType::Count => AggState::Count(0),
                AggType::Sum => AggState::Sum {
                    sum: 0.0,
                    is_int: true,
                },
                AggType::Avg => AggState::Avg { sum: 0.0, count: 0 },
                AggType::Min => AggState::Min(None),
                AggType::Max => AggState::Max(None),
                AggType::GroupConcat(ref sep) => AggState::GroupConcat {
                    parts: Vec::new(),
                    sep: sep.clone(),
                },
                AggType::Stddev => AggState::Stddev {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    is_variance: false,
                },
                AggType::Variance => AggState::Stddev {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    is_variance: true,
                },
                AggType::JsonArrayAgg => AggState::JsonArrayAgg { values: Vec::new() },
                AggType::JsonObjectAgg(ref val_col) => {
                    let val_col_idx = schema.and_then(|s| s.column_index_by_name(val_col));
                    AggState::JsonObjectAgg {
                        map: serde_json::Map::new(),
                        val_col_idx,
                    }
                }
                AggType::BoolAnd => AggState::BoolAnd(None),
                AggType::BoolOr => AggState::BoolOr(None),
                AggType::ArrayAgg => AggState::ArrayAgg { values: Vec::new() },
                AggType::PercentileCont(f) => AggState::PercentileCont {
                    frac: *f,
                    vals: Vec::new(),
                },
                AggType::PercentileDisc(f) => AggState::PercentileDisc {
                    frac: *f,
                    vals: Vec::new(),
                },
            })
            .collect();
        Self { accumulators }
    }

    /// 输入一行，更新所有累加器。col_indices[i] 是第 i 个聚合对应的列索引（COUNT(*) 为 None）。
    pub(super) fn feed(&mut self, row: &[Value], col_indices: &[Option<usize>]) {
        for (i, state) in self.accumulators.iter_mut().enumerate() {
            match state {
                AggState::Count(ref mut c) => {
                    if let Some(ci) = col_indices[i] {
                        if !matches!(row[ci], Value::Null) {
                            *c += 1;
                        }
                    } else {
                        *c += 1;
                    }
                }
                AggState::Sum {
                    ref mut sum,
                    ref mut is_int,
                } => {
                    if let Some(ci) = col_indices[i] {
                        match &row[ci] {
                            Value::Integer(n) => *sum += *n as f64,
                            Value::Float(n) => {
                                *sum += n;
                                *is_int = false;
                            }
                            _ => {}
                        }
                    }
                }
                AggState::Avg {
                    ref mut sum,
                    ref mut count,
                } => {
                    if let Some(ci) = col_indices[i] {
                        match &row[ci] {
                            Value::Integer(n) => {
                                *sum += *n as f64;
                                *count += 1;
                            }
                            Value::Float(n) => {
                                *sum += n;
                                *count += 1;
                            }
                            _ => {}
                        }
                    }
                }
                AggState::Min(ref mut cur) => {
                    if let Some(ci) = col_indices[i] {
                        let v = &row[ci];
                        if !matches!(v, Value::Null) {
                            *cur = Some(match cur {
                                None => v.clone(),
                                Some(ref c) => {
                                    if value_cmp(v, c).map(|o| o.is_lt()).unwrap_or(false) {
                                        v.clone()
                                    } else {
                                        c.clone()
                                    }
                                }
                            });
                        }
                    }
                }
                AggState::Max(ref mut cur) => {
                    if let Some(ci) = col_indices[i] {
                        let v = &row[ci];
                        if !matches!(v, Value::Null) {
                            *cur = Some(match cur {
                                None => v.clone(),
                                Some(ref c) => {
                                    if value_cmp(v, c).map(|o| o.is_gt()).unwrap_or(false) {
                                        v.clone()
                                    } else {
                                        c.clone()
                                    }
                                }
                            });
                        }
                    }
                }
                AggState::GroupConcat { ref mut parts, .. } => {
                    if let Some(ci) = col_indices[i] {
                        let v = &row[ci];
                        match v {
                            Value::Text(s) => parts.push(s.clone()),
                            Value::Integer(n) => parts.push(n.to_string()),
                            Value::Float(f) => parts.push(f.to_string()),
                            Value::Boolean(b) => parts.push(b.to_string()),
                            _ => {}
                        }
                    }
                }
                AggState::Stddev {
                    ref mut count,
                    ref mut mean,
                    ref mut m2,
                    ..
                } => {
                    if let Some(ci) = col_indices[i] {
                        let val = match &row[ci] {
                            Value::Integer(n) => Some(*n as f64),
                            Value::Float(n) => Some(*n),
                            _ => None,
                        };
                        if let Some(v) = val {
                            *count += 1;
                            let delta = v - *mean;
                            *mean += delta / *count as f64;
                            let delta2 = v - *mean;
                            *m2 += delta * delta2;
                        }
                    }
                }
                AggState::JsonArrayAgg { ref mut values } => {
                    if let Some(ci) = col_indices[i] {
                        values.push(val_to_json(&row[ci]));
                    }
                }
                AggState::JsonObjectAgg {
                    ref mut map,
                    val_col_idx,
                } => {
                    if let Some(ci) = col_indices[i] {
                        let key = match &row[ci] {
                            Value::Text(s) => s.clone(),
                            Value::Integer(n) => n.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Boolean(b) => b.to_string(),
                            Value::Null => continue,
                            other => format!("{:?}", other),
                        };
                        let json_val = if let Some(vi) = val_col_idx {
                            if *vi < row.len() {
                                val_to_json(&row[*vi])
                            } else {
                                serde_json::Value::Null
                            }
                        } else {
                            val_to_json(&row[ci])
                        };
                        map.insert(key, json_val);
                    }
                }
                AggState::BoolAnd(ref mut acc) => {
                    if let Some(ci) = col_indices[i] {
                        let b = match &row[ci] {
                            Value::Boolean(v) => Some(*v),
                            Value::Integer(n) => Some(*n != 0),
                            Value::Null => None,
                            _ => None,
                        };
                        if let Some(v) = b {
                            *acc = Some(acc.unwrap_or(true) && v);
                        }
                    }
                }
                AggState::BoolOr(ref mut acc) => {
                    if let Some(ci) = col_indices[i] {
                        let b = match &row[ci] {
                            Value::Boolean(v) => Some(*v),
                            Value::Integer(n) => Some(*n != 0),
                            Value::Null => None,
                            _ => None,
                        };
                        if let Some(v) = b {
                            *acc = Some(acc.unwrap_or(false) || v);
                        }
                    }
                }
                AggState::ArrayAgg { ref mut values } => {
                    if let Some(ci) = col_indices[i] {
                        if !matches!(row[ci], Value::Null) {
                            values.push(val_to_json(&row[ci]));
                        }
                    }
                }
                AggState::PercentileCont { ref mut vals, .. }
                | AggState::PercentileDisc { ref mut vals, .. } => {
                    if let Some(ci) = col_indices[i] {
                        match &row[ci] {
                            Value::Integer(n) => vals.push(*n as f64),
                            Value::Float(f) => vals.push(*f),
                            _ => {} // NULL 和非数值跳过
                        }
                    }
                }
            }
        }
    }

    /// M93 方案A：是否全部为数值聚合（SUM/AVG/MIN/MAX/COUNT），可走零分配路径。
    pub(super) fn is_all_numeric(&self) -> bool {
        self.accumulators.iter().all(|s| {
            matches!(
                s,
                AggState::Sum { .. }
                    | AggState::Avg { .. }
                    | AggState::Min(_)
                    | AggState::Max(_)
                    | AggState::Count(_)
                    | AggState::Stddev { .. }
            )
        })
    }

    /// M93 方案A：零分配喂入 f64 值。
    pub(super) fn feed_f64(&mut self, val: Option<f64>, acc_idx: usize, col_is_int: bool) {
        if acc_idx >= self.accumulators.len() {
            return;
        }
        match &mut self.accumulators[acc_idx] {
            AggState::Count(c) => {
                if val.is_some() {
                    *c += 1;
                }
            }
            AggState::Sum { sum, .. } => {
                if let Some(v) = val {
                    *sum += v;
                }
            }
            AggState::Avg { sum, count } => {
                if let Some(v) = val {
                    *sum += v;
                    *count += 1;
                }
            }
            AggState::Min(cur) => {
                if let Some(v) = val {
                    let nv = if col_is_int {
                        Value::Integer(v as i64)
                    } else {
                        Value::Float(v)
                    };
                    *cur = Some(match cur {
                        None => nv,
                        Some(ref c) if value_cmp(&nv, c).map(|o| o.is_lt()).unwrap_or(false) => nv,
                        Some(c) => c.clone(),
                    });
                }
            }
            AggState::Max(cur) => {
                if let Some(v) = val {
                    let nv = if col_is_int {
                        Value::Integer(v as i64)
                    } else {
                        Value::Float(v)
                    };
                    *cur = Some(match cur {
                        None => nv,
                        Some(ref c) if value_cmp(&nv, c).map(|o| o.is_gt()).unwrap_or(false) => nv,
                        Some(c) => c.clone(),
                    });
                }
            }
            // GroupConcat 不走零分配路径（is_all_numeric 返回 false）
            AggState::GroupConcat { .. } => {}
            // JsonArrayAgg/JsonObjectAgg 不走零分配路径
            AggState::JsonArrayAgg { .. } | AggState::JsonObjectAgg { .. } => {}
            // BoolAnd/BoolOr 不走零分配路径
            AggState::BoolAnd(_) | AggState::BoolOr(_) => {}
            // ArrayAgg 不走零分配路径
            AggState::ArrayAgg { .. } => {}
            // Percentile 不走零分配路径（is_all_numeric 返回 false）
            AggState::PercentileCont { .. } | AggState::PercentileDisc { .. } => {}
            AggState::Stddev {
                count, mean, m2, ..
            } => {
                if let Some(v) = val {
                    *count += 1;
                    let delta = v - *mean;
                    *mean += delta / *count as f64;
                    let delta2 = v - *mean;
                    *m2 += delta * delta2;
                }
            }
        }
    }

    pub(super) fn finish(self) -> Vec<Value> {
        self.accumulators
            .into_iter()
            .map(|s| match s {
                AggState::Count(c) => Value::Integer(c),
                AggState::Sum { sum, is_int } => {
                    if is_int {
                        Value::Integer(sum as i64)
                    } else {
                        Value::Float(sum)
                    }
                }
                AggState::Avg { sum, count } => {
                    if count > 0 {
                        Value::Float(sum / count as f64)
                    } else {
                        Value::Null
                    }
                }
                AggState::Min(v) => v.unwrap_or(Value::Null),
                AggState::Max(v) => v.unwrap_or(Value::Null),
                AggState::GroupConcat { parts, sep } => {
                    if parts.is_empty() {
                        Value::Null
                    } else {
                        Value::Text(parts.join(&sep))
                    }
                }
                AggState::Stddev {
                    count,
                    mean: _,
                    m2,
                    is_variance,
                } => {
                    if count == 0 {
                        Value::Null
                    } else {
                        let variance = m2 / count as f64;
                        Value::Float(if is_variance {
                            variance
                        } else {
                            variance.sqrt()
                        })
                    }
                }
                AggState::JsonArrayAgg { values } => {
                    Value::Text(serde_json::to_string(&values).unwrap_or_else(|_| "[]".into()))
                }
                AggState::JsonObjectAgg { map, .. } => Value::Text(
                    serde_json::to_string(&serde_json::Value::Object(map))
                        .unwrap_or_else(|_| "{}".into()),
                ),
                AggState::BoolAnd(v) => match v {
                    Some(b) => Value::Boolean(b),
                    None => Value::Null,
                },
                AggState::BoolOr(v) => match v {
                    Some(b) => Value::Boolean(b),
                    None => Value::Null,
                },
                AggState::ArrayAgg { values } => {
                    if values.is_empty() {
                        Value::Null
                    } else {
                        Value::Text(serde_json::to_string(&values).unwrap_or_else(|_| "[]".into()))
                    }
                }
                AggState::PercentileCont { frac, mut vals } => {
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let n = vals.len();
                        let idx = frac * (n - 1) as f64;
                        let lo = idx.floor() as usize;
                        let hi = idx.ceil() as usize;
                        let v = if lo == hi {
                            vals[lo]
                        } else {
                            vals[lo] + (vals[hi] - vals[lo]) * (idx - lo as f64)
                        };
                        Value::Float(v)
                    }
                }
                AggState::PercentileDisc { frac, mut vals } => {
                    if vals.is_empty() {
                        Value::Null
                    } else {
                        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let n = vals.len();
                        let idx = (frac * n as f64).ceil() as usize;
                        let idx = idx.clamp(1, n) - 1;
                        Value::Float(vals[idx])
                    }
                }
            })
            .collect()
    }
}

/// Value → serde_json::Value 转换。
fn val_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(n) => serde_json::json!(*n),
        Value::Float(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::json!(s),
        Value::Boolean(b) => serde_json::json!(*b),
        other => serde_json::json!(format!("{:?}", other)),
    }
}
