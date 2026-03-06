/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Top-N 堆排序：ORDER BY + LIMIT 场景下只保留 top N 行，O(N) 内存。
//! 避免全表加载到内存再排序的 O(表大小) 内存问题。

use crate::types::Value;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::helpers::value_cmp;
use super::parser::{WhereExpr, WhereOp};

/// M76：从 AND 表达式中提取最优索引 Eq 条件。
/// 返回 (索引列名, 值, 剩余过滤条件)。优先 PK，其次索引列。
pub(super) fn extract_indexed_eq<'a>(
    expr: &'a WhereExpr,
    pk_col: &str,
    index_cols: &[&str],
) -> Option<(&'a str, &'a Value, Vec<&'a WhereExpr>)> {
    let children = match expr {
        WhereExpr::And(children) if children.len() > 1 => children,
        _ => return None,
    };
    let find = |col_match: &dyn Fn(&str) -> bool| -> Option<(usize, &'a str, &'a Value)> {
        children.iter().enumerate().find_map(|(i, child)| {
            if let WhereExpr::Leaf(c) = child {
                if c.op == WhereOp::Eq && c.jsonb_path.is_none() && col_match(&c.column) {
                    return Some((i, c.column.as_str(), &c.value));
                }
            }
            None
        })
    };
    let (idx, col, val) =
        find(&|c: &str| c == pk_col).or_else(|| find(&|c: &str| index_cols.contains(&c)))?;
    let rest = children
        .iter()
        .enumerate()
        .filter(|&(j, _)| j != idx)
        .map(|(_, e)| e)
        .collect();
    Some((col, val, rest))
}

/// 索引范围条件描述。
#[derive(Debug)]
pub(super) enum IndexRangeCond<'a> {
    /// `col > val`
    Gt(&'a str, &'a Value),
    /// `col >= val`
    Ge(&'a str, &'a Value),
    /// `col < val`
    Lt(&'a str, &'a Value),
    /// `col <= val`
    Le(&'a str, &'a Value),
    /// `col BETWEEN low AND high`
    Between(&'a str, &'a Value, &'a Value),
}

impl<'a> IndexRangeCond<'a> {
    /// 返回索引列名。
    pub(super) fn column(&self) -> &'a str {
        match self {
            Self::Gt(c, _)
            | Self::Ge(c, _)
            | Self::Lt(c, _)
            | Self::Le(c, _)
            | Self::Between(c, _, _) => c,
        }
    }
}

/// 从单个 WHERE 条件中提取索引范围条件。
/// 仅匹配有索引的列上的 Gt/Ge/Lt/Le/Between 操作。
pub(super) fn extract_indexed_range<'a>(
    expr: &'a WhereExpr,
    index_cols: &[&str],
) -> Option<(IndexRangeCond<'a>, Vec<&'a WhereExpr>)> {
    match expr {
        WhereExpr::Leaf(c) if c.jsonb_path.is_none() && index_cols.contains(&c.column.as_str()) => {
            let cond = match c.op {
                WhereOp::Gt => Some(IndexRangeCond::Gt(&c.column, &c.value)),
                WhereOp::Ge => Some(IndexRangeCond::Ge(&c.column, &c.value)),
                WhereOp::Lt => Some(IndexRangeCond::Lt(&c.column, &c.value)),
                WhereOp::Le => Some(IndexRangeCond::Le(&c.column, &c.value)),
                WhereOp::Between => c
                    .value_high
                    .as_ref()
                    .map(|high| IndexRangeCond::Between(&c.column, &c.value, high)),
                _ => None,
            };
            cond.map(|c| (c, vec![]))
        }
        WhereExpr::And(children) if !children.is_empty() => {
            // 在 AND 子条件中找第一个可走索引的范围条件
            for (i, child) in children.iter().enumerate() {
                if let WhereExpr::Leaf(c) = child {
                    if c.jsonb_path.is_none() && index_cols.contains(&c.column.as_str()) {
                        let cond = match c.op {
                            WhereOp::Gt => Some(IndexRangeCond::Gt(&c.column, &c.value)),
                            WhereOp::Ge => Some(IndexRangeCond::Ge(&c.column, &c.value)),
                            WhereOp::Lt => Some(IndexRangeCond::Lt(&c.column, &c.value)),
                            WhereOp::Le => Some(IndexRangeCond::Le(&c.column, &c.value)),
                            WhereOp::Between => c
                                .value_high
                                .as_ref()
                                .map(|high| IndexRangeCond::Between(&c.column, &c.value, high)),
                            _ => None,
                        };
                        if let Some(cond) = cond {
                            let rest: Vec<&WhereExpr> = children
                                .iter()
                                .enumerate()
                                .filter(|&(j, _)| j != i)
                                .map(|(_, e)| e)
                                .collect();
                            return Some((cond, rest));
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// 堆中的行条目，携带预计算的排序键。
struct HeapEntry {
    row: Vec<Value>,
    /// 排序键：(值, 是否DESC, NULLS FIRST) 列表，用于 Ord 比较。
    sort_key: Vec<(Value, bool, bool)>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // 按 ORDER BY 的期望顺序比较。
        // BinaryHeap 是 max-heap：peek() 返回最大元素。
        // 对于 ASC，最大 = 排序最后 = 应淘汰的行，正确。
        // 对于 DESC，反转比较使最小的 DESC 值排在堆顶，也是应淘汰的行。
        for ((va, desc_a, nf_a), (vb, _, _)) in self.sort_key.iter().zip(other.sort_key.iter()) {
            // NULLS FIRST/LAST 处理
            match (matches!(va, Value::Null), matches!(vb, Value::Null)) {
                (true, true) => continue,
                (true, false) => {
                    return if *nf_a {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    return if *nf_a {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => {}
            }
            let cmp = value_cmp(va, vb).unwrap_or(Ordering::Equal);
            let cmp = if *desc_a { cmp.reverse() } else { cmp };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

/// Top-N 收集器：流式接收行，只保留排序后前 N 条。
///
/// 内存复杂度：O(capacity)，与表大小无关。
/// 时间复杂度：O(total_rows × log(capacity))。
pub(super) struct TopNHeap {
    heap: BinaryHeap<HeapEntry>,
    /// (列索引, 是否DESC, NULLS FIRST 有效值) — 排序规则
    col_indices: Vec<(usize, bool, bool)>,
    capacity: usize,
}

impl TopNHeap {
    /// 创建 Top-N 收集器。
    /// `col_indices`: ORDER BY 列的 (列索引, 是否DESC, Option<NULLS FIRST>) 列表。
    /// `capacity`: 最多保留多少行 (= offset + limit)。
    pub(super) fn new(col_indices: Vec<(usize, bool, Option<bool>)>, capacity: usize) -> Self {
        // 预计算 nulls_first 有效值：默认 ASC→NULLS LAST(false), DESC→NULLS FIRST(true)
        let resolved: Vec<(usize, bool, bool)> = col_indices
            .into_iter()
            .map(|(idx, desc, nf)| (idx, desc, nf.unwrap_or(desc)))
            .collect();
        Self {
            heap: BinaryHeap::with_capacity(capacity + 1),
            col_indices: resolved,
            capacity,
        }
    }

    /// 推入一行。如果堆已满且新行比堆顶更优，则淘汰堆顶。
    pub(super) fn push(&mut self, row: Vec<Value>) {
        if self.capacity == 0 {
            return;
        }
        let entry = self.make_entry(row);
        if self.heap.len() < self.capacity {
            self.heap.push(entry);
        } else if let Some(worst) = self.heap.peek() {
            if entry.cmp(worst) == Ordering::Less {
                // 新行比堆顶更优（排序更靠前），淘汰堆顶
                self.heap.pop();
                self.heap.push(entry);
            }
        }
    }

    /// 提取结果，按期望排序顺序返回。
    pub(super) fn into_sorted(self) -> Vec<Vec<Value>> {
        let mut entries: Vec<HeapEntry> = self.heap.into_vec();
        // 按期望顺序排序（Ord 实现的反向：小的在前）
        entries.sort();
        entries.into_iter().map(|e| e.row).collect()
    }

    fn make_entry(&self, row: Vec<Value>) -> HeapEntry {
        let sort_key = self
            .col_indices
            .iter()
            .map(|&(ci, desc, nf)| {
                let val = row.get(ci).cloned().unwrap_or(Value::Null);
                (val, desc, nf)
            })
            .collect();
        HeapEntry { row, sort_key }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topn_asc_basic() {
        // 10 行取 top 3 (ASC)
        let mut heap = TopNHeap::new(vec![(0, false, None)], 3);
        for i in (0..10).rev() {
            heap.push(vec![Value::Integer(i)]);
        }
        let result = heap.into_sorted();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::Integer(0));
        assert_eq!(result[1][0], Value::Integer(1));
        assert_eq!(result[2][0], Value::Integer(2));
    }

    #[test]
    fn topn_desc_basic() {
        // 10 行取 top 3 (DESC)
        let mut heap = TopNHeap::new(vec![(0, true, None)], 3);
        for i in 0..10 {
            heap.push(vec![Value::Integer(i)]);
        }
        let result = heap.into_sorted();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::Integer(9));
        assert_eq!(result[1][0], Value::Integer(8));
        assert_eq!(result[2][0], Value::Integer(7));
    }

    #[test]
    fn topn_zero_capacity() {
        let mut heap = TopNHeap::new(vec![(0, false, None)], 0);
        heap.push(vec![Value::Integer(1)]);
        assert!(heap.into_sorted().is_empty());
    }
}
