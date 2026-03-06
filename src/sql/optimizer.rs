/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL WHERE 表达式优化器。
//!
//! 实现了以下优化 Pass，算法借鉴自 Apache DataFusion 优化器框架：
//!
//! - [`split_conjunction`]：将 AND 表达式拆分为独立谓词列表。
//!   来源：<https://github.com/apache/datafusion/blob/main/datafusion/optimizer/src/utils.rs>
//!
//! - [`flatten_conjunctions`]：打平嵌套的 AND-of-AND / OR-of-OR，
//!   减少运行时求值栈深度。
//!   来源：<https://github.com/apache/datafusion/blob/main/datafusion/expr/src/utils.rs>
//!
//! - [`reorder_predicates`]：在 AND 子句内按谓词代价从低到高排序，
//!   让短路求值更早终止（廉价谓词过滤掉更多行后才执行昂贵谓词）。
//!   灵感来源：DataFusion 物理计划优化器的谓词重排序策略：
//!   <https://github.com/apache/datafusion/blob/main/datafusion/physical-optimizer/src/filter_pushdown.rs>
//!
//! 这些算法均为独立移植，**不引入 datafusion 依赖**，
//! 并已针对 Talon 的 `WhereExpr` / `WhereCondition` 类型重写。

use super::parser::{WhereExpr, WhereOp};

// ---------------------------------------------------------------------------
// 公开接口
// ---------------------------------------------------------------------------

/// 对 WHERE 表达式应用所有优化 Pass，返回优化后的表达式。
///
/// 执行顺序：
/// 1. [`flatten_conjunctions`]  — 打平嵌套 AND/OR
/// 2. [`reorder_predicates`]    — 按代价重排 AND 谓词
///
/// 参照 Apache DataFusion 多 Pass 优化器框架：
/// <https://github.com/apache/datafusion/tree/main/datafusion/optimizer/src>
pub(super) fn optimize_where(expr: WhereExpr) -> WhereExpr {
    let expr = flatten_conjunctions(expr);
    reorder_predicates(expr)
}

/// 将 AND 表达式递归拆分为独立谓词引用列表。
///
/// 示例：`AND(AND(a, b), c)` → `[a, b, c]`
///
/// 改编自 Apache DataFusion 的 `split_conjunction`：
/// <https://github.com/apache/datafusion/blob/main/datafusion/optimizer/src/utils.rs>
pub(super) fn split_conjunction(expr: &WhereExpr) -> Vec<&WhereExpr> {
    let mut result = Vec::new();
    collect_conjunction(expr, &mut result);
    result
}

// ---------------------------------------------------------------------------
// 私有实现
// ---------------------------------------------------------------------------

/// 谓词代价估算（0 = 最廉价，越大越昂贵）。
///
/// 用于 [`reorder_predicates`] 中的稳定排序键。
/// 灵感来自 Apache DataFusion 对表达式求值代价的分类：
/// <https://github.com/apache/datafusion/blob/main/datafusion/physical-expr/src/expressions/binary.rs>
fn predicate_cost(expr: &WhereExpr) -> u8 {
    match expr {
        WhereExpr::Leaf(c) => {
            match c.op {
                // 最快：NULL 检查（无需读取值，仅判断 NULL 标记）
                WhereOp::IsNull | WhereOp::IsNotNull => 0,
                // 快：等值/不等（一次哈希或直接比较）
                WhereOp::Eq | WhereOp::Ne => 1,
                // 中：范围比较
                WhereOp::Lt | WhereOp::Le | WhereOp::Gt | WhereOp::Ge => 2,
                // 中：范围区间
                WhereOp::Between | WhereOp::NotBetween => 3,
                // IN 列表 vs IN 子查询（子查询代价高得多）
                WhereOp::In | WhereOp::NotIn => {
                    if c.subquery.is_none() {
                        3 // 字面值列表，O(n) 扫描
                    } else {
                        9 // 子查询，需要完整执行嵌套查询
                    }
                }
                // 较慢：GLOB 模式（比 LIKE 快一些，无回溯）
                WhereOp::Glob | WhereOp::NotGlob => 4,
                // 慢：LIKE 模式（支持 `%` 回溯，最坏 O(n²)）
                WhereOp::Like | WhereOp::NotLike => 5,
                // 地理空间运算（Haversine 公式，较慢）
                WhereOp::StWithin => 6,
                // 很慢：REGEXP（NFA/DFA 构造 + 匹配）
                WhereOp::Regexp | WhereOp::NotRegexp => 7,
                // 最慢：EXISTS/NOT EXISTS 子查询
                WhereOp::Exists | WhereOp::NotExists => 10,
            }
        }
        // 复合表达式：取子代价最大值（体现最坏情况）
        WhereExpr::And(children) | WhereExpr::Or(children) => {
            children.iter().map(predicate_cost).max().unwrap_or(0)
        }
    }
}

/// 内部递归：收集 AND 子句中的所有叶子谓词。
fn collect_conjunction<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) {
    match expr {
        WhereExpr::And(children) => {
            for child in children {
                collect_conjunction(child, out);
            }
        }
        other => out.push(other),
    }
}

/// 打平嵌套的 AND-of-AND 或 OR-of-OR 表达式。
///
/// 示例：`AND(AND(a, b), AND(c, d))` → `AND(a, b, c, d)`
///
/// 改编自 Apache DataFusion 表达式归一化逻辑：
/// <https://github.com/apache/datafusion/blob/main/datafusion/expr/src/utils.rs>
fn flatten_conjunctions(expr: WhereExpr) -> WhereExpr {
    match expr {
        WhereExpr::And(children) => {
            let mut flat = Vec::with_capacity(children.len());
            for child in children {
                match flatten_conjunctions(child) {
                    // 内层 AND 直接合并到外层
                    WhereExpr::And(nested) => flat.extend(nested),
                    other => flat.push(other),
                }
            }
            match flat.len() {
                1 => flat.remove(0),
                _ => WhereExpr::And(flat),
            }
        }
        WhereExpr::Or(children) => {
            let mut flat = Vec::with_capacity(children.len());
            for child in children {
                match flatten_conjunctions(child) {
                    // 内层 OR 直接合并到外层
                    WhereExpr::Or(nested) => flat.extend(nested),
                    other => flat.push(other),
                }
            }
            match flat.len() {
                1 => flat.remove(0),
                _ => WhereExpr::Or(flat),
            }
        }
        // 叶子节点不变
        leaf => leaf,
    }
}

/// 在 AND 子句内按谓词代价从低到高稳定排序，使短路求值尽早终止。
///
/// - **AND**：廉价谓词先执行；一旦某谓词为 `false`，后续高代价谓词被跳过。
/// - **OR**：廉价谓词先执行；一旦某谓词为 `true`，后续高代价谓词被跳过。
///
/// 使用 [`Vec::sort_by_key`]（Rust 标准库的稳定排序）确保相同代价的谓词
/// 保持原始顺序，避免影响依赖顺序的边缘情况（如带副作用的子查询）。
/// 注：Rust 的 `sort_by_key` 基于归并排序，保证稳定性，
/// 不同于 `sort_unstable_by_key`（不稳定，仅速度更快）。
///
/// 灵感来自 Apache DataFusion 谓词重排序策略：
/// <https://github.com/apache/datafusion/blob/main/datafusion/physical-optimizer/src/filter_pushdown.rs>
fn reorder_predicates(expr: WhereExpr) -> WhereExpr {
    match expr {
        WhereExpr::And(mut children) => {
            // 先递归处理子表达式
            children = children.into_iter().map(reorder_predicates).collect();
            // 按代价稳定升序排序（廉价谓词先执行）；sort_by_key 是稳定排序
            children.sort_by_key(predicate_cost);
            WhereExpr::And(children)
        }
        WhereExpr::Or(mut children) => {
            // OR 同样受益于廉价谓词优先（短路 true）
            children = children.into_iter().map(reorder_predicates).collect();
            children.sort_by_key(predicate_cost);
            WhereExpr::Or(children)
        }
        leaf => leaf,
    }
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::{WhereCondition, WhereOp};
    use crate::types::Value;

    fn leaf_eq(col: &str, val: i64) -> WhereExpr {
        WhereExpr::Leaf(WhereCondition {
            column: col.to_string(),
            op: WhereOp::Eq,
            value: Value::Integer(val),
            in_values: vec![],
            value_high: None,
            jsonb_path: None,
            subquery: None,
            escape_char: None,
            value_column: None,
        })
    }

    fn leaf_like(col: &str, pattern: &str) -> WhereExpr {
        WhereExpr::Leaf(WhereCondition {
            column: col.to_string(),
            op: WhereOp::Like,
            value: Value::Text(pattern.to_string()),
            in_values: vec![],
            value_high: None,
            jsonb_path: None,
            subquery: None,
            escape_char: None,
            value_column: None,
        })
    }

    fn leaf_regexp(col: &str, pattern: &str) -> WhereExpr {
        WhereExpr::Leaf(WhereCondition {
            column: col.to_string(),
            op: WhereOp::Regexp,
            value: Value::Text(pattern.to_string()),
            in_values: vec![],
            value_high: None,
            jsonb_path: None,
            subquery: None,
            escape_char: None,
            value_column: None,
        })
    }

    /// `split_conjunction` 拆分 AND 为独立谓词列表
    #[test]
    fn test_split_conjunction_flat() {
        let a = leaf_eq("a", 1);
        let b = leaf_eq("b", 2);
        let and_expr = WhereExpr::And(vec![a, b]);
        let parts = split_conjunction(&and_expr);
        assert_eq!(parts.len(), 2);
    }

    /// `split_conjunction` 递归展开嵌套 AND
    #[test]
    fn test_split_conjunction_nested() {
        let a = leaf_eq("a", 1);
        let b = leaf_eq("b", 2);
        let c = leaf_eq("c", 3);
        let inner = WhereExpr::And(vec![a, b]);
        let outer = WhereExpr::And(vec![inner, c]);
        let parts = split_conjunction(&outer);
        assert_eq!(parts.len(), 3);
    }

    /// OR 表达式不被 `split_conjunction` 展开
    #[test]
    fn test_split_conjunction_or_untouched() {
        let a = leaf_eq("a", 1);
        let b = leaf_eq("b", 2);
        let or_expr = WhereExpr::Or(vec![a, b]);
        let parts = split_conjunction(&or_expr);
        // OR 作为整体算一个谓词
        assert_eq!(parts.len(), 1);
    }

    /// `flatten_conjunctions` 打平嵌套 AND-of-AND
    #[test]
    fn test_flatten_and() {
        let a = leaf_eq("a", 1);
        let b = leaf_eq("b", 2);
        let c = leaf_eq("c", 3);
        let inner = WhereExpr::And(vec![a, b]);
        let outer = WhereExpr::And(vec![inner, c]);
        let flat = flatten_conjunctions(outer);
        match flat {
            WhereExpr::And(children) => assert_eq!(children.len(), 3),
            _ => panic!("expected AND"),
        }
    }

    /// 单元素 AND 被提升为叶子节点
    #[test]
    fn test_flatten_single_and() {
        let a = leaf_eq("a", 1);
        let and1 = WhereExpr::And(vec![a]);
        let flat = flatten_conjunctions(and1);
        // 单元素 AND 应提升为叶子
        assert!(matches!(flat, WhereExpr::Leaf(_)));
    }

    /// `reorder_predicates` 将廉价谓词排到前面
    #[test]
    fn test_reorder_cheap_first() {
        // 故意把慢谓词放在前面
        let regexp = leaf_regexp("email", "^[a-z]+@");
        let eq_cond = leaf_eq("id", 1);
        let and_expr = WhereExpr::And(vec![regexp, eq_cond]);
        let optimized = reorder_predicates(and_expr);
        match optimized {
            WhereExpr::And(children) => {
                // eq 代价(1) < regexp 代价(7)，eq 应排在前面
                assert_eq!(predicate_cost(&children[0]), 1); // Eq
                assert_eq!(predicate_cost(&children[1]), 7); // Regexp
            }
            _ => panic!("expected AND"),
        }
    }

    /// `reorder_predicates` 对 LIKE 和 REGEXP 正确排序
    #[test]
    fn test_reorder_like_before_regexp() {
        let regexp = leaf_regexp("email", "^[a-z]+@");
        let like = leaf_like("name", "Alice%");
        let and_expr = WhereExpr::And(vec![regexp, like]);
        let optimized = reorder_predicates(and_expr);
        match optimized {
            WhereExpr::And(children) => {
                // like 代价(5) < regexp 代价(7)
                assert_eq!(predicate_cost(&children[0]), 5); // Like
                assert_eq!(predicate_cost(&children[1]), 7); // Regexp
            }
            _ => panic!("expected AND"),
        }
    }

    /// `optimize_where` 端到端：打平 + 重排
    #[test]
    fn test_optimize_where_end_to_end() {
        let regexp = leaf_regexp("email", "^[a-z]+@");
        let eq_cond = leaf_eq("id", 1);
        let like = leaf_like("name", "A%");
        // 构造 AND(AND(regexp, eq), like)
        let inner = WhereExpr::And(vec![regexp, eq_cond]);
        let outer = WhereExpr::And(vec![inner, like]);
        let optimized = optimize_where(outer);
        // 打平后得到 3 个谓词，重排后 eq < like < regexp
        match optimized {
            WhereExpr::And(children) => {
                assert_eq!(children.len(), 3);
                let costs: Vec<u8> = children.iter().map(predicate_cost).collect();
                assert!(
                    costs.windows(2).all(|w| w[0] <= w[1]),
                    "谓词应按代价升序排列: {:?}",
                    costs
                );
            }
            _ => panic!("expected AND after optimization"),
        }
    }
}
