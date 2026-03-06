/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M177：窗口函数解析器 — 从 SELECT 列列表中提取窗口函数表达式。
//!
//! 支持语法：
//! - `ROW_NUMBER() OVER ([PARTITION BY ...] ORDER BY ...) [AS alias]`
//! - `RANK() OVER (...)` / `DENSE_RANK() OVER (...)`
//! - `LAG(col [, offset [, default]]) OVER (...)`
//! - `LEAD(col [, offset [, default]]) OVER (...)`
//! - `NTILE(n) OVER (...)`
//! - `SUM(col) OVER (...)` / `COUNT(*) OVER (...)` / `AVG/MIN/MAX(col) OVER (...)`

use super::types::{WindowExpr, WindowFuncKind};
use super::utils::unquote_ident;
use crate::types::Value;

/// 从 SELECT 列列表中提取窗口函数表达式。
///
/// 返回 (过滤后的列列表, 窗口函数列表)。
/// 窗口函数列从 columns 中移除，替换为别名占位符。
pub(crate) fn extract_window_functions(columns: &[String]) -> (Vec<String>, Vec<WindowExpr>) {
    let mut out_cols = Vec::with_capacity(columns.len());
    let mut win_fns = Vec::new();
    for col in columns {
        let upper = col.to_uppercase();
        // 快速检测：必须同时包含 OVER 和 (
        if !upper.contains(" OVER") && !upper.contains(" OVER(") {
            out_cols.push(col.clone());
            continue;
        }
        match parse_window_expr(col) {
            Some(wexpr) => {
                out_cols.push(wexpr.alias.clone());
                win_fns.push(wexpr);
            }
            None => out_cols.push(col.clone()),
        }
    }
    (out_cols, win_fns)
}

/// 解析单个窗口函数表达式。
///
/// 格式：`FUNC(...) OVER ([PARTITION BY ...] [ORDER BY ...]) [AS alias]`
fn parse_window_expr(raw: &str) -> Option<WindowExpr> {
    let trimmed = raw.trim();
    let upper = trimmed.to_uppercase();

    // 找到 OVER 关键字位置（忽略括号内的 OVER）
    let over_pos = find_over_keyword(&upper)?;
    let func_part = trimmed[..over_pos].trim();
    let after_over = trimmed[over_pos + 4..].trim();

    // 解析函数部分
    let func = parse_func_kind(func_part)?;

    // 解析 OVER (...) 部分
    let (partition_by, order_by, rest) = parse_over_clause(after_over)?;

    // 解析 AS alias
    let alias = parse_alias(rest.trim(), func_part);

    Some(WindowExpr {
        func,
        partition_by,
        order_by,
        alias,
    })
}

/// 查找 OVER 关键字位置（在括号外）。
fn find_over_keyword(upper: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let len = bytes.len();
    let mut depth = 0i32;
    let mut i = 0;
    while i < len {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'O' if depth == 0 && i + 4 <= len && &upper[i..i + 4] == "OVER" => {
                // 确保 OVER 后面是空格或 (
                if i + 4 == len || bytes[i + 4] == b' ' || bytes[i + 4] == b'(' {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// 解析窗口函数名和参数。
fn parse_func_kind(func_part: &str) -> Option<WindowFuncKind> {
    let upper = func_part.trim().to_uppercase();
    let upper_no_space = upper.replace(' ', "");

    if upper_no_space == "ROW_NUMBER()" {
        return Some(WindowFuncKind::RowNumber);
    }
    if upper_no_space == "RANK()" {
        return Some(WindowFuncKind::Rank);
    }
    if upper_no_space == "DENSE_RANK()" {
        return Some(WindowFuncKind::DenseRank);
    }

    // NTILE(n)
    if upper_no_space.starts_with("NTILE(") && upper_no_space.ends_with(')') {
        let inner = &upper_no_space[6..upper_no_space.len() - 1];
        let n: usize = inner.trim().parse().ok()?;
        return Some(WindowFuncKind::Ntile(n));
    }

    // LAG(col [, offset [, default]])
    if upper_no_space.starts_with("LAG(") && upper_no_space.ends_with(')') {
        let inner = &func_part.trim()[4..func_part.trim().len() - 1];
        let (col, offset, default) = parse_lag_lead_args(inner);
        return Some(WindowFuncKind::Lag {
            col,
            offset,
            default,
        });
    }

    // LEAD(col [, offset [, default]])
    if upper_no_space.starts_with("LEAD(") && upper_no_space.ends_with(')') {
        let inner = &func_part.trim()[5..func_part.trim().len() - 1];
        let (col, offset, default) = parse_lag_lead_args(inner);
        return Some(WindowFuncKind::Lead {
            col,
            offset,
            default,
        });
    }

    // Aggregate window functions: SUM(col), COUNT(*), AVG(col), MIN(col), MAX(col)
    if upper_no_space.starts_with("SUM(") && upper_no_space.ends_with(')') {
        let inner = extract_inner_col(func_part, 4);
        return Some(WindowFuncKind::Sum(inner));
    }
    if upper_no_space.starts_with("COUNT(") && upper_no_space.ends_with(')') {
        return Some(WindowFuncKind::Count);
    }
    if upper_no_space.starts_with("AVG(") && upper_no_space.ends_with(')') {
        let inner = extract_inner_col(func_part, 4);
        return Some(WindowFuncKind::Avg(inner));
    }
    if upper_no_space.starts_with("MIN(") && upper_no_space.ends_with(')') {
        let inner = extract_inner_col(func_part, 4);
        return Some(WindowFuncKind::Min(inner));
    }
    if upper_no_space.starts_with("MAX(") && upper_no_space.ends_with(')') {
        let inner = extract_inner_col(func_part, 4);
        return Some(WindowFuncKind::Max(inner));
    }

    None
}

/// 从 `FUNC(col)` 中提取内部列名。
fn extract_inner_col(func_part: &str, prefix_len: usize) -> String {
    let trimmed = func_part.trim();
    let inner = &trimmed[prefix_len..trimmed.len() - 1];
    unquote_ident(inner.trim())
}

/// 解析 LAG/LEAD 参数：`col [, offset [, default]]`。
fn parse_lag_lead_args(args: &str) -> (String, usize, Option<Value>) {
    let parts: Vec<&str> = args.splitn(3, ',').collect();
    let col = unquote_ident(parts[0].trim());
    let offset = parts
        .get(1)
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(1);
    let default = parts.get(2).map(|s| parse_literal_value(s.trim()));
    (col, offset, default)
}

/// 解析简单字面量值。
fn parse_literal_value(s: &str) -> Value {
    if s.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    if let Ok(i) = s.parse::<i64>() {
        return Value::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float(f);
    }
    // 字符串字面量（去除引号）
    let trimmed = s.trim();
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        return Value::Text(trimmed[1..trimmed.len() - 1].to_string());
    }
    Value::Text(s.to_string())
}

/// 解析 OVER (...) 子句。
///
/// 返回 (partition_by, order_by, 剩余文本)。
fn parse_over_clause(s: &str) -> Option<(Vec<String>, Vec<(String, bool)>, &str)> {
    let s = s.trim();
    if !s.starts_with('(') {
        return None;
    }
    // 找到匹配的右括号
    let mut depth = 0;
    let mut close = None;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let close_pos = close?;
    let inner = &s[1..close_pos];
    let rest = &s[close_pos + 1..];
    let inner_upper = inner.to_uppercase();

    let mut partition_by = Vec::new();
    let mut order_by = Vec::new();

    // 解析 PARTITION BY
    let order_start = inner_upper.find("ORDER");
    let pb_str = if let Some(_pb_pos) = inner_upper.find("PARTITION") {
        // 重新计算相对位置
        let pb_content_start = inner_upper.find("PARTITION").unwrap() + 9;
        let pb_content = inner[pb_content_start..].trim_start();
        let pb_content = if pb_content.to_uppercase().starts_with("BY") {
            pb_content[2..].trim_start()
        } else {
            pb_content
        };
        let end2 = if let Some(op) = pb_content.to_uppercase().find("ORDER") {
            op
        } else {
            pb_content.len()
        };
        Some(&pb_content[..end2])
    } else {
        None
    };

    if let Some(pb) = pb_str {
        for part in pb.split(',') {
            let col = unquote_ident(part.trim());
            if !col.is_empty() {
                partition_by.push(col);
            }
        }
    }

    // 解析 ORDER BY
    if let Some(ob_pos) = order_start {
        let after_order = inner[ob_pos + 5..].trim_start();
        let after_by = if after_order.to_uppercase().starts_with("BY") {
            after_order[2..].trim_start()
        } else {
            after_order
        };
        for part in after_by.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (col, desc) = if part.to_uppercase().ends_with(" DESC") {
                (unquote_ident(&part[..part.len() - 5]), true)
            } else if part.to_uppercase().ends_with(" ASC") {
                (unquote_ident(&part[..part.len() - 4]), false)
            } else {
                (unquote_ident(part), false)
            };
            if !col.is_empty() {
                order_by.push((col, desc));
            }
        }
    }

    Some((partition_by, order_by, rest))
}

/// 解析别名：`AS alias` 或自动生成。
fn parse_alias(rest: &str, func_part: &str) -> String {
    let upper = rest.trim().to_uppercase();
    if upper.starts_with("AS ") {
        let alias = rest.trim()[3..].trim();
        return unquote_ident(alias.split_whitespace().next().unwrap_or(alias));
    }
    // 自动生成别名：函数名小写
    let func_upper = func_part.trim().to_uppercase().replace(' ', "");
    if func_upper.starts_with("ROW_NUMBER") {
        "row_number".to_string()
    } else if func_upper.starts_with("RANK") {
        "rank".to_string()
    } else if func_upper.starts_with("DENSE_RANK") {
        "dense_rank".to_string()
    } else if func_upper.starts_with("NTILE") {
        "ntile".to_string()
    } else if func_upper.starts_with("LAG") {
        "lag".to_string()
    } else if func_upper.starts_with("LEAD") {
        "lead".to_string()
    } else if func_upper.starts_with("SUM") {
        "sum".to_string()
    } else if func_upper.starts_with("COUNT") {
        "count".to_string()
    } else if func_upper.starts_with("AVG") {
        "avg".to_string()
    } else if func_upper.starts_with("MIN") {
        "min".to_string()
    } else if func_upper.starts_with("MAX") {
        "max".to_string()
    } else {
        "window_fn".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_row_number() {
        let cols = vec![
            "id".to_string(),
            "ROW_NUMBER() OVER (ORDER BY id) AS rn".to_string(),
        ];
        let (out, wins) = extract_window_functions(&cols);
        assert_eq!(out, vec!["id", "rn"]);
        assert_eq!(wins.len(), 1);
        assert!(matches!(wins[0].func, WindowFuncKind::RowNumber));
        assert_eq!(wins[0].alias, "rn");
        assert!(wins[0].partition_by.is_empty());
        assert_eq!(wins[0].order_by, vec![("id".to_string(), false)]);
    }

    #[test]
    fn extract_rank_with_partition() {
        let cols = vec!["RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk".to_string()];
        let (out, wins) = extract_window_functions(&cols);
        assert_eq!(out, vec!["rnk"]);
        assert_eq!(wins.len(), 1);
        assert!(matches!(wins[0].func, WindowFuncKind::Rank));
        assert_eq!(wins[0].partition_by, vec!["dept".to_string()]);
        assert_eq!(wins[0].order_by, vec![("salary".to_string(), true)]);
    }

    #[test]
    fn extract_lag_lead() {
        let cols = vec![
            "LAG(salary, 1, 0) OVER (ORDER BY id) AS prev_sal".to_string(),
            "LEAD(salary) OVER (ORDER BY id) AS next_sal".to_string(),
        ];
        let (out, wins) = extract_window_functions(&cols);
        assert_eq!(out, vec!["prev_sal", "next_sal"]);
        assert_eq!(wins.len(), 2);
        match &wins[0].func {
            WindowFuncKind::Lag {
                col,
                offset,
                default,
            } => {
                assert_eq!(col, "salary");
                assert_eq!(*offset, 1);
                assert_eq!(*default, Some(Value::Integer(0)));
            }
            _ => panic!("expected Lag"),
        }
        match &wins[1].func {
            WindowFuncKind::Lead { col, offset, .. } => {
                assert_eq!(col, "salary");
                assert_eq!(*offset, 1);
            }
            _ => panic!("expected Lead"),
        }
    }

    #[test]
    fn extract_ntile() {
        let cols = vec!["NTILE(4) OVER (ORDER BY score DESC) AS quartile".to_string()];
        let (_, wins) = extract_window_functions(&cols);
        assert!(matches!(wins[0].func, WindowFuncKind::Ntile(4)));
    }

    #[test]
    fn no_window_function() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let (out, wins) = extract_window_functions(&cols);
        assert_eq!(out, cols);
        assert!(wins.is_empty());
    }

    #[test]
    fn auto_alias() {
        let cols = vec!["ROW_NUMBER() OVER (ORDER BY id)".to_string()];
        let (out, wins) = extract_window_functions(&cols);
        assert_eq!(out, vec!["row_number"]);
        assert_eq!(wins[0].alias, "row_number");
    }
}
