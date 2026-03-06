/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量搜索表达式解析：从 SELECT 列中提取 vec_distance/vec_cosine/vec_l2/vec_dot。

use super::types::VecSearchExpr;

/// 向量函数前缀与对应的 metric 名称。
const VEC_FUNCS: &[(&str, &str)] = &[
    ("VEC_DISTANCE(", "distance"),
    ("VEC_COSINE(", "cosine"),
    ("VEC_L2(", "l2"),
    ("VEC_DOT(", "dot"),
];

/// 从 SELECT 列列表中提取向量搜索表达式。
/// 返回 (过滤后的普通列, Option<VecSearchExpr>)。
/// 向量函数列从 columns 中移除，普通列保留。
pub(crate) fn extract_vec_search(columns: &[String]) -> (Vec<String>, Option<VecSearchExpr>) {
    let mut normal_cols = Vec::new();
    let mut vec_expr: Option<VecSearchExpr> = None;

    for col in columns {
        let trimmed = col.trim();
        if let Some(expr) = try_parse_vec_func(trimmed) {
            vec_expr = Some(expr);
        } else {
            normal_cols.push(col.clone());
        }
    }
    // 如果没有普通列且有向量搜索，保留 "*" 以获取全部列
    if normal_cols.is_empty() && vec_expr.is_some() {
        normal_cols.push("*".to_string());
    }
    (normal_cols, vec_expr)
}

/// 尝试解析单个列表达式为向量搜索函数。
/// 支持格式：`vec_distance(col, [0.1, 0.2, ...]) AS alias`
fn try_parse_vec_func(expr: &str) -> Option<VecSearchExpr> {
    // 分离 AS alias
    let (func_part, alias) = split_as_alias(expr);
    let upper = func_part.to_uppercase().replace(' ', "");

    for &(prefix, metric) in VEC_FUNCS {
        if upper.starts_with(prefix) && upper.ends_with(')') {
            // 从原始字符串（去空格版）提取括号内内容
            let orig_no_space = func_part.replace(' ', "");
            let inner = &orig_no_space[prefix.len()..orig_no_space.len() - 1];
            return parse_vec_func_args(inner, metric, alias);
        }
    }
    None
}

/// 分离 "expr AS alias" → ("expr", Some("alias"))。
fn split_as_alias(s: &str) -> (&str, Option<String>) {
    // 在字节层面查找 " AS "（ASCII 大小写不敏感），避免 to_uppercase() 的 UTF-8 偏移问题
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut last_as_pos = None;
    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'[' => bracket_depth += 1,
            b']' => bracket_depth -= 1,
            _ => {}
        }
        if depth == 0 && bracket_depth == 0 && i + 4 <= bytes.len() {
            // 匹配 " AS "（空格 + A/a + S/s + 空格）
            if bytes[i] == b' '
                && (bytes[i + 1] == b'A' || bytes[i + 1] == b'a')
                && (bytes[i + 2] == b'S' || bytes[i + 2] == b's')
                && bytes[i + 3] == b' '
            {
                last_as_pos = Some(i);
            }
        }
    }
    match last_as_pos {
        Some(pos) => {
            let alias = s[pos + 4..].trim().to_string();
            (&s[..pos], Some(alias))
        }
        None => (s, None),
    }
}

/// 解析向量函数参数：`col,[0.1,0.2,...]`。
fn parse_vec_func_args(inner: &str, metric: &str, alias: Option<String>) -> Option<VecSearchExpr> {
    // 找到第一个 '[' 的位置，之前是列名
    let bracket_start = inner.find('[')?;
    let bracket_end = inner.rfind(']')?;
    if bracket_end <= bracket_start {
        return None;
    }
    // 列名：bracket_start 之前，去掉尾部逗号
    let col_part = inner[..bracket_start].trim_end_matches(',').trim();
    if col_part.is_empty() {
        return None;
    }
    // 向量字面量
    let vec_str = &inner[bracket_start + 1..bracket_end];
    let query_vec = parse_vec_literal(vec_str)?;
    if query_vec.is_empty() {
        return None;
    }
    Some(VecSearchExpr {
        column: col_part.to_string(),
        query_vec,
        metric: metric.to_string(),
        alias,
    })
}

/// 解析向量字面量：`0.1,0.2,0.3,...` → Vec<f32>。
fn parse_vec_literal(s: &str) -> Option<Vec<f32>> {
    let mut result = Vec::new();
    for part in s.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let val: f32 = trimmed.parse().ok()?;
        result.push(val);
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_vec_distance_with_alias() {
        let cols = vec![
            "id".to_string(),
            "vec_distance(embedding, [0.1, 0.2, 0.3]) AS dist".to_string(),
        ];
        let (normal, vs) = extract_vec_search(&cols);
        assert_eq!(normal, vec!["id".to_string()]);
        let vs = vs.unwrap();
        assert_eq!(vs.column, "embedding");
        assert_eq!(vs.metric, "distance");
        assert_eq!(vs.query_vec, vec![0.1, 0.2, 0.3]);
        assert_eq!(vs.alias.as_deref(), Some("dist"));
    }

    #[test]
    fn parse_vec_cosine_no_alias() {
        let cols = vec!["vec_cosine(emb, [1.0, 2.0])".to_string()];
        let (normal, vs) = extract_vec_search(&cols);
        assert_eq!(normal, vec!["*".to_string()]);
        let vs = vs.unwrap();
        assert_eq!(vs.column, "emb");
        assert_eq!(vs.metric, "cosine");
        assert!(vs.alias.is_none());
    }

    #[test]
    fn no_vec_func() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let (normal, vs) = extract_vec_search(&cols);
        assert_eq!(normal.len(), 2);
        assert!(vs.is_none());
    }
}
