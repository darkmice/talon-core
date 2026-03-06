/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SELECT 表达式列求值：支持算术表达式 `a + b`、`price * 100` 等。

use crate::types::{Schema, Value};

/// 列投影指令：普通列索引 / 算术表达式 / CASE WHEN / 函数调用。
pub(super) enum ProjectOp {
    Col(usize),
    Expr(ExprCol),
    Case(CaseExpr),
    /// SQL 内置函数调用（UPPER/LOWER/COALESCE/CAST 等）。
    Func(FuncCall),
}

/// SQL 内置函数调用：函数名 + 参数列表。
pub(super) struct FuncCall {
    /// 函数名（大写）。
    pub name: String,
    /// 参数列表：列引用或字面量。
    pub args: Vec<FuncArg>,
}

/// 函数参数：列引用或字面量。
pub(super) enum FuncArg {
    /// 列索引引用。
    Col(usize),
    /// 字面量值。
    Lit(Value),
}

/// CASE WHEN 表达式：`CASE WHEN cond THEN val [WHEN ...] [ELSE val] END`。
pub(super) struct CaseExpr {
    branches: Vec<CaseBranch>,
    else_val: Option<CaseValue>,
}

/// CASE WHEN 分支。
struct CaseBranch {
    cond: CaseCond,
    then_val: CaseValue,
}

/// CASE WHEN 条件：col op literal。
struct CaseCond {
    col_idx: usize,
    op: CaseOp,
    lit: Value,
}

/// CASE WHEN 比较运算符。
#[derive(Clone, Copy)]
enum CaseOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// CASE WHEN 结果值：列引用或字面量。
enum CaseValue {
    Col(usize),
    Lit(Value),
}

/// 算术表达式列：`left op right`。
pub(super) struct ExprCol {
    left: ExprOperand,
    op: char,
    right: ExprOperand,
}

/// 表达式操作数：列引用 或 常量。
enum ExprOperand {
    Col(usize),
    Lit(f64),
}

/// 编译单个 SELECT 列为投影指令。
pub(super) fn compile_project_op(col: &str, schema: &Schema) -> Result<ProjectOp, crate::Error> {
    let col = col.trim();
    // 去掉 AS alias：`expr AS alias` → `expr`
    let expr_part = strip_as_alias(col);
    // CASE WHEN 表达式
    if expr_part.to_uppercase().starts_with("CASE ") {
        let case_expr = parse_case_expr(expr_part, schema)?;
        return Ok(ProjectOp::Case(case_expr));
    }
    // SQL 内置函数调用：FUNC(args...)
    if let Some(fc) = try_parse_func_call(expr_part, schema)? {
        return Ok(ProjectOp::Func(fc));
    }
    // 先尝试直接匹配列名
    if let Some(idx) = schema.column_index_by_name(expr_part) {
        return Ok(ProjectOp::Col(idx));
    }
    // 尝试解析算术表达式
    if let Some(expr) = parse_arith_expr(expr_part, schema) {
        return Ok(ProjectOp::Expr(expr));
    }
    Err(crate::Error::SqlExec(format!("SELECT 列不存在: {}", col)))
}

/// 对一行数据执行投影指令。
pub(super) fn eval_project_op(row: &[Value], op: &ProjectOp) -> Value {
    match op {
        ProjectOp::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
        ProjectOp::Expr(expr) => eval_expr(row, expr),
        ProjectOp::Case(case) => eval_case(row, case),
        ProjectOp::Func(fc) => {
            let args: Vec<Value> = fc
                .args
                .iter()
                .map(|a| match a {
                    FuncArg::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
                    FuncArg::Lit(v) => v.clone(),
                })
                .collect();
            super::sql_funcs::eval_sql_func(&fc.name, &args)
        }
    }
}

/// 去掉 `AS alias` 后缀，返回表达式部分。
/// 对 CASE WHEN 表达式，只匹配 `END AS alias`（避免误匹配 CASE 体内的 AS）。
/// 对函数调用如 `CAST(x AS type)`，不剥离括号内的 AS。
fn strip_as_alias(s: &str) -> &str {
    let upper = s.to_uppercase();
    if let Some(pos) = upper.rfind(" AS ") {
        // 检查 AS 是否在括号内 — 如果是则不剥离
        let before = &s[..pos];
        let open = before.chars().filter(|&c| c == '(').count();
        let close = before.chars().filter(|&c| c == ')').count();
        if open > close {
            // AS 在未闭合的括号内（如 CAST(x AS type)），不剥离
            return s.trim();
        }
        // CASE WHEN 表达式：只在 END 之后才剥 AS
        if upper.starts_with("CASE ") {
            let before_upper = upper[..pos].trim();
            if before_upper.ends_with("END") {
                return s[..pos].trim();
            }
            return s.trim();
        }
        s[..pos].trim()
    } else {
        s.trim()
    }
}

/// 解析简单算术表达式：`operand op operand`。
/// 支持 +、-、*、/。操作数可以是列名或数字常量。
fn parse_arith_expr(s: &str, schema: &Schema) -> Option<ExprCol> {
    // 先找 +/-（低优先级），再找 */（高优先级）
    for &op_char in &['+', '-'] {
        if let Some(pos) = find_op_pos(s, op_char) {
            let left = parse_operand(s[..pos].trim(), schema)?;
            let right = parse_operand(s[pos + 1..].trim(), schema)?;
            return Some(ExprCol {
                left,
                op: op_char,
                right,
            });
        }
    }
    for &op_char in &['*', '/'] {
        if let Some(pos) = find_op_pos(s, op_char) {
            let left = parse_operand(s[..pos].trim(), schema)?;
            let right = parse_operand(s[pos + 1..].trim(), schema)?;
            return Some(ExprCol {
                left,
                op: op_char,
                right,
            });
        }
    }
    None
}

/// 查找运算符位置（跳过引号和括号内的，从右向左扫描实现左结合）。
fn find_op_pos(s: &str, op: char) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_quote = false;
    let mut i = bytes.len();
    while i > 0 {
        i -= 1;
        if bytes[i] == b'\'' {
            in_quote = !in_quote;
        } else if !in_quote {
            if bytes[i] == b')' {
                depth += 1;
            } else if bytes[i] == b'(' {
                depth -= 1;
            } else if depth == 0 && bytes[i] == op as u8 && i > 0 {
                return Some(i);
            }
        }
    }
    None
}

/// 解析操作数：列名引用 或 数字常量。
fn parse_operand(s: &str, schema: &Schema) -> Option<ExprOperand> {
    if let Some(idx) = schema.column_index_by_name(s) {
        return Some(ExprOperand::Col(idx));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Some(ExprOperand::Lit(n));
    }
    None
}

/// 对一行数据求算术表达式值。
fn eval_expr(row: &[Value], expr: &ExprCol) -> Value {
    let l = operand_to_f64(row, &expr.left);
    let r = operand_to_f64(row, &expr.right);
    match (l, r) {
        (Some(a), Some(b)) => {
            let result = match expr.op {
                '+' => a + b,
                '-' => a - b,
                '*' => a * b,
                '/' => {
                    if b == 0.0 {
                        return Value::Null;
                    }
                    a / b
                }
                _ => return Value::Null,
            };
            if result == result.trunc() && result.abs() < 9.007_199_254_740_992e15 {
                Value::Integer(result as i64)
            } else {
                Value::Float(result)
            }
        }
        _ => Value::Null,
    }
}

/// 从行中提取操作数的 f64 值。
fn operand_to_f64(row: &[Value], op: &ExprOperand) -> Option<f64> {
    match op {
        ExprOperand::Lit(n) => Some(*n),
        ExprOperand::Col(i) => match row.get(*i) {
            Some(Value::Integer(n)) => Some(*n as f64),
            Some(Value::Float(f)) => Some(*f),
            _ => None,
        },
    }
}

// ── CASE WHEN 解析与求值 ─────────────────────────────────

/// 解析 CASE WHEN 表达式。
/// 语法：`CASE WHEN col op val THEN result [WHEN ...] [ELSE result] END`
fn parse_case_expr(s: &str, schema: &Schema) -> Result<CaseExpr, crate::Error> {
    let upper = s.to_uppercase();
    if !upper.trim().ends_with("END") {
        return Err(crate::Error::SqlParse("CASE expression missing END".into()));
    }
    // 去掉 CASE 和 END，得到中间部分
    let inner = s[4..].trim();
    let inner = inner[..inner.len() - 3].trim();
    // 拆分为 token 列表（保留引号内容）
    let parts = split_case_parts(inner);
    let mut branches = Vec::new();
    let mut else_val = None;
    let mut i = 0;
    while i < parts.len() {
        let kw = parts[i].to_uppercase();
        if kw == "WHEN" {
            // 收集 WHEN 到 THEN 之间的条件
            let mut cond_parts = Vec::new();
            i += 1;
            while i < parts.len() && parts[i].to_uppercase() != "THEN" {
                cond_parts.push(parts[i].as_str());
                i += 1;
            }
            if i >= parts.len() {
                return Err(crate::Error::SqlParse("CASE WHEN missing THEN".into()));
            }
            i += 1; // skip THEN
                    // 收集 THEN 到下一个 WHEN/ELSE/END 之间的值
            let mut val_parts = Vec::new();
            while i < parts.len() {
                let pk = parts[i].to_uppercase();
                if pk == "WHEN" || pk == "ELSE" || pk == "END" {
                    break;
                }
                val_parts.push(parts[i].as_str());
                i += 1;
            }
            let cond_str = cond_parts.join(" ");
            let val_str = val_parts.join(" ");
            let cond = parse_case_cond(&cond_str, schema)?;
            let then_val = parse_case_value(&val_str, schema)?;
            branches.push(CaseBranch { cond, then_val });
        } else if kw == "ELSE" {
            i += 1;
            let mut val_parts = Vec::new();
            while i < parts.len() {
                let pk = parts[i].to_uppercase();
                if pk == "END" {
                    break;
                }
                val_parts.push(parts[i].as_str());
                i += 1;
            }
            let val_str = val_parts.join(" ");
            else_val = Some(parse_case_value(&val_str, schema)?);
            break;
        } else {
            i += 1;
        }
    }
    if branches.is_empty() {
        return Err(crate::Error::SqlParse(
            "CASE expression has no WHEN branches".into(),
        ));
    }
    Ok(CaseExpr { branches, else_val })
}

/// 将 CASE 表达式内部按空白分割为 token，但保留引号内容为单个 token。
fn split_case_parts(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // 跳过空白
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if bytes[i] == b'\'' {
            // 引号字符串：收集到匹配的引号
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            parts.push(s[start..i].to_string());
        } else {
            // 普通 token
            let start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            parts.push(s[start..i].to_string());
        }
    }
    parts
}

/// 解析 CASE WHEN 条件：`col op literal`。
fn parse_case_cond(s: &str, schema: &Schema) -> Result<CaseCond, crate::Error> {
    let s = s.trim();
    // 尝试各种比较运算符（先尝试双字符，再单字符）
    for (op_str, op) in &[
        (">=", CaseOp::Ge),
        ("<=", CaseOp::Le),
        ("!=", CaseOp::Ne),
        ("<>", CaseOp::Ne),
        (">", CaseOp::Gt),
        ("<", CaseOp::Lt),
        ("=", CaseOp::Eq),
    ] {
        if let Some(pos) = s.find(op_str) {
            let col_name = s[..pos].trim().to_ascii_lowercase();
            let val_str = s[pos + op_str.len()..].trim();
            let col_idx = schema.column_index_by_name(&col_name).ok_or_else(|| {
                crate::Error::SqlExec(format!("CASE WHEN 列不存在: {}", col_name))
            })?;
            let lit = crate::sql::parser::parse_row_single_pass(val_str)
                .map_err(|_| crate::Error::SqlParse(format!("CASE WHEN 值解析失败: {}", val_str)))?
                .into_iter()
                .next()
                .ok_or_else(|| crate::Error::SqlParse(format!("CASE WHEN 值为空: {}", val_str)))?;
            return Ok(CaseCond {
                col_idx,
                op: *op,
                lit,
            });
        }
    }
    Err(crate::Error::SqlParse(format!(
        "CASE WHEN 条件解析失败: {}",
        s
    )))
}

/// 解析 CASE WHEN 结果值。
fn parse_case_value(s: &str, schema: &Schema) -> Result<CaseValue, crate::Error> {
    let s = s.trim();
    // 先尝试列引用
    if let Some(idx) = schema.column_index_by_name(&s.to_ascii_lowercase()) {
        return Ok(CaseValue::Col(idx));
    }
    // 再尝试字面量
    let vals = crate::sql::parser::parse_row_single_pass(s)
        .map_err(|_| crate::Error::SqlParse(format!("CASE THEN/ELSE 值解析失败: {}", s)))?;
    if let Some(v) = vals.into_iter().next() {
        Ok(CaseValue::Lit(v))
    } else {
        Err(crate::Error::SqlParse(format!(
            "CASE THEN/ELSE 值为空: {}",
            s
        )))
    }
}

/// 对一行数据求 CASE WHEN 表达式值。
fn eval_case(row: &[Value], case: &CaseExpr) -> Value {
    for branch in &case.branches {
        if eval_case_cond(row, &branch.cond) {
            return eval_case_value(row, &branch.then_val);
        }
    }
    match &case.else_val {
        Some(v) => eval_case_value(row, v),
        None => Value::Null,
    }
}

/// 求值 CASE WHEN 条件。
fn eval_case_cond(row: &[Value], cond: &CaseCond) -> bool {
    let col_val = match row.get(cond.col_idx) {
        Some(v) => v,
        None => return false,
    };
    match cond.op {
        CaseOp::Eq => col_val == &cond.lit,
        CaseOp::Ne => col_val != &cond.lit,
        CaseOp::Gt => val_cmp(col_val, &cond.lit) == Some(std::cmp::Ordering::Greater),
        CaseOp::Ge => matches!(
            val_cmp(col_val, &cond.lit),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        CaseOp::Lt => val_cmp(col_val, &cond.lit) == Some(std::cmp::Ordering::Less),
        CaseOp::Le => matches!(
            val_cmp(col_val, &cond.lit),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
    }
}

/// 求值 CASE WHEN 结果值。
fn eval_case_value(row: &[Value], v: &CaseValue) -> Value {
    match v {
        CaseValue::Lit(val) => val.clone(),
        CaseValue::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
    }
}

/// 值比较（数值类型互通）。
fn val_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)),
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::Time(x), Value::Time(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// ── SQL 内置函数调用解析 ─────────────────────────────────

/// 尝试解析 SQL 函数调用：`FUNC(arg1, arg2, ...)`。
/// CAST 特殊语法：`CAST(expr AS type)` → args = [expr_val, Text("TYPE")]。
fn try_parse_func_call(s: &str, schema: &Schema) -> Result<Option<FuncCall>, crate::Error> {
    let s = s.trim();
    // 查找函数名：字母开头，后跟 '('
    let paren_pos = match s.find('(') {
        Some(p) if p > 0 => p,
        _ => return Ok(None),
    };
    let func_name = s[..paren_pos].trim().to_uppercase();
    if !super::sql_funcs::is_known_func(&func_name) {
        return Ok(None);
    }
    // 找到匹配的右括号
    if !s.ends_with(')') {
        return Ok(None);
    }
    let inner = &s[paren_pos + 1..s.len() - 1];
    // CAST 特殊处理：CAST(expr AS type)
    if func_name == "CAST" {
        let args = parse_cast_args(inner, schema)?;
        return Ok(Some(FuncCall {
            name: func_name,
            args,
        }));
    }
    // 通用函数参数解析
    let args = parse_func_args(inner, schema)?;
    Ok(Some(FuncCall {
        name: func_name,
        args,
    }))
}

/// 解析通用函数参数列表（逗号分隔，支持列引用和字面量）。
fn parse_func_args(s: &str, schema: &Schema) -> Result<Vec<FuncArg>, crate::Error> {
    if s.trim().is_empty() {
        return Ok(vec![]);
    }
    let parts = split_func_args(s);
    let mut args = Vec::with_capacity(parts.len());
    for part in &parts {
        let part = part.trim();
        // 先尝试列引用
        if let Some(idx) = schema.column_index_by_name(&part.to_ascii_lowercase()) {
            args.push(FuncArg::Col(idx));
            continue;
        }
        // 再尝试字面量
        if let Some(val) = crate::sql::parser::parse_value(part) {
            args.push(FuncArg::Lit(val));
            continue;
        }
        // 数字字面量
        if let Ok(n) = part.parse::<i64>() {
            args.push(FuncArg::Lit(Value::Integer(n)));
            continue;
        }
        if let Ok(f) = part.parse::<f64>() {
            args.push(FuncArg::Lit(Value::Float(f)));
            continue;
        }
        return Err(crate::Error::SqlExec(format!("函数参数解析失败: {}", part)));
    }
    Ok(args)
}

/// 解析 CAST(expr AS type) 参数：返回 [值, Text("TYPE")]。
fn parse_cast_args(s: &str, schema: &Schema) -> Result<Vec<FuncArg>, crate::Error> {
    // 查找 " AS " 关键字（大小写不敏感）
    let upper = s.to_uppercase();
    let as_pos = upper
        .find(" AS ")
        .ok_or_else(|| crate::Error::SqlParse("CAST missing AS keyword".into()))?;
    let expr_part = s[..as_pos].trim();
    let type_part = s[as_pos + 4..].trim();
    // 解析表达式部分
    let expr_arg = if let Some(idx) = schema.column_index_by_name(&expr_part.to_ascii_lowercase()) {
        FuncArg::Col(idx)
    } else if let Some(val) = crate::sql::parser::parse_value(expr_part) {
        FuncArg::Lit(val)
    } else {
        return Err(crate::Error::SqlExec(format!(
            "CAST 表达式解析失败: {}",
            expr_part
        )));
    };
    // 类型名作为 Text 字面量传递
    let type_arg = FuncArg::Lit(Value::Text(type_part.to_uppercase()));
    Ok(vec![expr_arg, type_arg])
}

/// 按逗号分割函数参数（尊重引号和括号嵌套）。
fn split_func_args(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut depth = 0i32;
    let mut in_quote = false;
    for i in 0..bytes.len() {
        if bytes[i] == b'\'' {
            if in_quote && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                continue; // 转义引号
            }
            in_quote = !in_quote;
        } else if !in_quote {
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth -= 1;
            } else if bytes[i] == b',' && depth == 0 {
                parts.push(s[start..i].to_string());
                start = i + 1;
            }
        }
    }
    parts.push(s[start..].to_string());
    parts
}
