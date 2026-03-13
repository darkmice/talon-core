/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL AST 类型定义与基础解析辅助函数。
use crate::types::{ColumnType, Value};
/// 列定义（CREATE TABLE 时使用）。
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// 列名。
    pub name: String,
    /// 列类型。
    pub col_type: ColumnType,
    /// 是否允许 NULL（默认 true，NOT NULL 时为 false）。
    pub nullable: bool,
    /// DEFAULT 表达式对应的值。
    pub default_value: Option<Value>,
    /// 是否为 AUTOINCREMENT 主键（仅第一列有效）。
    pub auto_increment: bool,
    /// M127：列级外键引用 `REFERENCES parent(col)`。
    pub foreign_key: Option<(String, String)>,
}
/// SQL 语句 AST。
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Stmt {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        /// IF NOT EXISTS — 表已存在时静默跳过。
        if_not_exists: bool,
        /// 复合唯一约束：`UNIQUE(col1, col2)`。
        unique_constraints: Vec<Vec<String>>,
        /// CHECK 约束原始 SQL 文本列表（列级 + 表级合并）。
        /// 运行时解析为 WhereExpr 并缓存到 TableCache。
        check_constraints: Vec<String>,
        /// M126：临时表标记 — CREATE TEMP TABLE。
        temporary: bool,
        /// M127：外键约束列表 (子列, 父表, 父列)。
        foreign_keys: Vec<(String, String, String)>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Value>>,
        /// INSERT OR REPLACE 模式：主键冲突时覆盖而非报错。
        or_replace: bool,
        /// INSERT OR IGNORE 模式：主键冲突时静默跳过该行。
        or_ignore: bool,
        /// ON CONFLICT DO UPDATE SET ... 子句（标准 SQL UPSERT）。
        on_conflict: Option<OnConflict>,
        /// RETURNING 子句（PostgreSQL 兼容）：返回被插入行的指定列。
        returning: Option<Vec<String>>,
        /// INSERT INTO ... SELECT 子句：从查询结果插入。
        source_select: Option<Box<Stmt>>,
    },
    Select {
        table: String,
        columns: Vec<String>,
        where_clause: Option<WhereExpr>,
        /// ORDER BY 列：(列名, 是否DESC, NULLS FIRST 覆盖)。
        /// 第三元素：None=默认(ASC→NULLS LAST, DESC→NULLS FIRST)，
        /// Some(true)=NULLS FIRST，Some(false)=NULLS LAST。
        order_by: Option<Vec<(String, bool, Option<bool>)>>,
        limit: Option<u64>,
        /// OFFSET N — 跳过前 N 行（分页查询）。
        offset: Option<u64>,
        distinct: bool,
        /// DISTINCT ON (col1, col2, ...) — PostgreSQL 兼容去重。
        distinct_on: Option<Vec<String>>,
        /// 向量搜索表达式（如 `vec_distance(col, [...]) AS dist`）。
        vec_search: Option<VecSearchExpr>,
        /// M94：地理空间搜索表达式（ST_DISTANCE）。
        geo_search: Option<GeoSearchExpr>,
        /// M92：JOIN 子句。
        join: Option<JoinClause>,
        /// GROUP BY 列名列表。
        group_by: Option<Vec<String>>,
        /// HAVING 过滤条件（对聚合结果过滤）。
        having: Option<WhereExpr>,
        /// M113：CTE 子句列表（`WITH name AS (SELECT ...)`）。
        ctes: Vec<CteClause>,
        /// M177：窗口函数表达式列表。
        window_functions: Vec<WindowExpr>,
    },
    Delete {
        table: String,
        where_clause: Option<WhereExpr>,
        /// RETURNING 子句（PostgreSQL 兼容）：返回被删除行的指定列。
        returning: Option<Vec<String>>,
        /// M163: DELETE ... USING source_table（PostgreSQL 兼容多表删除）。
        using_table: Option<String>,
    },
    Update {
        table: String,
        assignments: Vec<(String, SetExpr)>,
        where_clause: Option<WhereExpr>,
        /// RETURNING 子句（PostgreSQL 兼容）：返回被更新行的指定列。
        returning: Option<Vec<String>>,
        /// M116：UPDATE ... FROM source_table — 跨表更新的源表名。
        from_table: Option<String>,
        /// M117：UPDATE ... ORDER BY col [ASC|DESC] [NULLS FIRST|LAST]。
        order_by: Option<Vec<(String, bool, Option<bool>)>>,
        /// M117：UPDATE ... LIMIT n — 限制更新行数。
        limit: Option<u64>,
    },
    CreateIndex {
        index_name: String,
        table: String,
        /// M112：支持复合索引，单列索引为 `vec!["col"]`。
        columns: Vec<String>,
        /// M111：是否为唯一索引（CREATE UNIQUE INDEX）。
        unique: bool,
    },
    /// 开始事务。
    Begin,
    /// 提交事务。
    Commit,
    /// 回滚事务。
    Rollback,
    /// SAVEPOINT name — 创建事务保存点。
    Savepoint {
        name: String,
    },
    /// RELEASE SAVEPOINT name — 释放保存点。
    Release {
        name: String,
    },
    /// ROLLBACK TO SAVEPOINT name — 回滚到保存点。
    RollbackTo {
        name: String,
    },
    /// ALTER TABLE ADD COLUMN。
    AlterTable {
        table: String,
        action: AlterAction,
    },
    /// SHOW TABLES — 列出所有表。
    ShowTables,
    /// SHOW INDEXES [ON table] — 列出索引。
    ShowIndexes {
        table: Option<String>,
    },
    /// DESCRIBE table — 显示表结构。
    Describe {
        table: String,
    },
    /// CREATE VECTOR INDEX idx ON table(col) USING HNSW WITH (metric=..., m=..., ef_construction=...)。
    CreateVectorIndex {
        index_name: String,
        table: String,
        column: String,
        metric: String,
        m: usize,
        ef_construction: usize,
    },
    /// DROP VECTOR INDEX [IF EXISTS] idx_name。
    DropVectorIndex {
        index_name: String,
        if_exists: bool,
    },
    /// DROP INDEX [IF EXISTS] idx_name。
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
    /// TRUNCATE TABLE name — 快速清空表数据。
    Truncate {
        table: String,
    },
    /// UNION / UNION ALL / INTERSECT / EXCEPT — 集合操作。
    Union {
        left: Box<Stmt>,
        right: Box<Stmt>,
        all: bool,
        /// 集合操作类型。
        op: SetOpKind,
    },
    /// EXPLAIN SELECT ... — 查询计划分析。
    Explain {
        inner: Box<Stmt>,
    },
    /// M125：CREATE VIEW name AS SELECT ... — 创建只读视图。
    CreateView {
        name: String,
        /// IF NOT EXISTS — 视图已存在时静默跳过。
        if_not_exists: bool,
        /// 视图定义的 SQL 文本（SELECT 语句原文）。
        sql: String,
    },
    /// M125：DROP VIEW [IF EXISTS] name — 删除视图。
    DropView {
        name: String,
        if_exists: bool,
    },
    /// M164：COMMENT ON TABLE/COLUMN — 添加注释。
    Comment {
        /// 注释目标：表名。
        table: String,
        /// 列名（None 表示表级注释）。
        column: Option<String>,
        /// 注释文本。
        text: String,
    },
    /// M197：ANALYZE table — 收集表级统计信息（行数、NDV、min/max）。
    Analyze {
        table: String,
    },
}
/// SQL 嵌入式向量搜索表达式。
/// 对应 `vec_distance(col, [...]) AS alias` 或 `vec_cosine` / `vec_l2` / `vec_dot`。
#[derive(Debug, Clone)]
pub struct VecSearchExpr {
    /// 向量列名。
    pub column: String,
    /// 查询向量字面量。
    pub query_vec: Vec<f32>,
    /// 距离度量：cosine / l2 / dot / distance（使用索引定义的度量）。
    pub metric: String,
    /// 别名（AS 后的名称），用于 ORDER BY 引用。
    pub alias: Option<String>,
}
/// M110：UPDATE SET 赋值表达式。
#[derive(Debug, Clone)]
pub enum SetExpr {
    /// 字面量赋值：`SET col = 42`
    Literal(Value),
    /// 列算术表达式：`SET col = col + 1`
    ColumnArith(String, ArithOp, Value),
    /// M116：跨表列引用：`SET col = src_table.col`
    ColumnRef(String, String),
}
/// M110：算术运算符。
#[derive(Debug, Clone, Copy)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}
/// INSERT ... ON CONFLICT DO UPDATE SET ... 子句。
///
/// 语法：`ON CONFLICT (col1[, col2]) DO UPDATE SET col1 = EXCLUDED.col1, ...`
/// `EXCLUDED` 引用 INSERT 中的新行值。
#[derive(Debug, Clone)]
pub struct OnConflict {
    /// 冲突列名列表（单列 PK 或复合唯一约束列）。
    pub conflict_columns: Vec<String>,
    /// DO UPDATE SET 赋值列表：(目标列名, 源列名)。
    /// 源列名可以是 `EXCLUDED.col`（引用新行值）或普通列名（引用旧行值）。
    pub assignments: Vec<(String, OnConflictValue)>,
}
/// ON CONFLICT SET 赋值的值来源。
#[derive(Debug, Clone)]
pub enum OnConflictValue {
    /// 引用 INSERT 新行中的列值：`EXCLUDED.col`。
    Excluded(String),
    /// 字面量值。
    Literal(Value),
}
/// ALTER TABLE 操作类型。
#[derive(Debug, Clone)]
pub enum AlterAction {
    /// 添加列（可选默认值）。
    AddColumn {
        name: String,
        col_type: ColumnType,
        default: Option<Value>,
    },
    /// 删除列（O(1)，标记删除，不重建数据）。
    DropColumn { name: String },
    /// 重命名列（O(1)，只改 schema 元数据）。
    RenameColumn { old_name: String, new_name: String },
    /// 重命名表（O(N)，需迁移数据 keyspace）。
    RenameTo { new_name: String },
    /// M165：修改列默认值。
    SetDefault { column: String, value: Value },
    /// M165：删除列默认值。
    DropDefault { column: String },
    /// M169：修改列类型（PostgreSQL ALTER COLUMN TYPE / MySQL MODIFY）。
    AlterType {
        column: String,
        new_type: crate::types::ColumnType,
    },
}
/// 集合操作类型（UNION / INTERSECT / EXCEPT）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetOpKind {
    /// UNION — 合并两个结果集。
    Union,
    /// INTERSECT — 取两个结果集的交集。
    Intersect,
    /// EXCEPT — 取左结果集减去右结果集的差集。
    Except,
}
/// JOIN 类型。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    /// CROSS JOIN — 笛卡尔积，无 ON 条件。
    Cross,
    /// NATURAL JOIN — 自动匹配同名列做等值连接。
    Natural,
    /// FULL OUTER JOIN — 左右表全保留，不匹配的用 NULL 填充。
    Full,
}
/// M121：JOIN 子句，支持表别名和多条件 ON。
/// 多表 JOIN 通过 `next` 链式连接：`A JOIN B ON ... JOIN C ON ...`。
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    /// M121：右表别名（`JOIN t AS alias`）。
    pub table_alias: Option<String>,
    pub left_col: String,
    pub right_col: String,
    /// 链式 JOIN：下一个 JOIN 子句（多表 JOIN 支持）。
    pub next: Option<Box<JoinClause>>,
}
/// WHERE 表达式树：支持 AND / OR / 括号嵌套。
///
/// AND 优先级高于 OR（标准 SQL 语义），括号可提升优先级。
/// 例如 `a = 1 OR b = 2 AND c = 3` 解析为 `Or(Leaf(a=1), And(Leaf(b=2), Leaf(c=3)))`。
#[derive(Debug, Clone)]
pub enum WhereExpr {
    /// AND 连接的子表达式列表。
    And(Vec<WhereExpr>),
    /// OR 连接的子表达式列表。
    Or(Vec<WhereExpr>),
    /// 叶子节点：单个条件。
    Leaf(WhereCondition),
}
/// WHERE 条件（单个 col op val，或 LIKE/IN/BETWEEN）。
#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub column: String,
    pub op: WhereOp,
    pub value: Value,
    /// IN 列表值（仅 op == In 时使用）。
    pub in_values: Vec<Value>,
    /// BETWEEN 上界（仅 op == Between 时使用，value 为下界）。
    pub value_high: Option<Value>,
    /// JSONB path key（仅 `col->>'key'` 形式时使用）。
    pub jsonb_path: Option<String>,
    /// 子查询（仅 `IN (SELECT ...)` 时使用）。
    pub subquery: Option<Box<Stmt>>,
    /// LIKE ... ESCAPE 'x' 转义字符（仅 Like/NotLike 时使用）。
    pub escape_char: Option<char>,
    /// M118：右侧列引用（CHECK 约束中 `lo <= hi` 的 `hi`）。
    /// 运行时从行数据中取该列的值作为比较右侧。
    pub value_column: Option<String>,
}
/// WHERE 操作符。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WhereOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    NotLike,
    In,
    NotIn,
    Between,
    NotBetween,
    IsNull,
    IsNotNull,
    /// M94：ST_WITHIN(col, lat, lng, radius_m)。value=GeoPoint, value_high=Float(radius)。
    StWithin,
    /// GLOB 模式匹配（大小写敏感，`*` 任意字符串，`?` 单字符）。
    Glob,
    /// NOT GLOB。
    NotGlob,
    /// REGEXP 正则匹配（`col REGEXP 'pattern'`）。
    Regexp,
    /// NOT REGEXP。
    NotRegexp,
    /// EXISTS (SELECT ...) — 子查询有结果返回 true。
    Exists,
    /// NOT EXISTS (SELECT ...) — 子查询无结果返回 true。
    NotExists,
}
/// M94：地理空间搜索表达式（SELECT ST_DISTANCE(col, GEOPOINT(lat,lng)) AS alias）。
#[derive(Debug, Clone)]
pub struct GeoSearchExpr {
    pub column: String,
    pub target_lat: f64,
    pub target_lng: f64,
    pub alias: Option<String>,
}
/// CTE（公共表表达式）子句：`WITH name AS (SELECT ...)`。
#[derive(Debug, Clone)]
pub struct CteClause {
    /// CTE 名称。
    pub name: String,
    /// CTE 查询。
    pub query: Box<Stmt>,
}

/// M177：窗口函数类型。
#[derive(Debug, Clone)]
pub enum WindowFuncKind {
    /// ROW_NUMBER() — 分区内连续行号。
    RowNumber,
    /// RANK() — 并列排名（有间隔）。
    Rank,
    /// DENSE_RANK() — 并列排名（无间隔）。
    DenseRank,
    /// NTILE(n) — 分区内 n 等分桶号。
    Ntile(usize),
    /// LAG(col, offset, default) — 前 offset 行的值。
    Lag {
        col: String,
        offset: usize,
        default: Option<crate::types::Value>,
    },
    /// LEAD(col, offset, default) — 后 offset 行的值。
    Lead {
        col: String,
        offset: usize,
        default: Option<crate::types::Value>,
    },
    /// SUM(col) OVER (...) — 分区内累计求和。
    Sum(String),
    /// COUNT(*) OVER (...) — 分区内计数。
    Count,
    /// AVG(col) OVER (...) — 分区内平均值。
    Avg(String),
    /// MIN(col) OVER (...) — 分区内最小值。
    Min(String),
    /// MAX(col) OVER (...) — 分区内最大值。
    Max(String),
}

/// M177：窗口函数表达式。
#[derive(Debug, Clone)]
pub struct WindowExpr {
    /// 窗口函数类型。
    pub func: WindowFuncKind,
    /// PARTITION BY 列列表。
    pub partition_by: Vec<String>,
    /// ORDER BY 列列表：(列名, 是否 DESC)。
    pub order_by: Vec<(String, bool)>,
    /// AS 别名（用于 SELECT 输出列名）。
    pub alias: String,
}
