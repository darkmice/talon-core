# SQL 引擎

完整的关系型数据库引擎，支持丰富的 SQL 方言、PostgreSQL/MySQL/SQLite 兼容语法和高性能批量操作。

## 概述

SQL 引擎提供完整的关系型数据库能力，包括 ACID 事务（MVCC 快照隔离）、二级索引、窗口函数、CTE、多表 JOIN、视图、保存点、外键和参数化查询。

## 快速开始

```rust
use talon::Talon;

let db = Talon::open("./data")?;

db.run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
db.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")?;
let rows = db.run_sql("SELECT * FROM users WHERE age > 25")?;
```

## API 参考

### `Talon::run_sql`

```rust
pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, Error>
```

### `Talon::run_sql_param`

参数化查询，支持 `?` 和 `$1` 占位符。

```rust
pub fn run_sql_param(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, Error>
```

### `Talon::run_sql_batch`

批量执行，一次锁获取。

```rust
pub fn run_sql_batch(&self, sqls: &[&str]) -> Result<Vec<Result<Vec<Vec<Value>>, Error>>, Error>
```

### `Talon::batch_insert_rows`

高性能原生批量插入 — 绕过 SQL 解析。

```rust
pub fn batch_insert_rows(&self, table: &str, columns: &[&str], rows: Vec<Vec<Value>>) -> Result<(), Error>
```

性能：**241,697 行/秒**。

### `Talon::import_sql` / `import_sql_file`

从 SQL 转储流导入（支持 SQLite `.dump` 格式）。

```rust
pub fn import_sql(&self, reader: impl std::io::BufRead) -> Result<SqlImportStats, Error>
pub fn import_sql_file(&self, path: impl AsRef<Path>) -> Result<SqlImportStats, Error>
```

## 完整 SQL 方言参考

### 解析器与方言核心

Talon 的 SQL 解析器基于 `sqlparser-rs` 构建，并且其执行引擎集成了从 `apache/datafusion` 移植的表达式和函数。这一底层架构使 Talon 能够原生支持广泛的 标准 SQL 和高级语法，主要包括：

- **复杂查询**：公用表表达式（CTE / `WITH` 语句）、嵌套子查询及派生表。
- **高级连接**：支持 `INNER`、`LEFT`、`RIGHT`、`FULL OUTER`、`CROSS` 以及 `NATURAL` JOIN。
- **分析特性**：完整的窗口函数支持（`OVER`、`PARTITION BY`、`ORDER BY`）以及丰富的聚合操作。
- **集合操作**：支持 `UNION`、`UNION ALL`、`INTERSECT` 和 `EXCEPT`。
- **复杂表达式**：全面支持 `CASE` 表达式、`CAST`/`CONVERT` 类型转换、算术运算以及复杂的逻辑条件组合。

### DDL（数据定义语言）

#### CREATE TABLE

```sql
CREATE TABLE t (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT DEFAULT 'unknown',
    score FLOAT,
    data JSONB,
    embedding VECTOR(768),
    created_at TIMESTAMP,
    UNIQUE(name, email),                        -- 复合唯一约束
    CHECK(score >= 0 AND score <= 100),          -- 检查约束
    FOREIGN KEY (dept_id) REFERENCES depts(id)   -- 外键
);

CREATE TABLE IF NOT EXISTS t (...);  -- 已存在时静默跳过
CREATE TEMP TABLE scratch (id INTEGER, val TEXT);  -- 临时表
```

**列约束：** `PRIMARY KEY`、`AUTOINCREMENT`、`NOT NULL`、`DEFAULT`、`UNIQUE`、`CHECK(expr)`、`REFERENCES parent(col)`。

**表约束：** `UNIQUE(col1, col2)`、`CHECK(expr)`、`FOREIGN KEY (col) REFERENCES parent(col)`。

#### ALTER TABLE

```sql
ALTER TABLE t ADD COLUMN email TEXT;
ALTER TABLE t ADD COLUMN email TEXT DEFAULT 'none';
ALTER TABLE t DROP COLUMN email;
ALTER TABLE t RENAME COLUMN name TO username;
ALTER TABLE t RENAME TO new_table_name;
ALTER TABLE t ALTER COLUMN score TYPE INTEGER;          -- 修改列类型
ALTER TABLE t ALTER COLUMN name SET DEFAULT 'unknown';  -- 设置默认值
ALTER TABLE t ALTER COLUMN name DROP DEFAULT;           -- 删除默认值
```

#### DROP / TRUNCATE

```sql
DROP TABLE t;
DROP TABLE IF EXISTS t;
TRUNCATE TABLE t;   -- 快速清空数据，保留表结构
```

#### 索引

```sql
CREATE INDEX idx_name ON t(name);
CREATE INDEX idx_comp ON t(name, age);    -- 复合索引
CREATE UNIQUE INDEX idx_email ON t(email);
DROP INDEX idx_name;
DROP INDEX IF EXISTS idx_name;

-- 向量索引（HNSW）
CREATE VECTOR INDEX emb_idx ON t(embedding) USING HNSW
    WITH (metric='cosine', m=16, ef_construction=200);
DROP VECTOR INDEX IF EXISTS emb_idx;
```

#### 视图

```sql
CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;
CREATE VIEW IF NOT EXISTS v AS SELECT ...;
DROP VIEW active_users;
DROP VIEW IF EXISTS active_users;
```

#### 注释

```sql
COMMENT ON TABLE users IS '用户表';
COMMENT ON COLUMN users.name IS '用户名';
```

#### 元数据查询

```sql
SHOW TABLES;
SHOW INDEXES;
SHOW INDEXES ON users;
DESCRIBE users;
EXPLAIN SELECT * FROM users WHERE age > 25;
```

### DML（数据操作语言）

#### INSERT

```sql
INSERT INTO t (id, name) VALUES (1, 'Alice');
INSERT INTO t (name, age) VALUES ('Alice', 30), ('Bob', 25);  -- 多行

-- Upsert：冲突时更新
INSERT INTO t VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

-- Replace：冲突时覆盖
INSERT OR REPLACE INTO t VALUES (1, 'Alice', 30);

-- Ignore：冲突时跳过
INSERT OR IGNORE INTO t VALUES (1, 'Alice', 30);

-- INSERT ... SELECT
INSERT INTO archive SELECT * FROM logs WHERE created_at < '2024-01-01';

-- RETURNING（PostgreSQL 兼容）
INSERT INTO t (name) VALUES ('Bob') RETURNING id, name;
```

#### UPDATE

```sql
UPDATE t SET name = 'Charlie' WHERE id = 1;
UPDATE t SET score = score + 10 WHERE score < 50;        -- 算术赋值
UPDATE t SET price = price * 1.1;

-- 跨表更新（PostgreSQL FROM 语法）
UPDATE t SET name = src.name FROM source_table AS src WHERE t.id = src.id;

-- ORDER BY + LIMIT（更新前 N 条）
UPDATE t SET status = 'reviewed' WHERE status = 'pending' ORDER BY created_at LIMIT 100;

-- RETURNING
UPDATE t SET score = score + 1 WHERE id = 1 RETURNING id, score;
```

#### DELETE

```sql
DELETE FROM t WHERE id = 1;

-- 多表删除（PostgreSQL USING 语法）
DELETE FROM t1 USING t2 WHERE t1.id = t2.id AND t2.active = 0;

-- RETURNING
DELETE FROM t WHERE expired = 1 RETURNING id, name;
```

### 事务与保存点

```sql
BEGIN;                          -- 或 BEGIN TRANSACTION
SAVEPOINT sp1;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
SAVEPOINT sp2;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
ROLLBACK TO SAVEPOINT sp2;     -- 仅回滚 sp2
RELEASE SAVEPOINT sp1;         -- 提交 sp1
COMMIT;                        -- 或 END / END TRANSACTION
ROLLBACK;                      -- 或 ABORT
```

### 查询特性

#### WHERE 操作符

| 操作符 | 示例 |
|--------|------|
| `=`, `!=`, `<`, `>`, `<=`, `>=` | `WHERE age > 25` |
| `IN (...)` | `WHERE id IN (1, 2, 3)` |
| `NOT IN (...)` | `WHERE id NOT IN (4, 5)` |
| `IN (SELECT ...)` | `WHERE id IN (SELECT uid FROM orders)` |
| `BETWEEN ... AND` | `WHERE age BETWEEN 18 AND 65` |
| `NOT BETWEEN ... AND` | `WHERE age NOT BETWEEN 0 AND 17` |
| `LIKE` / `NOT LIKE` | `WHERE name LIKE 'A%'` |
| `LIKE ... ESCAPE` | `WHERE code LIKE '10\%' ESCAPE '\'` |
| `GLOB` / `NOT GLOB` | `WHERE file GLOB '*.rs'` |
| `REGEXP` / `NOT REGEXP` | `WHERE email REGEXP '^[a-z]+@'` |
| `IS NULL` / `IS NOT NULL` | `WHERE email IS NOT NULL` |
| `EXISTS (SELECT ...)` | `WHERE EXISTS (SELECT 1 FROM orders WHERE uid = u.id)` |
| `NOT EXISTS (SELECT ...)` | `WHERE NOT EXISTS (...)` |
| `AND`, `OR`, `()` | `WHERE (a = 1 OR b = 2) AND c = 3` |

#### JOIN

```sql
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 CROSS JOIN t2;               -- 笛卡尔积
SELECT * FROM t1 NATURAL JOIN t2;              -- 自动匹配同名列
SELECT * FROM t1 JOIN t2 AS b ON t1.id = b.fk; -- 表别名

-- 多表链式 JOIN
SELECT * FROM t1
    JOIN t2 ON t1.id = t2.fk1
    JOIN t3 ON t2.id = t3.fk2;
```

#### 聚合函数

```sql
-- 标准聚合
SELECT COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col) FROM t;

-- 字符串聚合
SELECT GROUP_CONCAT(name, ', ') FROM t GROUP BY dept;
SELECT STRING_AGG(name, '; ') FROM t GROUP BY dept;      -- PostgreSQL 别名

-- 统计聚合
SELECT STDDEV(salary), VARIANCE(salary) FROM employees;

-- 数组 / JSON 聚合
SELECT ARRAY_AGG(name) FROM employees GROUP BY dept;
SELECT JSON_ARRAYAGG(name) FROM employees GROUP BY dept;
SELECT JSON_OBJECTAGG(key_col, val_col) FROM config;

-- 布尔聚合
SELECT BOOL_AND(active), BOOL_OR(flagged) FROM users;

-- 百分位
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM employees;  -- 中位数
SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY response_time) FROM logs; -- P90

-- GROUP BY / HAVING
SELECT dept, COUNT(*) AS cnt FROM t GROUP BY dept HAVING cnt > 5;
```

#### 窗口函数

```sql
SELECT name, salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
    RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rnk,
    LAG(salary, 1, 0) OVER (ORDER BY salary) AS prev_salary,
    LEAD(salary, 1, 0) OVER (ORDER BY salary) AS next_salary,
    NTILE(4) OVER (ORDER BY salary) AS quartile,
    SUM(salary) OVER (PARTITION BY dept) AS dept_total,
    AVG(salary) OVER (PARTITION BY dept) AS dept_avg,
    COUNT(*) OVER (PARTITION BY dept) AS dept_count,
    MIN(salary) OVER (PARTITION BY dept) AS dept_min,
    MAX(salary) OVER (PARTITION BY dept) AS dept_max
FROM employees;
```

#### CTE（公共表表达式）

```sql
WITH
    top_users AS (SELECT * FROM users WHERE score > 90),
    user_orders AS (SELECT uid, COUNT(*) AS cnt FROM orders GROUP BY uid)
SELECT t.name, o.cnt
FROM top_users t
JOIN user_orders o ON t.id = o.uid;
```

#### 集合操作

```sql
SELECT name FROM customers UNION ALL SELECT name FROM suppliers;
SELECT name FROM customers UNION SELECT name FROM suppliers;        -- 去重
SELECT id FROM t1 INTERSECT SELECT id FROM t2;
SELECT id FROM t1 EXCEPT SELECT id FROM t2;
```

#### 子查询

```sql
SELECT * FROM t WHERE id IN (SELECT user_id FROM orders WHERE total > 1000);
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM orders WHERE orders.uid = t.id);
```

#### ORDER BY / LIMIT / OFFSET

```sql
SELECT * FROM t ORDER BY score DESC, name ASC;
SELECT * FROM t ORDER BY score DESC NULLS LAST;
SELECT * FROM t ORDER BY score ASC NULLS FIRST;
SELECT * FROM t ORDER BY score DESC LIMIT 10 OFFSET 20;
```

### 内置函数

#### 字符串函数

| 函数 | 别名 | 说明 |
|------|------|------|
| `UPPER(x)` | `UCASE` | 转大写 |
| `LOWER(x)` | `LCASE` | 转小写 |
| `LENGTH(x)` | `LEN` | 字符串长度 |
| `SUBSTR(x, start, len)` | `SUBSTRING` | 截取子串 |
| `TRIM(x)` | — | 去除首尾空白 |
| `LTRIM(x)` / `RTRIM(x)` | — | 去除左/右空白 |
| `REPLACE(x, from, to)` | — | 替换 |
| `CONCAT(a, b, ...)` | — | 拼接 |
| `LEFT(x, n)` / `RIGHT(x, n)` | — | 前/后 n 个字符 |
| `REVERSE(x)` | — | 反转 |
| `LPAD(x, len, pad)` / `RPAD(x, len, pad)` | — | 左/右填充 |
| `INSTR(x, sub)` | — | 查找位置（1 起） |
| `CHARINDEX(sub, x)` | — | 查找位置（SQL Server 兼容） |
| `CHAR(n)` / `ASCII(x)` | — | Unicode 码点转换 |
| `REGEXP_REPLACE(str, pat, rep)` | — | 使用正则表达式替换 |
| `REGEXP_LIKE(str, pat)` | — | 正则匹配返回布尔值 |
| `SPLIT_PART(str, de, idx)` | — | 分割并返回第 N 个部分（1 起） |
| `REPEAT(str, n)` | — | 重复字符串 N 次 |
| `TRANSLATE(str, from, to)` | — | 逐字符替换 |

#### 数学函数

| 函数 | 别名 | 说明 |
|------|------|------|
| `ABS(x)` | — | 绝对值 |
| `ROUND(x, n)` | — | 四舍五入到 n 位 |
| `CEIL(x)` | `CEILING` | 向上取整 |
| `FLOOR(x)` | — | 向下取整 |
| `TRUNCATE(x, n)` | `TRUNC` | 截断到 n 位 |
| `MOD(x, y)` | — | 取模 |
| `POWER(x, y)` | `POW` | 幂运算 |
| `SQRT(x)` | — | 平方根 |
| `SIGN(x)` | — | 符号（-1, 0, 1） |
| `EXP(x)` | — | e^x |
| `LOG(x)` | `LN` | 自然对数 |
| `LOG10(x)` | — | 以 10 为底对数 |
| `PI()` | — | π 常量 |
| `RAND()` | `RANDOM` | 随机浮点 [0, 1) |

#### 日期时间函数

| 函数 | 别名 | 说明 |
|------|------|------|
| `NOW()` | `GETDATE`, `CURRENT_TIMESTAMP` | 当前时间戳（毫秒） |
| `YEAR(ts)` / `MONTH(ts)` / `DAY(ts)` | `DAYOFMONTH` | 提取年/月/日 |
| `HOUR(ts)` / `MINUTE(ts)` / `SECOND(ts)` | — | 提取时/分/秒 |
| `QUARTER(ts)` / `WEEK(ts)` | — | 提取季度/ISO 周 |
| `WEEKDAY(ts)` / `DAYOFWEEK(ts)` | — | 星期几（0=周一 / 1=周日） |
| `LAST_DAY(ts)` | — | 月末日 |
| `DATEPART(unit, ts)` | — | 按单位名提取 |
| `DATEDIFF(unit, a, b)` | — | 日期差 |
| `DATEADD(unit, n, ts)` | — | 日期加 |
| `DATE_ADD(ts, n)` / `DATE_SUB(ts, n)` | — | 加/减天数（MySQL 兼容） |
| `DATE_FORMAT(ts, fmt)` | — | 格式化输出 |
| `TIME_BUCKET(interval, ts)` | — | 时间桶截断（TimescaleDB 兼容） |
| `TIMESTAMPDIFF(unit, a, b)` | — | 时间差（MySQL 兼容） |
| `TIMESTAMPADD(unit, n, ts)` | — | 时间加（MySQL 兼容） |
| `DATE_TRUNC(unit, ts)` | — | 时间精度截断（PostgreSQL 兼容） |

#### JSON 函数

| 函数 | 说明 |
|------|------|
| `JSON_EXTRACT(doc, path)` | 按路径提取值 |
| `JSON_EXTRACT_TEXT(doc, path)` | 按路径提取文本 |
| `JSON_SET(doc, path, val)` | 设置路径值 |
| `JSON_REMOVE(doc, path)` | 删除键 |
| `JSON_TYPE(doc)` | 返回 JSON 类型名 |
| `JSON_ARRAY_LENGTH(doc)` | 数组元素数 |
| `JSON_KEYS(doc)` | 对象键列表 |
| `JSON_VALID(doc)` | 验证 JSON 语法 |
| `JSON_CONTAINS(doc, val)` | 包含检查 |

**JSONB 箭头操作符：**
```sql
SELECT data->>'name' FROM users WHERE data->>'age' > '25';
```

#### 哈希函数

`MD5(x)`、`SHA1(x)`、`SHA2(x, bits)`

#### 条件函数

| 函数 | 别名 | 说明 |
|------|------|------|
| `COALESCE(a, b, ...)` | — | 第一个非 NULL 值 |
| `IFNULL(x, default)` | `ISNULL` | 替换 NULL |
| `NULLIF(a, b)` | — | 相等返回 NULL |
| `IF(cond, then, else)` | `IIF` | 条件表达式 |
| `CAST(x AS type)` | — | 类型转换 |
| `CONVERT(x, type)` | — | 类型转换（SQL Server 兼容） |

**CASE 表达式：**
```sql
SELECT CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 80 THEN 'B'
    ELSE 'C'
END AS grade FROM students;
```

#### 系统函数

`DATABASE()`、`VERSION()`、`USER()` / `CURRENT_USER()`、`CONNECTION_ID()`

### SQL 内向量搜索

```sql
SELECT id, vec_cosine(emb, '[0.1, 0.2, ...]') AS score FROM docs ORDER BY score LIMIT 10;
SELECT id, vec_l2(emb, '[0.1, 0.2, ...]') AS dist FROM docs ORDER BY dist LIMIT 10;
SELECT id, vec_dot(emb, '[0.1, 0.2, ...]') AS sim FROM docs ORDER BY sim DESC LIMIT 10;

-- 混合查询：标量过滤 + 向量 KNN
SELECT id, vec_cosine(emb, '[0.1, ...]') AS score
FROM docs WHERE category = 'tech' ORDER BY score LIMIT 10;
```

### SQL 内地理搜索

```sql
SELECT id, ST_DISTANCE(location, GEOPOINT(39.9, 116.4)) AS dist_m FROM places ORDER BY dist_m LIMIT 10;
SELECT * FROM places WHERE ST_WITHIN(location, 39.9, 116.4, 1000);  -- 1km 范围内
```

### 数据类型

| 类型 | 说明 | 示例 |
|------|------|------|
| `INTEGER` | 64 位有符号整数 | `42` |
| `FLOAT` | 64 位 IEEE 754 | `3.14` |
| `TEXT` | UTF-8 字符串 | `'hello'` |
| `BOOLEAN` | 布尔值 | `TRUE` |
| `BLOB` | 二进制数据 | `X'DEADBEEF'` |
| `TIMESTAMP` | ISO 8601 时间（毫秒精度） | `'2024-01-01T00:00:00Z'` |
| `DATE` | 32 位整数（Unix 纪元天数） | `'2024-01-01'` |
| `TIME` | 64 位整数（当天时间） | `'14:30:00'` |
| `JSON` / `JSONB` | JSON 文档 | `'{"key": "value"}'` |
| `GEOPOINT` | 经纬度坐标 | `GEOPOINT(39.9, 116.4)` |
| `VECTOR(N)` | N 维浮点向量 | — |
| `NULL` | 空值 | `NULL` |

### SQLite / PostgreSQL / MySQL 兼容性

| 特性 | SQLite | PostgreSQL | MySQL | Talon |
|------|--------|------------|-------|-------|
| `INSERT OR REPLACE` | ✅ | ❌ | ❌ | ✅ |
| `INSERT OR IGNORE` | ✅ | ❌ | ❌ | ✅ |
| `ON CONFLICT DO UPDATE` | ❌ | ✅ | ❌ | ✅ |
| `RETURNING` | ✅ 3.35+ | ✅ | ❌ | ✅ |
| `DISTINCT ON` | ❌ | ✅ | ❌ | ✅ |
| `DELETE ... USING` | ❌ | ✅ | ❌ | ✅ |
| `UPDATE ... FROM` | ❌ | ✅ | ❌ | ✅ |
| `UPDATE ... ORDER BY LIMIT` | ❌ | ❌ | ✅ | ✅ |
| `$1` 参数语法 | ❌ | ✅ | ❌ | ✅（自动转换） |
| `IFNULL` / `ISNULL` | ✅ | ❌ | ✅ | ✅ |
| 窗口函数 | ✅ | ✅ | ✅ 8.0+ | ✅ |
| CTE（`WITH ... AS`） | ✅ | ✅ | ✅ 8.0+ | ✅ |
| `SAVEPOINT` | ✅ | ✅ | ✅ | ✅ |
| 外键 | ✅ | ✅ | ✅ | ✅ |
| 视图 | ✅ | ✅ | ✅ | ✅ |
| 临时表 | ✅ | ✅ | ✅ | ✅ |
| `GLOB` | ✅ | ❌ | ❌ | ✅ |
| `REGEXP` | ❌ ext | ✅ | ✅ | ✅ |
| `JSON_EXTRACT` | ✅ | ❌ (`->`) | ✅ | ✅ |
| `JSONB ->>` | ❌ | ✅ | ✅ | ✅ |
| `TIME_BUCKET` | ❌ | ✅ ext | ❌ | ✅ |
| 向量索引（HNSW） | ❌ | ❌ ext | ❌ | ✅ 原生 |
| 地理（`ST_DISTANCE`） | ❌ | ✅ ext | ❌ | ✅ 原生 |

## 性能

| 基准测试 | 结果 |
|----------|------|
| 单条 INSERT（71 列） | 46,667 QPS |
| 批量 INSERT | 241,697 行/秒 |
| JOIN（100K × 1K） | P95 8.6ms |
| 聚合（1M 行） | < 1ms |
| COUNT(*) 无 WHERE | O(1) 统计缓存 |

## 错误处理

所有 SQL 操作返回 `Result<_, talon::Error>`。常见错误类型：

- `Error::SqlParse` — SQL 语法错误
- `Error::SqlExec` — 运行时执行错误（如约束违反、列不存在）
- `Error::ReadOnly` — Replica 节点上的写操作
