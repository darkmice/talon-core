# SQL Engine

Full relational database engine with rich SQL dialect, PostgreSQL/MySQL/SQLite compatibility, and high-performance batch operations.

## Overview

The SQL Engine provides a complete relational database with ACID transactions (MVCC snapshot isolation), secondary indexes, and advanced query capabilities including window functions, CTEs, multi-table JOINs, views, savepoints, foreign keys, and parameterized queries.

## Quick Start

```rust
use talon::Talon;

let db = Talon::open("./data")?;

db.run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
db.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")?;
let rows = db.run_sql("SELECT * FROM users WHERE age > 25")?;
```

## API Reference

### `Talon::run_sql`

```rust
pub fn run_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>, Error>
```

### `Talon::run_sql_param`

Parameterized queries with `?` or `$1` placeholders.

```rust
pub fn run_sql_param(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, Error>
```

### `Talon::run_sql_batch`

Multiple statements, single lock acquisition.

```rust
pub fn run_sql_batch(&self, sqls: &[&str]) -> Result<Vec<Result<Vec<Vec<Value>>, Error>>, Error>
```

### `Talon::batch_insert_rows`

High-performance native batch insert — bypasses SQL parsing.

```rust
pub fn batch_insert_rows(&self, table: &str, columns: &[&str], rows: Vec<Vec<Value>>) -> Result<(), Error>
```

Performance: **241,697 rows/s**.

### `Talon::import_sql` / `import_sql_file`

Import from SQL dump stream (supports SQLite `.dump` format).

```rust
pub fn import_sql(&self, reader: impl std::io::BufRead) -> Result<SqlImportStats, Error>
pub fn import_sql_file(&self, path: impl AsRef<Path>) -> Result<SqlImportStats, Error>
```

## Complete SQL Dialect Reference

### Parser & Dialect Core

Talon's SQL parser is built on top of `sqlparser-rs`, and its execution engine integrates expressions and functions ported from `apache/datafusion`. This foundation provides robust support for a wide range of standard and advanced SQL syntaxes, including:

- **Complex Queries**: Common Table Expressions (CTEs), nested subqueries, and derived tables.
- **Advanced Joins**: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`, and `NATURAL` joins.
- **Analytical Features**: Window functions (`OVER`, `PARTITION BY`, `ORDER BY`) and rich aggregations.
- **Set Operations**: `UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT`.
- **Expressions**: Robust support for `CASE` statements, `CAST`/`CONVERT`, arithmetic operations, and complex logical conditions.

### DDL (Data Definition Language)

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
    UNIQUE(name, email),                        -- composite unique constraint
    CHECK(score >= 0 AND score <= 100),          -- check constraint
    FOREIGN KEY (dept_id) REFERENCES depts(id)   -- foreign key
);

CREATE TABLE IF NOT EXISTS t (...);  -- silent skip if exists
CREATE TEMP TABLE scratch (id INTEGER, val TEXT);  -- session-scoped temporary table
```

**Column constraints:** `PRIMARY KEY`, `AUTOINCREMENT`, `NOT NULL`, `DEFAULT value`, `UNIQUE`, `CHECK(expr)`, `REFERENCES parent(col)`.

**Table constraints:** `UNIQUE(col1, col2)`, `CHECK(expr)`, `FOREIGN KEY (col) REFERENCES parent(col)`.

#### ALTER TABLE

```sql
ALTER TABLE t ADD COLUMN email TEXT;
ALTER TABLE t ADD COLUMN email TEXT DEFAULT 'none';
ALTER TABLE t DROP COLUMN email;
ALTER TABLE t RENAME COLUMN name TO username;
ALTER TABLE t RENAME TO new_table_name;
ALTER TABLE t ALTER COLUMN score TYPE INTEGER;          -- change column type
ALTER TABLE t ALTER COLUMN name SET DEFAULT 'unknown';  -- set default
ALTER TABLE t ALTER COLUMN name DROP DEFAULT;           -- remove default
```

#### DROP / TRUNCATE

```sql
DROP TABLE t;
DROP TABLE IF EXISTS t;
TRUNCATE TABLE t;   -- fast data wipe, keeps schema
```

#### Indexes

```sql
CREATE INDEX idx_name ON t(name);
CREATE INDEX idx_comp ON t(name, age);    -- composite index
CREATE UNIQUE INDEX idx_email ON t(email);
DROP INDEX idx_name;
DROP INDEX IF EXISTS idx_name;

-- Vector index (HNSW)
CREATE VECTOR INDEX emb_idx ON t(embedding) USING HNSW
    WITH (metric='cosine', m=16, ef_construction=200);
DROP VECTOR INDEX IF EXISTS emb_idx;
```

#### Views

```sql
CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;
CREATE VIEW IF NOT EXISTS v AS SELECT ...;
DROP VIEW active_users;
DROP VIEW IF EXISTS active_users;
```

#### Comments

```sql
COMMENT ON TABLE users IS 'Main user table';
COMMENT ON COLUMN users.name IS 'Full name';
```

#### Metadata Queries

```sql
SHOW TABLES;
SHOW INDEXES;
SHOW INDEXES ON users;
DESCRIBE users;
EXPLAIN SELECT * FROM users WHERE age > 25;
```

### DML (Data Manipulation Language)

#### INSERT

```sql
INSERT INTO t (id, name) VALUES (1, 'Alice');
INSERT INTO t VALUES (1, 'Alice', 30);

-- Multiple rows
INSERT INTO t (name, age) VALUES ('Alice', 30), ('Bob', 25);

-- Upsert: conflict → update
INSERT INTO t VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

-- Replace: conflict → full overwrite
INSERT OR REPLACE INTO t VALUES (1, 'Alice', 30);

-- Ignore: conflict → silent skip
INSERT OR IGNORE INTO t VALUES (1, 'Alice', 30);

-- INSERT ... SELECT
INSERT INTO archive SELECT * FROM logs WHERE created_at < '2024-01-01';

-- RETURNING (PostgreSQL compatible)
INSERT INTO t (name) VALUES ('Bob') RETURNING id, name;
```

#### UPDATE

```sql
UPDATE t SET name = 'Charlie' WHERE id = 1;

-- Arithmetic assignment
UPDATE t SET score = score + 10 WHERE score < 50;
UPDATE t SET price = price * 1.1;

-- Cross-table update (PostgreSQL FROM syntax)
UPDATE t SET name = src.name FROM source_table AS src WHERE t.id = src.id;

-- ORDER BY + LIMIT (update top N)
UPDATE t SET status = 'reviewed' WHERE status = 'pending' ORDER BY created_at LIMIT 100;

-- RETURNING
UPDATE t SET score = score + 1 WHERE id = 1 RETURNING id, score;
```

#### DELETE

```sql
DELETE FROM t WHERE id = 1;

-- Multi-table delete (PostgreSQL USING syntax)
DELETE FROM t1 USING t2 WHERE t1.id = t2.id AND t2.active = 0;

-- RETURNING
DELETE FROM t WHERE expired = 1 RETURNING id, name;
```

### Transactions & Savepoints

```sql
BEGIN;                          -- or BEGIN TRANSACTION
SAVEPOINT sp1;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
SAVEPOINT sp2;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
ROLLBACK TO SAVEPOINT sp2;     -- undo only sp2
RELEASE SAVEPOINT sp1;         -- commit sp1
COMMIT;                        -- or END / END TRANSACTION
ROLLBACK;                      -- or ABORT
```

### Query Features

#### WHERE Operators

| Operator | Example |
|----------|---------|
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

#### JOINs

```sql
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.fk;
SELECT * FROM t1 CROSS JOIN t2;               -- cartesian product
SELECT * FROM t1 NATURAL JOIN t2;              -- auto-match same-name columns
SELECT * FROM t1 JOIN t2 AS b ON t1.id = b.fk; -- table alias

-- Multi-table chain JOINs
SELECT * FROM t1
    JOIN t2 ON t1.id = t2.fk1
    JOIN t3 ON t2.id = t3.fk2;
```

#### Aggregation Functions

```sql
-- Standard
SELECT COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col) FROM t;

-- String aggregation
SELECT GROUP_CONCAT(name, ', ') FROM t GROUP BY dept;
SELECT STRING_AGG(name, '; ') FROM t GROUP BY dept;      -- PostgreSQL alias

-- Statistical
SELECT STDDEV(salary), VARIANCE(salary) FROM employees;

-- Array / JSON aggregation
SELECT ARRAY_AGG(name) FROM employees GROUP BY dept;
SELECT JSON_ARRAYAGG(name) FROM employees GROUP BY dept;
SELECT JSON_OBJECTAGG(key_col, val_col) FROM config;

-- Boolean aggregation
SELECT BOOL_AND(active), BOOL_OR(flagged) FROM users;

-- Percentile
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM employees;  -- median
SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY response_time) FROM logs; -- P90

-- GROUP BY / HAVING
SELECT dept, COUNT(*) AS cnt FROM t GROUP BY dept HAVING cnt > 5;
```

#### Window Functions

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

#### CTE (Common Table Expressions)

```sql
WITH
    top_users AS (SELECT * FROM users WHERE score > 90),
    user_orders AS (SELECT uid, COUNT(*) AS cnt FROM orders GROUP BY uid)
SELECT t.name, o.cnt
FROM top_users t
JOIN user_orders o ON t.id = o.uid;
```

#### Set Operations

```sql
SELECT name FROM customers UNION ALL SELECT name FROM suppliers;
SELECT name FROM customers UNION SELECT name FROM suppliers;        -- deduplicated
SELECT id FROM t1 INTERSECT SELECT id FROM t2;
SELECT id FROM t1 EXCEPT SELECT id FROM t2;
```

#### Subqueries

```sql
SELECT * FROM t WHERE id IN (SELECT user_id FROM orders WHERE total > 1000);
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM orders WHERE orders.uid = t.id);
SELECT * FROM t WHERE salary > (SELECT AVG(salary) FROM t);
```

#### ORDER BY / LIMIT / OFFSET

```sql
SELECT * FROM t ORDER BY score DESC, name ASC;
SELECT * FROM t ORDER BY score DESC NULLS LAST;
SELECT * FROM t ORDER BY score ASC NULLS FIRST;
SELECT * FROM t ORDER BY score DESC LIMIT 10 OFFSET 20;
```

#### DISTINCT / DISTINCT ON

```sql
SELECT DISTINCT dept FROM employees;
SELECT DISTINCT ON (dept) dept, name, salary FROM employees ORDER BY dept, salary DESC;
```

### Built-in Functions

#### String Functions

| Function | Aliases | Description |
|----------|---------|-------------|
| `UPPER(x)` | `UCASE` | Convert to uppercase |
| `LOWER(x)` | `LCASE` | Convert to lowercase |
| `LENGTH(x)` | `LEN` | String length |
| `SUBSTR(x, start, len)` | `SUBSTRING` | Extract substring |
| `TRIM(x)` | — | Remove leading/trailing whitespace |
| `LTRIM(x)` | — | Remove leading whitespace |
| `RTRIM(x)` | — | Remove trailing whitespace |
| `REPLACE(x, from, to)` | — | Replace occurrences |
| `CONCAT(a, b, ...)` | — | Concatenate strings |
| `LEFT(x, n)` | — | First n characters |
| `RIGHT(x, n)` | — | Last n characters |
| `REVERSE(x)` | — | Reverse string |
| `LPAD(x, len, pad)` | — | Left-pad to length |
| `RPAD(x, len, pad)` | — | Right-pad to length |
| `INSTR(x, substr)` | — | Find substring position (1-based) |
| `CHARINDEX(substr, x)` | — | Find position (SQL Server compat) |
| `CHAR(n)` | — | Unicode codepoint to character |
| `ASCII(x)` | — | First character to codepoint |
| `REGEXP_REPLACE(str, pat, rep)` | — | Replace by regular expression |
| `REGEXP_LIKE(str, pat)` | — | Regex match boolean |
| `SPLIT_PART(str, delim, idx)` | — | Split and return nth part (1-based) |
| `REPEAT(str, n)` | — | Repeat string n times |
| `TRANSLATE(str, from, to)` | — | Character-by-character replacement |

#### Numeric Functions

| Function | Aliases | Description |
|----------|---------|-------------|
| `ABS(x)` | — | Absolute value |
| `ROUND(x, n)` | — | Round to n decimal places |
| `CEIL(x)` | `CEILING` | Round up |
| `FLOOR(x)` | — | Round down |
| `TRUNCATE(x, n)` | `TRUNC` | Truncate to n decimal places |
| `MOD(x, y)` | — | Modulo |
| `POWER(x, y)` | `POW` | Exponentiation |
| `SQRT(x)` | — | Square root |
| `SIGN(x)` | — | Sign (-1, 0, 1) |
| `EXP(x)` | — | e^x |
| `LOG(x)` | `LN` | Natural logarithm |
| `LOG10(x)` | — | Base-10 logarithm |
| `PI()` | — | π constant |
| `RAND()` | `RANDOM` | Random float [0, 1) |

#### Date/Time Functions

| Function | Aliases | Description |
|----------|---------|-------------|
| `NOW()` | `GETDATE`, `CURRENT_TIMESTAMP` | Current timestamp (ms) |
| `YEAR(ts)` | — | Extract year |
| `MONTH(ts)` | — | Extract month |
| `DAY(ts)` | `DAYOFMONTH` | Extract day |
| `HOUR(ts)` | — | Extract hour |
| `MINUTE(ts)` | — | Extract minute |
| `SECOND(ts)` | — | Extract second |
| `QUARTER(ts)` | — | Extract quarter (1-4) |
| `WEEK(ts)` | — | Extract ISO week number |
| `WEEKDAY(ts)` | — | Day of week (0=Mon) |
| `DAYOFWEEK(ts)` | — | Day of week (1=Sun) |
| `LAST_DAY(ts)` | — | Last day of month |
| `DATEPART(unit, ts)` | — | Extract part by unit name |
| `DATEDIFF(unit, a, b)` | — | Difference in units |
| `DATEADD(unit, n, ts)` | — | Add interval |
| `DATE_ADD(ts, n)` | — | Add days (MySQL compat) |
| `DATE_SUB(ts, n)` | — | Subtract days (MySQL compat) |
| `DATE_FORMAT(ts, fmt)` | — | Format to string |
| `TIME_BUCKET(interval, ts)` | — | Truncate to interval (TimescaleDB compat) |
| `TIMESTAMPDIFF(unit, a, b)` | — | Difference (MySQL compat) |
| `TIMESTAMPADD(unit, n, ts)` | — | Add interval (MySQL compat) |
| `DATE_TRUNC(unit, ts)` | — | Truncate to precision (PostgreSQL compat) |

#### JSON Functions

| Function | Description |
|----------|-------------|
| `JSON_EXTRACT(doc, path)` | Extract value by JSON path |
| `JSON_EXTRACT_TEXT(doc, path)` | Extract as text |
| `JSON_SET(doc, path, val)` | Set value at path |
| `JSON_REMOVE(doc, path)` | Remove key |
| `JSON_TYPE(doc)` | Return JSON type name |
| `JSON_ARRAY_LENGTH(doc)` | Array element count |
| `JSON_KEYS(doc)` | Object key list |
| `JSON_VALID(doc)` | Validate JSON syntax |
| `JSON_CONTAINS(doc, val)` | Check containment |

**JSONB arrow operator:**
```sql
SELECT data->>'name' FROM users WHERE data->>'age' > '25';
```

#### Hash Functions

| Function | Description |
|----------|-------------|
| `MD5(x)` | MD5 hex digest |
| `SHA1(x)` | SHA-1 hex digest |
| `SHA2(x, bits)` | SHA-2 (256/512) hex digest |

#### Conditional Functions

| Function | Aliases | Description |
|----------|---------|-------------|
| `COALESCE(a, b, ...)` | — | First non-NULL value |
| `IFNULL(x, default)` | `ISNULL` | Replace NULL |
| `NULLIF(a, b)` | — | NULL if a = b |
| `IF(cond, then, else)` | `IIF` | Conditional expression |
| `CAST(x AS type)` | — | Type conversion |
| `CONVERT(x, type)` | — | Type conversion (SQL Server compat) |

**CASE expression:**
```sql
SELECT CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 80 THEN 'B'
    ELSE 'C'
END AS grade FROM students;
```

#### System Functions

| Function | Description |
|----------|-------------|
| `DATABASE()` | Current database name (`talon`) |
| `VERSION()` | Talon version |
| `USER()` / `CURRENT_USER()` | Current user |
| `CONNECTION_ID()` | Connection ID |

### Vector Search in SQL

```sql
-- Vector distance functions
SELECT id, vec_cosine(emb, '[0.1, 0.2, ...]') AS score FROM docs ORDER BY score LIMIT 10;
SELECT id, vec_l2(emb, '[0.1, 0.2, ...]') AS dist FROM docs ORDER BY dist LIMIT 10;
SELECT id, vec_dot(emb, '[0.1, 0.2, ...]') AS sim FROM docs ORDER BY sim DESC LIMIT 10;

-- Hybrid query: scalar filter + vector KNN
SELECT id, vec_cosine(emb, '[0.1, ...]') AS score
FROM docs WHERE category = 'tech' ORDER BY score LIMIT 10;
```

### Geo Search in SQL

```sql
-- Distance calculation
SELECT id, ST_DISTANCE(location, GEOPOINT(39.9, 116.4)) AS dist_m FROM places ORDER BY dist_m LIMIT 10;

-- Within radius filter
SELECT * FROM places WHERE ST_WITHIN(location, 39.9, 116.4, 1000);  -- within 1km
```

### Data Types

| Type | Description | Example |
|------|-------------|---------|
| `INTEGER` | 64-bit signed integer | `42` |
| `FLOAT` | 64-bit IEEE 754 | `3.14` |
| `TEXT` | UTF-8 string | `'hello'` |
| `BOOLEAN` | true/false | `TRUE` |
| `BLOB` | Binary data | `X'DEADBEEF'` |
| `TIMESTAMP` | ISO 8601 datetime (ms precision) | `'2024-01-01T00:00:00Z'` |
| `DATE` | 32-bit integer (days since epoch) | `'2024-01-01'` |
| `TIME` | 64-bit integer (time of day) | `'14:30:00'` |
| `JSON` / `JSONB` | JSON document | `'{"key": "value"}'` |
| `GEOPOINT` | Lat/lng coordinate | `GEOPOINT(39.9, 116.4)` |
| `VECTOR(N)` | N-dimensional float vector | — |
| `NULL` | Null value | `NULL` |

### SQLite / PostgreSQL / MySQL Compatibility

| Feature | SQLite | PostgreSQL | MySQL | Talon |
|---------|--------|------------|-------|-------|
| `INSERT OR REPLACE` | ✅ | ❌ | ❌ | ✅ |
| `INSERT OR IGNORE` | ✅ | ❌ | ❌ | ✅ |
| `ON CONFLICT DO UPDATE` | ❌ | ✅ | ❌ | ✅ |
| `RETURNING` | ✅ 3.35+ | ✅ | ❌ | ✅ |
| `DISTINCT ON` | ❌ | ✅ | ❌ | ✅ |
| `DELETE ... USING` | ❌ | ✅ | ❌ | ✅ |
| `UPDATE ... FROM` | ❌ | ✅ | ❌ | ✅ |
| `UPDATE ... ORDER BY LIMIT` | ❌ | ❌ | ✅ | ✅ |
| `$1` param syntax | ❌ | ✅ | ❌ | ✅ (auto-convert) |
| `IFNULL` / `ISNULL` | ✅ | ❌ | ✅ | ✅ |
| Window functions | ✅ | ✅ | ✅ 8.0+ | ✅ |
| CTE (`WITH ... AS`) | ✅ | ✅ | ✅ 8.0+ | ✅ |
| `SAVEPOINT` | ✅ | ✅ | ✅ | ✅ |
| Foreign keys | ✅ | ✅ | ✅ | ✅ |
| Views | ✅ | ✅ | ✅ | ✅ |
| Temporary tables | ✅ | ✅ | ✅ | ✅ |
| `GLOB` | ✅ | ❌ | ❌ | ✅ |
| `REGEXP` | ❌ ext | ✅ | ✅ | ✅ |
| `JSON_EXTRACT` | ✅ | ❌ (`->`) | ✅ | ✅ |
| `JSONB ->>` | ❌ | ✅ | ✅ | ✅ |
| `TIME_BUCKET` | ❌ | ✅ ext | ❌ | ✅ |
| Vector index (HNSW) | ❌ | ❌ ext | ❌ | ✅ native |
| Geo (`ST_DISTANCE`) | ❌ | ✅ ext | ❌ | ✅ native |

## Performance

| Benchmark | Result |
|-----------|--------|
| Single INSERT (71 cols) | 46,667 QPS |
| Batch INSERT | 241,697 rows/s |
| JOIN (100K × 1K) | P95 8.6ms |
| Aggregate (1M rows) | < 1ms |
| COUNT(*) (no WHERE) | O(1) via stats |

## Error Handling

All SQL operations return `Result<_, talon::Error>`. Common error variants:

- `Error::SqlParse` — SQL syntax error
- `Error::SqlExec` — Runtime execution error (e.g., constraint violation, column not found)
- `Error::ReadOnly` — Write operation on Replica node
