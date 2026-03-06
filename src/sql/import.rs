/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL dump 导入器：流式解析 SQLite `.dump` 格式，批量写入。
//!
//! 支持：CREATE TABLE / INSERT INTO / CREATE INDEX / BEGIN / COMMIT。
//! 不支持的语句（TRIGGER、VIEW 等）跳过并计数。
//! INSERT 自动按表分桶，每 BATCH_SIZE 行调用 `batch_insert_rows`。

use super::engine::SqlEngine;
use super::parser::parse_row_single_pass;
use crate::types::Value;
use crate::Error;
use std::collections::HashMap;
use std::io::BufRead;

/// INSERT 解析结果：(表名, 列名列表, 值行列表)。
type InsertParseResult = (String, Vec<String>, Vec<Vec<Value>>);

/// 每批提交的行数上限。
const BATCH_SIZE: usize = 1000;
/// 单条 SQL 语句最大长度（16 MB）。
const MAX_STMT_LEN: usize = 16 * 1024 * 1024;

/// SQL 导入结果统计。
#[derive(Debug, Default)]
pub struct SqlImportStats {
    /// 成功创建的表数量。
    pub tables_created: u64,
    /// 成功插入的行数量。
    pub rows_inserted: u64,
    /// 成功创建的索引数量。
    pub indexes_created: u64,
    /// 跳过的语句数量（不支持或解析失败）。
    pub statements_skipped: u64,
    /// 跳过的语句详情（最多保留前 50 条）。
    pub errors: Vec<String>,
}

impl SqlImportStats {
    fn record_skip(&mut self, reason: String) {
        self.statements_skipped += 1;
        if self.errors.len() < 50 {
            self.errors.push(reason);
        }
    }
}

/// INSERT 行缓冲区：按表名分桶，积满 BATCH_SIZE 后批量提交。
struct InsertBuffer {
    /// table → (columns, rows)
    buckets: HashMap<String, (Vec<String>, Vec<Vec<Value>>)>,
}

impl InsertBuffer {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    /// 添加一批行到指定表的桶中，返回该桶是否已满需要刷出。
    fn push(&mut self, table: &str, columns: &[String], rows: Vec<Vec<Value>>) -> bool {
        let entry = self
            .buckets
            .entry(table.to_string())
            .or_insert_with(|| (columns.to_vec(), Vec::with_capacity(BATCH_SIZE)));
        entry.1.extend(rows);
        entry.1.len() >= BATCH_SIZE
    }

    /// 刷出指定表的桶，返回 (columns, rows)。
    fn take(&mut self, table: &str) -> Option<(Vec<String>, Vec<Vec<Value>>)> {
        self.buckets.remove(table)
    }

    /// 刷出所有剩余桶。
    fn drain_all(&mut self) -> Vec<(String, Vec<String>, Vec<Vec<Value>>)> {
        let mut result = Vec::new();
        for (table, (cols, rows)) in self.buckets.drain() {
            if !rows.is_empty() {
                result.push((table, cols, rows));
            }
        }
        result
    }
}

impl SqlEngine {
    /// 从 SQL dump 流导入数据。
    ///
    /// 逐行读取，拼接完整语句（以 `;` 结尾），按类型分派处理。
    /// INSERT 自动批量化，每 1000 行提交一次。
    pub fn import_sql_stream(&mut self, reader: impl BufRead) -> Result<SqlImportStats, Error> {
        let mut stats = SqlImportStats::default();
        let mut buf = InsertBuffer::new();
        let mut stmt_buf = String::new();
        let mut in_quote = false;
        let mut first_line = true;

        for line_result in reader.lines() {
            let line = line_result?;
            let line = if first_line {
                first_line = false;
                // 跳过 UTF-8 BOM
                line.strip_prefix('\u{feff}').unwrap_or(&line).to_string()
            } else {
                line
            };

            // 跳过空行、注释、SQLite 命令
            let trimmed = line.trim();
            if stmt_buf.is_empty()
                && (trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with('.'))
            {
                continue;
            }

            // 拼接多行语句
            if !stmt_buf.is_empty() {
                stmt_buf.push(' ');
            }
            stmt_buf.push_str(trimmed);

            // 长度保护
            if stmt_buf.len() > MAX_STMT_LEN {
                stats.record_skip(format!(
                    "语句超过 {}MB 上限，已跳过",
                    MAX_STMT_LEN / 1024 / 1024
                ));
                stmt_buf.clear();
                in_quote = false;
                continue;
            }

            // 跟踪引号状态，判断 `;` 是否在字符串外
            for ch in trimmed.chars() {
                if ch == '\'' {
                    in_quote = !in_quote;
                }
            }

            // 语句尚未结束（没有以 `;` 结尾或仍在引号内）
            if in_quote || !stmt_buf.trim_end().ends_with(';') {
                continue;
            }

            // 完整语句就绪，去掉末尾分号
            let stmt = stmt_buf.trim().trim_end_matches(';').trim().to_string();
            stmt_buf.clear();
            in_quote = false;

            if stmt.is_empty() {
                continue;
            }

            // 分类处理
            let safe_30 = safe_prefix(&stmt, 30);
            let upper = stmt[..safe_30].to_uppercase();

            if upper.starts_with("CREATE TABLE") {
                match self.run_sql(&stmt) {
                    Ok(_) => stats.tables_created += 1,
                    Err(e) => stats.record_skip(format!("CREATE TABLE 失败: {}", e)),
                }
            } else if upper.starts_with("INSERT OR IGNORE") {
                // OR IGNORE 需要保留 ignore 语义，走完整 SQL 路径
                match self.run_sql(&stmt) {
                    Ok(rows) => stats.rows_inserted += rows.len().max(1) as u64,
                    Err(e) => stats.record_skip(format!("INSERT OR IGNORE 失败: {}", e)),
                }
            } else if upper.starts_with("INSERT INTO")
                || upper.starts_with("INSERT OR REPLACE")
                || upper.starts_with("REPLACE INTO")
            {
                match self.parse_insert_for_import(&stmt) {
                    Ok((table, columns, rows)) => {
                        let needs_flush = buf.push(&table, &columns, rows);
                        // 刷出满桶（只检查刚写入的表）
                        if needs_flush {
                            if let Some((cols, flush_rows)) = buf.take(&table) {
                                let n = flush_rows.len() as u64;
                                let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
                                match self.batch_insert_rows(&table, &col_refs, flush_rows) {
                                    Ok(()) => stats.rows_inserted += n,
                                    Err(e) => stats
                                        .record_skip(format!("批量 INSERT {} 失败: {}", table, e)),
                                }
                            }
                        }
                    }
                    Err(e) => stats.record_skip(format!("INSERT 解析失败: {}", e)),
                }
            } else if upper.starts_with("CREATE INDEX") || upper.starts_with("CREATE UNIQUE INDEX")
            {
                // 将 UNIQUE INDEX 转为普通 INDEX（Talon 不支持 UNIQUE 约束）
                let normalized = if upper.starts_with("CREATE UNIQUE") {
                    stmt.replacen("UNIQUE ", "", 1)
                        .replacen("unique ", "", 1)
                        .replacen("Unique ", "", 1)
                } else {
                    stmt.clone()
                };
                match self.run_sql(&normalized) {
                    Ok(_) => stats.indexes_created += 1,
                    Err(e) => stats.record_skip(format!("CREATE INDEX 失败: {}", e)),
                }
            } else if upper.starts_with("BEGIN")
                || upper.starts_with("COMMIT")
                || upper.starts_with("END")
            {
                // SQLite dump 中的事务标记，忽略（我们用批量提交）
                continue;
            } else if upper.starts_with("DELETE FROM")
                || upper.starts_with("UPDATE ")
                || upper.starts_with("DROP ")
            {
                // 支持 DELETE/UPDATE/DROP 直通
                match self.run_sql(&stmt) {
                    Ok(_) => {}
                    Err(e) => stats.record_skip(format!("语句执行失败: {}", e)),
                }
            } else {
                // CREATE TRIGGER, CREATE VIEW 等不支持的语句
                let safe_80 = safe_prefix(&stmt, 80);
                stats.record_skip(format!("不支持的语句: {}", &stmt[..safe_80]));
            }
        }

        // 刷出所有剩余桶
        for (table, cols, rows) in buf.drain_all() {
            let n = rows.len() as u64;
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            match self.batch_insert_rows(&table, &col_refs, rows) {
                Ok(()) => stats.rows_inserted += n,
                Err(e) => stats.record_skip(format!("批量 INSERT {} 失败: {}", table, e)),
            }
        }

        Ok(stats)
    }

    /// 解析 INSERT 语句，提取表名、列名、值行。
    /// 不走完整 parser，直接快速提取以避免 parse 开销。
    fn parse_insert_for_import(&self, sql: &str) -> Result<InsertParseResult, Error> {
        let trimmed = sql.trim();
        // 只对前缀做大写匹配，避免对整条 INSERT 的 VALUES 做无效大写化
        let prefix_len = safe_prefix(trimmed, 50);
        let upper_prefix = trimmed[..prefix_len].to_uppercase();

        // 找到表名
        let table_start = if let Some(pos) = upper_prefix.find("INTO ") {
            pos + 5
        } else {
            return Err(Error::SqlParse("INSERT 缺少 INTO".into()));
        };

        let rest = &trimmed[table_start..];
        let rest_trimmed = rest.trim_start();

        // 提取表名（到空格或左括号）
        let table_end = rest_trimmed
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(rest_trimmed.len());
        let table = super::parser::unquote_ident(&rest_trimmed[..table_end]);

        let after_table = rest_trimmed[table_end..].trim_start();

        // 提取列名（如果有括号且在 VALUES 之前）
        let (columns, values_part) = if after_table.starts_with('(') {
            let after_upper = after_table.to_uppercase();
            // 检查这个括号是列名还是 VALUES
            if let Some(vals_pos) = after_upper.find("VALUES") {
                // 括号在 VALUES 之前 → 是列名
                let paren_end = after_table
                    .find(')')
                    .ok_or_else(|| Error::SqlParse("INSERT 列名括号未闭合".into()))?;
                let cols_str = &after_table[1..paren_end];
                let cols: Vec<String> = cols_str
                    .split(',')
                    .map(|c| super::parser::unquote_ident(c.trim()))
                    .collect();
                let vp = after_table[vals_pos + 6..].trim_start();
                (cols, vp)
            } else {
                (vec![], after_table)
            }
        } else {
            // 无括号，直接找 VALUES
            let after_upper = after_table.to_uppercase();
            if let Some(vals_pos) = after_upper.find("VALUES") {
                (vec![], after_table[vals_pos + 6..].trim_start())
            } else {
                return Err(Error::SqlParse("INSERT 缺少 VALUES".into()));
            }
        };

        // 解析 VALUES 行
        let rows = parse_values_rows(values_part)?;

        Ok((table, columns, rows))
    }
}

/// UTF-8 安全的前缀截取：返回不超过 max_bytes 的最大合法字节边界。
fn safe_prefix(s: &str, max_bytes: usize) -> usize {
    if s.len() <= max_bytes {
        return s.len();
    }
    // 找到 <= max_bytes 的最后一个 char boundary
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// 解析 `(v1, v2), (v3, v4)` 格式的多行 VALUES。
fn parse_values_rows(s: &str) -> Result<Vec<Vec<Value>>, Error> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(vec![]);
    }

    let mut rows = Vec::new();
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // 跳过空白和逗号
        while i < len
            && (bytes[i] == b' '
                || bytes[i] == b','
                || bytes[i] == b'\n'
                || bytes[i] == b'\r'
                || bytes[i] == b'\t')
        {
            i += 1;
        }
        if i >= len {
            break;
        }
        if bytes[i] != b'(' {
            break;
        }
        i += 1; // 跳过 '('

        // 找到配对的 ')'，考虑引号
        let start = i;
        let mut depth = 1i32;
        let mut in_q = false;
        while i < len && depth > 0 {
            if bytes[i] == b'\'' {
                if in_q && i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_q = !in_q;
            } else if !in_q {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                }
            }
            if depth > 0 {
                i += 1;
            }
        }
        let inner = &s[start..i];
        i += 1; // 跳过 ')'

        let row = parse_row_single_pass(inner)?;
        rows.push(row);
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;
    use std::io::BufReader;

    fn setup() -> (tempfile::TempDir, SqlEngine) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let engine = SqlEngine::new(&store).unwrap();
        (dir, engine)
    }

    #[test]
    fn import_sqlite_dump_basic() {
        let (_dir, mut engine) = setup();
        let dump = "\
-- SQLite dump
BEGIN TRANSACTION;
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
INSERT INTO users VALUES(1,'Alice',30);
INSERT INTO users VALUES(2,'Bob',25);
INSERT INTO users VALUES(3,'Carol',28);
COMMIT;
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 3);
        assert_eq!(stats.statements_skipped, 0, "errors: {:?}", stats.errors);

        let rows = engine.run_sql("SELECT * FROM users ORDER BY id").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][1], Value::Text("Alice".into()));
    }

    #[test]
    fn import_multi_row_values() {
        let (_dir, mut engine) = setup();
        let dump = "\
CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO items VALUES(1,'a'),(2,'b'),(3,'c');
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 3);
    }

    #[test]
    fn import_with_columns() {
        let (_dir, mut engine) = setup();
        let dump = "\
CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT);
INSERT INTO t (id, name, score) VALUES (1, 'x', 3.15);
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.rows_inserted, 1);

        let rows = engine.run_sql("SELECT score FROM t WHERE id = 1").unwrap();
        assert_eq!(rows[0][0], Value::Float(3.15));
    }

    #[test]
    fn import_skips_unsupported() {
        let (_dir, mut engine) = setup();
        let dump = "\
CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'a');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END;
CREATE VIEW v AS SELECT * FROM t;
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 1);
        assert_eq!(stats.statements_skipped, 2);
    }

    #[test]
    fn import_unique_index_normalized() {
        let (_dir, mut engine) = setup();
        let dump = "\
CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'a');
CREATE UNIQUE INDEX idx_name ON t(name);
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 1);
        assert_eq!(stats.indexes_created, 1);
    }

    #[test]
    fn import_or_replace() {
        let (_dir, mut engine) = setup();
        let dump = "\
CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
INSERT OR REPLACE INTO t VALUES(1,'first');
INSERT OR REPLACE INTO t VALUES(1,'second');
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 2);
    }

    #[test]
    fn import_batch_flush() {
        let (_dir, mut engine) = setup();
        // 生成超过 BATCH_SIZE 行触发自动 flush
        let mut dump = String::from("CREATE TABLE big (id INTEGER PRIMARY KEY, val TEXT);\n");
        for i in 0..1500 {
            dump.push_str(&format!("INSERT INTO big VALUES({}, 'row{}');\n", i, i));
        }
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 1500);

        let rows = engine.run_sql("SELECT COUNT(*) FROM big").unwrap();
        assert_eq!(rows[0][0], Value::Integer(1500));
    }

    #[test]
    fn import_sqlite_type_aliases() {
        let (_dir, mut engine) = setup();
        let dump = "\
CREATE TABLE t (id SMALLINT PRIMARY KEY, name NVARCHAR, score NUMERIC, data CLOB);
INSERT INTO t VALUES(1,'test',42,'blob_data');
";
        let reader = BufReader::new(dump.as_bytes());
        let stats = engine.import_sql_stream(reader).unwrap();
        assert_eq!(stats.tables_created, 1);
        assert_eq!(stats.rows_inserted, 1);
    }
}
