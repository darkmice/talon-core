/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 内置函数测试：字符串、空值处理、数学、类型转换。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

fn setup_users(eng: &mut SqlEngine) {
    eng.run_sql("CREATE TABLE users (id INT, name TEXT, email TEXT, score INT, price FLOAT)")
        .unwrap();
    eng.run_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com', 95, 19.99)")
        .unwrap();
    eng.run_sql("INSERT INTO users VALUES (2, 'Bob', NULL, 60, 9.50)")
        .unwrap();
    eng.run_sql("INSERT INTO users VALUES (3, '  Hello World  ', 'hw@test.com', -15, 0.0)")
        .unwrap();
}

// ══════════════════════════════════════════════════════════════
// 字符串函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_upper() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT UPPER(name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ALICE".into()));
}

#[test]
fn func_lower() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT LOWER(name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("alice".into()));
}

#[test]
fn func_length() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT LENGTH(name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(5)); // "Alice" = 5 chars
}

#[test]
fn func_substr() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // SUBSTR(name, 1, 3) → "Ali"
    let rows = eng
        .run_sql("SELECT SUBSTR(name, 1, 3) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Ali".into()));
    // SUBSTR(name, 3) → "ice" (from position 3 to end)
    let rows = eng
        .run_sql("SELECT SUBSTR(name, 3) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ice".into()));
}

#[test]
fn func_trim() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT TRIM(name) FROM users WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Hello World".into()));
}

#[test]
fn func_replace() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT REPLACE(name, 'Alice', 'Carol') FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Carol".into()));
}

#[test]
fn func_concat() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT CONCAT(name, ' <', email, '>') FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Alice <alice@test.com>".into()));
}

#[test]
fn func_concat_with_null() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // MySQL 行为：CONCAT 中 NULL 视为空串
    let rows = eng
        .run_sql("SELECT CONCAT(name, email) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Bob".into()));
}

// ══════════════════════════════════════════════════════════════
// 空值处理函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_coalesce() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // email 为 NULL 时返回 'N/A'
    let rows = eng
        .run_sql("SELECT COALESCE(email, 'N/A') FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("N/A".into()));
    // email 非 NULL 时返回 email
    let rows = eng
        .run_sql("SELECT COALESCE(email, 'N/A') FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("alice@test.com".into()));
}

#[test]
fn func_ifnull() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT IFNULL(email, 'unknown') FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("unknown".into()));
}

#[test]
fn func_nullif() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // NULLIF(score, 60) → NULL when score=60
    let rows = eng
        .run_sql("SELECT NULLIF(score, 60) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
    // NULLIF(score, 60) → 95 when score=95
    let rows = eng
        .run_sql("SELECT NULLIF(score, 60) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(95));
}

// ══════════════════════════════════════════════════════════════
// 数学函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_abs() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT ABS(score) FROM users WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(15));
}

#[test]
fn func_round() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT ROUND(price, 1) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Float(20.0));
    let rows = eng
        .run_sql("SELECT ROUND(price) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(10));
}

#[test]
fn func_ceil_floor() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT CEIL(price) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(20));
    let rows = eng
        .run_sql("SELECT FLOOR(price) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(19));
}

// ══════════════════════════════════════════════════════════════
// CAST 类型转换
// ══════════════════════════════════════════════════════════════

#[test]
fn func_cast_int_to_text() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT CAST(id AS TEXT) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("1".into()));
}

#[test]
fn func_cast_float_to_int() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT CAST(price AS INTEGER) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(19)); // 19.99 → 19
}

#[test]
fn func_cast_text_to_int() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, '42')").unwrap();
    let rows = eng
        .run_sql("SELECT CAST(val AS INTEGER) FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(42));
}

// ══════════════════════════════════════════════════════════════
// NULL 传播
// ══════════════════════════════════════════════════════════════

#[test]
fn func_null_propagation() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // UPPER(NULL) → NULL
    let rows = eng
        .run_sql("SELECT UPPER(email) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
    // LENGTH(NULL) → NULL
    let rows = eng
        .run_sql("SELECT LENGTH(email) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
    // ABS(NULL) — 需要 NULL 列，用 CAST 测试
    let rows = eng
        .run_sql("SELECT CAST(email AS INTEGER) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

// ══════════════════════════════════════════════════════════════
// 函数 + AS 别名
// ══════════════════════════════════════════════════════════════

#[test]
fn func_with_alias() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT UPPER(name) AS upper_name FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ALICE".into()));
}

// ══════════════════════════════════════════════════════════════
// 大小写不敏感
// ══════════════════════════════════════════════════════════════

#[test]
fn func_case_insensitive() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT upper(name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ALICE".into()));
    let rows = eng
        .run_sql("SELECT Upper(name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ALICE".into()));
}

// ══════════════════════════════════════════════════════════════
// 字符串扩展函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_ltrim_rtrim() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT LTRIM(name) FROM users WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Hello World  ".into()));
    let rows = eng
        .run_sql("SELECT RTRIM(name) FROM users WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("  Hello World".into()));
}

#[test]
fn func_left_right() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT LEFT(name, 3) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Ali".into()));
    let rows = eng
        .run_sql("SELECT RIGHT(name, 3) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ice".into()));
}

#[test]
fn func_reverse() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT REVERSE(name) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("boB".into()));
}

#[test]
fn func_lpad_rpad() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT LPAD(name, 8, '*') FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("*****Bob".into()));
    let rows = eng
        .run_sql("SELECT RPAD(name, 8, '*') FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Bob*****".into()));
}

#[test]
fn func_charindex_instr() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // CHARINDEX(substr, s) — SQL Server 风格
    let rows = eng
        .run_sql("SELECT CHARINDEX('ice', name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(3)); // "Alice" 中 "ice" 从位置 3 开始
                                               // INSTR(s, substr) — MySQL 风格
    let rows = eng
        .run_sql("SELECT INSTR(name, 'ice') FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(3));
    // 不存在时返回 0
    let rows = eng
        .run_sql("SELECT CHARINDEX('xyz', name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(0));
}

#[test]
fn func_instr_utf8_safe() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_utf8 (id INT, txt TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_utf8 VALUES (1, '你好世界')")
        .unwrap();
    // INSTR 应返回字符位置（3），而非字节位置（7）
    let rows = eng
        .run_sql("SELECT INSTR(txt, '世') FROM t_utf8 WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(3));
    // CHARINDEX 也应返回字符位置
    let rows = eng
        .run_sql("SELECT CHARINDEX('世', txt) FROM t_utf8 WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(3));
}

#[test]
fn func_char_ascii() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // ASCII('A') = 65
    let rows = eng
        .run_sql("SELECT ASCII(name) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(65)); // 'A' = 65
                                                // CHAR(65) = 'A'
    let rows = eng
        .run_sql("SELECT CHAR(65) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("A".into()));
}

// ══════════════════════════════════════════════════════════════
// 条件函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_if_iif() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, flag BOOLEAN)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, TRUE)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, FALSE)").unwrap();
    let rows = eng
        .run_sql("SELECT IF(flag, 'yes', 'no') FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("yes".into()));
    let rows = eng
        .run_sql("SELECT IIF(flag, 'yes', 'no') FROM t WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("no".into()));
}

// ══════════════════════════════════════════════════════════════
// 数学扩展函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_mod() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT MOD(score, 10) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(5)); // 95 % 10 = 5
}

#[test]
fn func_power_sqrt() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT POWER(score, 2) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Float(3600.0)); // 60^2
    let rows = eng
        .run_sql("SELECT SQRT(score) FROM users WHERE id = 1")
        .unwrap();
    // sqrt(95) ≈ 9.7468
    if let Value::Float(f) = &rows[0][0] {
        assert!((*f - 9.7468).abs() < 0.001);
    } else {
        panic!("expected Float");
    }
}

#[test]
fn func_sign() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT SIGN(score) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(1));
    let rows = eng
        .run_sql("SELECT SIGN(score) FROM users WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(-1));
}

#[test]
fn func_truncate() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT TRUNCATE(price, 0) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(19)); // 19.99 截断 → 19
}

#[test]
fn func_exp_log() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // EXP(0) = 1.0
    let rows = eng
        .run_sql("SELECT EXP(price) FROM users WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Float(1.0)); // e^0 = 1
                                               // LOG(1) = 0.0
    let rows = eng
        .run_sql("SELECT LOG(1) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Float(0.0));
    // LOG10(100) = 2.0
    let rows = eng
        .run_sql("SELECT LOG10(100) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Float(2.0));
}

#[test]
fn func_pi() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng.run_sql("SELECT PI() FROM users WHERE id = 1").unwrap();
    if let Value::Float(f) = &rows[0][0] {
        assert!((*f - std::f64::consts::PI).abs() < 1e-10);
    } else {
        panic!("expected Float");
    }
}

// ══════════════════════════════════════════════════════════════
// CONVERT 类型转换
// ══════════════════════════════════════════════════════════════

#[test]
fn func_convert() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT CONVERT('TEXT', score) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("95".into()));
}

// ══════════════════════════════════════════════════════════════
// 日期时间函数（用 INT 列存储毫秒时间戳，避免 schema 类型校验问题）
// ══════════════════════════════════════════════════════════════

#[test]
fn func_year_month_day() {
    let (_dir, mut eng) = tmp_engine();
    // 用 INT 列存储毫秒时间戳（函数接受 Integer 和 Timestamp）
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    // 2025-06-15 10:30:45 UTC = 1750000245000 ms
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT YEAR(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(2025));
    let rows = eng
        .run_sql("SELECT MONTH(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(6));
    let rows = eng
        .run_sql("SELECT DAY(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(15));
}

#[test]
fn func_hour_minute_second() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT HOUR(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(15));
    let rows = eng
        .run_sql("SELECT MINUTE(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(10));
    let rows = eng
        .run_sql("SELECT SECOND(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(45));
}

#[test]
fn func_datediff() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts1 INT, ts2 INT)")
        .unwrap();
    // ts1 = 2025-06-15, ts2 = 2025-06-10 → diff = 5 days
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000, 1749568245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATEDIFF(ts1, ts2) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(5));
}

#[test]
fn func_dateadd() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    // DATEADD('DAY', 1, ts) → +86400000 ms
    let rows = eng
        .run_sql("SELECT DATEADD('DAY', 1, ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Timestamp(1750000245000 + 86_400_000));
}

// ══════════════════════════════════════════════════════════════
// P1 日期时间扩展
// ══════════════════════════════════════════════════════════════

#[test]
fn func_quarter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap(); // 2025-06-15
    let rows = eng
        .run_sql("SELECT QUARTER(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(2)); // June = Q2
}

#[test]
fn func_week() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT WEEK(ts) FROM events WHERE id = 1")
        .unwrap();
    // 2025-06-15 应该是第 24 周左右
    if let Value::Integer(w) = &rows[0][0] {
        assert!(*w >= 23 && *w <= 25, "week={}", w);
    } else {
        panic!("expected Integer");
    }
}

#[test]
fn func_weekday_dayofweek() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    // 2025-01-06 = Monday
    // 2025-01-06 00:00:00 UTC = 1736121600000 ms
    eng.run_sql("INSERT INTO events VALUES (1, 1736121600000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT WEEKDAY(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(1)); // Monday = 1 (0=Sunday)
    let rows = eng
        .run_sql("SELECT DAYOFWEEK(ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(2)); // Monday = 2 (1=Sunday)
}

#[test]
fn func_datepart() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATEPART('YEAR', ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(2025));
    let rows = eng
        .run_sql("SELECT DATEPART('QUARTER', ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn func_date_format() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATE_FORMAT(ts, '%Y-%m-%d') FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("2025-06-15".into()));
    let rows = eng
        .run_sql("SELECT DATE_FORMAT(ts, '%H:%i:%s') FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("15:10:45".into()));
}

#[test]
fn func_time_bucket() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    // TIME_BUCKET('1 hour', ts) → 对齐到小时
    let rows = eng
        .run_sql("SELECT TIME_BUCKET('1 hour', ts) FROM events WHERE id = 1")
        .unwrap();
    if let Value::Timestamp(ts) = &rows[0][0] {
        // 应该对齐到整小时
        assert_eq!(*ts % 3_600_000, 0);
    } else {
        panic!("expected Timestamp");
    }
}

// ══════════════════════════════════════════════════════════════
// P2 日期时间扩展
// ══════════════════════════════════════════════════════════════

#[test]
fn func_last_day() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap(); // 2025-06-15
    let rows = eng
        .run_sql("SELECT LAST_DAY(ts) FROM events WHERE id = 1")
        .unwrap();
    if let Value::Timestamp(ts) = &rows[0][0] {
        // 6月最后一天是30号，提取 DAY 验证
        let day_val = super::sql_funcs_dt::func_day(&[Value::Timestamp(*ts)]);
        assert_eq!(day_val, Value::Integer(30));
    } else {
        panic!("expected Timestamp");
    }
}

#[test]
fn func_timestampdiff() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts1 INT, ts2 INT)")
        .unwrap();
    // ts1 = 2025-06-10, ts2 = 2025-06-15 → diff = 5 days
    eng.run_sql("INSERT INTO events VALUES (1, 1749568245000, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT TIMESTAMPDIFF('DAY', ts1, ts2) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(5));
    let rows = eng
        .run_sql("SELECT TIMESTAMPDIFF('HOUR', ts1, ts2) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(120)); // 5 * 24 = 120
}

#[test]
fn func_timestampadd() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, ts INT)").unwrap();
    eng.run_sql("INSERT INTO events VALUES (1, 1750000245000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT TIMESTAMPADD('DAY', 2, ts) FROM events WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Timestamp(1750000245000 + 2 * 86_400_000));
}

// ══════════════════════════════════════════════════════════════
// 哈希函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_md5() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT MD5(name) FROM users WHERE id = 1")
        .unwrap();
    if let Value::Text(s) = &rows[0][0] {
        assert_eq!(s.len(), 32); // MD5 = 32 hex chars
                                 // RFC 1321 已知向量：MD5("Alice") = 64489c85dc2fe0787b85cd87214b3810
        assert_eq!(s, "64489c85dc2fe0787b85cd87214b3810");
    } else {
        panic!("expected Text");
    }
}

#[test]
fn func_sha1() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT SHA1(name) FROM users WHERE id = 1")
        .unwrap();
    if let Value::Text(s) = &rows[0][0] {
        assert_eq!(s.len(), 40); // SHA1 = 40 hex chars
                                 // 已知向量：SHA1("Alice") = 35318264c9a98faf79965c270ac80c5606774df1
        assert_eq!(s, "35318264c9a98faf79965c270ac80c5606774df1");
    } else {
        panic!("expected Text");
    }
}

#[test]
fn func_sha2() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT SHA2(name, 256) FROM users WHERE id = 1")
        .unwrap();
    if let Value::Text(s) = &rows[0][0] {
        assert_eq!(s.len(), 64); // SHA256 = 64 hex chars
                                 // 已知向量：SHA256("Alice") = 3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043
        assert_eq!(
            s,
            "3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043"
        );
    } else {
        panic!("expected Text");
    }
}

#[test]
fn func_sha2_unsupported_bits() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    // SHA2(x, 512) 暂不支持，应返回 NULL
    let rows = eng
        .run_sql("SELECT SHA2(name, 512) FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn func_md5_null() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT MD5(email) FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null); // NULL → NULL
}

// ══════════════════════════════════════════════════════════════
// 系统函数
// ══════════════════════════════════════════════════════════════

#[test]
fn func_database_version() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT DATABASE() FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("talon".into()));
    let rows = eng
        .run_sql("SELECT VERSION() FROM users WHERE id = 1")
        .unwrap();
    if let Value::Text(s) = &rows[0][0] {
        assert!(!s.is_empty());
    } else {
        panic!("expected Text");
    }
}

#[test]
fn func_user_connection_id() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT USER() FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("talon".into()));
    let rows = eng
        .run_sql("SELECT CONNECTION_ID() FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(0));
}

#[test]
fn func_row_count_last_insert_id() {
    let (_dir, mut eng) = tmp_engine();
    setup_users(&mut eng);
    let rows = eng
        .run_sql("SELECT ROW_COUNT() FROM users WHERE id = 1")
        .unwrap();
    // 占位值，返回 Integer 即可
    assert!(matches!(rows[0][0], Value::Integer(_)));
    let rows = eng
        .run_sql("SELECT LAST_INSERT_ID() FROM users WHERE id = 1")
        .unwrap();
    assert!(matches!(rows[0][0], Value::Integer(_)));
}

// ══════════════════════════════════════════════════════════════
// JSON 函数
// ══════════════════════════════════════════════════════════════

fn setup_json_table(eng: &mut SqlEngine) {
    eng.run_sql("CREATE TABLE jdocs (id INT, data JSONB)")
        .unwrap();
    eng.run_sql(r#"INSERT INTO jdocs VALUES (1, '{"name":"Alice","age":30,"tags":["rust","db"],"addr":{"city":"SH"}}')"#)
        .unwrap();
    eng.run_sql(r#"INSERT INTO jdocs VALUES (2, '{"name":"Bob","age":25,"tags":["go"],"addr":{"city":"BJ"}}')"#)
        .unwrap();
    eng.run_sql(r#"INSERT INTO jdocs VALUES (3, '{"name":"Carol","age":null,"tags":[]}')"#)
        .unwrap();
}

#[test]
fn func_json_extract_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.name') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Alice".into()));
    // 数值提取
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.age') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(30));
}

#[test]
fn func_json_extract_nested() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.addr.city') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("SH".into()));
}

#[test]
fn func_json_extract_array_index() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.tags[0]') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("rust".into()));
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.tags[1]') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("db".into()));
}

#[test]
fn func_json_extract_missing_path() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.nonexist') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn func_json_extract_null_input() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE jnull (id INT, data JSONB)")
        .unwrap();
    eng.run_sql("INSERT INTO jnull VALUES (1, NULL)").unwrap();
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT(data, '$.key') FROM jnull WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn func_json_extract_text_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    // 字符串值直接返回文本
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT_TEXT(data, '$.name') FROM jdocs WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Bob".into()));
    // 数值转为文本
    let rows = eng
        .run_sql("SELECT JSON_EXTRACT_TEXT(data, '$.age') FROM jdocs WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("25".into()));
}

#[test]
fn func_json_set_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_SET(data, '$.name', 'Zara') FROM jdocs WHERE id = 1")
        .unwrap();
    if let Value::Jsonb(j) = &rows[0][0] {
        assert_eq!(j["name"], "Zara");
        // 其他字段不变
        assert_eq!(j["age"], 30);
    } else {
        panic!("expected Jsonb, got {:?}", rows[0][0]);
    }
}

#[test]
fn func_json_set_nested() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_SET(data, '$.addr.city', 'GZ') FROM jdocs WHERE id = 1")
        .unwrap();
    if let Value::Jsonb(j) = &rows[0][0] {
        assert_eq!(j["addr"]["city"], "GZ");
    } else {
        panic!("expected Jsonb");
    }
}

#[test]
fn func_json_set_new_key() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_SET(data, '$.email', 'a@b.com') FROM jdocs WHERE id = 1")
        .unwrap();
    if let Value::Jsonb(j) = &rows[0][0] {
        assert_eq!(j["email"], "a@b.com");
    } else {
        panic!("expected Jsonb");
    }
}

#[test]
fn func_json_remove_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_REMOVE(data, '$.age') FROM jdocs WHERE id = 1")
        .unwrap();
    if let Value::Jsonb(j) = &rows[0][0] {
        assert!(j.get("age").is_none());
        assert_eq!(j["name"], "Alice");
    } else {
        panic!("expected Jsonb");
    }
}

#[test]
fn func_json_remove_nested() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_REMOVE(data, '$.addr.city') FROM jdocs WHERE id = 1")
        .unwrap();
    if let Value::Jsonb(j) = &rows[0][0] {
        assert!(j["addr"].get("city").is_none());
    } else {
        panic!("expected Jsonb");
    }
}

#[test]
fn func_json_type_variants() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    // object
    let rows = eng
        .run_sql("SELECT JSON_TYPE(data) FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("object".into()));
}

#[test]
fn func_json_array_length_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_ARRAY_LENGTH(data, '$.tags') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(2));
    // 空数组
    let rows = eng
        .run_sql("SELECT JSON_ARRAY_LENGTH(data, '$.tags') FROM jdocs WHERE id = 3")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(0));
}

#[test]
fn func_json_array_length_non_array() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    // 对象不是数组，返回 NULL
    let rows = eng
        .run_sql("SELECT JSON_ARRAY_LENGTH(data, '$.addr') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn func_json_keys_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_KEYS(data) FROM jdocs WHERE id = 3")
        .unwrap();
    // Carol 的 keys: name, age, tags
    if let Value::Text(s) = &rows[0][0] {
        assert!(s.contains("name"));
        assert!(s.contains("age"));
        assert!(s.contains("tags"));
    } else {
        panic!("expected Text");
    }
}

#[test]
fn func_json_valid_true() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE jtxt (id INT, raw TEXT)").unwrap();
    eng.run_sql(r#"INSERT INTO jtxt VALUES (1, '{"a":1}')"#)
        .unwrap();
    eng.run_sql(r#"INSERT INTO jtxt VALUES (2, 'not json')"#)
        .unwrap();
    let rows = eng
        .run_sql("SELECT JSON_VALID(raw) FROM jtxt WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(true));
    let rows = eng
        .run_sql("SELECT JSON_VALID(raw) FROM jtxt WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(false));
}

#[test]
fn func_json_valid_jsonb_col() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_VALID(data) FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(true));
}

#[test]
fn func_json_contains_object_key() {
    let (_dir, mut eng) = tmp_engine();
    setup_json_table(&mut eng);
    let rows = eng
        .run_sql("SELECT JSON_CONTAINS(data, 'name') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(true));
    let rows = eng
        .run_sql("SELECT JSON_CONTAINS(data, 'missing') FROM jdocs WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(false));
}

#[test]
fn func_json_contains_array_element() {
    let (_dir, mut eng) = tmp_engine();
    // 直接用数组作为顶层 JSON 测试 json_contains
    eng.run_sql("CREATE TABLE jarr (id INT, data JSONB)")
        .unwrap();
    eng.run_sql(r#"INSERT INTO jarr VALUES (1, '["rust","db","go"]')"#)
        .unwrap();
    let rows = eng
        .run_sql("SELECT JSON_CONTAINS(data, 'rust') FROM jarr WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(true));
    let rows = eng
        .run_sql("SELECT JSON_CONTAINS(data, 'python') FROM jarr WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(false));
}

// ══════════════════════════════════════════════════════════════
// 移植自 apache/datafusion 的函数测试
// ══════════════════════════════════════════════════════════════

fn setup_texts(eng: &mut SqlEngine) {
    eng.run_sql("CREATE TABLE texts (id INT, content TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO texts VALUES (1, 'hello world 123')")
        .unwrap();
    eng.run_sql("INSERT INTO texts VALUES (2, 'foo,bar,baz')")
        .unwrap();
    eng.run_sql("INSERT INTO texts VALUES (3, 'aAbBcC')")
        .unwrap();
    eng.run_sql("INSERT INTO texts VALUES (4, NULL)")
        .unwrap();
}

#[test]
fn func_regexp_replace_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    // 替换第一个数字序列为 "NUM"
    let rows = eng
        .run_sql("SELECT REGEXP_REPLACE(content, '[0-9]+', 'NUM') FROM texts WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("hello world NUM".into()));
}

#[test]
fn func_regexp_replace_global() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    // 替换所有小写字母（每段连续小写字母作为一个匹配）
    let rows = eng
        .run_sql("SELECT REGEXP_REPLACE(content, '[a-z]+', 'X', 'g') FROM texts WHERE id = 3")
        .unwrap();
    // 'aAbBcC' → 'a'(匹配)→X, 'A'(不匹配)→A, 'b'(匹配)→X, 'B'(不匹配)→B, 'c'(匹配)→X, 'C'(不匹配)→C
    assert_eq!(rows[0][0], Value::Text("XAXBXC".into()));
}

#[test]
fn func_regexp_replace_null() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    let rows = eng
        .run_sql("SELECT REGEXP_REPLACE(content, '[0-9]+', 'N') FROM texts WHERE id = 4")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn func_regexp_like_match() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    let rows = eng
        .run_sql("SELECT REGEXP_LIKE(content, '^hello') FROM texts WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(true));
}

#[test]
fn func_regexp_like_no_match() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    let rows = eng
        .run_sql("SELECT REGEXP_LIKE(content, '^world') FROM texts WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Boolean(false));
}

#[test]
fn func_regexp_like_null() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    let rows = eng
        .run_sql("SELECT REGEXP_LIKE(content, '^hello') FROM texts WHERE id = 4")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn func_split_part_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    // "foo,bar,baz" 分割，第 2 部分 = "bar"
    let rows = eng
        .run_sql("SELECT SPLIT_PART(content, ',', 2) FROM texts WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("bar".into()));
}

#[test]
fn func_split_part_first() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    let rows = eng
        .run_sql("SELECT SPLIT_PART(content, ',', 1) FROM texts WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("foo".into()));
}

#[test]
fn func_split_part_out_of_range() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    // 超出范围返回空字符串
    let rows = eng
        .run_sql("SELECT SPLIT_PART(content, ',', 10) FROM texts WHERE id = 2")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text(String::new()));
}

#[test]
fn func_repeat_basic() {
    let (_dir, mut eng) = tmp_engine();
    setup_texts(&mut eng);
    eng.run_sql("CREATE TABLE rep_test (id INT, s TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO rep_test VALUES (1, 'ab')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT REPEAT(s, 3) FROM rep_test WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ababab".into()));
}

#[test]
fn func_repeat_zero() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE rep2 (id INT, s TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO rep2 VALUES (1, 'ab')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT REPEAT(s, 0) FROM rep2 WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text(String::new()));
}

#[test]
fn func_translate_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE tr_test (id INT, s TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO tr_test VALUES (1, 'hello')")
        .unwrap();
    // 将 'helo' 替换为 'HELO'
    let rows = eng
        .run_sql("SELECT TRANSLATE(s, 'helo', 'HELO') FROM tr_test WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("HELLO".into()));
}

#[test]
fn func_translate_delete_chars() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE tr2 (id INT, s TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO tr2 VALUES (1, 'hello')")
        .unwrap();
    // to_chars 比 from_chars 短 → 超出部分被删除
    let rows = eng
        .run_sql("SELECT TRANSLATE(s, 'helo', 'HE') FROM tr2 WHERE id = 1")
        .unwrap();
    // 'h'→'H', 'e'→'E', 'l'→删除, 'l'→删除, 'o'→删除 → "HE"
    assert_eq!(rows[0][0], Value::Text("HE".into()));
}

#[test]
fn func_date_trunc_hour() {
    let (_dir, mut eng) = tmp_engine();
    // 用 INT 列存储毫秒时间戳（与 YEAR/MONTH/DAY 等函数的测试保持一致）
    eng.run_sql("CREATE TABLE events (id INT, ts INT)")
        .unwrap();
    // 2024-01-15 10:50:45 UTC = 1705315845000 ms
    eng.run_sql("INSERT INTO events VALUES (1, 1705315845000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATE_TRUNC('hour', ts) FROM events WHERE id = 1")
        .unwrap();
    // 2024-01-15 10:00:00 UTC = 1705312800000 ms
    assert_eq!(rows[0][0], Value::Timestamp(1705312800000));
}

#[test]
fn func_date_trunc_day() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events2 (id INT, ts INT)")
        .unwrap();
    // 2024-01-15 10:50:45 UTC = 1705315845000 ms
    eng.run_sql("INSERT INTO events2 VALUES (1, 1705315845000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATE_TRUNC('day', ts) FROM events2 WHERE id = 1")
        .unwrap();
    // 2024-01-15 00:00:00 UTC = 1705276800000 ms
    assert_eq!(rows[0][0], Value::Timestamp(1705276800000));
}

#[test]
fn func_date_trunc_month() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events3 (id INT, ts INT)")
        .unwrap();
    // 2024-01-15 10:50:45 UTC = 1705315845000 ms
    eng.run_sql("INSERT INTO events3 VALUES (1, 1705315845000)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATE_TRUNC('month', ts) FROM events3 WHERE id = 1")
        .unwrap();
    // 2024-01-01 00:00:00 UTC = 1704067200000 ms
    assert_eq!(rows[0][0], Value::Timestamp(1704067200000));
}

#[test]
fn func_date_trunc_null() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events4 (id INT, ts INT)")
        .unwrap();
    eng.run_sql("INSERT INTO events4 VALUES (1, NULL)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DATE_TRUNC('day', ts) FROM events4 WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Null);
}
