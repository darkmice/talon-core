/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 日期时间函数实现。
//!
//! Talon 的 Timestamp 存储为自 Unix 纪元以来的毫秒数（i64）。
//! 日期时间函数基于此进行提取和运算。

use crate::types::Value;

/// 从毫秒时间戳提取各字段的辅助结构。
struct DtParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

/// 从毫秒时间戳解析日期时间各部分。
fn ts_to_parts(ms: i64) -> DtParts {
    let secs = ms / 1000;
    let (mut days, time_of_day) = if secs >= 0 {
        (secs / 86400, (secs % 86400) as u32)
    } else {
        let d = (secs - 86399) / 86400; // 向负无穷取整
        let t = (secs - d * 86400) as u32;
        (d, t)
    };
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    // 从 Unix 纪元天数（1970-01-01 = day 0）计算年月日
    days += 719468; // 转为从 0000-03-01 起的天数
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    DtParts {
        year: y as i32,
        month: m,
        day: d,
        hour,
        minute,
        second,
    }
}

/// 从日期部分构造毫秒时间戳。
fn parts_to_ts(p: &DtParts) -> i64 {
    let (y, m) = if p.month <= 2 {
        (p.year as i64 - 1, p.month + 9)
    } else {
        (p.year as i64, p.month - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * m + 2) / 5 + p.day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;
    (days * 86400 + p.hour as i64 * 3600 + p.minute as i64 * 60 + p.second as i64) * 1000
}

/// 从 Value 提取毫秒时间戳。
fn extract_ts(v: &Value) -> Option<i64> {
    match v {
        Value::Timestamp(ts) => Some(*ts),
        Value::Integer(n) => Some(*n),
        _ => None,
    }
}

/// NOW() / GETDATE() / CURRENT_TIMESTAMP — 当前时间戳（毫秒）。
pub(super) fn func_now() -> Value {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    Value::Timestamp(ts)
}

/// YEAR(dt) — 提取年份。
pub(super) fn func_year(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(ts_to_parts(ms).year as i64),
        None => Value::Null,
    }
}

/// MONTH(dt) — 提取月份（1~12）。
pub(super) fn func_month(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(ts_to_parts(ms).month as i64),
        None => Value::Null,
    }
}

/// DAY(dt) / DAYOFMONTH(dt) — 提取日（1~31）。
pub(super) fn func_day(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(ts_to_parts(ms).day as i64),
        None => Value::Null,
    }
}

/// HOUR(dt) — 提取小时（0~23）。
pub(super) fn func_hour(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(ts_to_parts(ms).hour as i64),
        None => Value::Null,
    }
}

/// MINUTE(dt) — 提取分钟（0~59）。
pub(super) fn func_minute(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(ts_to_parts(ms).minute as i64),
        None => Value::Null,
    }
}

/// SECOND(dt) — 提取秒（0~59）。
pub(super) fn func_second(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(ts_to_parts(ms).second as i64),
        None => Value::Null,
    }
}

/// DATEDIFF(dt1, dt2) — 日期差（天），MySQL 风格：dt1 - dt2。
pub(super) fn func_datediff(args: &[Value]) -> Value {
    let ts1 = args.first().and_then(extract_ts);
    let ts2 = args.get(1).and_then(extract_ts);
    match (ts1, ts2) {
        (Some(a), Some(b)) => Value::Integer((a - b) / 86_400_000),
        _ => Value::Null,
    }
}

/// DATEADD(part, n, dt) — SQL Server 风格日期加减。
/// part: 'year'/'month'/'day'/'hour'/'minute'/'second'。
pub(super) fn func_dateadd(args: &[Value]) -> Value {
    let part = match args.first() {
        Some(Value::Text(s)) => s.to_uppercase(),
        _ => return Value::Null,
    };
    let n = match args.get(1) {
        Some(Value::Integer(n)) => *n,
        Some(Value::Float(f)) => *f as i64,
        _ => return Value::Null,
    };
    let ts = match args.get(2).and_then(extract_ts) {
        Some(ts) => ts,
        None => return Value::Null,
    };
    Value::Timestamp(add_interval(ts, &part, n))
}

/// DATE_ADD(dt, INTERVAL n unit) — MySQL 风格。
/// 简化实现：args[0]=dt, args[1]=n, args[2]=unit（由解析器预处理）。
/// 当前简化：args[0]=dt, args[1]=毫秒增量（由调用方计算）。
/// 实际使用：args = [dt, n, Text("unit")]。
pub(super) fn func_date_add(args: &[Value]) -> Value {
    let ts = match args.first().and_then(extract_ts) {
        Some(ts) => ts,
        None => return Value::Null,
    };
    let n = match args.get(1) {
        Some(Value::Integer(n)) => *n,
        Some(Value::Float(f)) => *f as i64,
        _ => return Value::Null,
    };
    let unit = match args.get(2) {
        Some(Value::Text(s)) => s.to_uppercase(),
        _ => "DAY".to_string(),
    };
    Value::Timestamp(add_interval(ts, &unit, n))
}

/// DATE_SUB(dt, INTERVAL n unit) — MySQL 风格（等同 DATE_ADD 取负）。
pub(super) fn func_date_sub(args: &[Value]) -> Value {
    let ts = match args.first().and_then(extract_ts) {
        Some(ts) => ts,
        None => return Value::Null,
    };
    let n = match args.get(1) {
        Some(Value::Integer(n)) => *n,
        Some(Value::Float(f)) => *f as i64,
        _ => return Value::Null,
    };
    let unit = match args.get(2) {
        Some(Value::Text(s)) => s.to_uppercase(),
        _ => "DAY".to_string(),
    };
    Value::Timestamp(add_interval(ts, &unit, -n))
}

/// 日期加减核心：根据 part/unit 和增量 n 计算新时间戳。
fn add_interval(ts_ms: i64, unit: &str, n: i64) -> i64 {
    match unit {
        "SECOND" | "SECONDS" => ts_ms + n * 1000,
        "MINUTE" | "MINUTES" => ts_ms + n * 60_000,
        "HOUR" | "HOURS" => ts_ms + n * 3_600_000,
        "DAY" | "DAYS" => ts_ms + n * 86_400_000,
        "MONTH" | "MONTHS" => {
            let mut p = ts_to_parts(ts_ms);
            let total_months = p.year as i64 * 12 + (p.month as i64 - 1) + n;
            p.year = (total_months / 12) as i32;
            p.month = ((total_months % 12 + 12) % 12 + 1) as u32;
            // 修正日期溢出（如 1月31日 + 1月 → 2月28日）
            let max_day = days_in_month(p.year, p.month);
            if p.day > max_day {
                p.day = max_day;
            }
            parts_to_ts(&p)
        }
        "YEAR" | "YEARS" => {
            let mut p = ts_to_parts(ts_ms);
            p.year += n as i32;
            let max_day = days_in_month(p.year, p.month);
            if p.day > max_day {
                p.day = max_day;
            }
            parts_to_ts(&p)
        }
        _ => ts_ms, // 未知单位不变
    }
}

/// 指定年月的天数。
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// 是否闰年。
fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// DATEPART(part, dt) — SQL Server 风格，提取日期部分。
/// 支持 YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/QUARTER/WEEK/WEEKDAY/DAYOFWEEK。
pub(super) fn func_datepart(args: &[Value]) -> Value {
    let part = match args.first() {
        Some(Value::Text(s)) => s.to_uppercase(),
        _ => return Value::Null,
    };
    let ms = match args.get(1).and_then(extract_ts) {
        Some(ms) => ms,
        None => return Value::Null,
    };
    let p = ts_to_parts(ms);
    match part.as_str() {
        "YEAR" | "YY" | "YYYY" => Value::Integer(p.year as i64),
        "MONTH" | "MM" | "M" => Value::Integer(p.month as i64),
        "DAY" | "DD" | "D" => Value::Integer(p.day as i64),
        "HOUR" | "HH" => Value::Integer(p.hour as i64),
        "MINUTE" | "MI" | "N" => Value::Integer(p.minute as i64),
        "SECOND" | "SS" | "S" => Value::Integer(p.second as i64),
        "QUARTER" | "QQ" | "Q" => Value::Integer(((p.month - 1) / 3 + 1) as i64),
        "WEEK" | "WK" | "WW" => Value::Integer(week_of_year(p.year, p.month, p.day) as i64),
        "WEEKDAY" | "DW" => Value::Integer(day_of_week(ms) as i64),
        _ => Value::Null,
    }
}

/// DATE_FORMAT(dt, format) — MySQL 风格日期格式化。
/// 支持 %Y %m %d %H %i %s %W %j %U 等常用格式符。
pub(super) fn func_date_format(args: &[Value]) -> Value {
    let ms = match args.first().and_then(extract_ts) {
        Some(ms) => ms,
        None => return Value::Null,
    };
    let fmt = match args.get(1) {
        Some(Value::Text(s)) => s.as_str(),
        _ => return Value::Null,
    };
    let p = ts_to_parts(ms);
    let mut result = String::with_capacity(fmt.len() + 16);
    let bytes = fmt.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 1 < bytes.len() {
            i += 1;
            match bytes[i] {
                b'Y' => result.push_str(&format!("{:04}", p.year)),
                b'y' => result.push_str(&format!("{:02}", p.year % 100)),
                b'm' => result.push_str(&format!("{:02}", p.month)),
                b'c' => result.push_str(&p.month.to_string()),
                b'd' => result.push_str(&format!("{:02}", p.day)),
                b'e' => result.push_str(&p.day.to_string()),
                b'H' => result.push_str(&format!("{:02}", p.hour)),
                b'k' => result.push_str(&p.hour.to_string()),
                b'i' => result.push_str(&format!("{:02}", p.minute)),
                b's' | b'S' => result.push_str(&format!("{:02}", p.second)),
                b'j' => result.push_str(&format!("{:03}", day_of_year(p.year, p.month, p.day))),
                b'U' => result.push_str(&format!("{:02}", week_of_year(p.year, p.month, p.day))),
                b'w' => result.push_str(&day_of_week(ms).to_string()),
                b'%' => result.push('%'),
                _ => {
                    result.push('%');
                    result.push(bytes[i] as char);
                }
            }
        } else {
            result.push(bytes[i] as char);
        }
        i += 1;
    }
    Value::Text(result)
}

/// TIME_BUCKET(bucket, dt) — 时序聚合，将时间戳对齐到指定桶。
/// bucket 格式如 '1 hour'、'5 minute'、'1 day'。
pub(super) fn func_time_bucket(args: &[Value]) -> Value {
    let bucket = match args.first() {
        Some(Value::Text(s)) => s.trim().to_lowercase(),
        _ => return Value::Null,
    };
    let ms = match args.get(1).and_then(extract_ts) {
        Some(ms) => ms,
        None => return Value::Null,
    };
    // 解析 "N unit" 格式
    let parts: Vec<&str> = bucket.split_whitespace().collect();
    let (n, unit) = if parts.len() == 2 {
        let n = parts[0].parse::<i64>().unwrap_or(1);
        (n, parts[1].to_uppercase())
    } else if parts.len() == 1 {
        // 尝试 "1hour" 格式
        let s = parts[0];
        let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let n = s[..num_end].parse::<i64>().unwrap_or(1);
        (n, s[num_end..].to_uppercase())
    } else {
        return Value::Null;
    };
    let bucket_ms = match unit.trim_end_matches('S').as_ref() {
        "SECOND" => n * 1000,
        "MINUTE" => n * 60_000,
        "HOUR" => n * 3_600_000,
        "DAY" => n * 86_400_000,
        _ => return Value::Null,
    };
    if bucket_ms <= 0 {
        return Value::Null;
    }
    Value::Timestamp(ms / bucket_ms * bucket_ms)
}

/// WEEKDAY(dt) — 0=周日~6=周六。
pub(super) fn func_weekday(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(day_of_week(ms) as i64),
        None => Value::Null,
    }
}

/// DAYOFWEEK(dt) — 1=周日~7=周六（MySQL 兼容）。
pub(super) fn func_dayofweek(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => Value::Integer(day_of_week(ms) as i64 + 1),
        None => Value::Null,
    }
}

/// QUARTER(dt) — 季度 1~4。
pub(super) fn func_quarter(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => {
            let m = ts_to_parts(ms).month;
            Value::Integer(((m - 1) / 3 + 1) as i64)
        }
        None => Value::Null,
    }
}

/// WEEK(dt) — 一年中的第几周（0~53）。
pub(super) fn func_week(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => {
            let p = ts_to_parts(ms);
            Value::Integer(week_of_year(p.year, p.month, p.day) as i64)
        }
        None => Value::Null,
    }
}

/// LAST_DAY(dt) — 当月最后一天的时间戳。
pub(super) fn func_last_day(args: &[Value]) -> Value {
    match args.first().and_then(extract_ts) {
        Some(ms) => {
            let mut p = ts_to_parts(ms);
            p.day = days_in_month(p.year, p.month);
            Value::Timestamp(parts_to_ts(&p))
        }
        None => Value::Null,
    }
}

/// TIMESTAMPDIFF(unit, dt1, dt2) — 时间差（指定单位），MySQL 风格：dt2 - dt1。
pub(super) fn func_timestampdiff(args: &[Value]) -> Value {
    let unit = match args.first() {
        Some(Value::Text(s)) => s.to_uppercase(),
        _ => return Value::Null,
    };
    let ts1 = match args.get(1).and_then(extract_ts) {
        Some(ts) => ts,
        None => return Value::Null,
    };
    let ts2 = match args.get(2).and_then(extract_ts) {
        Some(ts) => ts,
        None => return Value::Null,
    };
    let diff_ms = ts2 - ts1;
    let result = match unit.as_str() {
        "SECOND" | "SECONDS" => diff_ms / 1000,
        "MINUTE" | "MINUTES" => diff_ms / 60_000,
        "HOUR" | "HOURS" => diff_ms / 3_600_000,
        "DAY" | "DAYS" => diff_ms / 86_400_000,
        "MONTH" | "MONTHS" => {
            let p1 = ts_to_parts(ts1);
            let p2 = ts_to_parts(ts2);
            (p2.year as i64 - p1.year as i64) * 12 + (p2.month as i64 - p1.month as i64)
        }
        "YEAR" | "YEARS" => {
            let p1 = ts_to_parts(ts1);
            let p2 = ts_to_parts(ts2);
            p2.year as i64 - p1.year as i64
        }
        _ => return Value::Null,
    };
    Value::Integer(result)
}

/// TIMESTAMPADD(unit, n, dt) — 时间加减（指定单位），MySQL 风格。
/// 等同于 DATEADD 语义。
pub(super) fn func_timestampadd(args: &[Value]) -> Value {
    func_dateadd(args)
}

/// 计算星期几：0=周日~6=周六（基于 Unix 纪元 1970-01-01 是周四）。
fn day_of_week(ms: i64) -> u32 {
    let days = ms.div_euclid(86_400_000);
    ((days + 4) % 7) as u32 // 1970-01-01 = Thursday (4)
}

/// 计算一年中的第几天（1~366）。
fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    let mut doy = day;
    for m in 1..month {
        doy += days_in_month(year, m);
    }
    doy
}

/// 计算一年中的第几周（0~53，周日为一周起始）。
fn week_of_year(year: i32, month: u32, day: u32) -> u32 {
    let doy = day_of_year(year, month, day);
    // 计算 1 月 1 日是星期几
    let jan1_ms = parts_to_ts(&DtParts {
        year,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
    });
    let jan1_dow = day_of_week(jan1_ms);
    (doy + jan1_dow - 1) / 7
}

// ── 移植自 apache/datafusion-sqlparser-rs 语法 / apache/datafusion 实现 ──

/// DATE_TRUNC(precision, timestamp) — 将时间戳截断到指定精度。
///
/// 语法参考：apache/datafusion-sqlparser-rs `DateTrunc` 表达式。
/// 实现参考：apache/datafusion `datetime_expressions::date_trunc`。
///
/// 支持的精度（大小写不敏感）：
/// `microsecond`、`millisecond`、`second`、`minute`、`hour`、
/// `day`、`week`（周一为起点）、`month`、`quarter`、`year`、`decade`、`century`。
///
/// 用途：AI 时序场景下按时间粒度聚合对话日志、事件指标（如按小时统计 token 用量）。
pub(super) fn func_date_trunc(args: &[Value]) -> Value {
    let precision = match args.first() {
        Some(Value::Text(s)) => s.to_uppercase(),
        Some(Value::Null) | None => return Value::Null,
        _ => return Value::Null,
    };
    let ms = match args.get(1) {
        Some(Value::Timestamp(t)) => *t,
        Some(Value::Integer(n)) => *n,
        Some(Value::Null) => return Value::Null,
        _ => return Value::Null,
    };
    let p = ts_to_parts(ms);
    let truncated_ms = match precision.as_str() {
        "MICROSECOND" | "MICROSECONDS" => ms, // Talon 时间精度为毫秒，微秒截断等同于毫秒截断
        "MILLISECOND" | "MILLISECONDS" => ms,
        "SECOND" | "SECONDS" => {
            let secs = ms / 1000;
            secs * 1000
        }
        "MINUTE" | "MINUTES" => {
            let mins = ms / 60_000;
            mins * 60_000
        }
        "HOUR" | "HOURS" => {
            let hours = ms / 3_600_000;
            hours * 3_600_000
        }
        "DAY" | "DAYS" => parts_to_ts(&DtParts {
            year: p.year,
            month: p.month,
            day: p.day,
            hour: 0,
            minute: 0,
            second: 0,
        }),
        "WEEK" | "WEEKS" => {
            // 截断到最近的周一
            let dow = day_of_week(ms) as i64; // 0=周日, 1=周一, …, 6=周六
            let days_since_monday = if dow == 0 { 6 } else { dow - 1 };
            let day_start = parts_to_ts(&DtParts {
                year: p.year,
                month: p.month,
                day: p.day,
                hour: 0,
                minute: 0,
                second: 0,
            });
            day_start - days_since_monday * 86_400_000
        }
        "MONTH" | "MONTHS" => parts_to_ts(&DtParts {
            year: p.year,
            month: p.month,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        }),
        "QUARTER" | "QUARTERS" => {
            let q_month = ((p.month - 1) / 3) * 3 + 1;
            parts_to_ts(&DtParts {
                year: p.year,
                month: q_month,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
            })
        }
        "YEAR" | "YEARS" => parts_to_ts(&DtParts {
            year: p.year,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        }),
        "DECADE" | "DECADES" => parts_to_ts(&DtParts {
            year: (p.year / 10) * 10,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        }),
        "CENTURY" | "CENTURIES" => {
            let century_year = if p.year > 0 {
                ((p.year - 1) / 100) * 100 + 1
            } else {
                (p.year / 100) * 100
            };
            parts_to_ts(&DtParts {
                year: century_year,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
            })
        }
        _ => return Value::Null,
    };
    Value::Timestamp(truncated_ms)
}
