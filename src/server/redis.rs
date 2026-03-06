/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Redis RESP 协议兼容层：让 redis-cli 直连 Talon KV 引擎。
//!
//! 支持命令子集：GET/SET/DEL/MGET/MSET/EXISTS/EXPIRE/TTL/KEYS/INCR/DECR/PING/INFO。
//! RESP2 协议解析（Simple String / Error / Integer / Bulk String / Array）。

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::Error;
use crate::Talon;

/// Redis 兼容服务器。
pub struct RedisServer {
    db: Arc<Talon>,
    addr: String,
}

impl RedisServer {
    /// 创建 Redis 兼容服务器。
    pub fn new(db: Arc<Talon>, addr: String) -> Self {
        RedisServer { db, addr }
    }

    /// 启动监听（阻塞）。
    pub fn run(&self, stop: Arc<AtomicBool>) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.addr)?;
        listener.set_nonblocking(true)?;
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let db = Arc::clone(&self.db);
                    std::thread::spawn(move || {
                        let _ = handle_client(stream, &db);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
        Ok(())
    }
}

/// 处理单个 Redis 客户端连接。
fn handle_client(stream: std::net::TcpStream, db: &Talon) -> Result<(), Error> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    loop {
        let cmd = match parse_command(&mut reader) {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        if cmd.is_empty() {
            continue;
        }
        let response = execute_command(db, &cmd);
        writer.write_all(response.as_bytes())?;
        writer.flush()?;
    }
}

/// RESP 协议安全限制。
const MAX_BULK_LEN: usize = 64 * 1024 * 1024; // 64 MB
const MAX_ARRAY_COUNT: usize = 1024;

/// 解析 RESP 命令（支持 inline 和 RESP 数组格式）。
fn parse_command(reader: &mut BufReader<std::net::TcpStream>) -> Result<Vec<String>, Error> {
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .map_err(|e| Error::Protocol(e.to_string()))?;
    if n == 0 {
        return Err(Error::Protocol("connection closed".into()));
    }
    let line = line.trim_end();
    if line.starts_with('*') {
        // RESP 数组格式: *N\r\n$len\r\ndata\r\n...
        let count: usize = line
            .strip_prefix('*')
            .unwrap_or("0")
            .parse()
            .map_err(|_| Error::Protocol("invalid array count".into()))?;
        if count > MAX_ARRAY_COUNT {
            return Err(Error::Protocol(format!(
                "array count {} exceeds limit {}",
                count, MAX_ARRAY_COUNT
            )));
        }
        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let mut header = String::new();
            reader
                .read_line(&mut header)
                .map_err(|e| Error::Protocol(e.to_string()))?;
            let header = header.trim_end();
            if !header.starts_with('$') {
                return Err(Error::Protocol("expected bulk string".into()));
            }
            let len: usize = header[1..]
                .parse()
                .map_err(|_| Error::Protocol("invalid bulk length".into()))?;
            if len > MAX_BULK_LEN {
                return Err(Error::Protocol(format!(
                    "bulk length {} exceeds limit {}",
                    len, MAX_BULK_LEN
                )));
            }
            let mut buf = vec![0u8; len + 2]; // +2 for \r\n
            reader
                .read_exact(&mut buf)
                .map_err(|e| Error::Protocol(e.to_string()))?;
            args.push(String::from_utf8_lossy(&buf[..len]).to_string());
        }
        Ok(args)
    } else {
        // Inline 格式: COMMAND arg1 arg2 ...
        Ok(line.split_whitespace().map(|s| s.to_string()).collect())
    }
}

/// 执行 Redis 命令，返回 RESP 格式响应。
fn execute_command(db: &Talon, args: &[String]) -> String {
    if args.is_empty() {
        return resp_err("ERR empty command");
    }
    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "PING" => resp_simple("PONG"),
        "ECHO" => {
            if args.len() < 2 {
                resp_err("ERR wrong number of arguments")
            } else {
                resp_bulk(&args[1])
            }
        }
        "SET" => cmd_set(db, args),
        "GET" => cmd_get(db, args),
        "DEL" => cmd_del(db, args),
        "EXISTS" => cmd_exists(db, args),
        "EXPIRE" => cmd_expire(db, args),
        "TTL" => cmd_ttl(db, args),
        "INCR" => cmd_incr(db, args),
        "DECR" => cmd_decr(db, args),
        "MGET" => cmd_mget(db, args),
        "MSET" => cmd_mset(db, args),
        "KEYS" => cmd_keys(db, args),
        "DBSIZE" => cmd_dbsize(db),
        "INFO" => resp_bulk("# Talon Redis-compatible server\r\nredis_version:7.0.0-talon\r\n"),
        "COMMAND" => resp_simple("OK"),
        "QUIT" => resp_simple("OK"),
        _ => resp_err(&format!("ERR unknown command '{}'", cmd)),
    }
}

fn cmd_set(db: &Talon, args: &[String]) -> String {
    if args.len() < 3 {
        return resp_err("ERR wrong number of arguments for 'set' command");
    }
    let key = args[1].as_bytes();
    let value = args[2].as_bytes();
    // 解析可选 EX/PX TTL
    let mut ttl_secs = None;
    let mut i = 3;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "EX" if i + 1 < args.len() => {
                ttl_secs = args[i + 1].parse().ok();
                i += 2;
            }
            "PX" if i + 1 < args.len() => {
                ttl_secs = args[i + 1].parse::<u64>().ok().map(|ms| ms / 1000);
                i += 2;
            }
            _ => i += 1,
        }
    }
    match db.kv() {
        Ok(kv) => match kv.set(key, value, ttl_secs) {
            Ok(()) => resp_simple("OK"),
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_get(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'get' command");
    }
    match db.kv_read() {
        Ok(kv) => match kv.get(args[1].as_bytes()) {
            Ok(Some(v)) => resp_bulk(&String::from_utf8_lossy(&v)),
            Ok(None) => resp_null(),
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_del(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'del' command");
    }
    let mut count = 0i64;
    match db.kv() {
        Ok(kv) => {
            for key in &args[1..] {
                if kv.exists(key.as_bytes()).unwrap_or(false) && kv.del(key.as_bytes()).is_ok() {
                    count += 1;
                }
            }
            resp_integer(count)
        }
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_exists(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'exists' command");
    }
    match db.kv_read() {
        Ok(kv) => {
            let mut count = 0i64;
            for key in &args[1..] {
                if kv.exists(key.as_bytes()).unwrap_or(false) {
                    count += 1;
                }
            }
            resp_integer(count)
        }
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_expire(db: &Talon, args: &[String]) -> String {
    if args.len() < 3 {
        return resp_err("ERR wrong number of arguments for 'expire' command");
    }
    let secs: u64 = match args[2].parse() {
        Ok(s) => s,
        Err(_) => return resp_err("ERR value is not an integer"),
    };
    match db.kv() {
        Ok(kv) => {
            // Redis: EXPIRE 对不存在的 key 返回 0
            if !kv.exists(args[1].as_bytes()).unwrap_or(false) {
                return resp_integer(0);
            }
            match kv.expire(args[1].as_bytes(), secs) {
                Ok(()) => resp_integer(1),
                Err(e) => resp_err(&e.to_string()),
            }
        }
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_ttl(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'ttl' command");
    }
    match db.kv_read() {
        Ok(kv) => match kv.ttl(args[1].as_bytes()) {
            Ok(Some(t)) => resp_integer(t as i64),
            Ok(None) => resp_integer(-1),
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_incr(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'incr' command");
    }
    match db.kv() {
        Ok(kv) => match kv.incr(args[1].as_bytes()) {
            Ok(v) => resp_integer(v),
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_decr(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'decr' command");
    }
    match db.kv() {
        Ok(kv) => match kv.decrby(args[1].as_bytes(), 1) {
            Ok(v) => resp_integer(v),
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_mget(db: &Talon, args: &[String]) -> String {
    if args.len() < 2 {
        return resp_err("ERR wrong number of arguments for 'mget' command");
    }
    let keys: Vec<&[u8]> = args[1..].iter().map(|s| s.as_bytes()).collect();
    match db.kv_read() {
        Ok(kv) => match kv.mget(&keys) {
            Ok(vals) => {
                use std::fmt::Write;
                let mut resp = String::with_capacity(4 + 10 + vals.len() * 32);
                resp.push('*');
                let _ = write!(resp, "{}", vals.len());
                resp.push_str("\r\n");
                for v in &vals {
                    match v {
                        Some(data) => {
                            let s = String::from_utf8_lossy(data);
                            resp.push('$');
                            let _ = write!(resp, "{}", s.len());
                            resp.push_str("\r\n");
                            resp.push_str(&s);
                            resp.push_str("\r\n");
                        }
                        None => resp.push_str("$-1\r\n"),
                    }
                }
                resp
            }
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_mset(db: &Talon, args: &[String]) -> String {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return resp_err("ERR wrong number of arguments for 'mset' command");
    }
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    let mut i = 1;
    while i + 1 < args.len() {
        keys.push(args[i].as_bytes());
        vals.push(args[i + 1].as_bytes());
        i += 2;
    }
    match db.kv() {
        Ok(kv) => match kv.mset(&keys, &vals) {
            Ok(()) => resp_simple("OK"),
            Err(e) => resp_err(&e.to_string()),
        },
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_keys(db: &Talon, args: &[String]) -> String {
    let pattern = if args.len() >= 2 { &args[1] } else { "*" };
    match db.kv_read() {
        Ok(kv) => {
            let keys = if pattern == "*" {
                kv.keys_prefix(b"").unwrap_or_default()
            } else if pattern.ends_with('*') && !pattern[..pattern.len() - 1].contains('*') {
                let prefix = &pattern[..pattern.len() - 1];
                kv.keys_prefix(prefix.as_bytes()).unwrap_or_default()
            } else {
                kv.keys_match(pattern.as_bytes()).unwrap_or_default()
            };
            use std::fmt::Write;
            let mut resp = String::with_capacity(4 + 10 + keys.len() * 32);
            resp.push('*');
            let _ = write!(resp, "{}", keys.len());
            resp.push_str("\r\n");
            for k in &keys {
                let s = String::from_utf8_lossy(k);
                resp.push('$');
                let _ = write!(resp, "{}", s.len());
                resp.push_str("\r\n");
                resp.push_str(&s);
                resp.push_str("\r\n");
            }
            resp
        }
        Err(e) => resp_err(&e.to_string()),
    }
}

fn cmd_dbsize(db: &Talon) -> String {
    match db.kv_read() {
        Ok(kv) => {
            let n = kv.key_count().unwrap_or(0);
            resp_integer(n as i64)
        }
        Err(e) => resp_err(&e.to_string()),
    }
}

// ── RESP 格式化（对标 mini-redis：write! 直接写入预分配缓冲区，避免逐行 format!）──

fn resp_simple(s: &str) -> String {
    let mut buf = String::with_capacity(1 + s.len() + 2);
    buf.push('+');
    buf.push_str(s);
    buf.push_str("\r\n");
    buf
}

fn resp_err(s: &str) -> String {
    let mut buf = String::with_capacity(1 + s.len() + 2);
    buf.push('-');
    buf.push_str(s);
    buf.push_str("\r\n");
    buf
}

fn resp_integer(n: i64) -> String {
    use std::fmt::Write;
    let mut buf = String::with_capacity(24);
    buf.push(':');
    let _ = write!(buf, "{}", n);
    buf.push_str("\r\n");
    buf
}

fn resp_bulk(s: &str) -> String {
    use std::fmt::Write;
    let mut buf = String::with_capacity(1 + 10 + 2 + s.len() + 2);
    buf.push('$');
    let _ = write!(buf, "{}", s.len());
    buf.push_str("\r\n");
    buf.push_str(s);
    buf.push_str("\r\n");
    buf
}

fn resp_null() -> String {
    "$-1\r\n".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp_format_simple() {
        assert_eq!(resp_simple("OK"), "+OK\r\n");
        assert_eq!(resp_err("ERR test"), "-ERR test\r\n");
        assert_eq!(resp_integer(42), ":42\r\n");
        assert_eq!(resp_bulk("hello"), "$5\r\nhello\r\n");
        assert_eq!(resp_null(), "$-1\r\n");
    }

    #[test]
    fn execute_ping() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Talon::open(dir.path()).unwrap();
        let r = execute_command(&db, &["PING".into()]);
        assert_eq!(r, "+PONG\r\n");
    }

    #[test]
    fn execute_set_get() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Talon::open(dir.path()).unwrap();
        let r = execute_command(&db, &["SET".into(), "k1".into(), "v1".into()]);
        assert_eq!(r, "+OK\r\n");
        let r = execute_command(&db, &["GET".into(), "k1".into()]);
        assert_eq!(r, "$2\r\nv1\r\n");
    }

    #[test]
    fn execute_del_exists() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Talon::open(dir.path()).unwrap();
        execute_command(&db, &["SET".into(), "k1".into(), "v1".into()]);
        let r = execute_command(&db, &["EXISTS".into(), "k1".into()]);
        assert_eq!(r, ":1\r\n");
        let r = execute_command(&db, &["DEL".into(), "k1".into()]);
        assert_eq!(r, ":1\r\n");
        let r = execute_command(&db, &["EXISTS".into(), "k1".into()]);
        assert_eq!(r, ":0\r\n");
    }

    #[test]
    fn execute_incr_decr() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Talon::open(dir.path()).unwrap();
        let r = execute_command(&db, &["INCR".into(), "counter".into()]);
        assert_eq!(r, ":1\r\n");
        let r = execute_command(&db, &["INCR".into(), "counter".into()]);
        assert_eq!(r, ":2\r\n");
        let r = execute_command(&db, &["DECR".into(), "counter".into()]);
        assert_eq!(r, ":1\r\n");
    }

    #[test]
    fn execute_mset_mget() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Talon::open(dir.path()).unwrap();
        let r = execute_command(
            &db,
            &[
                "MSET".into(),
                "a".into(),
                "1".into(),
                "b".into(),
                "2".into(),
            ],
        );
        assert_eq!(r, "+OK\r\n");
        let r = execute_command(&db, &["MGET".into(), "a".into(), "b".into(), "c".into()]);
        assert!(r.starts_with("*3\r\n"));
        assert!(r.contains("$1\r\n1\r\n"));
        assert!(r.contains("$1\r\n2\r\n"));
        assert!(r.contains("$-1\r\n"));
    }

    #[test]
    fn execute_unknown_command() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Talon::open(dir.path()).unwrap();
        let r = execute_command(&db, &["ZADD".into()]);
        assert!(r.starts_with("-ERR unknown command"));
    }
}
