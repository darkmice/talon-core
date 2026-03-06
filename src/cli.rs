/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Talon CLI 客户端 — 交互式数据库管理 shell。
//!
//! 命令构建函数拆分到 cli_commands.rs。
//!
//! 用法：
//!   talon-cli "talon://:token@host:port"
//!   talon-cli --url "talon://localhost:7720"
//!   TALON_URL="talon://localhost:7720" talon-cli
//!
//! 交互式命令：
//!   SQL 语句直接输入（以 ; 结尾）
//!   `:kv get <key>`          — KV 读取
//!   `:kv set <key> <value>`  — KV 写入
//!   `:kv del <key>`          — KV 删除
//!   `:kv keys <prefix>`      — KV 列出 key
//!   `:mq len <topic>`        — MQ 队列长度
//!   :mq topics             — MQ 列出 topic
//!   :stats                 — 数据库统计
//!   :help                  — 显示帮助
//!   :quit / :exit          — 退出

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url_str = parse_cli_args(&args);
    let url = match talon::TalonUrl::parse(&url_str) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("连接字符串解析失败: {}", e);
            std::process::exit(1);
        }
    };
    if url.is_embedded() {
        eprintln!("CLI 客户端不支持嵌入式模式，请使用 talon://host:port 格式");
        std::process::exit(1);
    }
    let addr = url.addr();
    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("连接失败 {}: {}", addr, e);
            std::process::exit(1);
        }
    };
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(url.timeout_secs)))
        .ok();

    // 认证
    if let Some(ref token) = url.password {
        let auth_cmd = format!(r#"{{"auth":"{}"}}"#, token);
        if let Err(e) = send_frame(&mut stream, auth_cmd.as_bytes()) {
            eprintln!("发送认证帧失败: {}", e);
            std::process::exit(1);
        }
        match recv_frame(&mut stream) {
            Ok(resp) => {
                if resp.contains("auth failed") {
                    eprintln!("认证失败：token 错误");
                    std::process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("认证响应读取失败: {}", e);
                std::process::exit(1);
            }
        }
    }

    println!("Talon CLI — 已连接到 {}", url);
    println!("输入 SQL 语句或 :help 查看命令列表。:quit 退出。");
    println!();
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut lines = reader.lines();
    loop {
        print!("talon> ");
        std::io::stdout().flush().ok();
        let line = match lines.next() {
            Some(Ok(l)) => l,
            _ => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        // 内置命令
        if trimmed.starts_with(':') {
            match handle_builtin(trimmed, &mut stream) {
                BuiltinResult::Continue => continue,
                BuiltinResult::Quit => break,
                BuiltinResult::Error(e) => {
                    eprintln!("错误: {}", e);
                    continue;
                }
            }
        }
        // SQL 语句
        let sql = trimmed.trim_end_matches(';');
        let cmd = serde_json::json!({
            "module": "sql",
            "action": "query",
            "params": { "sql": sql }
        });
        match send_and_recv(&mut stream, &cmd.to_string()) {
            Ok(resp) => print_response(&resp),
            Err(e) => eprintln!("通信错误: {}", e),
        }
    }

    println!("\n再见！");
}
fn parse_cli_args(args: &[String]) -> String {
    // 优先级：--url 参数 > 位置参数 > TALON_URL 环境变量
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                i += 1;
                if i < args.len() {
                    return args[i].clone();
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            s if s.starts_with("talon://") => {
                return s.to_string();
            }
            _ => {}
        }
        i += 1;
    }
    // 环境变量
    if let Ok(url) = std::env::var("TALON_URL") {
        return url;
    }
    // 默认
    "talon://localhost:7720".to_string()
}
fn print_help() {
    println!("Talon CLI — 交互式数据库管理客户端");
    println!();
    println!("用法:");
    println!("  talon-cli \"talon://:token@host:port\"");
    println!("  talon-cli --url \"talon://localhost:7720\"");
    println!("  TALON_URL=\"talon://localhost:7720\" talon-cli");
    println!();
    println!("交互式命令:");
    println!("  <SQL>;              执行 SQL（SELECT/INSERT/UPDATE/DELETE/...）");
    println!("  :kv get <key>       KV 读取");
    println!("  :kv set <key> <val> KV 写入");
    println!("  :kv del <key>       KV 删除");
    println!("  :kv keys [prefix]   KV 列出 key");
    println!("  :kv incr <key>      KV 原子自增");
    println!("  :kv incrby <key> <n> KV 自增 N");
    println!("  :kv decrby <key> <n> KV 自减 N");
    println!("  :kv setnx <key> <v> KV 不存在时写入");
    println!("  :mq topics          MQ 列出 topic");
    println!("  :mq len <topic>     MQ 队列长度");
    println!("  :mq pub <topic> <msg>  MQ 发布消息");
    println!("  :vec count <name>      向量索引数量");
    println!("  :vec search <name> <k> <v1,v2,...>  向量搜索");
    println!("  :ts query <name>       时序查询");
    println!("  :ts list               列出时序");
    println!("  :ai sessions           列出 session");
    println!("  :ai session <id>       查看 session");
    println!("  :ai history <sid> [n]  查看消息历史");
    println!("  :ai memory count       记忆数量");
    println!("  :ai docs               列出 RAG 文档");
    println!("  :ai doc count          RAG 文档数");
    println!("  :graph create <name>   创建图");
    println!("  :graph add_vertex <g> <label>  添加节点");
    println!("  :graph get_vertex <g> <id>     查询节点");
    println!("  :graph add_edge <g> <from> <to> <label>  添加边");
    println!("  :graph neighbors <g> <id> [dir] 邻居查询");
    println!("  :graph bfs <g> <start> [depth]  BFS 遍历");
    println!("  :graph count <g>       节点/边计数");
    println!("  :graph pagerank <g> [limit]     PageRank");
    println!("  :geo search <name> <lng> <lat> <radius>  GEO 圆形搜索");
    println!("  :fts search <name> <query>      全文搜索");
    println!("  :stats                 数据库统计信息");
    println!("  :help                  显示本帮助");
    println!("  :quit / :exit          退出");
}
enum BuiltinResult {
    Continue,
    Quit,
    Error(String),
}
fn handle_builtin(input: &str, stream: &mut TcpStream) -> BuiltinResult {
    let parts: Vec<&str> = input.splitn(4, ' ').collect();
    let cmd = parts[0];
    match cmd {
        ":quit" | ":exit" | ":q" => BuiltinResult::Quit,
        ":help" | ":h" | ":?" => {
            print_help();
            BuiltinResult::Continue
        }
        ":stats" => {
            let req = r#"{"module":"sql","action":"query","params":{"sql":"SHOW TABLES"}}"#;
            match send_and_recv(stream, req) {
                Ok(resp) => {
                    print_response(&resp);
                    BuiltinResult::Continue
                }
                Err(e) => BuiltinResult::Error(e),
            }
        }
        ":kv" => {
            if parts.len() < 2 {
                return BuiltinResult::Error(
                    ":kv 需要子命令：get/set/del/keys/incr/incrby/decrby/setnx".into(),
                );
            }
            let sub = parts[1];
            let json = match sub {
                "get" => {
                    if parts.len() < 3 {
                        return BuiltinResult::Error(":kv get <key>".into());
                    }
                    serde_json::json!({
                        "module": "kv", "action": "get",
                        "params": { "key": parts[2] }
                    })
                }
                "set" => {
                    if parts.len() < 4 {
                        return BuiltinResult::Error(":kv set <key> <value>".into());
                    }
                    serde_json::json!({
                        "module": "kv", "action": "set",
                        "params": { "key": parts[2], "value": parts[3] }
                    })
                }
                "del" => {
                    if parts.len() < 3 {
                        return BuiltinResult::Error(":kv del <key>".into());
                    }
                    serde_json::json!({
                        "module": "kv", "action": "del",
                        "params": { "key": parts[2] }
                    })
                }
                "keys" => {
                    let prefix = if parts.len() >= 3 { parts[2] } else { "" };
                    serde_json::json!({
                        "module": "kv", "action": "keys",
                        "params": { "prefix": prefix }
                    })
                }
                "incr" => {
                    if parts.len() < 3 {
                        return BuiltinResult::Error(":kv incr <key>".into());
                    }
                    serde_json::json!({
                        "module": "kv", "action": "incr",
                        "params": { "key": parts[2] }
                    })
                }
                "incrby" => {
                    if parts.len() < 4 {
                        return BuiltinResult::Error(":kv incrby <key> <delta>".into());
                    }
                    let delta: i64 = parts[3].parse().unwrap_or(1);
                    serde_json::json!({
                        "module": "kv", "action": "incrby",
                        "params": { "key": parts[2], "delta": delta }
                    })
                }
                "decrby" => {
                    if parts.len() < 4 {
                        return BuiltinResult::Error(":kv decrby <key> <delta>".into());
                    }
                    let delta: i64 = parts[3].parse().unwrap_or(1);
                    serde_json::json!({
                        "module": "kv", "action": "decrby",
                        "params": { "key": parts[2], "delta": delta }
                    })
                }
                "setnx" => {
                    if parts.len() < 4 {
                        return BuiltinResult::Error(":kv setnx <key> <value>".into());
                    }
                    serde_json::json!({
                        "module": "kv", "action": "setnx",
                        "params": { "key": parts[2], "value": parts[3] }
                    })
                }
                _ => {
                    return BuiltinResult::Error(format!("未知 KV 子命令: {}", sub));
                }
            };
            match send_and_recv(stream, &json.to_string()) {
                Ok(resp) => {
                    print_response(&resp);
                    BuiltinResult::Continue
                }
                Err(e) => BuiltinResult::Error(e),
            }
        }
        ":mq" => {
            if parts.len() < 2 {
                return BuiltinResult::Error(":mq 需要子命令：topics/len/pub".into());
            }
            let sub = parts[1];
            let json = match sub {
                "topics" => {
                    serde_json::json!({
                        "module": "mq", "action": "list_topics",
                        "params": {}
                    })
                }
                "len" => {
                    if parts.len() < 3 {
                        return BuiltinResult::Error(":mq len <topic>".into());
                    }
                    serde_json::json!({
                        "module": "mq", "action": "len",
                        "params": { "topic": parts[2] }
                    })
                }
                "pub" => {
                    if parts.len() < 4 {
                        return BuiltinResult::Error(":mq pub <topic> <message>".into());
                    }
                    serde_json::json!({
                        "module": "mq", "action": "publish",
                        "params": { "topic": parts[2], "payload": parts[3] }
                    })
                }
                _ => {
                    return BuiltinResult::Error(format!("未知 MQ 子命令: {}", sub));
                }
            };
            match send_and_recv(stream, &json.to_string()) {
                Ok(resp) => {
                    print_response(&resp);
                    BuiltinResult::Continue
                }
                Err(e) => BuiltinResult::Error(e),
            }
        }
        ":vec" => run_cmd(stream, cli_commands::build_vec(&parts)),
        ":ts" => run_cmd(stream, cli_commands::build_ts(&parts)),
        ":ai" => run_cmd(stream, cli_commands::build_ai(&parts)),
        ":graph" => run_cmd(stream, cli_commands::build_graph(&parts)),
        ":geo" => run_cmd(stream, cli_commands::build_geo(&parts)),
        ":fts" => run_cmd(stream, cli_commands::build_fts(&parts)),
        _ => BuiltinResult::Error(format!("未知命令: {}。输入 :help 查看帮助。", cmd)),
    }
}

fn run_cmd(stream: &mut TcpStream, r: Result<serde_json::Value, String>) -> BuiltinResult {
    match r {
        Err(e) => BuiltinResult::Error(e),
        Ok(json) => match send_and_recv(stream, &json.to_string()) {
            Ok(resp) => {
                print_response(&resp);
                BuiltinResult::Continue
            }
            Err(e) => BuiltinResult::Error(e),
        },
    }
}

mod cli_commands;
// ── TCP 帧协议（与 server/tcp.rs 一致）──
fn send_frame(stream: &mut TcpStream, data: &[u8]) -> Result<(), String> {
    let len = data.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .map_err(|e| e.to_string())?;
    stream.write_all(data).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())
}
fn recv_frame(stream: &mut TcpStream) -> Result<String, String> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("读取帧长度失败: {}", e))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(format!("帧过大: {} bytes", len));
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .map_err(|e| format!("读取帧数据失败: {}", e))?;
    String::from_utf8(buf).map_err(|e| format!("UTF-8 解码失败: {}", e))
}
fn send_and_recv(stream: &mut TcpStream, cmd: &str) -> Result<String, String> {
    send_frame(stream, cmd.as_bytes())?;
    recv_frame(stream)
}
fn print_response(resp: &str) {
    // 尝试解析 JSON 并美化输出
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(resp) {
        if let Some(false) = v.get("ok").and_then(|o| o.as_bool()) {
            if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
                eprintln!("错误: {}", err);
                return;
            }
        }
        if let Some(data) = v.get("data") {
            // SQL 行结果
            if let Some(rows) = data.get("rows").and_then(|r| r.as_array()) {
                if rows.is_empty() {
                    println!("(0 行)");
                } else {
                    for (i, row) in rows.iter().enumerate() {
                        if let Some(arr) = row.as_array() {
                            let cols: Vec<String> = arr.iter().map(format_value).collect();
                            println!("{:>4} | {}", i + 1, cols.join(" | "));
                        } else {
                            println!("{:>4} | {}", i + 1, row);
                        }
                    }
                    println!("({} 行)", rows.len());
                }
                return;
            }
            // KV/MQ 等简单响应
            println!(
                "{}",
                serde_json::to_string_pretty(data).unwrap_or_else(|_| resp.to_string())
            );
        } else {
            println!(
                "{}",
                serde_json::to_string_pretty(&v).unwrap_or_else(|_| resp.to_string())
            );
        }
    } else {
        println!("{}", resp);
    }
}

fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Object(map) => {
            // Talon Value 格式：{"Integer": 42} / {"Text": "hello"} / "Null"
            if let Some(v) = map.get("Integer").and_then(|v| v.as_i64()) {
                v.to_string()
            } else if let Some(v) = map.get("Float").and_then(|v| v.as_f64()) {
                format!("{}", v)
            } else if let Some(v) = map.get("Text").and_then(|v| v.as_str()) {
                v.to_string()
            } else if let Some(v) = map.get("Timestamp").and_then(|v| v.as_i64()) {
                v.to_string()
            } else if map.contains_key("Null") {
                "NULL".to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        }
        serde_json::Value::Array(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}
