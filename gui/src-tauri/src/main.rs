/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Talon Manager — Tauri GUI 后端。
//!
//! 支持两种连接模式：
//! 1. TCP 模式 — 通过 TCP 连接远程 Talon Server
//! 2. 嵌入式模式 — 直接打开本地数据库目录（Talon::open）

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use tauri::State;

/// 连接模式：TCP 远程 / 嵌入式本地。
enum ConnMode {
    /// TCP 连接到远程 Talon Server。
    Tcp(TcpStream),
    /// 嵌入式模式：直接打开本地数据库目录。
    Embedded(talon::Talon),
}

/// 全局连接状态。
struct AppState {
    mode: Mutex<Option<ConnMode>>,
    label: Mutex<String>,
}

impl AppState {
    /// 统一命令执行入口：根据当前连接模式分发。
    fn dispatch(&self, cmd: &str) -> String {
        let mut guard = self.mode.lock().unwrap();
        let mode = match guard.as_mut() {
            Some(m) => m,
            None => return r#"{"ok":false,"error":"未连接，请先连接服务器或打开数据库"}"#.to_string(),
        };
        match mode {
            ConnMode::Embedded(db) => talon::execute_cmd(db, cmd),
            ConnMode::Tcp(stream) => {
                if let Err(e) = send_frame(stream, cmd.as_bytes()) {
                    *guard = None;
                    return format!(r#"{{"ok":false,"error":"连接已断开: {}。请重新连接。"}}"#, e);
                }
                match recv_frame(stream) {
                    Ok(resp) => resp,
                    Err(e) => {
                        *guard = None;
                        format!(r#"{{"ok":false,"error":"连接已断开: {}。请重新连接。"}}"#, e)
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ConnectParams {
    url: String,
}

#[derive(Serialize)]
struct ConnectResult {
    ok: bool,
    message: String,
}

// ── Tauri Commands ──

/// 打开本地数据库目录（嵌入式模式）。
#[tauri::command]
fn open_database(path: String, state: State<AppState>) -> ConnectResult {
    // 校验路径
    let p = std::path::Path::new(&path);
    if !p.exists() {
        // 父目录存在则允许创建新库
        if let Some(parent) = p.parent() {
            if !parent.exists() {
                return ConnectResult {
                    ok: false,
                    message: format!("路径不存在: {}", path),
                };
            }
        }
    }
    if p.exists() && p.is_file() {
        return ConnectResult {
            ok: false,
            message: "请选择一个目录，不是文件".into(),
        };
    }
    // 如果已有连接，先关闭
    *state.mode.lock().unwrap() = None;
    // 打开数据库
    match talon::Talon::open(&path) {
        Ok(db) => {
            *state.mode.lock().unwrap() = Some(ConnMode::Embedded(db));
            let label = p.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&path)
                .to_string();
            *state.label.lock().unwrap() = label.clone();
            ConnectResult {
                ok: true,
                message: format!("已打开本地数据库: {}", label),
            }
        }
        Err(e) => ConnectResult {
            ok: false,
            message: format!("打开数据库失败: {}", e),
        },
    }
}

/// 连接到 Talon Server（TCP 模式）。
#[tauri::command]
fn connect(params: ConnectParams, state: State<AppState>) -> ConnectResult {
    let parts = parse_url(&params.url);
    match parts {
        Some((host, port, token)) => {
            let addr = format!("{}:{}", host, port);
            match TcpStream::connect(&addr) {
                Ok(mut stream) => {
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                        .ok();
                    stream
                        .set_write_timeout(Some(std::time::Duration::from_secs(10)))
                        .ok();
                    // 认证
                    if let Some(tok) = token {
                        let auth = format!(r#"{{"auth":"{}"}}"#, tok);
                        if let Err(e) = send_frame(&mut stream, auth.as_bytes()) {
                            return ConnectResult {
                                ok: false,
                                message: format!("认证发送失败: {}", e),
                            };
                        }
                        match recv_frame(&mut stream) {
                            Ok(resp) => {
                                if resp.contains("auth failed") {
                                    return ConnectResult {
                                        ok: false,
                                        message: "认证失败: token 错误".into(),
                                    };
                                }
                            }
                            Err(e) => {
                                return ConnectResult {
                                    ok: false,
                                    message: format!("认证响应失败: {}", e),
                                };
                            }
                        }
                    }
                    // 关闭旧连接并设置新连接（单次加锁）
                    *state.mode.lock().unwrap() = Some(ConnMode::Tcp(stream));
                    *state.label.lock().unwrap() = params.url.clone();
                    ConnectResult {
                        ok: true,
                        message: format!("已连接到 {}", addr),
                    }
                }
                Err(e) => ConnectResult {
                    ok: false,
                    message: format!("连接失败: {}", e),
                },
            }
        }
        None => ConnectResult {
            ok: false,
            message: "连接字符串格式错误".into(),
        },
    }
}

/// 断开连接 / 关闭数据库。
#[tauri::command]
fn disconnect(state: State<AppState>) -> ConnectResult {
    *state.mode.lock().unwrap() = None;
    *state.label.lock().unwrap() = String::new();
    ConnectResult {
        ok: true,
        message: "已断开".into(),
    }
}

/// 执行命令（通用 JSON 协议）。
#[tauri::command]
fn execute(cmd: String, state: State<AppState>) -> String {
    state.dispatch(&cmd)
}

/// 执行 SQL 快捷方法。
#[tauri::command]
fn exec_sql(sql: String, state: State<AppState>) -> String {
    let cmd = serde_json::json!({
        "module": "sql",
        "action": "query",
        "params": { "sql": sql }
    });
    state.dispatch(&cmd.to_string())
}

/// KV 操作快捷方法。
#[tauri::command]
fn exec_kv(action: String, params: serde_json::Value, state: State<AppState>) -> String {
    let cmd = serde_json::json!({
        "module": "kv",
        "action": action,
        "params": params
    });
    state.dispatch(&cmd.to_string())
}

/// MQ 操作快捷方法。
#[tauri::command]
fn exec_mq(action: String, params: serde_json::Value, state: State<AppState>) -> String {
    let cmd = serde_json::json!({
        "module": "mq",
        "action": action,
        "params": params
    });
    state.dispatch(&cmd.to_string())
}

/// 获取数据库 schema 信息（表列表 + 每表列定义），供 SQL 智能提示使用。
#[tauri::command]
fn get_schema_info(state: State<AppState>) -> String {
    // 1. SHOW TABLES
    let tables_cmd = serde_json::json!({
        "module": "sql", "action": "query",
        "params": { "sql": "SHOW TABLES" }
    });
    let tables_resp = state.dispatch(&tables_cmd.to_string());
    let tables_val: serde_json::Value = match serde_json::from_str(&tables_resp) {
        Ok(v) => v,
        Err(_) => return r#"{"ok":false,"error":"解析 SHOW TABLES 结果失败"}"#.to_string(),
    };
    if tables_val.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return tables_resp;
    }
    let rows = tables_val
        .pointer("/data/rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut table_names: Vec<String> = Vec::new();
    for row in &rows {
        let name = if let Some(arr) = row.as_array() {
            arr.first()
                .and_then(|v| v.get("Text").and_then(|t| t.as_str()))
                .or_else(|| arr.first().and_then(|v| v.as_str()))
                .unwrap_or("")
        } else {
            row.get("Text")
                .and_then(|t| t.as_str())
                .or_else(|| row.as_str())
                .unwrap_or("")
        };
        if !name.is_empty() {
            table_names.push(name.to_string());
        }
    }
    // 2. DESCRIBE 每个表
    let mut tables_info = Vec::new();
    for tname in &table_names {
        let desc_cmd = serde_json::json!({
            "module": "sql", "action": "query",
            "params": { "sql": format!("DESCRIBE `{}`", tname) }
        });
        let desc_resp = state.dispatch(&desc_cmd.to_string());
        let desc_val: serde_json::Value =
            serde_json::from_str(&desc_resp).unwrap_or(serde_json::json!({}));
        let mut columns = Vec::new();
        if let Some(desc_rows) = desc_val.pointer("/data/rows").and_then(|v| v.as_array()) {
            for drow in desc_rows {
                if let Some(arr) = drow.as_array() {
                    let col_name = arr
                        .first()
                        .and_then(|v| v.get("Text").and_then(|t| t.as_str()).or(v.as_str()))
                        .unwrap_or("")
                        .to_string();
                    let col_type = arr
                        .get(1)
                        .and_then(|v| v.get("Text").and_then(|t| t.as_str()).or(v.as_str()))
                        .unwrap_or("")
                        .to_string();
                    if !col_name.is_empty() {
                        columns.push(serde_json::json!({"name": col_name, "type": col_type}));
                    }
                }
            }
        }
        tables_info.push(serde_json::json!({"name": tname, "columns": columns}));
    }
    serde_json::json!({"ok": true, "data": {"tables": tables_info}}).to_string()
}

// ── TCP 帧协议 ──

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

/// 简易 URL 解析：talon://[:token@]host:port
fn parse_url(url: &str) -> Option<(String, u16, Option<String>)> {
    let rest = url.strip_prefix("talon://")?;
    let (userinfo, hostport) = if let Some(at) = rest.rfind('@') {
        (Some(&rest[..at]), &rest[at + 1..])
    } else {
        (None, rest)
    };
    let token = userinfo.and_then(|ui| {
        if let Some(colon) = ui.find(':') {
            let pass = &ui[colon + 1..];
            if pass.is_empty() {
                None
            } else {
                Some(pass.to_string())
            }
        } else {
            None
        }
    });
    // 去掉 query string
    let hostport = hostport.split('?').next().unwrap_or(hostport);
    let (host, port) = if let Some(colon) = hostport.rfind(':') {
        let h = &hostport[..colon];
        let p = hostport[colon + 1..].parse::<u16>().ok()?;
        (h.to_string(), p)
    } else {
        (hostport.to_string(), 7720)
    };
    Some((host, port, token))
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState {
            mode: Mutex::new(None),
            label: Mutex::new(String::new()),
        })
        .invoke_handler(tauri::generate_handler![
            connect,
            disconnect,
            open_database,
            execute,
            exec_sql,
            exec_kv,
            exec_mq,
            get_schema_info,
        ])
        .run(tauri::generate_context!())
        .expect("启动 Talon Manager 失败");
}
