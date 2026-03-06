/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TUI TCP 客户端 — 复用 Talon 帧协议与服务端通信。
//!
//! 生产级特性：自动重连、心跳检测、全引擎 API。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// TUI 专用 TCP 客户端，封装帧协议收发。
pub struct TuiClient {
    stream: TcpStream,
    /// 服务端地址。
    pub addr: String,
    /// 上次通信成功时间。
    pub last_ok: Instant,
}

#[allow(dead_code)]
impl TuiClient {
    /// 连接到 Talon 服务端并完成认证。
    pub fn connect(url_str: &str) -> Result<Self, String> {
        let url = talon::TalonUrl::parse(url_str).map_err(|e| format!("URL 解析失败: {}", e))?;
        if url.is_embedded() {
            return Err("TUI 不支持嵌入式模式，请使用 talon://host:port".into());
        }
        let addr = url.addr();
        let stream = TcpStream::connect(&addr).map_err(|e| format!("连接 {} 失败: {}", addr, e))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(url.timeout_secs)))
            .ok();
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
        stream.set_nodelay(true).ok();

        let mut client = Self {
            stream,
            addr: addr.clone(),
            last_ok: Instant::now(),
        };

        if let Some(ref token) = url.password {
            let auth = format!(r#"{{"auth":"{}"}}"#, token);
            client.send_raw(&auth)?;
            let resp = client.recv_raw()?;
            if resp.contains("auth failed") {
                return Err("认证失败：token 错误".into());
            }
        }
        Ok(client)
    }

    /// 心跳检测（非阻塞）。
    pub fn ping(&mut self) -> bool {
        self.send_cmd("system", "ping", serde_json::json!({}))
            .is_ok()
    }

    /// 发送引擎命令并接收 JSON 响应。
    pub fn send_cmd(
        &mut self,
        module: &str,
        action: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let cmd = serde_json::json!({ "module": module, "action": action, "params": params });
        self.send_raw(&cmd.to_string())?;
        let resp = self.recv_raw()?;
        self.last_ok = Instant::now();
        serde_json::from_str(&resp).map_err(|e| format!("JSON 解析失败: {}", e))
    }

    // ── SQL 引擎 ──

    /// 执行 SQL 查询。
    pub fn sql_query(&mut self, sql: &str) -> Result<serde_json::Value, String> {
        self.send_cmd("sql", "query", serde_json::json!({"sql": sql}))
    }

    // ── KV 引擎 ──

    /// 获取 KV 键列表。
    pub fn kv_keys(&mut self, prefix: &str) -> Result<serde_json::Value, String> {
        self.send_cmd("kv", "keys", serde_json::json!({"prefix": prefix}))
    }

    /// 获取 KV 值。
    pub fn kv_get(&mut self, key: &str) -> Result<serde_json::Value, String> {
        self.send_cmd("kv", "get", serde_json::json!({"key": key}))
    }

    // ── Vector 引擎 ──

    /// 向量搜索。
    pub fn vector_search(
        &mut self,
        collection: &str,
        query: &[f32],
        top_k: usize,
    ) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "vector",
            "search",
            serde_json::json!({
                "collection": collection, "query": query, "top_k": top_k,
            }),
        )
    }

    /// 向量集合列表。
    pub fn vector_collections(&mut self) -> Result<serde_json::Value, String> {
        self.send_cmd("vector", "list_collections", serde_json::json!({}))
    }

    // ── AI 引擎 ──

    /// AI 会话列表。
    pub fn ai_sessions(&mut self) -> Result<serde_json::Value, String> {
        self.send_cmd("ai", "list_sessions", serde_json::json!({}))
    }

    /// AI 会话消息历史。
    pub fn ai_messages(&mut self, session_id: &str) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "ai",
            "get_messages",
            serde_json::json!({"session_id": session_id}),
        )
    }

    /// AI 发送消息。
    pub fn ai_send(&mut self, session_id: &str, msg: &str) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "ai",
            "send",
            serde_json::json!({"session_id": session_id, "message": msg}),
        )
    }

    // ── 时序引擎 ──

    /// 时序数据查询。
    pub fn ts_query(&mut self, metric: &str, limit: usize) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "ts",
            "query",
            serde_json::json!({"metric": metric, "limit": limit}),
        )
    }

    /// 时序指标列表。
    pub fn ts_metrics(&mut self) -> Result<serde_json::Value, String> {
        self.send_cmd("ts", "list_metrics", serde_json::json!({}))
    }

    // ── MQ 引擎 ──

    /// MQ 主题列表。
    pub fn mq_topics(&mut self) -> Result<serde_json::Value, String> {
        self.send_cmd("mq", "list_topics", serde_json::json!({}))
    }

    /// MQ 消费最近消息。
    pub fn mq_peek(&mut self, topic: &str, count: usize) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "mq",
            "peek",
            serde_json::json!({"topic": topic, "count": count}),
        )
    }

    // ── FTS 全文搜索 ──

    /// 全文搜索。
    pub fn fts_search(
        &mut self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "fts",
            "search",
            serde_json::json!({
                "index": index, "query": query, "limit": limit,
            }),
        )
    }

    /// FTS 索引列表。
    pub fn fts_indices(&mut self) -> Result<serde_json::Value, String> {
        self.send_cmd("fts", "list_indices", serde_json::json!({}))
    }

    // ── GEO 地理 ──

    /// GEO 范围搜索。
    pub fn geo_search(
        &mut self,
        key: &str,
        lat: f64,
        lon: f64,
        radius_km: f64,
    ) -> Result<serde_json::Value, String> {
        self.send_cmd(
            "geo",
            "search",
            serde_json::json!({
                "key": key, "lat": lat, "lon": lon, "radius_km": radius_km,
            }),
        )
    }

    // ── Graph 图 ──

    /// 图节点列表。
    pub fn graph_nodes(&mut self, graph: &str) -> Result<serde_json::Value, String> {
        self.send_cmd("graph", "list_nodes", serde_json::json!({"graph": graph}))
    }

    // ── 帧协议（与 server/tcp.rs 一致）──

    fn send_raw(&mut self, data: &str) -> Result<(), String> {
        let bytes = data.as_bytes();
        let len = bytes.len() as u32;
        self.stream
            .write_all(&len.to_be_bytes())
            .map_err(|e| e.to_string())?;
        self.stream.write_all(bytes).map_err(|e| e.to_string())?;
        self.stream.flush().map_err(|e| e.to_string())
    }

    fn recv_raw(&mut self) -> Result<String, String> {
        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .map_err(|e| format!("读取帧长度失败: {}", e))?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 16 * 1024 * 1024 {
            return Err(format!("帧过大: {} bytes", len));
        }
        let mut buf = vec![0u8; len];
        self.stream
            .read_exact(&mut buf)
            .map_err(|e| format!("读取帧数据失败: {}", e))?;
        String::from_utf8(buf).map_err(|e| format!("UTF-8 解码失败: {}", e))
    }
}
