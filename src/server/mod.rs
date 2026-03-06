/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Server 模式：TCP 二进制协议 + HTTP/JSON API。
//!
//! M2.3 实现；依赖所有引擎模块。
//! 使用 std::net 实现，不引入外部 HTTP 框架，保持零外部依赖原则。

mod connection_string;
mod handlers;
mod handlers_fts;
mod handlers_geo;
mod handlers_graph;
mod handlers_ts;
mod handlers_vec;
mod http;
mod protocol;
pub mod redis;
mod tcp;

pub use connection_string::{Protocol, TalonUrl};
pub use http::HttpServer;
pub use protocol::{Request, Response};
pub use tcp::TcpServer;

/// Server 配置。
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// HTTP 监听地址，如 "127.0.0.1:7720"。
    pub http_addr: String,
    /// 认证 token；None 表示不启用认证。
    pub auth_token: Option<String>,
    /// 最大并发连接数；0 表示无限制。默认 256。
    pub max_connections: usize,
    /// 自动持久化间隔（秒）；0 表示不自动持久化。默认 30。
    pub auto_persist_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            http_addr: "127.0.0.1:7720".to_string(),
            auth_token: None,
            max_connections: 256,
            auto_persist_secs: 30,
        }
    }
}
