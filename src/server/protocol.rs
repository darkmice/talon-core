/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 请求/响应协议定义：JSON 格式，供 HTTP API 使用。

use serde::{Deserialize, Serialize};

/// 客户端请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// 命令类型：sql / kv / ts / mq / vector。
    pub cmd: String,
    /// 操作：如 "query", "set", "get", "publish" 等。
    pub action: String,
    /// 参数（JSON 对象）。
    #[serde(default)]
    pub params: serde_json::Value,
}

/// 服务端响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// 是否成功。
    pub ok: bool,
    /// 结果数据（成功时）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// 错误信息（失败时）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    /// 构造成功响应。
    pub fn ok(data: serde_json::Value) -> Self {
        Response {
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    /// 构造空成功响应。
    pub fn ok_empty() -> Self {
        Response {
            ok: true,
            data: None,
            error: None,
        }
    }

    /// 构造错误响应。
    pub fn err(msg: impl Into<String>) -> Self {
        Response {
            ok: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}
