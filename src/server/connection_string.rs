/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 连接字符串解析：`talon://[user:password@]host:port[?param=value&...]`
//!
//! 兼容嵌入式模式：`talon:///path/to/data`
//!
//! M105 实现；零外部依赖，手写 URI parser。

use crate::error::Error;

/// 解析后的 Talon 连接 URL。
#[derive(Debug, Clone, PartialEq)]
pub struct TalonUrl {
    /// 主机地址（嵌入式模式为空）。
    pub host: String,
    /// 端口号（默认 7720）。
    pub port: u16,
    /// 用户名（预留，当前不使用）。
    pub user: Option<String>,
    /// 密码 / auth token。
    pub password: Option<String>,
    /// 数据路径（嵌入式模式使用）。
    pub path: Option<String>,
    /// 传输协议。
    pub protocol: Protocol,
    /// 连接超时（秒）。
    pub timeout_secs: u64,
    /// 最大连接数。
    pub max_connections: usize,
    /// TLS 启用标志（预留）。
    pub tls: bool,
}

/// 传输协议。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Protocol {
    /// TCP 二进制协议（默认）。
    Tcp,
    /// HTTP/JSON API。
    Http,
}

impl Default for TalonUrl {
    fn default() -> Self {
        TalonUrl {
            host: "localhost".to_string(),
            port: 7720,
            user: None,
            password: None,
            path: None,
            protocol: Protocol::Tcp,
            timeout_secs: 30,
            max_connections: 256,
            tls: false,
        }
    }
}

impl TalonUrl {
    /// 从连接字符串解析。
    ///
    /// 支持格式：
    /// - `talon://host:port` — 最简远程连接
    /// - `talon://:token@host:port` — 带 auth token
    /// - `talon://user:pass@host:port` — 带用户名密码
    /// - `talon:///path/to/data` — 嵌入式模式（本地路径）
    /// - `talon://host:port?timeout=10&protocol=http` — 带参数
    pub fn parse(s: &str) -> Result<Self, Error> {
        let s = s.trim();
        if !s.starts_with("talon://") {
            return Err(Error::InvalidConnectionString(
                "连接字符串必须以 talon:// 开头".into(),
            ));
        }
        let rest = &s[8..]; // skip "talon://"

        // 嵌入式模式：talon:///path
        if rest.starts_with('/') {
            let (path, params) = split_query(rest);
            let mut url = TalonUrl {
                host: String::new(),
                path: Some(path.to_string()),
                ..Default::default()
            };
            apply_params(&mut url, params)?;
            return Ok(url);
        }

        // 分离 query string
        let (authority_path, params) = split_query(rest);

        // 分离 userinfo 和 host
        let (userinfo, hostport) = if let Some(at_pos) = authority_path.rfind('@') {
            (
                Some(&authority_path[..at_pos]),
                &authority_path[at_pos + 1..],
            )
        } else {
            (None, authority_path)
        };

        // 解析 user:password
        let mut url = TalonUrl::default();
        if let Some(ui) = userinfo {
            if let Some(colon) = ui.find(':') {
                let user = &ui[..colon];
                let pass = &ui[colon + 1..];
                if !user.is_empty() {
                    url.user = Some(url_decode(user));
                }
                if !pass.is_empty() {
                    url.password = Some(url_decode(pass));
                }
            } else if !ui.is_empty() {
                url.user = Some(url_decode(ui));
            }
        }

        // 解析 host:port[/path]
        let (hostport_str, path) = if let Some(slash) = hostport.find('/') {
            (&hostport[..slash], Some(&hostport[slash..]))
        } else {
            (hostport, None)
        };

        if let Some(colon) = hostport_str.rfind(':') {
            let host = &hostport_str[..colon];
            let port_str = &hostport_str[colon + 1..];
            if !host.is_empty() {
                url.host = host.to_string();
            }
            if !port_str.is_empty() {
                url.port = port_str.parse::<u16>().map_err(|_| {
                    Error::InvalidConnectionString(format!("无效端口号: {}", port_str))
                })?;
            }
        } else if !hostport_str.is_empty() {
            url.host = hostport_str.to_string();
        }

        if let Some(p) = path {
            if p.len() > 1 {
                url.path = Some(p[1..].to_string()); // skip leading /
            }
        }

        apply_params(&mut url, params)?;
        Ok(url)
    }

    /// 生成连接字符串（序列化回 URI 格式）。
    pub fn to_string_url(&self) -> String {
        let mut s = String::from("talon://");
        // userinfo
        if self.user.is_some() || self.password.is_some() {
            if let Some(ref u) = self.user {
                s.push_str(u);
            }
            s.push(':');
            if let Some(ref p) = self.password {
                s.push_str(p);
            }
            s.push('@');
        }
        // host:port or path
        if self.host.is_empty() {
            if let Some(ref p) = self.path {
                s.push('/');
                s.push_str(p);
            }
        } else {
            s.push_str(&self.host);
            s.push(':');
            s.push_str(&self.port.to_string());
            if let Some(ref p) = self.path {
                s.push('/');
                s.push_str(p);
            }
        }
        // params
        let mut params = Vec::new();
        if self.timeout_secs != 30 {
            params.push(format!("timeout={}", self.timeout_secs));
        }
        if self.max_connections != 256 {
            params.push(format!("max_connections={}", self.max_connections));
        }
        if self.tls {
            params.push("tls=true".to_string());
        }
        if self.protocol != Protocol::Tcp {
            params.push(format!(
                "protocol={}",
                match self.protocol {
                    Protocol::Http => "http",
                    Protocol::Tcp => "tcp",
                }
            ));
        }
        if !params.is_empty() {
            s.push('?');
            s.push_str(&params.join("&"));
        }
        s
    }

    /// 转换为 ServerConfig。
    pub fn to_server_config(&self) -> super::ServerConfig {
        super::ServerConfig {
            http_addr: format!("{}:{}", self.host, self.port),
            auth_token: self.password.clone(),
            max_connections: self.max_connections,
            auto_persist_secs: 30,
        }
    }

    /// 返回 `host:port` 格式的地址。
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// 是否为嵌入式模式（无远程 host）。
    pub fn is_embedded(&self) -> bool {
        self.host.is_empty()
    }
}

impl std::fmt::Display for TalonUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 显示时隐藏密码
        if self.is_embedded() {
            write!(f, "talon:///{}", self.path.as_deref().unwrap_or(""))
        } else if self.password.is_some() {
            write!(f, "talon://***@{}:{}", self.host, self.port)
        } else {
            write!(f, "talon://{}:{}", self.host, self.port)
        }
    }
}

/// 分离 query string：`host:port?k=v` → (`host:port`, `k=v`)
fn split_query(s: &str) -> (&str, &str) {
    if let Some(pos) = s.find('?') {
        (&s[..pos], &s[pos + 1..])
    } else {
        (s, "")
    }
}

/// 应用 query 参数到 TalonUrl。
fn apply_params(url: &mut TalonUrl, params: &str) -> Result<(), Error> {
    if params.is_empty() {
        return Ok(());
    }
    for pair in params.split('&') {
        let (key, val) = if let Some(eq) = pair.find('=') {
            (&pair[..eq], &pair[eq + 1..])
        } else {
            (pair, "true")
        };
        match key {
            "timeout" => {
                url.timeout_secs = val.parse().map_err(|_| {
                    Error::InvalidConnectionString(format!("无效 timeout: {}", val))
                })?;
            }
            "max_connections" => {
                url.max_connections = val.parse().map_err(|_| {
                    Error::InvalidConnectionString(format!("无效 max_connections: {}", val))
                })?;
            }
            "tls" => {
                url.tls = val == "true" || val == "1";
            }
            "protocol" => match val {
                "tcp" => url.protocol = Protocol::Tcp,
                "http" => url.protocol = Protocol::Http,
                _ => {
                    return Err(Error::InvalidConnectionString(format!(
                        "无效 protocol: {}（支持 tcp/http）",
                        val
                    )));
                }
            },
            _ => {
                // 忽略未知参数（向后兼容）
            }
        }
    }
    Ok(())
}

/// 简易 URL decode（%XX → byte）。
fn url_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_else(|_| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple() {
        let url = TalonUrl::parse("talon://localhost:7720").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 7720);
        assert!(url.password.is_none());
        assert!(url.user.is_none());
    }

    #[test]
    fn parse_with_token() {
        let url = TalonUrl::parse("talon://:secret@myhost:8080").unwrap();
        assert_eq!(url.host, "myhost");
        assert_eq!(url.port, 8080);
        assert_eq!(url.password.as_deref(), Some("secret"));
        assert!(url.user.is_none());
    }

    #[test]
    fn parse_with_user_pass() {
        let url = TalonUrl::parse("talon://admin:p%40ss@db.example.com:7720").unwrap();
        assert_eq!(url.host, "db.example.com");
        assert_eq!(url.user.as_deref(), Some("admin"));
        assert_eq!(url.password.as_deref(), Some("p@ss")); // URL decoded
    }

    #[test]
    fn parse_embedded() {
        let url = TalonUrl::parse("talon:///var/data/talon").unwrap();
        assert!(url.is_embedded());
        assert_eq!(url.path.as_deref(), Some("/var/data/talon"));
    }

    #[test]
    fn parse_with_params() {
        let url = TalonUrl::parse("talon://host:9000?timeout=10&protocol=http&tls=true").unwrap();
        assert_eq!(url.host, "host");
        assert_eq!(url.port, 9000);
        assert_eq!(url.timeout_secs, 10);
        assert_eq!(url.protocol, Protocol::Http);
        assert!(url.tls);
    }

    #[test]
    fn parse_invalid_scheme() {
        assert!(TalonUrl::parse("postgres://localhost:5432").is_err());
    }

    #[test]
    fn roundtrip() {
        let url = TalonUrl::parse("talon://:tok@host:7720?timeout=5").unwrap();
        let s = url.to_string_url();
        let url2 = TalonUrl::parse(&s).unwrap();
        assert_eq!(url.host, url2.host);
        assert_eq!(url.port, url2.port);
        assert_eq!(url.password, url2.password);
        assert_eq!(url.timeout_secs, url2.timeout_secs);
    }

    #[test]
    fn to_server_config() {
        let url = TalonUrl::parse("talon://:mytoken@0.0.0.0:7654").unwrap();
        let cfg = url.to_server_config();
        assert_eq!(cfg.http_addr, "0.0.0.0:7654");
        assert_eq!(cfg.auth_token.as_deref(), Some("mytoken"));
        assert_eq!(cfg.max_connections, 256);
    }

    #[test]
    fn display_hides_password() {
        let url = TalonUrl::parse("talon://admin:secret@host:7720").unwrap();
        let display = format!("{}", url);
        assert!(!display.contains("secret"));
        assert!(display.contains("***"));
    }

    #[test]
    fn default_port() {
        let url = TalonUrl::parse("talon://myhost").unwrap();
        assert_eq!(url.host, "myhost");
        assert_eq!(url.port, 7720); // default
    }
}
