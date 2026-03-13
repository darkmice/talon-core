/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 词法分析器（Lexer/Tokenizer）：将 SQL 字符串切分为 Token 序列。
//!
//! 设计参考 sqlparser-rs 的 Tokenizer，但大幅简化：
//! - 仅覆盖 Talon 支持的 SQL 子集
//! - 零外部依赖，纯迭代器实现
//! - 记录每个 Token 的偏移位置，用于精确错误消息
//!
//! 目前主要用于 `parse()` 入口的快速关键字分派，
//! 后续可逐步迁移各 parser 子模块到 Token 流。

/// SQL Token 类型。
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token<'a> {
    /// SQL 关键字或标识符（未转大写，原始文本切片）。
    Word(&'a str),
    /// 数字字面量（整数或浮点）。
    Number(&'a str),
    /// 单引号字符串字面量（含引号）。
    SingleQuotedString(&'a str),
    /// 左括号 `(`。
    LParen,
    /// 右括号 `)`。
    RParen,
    /// 逗号 `,`。
    Comma,
    /// 分号 `;`。
    Semicolon,
    /// 比较运算符：`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`。
    CompOp(&'a str),
    /// 算术运算符：`+`, `-`, `*`, `/`, `%`。
    ArithOp(char),
    /// 点号 `.`（表.列引用）。
    Dot,
    /// 左方括号 `[`。
    LBracket,
    /// 右方括号 `]`。
    RBracket,
    /// 箭头 `->>` (JSONB path)。
    Arrow2,
    /// 箭头 `->` (JSONB path)。
    Arrow,
    /// 星号 `*`（SELECT * 或乘法）。
    Star,
    /// 其他单字符。
    Char(char),
}

/// 带位置信息的 Token。
#[derive(Debug, Clone)]
pub(crate) struct TokenWithPos<'a> {
    pub token: Token<'a>,
    /// 在原始 SQL 中的字节偏移。
    pub offset: usize,
}

/// 将 SQL 字符串切分为 Token 序列。
///
/// 复杂度 O(N)，单次遍历，零堆分配（返回切片引用）。
pub(crate) fn tokenize(sql: &str) -> Vec<TokenWithPos<'_>> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut tokens = Vec::with_capacity(len / 4 + 1);
    let mut i = 0;

    while i < len {
        // 跳过空白
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        let start = i;
        match bytes[i] {
            // 单引号字符串
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2; // 转义引号 ''
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                tokens.push(TokenWithPos {
                    token: Token::SingleQuotedString(&sql[start..i]),
                    offset: start,
                });
            }
            // 双引号标识符
            b'"' => {
                i += 1;
                while i < len && bytes[i] != b'"' {
                    i += 1;
                }
                if i < len {
                    i += 1; // 跳过右引号
                }
                tokens.push(TokenWithPos {
                    token: Token::Word(&sql[start..i]),
                    offset: start,
                });
            }
            // 反引号标识符
            b'`' => {
                i += 1;
                while i < len && bytes[i] != b'`' {
                    i += 1;
                }
                if i < len {
                    i += 1;
                }
                tokens.push(TokenWithPos {
                    token: Token::Word(&sql[start..i]),
                    offset: start,
                });
            }
            // 数字
            b'0'..=b'9' => {
                while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'.' || bytes[i] == b'e' || bytes[i] == b'E') {
                    i += 1;
                }
                tokens.push(TokenWithPos {
                    token: Token::Number(&sql[start..i]),
                    offset: start,
                });
            }
            // 标识符 / 关键字
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(TokenWithPos {
                    token: Token::Word(&sql[start..i]),
                    offset: start,
                });
            }
            // 运算符和标点
            b'(' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::LParen, offset: start });
            }
            b')' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::RParen, offset: start });
            }
            b',' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::Comma, offset: start });
            }
            b';' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::Semicolon, offset: start });
            }
            b'[' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::LBracket, offset: start });
            }
            b']' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::RBracket, offset: start });
            }
            b'.' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::Dot, offset: start });
            }
            b'*' => {
                i += 1;
                tokens.push(TokenWithPos { token: Token::Star, offset: start });
            }
            b'+' | b'/' | b'%' => {
                i += 1;
                tokens.push(TokenWithPos {
                    token: Token::ArithOp(bytes[start] as char),
                    offset: start,
                });
            }
            b'-' => {
                // -- 行注释
                if i + 1 < len && bytes[i + 1] == b'-' {
                    while i < len && bytes[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                // ->> JSONB
                if i + 2 < len && bytes[i + 1] == b'>' && bytes[i + 2] == b'>' {
                    i += 3;
                    tokens.push(TokenWithPos { token: Token::Arrow2, offset: start });
                    continue;
                }
                // -> JSONB
                if i + 1 < len && bytes[i + 1] == b'>' {
                    i += 2;
                    tokens.push(TokenWithPos { token: Token::Arrow, offset: start });
                    continue;
                }
                // 负号或减法
                i += 1;
                tokens.push(TokenWithPos {
                    token: Token::ArithOp('-'),
                    offset: start,
                });
            }
            b'!' => {
                if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                    tokens.push(TokenWithPos {
                        token: Token::CompOp(&sql[start..i]),
                        offset: start,
                    });
                } else {
                    i += 1;
                    tokens.push(TokenWithPos { token: Token::Char('!'), offset: start });
                }
            }
            b'<' => {
                if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                } else if i + 1 < len && bytes[i + 1] == b'>' {
                    i += 2;
                } else {
                    i += 1;
                }
                tokens.push(TokenWithPos {
                    token: Token::CompOp(&sql[start..i]),
                    offset: start,
                });
            }
            b'>' => {
                if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                } else {
                    i += 1;
                }
                tokens.push(TokenWithPos {
                    token: Token::CompOp(&sql[start..i]),
                    offset: start,
                });
            }
            b'=' => {
                i += 1;
                tokens.push(TokenWithPos {
                    token: Token::CompOp("="),
                    offset: start,
                });
            }
            // 其他
            _ => {
                i += 1;
                tokens.push(TokenWithPos {
                    token: Token::Char(bytes[start] as char),
                    offset: start,
                });
            }
        }
    }
    tokens
}

/// 快速提取 SQL 第一个关键字（Word token），用于 parse() 入口分派。
/// 返回大写形式。O(1) — 只看第一个 token。
pub(crate) fn first_keyword(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    // 跳过空白
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= len {
        return None;
    }
    // 必须是字母或下划线开头
    if !bytes[i].is_ascii_alphabetic() && bytes[i] != b'_' {
        return None;
    }
    let start = i;
    while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Some(sql[start..i].to_ascii_uppercase())
}

/// 从 Token 偏移计算行号和列号（1-based），用于错误消息。
pub(crate) fn offset_to_line_col(sql: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;
    for (i, ch) in sql.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_select() {
        let tokens = tokenize("SELECT id, name FROM users WHERE age > 18");
        assert!(tokens.len() >= 10);
        assert_eq!(tokens[0].token, Token::Word("SELECT"));
        assert_eq!(tokens[0].offset, 0);
    }

    #[test]
    fn test_tokenize_string_with_escape() {
        let tokens = tokenize("INSERT INTO t VALUES ('it''s')");
        let string_tokens: Vec<_> = tokens.iter().filter(|t| matches!(t.token, Token::SingleQuotedString(_))).collect();
        assert_eq!(string_tokens.len(), 1);
        assert_eq!(string_tokens[0].token, Token::SingleQuotedString("'it''s'"));
    }

    #[test]
    fn test_first_keyword() {
        assert_eq!(first_keyword("  SELECT * FROM t"), Some("SELECT".to_string()));
        assert_eq!(first_keyword("insert INTO t"), Some("INSERT".to_string()));
        assert_eq!(first_keyword("  "), None);
    }

    #[test]
    fn test_offset_to_line_col() {
        let sql = "SELECT\n  id\nFROM t";
        assert_eq!(offset_to_line_col(sql, 0), (1, 1)); // S
        assert_eq!(offset_to_line_col(sql, 7), (2, 1)); // 第二行开头空格
        assert_eq!(offset_to_line_col(sql, 12), (3, 1)); // FROM
    }

    #[test]
    fn test_tokenize_jsonb_arrow() {
        let tokens = tokenize("data->>'key'");
        assert!(tokens.iter().any(|t| t.token == Token::Arrow2));
    }

    #[test]
    fn test_tokenize_line_comment() {
        let tokens = tokenize("SELECT -- comment\nid FROM t");
        // -- comment should be skipped
        let words: Vec<_> = tokens.iter().filter(|t| matches!(t.token, Token::Word(_))).collect();
        assert_eq!(words.len(), 4); // SELECT, id, FROM, t
    }
}
