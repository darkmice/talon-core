/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 设计稿 4 — AI 对话引擎。
//!
//! 布局：会话列表 | 对话消息流 + 输入栏。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use super::{char_len, char_to_byte};
use crate::tui_app::{net, theme};

/// AI 对话页面状态。
pub struct AiChatState {
    pub sessions: Vec<SessionInfo>,
    pub selected_session: usize,
    pub messages: Vec<ChatMessage>,
    pub msg_scroll: usize,
    pub input: String,
    pub cursor_pos: usize,
    pub focus: AiFocus,
}

/// 会话信息。
pub struct SessionInfo {
    pub id: String,
    pub title: String,
    pub msg_count: usize,
    pub model: String,
}

/// 对话消息。
pub struct ChatMessage {
    pub role: String,
    pub content: String,
    pub tokens: usize,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum AiFocus {
    Sessions,
    Messages,
    Input,
}

impl AiChatState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            sessions: vec![
                SessionInfo { id: "sess_001".into(), title: "RAG Pipeline Debug".into(), msg_count: 12, model: "llama-3".into() },
                SessionInfo { id: "sess_002".into(), title: "Code Review Agent".into(), msg_count: 8, model: "codestral".into() },
                SessionInfo { id: "sess_003".into(), title: "Data Analysis".into(), msg_count: 24, model: "llama-3".into() },
                SessionInfo { id: "sess_004".into(), title: "Schema Design".into(), msg_count: 6, model: "qwen-2.5".into() },
            ],
            selected_session: 0,
            messages: vec![
                ChatMessage { role: "system".into(), content: "You are a helpful AI assistant for database operations.".into(), tokens: 12 },
                ChatMessage { role: "user".into(), content: "How do I set up a RAG pipeline with Talon's vector store?".into(), tokens: 18 },
                ChatMessage { role: "assistant".into(), content: "To set up a RAG pipeline with Talon:\n\n1. Create a vector collection:\n   VECTOR CREATE embeddings DIM 768\n\n2. Insert embeddings:\n   VECTOR INSERT embeddings VALUES ([0.1, 0.2, ...], 'doc_id')\n\n3. Search with context:\n   VECTOR SEARCH embeddings QUERY ([...]) TOP_K 5\n\n4. Feed results to your LLM context window.".into(), tokens: 84 },
                ChatMessage { role: "user".into(), content: "Can I use HNSW index for faster search?".into(), tokens: 11 },
                ChatMessage { role: "assistant".into(), content: "Yes! Talon uses HNSW by default for all vector collections. You can tune parameters:\n\n  VECTOR CONFIG embeddings SET m=16 ef_construction=200\n\nFor query-time:\n  VECTOR SEARCH embeddings QUERY ([...]) TOP_K 10 EF 100\n\nHigher ef = more accurate but slower. The default M=16, ef=64 works well for most RAG use cases.".into(), tokens: 72 },
            ],
            msg_scroll: 0,
            input: String::new(),
            cursor_pos: 0,
            focus: AiFocus::Sessions,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        self.focus == AiFocus::Input
    }

    /// 进入输入模式（聚焦消息输入框）。
    pub fn enter_input_mode(&mut self) {
        self.focus = AiFocus::Input;
    }

    /// 刷新数据（加载 AI 会话列表）。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.ai_sessions() {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.sessions = data
                        .iter()
                        .filter_map(|v| {
                            Some(SessionInfo {
                                id: v.get("id")?.as_str()?.to_string(),
                                title: v
                                    .get("title")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                msg_count: v.get("msg_count").and_then(|n| n.as_u64()).unwrap_or(0)
                                    as usize,
                                model: v
                                    .get("model")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            })
                        })
                        .collect();
                    self.selected_session = 0;
                }
            }
        }
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, client: &mut Option<net::TuiClient>) {
        match self.focus {
            AiFocus::Sessions => match key.code {
                KeyCode::Tab => self.focus = AiFocus::Input,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.selected_session = self.selected_session.saturating_sub(1);
                    self.load_session(client);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.selected_session < self.sessions.len().saturating_sub(1) {
                        self.selected_session += 1;
                        self.load_session(client);
                    }
                }
                _ => {}
            },
            AiFocus::Messages => match key.code {
                KeyCode::Tab => self.focus = AiFocus::Sessions,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.msg_scroll = self.msg_scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.msg_scroll += 1;
                }
                _ => {}
            },
            AiFocus::Input => match key.code {
                KeyCode::Esc => self.focus = AiFocus::Sessions,
                KeyCode::Tab => self.focus = AiFocus::Messages,
                KeyCode::Enter => self.send_message(client),
                KeyCode::Char(c) => {
                    let byte_pos = char_to_byte(&self.input, self.cursor_pos);
                    self.input.insert(byte_pos, c);
                    self.cursor_pos += 1;
                }
                KeyCode::Backspace => {
                    if self.cursor_pos > 0 {
                        self.cursor_pos -= 1;
                        let start = char_to_byte(&self.input, self.cursor_pos);
                        let end = char_to_byte(&self.input, self.cursor_pos + 1);
                        self.input.drain(start..end);
                    }
                }
                KeyCode::Left => {
                    self.cursor_pos = self.cursor_pos.saturating_sub(1);
                }
                KeyCode::Right => {
                    if self.cursor_pos < char_len(&self.input) {
                        self.cursor_pos += 1;
                    }
                }
                KeyCode::Home => self.cursor_pos = 0,
                KeyCode::End => self.cursor_pos = char_len(&self.input),
                _ => {}
            },
        }
    }

    fn load_session(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            let sid = self
                .sessions
                .get(self.selected_session)
                .map(|s| s.id.clone())
                .unwrap_or_default();
            if let Ok(resp) = c.ai_messages(&sid) {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.messages = data
                        .iter()
                        .filter_map(|m| {
                            Some(ChatMessage {
                                role: m.get("role")?.as_str()?.to_string(),
                                content: m.get("content")?.as_str()?.to_string(),
                                tokens: m.get("tokens").and_then(|t| t.as_u64()).unwrap_or(0)
                                    as usize,
                            })
                        })
                        .collect();
                    self.msg_scroll = 0;
                }
            }
        }
    }

    fn send_message(&mut self, client: &mut Option<net::TuiClient>) {
        let msg = self.input.trim().to_string();
        if msg.is_empty() {
            return;
        }
        self.messages.push(ChatMessage {
            role: "user".into(),
            content: msg.clone(),
            tokens: 0,
        });
        self.input.clear();
        self.cursor_pos = 0;

        if let Some(ref mut c) = client {
            let sid = self
                .sessions
                .get(self.selected_session)
                .map(|s| s.id.clone())
                .unwrap_or_default();
            if let Ok(resp) = c.ai_send(&sid, &msg) {
                if let Some(content) = resp
                    .get("data")
                    .and_then(|d| d.get("content"))
                    .and_then(|c| c.as_str())
                {
                    self.messages.push(ChatMessage {
                        role: "assistant".into(),
                        content: content.to_string(),
                        tokens: 0,
                    });
                }
            }
        }
    }

    /// 绘制 AI 对话页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(26), Constraint::Min(1)])
            .split(area);

        self.draw_sessions(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // 状态栏
                Constraint::Min(1),    // 消息流
                Constraint::Length(3), // 输入栏
                Constraint::Length(1), // 快捷键
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_messages(frame, right[1]);
        self.draw_input(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_sessions(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " SESSIONS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == AiFocus::Sessions {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, s) in self.sessions.iter().enumerate() {
            let sel = i == self.selected_session;
            let style = if sel {
                Style::default()
                    .fg(theme::YELLOW)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, s.title),
                style,
            )));
            lines.push(Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(&s.model, Style::default().fg(theme::PURPLE)),
                Span::styled(
                    format!("  {} msgs", s.msg_count),
                    Style::default().fg(theme::TEXT_DIM),
                ),
            ]));
            if i < self.sessions.len() - 1 {
                lines.push(Line::from(Span::styled(
                    "   ─────────────────",
                    Style::default().fg(theme::BORDER_DIM),
                )));
            }
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let sess = self.sessions.get(self.selected_session);
        let model = sess.map(|s| s.model.as_str()).unwrap_or("—");
        let total_tokens: usize = self.messages.iter().map(|m| m.tokens).sum();
        let bar = Line::from(vec![
            Span::styled(" 🤖 ", Style::default().fg(theme::YELLOW)),
            Span::styled("AI ENGINE", Style::default().fg(theme::YELLOW)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("Model: ", Style::default().fg(theme::TEXT_DIM)),
            Span::styled(model, Style::default().fg(theme::PURPLE)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("Tokens: {}", total_tokens),
                Style::default().fg(theme::TEXT_MUTED),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} msgs", self.messages.len()),
                Style::default().fg(theme::TEXT_MUTED),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_messages(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " CONVERSATION ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == AiFocus::Messages {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(Color::Rgb(12, 12, 18)));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for msg in &self.messages {
            let (role_style, prefix) = match msg.role.as_str() {
                "system" => (Style::default().fg(theme::TEXT_DIM).italic(), "  SYS "),
                "user" => (Style::default().fg(theme::BLUE).bold(), "  YOU "),
                "assistant" => (Style::default().fg(theme::GREEN).bold(), "  AI  "),
                _ => (Style::default().fg(theme::TEXT_MUTED), "  ??? "),
            };
            lines.push(Line::from(vec![
                Span::styled(prefix, role_style),
                Span::styled(
                    format!("({}tk)", msg.tokens),
                    Style::default().fg(theme::TEXT_DIM),
                ),
            ]));
            let content_style = if msg.role == "system" {
                Style::default().fg(theme::TEXT_DIM)
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            for line in msg.content.lines() {
                lines.push(Line::from(Span::styled(
                    format!("  {}", line),
                    content_style,
                )));
            }
            lines.push(Line::from(""));
        }
        let scroll = self.msg_scroll as u16;
        frame.render_widget(Paragraph::new(lines).scroll((scroll, 0)), inner);
    }

    fn draw_input(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " MESSAGE ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == AiFocus::Input {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let cursor = if self.focus == AiFocus::Input {
            "▏"
        } else {
            ""
        };
        let text = if self.input.is_empty() && self.focus != AiFocus::Input {
            "Type a message and press Enter...".to_string()
        } else {
            format!("{}{}", self.input, cursor)
        };
        let style = if self.input.is_empty() && self.focus != AiFocus::Input {
            Style::default().fg(theme::TEXT_DIM)
        } else {
            Style::default().fg(theme::TEXT)
        };
        frame.render_widget(Paragraph::new(Span::styled(text, style)), inner);
    }

    fn draw_keys(&self, frame: &mut Frame, area: Rect) {
        let bar = if self.focus == AiFocus::Input {
            Line::from(vec![
                Span::styled(" Enter", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Send  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Esc", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Nav  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Tab", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Focus", Style::default().fg(theme::TEXT_SECONDARY)),
            ])
        } else {
            Line::from(vec![
                Span::styled(" i", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Chat  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("j/k", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Nav  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("g/G", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Top/Bot  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("r", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Refresh  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("?", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Help", Style::default().fg(theme::TEXT_SECONDARY)),
            ])
        };
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }
}
