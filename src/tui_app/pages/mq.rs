/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 消息队列引擎页面。
//!
//! 布局：主题列表 | 状态栏 + 队列统计 + 消息流。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui_app::{net, theme};

/// 消息队列页面状态。
pub struct MqState {
    pub topics: Vec<TopicInfo>,
    pub selected: usize,
    pub recent_msgs: Vec<MqMessage>,
    pub msg_scroll: usize,
    pub focus: MqFocus,
}

/// MQ 主题信息。
pub struct TopicInfo {
    pub name: String,
    pub pending: usize,
    pub consumers: usize,
    pub throughput: String,
    pub maxlen: String,
}

/// MQ 消息。
pub struct MqMessage {
    pub topic: String,
    pub payload: String,
    pub time: String,
    pub seq: u64,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum MqFocus {
    Topics,
    Messages,
}

impl MqState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            topics: vec![
                TopicInfo {
                    name: "events.user".into(),
                    pending: 142,
                    consumers: 3,
                    throughput: "1.2k/s".into(),
                    maxlen: "100K".into(),
                },
                TopicInfo {
                    name: "events.system".into(),
                    pending: 8,
                    consumers: 1,
                    throughput: "80/s".into(),
                    maxlen: "50K".into(),
                },
                TopicInfo {
                    name: "jobs.pipeline".into(),
                    pending: 1024,
                    consumers: 4,
                    throughput: "500/s".into(),
                    maxlen: "1M".into(),
                },
                TopicInfo {
                    name: "logs.audit".into(),
                    pending: 0,
                    consumers: 2,
                    throughput: "200/s".into(),
                    maxlen: "500K".into(),
                },
                TopicInfo {
                    name: "ai.traces".into(),
                    pending: 56,
                    consumers: 2,
                    throughput: "150/s".into(),
                    maxlen: "200K".into(),
                },
                TopicInfo {
                    name: "cache.invalidate".into(),
                    pending: 3,
                    consumers: 1,
                    throughput: "50/s".into(),
                    maxlen: "10K".into(),
                },
            ],
            selected: 0,
            recent_msgs: vec![
                MqMessage {
                    topic: "events.user".into(),
                    payload: r#"{"type":"login","uid":"u_8829","ip":"10.0.1.5"}"#.into(),
                    time: "14:24:18.042".into(),
                    seq: 892041,
                },
                MqMessage {
                    topic: "events.user".into(),
                    payload: r#"{"type":"search","query":"vector embeddings","results":42}"#.into(),
                    time: "14:24:17.891".into(),
                    seq: 892040,
                },
                MqMessage {
                    topic: "jobs.pipeline".into(),
                    payload: r#"{"job":"embed","batch":42,"docs":128,"status":"ok"}"#.into(),
                    time: "14:24:16.504".into(),
                    seq: 445012,
                },
                MqMessage {
                    topic: "events.system".into(),
                    payload: r#"{"event":"gc","freed_mb":128,"duration_ms":45}"#.into(),
                    time: "14:24:15.221".into(),
                    seq: 110234,
                },
                MqMessage {
                    topic: "logs.audit".into(),
                    payload: r#"{"action":"DROP TABLE temp","user":"admin","ip":"10.0.1.2"}"#
                        .into(),
                    time: "14:24:14.803".into(),
                    seq: 67891,
                },
                MqMessage {
                    topic: "ai.traces".into(),
                    payload: r#"{"step":"llm_call","model":"llama-3","tokens":1204}"#.into(),
                    time: "14:24:13.112".into(),
                    seq: 34021,
                },
                MqMessage {
                    topic: "events.user".into(),
                    payload: r#"{"type":"upload","file":"data.csv","size_mb":12}"#.into(),
                    time: "14:24:12.567".into(),
                    seq: 892039,
                },
                MqMessage {
                    topic: "cache.invalidate".into(),
                    payload: r#"{"keys":["user:1001","session:abc"]}"#.into(),
                    time: "14:24:11.890".into(),
                    seq: 5501,
                },
            ],
            msg_scroll: 0,
            focus: MqFocus::Topics,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        false
    }

    /// 进入输入模式（本页无输入框）。
    pub fn enter_input_mode(&mut self) {}

    /// 刷新数据（加载 MQ 主题列表）。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.mq_topics() {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.topics = data
                        .iter()
                        .filter_map(|v| {
                            Some(TopicInfo {
                                name: v.get("name")?.as_str()?.to_string(),
                                pending: v.get("pending").and_then(|n| n.as_u64()).unwrap_or(0)
                                    as usize,
                                consumers: v.get("consumers").and_then(|n| n.as_u64()).unwrap_or(0)
                                    as usize,
                                throughput: v
                                    .get("throughput")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                maxlen: v
                                    .get("maxlen")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            })
                        })
                        .collect();
                    self.selected = 0;
                }
            }
        }
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, _client: &mut Option<net::TuiClient>) {
        match key.code {
            KeyCode::Tab => {
                self.focus = match self.focus {
                    MqFocus::Topics => MqFocus::Messages,
                    MqFocus::Messages => MqFocus::Topics,
                };
            }
            KeyCode::Up | KeyCode::Char('k') => match self.focus {
                MqFocus::Topics => {
                    self.selected = self.selected.saturating_sub(1);
                }
                MqFocus::Messages => {
                    self.msg_scroll = self.msg_scroll.saturating_sub(1);
                }
            },
            KeyCode::Down | KeyCode::Char('j') => match self.focus {
                MqFocus::Topics => {
                    if self.selected < self.topics.len().saturating_sub(1) {
                        self.selected += 1;
                    }
                }
                MqFocus::Messages => {
                    if self.msg_scroll < self.recent_msgs.len().saturating_sub(1) {
                        self.msg_scroll += 1;
                    }
                }
            },
            _ => {}
        }
    }

    /// 绘制消息队列页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(28), Constraint::Min(1)])
            .split(area);

        self.draw_topics(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(5),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_stats(frame, right[1]);
        self.draw_messages(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_topics(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " MQ TOPICS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == MqFocus::Topics {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, t) in self.topics.iter().enumerate() {
            let sel = i == self.selected;
            let style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, t.name),
                style,
            )));
            let pending_color = if t.pending > 100 {
                theme::YELLOW
            } else {
                theme::GREEN
            };
            lines.push(Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(
                    format!("{}pending", t.pending),
                    Style::default().fg(pending_color),
                ),
                Span::styled(
                    format!("  {}", t.throughput),
                    Style::default().fg(theme::TEXT_DIM),
                ),
            ]));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let total_pending: usize = self.topics.iter().map(|t| t.pending).sum();
        let total_consumers: usize = self.topics.iter().map(|t| t.consumers).sum();
        let bar = Line::from(vec![
            Span::styled(" ⇌ ", Style::default().fg(theme::PURPLE)),
            Span::styled("MESSAGE QUEUE ENGINE", Style::default().fg(theme::PURPLE)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} topics", self.topics.len()),
                Style::default().fg(theme::TEXT),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} pending", total_pending),
                Style::default().fg(theme::YELLOW),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} consumers", total_consumers),
                Style::default().fg(theme::GREEN),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_stats(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " TOPIC DETAIL ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        if let Some(t) = self.topics.get(self.selected) {
            let lines = vec![
                Line::from(vec![
                    Span::styled(" Topic:     ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(&t.name, Style::default().fg(theme::ACCENT).bold()),
                ]),
                Line::from(vec![
                    Span::styled(" Pending:   ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(
                        format!("{}", t.pending),
                        Style::default().fg(theme::YELLOW).bold(),
                    ),
                    Span::styled("    Consumers: ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(
                        format!("{}", t.consumers),
                        Style::default().fg(theme::GREEN).bold(),
                    ),
                    Span::styled("    Throughput: ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(&t.throughput, Style::default().fg(theme::TEXT).bold()),
                ]),
                Line::from(vec![
                    Span::styled(" MaxLen:    ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(&t.maxlen, Style::default().fg(theme::TEXT_MUTED)),
                    Span::styled("    Mode: ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled("At-Least-Once", Style::default().fg(theme::BLUE)),
                ]),
            ];
            frame.render_widget(Paragraph::new(lines), inner);
        }
    }

    fn draw_messages(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " RECENT MESSAGES ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .title(
                block::Title::from(Span::styled(
                    format!(" {} msgs ", self.recent_msgs.len()),
                    Style::default().fg(theme::GREEN),
                ))
                .alignment(Alignment::Right),
            )
            .border_style(if self.focus == MqFocus::Messages {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(Color::Rgb(12, 12, 18)));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, m) in self.recent_msgs.iter().enumerate() {
            let sel = i == self.msg_scroll;
            let time_style = if sel {
                Style::default().fg(theme::TEXT_MUTED)
            } else {
                Style::default().fg(theme::TEXT_DIM)
            };
            lines.push(Line::from(vec![
                Span::styled(
                    format!(" #{:<8}", m.seq),
                    Style::default().fg(theme::TEXT_DIM),
                ),
                Span::styled(&m.time, time_style),
                Span::styled("  ", Style::default()),
                Span::styled(
                    format!("{:<20}", m.topic),
                    Style::default().fg(theme::PURPLE),
                ),
            ]));
            let payload_style = if sel {
                Style::default().fg(theme::TEXT_SECONDARY)
            } else {
                Style::default().fg(theme::TEXT_DIM)
            };
            lines.push(Line::from(Span::styled(
                format!("           {}", m.payload),
                payload_style,
            )));
            if i < self.recent_msgs.len() - 1 {
                lines.push(Line::from(""));
            }
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_keys(&self, frame: &mut Frame, area: Rect) {
        let bar = Line::from(vec![
            Span::styled(" j/k", Style::default().fg(theme::BLUE).bold()),
            Span::styled(" Nav  ", Style::default().fg(theme::TEXT_SECONDARY)),
            Span::styled("g/G", Style::default().fg(theme::BLUE).bold()),
            Span::styled(" Top/Bot  ", Style::default().fg(theme::TEXT_SECONDARY)),
            Span::styled("Tab", Style::default().fg(theme::BLUE).bold()),
            Span::styled(" Focus  ", Style::default().fg(theme::TEXT_SECONDARY)),
            Span::styled("r", Style::default().fg(theme::BLUE).bold()),
            Span::styled(" Refresh  ", Style::default().fg(theme::TEXT_SECONDARY)),
            Span::styled("1-9", Style::default().fg(theme::BLUE).bold()),
            Span::styled(" Pages  ", Style::default().fg(theme::TEXT_SECONDARY)),
            Span::styled("?", Style::default().fg(theme::BLUE).bold()),
            Span::styled(" Help", Style::default().fg(theme::TEXT_SECONDARY)),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }
}
