/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 设计稿 — 全文搜索引擎（FTS）。
//!
//! 布局：索引列表 | 搜索栏 + BM25 结果 + 高亮预览。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use super::{char_len, char_to_byte};
use crate::tui_app::{net, theme};

/// 全文搜索页面状态。
pub struct FtsState {
    pub indices: Vec<FtsIndex>,
    pub selected_idx: usize,
    pub query_input: String,
    pub cursor_pos: usize,
    pub results: Vec<FtsResult>,
    pub result_scroll: usize,
    pub focus: FtsFocus,
}

/// FTS 索引信息。
pub struct FtsIndex {
    pub name: String,
    pub doc_count: usize,
    pub field_count: usize,
}

/// 搜索结果条目。
pub struct FtsResult {
    pub doc_id: String,
    pub score: f64,
    pub title: String,
    pub snippet: String,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum FtsFocus {
    Indices,
    Query,
    Results,
}

impl FtsState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            indices: vec![
                FtsIndex { name: "articles".into(), doc_count: 24500, field_count: 4 },
                FtsIndex { name: "docs_manual".into(), doc_count: 3200, field_count: 3 },
                FtsIndex { name: "code_comments".into(), doc_count: 89000, field_count: 2 },
                FtsIndex { name: "chat_history".into(), doc_count: 152000, field_count: 3 },
            ],
            selected_idx: 0,
            query_input: "Rust database engine".into(),
            cursor_pos: 20,
            results: vec![
                FtsResult {
                    doc_id: "art_2891".into(), score: 18.42,
                    title: "Building a Multi-Model Database in Rust".into(),
                    snippet: "A <em>Rust</em> <em>database</em> <em>engine</em> combining SQL, KV, vector and graph capabilities...".into(),
                },
                FtsResult {
                    doc_id: "art_1204".into(), score: 15.87,
                    title: "LSM-Tree Storage Engines: A Rust Perspective".into(),
                    snippet: "Modern <em>database</em> systems built in <em>Rust</em> leverage LSM-Tree for write-optimized <em>engine</em>...".into(),
                },
                FtsResult {
                    doc_id: "art_5567".into(), score: 14.23,
                    title: "Embedded Databases for AI Applications".into(),
                    snippet: "Why <em>Rust</em> is the ideal language for building embedded <em>database</em> <em>engine</em>s with zero dependencies...".into(),
                },
                FtsResult {
                    doc_id: "doc_0891".into(), score: 12.05,
                    title: "HNSW Vector Index Implementation".into(),
                    snippet: "The vector <em>engine</em> in our <em>Rust</em> <em>database</em> uses a custom HNSW implementation...".into(),
                },
                FtsResult {
                    doc_id: "art_7712".into(), score: 10.91,
                    title: "BM25 Scoring in Full-Text Search".into(),
                    snippet: "Implementing BM25 relevance scoring for a <em>Rust</em>-based search <em>engine</em>...".into(),
                },
                FtsResult {
                    doc_id: "art_3301".into(), score: 9.44,
                    title: "Hybrid Search: BM25 + Vector RRF Fusion".into(),
                    snippet: "Combining full-text BM25 with vector similarity in a single <em>database</em> <em>engine</em>...".into(),
                },
            ],
            result_scroll: 0,
            focus: FtsFocus::Indices,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        self.focus == FtsFocus::Query
    }

    /// 进入输入模式（聚焦查询输入框）。
    pub fn enter_input_mode(&mut self) {
        self.focus = FtsFocus::Query;
    }

    /// 刷新数据（加载 FTS 索引列表）。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.fts_indices() {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.indices = data
                        .iter()
                        .filter_map(|v| {
                            Some(FtsIndex {
                                name: v.get("name")?.as_str()?.to_string(),
                                doc_count: v.get("doc_count").and_then(|n| n.as_u64()).unwrap_or(0)
                                    as usize,
                                field_count: v
                                    .get("field_count")
                                    .and_then(|n| n.as_u64())
                                    .unwrap_or(0)
                                    as usize,
                            })
                        })
                        .collect();
                    self.selected_idx = 0;
                }
            }
        }
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, client: &mut Option<net::TuiClient>) {
        match self.focus {
            FtsFocus::Indices => match key.code {
                KeyCode::Tab => self.focus = FtsFocus::Query,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.selected_idx = self.selected_idx.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.selected_idx < self.indices.len().saturating_sub(1) {
                        self.selected_idx += 1;
                    }
                }
                _ => {}
            },
            FtsFocus::Query => match key.code {
                KeyCode::Esc => self.focus = FtsFocus::Indices,
                KeyCode::Tab => self.focus = FtsFocus::Results,
                KeyCode::Enter => self.execute_search(client),
                KeyCode::Char(c) => {
                    let byte_pos = char_to_byte(&self.query_input, self.cursor_pos);
                    self.query_input.insert(byte_pos, c);
                    self.cursor_pos += 1;
                }
                KeyCode::Backspace => {
                    if self.cursor_pos > 0 {
                        self.cursor_pos -= 1;
                        let start = char_to_byte(&self.query_input, self.cursor_pos);
                        let end = char_to_byte(&self.query_input, self.cursor_pos + 1);
                        self.query_input.drain(start..end);
                    }
                }
                KeyCode::Left => {
                    self.cursor_pos = self.cursor_pos.saturating_sub(1);
                }
                KeyCode::Right => {
                    if self.cursor_pos < char_len(&self.query_input) {
                        self.cursor_pos += 1;
                    }
                }
                KeyCode::Home => self.cursor_pos = 0,
                KeyCode::End => self.cursor_pos = char_len(&self.query_input),
                _ => {}
            },
            FtsFocus::Results => match key.code {
                KeyCode::Tab => self.focus = FtsFocus::Indices,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.result_scroll = self.result_scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.result_scroll < self.results.len().saturating_sub(1) {
                        self.result_scroll += 1;
                    }
                }
                _ => {}
            },
        }
    }

    fn execute_search(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            let idx = self
                .indices
                .get(self.selected_idx)
                .map(|i| i.name.clone())
                .unwrap_or_default();
            if let Ok(resp) = c.fts_search(&idx, &self.query_input, 10) {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.results = data
                        .iter()
                        .filter_map(|r| {
                            Some(FtsResult {
                                doc_id: r.get("id")?.as_str()?.to_string(),
                                score: r.get("score")?.as_f64()?,
                                title: r
                                    .get("title")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                snippet: r
                                    .get("snippet")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            })
                        })
                        .collect();
                    self.result_scroll = 0;
                }
            }
        }
    }

    /// 绘制全文搜索页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(26), Constraint::Min(1)])
            .split(area);

        self.draw_indices(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(3),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_query(frame, right[1]);
        self.draw_results(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_indices(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " FTS INDICES ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == FtsFocus::Indices {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, idx) in self.indices.iter().enumerate() {
            let sel = i == self.selected_idx;
            let style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, idx.name),
                style,
            )));
            lines.push(Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(
                    format!("{} docs", idx.doc_count),
                    Style::default().fg(if sel { theme::GREEN } else { theme::TEXT_DIM }),
                ),
                Span::styled(
                    format!("  {}f", idx.field_count),
                    Style::default().fg(theme::TEXT_DIM),
                ),
            ]));
            if i < self.indices.len() - 1 {
                lines.push(Line::from(""));
            }
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let idx = self.indices.get(self.selected_idx);
        let name = idx.map(|i| i.name.as_str()).unwrap_or("—");
        let bar = Line::from(vec![
            Span::styled(" ⊕ ", Style::default().fg(theme::BLUE)),
            Span::styled("FTS ENGINE", Style::default().fg(theme::BLUE)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("Index: ", Style::default().fg(theme::TEXT_DIM)),
            Span::styled(name, Style::default().fg(theme::TEXT)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("BM25 Scoring", Style::default().fg(theme::TEXT_MUTED)),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_query(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " FULL-TEXT QUERY ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == FtsFocus::Query {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let cursor = if self.focus == FtsFocus::Query {
            "▏"
        } else {
            ""
        };
        let text = format!("{}{}", self.query_input, cursor);
        frame.render_widget(
            Paragraph::new(Span::styled(text, Style::default().fg(theme::TEXT))),
            inner,
        );
    }

    fn draw_results(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " SEARCH RESULTS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .title(
                block::Title::from(Span::styled(
                    format!(" {} hits ", self.results.len()),
                    Style::default().fg(theme::GREEN),
                ))
                .alignment(Alignment::Right),
            )
            .border_style(if self.focus == FtsFocus::Results {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        if self.results.is_empty() {
            frame.render_widget(
                Paragraph::new("  Enter a query and press Enter to search")
                    .style(Style::default().fg(theme::TEXT_DIM)),
                inner,
            );
            return;
        }

        let mut lines: Vec<Line> = Vec::new();
        for (i, r) in self.results.iter().enumerate() {
            let sel = i == self.result_scroll;
            let idx_style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_DIM)
            };
            // 标题行：排名 + BM25 分数 + doc_id
            lines.push(Line::from(vec![
                Span::styled(format!(" {:>2}. ", i + 1), idx_style),
                Span::styled(
                    format!("BM25:{:.2}", r.score),
                    Style::default().fg(theme::GREEN).bold(),
                ),
                Span::styled(
                    format!("  {}", r.doc_id),
                    Style::default().fg(theme::PURPLE),
                ),
            ]));
            // 文档标题
            let title_style = if sel {
                Style::default().fg(theme::TEXT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            lines.push(Line::from(Span::styled(
                format!("      {}", r.title),
                title_style,
            )));
            // 高亮摘要（<em>标记用颜色替代）
            let snippet_spans = render_snippet(&r.snippet, sel);
            lines.push(Line::from(snippet_spans));
            if i < self.results.len() - 1 {
                lines.push(Line::from(Span::styled(
                    "      ─────────────────────────────────",
                    Style::default().fg(theme::BORDER_DIM),
                )));
            }
        }
        frame.render_widget(Paragraph::new(lines).scroll((0, 0)), inner);
    }

    fn draw_keys(&self, frame: &mut Frame, area: Rect) {
        let bar = if self.focus == FtsFocus::Query {
            Line::from(vec![
                Span::styled(" Enter", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Search  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Esc", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Nav  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Tab", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Focus", Style::default().fg(theme::TEXT_SECONDARY)),
            ])
        } else {
            Line::from(vec![
                Span::styled(" i", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Search  ", Style::default().fg(theme::TEXT_SECONDARY)),
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

/// 将 `<em>` 高亮标记渲染为彩色 Span。
fn render_snippet(snippet: &str, selected: bool) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    spans.push(Span::raw("      ".to_string()));
    let normal = if selected {
        Style::default().fg(theme::TEXT_MUTED)
    } else {
        Style::default().fg(theme::TEXT_DIM)
    };
    let highlight = Style::default().fg(theme::YELLOW).bold();
    let mut rest = snippet;
    while let Some(start) = rest.find("<em>") {
        if start > 0 {
            spans.push(Span::styled(rest[..start].to_string(), normal));
        }
        rest = &rest[start + 4..];
        if let Some(end) = rest.find("</em>") {
            spans.push(Span::styled(rest[..end].to_string(), highlight));
            rest = &rest[end + 5..];
        } else {
            break;
        }
    }
    if !rest.is_empty() {
        spans.push(Span::styled(rest.to_string(), normal));
    }
    spans
}
