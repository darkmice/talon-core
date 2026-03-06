/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 设计稿 3 — 向量搜索引擎。
//!
//! 布局：集合列表 | 搜索输入 + 结果列表 + 距离可视化条。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use super::{char_len, char_to_byte};
use crate::tui_app::{net, theme};

/// 向量搜索页面状态。
pub struct VectorSearchState {
    pub collections: Vec<CollectionInfo>,
    pub selected_col: usize,
    pub query_input: String,
    pub cursor_pos: usize,
    pub results: Vec<SearchResult>,
    pub result_scroll: usize,
    pub top_k: usize,
    pub focus: VectorFocus,
}

/// 向量集合信息。
pub struct CollectionInfo {
    pub name: String,
    pub count: usize,
    pub dim: usize,
}

/// 搜索结果条目。
pub struct SearchResult {
    pub id: String,
    pub score: f64,
    pub label: String,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum VectorFocus {
    Collections,
    Query,
    Results,
}

impl VectorSearchState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            collections: vec![
                CollectionInfo {
                    name: "embeddings_768".into(),
                    count: 102400,
                    dim: 768,
                },
                CollectionInfo {
                    name: "img_clip_512".into(),
                    count: 58200,
                    dim: 512,
                },
                CollectionInfo {
                    name: "code_ast_256".into(),
                    count: 31000,
                    dim: 256,
                },
                CollectionInfo {
                    name: "user_prefs_128".into(),
                    count: 8900,
                    dim: 128,
                },
            ],
            selected_col: 0,
            query_input: "machine learning optimization".into(),
            cursor_pos: 30,
            results: vec![
                SearchResult {
                    id: "doc_8291".into(),
                    score: 0.9847,
                    label: "Deep Learning Fundamentals: Gradient Descent & Backprop".into(),
                },
                SearchResult {
                    id: "doc_1042".into(),
                    score: 0.9623,
                    label: "Neural Network Optimization Techniques Survey".into(),
                },
                SearchResult {
                    id: "doc_5538".into(),
                    score: 0.9410,
                    label: "Stochastic Gradient Methods for Large-Scale ML".into(),
                },
                SearchResult {
                    id: "doc_0217".into(),
                    score: 0.9188,
                    label: "Adam, AdaGrad, and Modern Optimizers Compared".into(),
                },
                SearchResult {
                    id: "doc_7764".into(),
                    score: 0.8901,
                    label: "Transfer Learning in NLP: BERT to GPT Pipeline".into(),
                },
                SearchResult {
                    id: "doc_3346".into(),
                    score: 0.8650,
                    label: "Reinforcement Learning: Policy Optimization Methods".into(),
                },
                SearchResult {
                    id: "doc_9912".into(),
                    score: 0.8412,
                    label: "Feature Engineering for Tabular ML Models".into(),
                },
                SearchResult {
                    id: "doc_4401".into(),
                    score: 0.8190,
                    label: "Hyperparameter Tuning with Bayesian Optimization".into(),
                },
            ],
            result_scroll: 0,
            top_k: 10,
            focus: VectorFocus::Collections,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        self.focus == VectorFocus::Query
    }

    /// 进入输入模式（聚焦查询输入框）。
    pub fn enter_input_mode(&mut self) {
        self.focus = VectorFocus::Query;
    }

    /// 刷新数据（加载向量集合列表）。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.vector_collections() {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.collections = data
                        .iter()
                        .filter_map(|v| {
                            Some(CollectionInfo {
                                name: v.get("name")?.as_str()?.to_string(),
                                dim: v.get("dim").and_then(|d| d.as_u64()).unwrap_or(0) as usize,
                                count: v.get("count").and_then(|c| c.as_u64()).unwrap_or(0)
                                    as usize,
                            })
                        })
                        .collect();
                    self.selected_col = 0;
                }
            }
        }
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, client: &mut Option<net::TuiClient>) {
        match self.focus {
            VectorFocus::Collections => match key.code {
                KeyCode::Tab => self.focus = VectorFocus::Query,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.selected_col = self.selected_col.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.selected_col < self.collections.len().saturating_sub(1) {
                        self.selected_col += 1;
                    }
                }
                _ => {}
            },
            VectorFocus::Query => match key.code {
                KeyCode::Esc => self.focus = VectorFocus::Collections,
                KeyCode::Tab => self.focus = VectorFocus::Results,
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
            VectorFocus::Results => match key.code {
                KeyCode::Tab => self.focus = VectorFocus::Collections,
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
            let col = self
                .collections
                .get(self.selected_col)
                .map(|c| c.name.clone())
                .unwrap_or_default();
            let dummy_vec: Vec<f32> = vec![0.0; 4];
            if let Ok(resp) = c.vector_search(&col, &dummy_vec, self.top_k) {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.results = data
                        .iter()
                        .filter_map(|r| {
                            Some(SearchResult {
                                id: r.get("id")?.as_str()?.to_string(),
                                score: r.get("score")?.as_f64()?,
                                label: r
                                    .get("label")
                                    .and_then(|l| l.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                            })
                        })
                        .collect();
                }
            }
        }
    }

    /// 绘制向量搜索页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(28), Constraint::Min(1)])
            .split(area);

        self.draw_collections(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // 状态栏
                Constraint::Length(3), // 搜索输入
                Constraint::Min(1),    // 结果
                Constraint::Length(1), // 快捷键
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_query(frame, right[1]);
        self.draw_results(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_collections(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " COLLECTIONS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == VectorFocus::Collections {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, col) in self.collections.iter().enumerate() {
            let sel = i == self.selected_col;
            let style = if sel {
                Style::default()
                    .fg(theme::ACCENT)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, col.name),
                style,
            )));
            let meta = format!("     {}D  {} vecs", col.dim, col.count);
            lines.push(Line::from(Span::styled(
                meta,
                Style::default().fg(if sel {
                    theme::TEXT_MUTED
                } else {
                    theme::TEXT_DIM
                }),
            )));
            lines.push(Line::from(""));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let col = self.collections.get(self.selected_col);
        let name = col.map(|c| c.name.as_str()).unwrap_or("—");
        let bar = Line::from(vec![
            Span::styled(" ◆ ", Style::default().fg(theme::ACCENT)),
            Span::styled("VECTOR ENGINE", Style::default().fg(theme::ACCENT)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("Collection: ", Style::default().fg(theme::TEXT_DIM)),
            Span::styled(name, Style::default().fg(theme::TEXT)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("Top-K: {}", self.top_k),
                Style::default().fg(theme::TEXT_MUTED),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_query(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " SEMANTIC QUERY ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == VectorFocus::Query {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let cursor = if self.focus == VectorFocus::Query {
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
                    format!(" {} matches ", self.results.len()),
                    Style::default().fg(theme::GREEN),
                ))
                .alignment(Alignment::Right),
            )
            .border_style(if self.focus == VectorFocus::Results {
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
        let max_score = self.results.first().map(|r| r.score).unwrap_or(1.0);
        for (i, r) in self.results.iter().enumerate() {
            let sel = i == self.result_scroll;
            let bar_width = ((r.score / max_score) * 20.0) as usize;
            let bar: String = "█".repeat(bar_width) + &"░".repeat(20 - bar_width);
            let score_color = if r.score > 0.95 {
                theme::GREEN
            } else if r.score > 0.90 {
                theme::BLUE
            } else if r.score > 0.85 {
                theme::YELLOW
            } else {
                theme::TEXT_MUTED
            };
            let idx_style = if sel {
                Style::default()
                    .fg(theme::ACCENT)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::TEXT_DIM)
            };
            lines.push(Line::from(vec![
                Span::styled(format!(" {:>2}. ", i + 1), idx_style),
                Span::styled(
                    format!("{:.4}", r.score),
                    Style::default().fg(score_color).bold(),
                ),
                Span::styled("  ", Style::default()),
                Span::styled(bar.clone(), Style::default().fg(score_color)),
                Span::styled(format!("  {}", r.id), Style::default().fg(theme::PURPLE)),
            ]));
            let label_style = if sel {
                Style::default().fg(theme::TEXT)
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            lines.push(Line::from(Span::styled(
                format!("      {}", r.label),
                label_style,
            )));
            if i < self.results.len() - 1 {
                lines.push(Line::from(""));
            }
        }
        frame.render_widget(Paragraph::new(lines).scroll((0, 0)), inner);
    }

    fn draw_keys(&self, frame: &mut Frame, area: Rect) {
        let bar = if self.focus == VectorFocus::Query {
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
