/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序存储引擎页面。
//!
//! 布局：指标列表 | 状态栏 + Sparkline 图表 + 指标详情表。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui_app::{net, theme};

/// 时序引擎页面状态。
pub struct TsState {
    pub metrics: Vec<MetricInfo>,
    pub selected: usize,
    pub sparkline_data: Vec<u64>,
    pub focus: TsFocus,
}

/// 时序指标信息。
pub struct MetricInfo {
    pub name: String,
    pub latest: f64,
    pub unit: String,
    pub trend: String,
    pub retention: String,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum TsFocus {
    List,
    Detail,
}

impl TsState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            metrics: vec![
                MetricInfo {
                    name: "cpu.usage".into(),
                    latest: 42.5,
                    unit: "%".into(),
                    trend: "↗".into(),
                    retention: "30d".into(),
                },
                MetricInfo {
                    name: "mem.heap_mb".into(),
                    latest: 1240.0,
                    unit: "MB".into(),
                    trend: "→".into(),
                    retention: "30d".into(),
                },
                MetricInfo {
                    name: "disk.iops".into(),
                    latest: 3200.0,
                    unit: "ops/s".into(),
                    trend: "↘".into(),
                    retention: "7d".into(),
                },
                MetricInfo {
                    name: "net.rx_mbps".into(),
                    latest: 85.2,
                    unit: "Mb/s".into(),
                    trend: "↗".into(),
                    retention: "7d".into(),
                },
                MetricInfo {
                    name: "query.latency_ms".into(),
                    latest: 12.4,
                    unit: "ms".into(),
                    trend: "↘".into(),
                    retention: "90d".into(),
                },
                MetricInfo {
                    name: "kv.ops_sec".into(),
                    latest: 48000.0,
                    unit: "ops/s".into(),
                    trend: "→".into(),
                    retention: "30d".into(),
                },
                MetricInfo {
                    name: "vector.search_ms".into(),
                    latest: 0.8,
                    unit: "ms".into(),
                    trend: "↘".into(),
                    retention: "30d".into(),
                },
                MetricInfo {
                    name: "ai.tokens_sec".into(),
                    latest: 1250.0,
                    unit: "tok/s".into(),
                    trend: "↗".into(),
                    retention: "90d".into(),
                },
            ],
            selected: 0,
            sparkline_data: vec![
                20, 35, 28, 42, 55, 48, 62, 58, 71, 65, 72, 68, 55, 48, 42, 38, 45, 52, 60, 48, 42,
                38, 32, 45, 55, 62, 70, 65, 58, 42, 35, 48, 55, 62, 58, 52, 45, 40, 48, 55,
            ],
            focus: TsFocus::List,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        false
    }

    /// 进入输入模式（本页无输入框）。
    pub fn enter_input_mode(&mut self) {}

    /// 刷新数据（加载时序指标）。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.ts_metrics() {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.metrics = data
                        .iter()
                        .filter_map(|v| {
                            Some(MetricInfo {
                                name: v.get("name")?.as_str()?.to_string(),
                                latest: v.get("latest").and_then(|n| n.as_f64()).unwrap_or(0.0),
                                unit: v
                                    .get("unit")
                                    .and_then(|u| u.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                trend: v
                                    .get("trend")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("→")
                                    .to_string(),
                                retention: v
                                    .get("retention")
                                    .and_then(|r| r.as_str())
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
                    TsFocus::List => TsFocus::Detail,
                    TsFocus::Detail => TsFocus::List,
                };
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected < self.metrics.len().saturating_sub(1) {
                    self.selected += 1;
                }
            }
            _ => {}
        }
    }

    /// 绘制时序引擎页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(28), Constraint::Min(1)])
            .split(area);

        self.draw_list(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(8),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_sparkline(frame, right[1]);
        self.draw_detail(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_list(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " TS METRICS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == TsFocus::List {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, m) in self.metrics.iter().enumerate() {
            let sel = i == self.selected;
            let style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, m.name),
                style,
            )));
            lines.push(Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(
                    format!("{:.1}{}", m.latest, m.unit),
                    Style::default().fg(theme::GREEN),
                ),
                Span::styled(format!(" {} ", m.trend), Style::default().fg(theme::YELLOW)),
                Span::styled(&m.retention, Style::default().fg(theme::TEXT_DIM)),
            ]));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let m = self.metrics.get(self.selected);
        let name = m.map(|m| m.name.as_str()).unwrap_or("—");
        let bar = Line::from(vec![
            Span::styled(" ⏱ ", Style::default().fg(theme::GREEN)),
            Span::styled("TIME SERIES ENGINE", Style::default().fg(theme::GREEN)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("Metric: ", Style::default().fg(theme::TEXT_DIM)),
            Span::styled(name, Style::default().fg(theme::TEXT)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} series", self.metrics.len()),
                Style::default().fg(theme::TEXT_MUTED),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_sparkline(&self, frame: &mut Frame, area: Rect) {
        let m = self.metrics.get(self.selected);
        let title = m.map(|m| format!(" {} ", m.name)).unwrap_or_default();
        let block = Block::bordered()
            .title(Span::styled(title, Style::default().fg(theme::ACCENT)))
            .border_style(theme::border())
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let spark = Sparkline::default()
            .data(&self.sparkline_data)
            .style(Style::default().fg(theme::GREEN));
        frame.render_widget(spark, inner);
    }

    fn draw_detail(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " ALL METRICS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == TsFocus::Detail {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(vec![
            Span::styled(
                format!(" {:<24}", "METRIC"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
            Span::styled(
                format!("{:>12}", "VALUE"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
            Span::styled(
                format!("{:>8}", "TREND"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
            Span::styled(
                format!("{:>10}", "RETENTION"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
        ]));
        for (i, m) in self.metrics.iter().enumerate() {
            let sel = i == self.selected;
            let name_style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let bar_w = ((m.latest / 100.0).min(1.0) * 16.0) as usize;
            let bar = "█".repeat(bar_w) + &"░".repeat(16usize.saturating_sub(bar_w));
            lines.push(Line::from(vec![
                Span::styled(format!(" {:<24}", m.name), name_style),
                Span::styled(
                    format!("{:>8.1}{:<4}", m.latest, m.unit),
                    Style::default().fg(theme::GREEN),
                ),
                Span::styled(
                    format!("{:>8}", m.trend),
                    Style::default().fg(theme::YELLOW),
                ),
                Span::styled(
                    format!("{:>10}", m.retention),
                    Style::default().fg(theme::TEXT_DIM),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled(format!(" {:<24}", ""), Style::default()),
                Span::styled(bar, Style::default().fg(theme::BLUE)),
            ]));
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
