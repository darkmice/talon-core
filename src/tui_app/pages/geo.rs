/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO 地理引擎页面。
//!
//! 布局：坐标点列表 | ASCII 地图 + 坐标表格 + 围栏信息。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui_app::{net, theme};

/// GEO 地理引擎页面状态。
pub struct GeoState {
    pub points: Vec<GeoPoint>,
    pub selected: usize,
    pub fences: Vec<GeoFence>,
    pub focus: GeoFocus,
}

/// 地理坐标点。
pub struct GeoPoint {
    pub id: String,
    pub lat: f64,
    pub lon: f64,
    pub label: String,
    pub tags: String,
}

/// 地理围栏。
pub struct GeoFence {
    pub name: String,
    pub center: String,
    pub radius_km: f64,
    pub point_count: usize,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum GeoFocus {
    Points,
    Detail,
}

impl GeoState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            points: vec![
                GeoPoint {
                    id: "dc_us_west".into(),
                    lat: 37.7749,
                    lon: -122.4194,
                    label: "US-West (SF)".into(),
                    tags: "datacenter,us".into(),
                },
                GeoPoint {
                    id: "dc_us_east".into(),
                    lat: 40.7128,
                    lon: -74.0060,
                    label: "US-East (NYC)".into(),
                    tags: "datacenter,us".into(),
                },
                GeoPoint {
                    id: "dc_eu".into(),
                    lat: 52.5200,
                    lon: 13.4050,
                    label: "EU (Berlin)".into(),
                    tags: "datacenter,eu".into(),
                },
                GeoPoint {
                    id: "dc_asia".into(),
                    lat: 31.2304,
                    lon: 121.4737,
                    label: "Asia (Shanghai)".into(),
                    tags: "datacenter,asia".into(),
                },
                GeoPoint {
                    id: "dc_jp".into(),
                    lat: 35.6762,
                    lon: 139.6503,
                    label: "JP (Tokyo)".into(),
                    tags: "datacenter,asia".into(),
                },
                GeoPoint {
                    id: "edge_sg".into(),
                    lat: 1.3521,
                    lon: 103.8198,
                    label: "Edge (Singapore)".into(),
                    tags: "edge,asia".into(),
                },
                GeoPoint {
                    id: "edge_au".into(),
                    lat: -33.8688,
                    lon: 151.2093,
                    label: "Edge (Sydney)".into(),
                    tags: "edge,oceania".into(),
                },
            ],
            selected: 0,
            fences: vec![
                GeoFence {
                    name: "us_region".into(),
                    center: "39.8, -98.5".into(),
                    radius_km: 2500.0,
                    point_count: 2,
                },
                GeoFence {
                    name: "eu_region".into(),
                    center: "50.1, 9.2".into(),
                    radius_km: 1500.0,
                    point_count: 1,
                },
                GeoFence {
                    name: "asia_pacific".into(),
                    center: "25.0, 120.0".into(),
                    radius_km: 4000.0,
                    point_count: 4,
                },
            ],
            focus: GeoFocus::Points,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        false
    }

    /// 进入输入模式（本页无输入框）。
    pub fn enter_input_mode(&mut self) {}

    /// 刷新数据。
    pub fn refresh(&mut self, _client: &mut Option<net::TuiClient>) {
        // GEO 点列表刷新（服务端 API 就绪后接入）
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, _client: &mut Option<net::TuiClient>) {
        match key.code {
            KeyCode::Tab => {
                self.focus = match self.focus {
                    GeoFocus::Points => GeoFocus::Detail,
                    GeoFocus::Detail => GeoFocus::Points,
                };
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected < self.points.len().saturating_sub(1) {
                    self.selected += 1;
                }
            }
            _ => {}
        }
    }

    /// 绘制 GEO 地理引擎页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(28), Constraint::Min(1)])
            .split(area);

        self.draw_points(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(9),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_map(frame, right[1]);
        self.draw_detail(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_points(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " GEO POINTS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == GeoFocus::Points {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, p) in self.points.iter().enumerate() {
            let sel = i == self.selected;
            let style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, p.label),
                style,
            )));
            lines.push(Line::from(Span::styled(
                format!("     {:.4}, {:.4}", p.lat, p.lon),
                Style::default().fg(theme::TEXT_DIM),
            )));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let bar = Line::from(vec![
            Span::styled(" 🌍 ", Style::default().fg(theme::GREEN)),
            Span::styled("GEO ENGINE", Style::default().fg(theme::GREEN)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} points", self.points.len()),
                Style::default().fg(theme::TEXT),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} fences", self.fences.len()),
                Style::default().fg(theme::TEXT),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("Geohash 52-bit", Style::default().fg(theme::TEXT_MUTED)),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_map(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " WORLD MAP ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let map = vec![
            Line::from(Span::styled(
                "          ·  ·  EU·        ·       · JP·  ·",
                Style::default().fg(theme::TEXT_DIM),
            )),
            Line::from(Span::styled(
                "   ·  ·  NYC·  ·  ·  ·  ·  ·  · SH·  TK·",
                Style::default().fg(theme::TEXT_DIM),
            )),
            Line::from(Span::styled(
                " SF·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·SG·  ·",
                Style::default().fg(theme::TEXT_DIM),
            )),
            Line::from(Span::styled(
                "  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ",
                Style::default().fg(theme::TEXT_DIM),
            )),
            Line::from(Span::styled(
                "  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  · SY·  · ",
                Style::default().fg(theme::TEXT_DIM),
            )),
        ];
        frame.render_widget(Paragraph::new(map), inner);
    }

    fn draw_detail(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
            .split(area);

        // 坐标表格
        let tbl_block = Block::bordered()
            .title(Span::styled(
                " COORDINATES ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == GeoFocus::Detail {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let ti = tbl_block.inner(layout[0]);
        frame.render_widget(tbl_block, layout[0]);

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(vec![
            Span::styled(
                format!(" {:<14}", "ID"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
            Span::styled(
                format!("{:<11}", "LAT"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
            Span::styled(
                format!("{:<11}", "LON"),
                Style::default().fg(theme::TEXT_DIM).bold(),
            ),
            Span::styled("TAGS", Style::default().fg(theme::TEXT_DIM).bold()),
        ]));
        for (i, p) in self.points.iter().enumerate() {
            let sel = i == self.selected;
            let style = if sel {
                Style::default().fg(theme::ACCENT)
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            lines.push(Line::from(vec![
                Span::styled(format!(" {:<14}", p.id), style),
                Span::styled(
                    format!("{:<11.4}", p.lat),
                    Style::default().fg(theme::GREEN),
                ),
                Span::styled(format!("{:<11.4}", p.lon), Style::default().fg(theme::BLUE)),
                Span::styled(&p.tags, Style::default().fg(theme::TEXT_DIM)),
            ]));
        }
        frame.render_widget(Paragraph::new(lines), ti);

        // 围栏信息
        let fence_block = Block::bordered()
            .title(Span::styled(
                " GEOFENCES ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(theme::TERMINAL_BG));
        let fi = fence_block.inner(layout[1]);
        frame.render_widget(fence_block, layout[1]);

        let mut flines: Vec<Line> = Vec::new();
        for f in &self.fences {
            flines.push(Line::from(vec![
                Span::styled(
                    format!(" {:<16}", f.name),
                    Style::default().fg(theme::ACCENT),
                ),
                Span::styled(
                    format!("center: {:<14}", f.center),
                    Style::default().fg(theme::TEXT_MUTED),
                ),
                Span::styled(
                    format!("r={:.0}km", f.radius_km),
                    Style::default().fg(theme::YELLOW),
                ),
                Span::styled(
                    format!("  {} pts", f.point_count),
                    Style::default().fg(theme::GREEN),
                ),
            ]));
        }
        frame.render_widget(Paragraph::new(flines), fi);
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
