/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 设计稿 1 — KV Explorer 仪表盘。
//!
//! 布局：信息栏 | 统计卡片 | KV 列表 + 详情面板 | 系统日志。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui_app::{net, theme};

/// 仪表盘页面状态。
pub struct DashboardState {
    pub stats: EngineStats,
    pub kv_keys: Vec<String>,
    pub kv_selected: usize,
    pub kv_detail: Option<KvDetail>,
    pub logs: Vec<LogEntry>,
    pub keys_loaded: bool,
}

/// 引擎统计摘要。
pub struct EngineStats {
    pub sql_tables: String,
    pub kv_keys: String,
    pub vector_count: String,
    pub ai_latency: String,
    pub streams: String,
    pub graph_nodes: String,
    pub cache_hit: String,
    pub health: String,
}

/// KV 键详情。
pub struct KvDetail {
    pub key: String,
    pub value: String,
    pub size: usize,
    pub value_type: String,
}

/// 系统日志条目。
pub struct LogEntry {
    pub time: String,
    pub level: String,
    pub message: String,
}

impl DashboardState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            stats: EngineStats::demo(),
            kv_keys: vec![
                "session:anon_001",
                "session:anon_002",
                "session:user_123",
                "config:app_theme",
                "config:rate_limit",
                "cache:page_home",
                "cache:api_stats",
                "job:queue_01",
                "job:queue_02",
                "metric:cpu_avg",
                "metric:mem_peak",
                "user:pref:1002",
                "user:pref:1003",
                "sys:lock:migration",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
            kv_selected: 2,
            kv_detail: Some(KvDetail {
                key: "session:user_123".into(),
                value: concat!(
                    "{\n",
                    "  \"user_id\": \"u_882910\",\n",
                    "  \"role\": \"admin\",\n",
                    "  \"permissions\": [\"read:all\", \"write:logs\"],\n",
                    "  \"context\": {\n",
                    "    \"last_page\": \"/dashboard\",\n",
                    "    \"theme\": \"dark\",\n",
                    "    \"vector_context_id\": \"vc_9912\"\n",
                    "  },\n",
                    "  \"session_start\": 1698157321\n",
                    "}"
                )
                .into(),
                size: 482,
                value_type: "JSON Document".into(),
            }),
            logs: LogEntry::demo_logs(),
            keys_loaded: false,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        false
    }

    /// 进入输入模式（本页无输入框）。
    pub fn enter_input_mode(&mut self) {}

    /// 刷新数据。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        self.refresh_keys(client);
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, client: &mut Option<net::TuiClient>) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                if self.kv_selected > 0 {
                    self.kv_selected -= 1;
                    self.load_detail(client);
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.kv_selected < self.kv_keys.len().saturating_sub(1) {
                    self.kv_selected += 1;
                    self.load_detail(client);
                }
            }
            _ => {}
        }
    }

    /// 从服务端加载 KV 键列表。
    // TODO(Phase2): kv_keys("") 空前缀全量扫描，大库需加分页/流式加载
    pub fn refresh_keys(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.kv_keys("") {
                if let Some(data) = resp.get("data").and_then(|d| d.as_array()) {
                    self.kv_keys = data
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect();
                    self.kv_selected = 0;
                    self.keys_loaded = true;
                    self.load_detail(client);
                }
            }
        }
    }

    fn load_detail(&mut self, client: &mut Option<net::TuiClient>) {
        let key = match self.kv_keys.get(self.kv_selected) {
            Some(k) => k.clone(),
            None => return,
        };
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.kv_get(&key) {
                if let Some(data) = resp.get("data") {
                    let value =
                        serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string());
                    let size = value.len();
                    self.kv_detail = Some(KvDetail {
                        key,
                        value,
                        size,
                        value_type: "JSON".into(),
                    });
                }
            }
        }
    }

    /// 绘制整个仪表盘页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // 信息栏
                Constraint::Length(3), // 统计卡片
                Constraint::Min(8),    // KV 列表 + 详情
                Constraint::Length(8), // 系统日志
            ])
            .split(area);

        self.draw_info_bar(frame, layout[0]);
        self.draw_stats(frame, layout[1]);
        self.draw_main(frame, layout[2]);
        self.draw_logs(frame, layout[3]);
    }

    fn draw_info_bar(&self, frame: &mut Frame, area: Rect) {
        let bar = Block::default().style(Style::default().bg(theme::BORDER_DIM));
        frame.render_widget(bar, area);

        let left = Line::from(vec![Span::styled(
            " Talon-CLI v2.4.0 - connected",
            Style::default().fg(theme::TEXT_SECONDARY),
        )]);
        let right = Line::from(vec![Span::styled(
            "j/k Navigate  g/G Top/Bot  i Input  ? Help ",
            Style::default().fg(theme::TEXT_DIM),
        )]);

        frame.render_widget(Paragraph::new(left), area);
        if area.width > 50 {
            let rw = 44u16.min(area.width);
            let ra = Rect::new(area.x + area.width - rw, area.y, rw, 1);
            frame.render_widget(Paragraph::new(right).alignment(Alignment::Right), ra);
        }
    }

    fn draw_stats(&self, frame: &mut Frame, area: Rect) {
        let cards: &[(&str, &str, &str, Color)] = &[
            ("SQL ENGINE", &self.stats.sql_tables, "Tables", theme::BLUE),
            ("KV STORE", &self.stats.kv_keys, "Keys", theme::PURPLE),
            (
                "VECTOR",
                &self.stats.vector_count,
                "Embeddings",
                theme::GREEN,
            ),
            ("AI", &self.stats.ai_latency, "Latency", theme::YELLOW),
            ("STREAMS", &self.stats.streams, "Pipelines", theme::TEXT),
            ("GRAPH", &self.stats.graph_nodes, "Nodes", theme::TEXT),
            ("CACHE HIT", &self.stats.cache_hit, "Ratio", theme::TEXT),
            ("HEALTH", &self.stats.health, "Status", theme::GREEN),
        ];

        let n = cards.len().min((area.width as usize) / 10).max(1);
        let constraints: Vec<Constraint> = (0..n).map(|_| Constraint::Ratio(1, n as u32)).collect();
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(constraints)
            .split(area);

        for (i, &(label, value, _sub, color)) in cards.iter().enumerate().take(n) {
            let block = Block::bordered()
                .border_style(theme::border())
                .style(Style::default().bg(theme::TERMINAL_BG));
            let inner = block.inner(cols[i]);
            frame.render_widget(block, cols[i]);
            if inner.height >= 1 {
                frame.render_widget(
                    Paragraph::new(Span::styled(label, Style::default().fg(theme::TEXT_DIM))),
                    Rect::new(inner.x, inner.y, inner.width, 1),
                );
            }
            if inner.height >= 2 {
                frame.render_widget(
                    Paragraph::new(Span::styled(value, theme::stat_value(color))),
                    Rect::new(inner.x, inner.y + 1, inner.width, 1),
                );
            }
        }
    }

    fn draw_main(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
            .split(area);

        self.draw_kv_list(frame, cols[0]);
        self.draw_kv_detail(frame, cols[1]);
    }

    fn draw_kv_list(&self, frame: &mut Frame, area: Rect) {
        let items: Vec<ListItem> = self
            .kv_keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let style = if i == self.kv_selected {
                    Style::default()
                        .fg(theme::BLUE)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(theme::TEXT_MUTED)
                };
                ListItem::new(Line::from(Span::styled(key.as_str(), style)))
            })
            .collect();

        let block = Block::bordered()
            .title(Span::styled(
                " KV EXPLORER [DEFAULT] ",
                Style::default().fg(theme::TEXT_SECONDARY),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(theme::TERMINAL_BG));

        let list = List::new(items)
            .block(block)
            .highlight_style(theme::list_highlight())
            .highlight_symbol("▶ ");

        let mut state = ListState::default();
        state.select(Some(self.kv_selected));
        frame.render_stateful_widget(list, area, &mut state);
    }

    fn draw_kv_detail(&self, frame: &mut Frame, area: Rect) {
        let title = match self.kv_detail {
            Some(ref d) => format!(" DETAILS: {} ", d.key),
            None => " DETAILS: (none) ".into(),
        };
        let block = Block::bordered()
            .title(Span::styled(
                title,
                Style::default().fg(theme::TEXT_SECONDARY),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(Color::Rgb(17, 17, 22)));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        if let Some(ref d) = self.kv_detail {
            let detail_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(1),
                    Constraint::Min(1),
                ])
                .split(inner);

            // 元信息行
            let meta_cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(detail_layout[0]);

            let size_block = Block::bordered().border_style(theme::border());
            let si = size_block.inner(meta_cols[0]);
            frame.render_widget(size_block, meta_cols[0]);
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(Span::styled("Size", Style::default().fg(theme::TEXT_DIM))),
                    Line::from(Span::styled(
                        format!("{} bytes", d.size),
                        Style::default().fg(Color::White),
                    )),
                ]),
                si,
            );

            let type_block = Block::bordered().border_style(theme::border());
            let ti = type_block.inner(meta_cols[1]);
            frame.render_widget(type_block, meta_cols[1]);
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(Span::styled("Type", Style::default().fg(theme::TEXT_DIM))),
                    Line::from(Span::styled(
                        &d.value_type,
                        Style::default().fg(theme::PURPLE),
                    )),
                ]),
                ti,
            );

            // VALUE CONTENT 标签
            frame.render_widget(
                Paragraph::new(Span::styled(
                    " VALUE CONTENT",
                    Style::default().fg(theme::TEXT_DIM),
                )),
                detail_layout[1],
            );

            // JSON 内容
            let json_block = Block::bordered()
                .border_style(theme::border())
                .style(Style::default().bg(Color::Black));
            frame.render_widget(
                Paragraph::new(d.value.as_str())
                    .style(Style::default().fg(theme::TEXT_SECONDARY))
                    .block(json_block)
                    .wrap(Wrap { trim: false }),
                detail_layout[2],
            );
        }
    }

    fn draw_logs(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " SYSTEM LOG [TAIL -F] ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(Color::Black));

        let log_lines: Vec<Line> = self
            .logs
            .iter()
            .map(|e| {
                Line::from(vec![
                    Span::styled(&e.time, Style::default().fg(theme::TEXT_DIM)),
                    Span::raw(" "),
                    Span::styled(format!("{:<7}", e.level), theme::log_level(&e.level)),
                    Span::raw(" "),
                    Span::styled(&e.message, Style::default().fg(theme::TEXT_SECONDARY)),
                ])
            })
            .collect();

        frame.render_widget(Paragraph::new(log_lines).block(block), area);
    }
}

impl EngineStats {
    fn demo() -> Self {
        Self {
            sql_tables: "12".into(),
            kv_keys: "1.2M".into(),
            vector_count: "100K".into(),
            ai_latency: "45ms".into(),
            streams: "4".into(),
            graph_nodes: "8.4k".into(),
            cache_hit: "94%".into(),
            health: "OK".into(),
        }
    }
}

impl LogEntry {
    fn demo_logs() -> Vec<Self> {
        vec![
            LogEntry {
                time: "14:23:45".into(),
                level: "INFO".into(),
                message: "[SQL] Vacuum process started on table 'users'".into(),
            },
            LogEntry {
                time: "14:23:48".into(),
                level: "INFO".into(),
                message: "[KV] Snapshot taken: snap_4812".into(),
            },
            LogEntry {
                time: "14:24:01".into(),
                level: "SUCCESS".into(),
                message: "Client connected from 192.168.1.42".into(),
            },
            LogEntry {
                time: "14:24:12".into(),
                level: "WARN".into(),
                message: "Vector search latency spiked to 205ms (threshold: 200ms)".into(),
            },
            LogEntry {
                time: "14:24:15".into(),
                level: "INFO".into(),
                message: "[AI] Model 'llama-2-7b' loaded into GPU memory (VRAM: 14GB)".into(),
            },
            LogEntry {
                time: "14:24:18".into(),
                level: "SUCCESS".into(),
                message: "Query OK: SELECT * FROM audit_logs LIMIT 10".into(),
            },
        ]
    }
}
