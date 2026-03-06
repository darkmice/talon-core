/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 图引擎页面。
//!
//! 布局：节点列表 | 边关系 + 节点属性 + 图算法。

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui_app::{net, theme};

/// 图引擎页面状态。
pub struct GraphState {
    pub nodes: Vec<NodeInfo>,
    pub selected_node: usize,
    pub edges: Vec<EdgeInfo>,
    pub focus: GraphFocus,
}

/// 图节点。
pub struct NodeInfo {
    pub id: String,
    pub label: String,
    pub degree: usize,
    pub properties: Vec<(String, String)>,
}

/// 图边。
pub struct EdgeInfo {
    pub from: String,
    pub to: String,
    pub label: String,
    pub weight: f64,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum GraphFocus {
    Nodes,
    Detail,
}

impl GraphState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            nodes: vec![
                NodeInfo {
                    id: "n_001".into(),
                    label: "User:Alice".into(),
                    degree: 5,
                    properties: vec![
                        ("role".into(), "admin".into()),
                        ("dept".into(), "engineering".into()),
                    ],
                },
                NodeInfo {
                    id: "n_002".into(),
                    label: "User:Bob".into(),
                    degree: 3,
                    properties: vec![
                        ("role".into(), "dev".into()),
                        ("dept".into(), "backend".into()),
                    ],
                },
                NodeInfo {
                    id: "n_003".into(),
                    label: "Project:Talon".into(),
                    degree: 8,
                    properties: vec![
                        ("type".into(), "database".into()),
                        ("lang".into(), "rust".into()),
                    ],
                },
                NodeInfo {
                    id: "n_004".into(),
                    label: "Doc:Architecture".into(),
                    degree: 4,
                    properties: vec![
                        ("format".into(), "markdown".into()),
                        ("pages".into(), "42".into()),
                    ],
                },
                NodeInfo {
                    id: "n_005".into(),
                    label: "Team:Core".into(),
                    degree: 6,
                    properties: vec![
                        ("size".into(), "5".into()),
                        ("focus".into(), "storage".into()),
                    ],
                },
                NodeInfo {
                    id: "n_006".into(),
                    label: "Issue:Perf".into(),
                    degree: 2,
                    properties: vec![
                        ("priority".into(), "P0".into()),
                        ("status".into(), "open".into()),
                    ],
                },
            ],
            selected_node: 0,
            edges: vec![
                EdgeInfo {
                    from: "Alice".into(),
                    to: "Talon".into(),
                    label: "MAINTAINS".into(),
                    weight: 1.0,
                },
                EdgeInfo {
                    from: "Bob".into(),
                    to: "Talon".into(),
                    label: "CONTRIBUTES".into(),
                    weight: 0.8,
                },
                EdgeInfo {
                    from: "Alice".into(),
                    to: "Core".into(),
                    label: "LEADS".into(),
                    weight: 1.0,
                },
                EdgeInfo {
                    from: "Bob".into(),
                    to: "Core".into(),
                    label: "MEMBER_OF".into(),
                    weight: 0.5,
                },
                EdgeInfo {
                    from: "Talon".into(),
                    to: "Architecture".into(),
                    label: "HAS_DOC".into(),
                    weight: 0.6,
                },
                EdgeInfo {
                    from: "Core".into(),
                    to: "Talon".into(),
                    label: "OWNS".into(),
                    weight: 1.0,
                },
                EdgeInfo {
                    from: "Alice".into(),
                    to: "Issue:Perf".into(),
                    label: "ASSIGNED".into(),
                    weight: 0.9,
                },
                EdgeInfo {
                    from: "Talon".into(),
                    to: "Issue:Perf".into(),
                    label: "HAS_ISSUE".into(),
                    weight: 0.7,
                },
            ],
            focus: GraphFocus::Nodes,
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
        // Graph 节点列表刷新（服务端 API 就绪后接入）
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, _client: &mut Option<net::TuiClient>) {
        match key.code {
            KeyCode::Tab => {
                self.focus = match self.focus {
                    GraphFocus::Nodes => GraphFocus::Detail,
                    GraphFocus::Detail => GraphFocus::Nodes,
                };
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected_node = self.selected_node.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected_node < self.nodes.len().saturating_sub(1) {
                    self.selected_node += 1;
                }
            }
            _ => {}
        }
    }

    /// 绘制图引擎页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(28), Constraint::Min(1)])
            .split(area);

        self.draw_nodes(frame, cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Percentage(50),
                Constraint::Percentage(50),
                Constraint::Length(1),
            ])
            .split(cols[1]);

        self.draw_status(frame, right[0]);
        self.draw_edges(frame, right[1]);
        self.draw_properties(frame, right[2]);
        self.draw_keys(frame, right[3]);
    }

    fn draw_nodes(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " GRAPH NODES ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == GraphFocus::Nodes {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        for (i, n) in self.nodes.iter().enumerate() {
            let sel = i == self.selected_node;
            let style = if sel {
                Style::default().fg(theme::ACCENT).bold()
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { " ▶ " } else { "   " };
            lines.push(Line::from(Span::styled(
                format!("{}{}", prefix, n.label),
                style,
            )));
            lines.push(Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(&n.id, Style::default().fg(theme::TEXT_DIM)),
                Span::styled(
                    format!("  deg:{}", n.degree),
                    Style::default().fg(theme::PURPLE),
                ),
            ]));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let bar = Line::from(vec![
            Span::styled(" ⊛ ", Style::default().fg(theme::YELLOW)),
            Span::styled("GRAPH ENGINE", Style::default().fg(theme::YELLOW)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} nodes", self.nodes.len()),
                Style::default().fg(theme::TEXT),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                format!("{} edges", self.edges.len()),
                Style::default().fg(theme::TEXT),
            ),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled(
                "BFS · Dijkstra · PageRank",
                Style::default().fg(theme::TEXT_MUTED),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_edges(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " EDGES / RELATIONSHIPS ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == GraphFocus::Detail {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let lines: Vec<Line> = self
            .edges
            .iter()
            .map(|e| {
                Line::from(vec![
                    Span::styled(format!(" {:<12}", e.from), Style::default().fg(theme::BLUE)),
                    Span::styled(
                        format!("──{}──▶ ", e.label),
                        Style::default().fg(theme::YELLOW),
                    ),
                    Span::styled(format!("{:<12}", e.to), Style::default().fg(theme::GREEN)),
                    Span::styled(
                        format!("w={:.1}", e.weight),
                        Style::default().fg(theme::TEXT_DIM),
                    ),
                ])
            })
            .collect();
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_properties(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " NODE PROPERTIES ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(theme::border())
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        if let Some(node) = self.nodes.get(self.selected_node) {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled(" ID:     ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(&node.id, Style::default().fg(theme::ACCENT)),
                ]),
                Line::from(vec![
                    Span::styled(" Label:  ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(&node.label, Style::default().fg(theme::TEXT).bold()),
                ]),
                Line::from(vec![
                    Span::styled(" Degree: ", Style::default().fg(theme::TEXT_DIM)),
                    Span::styled(
                        format!("{}", node.degree),
                        Style::default().fg(theme::PURPLE),
                    ),
                ]),
                Line::from(""),
            ];
            for (k, v) in &node.properties {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("   {}: ", k),
                        Style::default().fg(theme::TEXT_MUTED),
                    ),
                    Span::styled(v, Style::default().fg(theme::TEXT_SECONDARY)),
                ]));
            }
            frame.render_widget(Paragraph::new(lines), inner);
        }
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
