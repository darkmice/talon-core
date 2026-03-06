/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import { defineConfig } from 'vitepress'

const enginesSidebar = (prefix: string) => [
  {
    text: prefix === '/zh/' ? '数据引擎' : 'Data Engines',
    items: [
      { text: prefix === '/zh/' ? 'SQL 引擎' : 'SQL Engine', link: `${prefix}engines/sql` },
      { text: prefix === '/zh/' ? 'KV 引擎' : 'KV Engine', link: `${prefix}engines/kv` },
      { text: prefix === '/zh/' ? '时序引擎' : 'TimeSeries Engine', link: `${prefix}engines/timeseries` },
      { text: prefix === '/zh/' ? '消息队列引擎' : 'MessageQueue Engine', link: `${prefix}engines/message-queue` },
      { text: prefix === '/zh/' ? '向量引擎' : 'Vector Engine', link: `${prefix}engines/vector` },
      { text: prefix === '/zh/' ? '全文搜索引擎' : 'Full-Text Search Engine', link: `${prefix}engines/full-text-search` },
      { text: prefix === '/zh/' ? 'GEO 地理引擎' : 'GEO Engine', link: `${prefix}engines/geo` },
      { text: prefix === '/zh/' ? '图引擎' : 'Graph Engine', link: `${prefix}engines/graph` },
      { text: prefix === '/zh/' ? 'AI 引擎' : 'AI Engine', link: `${prefix}engines/ai` },
    ],
  },
  {
    text: prefix === '/zh/' ? '跨引擎查询' : 'Cross-Engine',
    items: [
      { text: prefix === '/zh/' ? '融合查询' : 'Fusion Queries', link: `${prefix}engines/fusion-queries` },
    ],
  },
]

const guideSidebar = (prefix: string) => [
  {
    text: prefix === '/zh/' ? '入门指南' : 'Getting Started',
    items: [
      { text: prefix === '/zh/' ? '安装' : 'Installation', link: `${prefix}guide/installation` },
      { text: prefix === '/zh/' ? '快速开始' : 'Quick Start', link: `${prefix}guide/getting-started` },
      { text: prefix === '/zh/' ? '嵌入式 vs Server' : 'Embedded vs Server', link: `${prefix}guide/embedded-vs-server` },
    ],
  },
  {
    text: prefix === '/zh/' ? '多语言 SDK' : 'Language SDKs',
    items: [
      { text: prefix === '/zh/' ? 'SDK 概览' : 'SDK Overview', link: `${prefix}guide/sdk` },
      { text: 'Go', link: `${prefix}guide/sdk-go` },
      { text: 'Python', link: `${prefix}guide/sdk-python` },
      { text: 'Node.js', link: `${prefix}guide/sdk-nodejs` },
      { text: 'Java', link: `${prefix}guide/sdk-java` },
      { text: '.NET', link: `${prefix}guide/sdk-dotnet` },
    ],
  },
]

const serverSidebar = (prefix: string) => [
  {
    text: prefix === '/zh/' ? '服务端模式' : 'Server Mode',
    items: [
      { text: 'HTTP API', link: `${prefix}server/http-api` },
      { text: prefix === '/zh/' ? 'TCP 协议' : 'TCP Protocol', link: `${prefix}server/tcp-protocol` },
      { text: prefix === '/zh/' ? 'Redis 协议' : 'Redis Protocol', link: `${prefix}server/redis-protocol` },
    ],
  },
]

const aiSidebar = (prefix: string) => [
  {
    text: prefix === '/zh/' ? 'AI 可消费文档' : 'AI-Consumable Docs',
    items: [
      { text: prefix === '/zh/' ? '概述' : 'Overview', link: `${prefix}ai/overview` },
      { text: 'llms.txt', link: `${prefix}ai/llms-txt` },
      { text: 'OpenAPI Spec', link: `${prefix}ai/openapi` },
      { text: prefix === '/zh/' ? 'MCP 集成' : 'MCP Integration', link: `${prefix}ai/mcp` },
      { text: 'Agent Skills', link: `${prefix}ai/agent-skills` },
    ],
  },
]

export default defineConfig({
  title: 'Talon',
  description: 'AI-Native Multi-Model Data Engine — API Documentation',
  base: '/talon-docs/',
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/logo.svg' }],
  ],

  locales: {
    root: {
      label: 'English',
      lang: 'en-US',
    },
    zh: {
      label: '中文',
      lang: 'zh-CN',
      description: 'AI 原生多模融合数据引擎 — API 文档',
      themeConfig: {
        nav: [
          { text: '指南', link: '/zh/guide/getting-started' },
          { text: 'SDK', link: '/zh/guide/sdk' },
          { text: '引擎', link: '/zh/engines/sql' },
          { text: '服务端 API', link: '/zh/server/http-api' },
          { text: 'AI 文档', link: '/zh/ai/overview' },
          { text: 'GitHub', link: 'https://github.com/darkmice/talon-core' },
        ],
        sidebar: {
          '/zh/guide/': guideSidebar('/zh/'),
          '/zh/engines/': enginesSidebar('/zh/'),
          '/zh/server/': serverSidebar('/zh/'),
          '/zh/ai/': aiSidebar('/zh/'),
        },
        outline: { label: '目录' },
        docFooter: { prev: '上一页', next: '下一页' },
        lastUpdated: { text: '最后更新' },
        returnToTopLabel: '返回顶部',
        sidebarMenuLabel: '菜单',
        darkModeSwitchLabel: '深色模式',
      },
    },
  },

  themeConfig: {
    logo: '/logo.svg',

    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'SDK', link: '/guide/sdk' },
      { text: 'Engines', link: '/engines/sql' },
      { text: 'Server API', link: '/server/http-api' },
      { text: 'AI Docs', link: '/ai/overview' },
      { text: 'GitHub', link: 'https://github.com/darkmice/talon-core' },
    ],

    sidebar: {
      '/guide/': guideSidebar('/'),
      '/engines/': enginesSidebar('/'),
      '/server/': serverSidebar('/'),
      '/ai/': aiSidebar('/'),
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/darkmice/talon-core' },
    ],

    search: {
      provider: 'local',
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2024-present Talon Contributors',
    },

    outline: {
      level: [2, 3],
    },
  },
})
