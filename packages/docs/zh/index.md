---
layout: home

hero:
  name: Talon
  text: AI 原生多模融合数据引擎
  tagline: 9 大引擎合一 · 单二进制 · 零依赖 · 嵌入式 + Server 双模
  actions:
    - theme: brand
      text: 快速开始
      link: /zh/guide/getting-started
    - theme: alt
      text: 引擎 API
      link: /zh/engines/sql
    - theme: alt
      text: GitHub
      link: https://github.com/darkmice/talon-core

features:
  - icon: 🗃️
    title: SQL 引擎
    details: 完整关系型数据库，窗口函数、多表 JOIN、UPSERT、MVCC 事务
  - icon: ⚡
    title: KV 引擎
    details: Redis 兼容键值存储，TTL、原子计数器、批量操作，744K ops/s
  - icon: 📈
    title: 时序引擎
    details: 时间序列存储，降采样聚合、保留策略、InfluxDB 行协议导入
  - icon: 📬
    title: 消息队列引擎
    details: 内置消息队列，消费者组、死信队列、优先级、延迟消息，1.6M msg/s
  - icon: 🧠
    title: 向量引擎
    details: 自研 HNSW 索引，推荐/发现 API、元数据过滤、SQ8 量化
  - icon: 🔍
    title: 全文搜索引擎
    details: 倒排索引 + BM25 评分，ES 兼容查询、中文分词、混合搜索
  - icon: 🌍
    title: GEO 地理引擎
    details: Geohash 空间索引，Redis GEO 兼容、圆形/矩形搜索、地理围栏
  - icon: 🕸️
    title: 图引擎
    details: 属性图模型，BFS/DFS 遍历、最短路径、PageRank
  - icon: 🤖
    title: AI 引擎
    details: 原生 Session/Context/Memory/RAG/Agent/Trace 抽象，LLM Provider 配置、自动 Embedding/摘要、精确 Token 计数
---
