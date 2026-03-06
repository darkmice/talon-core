---
layout: home

hero:
  name: Talon
  text: AI-Native Multi-Model Data Engine
  tagline: 9 engines in one binary — SQL · KV · TimeSeries · MQ · Vector · FTS · GEO · Graph · AI
  actions:
    - theme: brand
      text: Getting Started
      link: /guide/getting-started
    - theme: alt
      text: Engine APIs
      link: /engines/sql
    - theme: alt
      text: GitHub
      link: https://github.com/darkmice/talon-core

features:
  - icon: 🗄️
    title: SQL Engine
    details: Full relational database with window functions, JOINs, UPSERT, transactions, and PostgreSQL/MySQL compatibility.
  - icon: ⚡
    title: KV Engine
    details: Redis-compatible key-value store with TTL, batch operations, and 744K ops/s throughput.
  - icon: 📈
    title: TimeSeries Engine
    details: High-performance time-series storage with downsampling, retention policies, and 540K pts/s ingestion.
  - icon: 📬
    title: MessageQueue Engine
    details: Built-in message queue with consumer groups, dead letter queues, and 1.6M msg/s throughput.
  - icon: 🧭
    title: Vector Engine
    details: Self-built HNSW index with recommend, discover, metadata filtering, and SQ8 quantization.
  - icon: 🔍
    title: Full-Text Search
    details: Inverted index + BM25 scoring, Elasticsearch-compatible queries, Chinese tokenizer, hybrid search.
  - icon: 🌍
    title: GEO Engine
    details: Geohash-based spatial indexing with Redis GEO command compatibility and GEOSEARCHSTORE.
  - icon: 🔗
    title: Graph Engine
    details: Property graph with BFS/DFS traversal, shortest path, PageRank, and 935K reads/s.
  - icon: 🤖
    title: AI Engine
    details: Native Session, Context, Memory, RAG, Agent, Trace, Intent abstractions for LLM applications.
---
