# AI 引擎

原生 Session、Context、Memory、RAG、Agent、Trace、Intent 和 Embedding Cache 抽象，为 LLM 应用而生。

## 概述

AI 引擎是 Talon 的第 9 大引擎 — 专为 LLM 应用开发设计的第一性语义抽象层。它消除了对外部框架（LangChain、LlamaIndex）的依赖，提供原生的会话管理、对话上下文、语义记忆、RAG 文档管理、Agent 编排、执行追踪、意图识别和 Embedding 缓存。

## 快速开始

```rust
use talon::{Talon, ContextMessage};
use std::collections::BTreeMap;

let db = Talon::open("./data")?;
let ai = db.ai()?;

ai.create_session("chat-001", BTreeMap::new(), None)?;

ai.append_message("chat-001", &ContextMessage {
    role: "user".into(),
    content: "什么是 Talon？".into(),
    token_count: Some(5),
})?;

ai.store_memory("chat-001", "用户偏好 Rust", &embedding, None)?;
let memories = ai.search_memories("chat-001", &query_embedding, 5)?;
```

## API 参考

### 获取引擎句柄

```rust
pub fn ai(&self) -> Result<AiEngine, Error>      // 写模式（Replica 返回 ReadOnly 错误）
pub fn ai_read(&self) -> Result<AiEngine, Error>  // 只读模式
```

### 会话管理

#### 基本 CRUD
```rust
pub fn create_session(&self, id: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<(), Error>
pub fn get_session(&self, id: &str) -> Result<Option<Session>, Error>
pub fn list_sessions(&self) -> Result<Vec<Session>, Error>
pub fn delete_session(&self, id: &str) -> Result<(), Error>  // 级联删除上下文+追踪
pub fn update_session(&self, id: &str, metadata: BTreeMap<String, String>) -> Result<(), Error>
```

#### 标签
```rust
pub fn add_session_tags(&self, id: &str, tags: &[String]) -> Result<(), Error>
pub fn remove_session_tags(&self, id: &str, tags: &[String]) -> Result<(), Error>
pub fn get_session_tags(&self, session_id: &str) -> Result<Vec<String>, Error>
pub fn search_sessions_by_tag(&self, tag: &str) -> Result<Vec<Session>, Error>
```

#### 归档
```rust
pub fn archive_session(&self, id: &str) -> Result<(), Error>
pub fn unarchive_session(&self, id: &str) -> Result<(), Error>
pub fn list_archived_sessions(&self) -> Result<Vec<Session>, Error>
pub fn search_sessions_by_metadata(&self, key: &str, value: &str) -> Result<Vec<Session>, Error>
```

#### 导出与统计
```rust
pub fn export_session(&self, session_id: &str) -> Result<ExportedSession, Error>
pub fn export_sessions(&self, session_ids: &[&str]) -> Result<Vec<ExportedSession>, Error>
pub fn session_stats(&self, session_id: &str) -> Result<SessionStats, Error>
pub fn sessions_stats(&self, session_ids: &[&str]) -> Result<Vec<SessionStats>, Error>
pub fn cleanup_expired_sessions(&self) -> Result<usize, Error>
```

### 对话上下文

```rust
pub fn append_message(&self, session_id: &str, msg: &ContextMessage) -> Result<(), Error>
pub fn get_history(&self, session_id: &str, last_n: usize) -> Result<Vec<ContextMessage>, Error>
pub fn get_recent_messages(&self, session_id: &str, limit: usize) -> Result<Vec<ContextMessage>, Error>
pub fn get_context_window(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
pub fn get_context_window_with_prompt(&self, session_id: &str, max_tokens: u32) -> Result<(Option<String>, Vec<ContextMessage>), Error>
pub fn clear_context(&self, session_id: &str) -> Result<u64, Error>
```

#### 系统提示与摘要
```rust
pub fn set_system_prompt(&self, session_id: &str, prompt: &str) -> Result<(), Error>
pub fn get_system_prompt(&self, session_id: &str) -> Result<Option<String>, Error>
pub fn set_context_summary(&self, session_id: &str, summary: &str) -> Result<(), Error>
pub fn get_context_summary(&self, session_id: &str) -> Result<Option<String>, Error>
```

- `get_context_window` — 根据 Token 预算自动截断，适配 LLM 上下文限制
- `get_context_window_with_prompt` — 同时返回系统提示
- `set_context_summary` — 存储对话摘要，用于超长对话

**`ContextMessage` 结构：**
```rust
pub struct ContextMessage {
    pub role: String,              // "user" | "assistant" | "system"
    pub content: String,           // 消息内容
    pub token_count: Option<u32>,  // Token 数（用于窗口管理）
}
```

### 语义记忆

#### 基本操作
```rust
pub fn store_memory(&self, entry: &MemoryEntry, embedding: &[f32]) -> Result<(), Error>
pub fn search_memory(&self, query_embedding: &[f32], k: usize) -> Result<Vec<MemoryEntry>, Error>
pub fn update_memory(&self, id: u64, entry: &MemoryEntry, embedding: &[f32]) -> Result<(), Error>
pub fn delete_memory(&self, id: u64) -> Result<(), Error>
pub fn memory_count(&self) -> Result<u64, Error>
```

#### 高级操作
```rust
pub fn search_memory_with_filter(&self, query_embedding: &[f32], k: usize, filter: impl Fn(&MemoryEntry) -> bool) -> Result<Vec<MemoryEntry>, Error>
pub fn list_memories(&self, offset: usize, limit: usize) -> Result<Vec<MemoryEntry>, Error>
pub fn store_memory_with_ttl(&self, entry: &MemoryEntry, embedding: &[f32], ttl_secs: u64) -> Result<(), Error>
pub fn store_memories_batch(&self, entries: &[(&MemoryEntry, &[f32])]) -> Result<(), Error>
```

#### 去重与清理
```rust
pub fn find_duplicate_memories(&self, threshold: f32) -> Result<Vec<DuplicatePair>, Error>
pub fn deduplicate_memories(&self, threshold: f32) -> Result<usize, Error>
pub fn cleanup_expired_memories(&self) -> Result<usize, Error>
pub fn memory_stats(&self) -> Result<MemoryStats, Error>
```

### RAG 文档管理

#### 文档 CRUD
```rust
pub fn store_document(&self, doc: &RagDocumentWithChunks) -> Result<u64, Error>
pub fn store_document_with_ttl(&self, doc: &RagDocumentWithChunks, ttl_secs: u64) -> Result<u64, Error>
pub fn get_document(&self, doc_id: u64) -> Result<Option<RagDocumentWithChunks>, Error>
pub fn list_documents(&self) -> Result<Vec<RagDocument>, Error>
pub fn delete_document(&self, doc_id: u64) -> Result<(), Error>
pub fn document_count(&self) -> Result<usize, Error>
pub fn replace_document(&self, doc_id: u64, doc: &RagDocumentWithChunks) -> Result<(), Error>
pub fn replace_document_with_ttl(&self, doc_id: u64, doc: &RagDocumentWithChunks, ttl_secs: u64) -> Result<(), Error>
pub fn store_documents_batch(&self, docs: &[RagDocumentWithChunks]) -> Result<Vec<u64>, Error>
pub fn delete_documents_batch(&self, doc_ids: &[u64]) -> Result<usize, Error>
pub fn cleanup_expired_documents(&self) -> Result<usize, Error>
```

#### Chunk 级操作
```rust
pub fn get_chunk(&self, chunk_id: u64) -> Result<Option<RagChunk>, Error>
pub fn update_chunk(&self, chunk_id: u64, text: &str, embedding: &[f32]) -> Result<(), Error>
pub fn delete_chunk(&self, chunk_id: u64) -> Result<(), Error>
```

#### 搜索
```rust
pub fn search_chunks(&self, query_embedding: &[f32], k: usize) -> Result<Vec<RagSearchResult>, Error>
pub fn search_chunks_hybrid(&self, query_text: &str, query_embedding: &[f32], k: usize) -> Result<Vec<RagSearchResult>, Error>
pub fn search_chunks_by_keyword(&self, keyword: &str, limit: usize) -> Result<Vec<RagSearchResult>, Error>
```

#### 版本控制与元数据
```rust
pub fn get_document_version(&self, doc_id: u64) -> Result<Option<u64>, Error>
pub fn list_document_versions(&self, doc_id: u64) -> Result<Vec<u64>, Error>
pub fn get_document_at_version(&self, doc_id: u64, version: u64) -> Result<Option<RagDocumentWithChunks>, Error>
pub fn document_stats(&self) -> Result<(usize, usize, usize), Error>  // (文档数, chunk数, 总字节)
pub fn search_documents_by_metadata(&self, key: &str, value: &str) -> Result<Vec<RagDocument>, Error>
```

**数据类型：**
```rust
pub struct RagDocumentWithChunks {
    pub document: RagDocument,
    pub chunks: Vec<RagChunkInput>,
}
pub struct RagDocument {
    pub id: u64, pub title: String,
    pub source: Option<String>, pub metadata: Option<serde_json::Value>,
}
pub struct RagChunkInput {
    pub text: String, pub embedding: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}
```

### Agent 原语

#### 工具调用缓存
```rust
pub fn cache_tool_result(&self, tool_name: &str, args_hash: &str, result: &[u8], ttl_secs: Option<u64>) -> Result<(), Error>
pub fn get_cached_tool_result(&self, tool_name: &str, args_hash: &str) -> Result<Option<Vec<u8>>, Error>
pub fn invalidate_tool_cache(&self, tool_name: &str) -> Result<u64, Error>
```

#### Agent 状态持久化
```rust
pub fn save_agent_state(&self, agent_id: &str, step: &AgentStep) -> Result<(), Error>
pub fn get_agent_state(&self, agent_id: &str) -> Result<Option<AgentStep>, Error>
pub fn list_agent_steps(&self, agent_id: &str) -> Result<Vec<AgentStep>, Error>
pub fn get_agent_step_count(&self, agent_id: &str) -> Result<usize, Error>
pub fn rollback_agent_to_step(&self, agent_id: &str, step_id: &str) -> Result<usize, Error>
pub fn clear_agent_steps(&self, agent_id: &str) -> Result<usize, Error>
pub fn delete_agent_state(&self, agent_id: &str) -> Result<(), Error>
```

- `rollback_agent_to_step` — 移除指定检查点之后的所有步骤，返回移除数量
- `clear_agent_steps` — 清空 agent 所有步骤
- `delete_agent_state` — 删除 agent 所有数据

```rust
pub struct AgentStep {
    pub step_id: String,
    pub action: String,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub timestamp_ms: i64,
}
```

### 执行追踪

#### 记录
```rust
pub fn log_trace(&self, record: &TraceRecord) -> Result<(), Error>
```

```rust
pub struct TraceRecord {
    pub run_id: String,
    pub session_id: Option<String>,
    pub operation: String,   // "llm_call", "tool_call", "embedding" 等
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub latency_ms: u64,
    pub token_usage: Option<u32>,
}
```

#### 查询
```rust
pub fn query_traces_by_session(&self, session_id: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_by_run(&self, run_id: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_by_operation(&self, operation: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_by_session_and_operation(&self, session_id: &str, operation: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_in_range(&self, start_ms: i64, end_ms: i64) -> Result<Vec<TraceRecord>, Error>
pub fn export_traces(&self, session_id: Option<&str>) -> Result<Vec<TraceRecord>, Error>
```

#### 分析
```rust
pub fn get_token_usage(&self, session_id: &str) -> Result<i64, Error>
pub fn get_token_usage_by_run(&self, run_id: &str) -> Result<i64, Error>
pub fn trace_stats(&self, session_id: Option<&str>) -> Result<TraceStats, Error>
pub fn trace_performance_report(&self, session_id: Option<&str>) -> Result<serde_json::Value, Error>
```
- `trace_stats` — 聚合统计：总数、Token 用量、按操作类型分组
- `trace_performance_report` — 延迟统计、慢操作检测

### Embedding 缓存

```rust
pub fn cache_embedding(&self, content_hash: &str, embedding: &[f32]) -> Result<(), Error>
pub fn get_cached_embedding(&self, content_hash: &str) -> Result<Option<Vec<f32>>, Error>
pub fn invalidate_embedding_cache(&self) -> Result<u64, Error>
pub fn embedding_cache_count(&self) -> Result<usize, Error>
```
缓存 embedding 向量，避免重复调用外部 embedding 服务 API。

### 意图识别

```rust
pub fn query_by_intent(&self, query: &IntentQuery) -> Result<IntentResult, Error>
```

```rust
pub struct IntentQuery { pub text: String, pub context: Option<Vec<ContextMessage>> }
pub struct IntentResult { pub kind: IntentKind, pub confidence: f32, pub suggested_action: Option<String> }
pub enum IntentKind { Sql, Kv, Vector, Fts, Geo, Graph, Ts, Mq, Ai, Unknown }
```

将自然语言查询路由到合适的引擎。

## 最佳实践

1. **会话生命周期** — 使用 TTL 自动清理过期会话
2. **Token 管理** — 使用 `get_context_window()` 适配 LLM 上下文限制
3. **系统提示** — 使用 `set_system_prompt()` 持久化系统提示，自动包含在上下文窗口中
4. **记忆去重** — 定期调用 `deduplicate_memories()` 自动去重
5. **工具缓存** — 缓存昂贵的 API 调用结果
6. **追踪** — 使用 `log_trace()` 记录所有 LLM 调用用于调试和成本追踪
7. **RAG 版本控制** — 使用 `replace_document()` 更新文档并保留版本历史
8. **Embedding 缓存** — 对文本内容哈希并缓存 embedding，降低 API 成本
