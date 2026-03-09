# AI 引擎

原生 Session、Context、Memory、RAG、Agent、Trace、Intent、Embedding Cache、LLM Provider、自动 Embedding、自动摘要和 Token 精确计数抽象，为 LLM 应用而生。

## 概述

AI 引擎是 Talon 的第 9 大引擎 — 专为 LLM 应用开发设计的第一性语义抽象层。它消除了对外部框架（LangChain、LlamaIndex）的依赖，提供原生的会话管理、对话上下文、语义记忆、RAG 文档管理、Agent 编排、执行追踪、意图识别、Embedding 缓存、LLM Provider 配置、自动 Embedding 生成、智能上下文压缩和精确 Token 计数。

::: tip 企业特性
AI 引擎由 `talon-ai` 提供，以商业授权的预编译库形式分发。SDK 用户的 AI 功能已内置在 `talon-bin` 二进制中，无需单独安装。
:::

## 快速开始

```rust
use talon::Talon;
use talon_ai::TalonAiExt;

let db = Talon::open("./data")?;
let ai = db.ai()?;

ai.create_session("chat-001", BTreeMap::new(), None)?;

ai.append_message("chat-001", &ContextMessage {
    role: "user".into(),
    content: "什么是 Talon？".into(),
    token_count: Some(5),
    ..Default::default()
})?;

ai.store_memory(&entry, &embedding)?;
let memories = ai.search_memory(&query_embedding, 5)?;
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
pub fn create_session(&self, id: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<Session, Error>
pub fn get_session(&self, id: &str) -> Result<Option<Session>, Error>
pub fn list_sessions(&self) -> Result<Vec<Session>, Error>
pub fn delete_session(&self, id: &str) -> Result<(), Error>  // 级联删除上下文+追踪
pub fn update_session(&self, id: &str, metadata: BTreeMap<String, String>) -> Result<Session, Error>
```

#### 幂等创建
```rust
pub fn create_session_if_not_exists(&self, id: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<(Session, bool), Error>
```
幂等创建 Session：已存在则返回现有 Session 不修改，不存在则创建。返回 `(session, is_new)`，`is_new=true` 表示本次新创建。适用于群聊等并发场景。

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
pub fn cleanup_expired_sessions(&self) -> Result<usize, Error>  // 批量清理过期 session（级联删除）
```

### 对话上下文

```rust
pub fn append_message(&self, session_id: &str, msg: &ContextMessage) -> Result<(), Error>
pub fn get_history(&self, session_id: &str, limit: Option<usize>) -> Result<Vec<ContextMessage>, Error>
pub fn get_recent_messages(&self, session_id: &str, n: usize) -> Result<Vec<ContextMessage>, Error>
pub fn get_context_window(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
pub fn get_context_window_with_prompt(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
pub fn clear_context(&self, session_id: &str) -> Result<u64, Error>
```

#### 系统提示与摘要
```rust
pub fn set_system_prompt(&self, session_id: &str, prompt: &str, token_count: u32) -> Result<(), Error>
pub fn get_system_prompt(&self, session_id: &str) -> Result<Option<String>, Error>
pub fn set_context_summary(&self, session_id: &str, summary: &str, token_count: u32) -> Result<(), Error>
pub fn get_context_summary(&self, session_id: &str) -> Result<Option<String>, Error>
```

#### 智能上下文窗口
```rust
pub fn get_context_window_smart(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
```
超长时自动触发摘要压缩：
- 如果已有摘要或对话不超长：直接返回
- 如果对话总 token > `max_tokens × 2` 且已配置 Chat Provider：自动摘要旧消息后返回
- 如果未配置 Chat Provider：退化为普通截取

#### 上下文压缩
```rust
pub fn compact_context(&self, session_id: &str, keep_recent_n: usize) -> Result<u64, Error>
```
保留最近 N 条消息，删除其余。典型用法配合 `set_context_summary`：
```rust
// 1. 先持久化摘要（即使后续 crash，摘要已保存）
ai.set_context_summary(sid, &summary_text, summary_tokens)?;
// 2. 再压缩消息
ai.compact_context(sid, 6)?;
```

- `get_context_window` — 根据 Token 预算自动截断，适配 LLM 上下文限制
- `get_context_window_with_prompt` — 构建完整 LLM 输入：system_prompt + summary + 最近消息
- `set_context_summary` — 存储对话摘要，用于超长对话

**`ContextMessage` 结构：**
```rust
pub struct ContextMessage {
    pub role: String,              // "user" | "assistant" | "system"
    pub content: String,           // 消息内容
    pub timestamp: i64,            // 自动设置（0 时自动填充当前时间）
    pub token_count: Option<u32>,  // Token 数（用于窗口管理）
}
```

### LLM Provider 配置

配置外部 LLM Provider，用于自动摘要和自动 Embedding 等功能。Chat 和 Embedding 可独立配置不同的提供商（例如 Chat 用 DeepSeek，Embedding 用本地 Ollama）。

#### `configure_llm`
```rust
pub fn configure_llm(&self, config: AiLlmConfig) -> Result<(), Error>
```

```rust
pub struct AiLlmConfig {
    pub chat: Option<LlmEndpoint>,     // 用于 auto_summarize 等
    pub embed: Option<EmbedEndpoint>,   // 用于 auto_store_memory / auto_search_memory 等
}

pub struct LlmEndpoint {
    pub base_url: String,      // 如 "https://api.openai.com/v1"、"http://localhost:11434/v1"
    pub api_key: Option<String>,
    pub model: String,         // 如 "gpt-4o-mini"、"deepseek-chat"、"qwen-turbo"、"llama3.2"
    pub max_retries: u8,       // 默认 2
    pub timeout_secs: u32,     // 默认 60
}

pub struct EmbedEndpoint {
    pub base_url: String,      // 如 "https://api.openai.com/v1"、"https://api.jina.ai/v1"
    pub api_key: Option<String>,
    pub model: String,         // 如 "text-embedding-3-small"、"bge-m3"、"nomic-embed-text"
    pub dimensions: u32,       // 必填：embedding 维度
    pub timeout_secs: u32,     // 默认 30
}
```

所有 Provider 使用 OpenAI 兼容 API 格式，支持 OpenAI、DeepSeek、Ollama、通义千问、Jina 等任何兼容端点。

#### `get_llm_config` / `clear_llm_config`
```rust
pub fn get_llm_config(&self) -> Result<Option<AiLlmConfig>, Error>
pub fn clear_llm_config(&self) -> Result<(), Error>  // 清除配置（回到手动模式）
```

### 自动 Embedding

通过已配置的 Embed Provider 自动生成 embedding，无需调用方手动计算向量。

```rust
pub fn auto_store_memory(&self, content: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<u64, Error>
pub fn auto_search_memory(&self, query: &str, k: usize) -> Result<Vec<MemorySearchResult>, Error>
```

- `auto_store_memory` — 存储记忆并自动生成 embedding
- `auto_search_memory` — 语义搜索记忆并自动生成 query embedding
- 需要先通过 `configure_llm` 配置 Embed Provider

### 自动摘要

通过已配置的 Chat Provider 自动生成上下文摘要。

```rust
pub fn auto_summarize(&self, session_id: &str, opts: SummarizeOptions) -> Result<String, Error>
```

```rust
pub struct SummarizeOptions {
    pub max_summary_tokens: u32,        // 摘要最大 token 数（默认 200）
    pub purge_old: bool,                // 是否清理已摘要的旧消息（默认 false）
    pub custom_prompt: Option<String>,  // 自定义摘要 prompt（None 用内置模板）
}
```

流程：
1. 获取 session 的全部历史消息
2. 拼接为对话文本，调用 LLM 生成摘要
3. 用 tiktoken 精确计算摘要 token 数
4. 调用 `set_context_summary` 存储
5. 若 `purge_old=true`，清理所有已有消息

### Token 精确计数

基于 tiktoken 的 BPE 精确计数，结果与 OpenAI 的 tokenizer 100% 一致。无需网络连接，词表数据编译时嵌入。

```rust
pub fn count_tokens(text: &str, encoding: TokenEncoding) -> Result<u32, Error>
pub fn count_tokens_default(text: &str) -> Result<u32, Error>   // 使用 o200k_base
pub fn count_tokens_batch(texts: &[&str], encoding: TokenEncoding) -> Result<Vec<u32>, Error>
```

```rust
pub enum TokenEncoding {
    Cl100kBase,  // GPT-4 / GPT-3.5-turbo / text-embedding-3-* 系列
    O200kBase,   // GPT-4o / o1 / o3 系列（默认）
}
```

### 语义记忆

#### 基本操作
```rust
pub fn store_memory(&self, entry: &MemoryEntry, embedding: &[f32]) -> Result<(), Error>
pub fn search_memory(&self, query_embedding: &[f32], k: usize) -> Result<Vec<MemorySearchResult>, Error>
pub fn update_memory(&self, id: u64, content: Option<&str>, metadata: Option<BTreeMap<String, String>>) -> Result<(), Error>
pub fn delete_memory(&self, id: u64) -> Result<(), Error>
pub fn memory_count(&self) -> Result<u64, Error>
```

`search_memory` 自动跳过已过期的记忆（惰性过期）。`update_memory` 更新文本和/或元数据，不改变向量；如需同时更新向量，应先 delete 再 store。

```rust
pub struct MemoryEntry {
    pub id: u64,
    pub content: String,
    pub metadata: BTreeMap<String, String>,
    pub created_at: i64,
    pub expires_at: Option<i64>,  // TTL 过期时间
}
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
pub fn cache_tool_result(&self, tool_name: &str, input_hash: &str, result: &str, ttl_secs: Option<u64>) -> Result<(), Error>
pub fn get_cached_tool_result(&self, tool_name: &str, input_hash: &str) -> Result<Option<ToolCacheEntry>, Error>
pub fn invalidate_tool_cache(&self, tool_name: &str) -> Result<u64, Error>
```
缓存昂贵的工具调用结果。`invalidate_tool_cache` 清除某工具的所有缓存。

#### Agent 状态持久化
```rust
pub fn save_agent_state(&self, agent_id: &str, step_id: &str, state: &str, metadata: BTreeMap<String, String>) -> Result<AgentStep, Error>
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
    pub state: String,         // JSON 状态，结构由调用方定义
    pub metadata: BTreeMap<String, String>,
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
2. **幂等创建** — 并发场景使用 `create_session_if_not_exists()` 避免竞态
3. **Token 管理** — 使用 `get_context_window()` 适配 LLM 上下文限制
4. **智能上下文** — 使用 `get_context_window_smart()` 自动摘要超长对话
5. **上下文压缩** — 配合 `set_context_summary()` + `compact_context()` 安全压缩上下文
6. **自动 Embedding** — 配置 Embed Provider 后使用 `auto_store_memory()` / `auto_search_memory()` 免手动计算向量
7. **精确 Token 计数** — 使用 `count_tokens()` 获取与 OpenAI 一致的精确计数
8. **系统提示** — 使用 `set_system_prompt()` 持久化系统提示，自动包含在上下文窗口中
9. **记忆去重** — 定期调用 `deduplicate_memories()` 自动去重
10. **工具缓存** — 缓存昂贵的 API 调用结果
11. **追踪** — 使用 `log_trace()` 记录所有 LLM 调用用于调试和成本追踪
12. **RAG 版本控制** — 使用 `replace_document()` 更新文档并保留版本历史
13. **Embedding 缓存** — 对文本内容哈希并缓存 embedding，降低 API 成本
