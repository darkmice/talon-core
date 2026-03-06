# Agent Skills

Talon 提供预置的 [Agent Skills](https://agentskills.io)，让 AI 编程助手自动掌握 Talon 的 API 用法。安装后，你的 AI 助手会自动了解如何编写 Talon 代码 —— SQL、KV、Vector、AI 引擎及全部 9 大引擎。

## 包含内容

| 文件 | 内容 |
|------|------|
| `SKILL.md` | 引擎选择指南、快速上手示例、AI 最佳实践 |
| `references/sql.md` | SQL 完整方言参考（DDL/DML/函数/CTE/事务） |
| `references/kv.md` | KV API（CRUD/TTL/计数器/扫描/快照） |
| `references/vector.md` | Vector API（HNSW/metadata filter/量化） |
| `references/ai.md` | AI 引擎（Session/Context/Memory/RAG/Agent/Trace） |
| `references/more-engines.md` | 时序、消息队列、全文搜索、GEO、图、融合查询 API |
| `references/sdk.md` | Go/Python/Node.js/Java/.NET SDK 用法 |

## 支持工具

Talon skill 遵循 [Agent Skills 开放标准](https://agentskills.io)，支持所有主流 AI 编程工具：

| 工具 | 厂商 | 类型 | Skills 路径 |
|------|------|------|-------------|
| [Claude Code](https://claude.ai) | Anthropic | CLI | `.claude/skills/` |
| [Cursor](https://cursor.sh) | Cursor | IDE | `.cursor/skills/` |
| [Windsurf](https://windsurf.com) | Codeium | IDE | `.windsurf/skills/` |
| [Cline](https://github.com/cline/cline) | 社区 | VSCode 插件 | `.cline/skills/` |
| [Gemini CLI](https://github.com/google-gemini/gemini-cli) | Google | CLI | `.gemini/skills/` |
| [Codex CLI](https://github.com/openai/codex) | OpenAI | CLI | `.codex/skills/` |
| [Kiro](https://kiro.dev) | AWS | IDE/CLI | `.kiro/skills/` |
| [OpenCode](https://opencode.ai) | 社区 | CLI | `.agents/skills/` |
| [Antigravity](https://github.com/google-deepmind/antigravity) | DeepMind | IDE | `.agent/skills/` |
| [AdaL CLI](https://sylph.ai) | SylphAI | CLI | `.adal/skills/` |

## 安装方式

### 方式一：npx skills（推荐）

使用 [skills.sh](https://skills.sh) CLI —— 一条命令，自动检测已安装的 IDE：

```bash
# 安装到当前项目（自动检测已安装的 AI 工具）
npx skills add darkmice/talon-docs

# 全局安装（所有项目可用）
npx skills add darkmice/talon-docs -g

# 仅安装到指定工具
npx skills add darkmice/talon-docs -a claude-code -a cursor

# 安装前查看可用 skill 列表
npx skills add darkmice/talon-docs --list
```

### 方式二：克隆文档仓库

```bash
git clone https://github.com/darkmice/talon-docs.git
```

在 AI IDE 中打开克隆的目录，skill 已为全部 10 个工具预装 —— 你的 AI 助手会自动发现它。

### 方式三：复制到项目中

将 skill 目录复制到你的项目中：

```bash
# Claude Code
mkdir -p .claude/skills
cp -r talon-docs/.claude/skills/talon .claude/skills/talon

# Cursor
mkdir -p .cursor/skills
cp -r talon-docs/.cursor/skills/talon .cursor/skills/talon

# Gemini CLI
mkdir -p .gemini/skills
cp -r talon-docs/.gemini/skills/talon .gemini/skills/talon

# 其他工具类似，替换对应目录名即可
```

### 方式四：手动全局安装

安装一次，所有项目都能使用：

```bash
# Claude Code（全局）
cp -r talon-docs/.claude/skills/talon ~/.claude/skills/talon

# Gemini CLI（全局）
cp -r talon-docs/.gemini/skills/talon ~/.gemini/skills/talon

# Kiro（全局）
cp -r talon-docs/.kiro/skills/talon ~/.kiro/skills/talon
```

## 使用方式

安装后，直接描述你想用 Talon 做什么，AI 助手会自动激活 skill：

> "用 Talon 创建一个向量索引，搜索相似的 embedding"

> "用 Talon 的 AI 引擎和混合搜索搭建 RAG 管道"

> "写一个 Go 程序，用 Talon KV 做带 TTL 的会话缓存"

也可以显式调用（在支持的工具中）：

```
/talon 帮我搭建一个对话管理系统
```

## 工作原理

1. **发现** — 启动时，AI 助手仅读取 `SKILL.md` 的 `name` 和 `description`（frontmatter）
2. **激活** — 当任务匹配 Talon 使用场景时，助手加载完整的 `SKILL.md` 指令
3. **渐进式加载** — 需要详细 API 时，助手按需加载特定的 reference 文件（如查询 SQL 时加载 `references/sql.md`）

这种方式保持上下文高效利用，同时让助手在需要时能获取完整文档。

## 仓库地址

- **Skills 源码**：[github.com/darkmice/talon-docs](https://github.com/darkmice/talon-docs)（gh-pages 分支）
- **Agent Skills 标准**：[agentskills.io](https://agentskills.io)
