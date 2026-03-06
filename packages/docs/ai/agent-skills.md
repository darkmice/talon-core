# Agent Skills

Talon provides pre-built [Agent Skills](https://agentskills.io) that teach AI coding assistants how to use Talon's APIs correctly. Once installed, your AI agent automatically knows how to write Talon code — SQL, KV, Vector, AI Engine, and all 9 engines.

## What's Included

The Talon skill includes:

| File | Content |
|------|---------|
| `SKILL.md` | Engine selection guide, quick start patterns, AI best practices |
| `references/sql.md` | Full SQL dialect reference (DDL/DML/functions/CTE/transactions) |
| `references/kv.md` | KV API (CRUD/TTL/counter/scan/snapshot) |
| `references/vector.md` | Vector API (HNSW/metadata filter/quantization) |
| `references/ai.md` | AI Engine (Session/Context/Memory/RAG/Agent/Trace) |
| `references/more-engines.md` | TimeSeries, MQ, FTS, GEO, Graph, Fusion APIs |
| `references/sdk.md` | Go/Python/Node.js/Java/.NET SDK usage patterns |

## Supported Tools

Talon skill follows the [Agent Skills open standard](https://agentskills.io) and works with all major AI coding tools:

| Tool | Vendor | Type | Skills Path |
|------|--------|------|-------------|
| [Claude Code](https://claude.ai) | Anthropic | CLI | `.claude/skills/` |
| [Cursor](https://cursor.sh) | Cursor | IDE | `.cursor/skills/` |
| [Windsurf](https://windsurf.com) | Codeium | IDE | `.windsurf/skills/` |
| [Cline](https://github.com/cline/cline) | Community | VSCode Extension | `.cline/skills/` |
| [Gemini CLI](https://github.com/google-gemini/gemini-cli) | Google | CLI | `.gemini/skills/` |
| [Codex CLI](https://github.com/openai/codex) | OpenAI | CLI | `.codex/skills/` |
| [Kiro](https://kiro.dev) | AWS | IDE/CLI | `.kiro/skills/` |
| [OpenCode](https://opencode.ai) | Community | CLI | `.agents/skills/` |
| [Antigravity](https://github.com/google-deepmind/antigravity) | DeepMind | IDE | `.agent/skills/` |
| [AdaL CLI](https://sylph.ai) | SylphAI | CLI | `.adal/skills/` |

## Installation

### Option 1: npx skills (recommended)

Using the [skills.sh](https://skills.sh) CLI — one command, auto-detects your IDE:

```bash
# Install to current project (auto-detects installed agents)
npx skills add darkmice/talon-docs

# Install globally (available across all projects)
npx skills add darkmice/talon-docs -g

# Install for specific agents only
npx skills add darkmice/talon-docs -a claude-code -a cursor

# List available skills before installing
npx skills add darkmice/talon-docs --list
```

### Option 2: Clone the docs repo

```bash
git clone https://github.com/darkmice/talon-docs.git
```

Open the cloned directory in your AI IDE. The skill is pre-installed for all 10 tools — your agent will automatically discover it.

### Option 3: Copy to your project

Copy the skill directory into your project for a specific tool:

```bash
# For Claude Code
mkdir -p .claude/skills
cp -r talon-docs/.claude/skills/talon .claude/skills/talon

# For Cursor
mkdir -p .cursor/skills
cp -r talon-docs/.cursor/skills/talon .cursor/skills/talon

# For Gemini CLI
mkdir -p .gemini/skills
cp -r talon-docs/.gemini/skills/talon .gemini/skills/talon

# For any other tool, replace the directory name accordingly
```

### Option 4: Global install (manual)

Install once, available everywhere:

```bash
# Claude Code (global)
cp -r talon-docs/.claude/skills/talon ~/.claude/skills/talon

# Gemini CLI (global)
cp -r talon-docs/.gemini/skills/talon ~/.gemini/skills/talon

# Kiro (global)
cp -r talon-docs/.kiro/skills/talon ~/.kiro/skills/talon
```

## Usage

Once installed, just describe what you want to do with Talon. Your AI agent will automatically activate the skill:

> "Create a vector index and search for similar embeddings using Talon"

> "Set up a RAG pipeline with Talon's AI engine and hybrid search"

> "Write a Go program that uses Talon KV for session caching with TTL"

You can also invoke the skill explicitly (in tools that support it):

```
/talon help me set up a conversation management system
```

## How It Works

1. **Discovery** — At startup, your AI agent reads the skill's `name` and `description` from `SKILL.md` frontmatter
2. **Activation** — When your task matches Talon usage, the agent loads the full `SKILL.md` instructions
3. **Progressive disclosure** — For detailed API info, the agent loads specific reference files on demand (e.g., `references/sql.md` for SQL queries)

This keeps context usage efficient while giving the agent access to comprehensive documentation when needed.

## Repository

- **Skills source**: [github.com/darkmice/talon-docs](https://github.com/darkmice/talon-docs) (gh-pages branch)
- **Agent Skills standard**: [agentskills.io](https://agentskills.io)
