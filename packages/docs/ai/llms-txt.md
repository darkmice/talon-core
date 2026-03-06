# llms.txt

The `llms.txt` file is served at `/llms.txt` and provides a concise API summary for LLM context windows.

The full version at `/llms-full.txt` contains complete API signatures for all 9 engines.

## How to Use

### In LLM System Prompts

```
Refer to the Talon API documentation at: https://docs.talon.dev/llms-full.txt
```

### In RAG Pipelines

```python
import requests
docs = requests.get("https://docs.talon.dev/llms-full.txt").text
# Chunk and index into your vector database
```

### Direct Download

- [llms.txt](/llms.txt) — Concise version (~500 tokens)
- [llms-full.txt](/llms-full.txt) — Full version (~3000 tokens)
