# llms.txt

`llms.txt` 文件在 `/llms.txt` 提供，为 LLM 上下文窗口提供精简的 API 概要。

完整版本在 `/llms-full.txt`，包含所有 9 大引擎的完整 API 签名。

## 如何使用

### 在 LLM 系统提示中

```
参考 Talon API 文档：https://docs.talon.dev/llms-full.txt
```

### 在 RAG 管道中

```python
import requests
docs = requests.get("https://docs.talon.dev/llms-full.txt").text
# 分块并索引到向量数据库
```

### 直接下载

- [llms.txt](/llms.txt) — 精简版（~500 tokens）
- [llms-full.txt](/llms-full.txt) — 完整版（~3000 tokens）
