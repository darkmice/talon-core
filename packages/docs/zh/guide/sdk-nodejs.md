# Node.js SDK

Node.js SDK 通过 `koffi` 加载 `libtalon.dylib`/`.so`。

## 安装

```bash
npm install talon-db
```

Native library 在 `npm install` 时自动从 GitHub Releases 下载，无需手动编译。

## 快速开始

```javascript
const { Talon } = require('./nodejs');
const db = new Talon('./data');
db.sql('SELECT 1 + 1 AS result');
db.close();
```

## API 速查

### SQL

```javascript
db.sql('CREATE TABLE users (id INT, name TEXT)');
```

### KV

```javascript
db.kvSet('key', 'value');
const val = db.kvGet('key');
db.kvSetNx('lock', '1', 30);
db.kvIncrBy('counter', 10);
db.kvDecrBy('counter', 5);
const keys = db.kvKeysLimit('user:', 0, 100);
const count = db.kvCount();
```

### FTS（全文搜索）

```javascript
db.ftsCreateIndex('articles');
db.ftsIndex('articles', 'doc1', { title: 'Hello World' });
const hits = db.ftsSearch('articles', 'hello', 10);
const fuzzy = db.ftsSearchFuzzy('articles', 'helo', 2, 10);
const hybrid = db.ftsHybridSearch('articles', 'vecs', 'query', vec, {
  ftsWeight: 0.7, vecWeight: 0.3,
});
```

### Geo（地理空间）

```javascript
db.geoCreate('shops');
db.geoAdd('shops', 'starbucks', 121.47, 31.23);
const nearby = db.geoSearch('shops', 121.47, 31.23, 1000);
const inside = db.geoFence('shops', 'starbucks', 121.47, 31.23, 500);
const members = db.geoMembers('shops');
```

### Graph（图引擎）

```javascript
db.graphCreate('social');
const v1 = db.graphAddVertex('social', 'person', { name: 'Alice' });
const v2 = db.graphAddVertex('social', 'person', { name: 'Bob' });
db.graphAddEdge('social', v1, v2, 'knows');
const path = db.graphShortestPath('social', v1, v2);
const pagerank = db.graphPagerank('social', 0.85, 20, 10);
```

### AI（Session / Context / Memory / Trace）

```javascript
db.aiCreateSession('s1');
db.aiAppendMessage('s1', { role: 'user', content: 'hi' });
const history = db.aiGetHistory('s1');
db.aiStoreMemory('s1', { key: 'pref', value: 'dark' });
```

### Vector / TS / MQ / Backup / Ops

```javascript
db.vectorInsert('idx', 1, [0.1, 0.2]);
db.tsInsert('metrics', { cpu: 85.5 });
db.mqPublish('events', { type: 'login' });
db.exportDb('/backup');
db.persist();
```
