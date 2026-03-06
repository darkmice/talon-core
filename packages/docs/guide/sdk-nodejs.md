# Node.js SDK

The Node.js SDK uses `koffi` to load `libtalon.dylib`/`.so` at runtime.

## Installation

```bash
npm install talon-db
```

Native library is auto-downloaded during `npm install` via postinstall script. No compilation required.

## Quick Start

```javascript
const { Talon } = require('talon-db');

const db = new Talon('./data');

db.sql('SELECT 1 + 1 AS result');

db.close();
```

## SQL

```javascript
db.sql('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
db.sql("INSERT INTO users VALUES (1, 'Alice')");
const rows = db.sql('SELECT * FROM users WHERE id = 1');
```

## KV

```javascript
// Basic CRUD
db.kvSet('user:1', 'Alice');
const val = db.kvGet('user:1');
db.kvDel('user:1');
const exists = db.kvExists('user:1');

// TTL
db.kvSet('session:abc', 'token', 3600);
db.kvExpire('session:abc', 1800);
const remaining = db.kvTtl('session:abc');

// Atomic operations
let newVal = db.kvIncr('counter');
newVal = db.kvIncrBy('counter', 10);
newVal = db.kvDecrBy('counter', 5);
const wasSet = db.kvSetNx('lock:job1', 'worker1', 30);

// Batch
db.kvMset(['k1', 'k2'], ['v1', 'v2']);
const vals = db.kvMget(['k1', 'k2']);

// Scan
const keys = db.kvKeys('user:');
const matched = db.kvKeysMatch('user:*');
const paged = db.kvKeysLimit('user:', 0, 100);
const pairs = db.kvScanLimit('user:', 0, 100);
const count = db.kvCount();
```

## Vector

```javascript
db.vectorCreate('embeddings', 384, 'cosine');
db.vectorInsert('embeddings', 1, [0.1, 0.2, ...]);
const results = db.vectorSearch('embeddings', queryVec, 10, 'cosine');
db.vectorDelete('embeddings', 1);
const info = db.vectorInfo('embeddings');
```

## Time Series

```javascript
db.tsCreate('metrics', ['host'], ['cpu', 'mem']);
db.tsInsert('metrics', { host: 'srv1', cpu: 85.5, mem: 4096 });
const points = db.tsQuery('metrics', { start: '2024-01-01T00:00:00Z' });
const agg = db.tsAggregate('metrics', 'cpu', 'avg');
```

## Message Queue

```javascript
db.mqCreate('events');
db.mqPublish('events', { type: 'login', user: 'alice' });
const msgs = db.mqPoll('events', 10);
db.mqAck('events', msgId);
db.mqSubscribe('events', 'consumer1');
```

## Full-Text Search

```javascript
db.ftsCreateIndex('articles');
db.ftsIndex('articles', 'doc1', {
  title: 'Introduction to AI',
  body: 'Artificial intelligence is transforming...',
});
const hits = db.ftsSearch('articles', 'artificial intelligence', 10);
const fuzzy = db.ftsSearchFuzzy('articles', 'artifcial', 2, 10);

// Hybrid search (BM25 + vector)
const hybrid = db.ftsHybridSearch('articles', 'vectors', 'AI search', queryVec, {
  metric: 'cosine', limit: 10,
  ftsWeight: 0.7, vecWeight: 0.3,
});

db.ftsAddAlias('articles_v2', 'articles');
db.ftsReindex('articles');
const mapping = db.ftsGetMapping('articles');
const indexes = db.ftsListIndexes();
```

## Geospatial

```javascript
db.geoCreate('shops');
db.geoAdd('shops', 'starbucks', 121.4737, 31.2304);
db.geoAddBatch('shops', [
  { key: 'mcdonalds', lng: 121.48, lat: 31.235 },
  { key: 'kfc', lng: 121.465, lat: 31.228 },
]);

// Position & distance
const pos = db.geoPos('shops', 'starbucks');
const dist = db.geoDist('shops', 'starbucks', 'mcdonalds', 'km');

// Search
const nearby = db.geoSearch('shops', 121.47, 31.23, 1000, 'm');
const inBox = db.geoSearchBox('shops', 121.46, 31.22, 121.49, 31.24);
const inside = db.geoFence('shops', 'starbucks', 121.47, 31.23, 500);
const members = db.geoMembers('shops');
```

## Graph

```javascript
db.graphCreate('social');

// Vertices
const v1 = db.graphAddVertex('social', 'person', { name: 'Alice' });
const v2 = db.graphAddVertex('social', 'person', { name: 'Bob' });
const vertex = db.graphGetVertex('social', v1);
db.graphUpdateVertex('social', v1, { name: 'Alice W.' });

// Edges
const e1 = db.graphAddEdge('social', v1, v2, 'knows', { since: '2024' });
const edge = db.graphGetEdge('social', e1);

// Traversal
const neighbors = db.graphNeighbors('social', v1, 'out');
const outEdges = db.graphOutEdges('social', v1);
const inEdges = db.graphInEdges('social', v2);
const byLabel = db.graphVerticesByLabel('social', 'person');

// Algorithms
const path = db.graphShortestPath('social', v1, v2);
const wPath = db.graphWeightedShortestPath('social', v1, v2, 10, 'weight');
const bfs = db.graphBfs('social', v1, 3);
const centrality = db.graphDegreeCentrality('social', 10);
const pagerank = db.graphPagerank('social', 0.85, 20, 10);

// Stats
const vCount = db.graphVertexCount('social');
const eCount = db.graphEdgeCount('social');
```

## AI (Session / Context / Memory / Trace)

```javascript
// Session management
db.aiCreateSession('s1');
const session = db.aiGetSession('s1');
db.aiDeleteSession('s1');
const sessions = db.aiListSessions(10);

// Context / Messages
db.aiAppendMessage('s1', { role: 'user', content: 'What is Talon?' });
const history = db.aiGetHistory('s1');
db.aiClearContext('s1');

// Memory
db.aiStoreMemory('s1', { key: 'preference', value: 'dark mode' });
const memories = db.aiSearchMemory('s1', 'preference', 10);

// Trace
db.aiLogTrace('s1', { event: 'llm_call', model: 'gpt-4', latency_ms: 230 });
const traces = db.aiQueryTraces('s1');
```

## Backup & Ops

```javascript
const exported = db.exportDb('/backup/dir');
const imported = db.importDb('/backup/dir');

const stats = db.databaseStats();
const health = db.healthCheck();
db.persist();
```
