# Redis 协议 (RESP)

Talon 的 Redis 兼容接口，使用任意 Redis 客户端进行 KV 操作。

## 概述

Talon 实现了 Redis RESP 协议，任何 Redis 客户端库都可以连接并执行 KV 操作。

## 连接

```bash
redis-cli -p 6380
```

## 支持的命令

### 字符串操作

```
SET key value [EX seconds]
GET key
DEL key [key ...]
MSET key1 value1 key2 value2
MGET key1 key2
SETNX key value
GETSET key value
APPEND key value
STRLEN key
GETRANGE key start end
SETRANGE key offset value
INCR key
INCRBY key increment
DECR key
DECRBY key decrement
INCRBYFLOAT key increment
```

### Key 操作

```
EXISTS key
EXPIRE key seconds
PEXPIRE key milliseconds
TTL key
PTTL key
PERSIST key
EXPIREAT key timestamp
EXPIRETIME key
RENAME key newkey
TYPE key
RANDOMKEY
KEYS pattern
DBSIZE
```

### 服务端

```
PING
INFO
COMMAND COUNT
```

## 示例

```bash
$ redis-cli -p 6380
127.0.0.1:6380> SET user:1 '{"name":"Alice","age":30}'
OK
127.0.0.1:6380> GET user:1
"{\"name\":\"Alice\",\"age\":30}"
127.0.0.1:6380> EXPIRE user:1 3600
(integer) 1
127.0.0.1:6380> TTL user:1
(integer) 3599
127.0.0.1:6380> INCR counter
(integer) 1
127.0.0.1:6380> KEYS user:*
1) "user:1"
```

## 限制

- 仅支持 KV 操作（通过 Redis 协议）
- 不支持 Redis 数据结构（List、Set、Hash、Sorted Set）
- 完整引擎访问请使用 HTTP API 或嵌入式 Rust API
