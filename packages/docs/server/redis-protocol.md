# Redis Protocol (RESP)

Talon's Redis-compatible interface for KV operations using any Redis client.

## Overview

Talon implements the Redis RESP protocol, allowing any Redis client library to connect and perform KV operations.

## Connection

```bash
redis-cli -p 6380
```

## Supported Commands

### String Operations

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

### Key Operations

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

### Server

```
PING
INFO
COMMAND COUNT
```

## Example

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

## Limitations

- Only KV operations are supported via Redis protocol
- No Redis data structures (List, Set, Hash, Sorted Set)
- For full engine access, use the HTTP API or embedded Rust API
