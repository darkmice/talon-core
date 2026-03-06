/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
/**
 * Talon — 面向本地 AI 的多模融合数据引擎
 * C ABI 头文件
 *
 * 用法：链接 libtalon.dylib / libtalon.so / talon.dll
 * 所有函数返回 0 表示成功，-1 表示失败（除 talon_open 返回指针）。
 */

#ifndef TALON_H
#define TALON_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** 不透明数据库句柄。 */
typedef struct TalonHandle TalonHandle;

/**
 * 打开数据库；成功返回句柄指针，失败返回 NULL。
 * @param path  数据目录路径（null-terminated UTF-8）
 */
TalonHandle *talon_open(const char *path);

/**
 * 关闭数据库并释放句柄。
 * @param handle  talon_open 返回的指针，只能调用一次。
 */
void talon_close(TalonHandle *handle);

/**
 * 执行 SQL 语句；结果以 JSON 写入 *out_json。
 * @param handle    数据库句柄
 * @param sql       SQL 语句（null-terminated UTF-8）
 * @param out_json  成功时写入 JSON 字符串指针，需用 talon_free_string 释放
 * @return 0 成功，-1 失败
 */
int talon_run_sql(const TalonHandle *handle, const char *sql, char **out_json);

/**
 * KV SET。
 * @param ttl_secs  TTL 秒数；<=0 表示无过期
 * @return 0 成功，-1 失败
 */
int talon_kv_set(const TalonHandle *handle,
                 const uint8_t *key, size_t key_len,
                 const uint8_t *value, size_t value_len,
                 int64_t ttl_secs);

/**
 * KV GET。
 * @param out_value  成功时写入数据指针，需用 talon_free_bytes 释放；不存在时为 NULL
 * @param out_len    数据长度；不存在时为 0
 * @return 0 成功，-1 失败
 */
int talon_kv_get(const TalonHandle *handle,
                 const uint8_t *key, size_t key_len,
                 uint8_t **out_value, size_t *out_len);

/**
 * KV DEL。
 * @return 0 成功，-1 失败
 */
int talon_kv_del(const TalonHandle *handle,
                 const uint8_t *key, size_t key_len);

/**
 * KV INCRBY — 原子自增 delta 步长；key 不存在视为 0。
 * @param delta     自增步长（可为负数实现自减）
 * @param out_value 成功时写入增后的值
 * @return 0 成功，-1 失败
 */
int talon_kv_incrby(const TalonHandle *handle,
                    const uint8_t *key, size_t key_len,
                    int64_t delta, int64_t *out_value);

/**
 * KV SETNX — key 不存在时写入，已存在时不操作。
 * @param was_set  成功时写入 1（已写入）或 0（key 已存在未操作）
 * @return 0 成功，-1 失败
 */
int talon_kv_setnx(const TalonHandle *handle,
                   const uint8_t *key, size_t key_len,
                   const uint8_t *value, size_t value_len,
                   int64_t ttl_secs, int *was_set);

/**
 * 向量插入。
 * @param index_name  索引名称（null-terminated UTF-8）
 * @param id          向量 ID
 * @param vec_data    f32 数组指针
 * @param vec_dim     向量维度
 * @return 0 成功，-1 失败
 */
int talon_vector_insert(const TalonHandle *handle,
                        const char *index_name,
                        uint64_t id,
                        const float *vec_data, size_t vec_dim);

/**
 * 向量 KNN 搜索；结果以 JSON 写入 *out_json。
 * @param metric  距离度量："cosine" / "l2" / "dot"
 * @return 0 成功，-1 失败
 */
int talon_vector_search(const TalonHandle *handle,
                        const char *index_name,
                        const float *vec_data, size_t vec_dim,
                        size_t k, const char *metric,
                        char **out_json);

/**
 * 刷盘，保证此前写入持久化。
 * @return 0 成功，-1 失败
 */
int talon_persist(const TalonHandle *handle);

/** 释放 talon_run_sql / talon_vector_search 返回的 JSON 字符串。 */
void talon_free_string(char *ptr);

/** 释放 talon_kv_get 返回的字节缓冲区。 */
void talon_free_bytes(uint8_t *ptr, size_t len);

/**
 * 通用 JSON 命令入口：一个函数覆盖全部引擎操作。
 *
 * 输入 JSON 格式：{"module":"kv|sql|ts|mq|vector|ai|backup|stats","action":"...","params":{...}}
 * 输出 JSON 格式：{"ok":true,"data":{...}} 或 {"ok":false,"error":"..."}
 *
 * @param handle    数据库句柄
 * @param cmd_json  JSON 命令字符串（null-terminated UTF-8）
 * @param out_json  成功时写入 JSON 结果指针，需用 talon_free_string 释放
 * @return 0 成功（含业务错误），-1 仅在句柄/参数无效时返回
 */
int talon_execute(const TalonHandle *handle, const char *cmd_json, char **out_json);

#ifdef __cplusplus
}
#endif

#endif /* TALON_H */
