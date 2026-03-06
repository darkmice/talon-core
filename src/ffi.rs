/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! C ABI 导出：供 Python / Node.js / Go 等语言通过 FFI 调用。
//!
//! M3 实现。所有函数以 `talon_` 为前缀，返回 0 表示成功，非 0 表示失败。
//! M25 新增 `talon_execute`：通用 JSON 命令入口，一个函数覆盖全部引擎操作。
//! 字符串参数为 C 风格 null-terminated，调用方负责内存管理。

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::Talon;

/// 不透明句柄类型。
pub struct TalonHandle {
    inner: Arc<Talon>,
    /// TCP Server 停止信号。
    server_stop: std::sync::Mutex<Option<Arc<AtomicBool>>>,
    /// TCP Server 线程句柄。
    server_thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for TalonHandle {
    fn drop(&mut self) {
        // 关闭数据库时自动停止 server
        if let Some(stop) = self.server_stop.lock().unwrap().take() {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(h) = self.server_thread.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

/// 打开数据库；成功返回句柄指针，失败返回 null。
///
/// # Safety
/// `path` 必须是有效的 null-terminated C 字符串。
#[no_mangle]
pub unsafe extern "C" fn talon_open(path: *const c_char) -> *mut TalonHandle {
    if path.is_null() {
        return ptr::null_mut();
    }
    let path = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    match Talon::open(path) {
        Ok(db) => Box::into_raw(Box::new(TalonHandle {
            inner: Arc::new(db),
            server_stop: std::sync::Mutex::new(None),
            server_thread: std::sync::Mutex::new(None),
        })),
        Err(_) => ptr::null_mut(),
    }
}

/// 关闭数据库并释放句柄。
///
/// # Safety
/// `handle` 必须是 `talon_open` 返回的有效指针，且只能调用一次。
#[no_mangle]
pub unsafe extern "C" fn talon_close(handle: *mut TalonHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

/// 执行 SQL；结果写入 `out_json`（调用方需用 `talon_free_string` 释放）。
/// 返回 0 成功，-1 失败。
///
/// # Safety
/// `handle` 和 `sql` 必须有效。`out_json` 不为 null 时写入结果指针。
#[no_mangle]
pub unsafe extern "C" fn talon_run_sql(
    handle: *const TalonHandle,
    sql: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if handle.is_null() || sql.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let sql = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match db.run_sql(sql) {
        Ok(rows) => {
            if !out_json.is_null() {
                let json = serde_json::json!({"rows": rows}).to_string();
                match CString::new(json) {
                    Ok(cs) => *out_json = cs.into_raw(),
                    Err(_) => return -1,
                }
            }
            0
        }
        Err(e) => {
            eprintln!("[Talon FFI] run_sql error: {} | sql={}", e, &sql[..sql.len().min(120)]);
            -1
        }
    }
}

/// KV SET。返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_kv_set(
    handle: *const TalonHandle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    ttl_secs: i64,
) -> i32 {
    if handle.is_null() || key.is_null() || value.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let key = std::slice::from_raw_parts(key, key_len);
    let value = std::slice::from_raw_parts(value, value_len);
    let ttl = if ttl_secs > 0 {
        Some(ttl_secs as u64)
    } else {
        None
    };
    match db.kv().and_then(|kv| kv.set(key, value, ttl)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// KV GET。成功时 `out_value` 指向分配的缓冲区，`out_len` 为长度；不存在时 out_len=0。
/// 返回 0 成功，-1 失败。调用方需用 `talon_free_bytes` 释放 out_value。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_kv_get(
    handle: *const TalonHandle,
    key: *const u8,
    key_len: usize,
    out_value: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if handle.is_null() || key.is_null() || out_value.is_null() || out_len.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let key = std::slice::from_raw_parts(key, key_len);
    match db.kv_read().and_then(|kv| kv.get(key)) {
        Ok(Some(val)) => {
            let mut boxed = val.into_boxed_slice();
            *out_len = boxed.len();
            *out_value = boxed.as_mut_ptr();
            std::mem::forget(boxed);
            0
        }
        Ok(None) => {
            *out_value = ptr::null_mut();
            *out_len = 0;
            0
        }
        Err(_) => -1,
    }
}

/// KV DEL。返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_kv_del(
    handle: *const TalonHandle,
    key: *const u8,
    key_len: usize,
) -> i32 {
    if handle.is_null() || key.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let key = std::slice::from_raw_parts(key, key_len);
    match db.kv().and_then(|kv| kv.del(key)) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// M115：KV INCRBY — 原子自增 delta 步长，key 不存在视为 0。
/// 返回增后的值写入 `*out_value`。返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_kv_incrby(
    handle: *const TalonHandle,
    key: *const u8,
    key_len: usize,
    delta: i64,
    out_value: *mut i64,
) -> i32 {
    if handle.is_null() || key.is_null() || out_value.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let key = std::slice::from_raw_parts(key, key_len);
    match db.kv().and_then(|kv| kv.incrby(key, delta)) {
        Ok(n) => {
            *out_value = n;
            0
        }
        Err(_) => -1,
    }
}

/// M115：KV SETNX — key 不存在时写入，已存在时不操作。
/// 返回 0 成功（`*was_set` 为 1 表示已写入，0 表示 key 已存在未操作），-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_kv_setnx(
    handle: *const TalonHandle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    ttl_secs: i64,
    was_set: *mut i32,
) -> i32 {
    if handle.is_null() || key.is_null() || value.is_null() || was_set.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let key = std::slice::from_raw_parts(key, key_len);
    let value = std::slice::from_raw_parts(value, value_len);
    let ttl = if ttl_secs > 0 {
        Some(ttl_secs as u64)
    } else {
        None
    };
    match db.kv().and_then(|kv| kv.setnx(key, value, ttl)) {
        Ok(set) => {
            *was_set = set as i32;
            0
        }
        Err(_) => -1,
    }
}

/// 向量插入。返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。`vec_data` 指向 f32 数组。
#[no_mangle]
pub unsafe extern "C" fn talon_vector_insert(
    handle: *const TalonHandle,
    index_name: *const c_char,
    id: u64,
    vec_data: *const f32,
    vec_dim: usize,
) -> i32 {
    if handle.is_null() || index_name.is_null() || vec_data.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let name = match CStr::from_ptr(index_name).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let vec = std::slice::from_raw_parts(vec_data, vec_dim);
    match db.vector(name).and_then(|ve| ve.insert(id, vec)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// 向量搜索。结果写入 `out_json`（JSON 格式）。返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_vector_search(
    handle: *const TalonHandle,
    index_name: *const c_char,
    vec_data: *const f32,
    vec_dim: usize,
    k: usize,
    metric: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if handle.is_null() || index_name.is_null() || vec_data.is_null() || metric.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let name = match CStr::from_ptr(index_name).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let vec = std::slice::from_raw_parts(vec_data, vec_dim);
    let metric = match CStr::from_ptr(metric).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match db.vector(name).and_then(|ve| ve.search(vec, k, metric)) {
        Ok(results) => {
            if !out_json.is_null() {
                let items: Vec<serde_json::Value> = results
                    .iter()
                    .map(|(id, dist)| serde_json::json!({"id": id, "distance": dist}))
                    .collect();
                let json = serde_json::json!({"results": items}).to_string();
                match CString::new(json) {
                    Ok(cs) => *out_json = cs.into_raw(),
                    Err(_) => return -1,
                }
            }
            0
        }
        Err(_) => -1,
    }
}

/// 刷盘。返回 0 成功，-1 失败。
///
/// # Safety
/// `handle` 必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_persist(handle: *const TalonHandle) -> i32 {
    if handle.is_null() {
        return -1;
    }
    match (*handle).inner.persist() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// 释放 `talon_run_sql` / `talon_vector_search` 返回的 JSON 字符串。
///
/// # Safety
/// `ptr` 必须是上述函数返回的指针。
#[no_mangle]
pub unsafe extern "C" fn talon_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

/// 释放 `talon_kv_get` 返回的字节缓冲区。
///
/// # Safety
/// `ptr` 和 `len` 必须匹配 `talon_kv_get` 的输出。
#[no_mangle]
pub unsafe extern "C" fn talon_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(Vec::from_raw_parts(ptr, len, len));
    }
}

/// 通用 JSON 命令入口：一个函数覆盖全部引擎操作。
///
/// 输入 JSON 格式：`{"module":"kv|sql|ts|mq|vector|ai|backup|stats","action":"...","params":{...}}`
/// 输出 JSON 格式：`{"ok":true,"data":{...}}` 或 `{"ok":false,"error":"..."}`
///
/// 结果写入 `out_json`，调用方需用 `talon_free_string` 释放。
/// 返回 0 成功（含业务错误），-1 仅在句柄/参数无效时返回。
///
/// # Safety
/// `handle` 和 `cmd_json` 必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_execute(
    handle: *const TalonHandle,
    cmd_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    if handle.is_null() || cmd_json.is_null() || out_json.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let cmd_str = match CStr::from_ptr(cmd_json).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let result_json = execute_cmd(db, cmd_str);
    match CString::new(result_json) {
        Ok(cs) => {
            *out_json = cs.into_raw();
            0
        }
        Err(_) => -1,
    }
}

/// 内部命令路由（实现在 ffi_exec 子模块）。
pub(crate) fn execute_cmd(db: &Talon, cmd_str: &str) -> String {
    crate::ffi_exec::execute_cmd(db, cmd_str)
}

// ── 二进制 FFI（零 JSON 序列化开销）─────────────────────────────────────

/// 二进制 SQL 执行：结果用紧凑 TLV 编码，消除 JSON 序列化开销。
///
/// 结果写入 `out_data`（调用方需用 `talon_free_bytes` 释放）。
/// 返回 0 成功，-1 失败。
///
/// # Safety
/// `handle`、`sql` 必须有效。`out_data`、`out_len` 不为 null。
#[no_mangle]
pub unsafe extern "C" fn talon_run_sql_bin(
    handle: *const TalonHandle,
    sql: *const c_char,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if handle.is_null() || sql.is_null() || out_data.is_null() || out_len.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let sql = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match db.run_sql(sql) {
        Ok(rows) => {
            let bin = crate::ffi_bin::encode_rows(&rows);
            let mut boxed = bin.into_boxed_slice();
            *out_len = boxed.len();
            *out_data = boxed.as_mut_ptr();
            std::mem::forget(boxed);
            0
        }
        Err(_) => -1,
    }
}

/// 二进制参数化 SQL：参数用 TLV 编码传入，结果用 TLV 编码返回。
///
/// 参数编码格式：`param_count: u32` + 每个参数的 `type_tag: u8 + payload`。
/// 返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_run_sql_param_bin(
    handle: *const TalonHandle,
    sql: *const c_char,
    params: *const u8,
    params_len: usize,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if handle.is_null() || sql.is_null() || out_data.is_null() || out_len.is_null() {
        return -1;
    }
    let db = &(*handle).inner;
    let sql = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    // 解码二进制参数
    let params_slice = if params.is_null() || params_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(params, params_len)
    };
    let param_values = if params_slice.is_empty() {
        vec![]
    } else {
        match crate::ffi_bin::decode_params(params_slice) {
            Ok(p) => p,
            Err(_) => return -1,
        }
    };
    match db.run_sql_param(sql, &param_values) {
        Ok(rows) => {
            let bin = crate::ffi_bin::encode_rows(&rows);
            let mut boxed = bin.into_boxed_slice();
            *out_len = boxed.len();
            *out_data = boxed.as_mut_ptr();
            std::mem::forget(boxed);
            0
        }
        Err(e) => {
            eprintln!("[Talon FFI] run_sql_param error: {} | sql={}", e, sql);
            -1
        }
    }
}

/// 二进制向量搜索：结果用紧凑编码（每条 12 bytes: u64 id + f32 distance）。
///
/// 返回 0 成功，-1 失败。
///
/// # Safety
/// 所有指针参数必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_vector_search_bin(
    handle: *const TalonHandle,
    index_name: *const c_char,
    vec_data: *const f32,
    vec_dim: usize,
    k: usize,
    metric: *const c_char,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if handle.is_null() || index_name.is_null() || vec_data.is_null()
        || metric.is_null() || out_data.is_null() || out_len.is_null()
    {
        return -1;
    }
    let db = &(*handle).inner;
    let name = match CStr::from_ptr(index_name).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let vec = std::slice::from_raw_parts(vec_data, vec_dim);
    let metric = match CStr::from_ptr(metric).to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    match db.vector(name).and_then(|ve| ve.search(vec, k, metric)) {
        Ok(results) => {
            let bin = crate::ffi_bin::encode_vector_results(&results);
            let mut boxed = bin.into_boxed_slice();
            *out_len = boxed.len();
            *out_data = boxed.as_mut_ptr();
            std::mem::forget(boxed);
            0
        }
        Err(_) => -1,
    }
}

// ── Server 管理 ──────────────────────────────────────────────

/// 在后台线程启动 TCP Server，供外部客户端工具连接。
///
/// 返回 0 成功，-1 失败（参数无效或 server 已启动）。
///
/// # Safety
/// `handle` 必须有效。`tcp_addr` 为 null-terminated C 字符串，如 "127.0.0.1:7720"。
#[no_mangle]
pub unsafe extern "C" fn talon_start_server(
    handle: *const TalonHandle,
    tcp_addr: *const c_char,
) -> i32 {
    if handle.is_null() || tcp_addr.is_null() {
        return -1;
    }
    let h = &*handle;
    let addr = match CStr::from_ptr(tcp_addr).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    // 已有 server 运行中，拒绝重复启动
    {
        let guard = h.server_stop.lock().unwrap();
        if guard.is_some() {
            return -2;
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let db = Arc::clone(&h.inner);
    let config = crate::server::ServerConfig {
        auth_token: None,
        max_connections: 64,
        ..Default::default()
    };
    let tcp_server = crate::server::TcpServer::new(config, db, addr);
    let thread = std::thread::Builder::new()
        .name("talon-tcp-server".into())
        .spawn(move || {
            if let Err(e) = tcp_server.run(stop2) {
                eprintln!("[Talon] TCP Server error: {}", e);
            }
        });
    match thread {
        Ok(jh) => {
            *h.server_stop.lock().unwrap() = Some(stop);
            *h.server_thread.lock().unwrap() = Some(jh);
            0
        }
        Err(_) => -1,
    }
}

/// 停止后台 TCP Server。
///
/// 返回 0 成功，-1 无 server 运行。
///
/// # Safety
/// `handle` 必须有效。
#[no_mangle]
pub unsafe extern "C" fn talon_stop_server(handle: *const TalonHandle) -> i32 {
    if handle.is_null() {
        return -1;
    }
    let h = &*handle;
    let stop = h.server_stop.lock().unwrap().take();
    match stop {
        Some(s) => {
            s.store(true, Ordering::Relaxed);
            if let Some(jh) = h.server_thread.lock().unwrap().take() {
                let _ = jh.join();
            }
            0
        }
        None => -1,
    }
}
