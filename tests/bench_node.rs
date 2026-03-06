/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Node 宽表基准测试：70+ 列真实业务表，100万行。
//! 对标产品需求 6.1 性能基准（P0）。
//! 运行：cargo test --test bench_node --release -- --nocapture
//!
//! 表结构来自 Stardust 平台 Node 表（节点管理），包含：
//! - 系统信息列（OS/CPU/Memory/IP 等）
//! - 硬件信息列（Processor/GPU/DiskID 等）
//! - 告警配置列（AlarmCpuRate/AlarmMemoryRate 等）
//! - 扩展列（CreateTime/UpdateTime/Remark 等）

use std::time::Instant;
use talon::Talon;

const ROWS: u64 = 1_000_000;
const SAMPLE: u64 = 1_000;

fn p95_us(latencies: &mut [f64]) -> f64 {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((latencies.len() as f64) * 0.95) as usize;
    latencies[idx.min(latencies.len() - 1)]
}

const CREATE_NODE: &str = "\
CREATE TABLE node (\
  id INT, \
  project_id INT, \
  name TEXT, \
  code TEXT, \
  secret TEXT, \
  enable INT, \
  product_code TEXT, \
  category TEXT, \
  version TEXT, \
  compile_time INT, \
  os TEXT, \
  os_version TEXT, \
  os_kind INT, \
  architecture TEXT, \
  machine_name TEXT, \
  user_name TEXT, \
  ip TEXT, \
  gateway TEXT, \
  dns TEXT, \
  cpu INT, \
  memory INT, \
  total_size INT, \
  drive_size INT, \
  drive_info TEXT, \
  max_open_files INT, \
  clib_version TEXT, \
  dpi TEXT, \
  resolution TEXT, \
  product TEXT, \
  vendor TEXT, \
  processor TEXT, \
  gpu TEXT, \
  uuid TEXT, \
  machine_guid TEXT, \
  serial_number TEXT, \
  board TEXT, \
  disk_id TEXT, \
  macs TEXT, \
  install_path TEXT, \
  runtime TEXT, \
  framework TEXT, \
  frameworks TEXT, \
  time_zone TEXT, \
  province_id INT, \
  city_id INT, \
  address TEXT, \
  location TEXT, \
  period INT, \
  sync_time INT, \
  new_server TEXT, \
  last_version TEXT, \
  channel INT, \
  web_hook TEXT, \
  alarm_cpu_rate INT, \
  alarm_memory_rate INT, \
  alarm_disk_rate INT, \
  alarm_tcp INT, \
  alarm_processes TEXT, \
  alarm_on_offline INT, \
  logins INT, \
  last_login INT, \
  last_login_ip TEXT, \
  last_active INT, \
  online_time INT, \
  create_user_id INT, \
  create_time INT, \
  create_ip TEXT, \
  update_user_id INT, \
  update_time INT, \
  update_ip TEXT, \
  remark TEXT\
)";

#[allow(clippy::format_in_format_args)]
fn gen_insert(i: u64) -> String {
    format!(
        "INSERT INTO node (id, project_id, name, code, secret, enable, product_code, category, \
         version, compile_time, os, os_version, os_kind, architecture, machine_name, user_name, \
         ip, gateway, dns, cpu, memory, total_size, drive_size, drive_info, max_open_files, \
         clib_version, dpi, resolution, product, vendor, processor, gpu, uuid, machine_guid, \
         serial_number, board, disk_id, macs, install_path, runtime, framework, frameworks, \
         time_zone, province_id, city_id, address, location, period, sync_time, new_server, \
         last_version, channel, web_hook, alarm_cpu_rate, alarm_memory_rate, alarm_disk_rate, \
         alarm_tcp, alarm_processes, alarm_on_offline, logins, last_login, last_login_ip, \
         last_active, online_time, create_user_id, create_time, create_ip, update_user_id, \
         update_time, update_ip, remark) VALUES (\
         {id}, {pid}, 'node_{id}', 'NK_{id}', 'secret_{id}', 1, 'prod_{cat}', 'cat_{cat}', \
         '3.2.{v}', {ts}, '{os}', '10.0.{v}', {osk}, '{arch}', 'machine_{id}', 'user_{uid}', \
         '192.168.{ip1}.{ip2}', '192.168.{ip1}.1', '8.8.8.8', {cpu}, {mem}, {disk}, {ddisk}, \
         'C:{disk}M,D:{ddisk}M', 65535, 'glibc-2.31', '96*96', '1920*1080', 'Server-{cat}', \
         'Dell', 'Intel Xeon E5-2680 v4', 'NVIDIA Tesla V100', \
         'uuid-{id}-abcd', 'guid-{id}', 'SN-{id}', 'Board-{cat}', \
         'DISK-{id}', 'AA:BB:CC:DD:{mac1}:{mac2}', '/opt/app_{id}', \
         '.NET 8.0', 'net8.0', 'net6.0,net7.0,net8.0', 'Asia/Shanghai', \
         {prov}, {city}, 'addr_{id}', '31.23,121.47', 60, 0, '', '{v}.0.0', \
         0, '', 80, 85, 90, 50000, '', 0, {logins}, {ts}, '10.0.{ip1}.{ip2}', \
         {ts}, {online}, {uid}, {ts}, '10.0.{ip1}.{ip2}', {uid}, {ts}, \
         '10.0.{ip1}.{ip2}', 'node {id} remark')",
        id = i,
        pid = i % 100,
        cat = i % 50,
        v = i % 200,
        ts = 1700000000 + (i % 86400) as i64,
        os = if i % 3 == 0 {
            "Windows"
        } else if i % 3 == 1 {
            "Linux"
        } else {
            "macOS"
        },
        osk = i % 4,
        arch = if i % 2 == 0 { "X64" } else { "Arm64" },
        uid = i % 1000,
        ip1 = i % 256,
        ip2 = (i / 256) % 256,
        cpu = 4 + (i % 61),
        mem = 4096 + (i % 61440),
        disk = 100000 + (i % 900000),
        ddisk = 200000 + (i % 1800000),
        mac1 = format!("{:02X}", i % 256),
        mac2 = format!("{:02X}", (i / 256) % 256),
        prov = 110000 + (i % 34) * 10000,
        city = 110100 + (i % 340) * 100,
        logins = i % 10000,
        online = i % 864000,
    )
}

fn fill_node_table(db: &Talon, rows: u64) {
    use talon::Value;
    println!("  Creating Node table (71 columns)...");
    db.run_sql(CREATE_NODE).unwrap();
    db.run_sql("CREATE INDEX idx_node_pid ON node(project_id)")
        .unwrap();
    db.run_sql("CREATE INDEX idx_node_cat ON node(category)")
        .unwrap();
    let cols: Vec<&str> = vec![
        "id",
        "project_id",
        "name",
        "code",
        "secret",
        "enable",
        "product_code",
        "category",
        "version",
        "compile_time",
        "os",
        "os_version",
        "os_kind",
        "architecture",
        "machine_name",
        "user_name",
        "ip",
        "gateway",
        "dns",
        "cpu",
        "memory",
        "total_size",
        "drive_size",
        "drive_info",
        "max_open_files",
        "clib_version",
        "dpi",
        "resolution",
        "product",
        "vendor",
        "processor",
        "gpu",
        "uuid",
        "machine_guid",
        "serial_number",
        "board",
        "disk_id",
        "macs",
        "install_path",
        "runtime",
        "framework",
        "frameworks",
        "time_zone",
        "province_id",
        "city_id",
        "address",
        "location",
        "period",
        "sync_time",
        "new_server",
        "last_version",
        "channel",
        "web_hook",
        "alarm_cpu_rate",
        "alarm_memory_rate",
        "alarm_disk_rate",
        "alarm_tcp",
        "alarm_processes",
        "alarm_on_offline",
        "logins",
        "last_login",
        "last_login_ip",
        "last_active",
        "online_time",
        "create_user_id",
        "create_time",
        "create_ip",
        "update_user_id",
        "update_time",
        "update_ip",
        "remark",
    ];
    println!("  Filling {} rows (native batch)...", rows);
    let t0 = Instant::now();
    let batch_size = 1000u64;
    let mut i = 0u64;
    while i < rows {
        let end = (i + batch_size).min(rows);
        let mut batch_rows = Vec::with_capacity((end - i) as usize);
        for j in i..end {
            let cat = j % 50;
            let v = j % 200;
            let ts = Value::Integer(1700000000 + (j % 86400) as i64);
            let os = if j % 3 == 0 {
                "Windows"
            } else if j % 3 == 1 {
                "Linux"
            } else {
                "macOS"
            };
            let arch = if j % 2 == 0 { "X64" } else { "Arm64" };
            batch_rows.push(vec![
                Value::Integer(j as i64),
                Value::Integer((j % 100) as i64),
                Value::Text(format!("node_{}", j)),
                Value::Text(format!("NK_{}", j)),
                Value::Text(format!("secret_{}", j)),
                Value::Integer(1),
                Value::Text(format!("prod_{}", cat)),
                Value::Text(format!("cat_{}", cat)),
                Value::Text(format!("3.2.{}", v)),
                ts.clone(),
                Value::Text(os.into()),
                Value::Text(format!("10.0.{}", v)),
                Value::Integer((j % 4) as i64),
                Value::Text(arch.into()),
                Value::Text(format!("machine_{}", j)),
                Value::Text(format!("user_{}", j % 1000)),
                Value::Text(format!("192.168.{}.{}", j % 256, (j / 256) % 256)),
                Value::Text(format!("192.168.{}.1", j % 256)),
                Value::Text("8.8.8.8".into()),
                Value::Integer((4 + (j % 61)) as i64),
                Value::Integer((4096 + (j % 61440)) as i64),
                Value::Integer((100000 + (j % 900000)) as i64),
                Value::Integer((200000 + (j % 1800000)) as i64),
                Value::Text("C:100M,D:200M".into()),
                Value::Integer(65535),
                Value::Text("glibc-2.31".into()),
                Value::Text("96*96".into()),
                Value::Text("1920*1080".into()),
                Value::Text(format!("Server-{}", cat)),
                Value::Text("Dell".into()),
                Value::Text("Intel Xeon".into()),
                Value::Text("NVIDIA V100".into()),
                Value::Text(format!("uuid-{}", j)),
                Value::Text(format!("guid-{}", j)),
                Value::Text(format!("SN-{}", j)),
                Value::Text(format!("Board-{}", cat)),
                Value::Text(format!("DISK-{}", j)),
                Value::Text(format!("AA:BB:CC:{:02X}", j % 256)),
                Value::Text(format!("/opt/app_{}", j)),
                Value::Text(".NET 8.0".into()),
                Value::Text("net8.0".into()),
                Value::Text("net6.0,net7.0,net8.0".into()),
                Value::Text("Asia/Shanghai".into()),
                Value::Integer((110000 + (j % 34) * 10000) as i64),
                Value::Integer((110100 + (j % 340) * 100) as i64),
                Value::Text(format!("addr_{}", j)),
                Value::Text("31.23,121.47".into()),
                Value::Integer(60),
                Value::Integer(0),
                Value::Text(String::new()),
                Value::Text(format!("{}.0.0", v)),
                Value::Integer(0),
                Value::Text(String::new()),
                Value::Integer(80),
                Value::Integer(85),
                Value::Integer(90),
                Value::Integer(50000),
                Value::Text(String::new()),
                Value::Integer(0),
                Value::Integer((j % 10000) as i64),
                ts.clone(),
                Value::Text(format!("10.0.{}.{}", j % 256, (j / 256) % 256)),
                ts.clone(),
                Value::Integer((j % 864000) as i64),
                Value::Integer((j % 1000) as i64),
                ts.clone(),
                Value::Text(format!("10.0.{}.{}", j % 256, (j / 256) % 256)),
                Value::Integer((j % 1000) as i64),
                ts.clone(),
                Value::Text(format!("10.0.{}.{}", j % 256, (j / 256) % 256)),
                Value::Text(format!("remark_{}", j)),
            ]);
        }
        db.batch_insert_rows("node", &cols, batch_rows).unwrap();
        i = end;
        if i % 100_000 == 0 {
            println!("    {}K rows...", i / 1000);
        }
    }
    db.persist().unwrap(); // 落盘校验
    println!("  Fill done in {:.2?}", t0.elapsed());
}

// ── 1. 点查询 P95 < 5ms (1M行, 70列宽表) ──

#[test]
fn node_point_query_pk() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!(
        "\n=== Node-1: Point Query by PK ({}M rows, 70 cols) ===",
        ROWS / 1_000_000
    );
    fill_node_table(&db, ROWS);

    let mut latencies = Vec::with_capacity(SAMPLE as usize);
    let step = ROWS / SAMPLE;
    for s in 0..SAMPLE {
        let pk = s * step;
        let t = Instant::now();
        let r = db
            .run_sql(&format!("SELECT * FROM node WHERE id = {}", pk))
            .unwrap();
        let us = t.elapsed().as_micros() as f64;
        latencies.push(us);
        assert!(!r.is_empty(), "PK {} missing", pk);
        assert_eq!(r[0].len(), 71, "should have 71 columns");
    }
    let p95 = p95_us(&mut latencies);
    let p95_ms = p95 / 1000.0;
    let avg_us = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!(
        "  Samples: {}, Avg: {:.1}us, P95: {:.1}us ({:.3}ms)",
        SAMPLE, avg_us, p95, p95_ms
    );
    println!(
        "  Target: P95 < 5ms  =>  {}",
        if p95_ms < 5.0 { "PASS" } else { "FAIL" }
    );
}

// ── 2. 范围查询 P95 < 50ms (索引列, LIMIT 100) ──

#[test]
fn node_range_query_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!(
        "\n=== Node-2: Range Query by Index ({}M rows, 70 cols) ===",
        ROWS / 1_000_000
    );
    fill_node_table(&db, ROWS);

    let mut latencies = Vec::with_capacity(SAMPLE as usize);
    for s in 0..SAMPLE {
        let cat = format!("cat_{}", s % 50);
        let t = Instant::now();
        let r = db
            .run_sql(&format!(
                "SELECT * FROM node WHERE category = '{}' LIMIT 100",
                cat
            ))
            .unwrap();
        let us = t.elapsed().as_micros() as f64;
        latencies.push(us);
        assert!(!r.is_empty());
    }
    let p95 = p95_us(&mut latencies);
    let p95_ms = p95 / 1000.0;
    let avg_us = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!(
        "  Samples: {}, Avg: {:.1}us, P95: {:.1}us ({:.3}ms)",
        SAMPLE, avg_us, p95, p95_ms
    );
    println!(
        "  Target: P95 < 50ms  =>  {}",
        if p95_ms < 50.0 { "PASS" } else { "FAIL" }
    );
}

// ── 3. 插入（单条）> 10,000 QPS (70列宽行) ──

#[test]
fn node_insert_single() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 10_000u64;
    println!("\n=== Node-3: Single INSERT > 10K QPS (70 cols) ===");
    db.run_sql(CREATE_NODE).unwrap();
    let t = Instant::now();
    for i in 0..n {
        db.run_sql(&gen_insert(i)).unwrap();
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let qps = n as f64 / elapsed.as_secs_f64();
    println!("  {} inserts in {:.2?}, QPS: {:.0}", n, elapsed, qps);
    println!(
        "  Target: > 10,000  =>  {}",
        if qps > 10_000.0 { "PASS" } else { "FAIL" }
    );
}

// ── 4. 插入（批量）> 100,000 行/秒 (70列宽行, 1000行/txn) ──

#[test]
fn node_insert_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 50_000u64;
    let batch = 1000u64;
    println!(
        "\n=== Node-4: Batch INSERT > 100K rows/s (70 cols, batch={}) ===",
        batch
    );
    db.run_sql(CREATE_NODE).unwrap();
    let t = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + batch).min(n);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&gen_insert(j)).unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let rps = n as f64 / elapsed.as_secs_f64();
    println!("  {} rows in {:.2?}, Rows/s: {:.0}", n, elapsed, rps);
    println!(
        "  Target: > 100,000  =>  {}",
        if rps > 100_000.0 { "PASS" } else { "FAIL" }
    );
}

// ── 4b. 原生批量插入 > 100K (跳过SQL解析) ──

#[test]
fn node_insert_batch_native() {
    use talon::Value;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 50_000u64;
    let batch = 1000u64;
    println!(
        "\n=== Node-4b: Native Batch INSERT > 100K rows/s (71 cols, batch={}) ===",
        batch
    );
    db.run_sql(CREATE_NODE).unwrap();
    let cols: Vec<&str> = vec![
        "id",
        "project_id",
        "name",
        "code",
        "secret",
        "enable",
        "product_code",
        "category",
        "version",
        "compile_time",
        "os",
        "os_version",
        "os_kind",
        "architecture",
        "machine_name",
        "user_name",
        "ip",
        "gateway",
        "dns",
        "cpu",
        "memory",
        "total_size",
        "drive_size",
        "drive_info",
        "max_open_files",
        "clib_version",
        "dpi",
        "resolution",
        "product",
        "vendor",
        "processor",
        "gpu",
        "uuid",
        "machine_guid",
        "serial_number",
        "board",
        "disk_id",
        "macs",
        "install_path",
        "runtime",
        "framework",
        "frameworks",
        "time_zone",
        "province_id",
        "city_id",
        "address",
        "location",
        "period",
        "sync_time",
        "new_server",
        "last_version",
        "channel",
        "web_hook",
        "alarm_cpu_rate",
        "alarm_memory_rate",
        "alarm_disk_rate",
        "alarm_tcp",
        "alarm_processes",
        "alarm_on_offline",
        "logins",
        "last_login",
        "last_login_ip",
        "last_active",
        "online_time",
        "create_user_id",
        "create_time",
        "create_ip",
        "update_user_id",
        "update_time",
        "update_ip",
        "remark",
    ];
    let t = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + batch).min(n);
        let mut rows = Vec::with_capacity((end - i) as usize);
        for j in i..end {
            let cat = j % 50;
            let v = j % 200;
            let ts = Value::Integer(1700000000 + (j % 86400) as i64);
            let os = if j % 3 == 0 {
                "Windows"
            } else if j % 3 == 1 {
                "Linux"
            } else {
                "macOS"
            };
            let arch = if j % 2 == 0 { "X64" } else { "Arm64" };
            let row = vec![
                Value::Integer(j as i64),
                Value::Integer((j % 100) as i64),
                Value::Text(format!("node_{}", j)),
                Value::Text(format!("NK_{}", j)),
                Value::Text(format!("secret_{}", j)),
                Value::Integer(1),
                Value::Text(format!("prod_{}", cat)),
                Value::Text(format!("cat_{}", cat)),
                Value::Text(format!("3.2.{}", v)),
                ts.clone(),
                Value::Text(os.into()),
                Value::Text(format!("10.0.{}", v)),
                Value::Integer((j % 4) as i64),
                Value::Text(arch.into()),
                Value::Text(format!("machine_{}", j)),
                Value::Text(format!("user_{}", j % 1000)),
                Value::Text(format!("192.168.{}.{}", j % 256, (j / 256) % 256)),
                Value::Text(format!("192.168.{}.1", j % 256)),
                Value::Text("8.8.8.8".into()),
                Value::Integer((4 + (j % 61)) as i64),
                Value::Integer((4096 + (j % 61440)) as i64),
                Value::Integer((100000 + (j % 900000)) as i64),
                Value::Integer((200000 + (j % 1800000)) as i64),
                Value::Text("C:100M,D:200M".into()),
                Value::Integer(65535),
                Value::Text("glibc-2.31".into()),
                Value::Text("96*96".into()),
                Value::Text("1920*1080".into()),
                Value::Text(format!("Server-{}", cat)),
                Value::Text("Dell".into()),
                Value::Text("Intel Xeon".into()),
                Value::Text("NVIDIA V100".into()),
                Value::Text(format!("uuid-{}", j)),
                Value::Text(format!("guid-{}", j)),
                Value::Text(format!("SN-{}", j)),
                Value::Text(format!("Board-{}", cat)),
                Value::Text(format!("DISK-{}", j)),
                Value::Text(format!("AA:BB:CC:{:02X}", j % 256)),
                Value::Text(format!("/opt/app_{}", j)),
                Value::Text(".NET 8.0".into()),
                Value::Text("net8.0".into()),
                Value::Text("net6.0,net7.0,net8.0".into()),
                Value::Text("Asia/Shanghai".into()),
                Value::Integer((110000 + (j % 34) * 10000) as i64),
                Value::Integer((110100 + (j % 340) * 100) as i64),
                Value::Text(format!("addr_{}", j)),
                Value::Text("31.23,121.47".into()),
                Value::Integer(60),
                Value::Integer(0),
                Value::Text(String::new()),
                Value::Text(format!("{}.0.0", v)),
                Value::Integer(0),
                Value::Text(String::new()),
                Value::Integer(80),
                Value::Integer(85),
                Value::Integer(90),
                Value::Integer(50000),
                Value::Text(String::new()),
                Value::Integer(0),
                Value::Integer((j % 10000) as i64),
                ts.clone(),
                Value::Text(format!("10.0.{}.{}", j % 256, (j / 256) % 256)),
                ts.clone(),
                Value::Integer((j % 864000) as i64),
                Value::Integer((j % 1000) as i64),
                ts.clone(),
                Value::Text(format!("10.0.{}.{}", j % 256, (j / 256) % 256)),
                Value::Integer((j % 1000) as i64),
                ts.clone(),
                Value::Text(format!("10.0.{}.{}", j % 256, (j / 256) % 256)),
                Value::Text(format!("remark_{}", j)),
            ];
            rows.push(row);
        }
        db.batch_insert_rows("node", &cols, rows).unwrap();
        i = end;
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t.elapsed();
    let rps = n as f64 / elapsed.as_secs_f64();
    println!("  {} rows in {:.2?}, Rows/s: {:.0}", n, elapsed, rps);
    println!(
        "  Target: > 100,000  =>  {}",
        if rps > 100_000.0 { "PASS" } else { "FAIL" }
    );
}

// ── 5. 聚合 P95 < 500ms (1M行, 70列宽表) ──

#[test]
fn node_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    println!(
        "\n=== Node-5: Aggregate ({}M rows, 70 cols) P95 < 500ms ===",
        ROWS / 1_000_000
    );
    fill_node_table(&db, ROWS);

    let queries = [
        "SELECT COUNT(*) FROM node",
        "SELECT SUM(memory) FROM node",
        "SELECT AVG(cpu) FROM node",
    ];
    for q in &queries {
        let mut latencies = Vec::with_capacity(10);
        for _ in 0..10 {
            let t = Instant::now();
            db.run_sql(q).unwrap();
            latencies.push(t.elapsed().as_micros() as f64);
        }
        let p95 = p95_us(&mut latencies);
        let p95_ms = p95 / 1000.0;
        let avg_ms = latencies.iter().sum::<f64>() / latencies.len() as f64 / 1000.0;
        let pass = if p95_ms < 500.0 { "PASS" } else { "FAIL" };
        println!(
            "  {}: avg={:.1}ms P95={:.1}ms [{}]",
            q, avg_ms, p95_ms, pass
        );
    }
}

// ── 6. JOIN (Node + Project, 1M×100) ──

#[test]
fn node_join_project() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let node_rows = 100_000u64;
    let projects = 100u64;
    println!(
        "\n=== Node-6: JOIN ({}K nodes x {} projects) ===",
        node_rows / 1000,
        projects
    );

    db.run_sql("CREATE TABLE project (id INT, name TEXT, org TEXT)")
        .unwrap();
    db.run_sql("BEGIN").unwrap();
    for i in 0..projects {
        db.run_sql(&format!(
            "INSERT INTO project (id, name, org) VALUES ({}, 'proj_{}', 'org_{}')",
            i,
            i,
            i % 10
        ))
        .unwrap();
    }
    db.run_sql("COMMIT").unwrap();

    db.run_sql(CREATE_NODE).unwrap();
    println!("  Filling {} nodes...", node_rows);
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < node_rows {
        let end = (i + 1000).min(node_rows);
        db.run_sql("BEGIN").unwrap();
        for j in i..end {
            db.run_sql(&gen_insert(j)).unwrap();
        }
        db.run_sql("COMMIT").unwrap();
        i = end;
        if i % 20_000 == 0 {
            println!("    {}K...", i / 1000);
        }
    }
    println!("  Fill done in {:.2?}", t0.elapsed());

    let mut latencies = Vec::with_capacity(100);
    for s in 0..100u64 {
        let pid = s % projects;
        let t = Instant::now();
        let r = db
            .run_sql(&format!(
                "SELECT name, org FROM node JOIN project ON node.project_id = project.id \
             WHERE org = 'org_{}' LIMIT 100",
                pid % 10
            ))
            .unwrap();
        latencies.push(t.elapsed().as_micros() as f64);
        assert!(!r.is_empty());
    }
    let p95 = p95_us(&mut latencies);
    let p95_ms = p95 / 1000.0;
    let avg_ms = latencies.iter().sum::<f64>() / latencies.len() as f64 / 1000.0;
    println!("  Samples: 100, Avg: {:.1}ms, P95: {:.1}ms", avg_ms, p95_ms);
    println!(
        "  Target: P95 < 200ms  =>  {}",
        if p95_ms < 200.0 { "PASS" } else { "FAIL" }
    );
}

// ── 7. 精准落盘校验：close→reopen→逐条验证 71 列 ──

#[test]
fn node_durability_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let rows = 10_000u64;
    println!(
        "\n=== Node-DUR: Durability Verify ({}K rows, 71 cols, close→reopen→verify) ===",
        rows / 1000
    );
    {
        let db = Talon::open(&path).unwrap();
        fill_node_table(&db, rows);
        drop(db);
    }
    {
        let db = Talon::open(&path).unwrap();
        let sample = 100u64;
        let step = rows / sample;
        for s in 0..sample {
            let pk = s * step;
            let r = db
                .run_sql(&format!("SELECT * FROM node WHERE id = {}", pk))
                .unwrap();
            assert!(!r.is_empty(), "PK {} 丢失！71列宽表数据未落盘", pk);
            assert_eq!(r[0].len(), 71, "PK {} 列数不对: got {}", pk, r[0].len());
            // 验证关键列值
            match &r[0][0] {
                talon::Value::Integer(id) => {
                    assert_eq!(*id, pk as i64, "PK {} id 不匹配", pk)
                }
                other => panic!("PK {} id 类型错误: {:?}", pk, other),
            }
            match &r[0][2] {
                talon::Value::Text(name) => {
                    assert_eq!(name, &format!("node_{}", pk), "PK {} name 不匹配", pk)
                }
                other => panic!("PK {} name 类型错误: {:?}", pk, other),
            }
        }
        let count = db.run_sql("SELECT COUNT(*) FROM node").unwrap();
        match &count[0][0] {
            talon::Value::Integer(n) => {
                assert_eq!(*n, rows as i64, "总行数不匹配")
            }
            other => panic!("COUNT 类型错误: {:?}", other),
        }
        println!(
            "  ✅ {} 行抽样验证通过（71列完整），总行数 {} 正确",
            sample, rows
        );
    }
}
