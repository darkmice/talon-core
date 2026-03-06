import { invoke } from "@tauri-apps/api/core";

export interface TalonResponse<T = unknown> {
  ok: boolean;
  data?: T;
  error?: string;
}

export async function connect(url: string): Promise<TalonResponse> {
  const resp = await invoke("connect", { params: { url } });
  return typeof resp === "string" ? JSON.parse(resp) : resp as unknown as TalonResponse;
}

export async function openDatabase(path: string): Promise<TalonResponse> {
  const resp = await invoke("open_database", { path });
  return typeof resp === "string" ? JSON.parse(resp) : resp as unknown as TalonResponse;
}

export async function disconnect(): Promise<TalonResponse> {
  const resp = await invoke("disconnect");
  return typeof resp === "string" ? JSON.parse(resp) : resp as unknown as TalonResponse;
}

export async function execute(cmd: Record<string, unknown>): Promise<TalonResponse> {
  const resp = await invoke("execute", { cmd: JSON.stringify(cmd) });
  return JSON.parse(resp as string);
}

export async function execSql(sql: string): Promise<TalonResponse> {
  const resp = await invoke("exec_sql", { sql });
  return JSON.parse(resp as string);
}

export async function execKv(action: string, params: Record<string, unknown> = {}): Promise<TalonResponse> {
  const resp = await invoke("exec_kv", { action, params });
  return JSON.parse(resp as string);
}

export async function execMq(action: string, params: Record<string, unknown> = {}): Promise<TalonResponse> {
  const resp = await invoke("exec_mq", { action, params });
  return JSON.parse(resp as string);
}

export async function execAi(action: string, params: Record<string, unknown> = {}): Promise<TalonResponse> {
  return execute({ module: "ai", action, params });
}

export async function execTs(action: string, params: Record<string, unknown> = {}): Promise<TalonResponse> {
  return execute({ module: "ts", action, params });
}

/** Escape a SQL identifier by doubling any backticks inside it */
export function escapeSqlIdent(name: string): string {
  if (!name) return '``';
  return `\`${name.replace(/`/g, "``")}\``;
}

export async function execFts(action: string, params: Record<string, unknown> = {}): Promise<TalonResponse> {
  return execute({ module: "fts", action, params });
}

export async function execGraph(action: string, params: Record<string, unknown> = {}): Promise<TalonResponse> {
  return execute({ module: "graph", action, params });
}

export async function getSchemaInfo(): Promise<TalonResponse> {
  const resp = await invoke("get_schema_info");
  return JSON.parse(resp as string);
}
