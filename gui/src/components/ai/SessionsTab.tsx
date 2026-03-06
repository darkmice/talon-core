import { useState, useMemo, useEffect, forwardRef, useImperativeHandle } from "react";
import { useTranslation } from "react-i18next";
import { execAi } from "../../lib/tauri";
import { useToastStore } from "../../stores/toastStore";
import { Button, DataTable, type ColumnDef } from "../ui";

interface SessionRow {
  id: string;
  created: string;
}

export interface SessionsTabHandle {
  reload: () => void;
}

const SessionsTab = forwardRef<SessionsTabHandle>(function SessionsTab(_props, ref) {
  const { t } = useTranslation();
  const addToast = useToastStore(s => s.addToast);
  const [sessions, setSessions] = useState<any[] | null>(null);
  const [detail, setDetail] = useState<any>(null);
  const [history, setHistory] = useState<any[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [deletingSession, setDeletingSession] = useState(false);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await execAi("list_sessions");
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setLoading(false); return; }
      setSessions((data.data as any)?.sessions || []);
      setDetail(null);
      setHistory(null);
    } catch (e) { setError(String(e)); }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  useImperativeHandle(ref, () => ({ reload: load }));

  const viewSession = async (id: string) => {
    try {
      const data = await execAi("get_session", { id });
      if (data.ok) setDetail((data.data as any)?.session);
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
  };

  const viewHistory = async (sid: string) => {
    try {
      const data = await execAi("get_history", { session_id: sid, limit: 50 });
      if (data.ok) setHistory((data.data as any)?.messages || []);
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
  };

  const delSession = async (id: string) => {
    if (deletingSession) return;
    if (!confirm(t("ai.confirmDeleteSession"))) return;
    setDeletingSession(true);
    try {
      const data = await execAi("delete_session", { id });
      if (data.ok) load();
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
    setDeletingSession(false);
  };

  const rows = useMemo<SessionRow[]>(() => {
    if (!sessions) return [];
    return sessions.map((s) => ({
      id: typeof s === "string" ? s : s.id || JSON.stringify(s),
      created: typeof s === "object" ? (s.created_at || s.created || "") : "",
    }));
  }, [sessions]);

  const columns = useMemo<ColumnDef<SessionRow, any>[]>(() => [
    {
      accessorKey: "id",
      header: "ID",
      cell: ({ getValue }) => <span className="text-primary max-w-[240px] truncate block" title={getValue()}>{getValue()}</span>,
    },
    {
      accessorKey: "created",
      header: t("ai.createdAt"),
      cell: ({ getValue }) => <span className="text-slate-400">{getValue()}</span>,
    },
    {
      id: "actions",
      header: t("ai.actions"),
      enableSorting: false,
      cell: ({ row }) => (
        <div className="flex gap-2">
          <button onClick={() => viewSession(row.original.id)} className="text-teal-400 hover:underline text-xs flex items-center gap-1"><span className="material-symbols-outlined text-[13px]">visibility</span> {t("ai.details")}</button>
          <button onClick={() => viewHistory(row.original.id)} className="text-blue-400 hover:underline text-xs flex items-center gap-1"><span className="material-symbols-outlined text-[13px]">chat</span> {t("ai.history")}</button>
          <button onClick={() => delSession(row.original.id)} className="text-red-400 hover:underline text-xs flex items-center gap-1"><span className="material-symbols-outlined text-[13px]">delete</span> {t("common.delete")}</button>
        </div>
      ),
    },
  ], [t]);

  return (
    <div>
      <Button variant="secondary" icon="refresh" onClick={load} className="mb-4">
        {t("ai.refreshSessions")}
      </Button>
      <div className="bg-surface border border-border-dark rounded-lg min-h-[200px] overflow-hidden">
        {error && <div className="m-4 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>}
        {sessions === null && !error && (
          loading ? (
            <div className="flex items-center justify-center py-10">
              <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
              <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
            </div>
          ) : <p className="text-slate-400 text-sm text-center py-10">{t("ai.sessionHint")}</p>
        )}
        {sessions !== null && !error && (
          <DataTable
            columns={columns}
            data={rows}
            emptyText={t("ai.noSessions")}
            emptyIcon="smart_toy"
            getRowId={(r) => r.id}
            compact
          />
        )}
      </div>
      {detail && (
        <div className="mt-4 bg-surface border border-border-dark rounded-lg p-4">
          <h3 className="text-sm text-slate-400 mb-2">{t("ai.sessionDetail")}</h3>
          <pre className="font-mono text-xs text-slate-300 whitespace-pre-wrap">{JSON.stringify(detail, null, 2)}</pre>
        </div>
      )}
      {history && (
        <div className="mt-4 bg-surface border border-border-dark rounded-lg p-4 max-h-[400px] overflow-y-auto">
          <h3 className="text-sm text-slate-400 mb-2">{t("ai.messageHistory", { count: history.length })}</h3>
          {history.map((msg, i) => (
            <div key={i} className={`mb-2 p-2 rounded text-xs font-mono ${msg.role === "user" ? "bg-dark-800 border-l-2 border-primary" : msg.role === "assistant" ? "bg-dark-800 border-l-2 border-emerald-400" : "bg-dark-800 border-l-2 border-slate-500"}`}>
              <span className="text-slate-400">[{msg.role}]</span> {msg.content}
            </div>
          ))}
        </div>
      )}
    </div>
  );
});

export default SessionsTab;
