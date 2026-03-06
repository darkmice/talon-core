import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { DataTable, EmptyState, type ColumnDef } from "../ui";

interface MqTopic {
  name: string;
  len: number;
}

interface PeekMessage {
  key?: string;
  payload?: string;
  data?: string;
  [key: string]: any;
}

interface MqTopicTableProps {
  topics: MqTopic[] | null;
  error: string | null;
  selectedTopic: string;
  peeked: PeekMessage[] | null;
  peekLoading: boolean;
  onSelectTopic: (name: string) => void;
  onDeleteTopic: (name: string) => void;
  onPeekRefresh: () => void;
}

export default function MqTopicTable({ topics, error, selectedTopic, peeked, peekLoading, onSelectTopic, onDeleteTopic, onPeekRefresh }: MqTopicTableProps) {
  const { t } = useTranslation();

  const columns = useMemo<ColumnDef<MqTopic, any>[]>(() => [
    {
      accessorKey: "name",
      header: t("mq.topicName"),
      cell: ({ getValue }) => (
        <div className="flex items-center gap-2">
          <span className="material-symbols-outlined text-slate-400 text-[16px]">topic</span>
          <span className="font-mono text-[13px] text-emerald-400 truncate max-w-[300px] block" title={getValue()}>{getValue()}</span>
        </div>
      ),
    },
    {
      accessorKey: "len",
      header: t("mq.messages"),
      size: 112,
      cell: ({ getValue }) => (
        <span className="font-mono text-slate-300">{typeof getValue() === "number" ? (getValue() as number).toLocaleString() : getValue()}</span>
      ),
    },
    {
      id: "actions",
      header: t("mq.actions"),
      size: 64,
      enableSorting: false,
      cell: ({ row }) => (
        <button onClick={e => { e.stopPropagation(); onDeleteTopic(row.original.name); }}
          className="text-slate-600 hover:text-red-400 transition" title={t("common.delete")}>
          <span className="material-symbols-outlined text-[16px]">delete</span>
        </button>
      ),
    },
  ], [t, onDeleteTopic]);

  return (
    <div className="flex-1 flex flex-col overflow-auto">
      {error && (
        <div className="m-4 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>
      )}
      {!error && topics === null && (
        <div className="flex items-center justify-center py-16">
          <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
          <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
        </div>
      )}
      {!error && topics !== null && (
        <DataTable
          columns={columns}
          data={topics}
          emptyText={t("common.noData")}
          emptyIcon="forum"
          onRowClick={(row) => onSelectTopic(row.name)}
          selectedRowId={selectedTopic}
          getRowId={(row) => row.name}
        />
      )}

      {/* Peek Messages Preview */}
      {selectedTopic && (
        <div className="border-t border-border-dark shrink-0 max-h-[220px] overflow-y-auto">
          <div className="flex items-center justify-between px-4 py-2 border-b border-border-dark sticky top-0 bg-dark-800 z-10">
            <div className="flex items-center gap-2">
              <span className="material-symbols-outlined text-blue-400 text-[14px]">visibility</span>
              <span className="text-xs font-semibold text-white">{t("mq.peekTitle")}</span>
              <span className="text-[10px] text-slate-400 font-mono">{selectedTopic}</span>
            </div>
            <button onClick={onPeekRefresh} disabled={peekLoading}
              className="text-xs text-primary hover:underline disabled:opacity-40">
              {peekLoading ? t("common.loading") : t("mq.refresh")}
            </button>
          </div>
          {peeked === null && (
            <div className="flex items-center justify-center py-4">
              <span className="material-symbols-outlined text-[16px] text-primary animate-spin">progress_activity</span>
              <span className="ml-1.5 text-xs text-slate-400">{t("common.loading")}</span>
            </div>
          )}
          {peeked && peeked.length === 0 && <p className="text-slate-400 text-xs text-center py-4">{t("mq.noMessages")}</p>}
          {peeked && peeked.length > 0 && peeked.map((msg: any, i: number) => {
            const pl = typeof msg === "string" ? msg : msg.payload || msg.data || JSON.stringify(msg);
            return (
              <div key={i} className="px-4 py-2 border-b border-border-dark hover:bg-white/[0.02] transition">
                <div className="flex items-center justify-between mb-0.5">
                  <span className="text-[10px] text-slate-400 font-mono">#{i + 1}</span>
                  {msg.key && <span className="text-[10px] text-slate-400 font-mono">{msg.key}</span>}
                </div>
                <pre className="text-[11px] font-mono text-slate-300 truncate max-w-full" title={typeof pl === "string" ? pl : JSON.stringify(pl)}>{typeof pl === "string" ? pl : JSON.stringify(pl)}</pre>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
