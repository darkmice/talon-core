import { useState, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { execTs } from "../../lib/tauri";
import { Button, Input, DataTable, type ColumnDef } from "../ui";

interface TsPoint {
  timestamp: string;
  tags: string;
  fields: string;
}

export default function QueryTab() {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [points, setPoints] = useState<any[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState(100);
  const [loading, setLoading] = useState(false);

  const doQuery = async () => {
    if (!name.trim() || loading) return;
    setError(null);
    setLoading(true);
    try {
      const data = await execTs("query", { name: name.trim(), limit });
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setLoading(false); return; }
      setPoints((data.data as any)?.points || []);
    } catch (e) { setError(String(e)); }
    setLoading(false);
  };

  const rows = useMemo<TsPoint[]>(() => {
    if (!points) return [];
    return points.map((p: any) => ({
      timestamp: p.timestamp ?? p.ts ?? "?",
      tags: JSON.stringify(p.tags || {}),
      fields: JSON.stringify(p.fields || {}),
    }));
  }, [points]);

  const columns = useMemo<ColumnDef<TsPoint, any>[]>(() => [
    {
      accessorKey: "timestamp",
      header: t("ts.timestamp"),
      cell: ({ getValue }) => <span className="text-emerald-400 text-[13px]">{getValue()}</span>,
    },
    {
      accessorKey: "tags",
      header: t("ts.tags"),
      cell: ({ getValue }) => <span className="text-slate-400 text-xs truncate block max-w-[300px]" title={getValue()}>{getValue()}</span>,
      enableSorting: false,
    },
    {
      accessorKey: "fields",
      header: t("ts.fields"),
      cell: ({ getValue }) => <span className="text-slate-300 text-xs truncate block max-w-[300px]" title={getValue()}>{getValue()}</span>,
      enableSorting: false,
    },
  ], [t]);

  return (
    <div>
      <div className="flex gap-3 items-center mb-4">
        <Input mono value={name} onChange={e => setName(e.target.value)}
          onKeyDown={e => e.key === "Enter" && doQuery()}
          placeholder={t("ts.metricName")} className="max-w-xs w-full" />
        <Input mono value={limit} onChange={e => setLimit(parseInt(e.target.value) || 100)}
          type="number" className="w-24" />
        <Button variant="primary" icon="search" onClick={doQuery} loading={loading} disabled={!name.trim()}>
          {t("ts.query")}
        </Button>
      </div>
      <div className="bg-surface border border-border-dark rounded-xl overflow-hidden min-h-[200px]">
        {error && <div className="m-4 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>}
        {points === null && !error && <p className="text-slate-400 text-sm text-center py-10">{t("ts.queryHint")}</p>}
        {points !== null && !error && (
          <>
            <DataTable
              columns={columns}
              data={rows}
              emptyText={t("common.noData")}
              emptyIcon="schedule"
              compact
            />
            {rows.length > 0 && (
              <div className="px-3 py-2 border-t border-border-dark">
                <span className="text-xs text-slate-400">{rows.length} {t("common.rows")}</span>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
