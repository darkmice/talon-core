import { useState, useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { execSql, execKv, execMq, escapeSqlIdent } from "../lib/tauri";
import { formatValue } from "../lib/formatValue";
import { useToastStore } from "../stores/toastStore";
import { Button, PageHeader } from "../components/ui";
import { MetricCards, TopTablesPanel, SystemStatusPanel } from "../components/stats";

export default function StatsPage() {
  const { t } = useTranslation();
  const [stats, setStats] = useState<{ tables: number; kvKeys: number; mqTopics: number } | null>(null);
  const [loading, setLoading] = useState(false);
  const [tableDetails, setTableDetails] = useState<{ name: string; count: number }[]>([]);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const loadingRef = useRef(false);

  const refresh = async () => {
    if (loadingRef.current) return;
    loadingRef.current = true;
    setLoading(true);
    const s = { tables: 0, kvKeys: 0, mqTopics: 0 };
    try {
      const td = await execSql("SHOW TABLES");
      if (td.ok) {
        const dd = td.data as any;
        const rows = dd?.rows || [];
        const names: string[] = rows.map((r: any) => Array.isArray(r) ? formatValue(r[0]) : formatValue(r));
        s.tables = names.length;
        const details = [];
        for (const name of names.slice(0, 20)) {
          try {
            const cnt = await execSql(`SELECT COUNT(*) FROM ${escapeSqlIdent(name)}`);
            const cd = cnt.data as any;
            if (cnt.ok && cd?.rows?.length > 0) {
              const row = cd.rows[0];
              details.push({ name, count: Number(formatValue(Array.isArray(row) ? row[0] : row)) });
            } else { details.push({ name, count: 0 }); }
          } catch { details.push({ name, count: 0 }); }
        }
        details.sort((a, b) => b.count - a.count);
        setTableDetails(details);
      }
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
    try {
      const kd = await execKv("count", {});
      if (kd.ok) s.kvKeys = (kd.data as any)?.count || 0;
    } catch { try {
      const kd2 = await execKv("keys", { prefix: "" });
      if (kd2.ok) s.kvKeys = ((kd2.data as any)?.keys || []).length;
    } catch (e) { useToastStore.getState().addToast("error", `KV: ${String(e)}`); } }
    try {
      const md = await execMq("topics", {});
      if (md.ok) s.mqTopics = ((md.data as any)?.topics || []).length;
    } catch (e) { useToastStore.getState().addToast("error", `MQ: ${String(e)}`); }
    setStats(s);
    loadingRef.current = false;
    setLoading(false);
  };

  useEffect(() => { refresh(); }, []);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(refresh, 30000);
    } else {
      if (intervalRef.current) clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh]);

  const fmtNum = (n: number) => {
    if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
    if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
    return n.toLocaleString();
  };

  const maxCount = tableDetails.reduce((m, d) => Math.max(m, typeof d.count === "number" ? d.count : 0), 1);

  const cards = stats ? [
    { label: t("stats.sqlTables"), value: stats.tables, icon: "table_chart", color: "text-blue-400", bg: "bg-blue-500/10" },
    { label: t("stats.kvPairs"), value: stats.kvKeys, icon: "vpn_key", color: "text-purple-400", bg: "bg-purple-500/10" },
    { label: t("stats.mqTopics"), value: stats.mqTopics, icon: "forum", color: "text-emerald-400", bg: "bg-emerald-500/10" },
  ] : [];

  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="bar_chart" title={t("stats.title")} subtitle={t("stats.subtitle")}>
        <button onClick={() => setAutoRefresh(!autoRefresh)}
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition border ${
            autoRefresh
              ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400"
              : "bg-surface border-border-dark text-slate-400 hover:text-white"
          }`}>
          <span className={`w-1.5 h-1.5 rounded-full ${autoRefresh ? "bg-emerald-400 animate-pulse" : "bg-slate-600"}`} />
          {autoRefresh ? t("stats.autoRefreshOn") : t("stats.autoRefreshOff")}
        </button>
        <Button variant="secondary" icon="refresh" size="sm" loading={loading} onClick={refresh}>
          {t("stats.refresh")}
        </Button>
      </PageHeader>
      <div className="flex-1 overflow-y-auto">
      <div className="p-6">

        {!stats && loading && (
          <div className="flex items-center justify-center py-16">
            <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
            <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
          </div>
        )}

        {stats && <MetricCards cards={cards} fmtNum={fmtNum} />}

        {stats && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
            <TopTablesPanel tableDetails={tableDetails} maxCount={maxCount} />
            <SystemStatusPanel />
          </div>
        )}
      </div>
      </div>
    </div>
  );
}
