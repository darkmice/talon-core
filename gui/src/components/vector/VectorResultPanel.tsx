import { useTranslation } from "react-i18next";
import { EmptyState } from "../ui";

interface VectorResult {
  id?: string;
  distance?: number;
  [key: string]: any;
}

interface VectorResultPanelProps {
  results: VectorResult[] | null;
  error: string | null;
  elapsed: number | null;
  scanned: string | null;
  viewMode: "table" | "json" | "visual";
  onViewModeChange: (v: "table" | "json" | "visual") => void;
}

export default function VectorResultPanel({ results, error, elapsed, scanned, viewMode, onViewModeChange }: VectorResultPanelProps) {
  const { t } = useTranslation();
  const maxDist = results ? Math.max(...results.map(r => r.distance ?? 0), 0.01) : 1;

  return (
    <div className="flex-1 flex flex-col min-w-0">
      {/* Result header */}
      <div data-tauri-drag-region className="flex items-center justify-between px-5 py-4 border-b border-border-dark shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-sm font-bold text-white">{t("vector.topMatches")}</h2>
          {elapsed !== null && (
            <span className="text-xs text-slate-400">
              {t("vector.execTime", { ms: elapsed })}
              {scanned && ` · ${t("vector.scanned", { count: Number(scanned) })}`}
            </span>
          )}
        </div>
        <div className="flex bg-dark-800 rounded-lg border border-border-dark p-0.5">
          {(["table", "json", "visual"] as const).map(v => (
            <button key={v} onClick={() => onViewModeChange(v)}
              className={`px-3 py-1 rounded-md text-xs font-medium transition ${viewMode === v ? "bg-surface text-white" : "text-slate-500 hover:text-slate-300"}`}>
              {t(`vector.${v}`)}
            </button>
          ))}
        </div>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-auto">
        {error && <div className="m-4 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>}
        {!results && !error && (
          <EmptyState icon="explore" title={t("vector.runSearch")} />
        )}
        {results && results.length === 0 && <p className="text-slate-400 text-sm text-center py-10">{t("common.noData")}</p>}

        {results && results.length > 0 && viewMode === "table" && (
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                <th className="bg-dark-700/50 px-3 py-2.5 text-center text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0 w-16">{t("vector.rank")}</th>
                <th className="bg-dark-700/50 px-3 py-2.5 text-left text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0 w-28">{t("vector.id")}</th>
                <th className="bg-dark-700/50 px-3 py-2.5 text-left text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0">{t("vector.distance")}</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => {
                const dist = r.distance ?? 0;
                const pct = Math.min(100, (dist / maxDist) * 100);
                const color = dist > 0.85 ? "bg-emerald-500" : dist > 0.7 ? "bg-yellow-500" : "bg-orange-500";
                return (
                  <tr key={i} className="hover:bg-white/[0.02] transition-colors">
                    <td className="px-3 py-3 border-b border-border-dark text-center text-slate-400">{i + 1}</td>
                    <td className="px-3 py-3 border-b border-border-dark font-mono text-[13px] text-white max-w-[120px] truncate" title={r.id ?? "?"}>{r.id ?? "?"}</td>
                    <td className="px-3 py-3 border-b border-border-dark">
                      <div className="flex items-center gap-3">
                        <span className="font-mono text-[13px] text-white w-16">{dist.toFixed(4)}</span>
                        <div className="flex-1 h-2.5 bg-dark-800 rounded-full overflow-hidden">
                          <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${pct}%` }} />
                        </div>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}

        {results && results.length > 0 && viewMode === "json" && (
          <pre className="p-5 font-mono text-[12px] text-slate-300 whitespace-pre-wrap leading-relaxed">
            {JSON.stringify(results, null, 2)}
          </pre>
        )}

        {results && results.length > 0 && viewMode === "visual" && (
          <div className="p-5 space-y-3">
            {results.map((r, i) => {
              const dist = r.distance ?? 0;
              const pct = Math.min(100, (dist / maxDist) * 100);
              const color = dist > 0.85 ? "from-emerald-500 to-emerald-400" : dist > 0.7 ? "from-yellow-500 to-yellow-400" : "from-orange-500 to-orange-400";
              return (
                <div key={i} className="flex items-center gap-3">
                  <span className="text-xs text-slate-400 w-6 text-right">{i + 1}</span>
                  <span className="font-mono text-[12px] text-white w-20 truncate" title={r.id ?? "?"}>{r.id ?? "?"}</span>
                  <div className="flex-1 h-6 bg-dark-800 rounded-lg overflow-hidden relative">
                    <div className={`h-full bg-gradient-to-r ${color} rounded-lg transition-all`} style={{ width: `${pct}%` }} />
                    <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] font-mono text-white/80">{dist.toFixed(4)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Footer */}
      {results && results.length > 0 && (
        <div className="flex items-center gap-4 px-5 py-2.5 border-t border-border-dark shrink-0 text-xs text-slate-400">
          <span>{t("vector.showing", { count: results.length, total: results.length })}</span>
        </div>
      )}
    </div>
  );
}
