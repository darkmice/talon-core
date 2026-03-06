import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { execAi } from "../../lib/tauri";
import { Button } from "../ui";

export default function MemoryTab() {
  const { t } = useTranslation();
  const [count, setCount] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await execAi("memory_count");
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setLoading(false); return; }
      setCount((data.data as any)?.count ?? 0);
    } catch (e) { setError(String(e)); }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  return (
    <div>
      <Button variant="secondary" icon="refresh" onClick={load} className="mb-4">
        {t("ai.refreshMemory")}
      </Button>
      <div className="bg-surface border border-border-dark rounded-lg p-4 min-h-[120px]">
        {error && <div className="mb-3 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>}
        {count === null && !error && (
          loading ? (
            <div className="flex items-center justify-center py-10">
              <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
              <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
            </div>
          ) : <p className="text-slate-400 text-sm text-center py-10">{t("ai.memoryHint")}</p>
        )}
        {count !== null && (
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
            <div className="bg-dark-800 border border-border-dark rounded-lg p-4">
              <p className="text-[11px] text-slate-400 uppercase tracking-wider">{t("ai.memoryEntries")}</p>
              <p className="text-2xl font-bold text-primary mt-1">{count}</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
