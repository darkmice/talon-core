import { useState } from "react";
import { useTranslation } from "react-i18next";
import { execute } from "../lib/tauri";
import { VectorSearchPanel, VectorResultPanel } from "../components/vector";

export default function VectorPage() {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [query, setQuery] = useState("");
  const [topk, setTopk] = useState(10);
  const [minScore, setMinScore] = useState(0.75);
  const [results, setResults] = useState<any[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [scanned, setScanned] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<"table" | "json" | "visual">("table");

  const doSearch = async () => {
    if (!name.trim()) { setError(t("vector.indexRequired")); return; }
    setError(null);
    setLoading(true);
    const t0 = performance.now();
    try {
      const cleaned = query.replace(/[\[\]\s]/g, "");
      const vector = cleaned.split(",").map(s => parseFloat(s.trim())).filter(n => !isNaN(n));
      if (vector.length === 0) { setError(t("vector.invalidVector")); setLoading(false); return; }
      const data = await execute({
        module: "vector", action: "search",
        params: { name, vector, k: topk, metric: "cosine" },
      });
      setElapsed(Math.round(performance.now() - t0));
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setResults(null); }
      else {
        setResults((data.data as any)?.results || []);
        setScanned((data.data as any)?.scanned?.toString() ?? null);
      }
    } catch (e) {
      setElapsed(Math.round(performance.now() - t0));
      setError(String(e)); setResults(null);
    }
    setLoading(false);
  };

  return (
    <div className="flex h-full">
      <VectorSearchPanel name={name} query={query} topk={topk} minScore={minScore} loading={loading}
        onNameChange={setName} onQueryChange={setQuery} onTopkChange={setTopk}
        onMinScoreChange={setMinScore} onSearch={doSearch} />
      <VectorResultPanel results={results} error={error} elapsed={elapsed} scanned={scanned}
        viewMode={viewMode} onViewModeChange={setViewMode} />
    </div>
  );
}
