import { useState, useEffect, useRef, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { execFts } from "../lib/tauri";
import { useToastStore } from "../stores/toastStore";
import { PageHeader } from "../components/ui";

interface FtsIndex {
  name: string;
  doc_count: number;
}

interface SearchHit {
  doc_id: string;
  score: number;
  fields: Record<string, string>;
}

export default function FtsPage() {
  const { t } = useTranslation();
  const [indexes, setIndexes] = useState<FtsIndex[]>([]);
  const [selectedIndex, setSelectedIndex] = useState("");
  const [newIndexName, setNewIndexName] = useState("");
  const [creating, setCreating] = useState(false);

  // Index document state
  const [docId, setDocId] = useState("");
  const [docFields, setDocFields] = useState("{}");
  const [indexing, setIndexing] = useState(false);

  // Search state
  const [searchQuery, setSearchQuery] = useState("");
  const [searchMode, setSearchMode] = useState<"normal" | "fuzzy">("normal");
  const [fuzzyDist, setFuzzyDist] = useState(1);
  const [searchLimit, setSearchLimit] = useState(20);
  const [hits, setHits] = useState<SearchHit[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);

  const loadIndexesRef = useRef(0);

  const loadIndexes = useCallback(async () => {
    const gen = ++loadIndexesRef.current;
    try {
      const res = await execFts("list_indexes");
      if (gen !== loadIndexesRef.current) return;
      if (res.ok) {
        const list = (res.data as any)?.indexes ?? [];
        setIndexes(list);
        if (list.length > 0) {
          setSelectedIndex((prev) => prev || list[0].name);
        }
      }
    } catch {
      // Network error — silently ignore on background refresh
    }
  }, []);

  useEffect(() => { loadIndexes(); }, [loadIndexes]);

  const createIndex = async () => {
    const name = newIndexName.trim();
    if (!name) return;
    setCreating(true);
    try {
      const res = await execFts("create_index", { name });
      if (res.ok) {
        useToastStore.getState().addToast("success", t("fts.createSuccess", { name }));
        setNewIndexName("");
        setSelectedIndex(name);
        await loadIndexes();
      } else {
        useToastStore.getState().addToast("error", res.error ?? t("common.unknownError"));
      }
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
    setCreating(false);
  };

  const dropIndex = async (name: string) => {
    if (!confirm(t("fts.confirmDrop", { name }))) return;
    try {
      const res = await execFts("drop_index", { name });
      if (res.ok) {
        useToastStore.getState().addToast("success", t("fts.dropSuccess", { name }));
        if (selectedIndex === name) {
          setSelectedIndex("");
          setHits(null);
        }
        await loadIndexes();
      } else {
        useToastStore.getState().addToast("error", res.error ?? t("common.unknownError"));
      }
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
  };

  const indexDocument = async () => {
    if (!selectedIndex || !docId.trim()) return;
    let fields: Record<string, string>;
    try {
      fields = JSON.parse(docFields);
    } catch {
      useToastStore.getState().addToast("error", t("common.invalidJson"));
      return;
    }
    setIndexing(true);
    try {
      const res = await execFts("index", { name: selectedIndex, doc_id: docId.trim(), fields });
      if (res.ok) {
        useToastStore.getState().addToast("success", t("fts.indexSuccess"));
        setDocId("");
        setDocFields("{}");
        await loadIndexes();
      } else {
        useToastStore.getState().addToast("error", res.error ?? t("common.unknownError"));
      }
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
    setIndexing(false);
  };

  const doSearch = async () => {
    if (!selectedIndex || !searchQuery.trim()) return;
    setSearching(true);
    setSearchError(null);
    const t0 = performance.now();
    try {
      const action = searchMode === "fuzzy" ? "search_fuzzy" : "search";
      const params: Record<string, unknown> = {
        name: selectedIndex,
        query: searchQuery.trim(),
        limit: searchLimit,
      };
      if (searchMode === "fuzzy") params.max_dist = fuzzyDist;
      const res = await execFts(action, params);
      setElapsed(Math.round(performance.now() - t0));
      if (res.ok) {
        setHits((res.data as any)?.hits ?? []);
      } else {
        setSearchError(res.error ?? t("common.unknownError"));
        setHits(null);
      }
    } catch (e) {
      setElapsed(Math.round(performance.now() - t0));
      setSearchError(String(e));
      setHits(null);
    }
    setSearching(false);
  };

  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="search" title={t("fts.title")} subtitle={t("fts.subtitle")} />
      <div className="flex flex-1 overflow-hidden">
        {/* Left panel: Index management */}
        <div className="w-[320px] flex-shrink-0 border-r border-border-dark flex flex-col overflow-y-auto">
          {/* Create index */}
          <div className="p-4 border-b border-border-dark">
            <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-2 block">
              {t("fts.createIndex")}
            </label>
            <div className="flex gap-2">
              <input
                value={newIndexName}
                onChange={e => setNewIndexName(e.target.value)}
                onKeyDown={e => e.key === "Enter" && createIndex()}
                placeholder={t("fts.indexNamePlaceholder")}
                className="flex-1 h-9 px-3 rounded-lg bg-dark-700 border border-border-dark text-white text-sm placeholder:text-slate-500 focus:outline-none focus:border-primary"
              />
              <button
                onClick={createIndex}
                disabled={creating || !newIndexName.trim()}
                className="h-9 px-4 rounded-lg bg-primary text-white text-sm font-medium hover:bg-primary/90 disabled:opacity-40"
              >
                {creating ? "..." : t("fts.createBtn")}
              </button>
            </div>
          </div>

          {/* Index list */}
          <div className="p-4 flex-1">
            <div className="flex items-center justify-between mb-3">
              <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider">
                {t("fts.indexes")}
              </label>
              <button onClick={loadIndexes} className="text-xs text-slate-500 hover:text-white">
                <span className="material-symbols-outlined text-[16px]">refresh</span>
              </button>
            </div>
            {indexes.length === 0 ? (
              <p className="text-sm text-slate-500">{t("fts.noIndexes")}</p>
            ) : (
              <div className="space-y-1">
                {indexes.map(idx => (
                  <div
                    key={idx.name}
                    onClick={() => setSelectedIndex(idx.name)}
                    className={`group flex items-center justify-between px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                      selectedIndex === idx.name
                        ? "bg-primary/20 text-primary border border-primary/20"
                        : "text-slate-300 hover:bg-white/5 border border-transparent"
                    }`}
                  >
                    <div>
                      <div className="text-sm font-medium">{idx.name}</div>
                      <div className="text-xs text-slate-500">
                        {t("fts.docCount", { count: idx.doc_count })}
                      </div>
                    </div>
                    <button
                      onClick={e => { e.stopPropagation(); dropIndex(idx.name); }}
                      className="opacity-0 group-hover:opacity-100 text-red-400 hover:text-red-300 transition-opacity"
                    >
                      <span className="material-symbols-outlined text-[18px]">delete</span>
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Index document form */}
          {selectedIndex && (
            <div className="p-4 border-t border-border-dark">
              <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-2 block">
                {t("fts.indexDocument")}
              </label>
              <input
                value={docId}
                onChange={e => setDocId(e.target.value)}
                placeholder={t("fts.docIdPlaceholder")}
                className="w-full h-9 px-3 mb-2 rounded-lg bg-dark-700 border border-border-dark text-white text-sm placeholder:text-slate-500 focus:outline-none focus:border-primary"
              />
              <textarea
                value={docFields}
                onChange={e => setDocFields(e.target.value)}
                placeholder='{"title": "...", "content": "..."}'
                rows={3}
                className="w-full px-3 py-2 mb-2 rounded-lg bg-dark-700 border border-border-dark text-white text-sm font-mono placeholder:text-slate-500 focus:outline-none focus:border-primary resize-none"
              />
              <button
                onClick={indexDocument}
                disabled={indexing || !docId.trim()}
                className="w-full h-9 rounded-lg bg-emerald-600 text-white text-sm font-medium hover:bg-emerald-500 disabled:opacity-40"
              >
                {indexing ? "..." : t("fts.indexBtn")}
              </button>
            </div>
          )}
        </div>

        {/* Right panel: Search */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Search bar */}
          <div className="p-4 border-b border-border-dark">
            <div className="flex gap-3 items-end">
              <div className="flex-1">
                <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-1 block">
                  {t("fts.searchQuery")}
                </label>
                <input
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && doSearch()}
                  placeholder={t("fts.searchPlaceholder")}
                  className="w-full h-10 px-4 rounded-lg bg-dark-700 border border-border-dark text-white text-sm placeholder:text-slate-500 focus:outline-none focus:border-primary"
                />
              </div>
              <div>
                <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-1 block">
                  {t("fts.mode")}
                </label>
                <select
                  value={searchMode}
                  onChange={e => setSearchMode(e.target.value as "normal" | "fuzzy")}
                  className="h-10 px-3 rounded-lg bg-dark-700 border border-border-dark text-white text-sm focus:outline-none focus:border-primary"
                >
                  <option value="normal">BM25</option>
                  <option value="fuzzy">Fuzzy</option>
                </select>
              </div>
              {searchMode === "fuzzy" && (
                <div>
                  <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-1 block">
                    {t("fts.maxDist")}
                  </label>
                  <input
                    type="number"
                    value={fuzzyDist}
                    onChange={e => setFuzzyDist(Number(e.target.value))}
                    min={1}
                    max={3}
                    className="w-16 h-10 px-3 rounded-lg bg-dark-700 border border-border-dark text-white text-sm focus:outline-none focus:border-primary"
                  />
                </div>
              )}
              <div>
                <label className="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-1 block">
                  {t("fts.limit")}
                </label>
                <input
                  type="number"
                  value={searchLimit}
                  onChange={e => setSearchLimit(Number(e.target.value))}
                  min={1}
                  max={100}
                  className="w-20 h-10 px-3 rounded-lg bg-dark-700 border border-border-dark text-white text-sm focus:outline-none focus:border-primary"
                />
              </div>
              <button
                onClick={doSearch}
                disabled={searching || !selectedIndex || !searchQuery.trim()}
                className="h-10 px-6 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary/90 disabled:opacity-40 flex items-center gap-2"
              >
                <span className="material-symbols-outlined text-[18px]">search</span>
                {searching ? t("common.searching") : t("fts.searchBtn")}
              </button>
            </div>
          </div>

          {/* Search results */}
          <div className="flex-1 overflow-y-auto p-4">
            {!selectedIndex && (
              <div className="flex flex-col items-center justify-center h-full text-slate-500">
                <span className="material-symbols-outlined text-[48px] mb-3">search</span>
                <p className="text-sm">{t("fts.selectIndexHint")}</p>
              </div>
            )}

            {selectedIndex && !hits && !searchError && (
              <div className="flex flex-col items-center justify-center h-full text-slate-500">
                <span className="material-symbols-outlined text-[48px] mb-3">manage_search</span>
                <p className="text-sm">{t("fts.searchHint")}</p>
              </div>
            )}

            {searchError && (
              <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
                {searchError}
              </div>
            )}

            {hits && (
              <div>
                <div className="flex items-center justify-between mb-4">
                  <p className="text-sm text-slate-400">
                    {t("fts.found", { count: hits.length })}
                    {elapsed !== null && <span className="ml-2 text-slate-500">({elapsed}ms)</span>}
                  </p>
                </div>
                {hits.length === 0 ? (
                  <p className="text-sm text-slate-500">{t("fts.noResults")}</p>
                ) : (
                  <div className="space-y-3">
                    {hits.map((hit, i) => (
                      <div key={i} className="p-4 rounded-lg bg-surface border border-border-dark">
                        <div className="flex items-center justify-between mb-2">
                          <span className="text-sm font-mono text-primary">{hit.doc_id}</span>
                          <span className="text-xs text-slate-500 font-mono">
                            {t("fts.score")}: {hit.score.toFixed(4)}
                          </span>
                        </div>
                        <div className="space-y-1">
                          {Object.entries(hit.fields).map(([key, value]) => (
                            <div key={key} className="text-sm">
                              <span className="text-slate-400 font-medium">{key}:</span>{" "}
                              <span className="text-slate-200">
                                {String(value)}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
