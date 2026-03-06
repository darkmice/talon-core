import { useTranslation } from "react-i18next";
import { Button, Input, Textarea, Label } from "../ui";

interface VectorSearchPanelProps {
  name: string;
  query: string;
  topk: number;
  minScore: number;
  loading: boolean;
  onNameChange: (v: string) => void;
  onQueryChange: (v: string) => void;
  onTopkChange: (v: number) => void;
  onMinScoreChange: (v: number) => void;
  onSearch: () => void;
}

export default function VectorSearchPanel({
  name, query, topk, minScore, loading,
  onNameChange, onQueryChange, onTopkChange, onMinScoreChange, onSearch,
}: VectorSearchPanelProps) {
  const { t } = useTranslation();

  return (
    <div className="w-[380px] border-r border-border-dark bg-sidebar flex flex-col shrink-0">
      <div data-tauri-drag-region className="flex items-center justify-between px-5 py-4 border-b border-border-dark">
        <div className="flex items-center gap-2">
          <span className="material-symbols-outlined text-primary text-[20px]">tune</span>
          <span className="text-sm font-semibold text-white">{t("vector.searchParams")}</span>
        </div>
        <button
          onClick={() => { onNameChange(""); onQueryChange(""); onTopkChange(10); onMinScoreChange(0); }}
          className="text-xs text-primary hover:underline"
        >{t("vector.reset")}</button>
      </div>
      <div className="flex-1 overflow-y-auto p-5 space-y-5 no-scrollbar">
        <div>
          <Label>{t("vector.targetIndex")}</Label>
          <Input mono value={name} onChange={e => onNameChange(e.target.value)} className="w-full" />
        </div>
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <Label className="mb-0">{t("vector.queryVector")}</Label>
            <span className="text-[10px] text-slate-400">{t("vector.queryVectorHint")}</span>
          </div>
          <Textarea mono value={query} onChange={e => onQueryChange(e.target.value)} className="h-28" />
          <div className="flex items-center justify-between mt-2">
            <div className="flex gap-2">
              <button
                onClick={() => onQueryChange("")}
                className="text-slate-500 hover:text-white transition"
                title={t("vector.clearQuery")}
              >
                <span className="material-symbols-outlined text-[16px]">refresh</span>
              </button>
              <button className="text-slate-600 cursor-not-allowed" disabled title={t("common.comingSoon")}>
                <span className="material-symbols-outlined text-[16px]">fullscreen</span>
              </button>
            </div>
            <button className="flex items-center gap-1 text-xs text-slate-600 cursor-not-allowed" disabled title={t("common.comingSoon")}>
              <span className="material-symbols-outlined text-[14px]">upload_file</span>
              {t("vector.loadFromFile")}
            </button>
          </div>
        </div>
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <Label className="mb-0">{t("vector.limitTopK")}</Label>
            <span className="text-sm font-semibold text-primary">{topk}</span>
          </div>
          <input type="range" min={1} max={100} value={topk} onChange={e => onTopkChange(Number(e.target.value))}
            className="w-full accent-primary" />
        </div>
        <div>
          <Label>{t("vector.minScore")}</Label>
          <div className="flex items-center gap-3">
            <Input mono size="sm" value={minScore} onChange={e => onMinScoreChange(Number(e.target.value))} className="w-20" />
            <span className="text-[10px] text-slate-400">0.0 - 1.0</span>
          </div>
        </div>
      </div>
      <div className="p-5 border-t border-border-dark">
        <Button variant="primary" icon="search" size="lg" loading={loading} onClick={onSearch} className="w-full">
          {t("vector.runSearch")}
        </Button>
      </div>
    </div>
  );
}
