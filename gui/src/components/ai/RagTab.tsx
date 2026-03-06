import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { execAi } from "../../lib/tauri";
import { useToastStore } from "../../stores/toastStore";
import { Button } from "../ui";

export default function RagTab() {
  const { t } = useTranslation();
  const addToast = useToastStore(s => s.addToast);
  const [docs, setDocs] = useState<any[] | null>(null);
  const [docCount, setDocCount] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [deletingDoc, setDeletingDoc] = useState(false);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [countRes, listRes] = await Promise.all([
        execAi("document_count"),
        execAi("list_documents"),
      ]);
      if (countRes.ok) setDocCount((countRes.data as any)?.count ?? 0);
      if (listRes.ok) setDocs((listRes.data as any)?.documents || []);
      if (!countRes.ok) setError(countRes.error ?? t("common.unknownError"));
      else if (!listRes.ok) setError(listRes.error ?? t("common.unknownError"));
    } catch (e) { setError(String(e)); }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const delDoc = async (docId: string) => {
    if (deletingDoc) return;
    if (!confirm(t("ai.confirmDeleteDocument"))) return;
    setDeletingDoc(true);
    try {
      const data = await execAi("delete_document", { doc_id: docId });
      if (data.ok) load();
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
    setDeletingDoc(false);
  };

  return (
    <div>
      <Button variant="secondary" icon="refresh" onClick={load} className="mb-4">
        {t("ai.refreshRag")}
      </Button>
      <div className="bg-surface border border-border-dark rounded-lg p-4 min-h-[200px]">
        {error && <div className="mb-3 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>}
        {docs === null && !error && (
          loading ? (
            <div className="flex items-center justify-center py-10">
              <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
              <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
            </div>
          ) : <p className="text-slate-400 text-sm text-center py-10">{t("ai.ragHint")}</p>
        )}
        {docCount !== null && (
          <div className="mb-4">
            <div className="bg-dark-800 border border-border-dark rounded-lg p-4 inline-block">
              <p className="text-[11px] text-slate-400 uppercase tracking-wider">{t("ai.documents")}</p>
              <p className="text-2xl font-bold text-primary mt-1">{docCount}</p>
            </div>
          </div>
        )}
        {docs && docs.length > 0 && (
          <table className="w-full border-collapse text-sm font-mono">
            <thead>
              <tr>
                <th className="bg-dark-700/50 px-3 py-2.5 text-left text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0">ID</th>
                <th className="bg-dark-700/50 px-3 py-2.5 text-left text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0">{t("ai.source")}</th>
                <th className="bg-dark-700/50 px-3 py-2.5 text-left text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0">{t("ai.chunks")}</th>
                <th className="bg-dark-700/50 px-3 py-2.5 text-left text-[11px] uppercase tracking-wider text-slate-400 font-semibold border-b border-border-dark sticky top-0">{t("ai.actions")}</th>
              </tr>
            </thead>
            <tbody>
              {docs.map((doc) => {
                const id = doc.id ?? doc.doc_id ?? "?";
                return (
                  <tr key={id} className="hover:bg-white/[0.02] transition-colors">
                    <td className="px-3 py-1.5 border-b border-border-dark text-primary max-w-[200px] truncate" title={id}>{id}</td>
                    <td className="px-3 py-1.5 border-b border-border-dark text-slate-300 max-w-[300px] truncate" title={doc.source || "-"}>{doc.source || "-"}</td>
                    <td className="px-3 py-1.5 border-b border-border-dark text-slate-300">{doc.chunk_count ?? doc.chunks?.length ?? "?"}</td>
                    <td className="px-3 py-1.5 border-b border-border-dark">
                      <button onClick={() => delDoc(id)} className="text-red-400 hover:underline text-xs flex items-center gap-1"><span className="material-symbols-outlined text-[13px]">delete</span> {t("common.delete")}</button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
        {docs && docs.length === 0 && <p className="text-slate-400 text-sm text-center py-4">{t("ai.noDocuments")}</p>}
      </div>
    </div>
  );
}
