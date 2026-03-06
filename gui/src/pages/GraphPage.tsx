import { useState } from "react";
import { useTranslation } from "react-i18next";
import { execGraph } from "../lib/tauri";
import { useToastStore } from "../stores/toastStore";
import { PageHeader } from "../components/ui";

export default function GraphPage() {
  const { t } = useTranslation();
  const [graphName, setGraphName] = useState("");
  const [vertexLabel, setVertexLabel] = useState("");
  const [vertexProps, setVertexProps] = useState("{}");
  const [edgeFromId, setEdgeFromId] = useState("");
  const [edgeToId, setEdgeToId] = useState("");
  const [edgeLabel, setEdgeLabel] = useState("");
  const [edgeProps, setEdgeProps] = useState("{}");
  const [queryType, setQueryType] = useState<"neighbors" | "bfs" | "shortest_path">("neighbors");
  const [queryVertexId, setQueryVertexId] = useState("");
  const [queryTarget, setQueryTarget] = useState("");
  const [queryDepth, setQueryDepth] = useState(3);
  const [queryDir, setQueryDir] = useState("out");
  const [results, setResults] = useState<any>(null);
  const [qErr, setQErr] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);

  const notify = (ty: "success" | "error", m: string) =>
    useToastStore.getState().addToast(ty, m);

  const toU64 = (s: string): number | null => {
    const n = Number(s);
    return Number.isFinite(n) && n >= 0 && Number.isInteger(n) ? n : null;
  };

  const createGraph = async () => {
    if (!graphName.trim()) return;
    try {
      const r = await execGraph("create", { graph: graphName.trim() });
      if (r.ok) notify("success", t("graph.createSuccess", { name: graphName }));
      else notify("error", r.error ?? t("common.unknownError"));
    } catch (e) { notify("error", String(e)); }
  };
  const addVertex = async () => {
    if (!graphName.trim() || !vertexLabel.trim()) return;
    let p: any;
    try { p = JSON.parse(vertexProps); } catch { notify("error", t("common.invalidJson")); return; }
    try {
      const r = await execGraph("add_vertex", { graph: graphName.trim(), label: vertexLabel.trim(), properties: p });
      if (r.ok) { notify("success", t("graph.vertexAdded", { id: (r.data as any)?.vertex_id ?? "" })); setVertexLabel(""); setVertexProps("{}"); }
      else notify("error", r.error ?? t("common.unknownError"));
    } catch (e) { notify("error", String(e)); }
  };
  const addEdge = async () => {
    if (!graphName.trim() || !edgeFromId.trim() || !edgeToId.trim()) return;
    const fid = toU64(edgeFromId), tid = toU64(edgeToId);
    if (fid === null || tid === null) { notify("error", t("graph.invalidVertexId")); return; }
    let p: any;
    try { p = JSON.parse(edgeProps); } catch { notify("error", t("common.invalidJson")); return; }
    try {
      const r = await execGraph("add_edge", { graph: graphName.trim(), from: fid, to: tid, label: edgeLabel.trim() || undefined, properties: p });
      if (r.ok) { notify("success", t("graph.edgeAdded", { id: (r.data as any)?.edge_id ?? "" })); setEdgeFromId(""); setEdgeToId(""); }
      else notify("error", r.error ?? t("common.unknownError"));
    } catch (e) { notify("error", String(e)); }
  };
  const doQuery = async () => {
    if (!graphName.trim() || !queryVertexId.trim()) return;
    const vid = toU64(queryVertexId);
    if (vid === null) { notify("error", t("graph.invalidVertexId")); return; }
    if (queryType === "shortest_path") {
      if (!queryTarget.trim()) return;
      if (toU64(queryTarget) === null) { notify("error", t("graph.invalidVertexId")); return; }
    }
    setBusy(true); setQErr(null);
    const t0 = performance.now();
    try {
      let r: any;
      if (queryType === "neighbors") {
        r = await execGraph("neighbors", { graph: graphName.trim(), id: vid, direction: queryDir });
      } else if (queryType === "bfs") {
        r = await execGraph("bfs", { graph: graphName.trim(), start: vid, max_depth: queryDepth, direction: queryDir });
      } else {
        r = await execGraph("shortest_path", { graph: graphName.trim(), from: vid, to: toU64(queryTarget)!, max_depth: queryDepth });
      }
      setElapsed(Math.round(performance.now() - t0));
      if (r.ok) setResults(r.data); else { setQErr(r.error ?? ""); setResults(null); }
    } catch (e) {
      setElapsed(Math.round(performance.now() - t0));
      setQErr(String(e)); setResults(null);
    }
    setBusy(false);
  };

  const inp = "w-full h-9 px-3 rounded-lg bg-dark-700 border border-border-dark text-white text-sm placeholder:text-slate-500 focus:outline-none focus:border-primary";
  const lbl = "text-xs text-slate-400 font-semibold uppercase tracking-wider mb-1 block";
  const btn = "h-9 px-4 rounded-lg text-white text-sm font-medium disabled:opacity-40";

  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="hub" title={t("graph.title")} subtitle={t("graph.subtitle")} />
      <div className="flex flex-1 overflow-hidden">
        <div className="w-[320px] flex-shrink-0 border-r border-border-dark overflow-y-auto">
          <div className="p-4 border-b border-border-dark">
            <label className={lbl}>{t("graph.graphName")}</label>
            <div className="flex gap-2">
              <input value={graphName} onChange={e => setGraphName(e.target.value)} placeholder={t("graph.graphNamePlaceholder")} className={inp + " flex-1"} />
              <button onClick={createGraph} disabled={!graphName.trim()} className={btn + " bg-primary hover:bg-primary/90"}>{t("graph.createBtn")}</button>
            </div>
          </div>
          <div className="p-4 border-b border-border-dark space-y-2">
            <label className={lbl}>{t("graph.addVertex")}</label>
            <input value={vertexLabel} onChange={e => setVertexLabel(e.target.value)} placeholder={t("graph.vertexLabelPlaceholder")} className={inp} />
            <textarea value={vertexProps} onChange={e => setVertexProps(e.target.value)} placeholder='{"key":"value"}' rows={2} className={inp + " h-auto py-2 font-mono resize-none"} />
            <button onClick={addVertex} disabled={!graphName.trim() || !vertexLabel.trim()} className={btn + " w-full bg-emerald-600 hover:bg-emerald-500"}>{t("graph.addVertexBtn")}</button>
          </div>
          <div className="p-4 space-y-2">
            <label className={lbl}>{t("graph.addEdge")}</label>
            <input value={edgeFromId} onChange={e => setEdgeFromId(e.target.value)} placeholder={t("graph.fromPlaceholder")} className={inp} />
            <input value={edgeToId} onChange={e => setEdgeToId(e.target.value)} placeholder={t("graph.toPlaceholder")} className={inp} />
            <input value={edgeLabel} onChange={e => setEdgeLabel(e.target.value)} placeholder={t("graph.edgeLabelPlaceholder")} className={inp} />
            <textarea value={edgeProps} onChange={e => setEdgeProps(e.target.value)} placeholder='{"weight":"1.0"}' rows={2} className={inp + " h-auto py-2 font-mono resize-none"} />
            <button onClick={addEdge} disabled={!graphName.trim() || !edgeFromId.trim() || !edgeToId.trim()} className={btn + " w-full bg-blue-600 hover:bg-blue-500"}>{t("graph.addEdgeBtn")}</button>
          </div>
        </div>
        <div className="flex-1 flex flex-col overflow-hidden">
          <div className="p-4 border-b border-border-dark">
            <div className="flex gap-3 items-end flex-wrap">
              <div>
                <label className={lbl}>{t("graph.queryType")}</label>
                <select value={queryType} onChange={e => setQueryType(e.target.value as any)} className={inp + " w-40"}>
                  <option value="neighbors">{t("graph.neighbors")}</option>
                  <option value="bfs">{t("graph.bfs")}</option>
                  <option value="shortest_path">{t("graph.shortestPath")}</option>
                </select>
              </div>
              <div className="flex-1">
                <label className={lbl}>{t("graph.vertexId")}</label>
                <input value={queryVertexId} onChange={e => setQueryVertexId(e.target.value)} onKeyDown={e => e.key === "Enter" && doQuery()} placeholder="0" className={inp} />
              </div>
              {queryType === "shortest_path" && (
                <div className="flex-1">
                  <label className={lbl}>{t("graph.targetVertex")}</label>
                  <input value={queryTarget} onChange={e => setQueryTarget(e.target.value)} placeholder="1" className={inp} />
                </div>
              )}
              {queryType !== "shortest_path" && (
                <div>
                  <label className={lbl}>{t("graph.direction")}</label>
                  <select value={queryDir} onChange={e => setQueryDir(e.target.value)} className={inp + " w-28"}>
                    <option value="out">Out</option>
                    <option value="in">In</option>
                    <option value="both">Both</option>
                  </select>
                </div>
              )}
              {queryType !== "neighbors" && (
                <div>
                  <label className={lbl}>{t("graph.maxDepth")}</label>
                  <input type="number" value={queryDepth} onChange={e => setQueryDepth(Number(e.target.value))} min={1} max={10} className={inp + " w-20"} />
                </div>
              )}
              <button onClick={doQuery} disabled={busy || !graphName.trim() || !queryVertexId.trim()} className={btn + " h-10 px-6 bg-primary hover:bg-primary/90 flex items-center gap-2"}>
                <span className="material-symbols-outlined text-[18px]">play_arrow</span>
                {busy ? "..." : t("graph.queryBtn")}
              </button>
            </div>
          </div>
          <div className="flex-1 overflow-y-auto p-4">
            {qErr && <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">{qErr}</div>}
            {!results && !qErr && (
              <div className="flex flex-col items-center justify-center h-full text-slate-500">
                <span className="material-symbols-outlined text-[48px] mb-3">hub</span>
                <p className="text-sm">{t("graph.queryHint")}</p>
              </div>
            )}
            {results && (
              <div>
                <p className="text-sm text-slate-400 mb-3">
                  {t("graph.queryResult")}
                  {elapsed !== null && <span className="ml-2 text-slate-500">({elapsed}ms)</span>}
                </p>
                <pre className="p-4 rounded-lg bg-dark-700 border border-border-dark text-sm text-slate-200 font-mono overflow-x-auto whitespace-pre-wrap">
                  {JSON.stringify(results, null, 2)}
                </pre>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
