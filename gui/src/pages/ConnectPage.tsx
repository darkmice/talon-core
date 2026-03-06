import { useState, useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { connect, disconnect, openDatabase } from "../lib/tauri";
import { open } from "@tauri-apps/plugin-dialog";
import { useAppStore } from "../stores/appStore";
import { useConnectionStore } from "../stores/connectionStore";
import { useToastStore } from "../stores/toastStore";
import { useExplorerStore } from "../stores/explorerStore";
import { useSettingsStore } from "../stores/settingsStore";
import { Label, Input } from "../components/ui";

const ICON_COLORS = ["bg-emerald-500", "bg-purple-500", "bg-orange-500", "bg-blue-500", "bg-pink-500"];

function extractHost(u: string) {
  try { return u.replace(/^talon:\/\//, "").replace(/.*@/, "").split("?")[0]; } catch { return u; }
}

export default function ConnectPage() {
  const { t } = useTranslation();
  const { connected, setConnected, setDisconnected } = useAppStore();
  const { history, addToHistory, clearHistory } = useConnectionStore();
  const addToast = useToastStore((s) => s.addToast);
  const autoConnect = useSettingsStore((s) => s.autoConnect);
  const [url, setUrl] = useState(() => history.length > 0 ? history[0].url : "talon://localhost:7721");
  const [msg, setMsg] = useState<{ type: string; text: string } | null>(null);
  const [loading, setLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const autoConnectAttempted = useRef(false);

  useEffect(() => {
    if (autoConnect && !connected && !autoConnectAttempted.current && history.length > 0) {
      autoConnectAttempted.current = true;
      doConnect();
    }
  }, []);

  const doConnect = async () => {
    if (!url.trim()) return;
    setLoading(true); setMsg(null);
    try {
      const result = await connect(url.trim()) as any;
      if (result.ok) {
        setMsg({ type: "success", text: result.message });
        const host = extractHost(url.trim());
        setConnected(host, "tcp");
        addToHistory(url.trim(), host, "tcp");
        addToast("success", t("connect.connectedTo", { host }));
      } else {
        setMsg({ type: "error", text: result.message });
        addToast("error", result.message || t("connect.errorConnect"));
      }
    } catch (e) { setMsg({ type: "error", text: String(e) }); addToast("error", String(e)); }
    setLoading(false);
  };

  const doOpenLocal = async () => {
    setMsg(null);
    try {
      const selected = await open({ directory: true, multiple: false, title: t("connect.selectDbDir") });
      if (!selected) return;
      const dirPath = typeof selected === "string" ? selected : (selected as any);
      setLoading(true);
      const result = await openDatabase(dirPath) as any;
      if (result.ok) {
        const label = dirPath.split("/").pop() || dirPath;
        setMsg({ type: "success", text: result.message });
        setConnected(label, "embedded");
        addToHistory(dirPath, label, "embedded");
        addToast("success", result.message);
      } else {
        setMsg({ type: "error", text: result.message });
        addToast("error", result.message);
      }
    } catch (e) { setMsg({ type: "error", text: String(e) }); addToast("error", String(e)); }
    setLoading(false);
  };

  const doReconnectLocal = async (path: string) => {
    setLoading(true); setMsg(null);
    try {
      const result = await openDatabase(path) as any;
      if (result.ok) {
        const label = path.split("/").pop() || path;
        setMsg({ type: "success", text: result.message });
        setConnected(label, "embedded");
        addToHistory(path, label, "embedded");
        addToast("success", result.message);
      } else {
        setMsg({ type: "error", text: result.message });
        addToast("error", result.message);
      }
    } catch (e) { setMsg({ type: "error", text: String(e) }); addToast("error", String(e)); }
    setLoading(false);
  };

  const doDisconnect = async () => {
    if (disconnecting) return;
    setDisconnecting(true);
    try { await disconnect(); } catch (e) { addToast("error", String(e)); }
    useExplorerStore.getState().resetAll();
    setDisconnected();
    setMsg(null);
    setDisconnecting(false);
    addToast("info", t("connect.disconnected"));
  };

  return (
    <div className="h-full flex items-center justify-center bg-dark-900">
      <div className="w-full max-w-md text-center">
        {/* Logo */}
        <div className="mb-8">
          <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center text-white font-bold text-3xl mx-auto mb-4 shadow-lg shadow-blue-500/20">
            T
          </div>
          <h1 className="text-2xl font-bold text-white">{t("connect.title")}</h1>
          <p className="text-slate-400 text-sm mt-1">{t("connect.subtitle")}</p>
        </div>

        {/* URL Input */}
        <div className="bg-surface border border-border-dark rounded-xl p-6 text-left">
          <Label>{t("connect.urlLabel")}</Label>
          <Input
            mono
            icon="link"
            value={url}
            onChange={e => setUrl(e.target.value)}
            onKeyDown={e => e.key === "Enter" && !connected && doConnect()}
            placeholder={t("connect.urlPlaceholder")}
            className="w-full"
          />

          {/* Buttons */}
          <div className="flex gap-3 mt-5">
            <button onClick={doDisconnect} disabled={!connected || disconnecting}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 bg-dark-800 border border-border-dark text-slate-300 text-sm rounded-lg hover:bg-dark-600 transition disabled:opacity-30 font-medium">
              <span className={`material-symbols-outlined text-[18px] ${disconnecting ? "animate-spin" : ""}`}>{disconnecting ? "progress_activity" : "link_off"}</span>
              {t("connect.disconnectBtn")}
            </button>
            <button onClick={doConnect} disabled={connected || loading || !url.trim()}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 bg-primary hover:bg-primary-hover text-white text-sm rounded-lg transition disabled:opacity-40 font-medium">
              <span className="material-symbols-outlined text-[18px]">login</span>
              {loading ? t("connect.connecting") : t("connect.connectBtn")}
            </button>
          </div>

          {/* Divider */}
          <div className="flex items-center gap-3 mt-5">
            <div className="flex-1 h-px bg-border-dark" />
            <span className="text-xs text-slate-600 uppercase tracking-wider">{t("connect.or")}</span>
            <div className="flex-1 h-px bg-border-dark" />
          </div>

          {/* Open Local Database */}
          <button onClick={doOpenLocal} disabled={connected || loading}
            className="w-full flex items-center justify-center gap-2 px-4 py-3 mt-4 bg-dark-800 border border-dashed border-border-dark text-slate-300 text-sm rounded-lg hover:border-primary/50 hover:bg-dark-700 transition disabled:opacity-30 font-medium">
            <span className="material-symbols-outlined text-[18px] text-emerald-400">folder_open</span>
            {t("connect.openLocalDb")}
          </button>
        </div>

        {/* Status message */}
        {msg && (
          <div className={`mt-4 px-4 py-3 rounded-lg text-sm flex items-center gap-2 ${
            msg.type === "success"
              ? "bg-emerald-500/10 border border-emerald-500/30 text-emerald-400"
              : "bg-red-500/10 border border-red-500/30 text-red-400"
          }`}>
            <span className={`w-2 h-2 rounded-full ${msg.type === "success" ? "bg-emerald-400" : "bg-red-400"}`} />
            {msg.text}
          </div>
        )}

        {/* Recent Connections */}
        {history.length > 0 && (
          <div className="mt-6 text-left">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">{t("connect.recentTitle")}</h3>
              <button onClick={() => { if (confirm(t("connect.confirmClearHistory"))) clearHistory(); }} className="text-xs text-primary hover:text-primary-hover transition">
                {t("connect.clearHistory")}
              </button>
            </div>
            <div className="space-y-2">
              {history.map((h, i) => {
                const isLocal = h.mode === "embedded";
                const displayLabel = isLocal ? h.label : h.label.includes("localhost") ? t("connect.localDev") : h.label.includes("staging") ? t("connect.staging") : h.label || t("connect.connectionN", { n: i + 1 });
                return (
                  <button key={i} onClick={() => isLocal ? doReconnectLocal(h.url) : setUrl(h.url)}
                    className="w-full flex items-center gap-3 px-4 py-3 bg-surface border border-border-dark rounded-lg hover:border-primary/40 transition text-left group">
                    <div className={`w-8 h-8 rounded-lg ${isLocal ? "bg-emerald-600" : ICON_COLORS[i % ICON_COLORS.length]} flex items-center justify-center text-white text-xs font-bold`}>
                      <span className="material-symbols-outlined text-[16px]">{isLocal ? "folder" : "dns"}</span>
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-white truncate" title={displayLabel}>{displayLabel}</p>
                      <p className="text-xs text-slate-400 font-mono truncate" title={h.url}>{h.url}</p>
                    </div>
                    {isLocal && <span className="text-[10px] bg-emerald-500/15 text-emerald-400 px-1.5 py-0.5 rounded font-medium">{t("connect.local")}</span>}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Footer */}
        <div className="mt-8 flex items-center justify-center gap-4 text-xs text-slate-500">
          <span>{t("connect.docs")}</span>
          <span>·</span>
          <span>{t("connect.support")}</span>
          <span>·</span>
          <span>v{__APP_VERSION__}</span>
        </div>
      </div>
    </div>
  );
}
