import Sidebar from "./components/Sidebar";
import ConnectPage from "./pages/ConnectPage";
import ExplorerPage from "./pages/ExplorerPage";
import SqlPage from "./pages/SqlPage";
import KvPage from "./pages/KvPage";
import MqPage from "./pages/MqPage";
import VectorPage from "./pages/VectorPage";
import GeoPage from "./pages/GeoPage";
import FtsPage from "./pages/FtsPage";
import GraphPage from "./pages/GraphPage";
import AiPage from "./pages/AiPage";
import TsPage from "./pages/TsPage";
import StatsPage from "./pages/StatsPage";
import SettingsPage from "./pages/SettingsPage";
import { useEffect } from "react";
import { useAppStore, type PageId } from "./stores/appStore";
import ToastContainer from "./components/ToastContainer";

const pageOrder: PageId[] = [
  "connect", "explorer", "sql", "kv", "mq", "vector", "geo", "fts", "graph", "ai", "ts", "stats",
];

const pages: Record<PageId, React.ReactNode> = {
  connect: <ConnectPage />,
  explorer: <ExplorerPage />,
  sql: <SqlPage />,
  kv: <KvPage />,
  mq: <MqPage />,
  vector: <VectorPage />,
  geo: <GeoPage />,
  fts: <FtsPage />,
  graph: <GraphPage />,
  ai: <AiPage />,
  ts: <TsPage />,
  stats: <StatsPage />,
  settings: <SettingsPage />,
};

export default function App() {
  const page = useAppStore((s) => s.page);
  const setPage = useAppStore((s) => s.setPage);
  const connected = useAppStore((s) => s.connected);

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (!e.metaKey && !e.ctrlKey) return;
      if (e.key === ",") { e.preventDefault(); setPage("settings"); return; }
      const num = parseInt(e.key);
      if (num >= 1 && num <= pageOrder.length) {
        e.preventDefault();
        const target = pageOrder[num - 1];
        if (target === "connect" || connected) setPage(target);
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [connected, setPage]);

  return (
    <div className="relative flex h-screen">
      <Sidebar />
      <main className="flex-1 overflow-y-auto bg-dark-800 relative">
        {/* Fallback drag strip for pages without explicit drag region */}
        <div data-tauri-drag-region className="absolute top-0 left-0 right-0 h-3 z-10" />
        {pages[page] || pages.connect}
      </main>
      <ToastContainer />
    </div>
  );
}
