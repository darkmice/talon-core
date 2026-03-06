import { useToastStore } from "../stores/toastStore";

const iconMap: Record<string, string> = {
  success: "check_circle",
  error: "error",
  info: "info",
  warning: "warning",
};

const colorMap: Record<string, string> = {
  success: "bg-emerald-500/10 border-emerald-500/30 text-emerald-400",
  error: "bg-red-500/10 border-red-500/30 text-red-400",
  info: "bg-blue-500/10 border-blue-500/30 text-blue-400",
  warning: "bg-yellow-500/10 border-yellow-500/30 text-yellow-400",
};

export default function ToastContainer() {
  const { toasts, removeToast } = useToastStore();

  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-5 right-5 z-[100] flex flex-col gap-2 pointer-events-none">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`pointer-events-auto flex items-center gap-2.5 px-4 py-3 rounded-lg border text-sm font-medium shadow-lg shadow-black/20 animate-slide-up ${colorMap[t.type] || colorMap.info}`}
        >
          <span className="material-symbols-outlined text-[18px]">
            {iconMap[t.type] || "info"}
          </span>
          <span className="flex-1">{t.message}</span>
          <button
            onClick={() => removeToast(t.id)}
            className="opacity-60 hover:opacity-100 transition"
            title="Close"
          >
            <span className="material-symbols-outlined text-[14px]">close</span>
          </button>
        </div>
      ))}
    </div>
  );
}
