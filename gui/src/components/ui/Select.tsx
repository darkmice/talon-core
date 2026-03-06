import { useState, useRef, useEffect, useCallback } from "react";

interface SelectOption {
  value: string;
  label: string;
}

interface SelectProps {
  options: SelectOption[];
  value?: string;
  onValueChange?: (value: string) => void;
  icon?: string;
  size?: "sm" | "md";
  placeholder?: string;
  className?: string;
  disabled?: boolean;
}

type SelectSize = "sm" | "md";

export default function Select({
  options, value, onValueChange, icon, size = "md",
  placeholder, className = "", disabled,
}: SelectProps) {
  const [open, setOpen] = useState(false);
  const [focusIdx, setFocusIdx] = useState(-1);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const handleKey = useCallback((e: React.KeyboardEvent) => {
    if (e.key === "Escape") { setOpen(false); return; }
    if (!open && (e.key === "Enter" || e.key === " " || e.key === "ArrowDown")) {
      e.preventDefault(); setOpen(true); setFocusIdx(options.findIndex(o => o.value === value)); return;
    }
    if (!open) return;
    if (e.key === "ArrowDown") { e.preventDefault(); setFocusIdx(i => Math.min(i + 1, options.length - 1)); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setFocusIdx(i => Math.max(i - 1, 0)); }
    else if (e.key === "Enter" && focusIdx >= 0) { e.preventDefault(); onValueChange?.(options[focusIdx].value); setOpen(false); }
  }, [open, focusIdx, options, value, onValueChange]);

  const selectedLabel = options.find(o => o.value === value)?.label ?? placeholder ?? "";

  const sizes: Record<SelectSize, string> = {
    sm: "h-[30px] px-2.5 text-xs",
    md: "h-[36px] px-3 text-sm",
  };

  return (
    <div ref={ref} className={`relative ${className}`}>
      <button
        type="button"
        disabled={disabled}
        onClick={() => !disabled && setOpen(!open)}
        onKeyDown={handleKey}
        className={`w-full flex items-center gap-2 bg-dark-800 border rounded-lg text-left transition outline-none
          ${open ? "border-primary/40 ring-1 ring-primary/20" : "border-border-dark hover:border-slate-600"}
          ${disabled ? "opacity-40 cursor-not-allowed" : "cursor-pointer"}
          ${sizes[size]}`}
      >
        {icon && (
          <span className="material-symbols-outlined text-slate-500 shrink-0"
            style={{ fontSize: size === "sm" ? "14px" : "16px" }}>{icon}</span>
        )}
        <span className={`flex-1 truncate ${value ? "text-slate-200" : "text-slate-500"}`} title={selectedLabel}>
          {selectedLabel}
        </span>
        <span className="material-symbols-outlined text-slate-500 text-[14px] shrink-0 transition-transform"
          style={{ transform: open ? "rotate(180deg)" : "none" }}>expand_more</span>
      </button>
      {open && (
        <div className="absolute z-50 mt-1 w-full min-w-[140px] bg-dark-700 border border-border-dark rounded-lg shadow-2xl shadow-black/40 overflow-hidden animate-slide-up">
          <div className="max-h-52 overflow-y-auto py-1 no-scrollbar">
            {options.map((opt, i) => (
              <button
                key={opt.value}
                type="button"
                onClick={() => { onValueChange?.(opt.value); setOpen(false); }}
                onMouseEnter={() => setFocusIdx(i)}
                className={`w-full text-left px-3 py-1.5 transition-colors flex items-center gap-2
                  ${size === "sm" ? "text-xs" : "text-sm"}
                  ${opt.value === value
                    ? "text-primary bg-primary/10"
                    : i === focusIdx
                      ? "text-white bg-white/5"
                      : "text-slate-300 hover:bg-white/5"}`}
              >
                {opt.value === value && (
                  <span className="material-symbols-outlined text-[14px] text-primary shrink-0">check</span>
                )}
                <span className={opt.value === value ? "" : "pl-[22px]"}>{opt.label}</span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
