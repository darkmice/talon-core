import type { ReactNode } from "react";

interface PageHeaderProps {
  icon?: string;
  title: string;
  subtitle?: string;
  children?: ReactNode;
  className?: string;
}

export default function PageHeader({
  icon, title, subtitle, children, className = "",
}: PageHeaderProps) {
  return (
    <header
      data-tauri-drag-region
      className={`h-14 border-b border-border-dark flex items-center justify-between px-5 bg-dark-800 shrink-0 ${className}`}
    >
      <div className="flex items-center gap-3">
        {icon && (
          <span className="material-symbols-outlined text-primary text-[20px]">{icon}</span>
        )}
        <div>
          <h2 className="text-base font-bold text-white tracking-tight">{title}</h2>
          {subtitle && <p className="text-[11px] text-slate-400 leading-tight">{subtitle}</p>}
        </div>
      </div>
      {children && <div className="flex items-center gap-3">{children}</div>}
    </header>
  );
}
