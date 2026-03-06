import type { ReactNode } from "react";

interface EmptyStateProps {
  icon?: string;
  title?: string;
  description?: string;
  children?: ReactNode;
}

export default function EmptyState({ icon = "database", title, description, children }: EmptyStateProps) {
  return (
    <div className="flex-1 flex items-center justify-center">
      <div className="text-center">
        <span className="material-symbols-outlined text-[48px] text-slate-700">{icon}</span>
        {title && <p className="text-slate-400 text-sm mt-3">{title}</p>}
        {description && <p className="text-slate-500 text-xs mt-1">{description}</p>}
        {children && <div className="mt-4">{children}</div>}
      </div>
    </div>
  );
}
