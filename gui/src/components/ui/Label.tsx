import type { ReactNode } from "react";

interface LabelProps {
  children: ReactNode;
  required?: boolean;
  className?: string;
}

export default function Label({ children, required, className = "" }: LabelProps) {
  return (
    <label className={`block text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1.5 ${className}`}>
      {children}
      {required && <span className="text-red-400 ml-0.5">*</span>}
    </label>
  );
}
