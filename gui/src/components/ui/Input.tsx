import type { InputHTMLAttributes } from "react";

type InputSize = "sm" | "md";

interface InputProps extends Omit<InputHTMLAttributes<HTMLInputElement>, "size"> {
  icon?: string;
  size?: InputSize;
  mono?: boolean;
}

export default function Input({
  icon, size = "md", mono, className = "", ...rest
}: InputProps) {
  const sizes: Record<InputSize, string> = {
    sm: "px-2.5 py-1.5 text-xs",
    md: "px-3 py-2 text-sm",
  };
  if (icon) {
    return (
      <div className={`flex items-center gap-2 bg-dark-800 border border-border-dark rounded-lg focus-within:border-primary/40 focus-within:ring-1 focus-within:ring-primary/20 transition ${sizes[size]} ${className}`}>
        <span className="material-symbols-outlined text-slate-500" style={{ fontSize: size === "sm" ? "14px" : "16px" }}>
          {icon}
        </span>
        <input
          className={`flex-1 bg-transparent outline-none placeholder-slate-600 ${mono ? "font-mono" : ""} text-slate-300`}
          {...rest}
        />
      </div>
    );
  }
  return (
    <input
      className={`bg-dark-800 border border-border-dark rounded-lg outline-none focus:border-primary/40 focus:ring-1 focus:ring-primary/20 transition text-slate-200 placeholder-slate-600 ${sizes[size]} ${mono ? "font-mono" : ""} ${className}`}
      {...rest}
    />
  );
}
