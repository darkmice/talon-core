import type { ButtonHTMLAttributes, ReactNode } from "react";

type Variant = "primary" | "secondary" | "ghost" | "danger";
type Size = "sm" | "md" | "lg";

interface ButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  children?: ReactNode;
  variant?: Variant;
  size?: Size;
  icon?: string;
  loading?: boolean;
}

export default function Button({
  children, variant = "primary", size = "md", icon, loading, disabled,
  className = "", type = "button", ...rest
}: ButtonProps) {
  const base = "inline-flex items-center justify-center gap-2 font-medium rounded-lg transition whitespace-nowrap";
  const sizes: Record<Size, string> = {
    sm: "px-3 py-1.5 text-xs",
    md: "px-4 py-2 text-sm",
    lg: "px-5 py-3 text-sm",
  };
  const variants: Record<Variant, string> = {
    primary: "btn-glow bg-primary hover:bg-primary-hover text-white shadow-[0_0_20px_rgba(19,91,236,0.3)] hover:shadow-[0_0_30px_rgba(19,91,236,0.5)]",
    secondary: "bg-surface border border-border-dark text-slate-300 hover:bg-dark-600",
    ghost: "text-slate-400 hover:bg-white/5 hover:text-white",
    danger: "bg-red-500/10 border border-red-500/30 text-red-400 hover:bg-red-500/20",
  };
  return (
    <button
      type={type}
      disabled={disabled || loading}
      className={`${base} ${sizes[size]} ${variants[variant]} ${disabled || loading ? "opacity-40 cursor-not-allowed" : ""} ${className}`}
      {...rest}
    >
      {(icon || loading) && (
        <span className={`material-symbols-outlined ${loading ? "animate-spin" : ""}`} style={{ fontSize: size === "sm" ? "14px" : "16px" }}>
          {loading ? "progress_activity" : icon}
        </span>
      )}
      {children}
    </button>
  );
}
