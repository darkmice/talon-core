import type { ReactNode } from "react";

type BadgeVariant = "default" | "primary" | "success" | "warning" | "danger" | "info";
type BadgeSize = "sm" | "md";

interface BadgeProps {
  children: ReactNode;
  variant?: BadgeVariant;
  size?: BadgeSize;
  className?: string;
}

export default function Badge({ children, variant = "default", size = "sm", className = "" }: BadgeProps) {
  const sizes: Record<BadgeSize, string> = {
    sm: "px-2 py-0.5 text-[10px]",
    md: "px-2.5 py-1 text-xs",
  };
  const variants: Record<BadgeVariant, string> = {
    default: "bg-surface text-slate-400 border border-border-dark",
    primary: "bg-primary/15 text-primary",
    success: "bg-emerald-500/15 text-emerald-400",
    warning: "bg-yellow-500/15 text-yellow-400",
    danger: "bg-red-500/15 text-red-400",
    info: "bg-blue-500/15 text-blue-400",
  };
  return (
    <span className={`inline-flex items-center rounded-full font-medium uppercase tracking-wider ${sizes[size]} ${variants[variant]} ${className}`}>
      {children}
    </span>
  );
}
