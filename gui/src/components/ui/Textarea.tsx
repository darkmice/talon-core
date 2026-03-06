import type { TextareaHTMLAttributes } from "react";

type TextareaSize = "sm" | "md";

interface TextareaProps extends Omit<TextareaHTMLAttributes<HTMLTextAreaElement>, "size"> {
  size?: TextareaSize;
  mono?: boolean;
}

export default function Textarea({
  size = "md", mono, className = "", ...rest
}: TextareaProps) {
  const sizes: Record<TextareaSize, string> = {
    sm: "px-2.5 py-1.5 text-xs",
    md: "px-3 py-2.5 text-sm",
  };
  return (
    <textarea
      className={`w-full bg-dark-800 border border-border-dark rounded-lg outline-none focus:border-primary/40 focus:ring-1 focus:ring-primary/20 transition text-slate-200 resize-none leading-relaxed ${sizes[size]} ${mono ? "font-mono text-[12px]" : ""} ${className}`}
      {...rest}
    />
  );
}
