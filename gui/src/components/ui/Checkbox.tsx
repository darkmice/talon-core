interface CheckboxProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  disabled?: boolean;
  className?: string;
}

export default function Checkbox({ checked, onChange, disabled, className = "" }: CheckboxProps) {
  return (
    <button
      type="button"
      role="checkbox"
      aria-checked={checked}
      onClick={() => !disabled && onChange(!checked)}
      disabled={disabled}
      className={`w-4 h-4 rounded border flex items-center justify-center transition-colors
        ${checked
          ? "bg-primary border-primary text-white"
          : "bg-transparent border-slate-600 hover:border-slate-400"
        }
        ${disabled ? "opacity-40 cursor-not-allowed" : "cursor-pointer"}
        ${className}`}
    >
      {checked && (
        <svg width="10" height="10" viewBox="0 0 10 10" fill="none" className="text-white">
          <path d="M2 5L4.5 7.5L8 3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      )}
    </button>
  );
}
