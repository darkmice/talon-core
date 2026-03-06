interface ToggleProps {
  value: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
}

export default function Toggle({ value, onChange, disabled = false }: ToggleProps) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={value}
      onClick={() => !disabled && onChange(!value)}
      disabled={disabled}
      className={`relative w-11 h-6 rounded-full transition-colors ${value ? "bg-primary" : "bg-slate-700"} ${disabled ? "opacity-40 cursor-not-allowed" : "cursor-pointer"}`}
    >
      <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-transform ${value ? "left-6" : "left-1"}`} />
    </button>
  );
}
