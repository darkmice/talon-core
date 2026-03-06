interface TabItem {
  id: string;
  label: string;
}

interface TabsProps {
  items: TabItem[];
  active: string;
  onChange: (id: string) => void;
  className?: string;
}

export default function Tabs({ items, active, onChange, className = "" }: TabsProps) {
  return (
    <div className={`flex gap-0 ${className}`}>
      {items.map(({ id, label }) => (
        <button
          key={id}
          onClick={() => onChange(id)}
          className={`px-4 py-3 text-sm font-medium transition border-b-2 ${
            active === id
              ? "border-primary text-primary"
              : "border-transparent text-slate-500 hover:text-slate-300"
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
