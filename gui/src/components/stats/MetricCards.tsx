import { useTranslation } from "react-i18next";

interface MetricCard {
  label: string;
  value: number;
  icon: string;
  color: string;
  bg: string;
}

interface MetricCardsProps {
  cards: MetricCard[];
  fmtNum: (n: number) => string;
}

export default function MetricCards({ cards, fmtNum }: MetricCardsProps) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
      {cards.map(({ label, value, icon, color, bg }) => (
        <div key={label} className="bg-surface border border-border-dark rounded-xl p-5 flex items-center justify-between">
          <div>
            <p className="text-xs text-slate-400 font-medium">{label}</p>
            <p className="text-3xl font-bold text-white mt-1 tabular-nums">{fmtNum(value)}</p>
          </div>
          <div className={`w-12 h-12 rounded-xl ${bg} flex items-center justify-center`}>
            <span className={`material-symbols-outlined text-[24px] ${color}`}>{icon}</span>
          </div>
        </div>
      ))}
    </div>
  );
}
