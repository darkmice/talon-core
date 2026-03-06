import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Input, Label } from "../ui";

export default function DistanceTab() {
  const { t } = useTranslation();
  const [lat1, setLat1] = useState("39.9042");
  const [lng1, setLng1] = useState("116.4074");
  const [lat2, setLat2] = useState("31.2304");
  const [lng2, setLng2] = useState("121.4737");
  const [result, setResult] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const calc = () => {
    const toRad = (d: number) => d * Math.PI / 180;
    const R = 6371000;
    const la1 = parseFloat(lat1), lo1 = parseFloat(lng1);
    const la2 = parseFloat(lat2), lo2 = parseFloat(lng2);
    if ([la1, lo1, la2, lo2].some(isNaN)) { setError(t("geo.invalidCoords")); setResult(null); return; }
    setError(null);
    const dLat = toRad(la2 - la1), dLng = toRad(lo2 - lo1);
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(la1)) * Math.cos(toRad(la2)) * Math.sin(dLng / 2) ** 2;
    const dist = 2 * R * Math.asin(Math.sqrt(a));
    setResult(dist);
  };

  return (
    <div className="max-w-lg">
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div className="bg-surface border border-border-dark rounded-lg p-4">
          <h3 className="text-sm font-medium text-white mb-3 flex items-center gap-1.5"><span className="material-symbols-outlined text-[14px] text-primary">location_on</span> {t("geo.pointA")}</h3>
          <div className="flex flex-col gap-2">
            <div>
              <Label>{t("geo.lat")}</Label>
              <Input mono value={lat1} onChange={e => setLat1(e.target.value)} className="w-full" />
            </div>
            <div>
              <Label>{t("geo.lng")}</Label>
              <Input mono value={lng1} onChange={e => setLng1(e.target.value)} className="w-full" />
            </div>
          </div>
        </div>
        <div className="bg-surface border border-border-dark rounded-lg p-4">
          <h3 className="text-sm font-medium text-white mb-3 flex items-center gap-1.5"><span className="material-symbols-outlined text-[14px] text-emerald-400">location_on</span> {t("geo.pointB")}</h3>
          <div className="flex flex-col gap-2">
            <div>
              <Label>{t("geo.lat")}</Label>
              <Input mono value={lat2} onChange={e => setLat2(e.target.value)} className="w-full" />
            </div>
            <div>
              <Label>{t("geo.lng")}</Label>
              <Input mono value={lng2} onChange={e => setLng2(e.target.value)} className="w-full" />
            </div>
          </div>
        </div>
      </div>
      <Button variant="primary" icon="straighten" onClick={calc}>
        {t("geo.calcBtn")}
      </Button>
      {error && (
        <div className="mt-3 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-2.5">{error}</div>
      )}
      {result !== null && !error && (
        <div className="mt-4 bg-surface border border-border-dark rounded-xl p-5">
          <p className="text-[11px] text-slate-400 uppercase tracking-wider">{t("geo.haversineTitle")}</p>
          <p className="text-3xl font-bold text-primary mt-1">{result < 1000 ? `${result.toFixed(1)} m` : `${(result / 1000).toFixed(2)} km`}</p>
          <p className="text-xs text-slate-400 mt-2">
            {t("geo.distanceDetail", { m: result.toFixed(1), km: (result / 1000).toFixed(3) })}
          </p>
        </div>
      )}
    </div>
  );
}
