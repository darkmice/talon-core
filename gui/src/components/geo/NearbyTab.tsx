import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { execSql, escapeSqlIdent } from "../../lib/tauri";
import { formatValue } from "../../lib/formatValue";
import { Button, Select, Input, Label } from "../ui";

interface NearbyTabProps {
  schema: any;
}

export default function NearbyTab({ schema }: NearbyTabProps) {
  const { t } = useTranslation();
  const [table, setTable] = useState("");
  const [column, setColumn] = useState("");
  const [lat, setLat] = useState("39.9042");
  const [lng, setLng] = useState("116.4074");
  const [radius, setRadius] = useState("5000");
  const [topK, setTopK] = useState("10");
  const [rows, setRows] = useState<any[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);

  const geoTables = (schema?.tables || []).filter((tbl: any) =>
    tbl.columns.some((c: any) => c.type && c.type.toUpperCase().includes("GEO"))
  );

  useEffect(() => {
    if (geoTables.length > 0 && !table) {
      setTable(geoTables[0].name);
      const geoCol = geoTables[0].columns.find((c: any) => c.type && c.type.toUpperCase().includes("GEO"));
      if (geoCol) setColumn(geoCol.name);
    }
  }, [schema]);

  const onTableChange = (name: string) => {
    setTable(name);
    const tbl = geoTables.find((tb: any) => tb.name === name);
    if (tbl) {
      const geoCol = tbl.columns.find((c: any) => c.type && c.type.toUpperCase().includes("GEO"));
      if (geoCol) setColumn(geoCol.name);
    }
  };

  const doSearch = async () => {
    if (!table || !column || !lat || !lng) return;
    setLoading(true);
    setError(null);
    const t0 = performance.now();
    const r = parseFloat(radius) || 0;
    const k = parseInt(topK) || 10;
    const withFilter = r > 0;
    const tblIdent = escapeSqlIdent(table);
    const colIdent = escapeSqlIdent(column);
    const safeLat = parseFloat(lat) || 0;
    const safeLng = parseFloat(lng) || 0;
    let sql = `SELECT *, ST_DISTANCE(${colIdent}, GEOPOINT(${safeLat}, ${safeLng})) AS dist FROM ${tblIdent}`;
    if (withFilter) sql += ` WHERE ST_WITHIN(${colIdent}, ${safeLat}, ${safeLng}, ${r})`;
    sql += ` ORDER BY dist ASC LIMIT ${k}`;
    try {
      const data = await execSql(sql);
      setElapsed(Math.round(performance.now() - t0));
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setRows(null); }
      else setRows((data.data as any)?.rows || []);
    } catch (e) {
      setElapsed(Math.round(performance.now() - t0));
      setError(String(e));
      setRows(null);
    }
    setLoading(false);
  };

  return (
    <div>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 mb-4 items-end">
        <div>
          <Label>{t("geo.selectTable")}</Label>
          <Select value={table} onValueChange={onTableChange}
            options={geoTables.length === 0
              ? [{ value: "", label: t("geo.noGeoTables") }]
              : geoTables.map((tbl: any) => ({ value: tbl.name, label: tbl.name }))}
            className="w-full" />
        </div>
        <div>
          <Label>{t("geo.geoColumn")}</Label>
          <Input mono value={column} onChange={e => setColumn(e.target.value)} placeholder={t("geo.geoColumnPlaceholder")} className="w-full" />
        </div>
        <div>
          <Label>{t("geo.centerLat")}</Label>
          <Input mono value={lat} onChange={e => setLat(e.target.value)} placeholder="39.9042" className="w-full" />
        </div>
        <div>
          <Label>{t("geo.centerLng")}</Label>
          <Input mono value={lng} onChange={e => setLng(e.target.value)} placeholder="116.4074" className="w-full" />
        </div>
        <div>
          <Label>{t("geo.radiusM")}</Label>
          <Input type="number" value={radius} onChange={e => setRadius(e.target.value)} placeholder="5000" className="w-full" />
        </div>
        <div>
          <Label>{t("geo.topK")}</Label>
          <Input type="number" value={topK} onChange={e => setTopK(e.target.value)} placeholder="10" className="w-full" />
        </div>
      </div>
      <Button variant="primary" icon="search" onClick={doSearch} disabled={loading || !table} className="mb-4">
        {loading ? t("common.searching") : t("geo.nearbySearchBtn")}
      </Button>

      <div className="bg-surface border border-border-dark rounded-lg p-4 min-h-[200px] overflow-x-auto">
        {error && <div className="bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>}
        {rows === null && !error && (
          <div className="text-center py-10">
            <span className="material-symbols-outlined text-[36px] text-slate-600">location_on</span>
            <p className="text-slate-400 text-sm">{t("geo.selectHint")}</p>
            <p className="text-slate-500 text-xs mt-1">{t("geo.queryHint")}</p>
          </div>
        )}
        {rows && rows.length === 0 && <p className="text-slate-400 text-sm text-center py-10">{t("geo.noResults")}</p>}
        {rows && rows.length > 0 && (
          <div>
            <table className="w-full border-collapse text-sm font-mono">
              <thead>
                <tr>
                  <th className="bg-dark-700 px-2 py-2 text-center text-[10px] text-slate-600 border-b border-r border-border-dark sticky top-0 z-20 w-10">#</th>
                  {Array.from({ length: rows[0]?.length || 0 }, (_, i) => {
                    const isLast = i === (rows[0]?.length || 1) - 1;
                    return (
                      <th key={i} className={`bg-dark-700 px-3 py-2 text-left text-[11px] uppercase tracking-wider border-b border-border-dark sticky top-0 z-20 ${isLast ? "text-emerald-400" : "text-slate-400"}`}>
                        {isLast ? t("geo.distanceCol") : t("common.colN", { n: i + 1 })}
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, ri) => {
                  const cells = Array.isArray(row) ? row : Object.values(row);
                  return (
                    <tr key={ri} className="hover:bg-white/[0.02] transition-colors">
                      <td className="px-2 py-1.5 border-b border-r border-border-dark text-[10px] text-slate-600 text-center">{ri + 1}</td>
                      {cells.map((cell, ci) => {
                        const v = formatValue(cell);
                        const isLast = ci === cells.length - 1;
                        const isNull = v === "NULL";
                        return (
                          <td key={ci} className={`px-3 py-1.5 border-b border-border-dark max-w-[250px] truncate ${isLast ? "text-emerald-400 font-medium" : ""}`} title={v}>
                            {isNull ? <span className="text-slate-600 italic">NULL</span> :
                              isLast ? `${parseFloat(v).toFixed(1)}` : v}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <div className="flex items-center gap-3 mt-2 text-xs text-slate-400">
              <span>{t("geo.resultCount", { count: rows.length })}</span>
              {elapsed !== null && <span>{elapsed} ms</span>}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
