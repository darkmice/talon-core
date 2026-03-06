import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { EmptyState } from "./index";

interface DataTableProps<T> {
  columns: ColumnDef<T, any>[];
  data: T[];
  loading?: boolean;
  emptyText?: string;
  emptyIcon?: string;
  onRowClick?: (row: T) => void;
  selectedRowId?: string | null;
  getRowId?: (row: T) => string;
  globalFilter?: string;
  compact?: boolean;
  className?: string;
}

export default function DataTable<T>({
  columns,
  data,
  loading,
  emptyText,
  emptyIcon,
  onRowClick,
  selectedRowId,
  getRowId,
  globalFilter,
  compact,
  className = "",
}: DataTableProps<T>) {
  const { t } = useTranslation();
  const [sorting, setSorting] = useState<SortingState>([]);

  const table = useReactTable({
    data,
    columns,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getRowId: getRowId ? (row) => getRowId(row) : undefined,
  });

  const py = compact ? "py-1.5" : "py-2.5";
  const textSize = compact ? "text-xs" : "text-sm";

  return (
    <div className={`flex-1 overflow-auto ${className}`}>
      {loading && (
        <div className="flex items-center justify-center py-16">
          <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
          <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
        </div>
      )}

      {!loading && data.length === 0 && (
        <EmptyState icon={emptyIcon || "table_chart"} title={emptyText || t("common.noData")} />
      )}

      {!loading && data.length > 0 && (
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10">
            {table.getHeaderGroups().map(hg => (
              <tr key={hg.id}>
                {hg.headers.map(header => {
                  const canSort = header.column.getCanSort();
                  const sorted = header.column.getIsSorted();
                  return (
                    <th
                      key={header.id}
                      onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                      className={`bg-dark-700/80 backdrop-blur-sm px-4 ${py} text-left text-[11px] uppercase tracking-wider font-semibold border-b border-border-dark
                        ${canSort ? "cursor-pointer select-none hover:text-slate-200" : ""}
                        ${sorted ? "text-primary" : "text-slate-400"}`}
                      style={{ width: header.getSize() !== 150 ? header.getSize() : undefined }}
                    >
                      <div className="flex items-center gap-1">
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {sorted === "asc" && <span className="material-symbols-outlined text-[12px]">arrow_upward</span>}
                        {sorted === "desc" && <span className="material-symbols-outlined text-[12px]">arrow_downward</span>}
                      </div>
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody className={`${textSize} font-mono text-slate-300`}>
            {table.getRowModel().rows.map((row, ri) => {
              const isSelected = selectedRowId != null && row.id === selectedRowId;
              const stripe = ri % 2 === 1 ? "bg-white/[0.01]" : "";
              return (
                <tr
                  key={row.id}
                  onClick={() => onRowClick?.(row.original)}
                  className={`hover:bg-white/[0.03] transition-colors group
                    ${onRowClick ? "cursor-pointer" : ""}
                    ${isSelected ? "bg-primary/5" : stripe}`}
                >
                  {row.getVisibleCells().map(cell => (
                    <td key={cell.id} className={`px-4 ${py} border-b border-border-dark`}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

export type { DataTableProps };
export { type ColumnDef } from "@tanstack/react-table";
