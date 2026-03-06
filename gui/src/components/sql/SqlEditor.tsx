import { useRef, useMemo, useCallback, type RefObject } from "react";
import { useTranslation } from "react-i18next";
import SqlAutocomplete from "../SqlAutocomplete";
import { highlightSql } from "./sqlUtils";

interface SqlEditorProps {
  sql: string;
  setSql: (v: string) => void;
  schema: any;
  onRun: () => void;
  onFormat?: () => void;
  onCursorChange?: (pos: { ln: number; col: number }) => void;
  textareaRef?: RefObject<HTMLTextAreaElement | null>;
}

export default function SqlEditor({ sql, setSql, schema, onRun, onFormat, onCursorChange, textareaRef: externalRef }: SqlEditorProps) {
  const { t } = useTranslation();
  const internalRef = useRef<HTMLTextAreaElement>(null);
  const textareaRef = externalRef || internalRef;
  const preRef = useRef<HTMLPreElement>(null);
  const lineNumRef = useRef<HTMLDivElement>(null);
  const highlightedHtml = useMemo(() => highlightSql(sql) + "\n", [sql]);
  const lines = sql.split("\n");

  const syncScroll = useCallback(() => {
    const ta = textareaRef.current;
    const pre = preRef.current;
    const ln = lineNumRef.current;
    if (ta && pre) {
      pre.scrollTop = ta.scrollTop;
      pre.scrollLeft = ta.scrollLeft;
    }
    if (ta && ln) {
      ln.scrollTop = ta.scrollTop;
    }
  }, []);

  const updateCursor = useCallback(() => {
    const ta = textareaRef.current;
    if (!ta || !onCursorChange) return;
    const pos = ta.selectionStart;
    const before = sql.slice(0, pos);
    const ln = before.split("\n").length;
    const col = pos - before.lastIndexOf("\n");
    onCursorChange({ ln, col });
  }, [sql, onCursorChange]);

  return (
    <div className="flex flex-1 overflow-hidden">
      {/* Line numbers */}
      <div ref={lineNumRef} className="w-10 shrink-0 bg-dark-900 pt-3 text-right pr-2 select-none overflow-y-hidden">
        {lines.map((_, i) => (
          <div key={i}
            onMouseDown={(e) => {
              e.preventDefault();
              const ta = textareaRef.current;
              if (!ta) return;
              const linesBefore = lines.slice(0, i);
              const start = linesBefore.reduce((sum, l) => sum + l.length + 1, 0);
              const end = start + lines[i].length;
              ta.focus();
              ta.setSelectionRange(start, end);
              setTimeout(updateCursor, 0);
            }}
            className="text-[12px] leading-[22px] text-slate-700 font-mono cursor-pointer hover:text-slate-400 transition-colors">{i + 1}</div>
        ))}
      </div>
      {/* Textarea with syntax highlight overlay */}
      <div className="flex-1 relative overflow-hidden bg-dark-800">
        <pre
          ref={preRef}
          aria-hidden
          className="absolute inset-0 px-3 py-3 font-mono text-[13px] leading-[22px] whitespace-pre-wrap break-words pointer-events-none overflow-auto no-scrollbar m-0"
          dangerouslySetInnerHTML={{ __html: highlightedHtml }}
        />
        {!sql && (
          <div className="absolute inset-0 px-3 py-3 font-mono text-[13px] leading-[22px] text-slate-600 pointer-events-none z-[5] whitespace-pre-wrap">
            {t("sql.placeholder")}
          </div>
        )}
        <textarea
          ref={textareaRef}
          value={sql}
          onChange={e => { setSql(e.target.value); setTimeout(updateCursor, 0); }}
          onScroll={syncScroll}
          onKeyDown={e => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); onRun(); }
            if ((e.ctrlKey || e.metaKey) && e.shiftKey && (e.key === "f" || e.key === "F")) { e.preventDefault(); onFormat?.(); }
            if ((e.ctrlKey || e.metaKey) && !e.shiftKey && (e.key === "d" || e.key === "D")) {
              e.preventDefault();
              const ta = textareaRef.current;
              if (ta) {
                const pos = ta.selectionStart;
                const curLines = sql.split("\n");
                const before = sql.slice(0, pos);
                const lineIdx = before.split("\n").length - 1;
                curLines.splice(lineIdx, 1);
                const newSql = curLines.join("\n");
                setSql(newSql);
                const newPos = Math.min(pos, newSql.length);
                setTimeout(() => { ta.selectionStart = ta.selectionEnd = newPos; }, 0);
              }
            }
            if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key === "/") {
              e.preventDefault();
              const ta = textareaRef.current;
              if (ta) {
                const start = ta.selectionStart;
                const end = ta.selectionEnd;
                const curLines = sql.split("\n");
                const beforeStart = sql.slice(0, start);
                const beforeEnd = sql.slice(0, end);
                const startLine = beforeStart.split("\n").length - 1;
                const endLine = beforeEnd.split("\n").length - 1;
                const allCommented = curLines.slice(startLine, endLine + 1).every(l => l.trimStart().startsWith("--"));
                for (let li = startLine; li <= endLine; li++) {
                  if (allCommented) {
                    curLines[li] = curLines[li].replace(/^(\s*)-- ?/, "$1");
                  } else {
                    curLines[li] = "-- " + curLines[li];
                  }
                }
                const newSql = curLines.join("\n");
                setSql(newSql);
                setTimeout(() => { ta.selectionStart = 0; ta.selectionEnd = newSql.length; updateCursor(); }, 0);
              }
            }
            if (e.key === "Tab" && !e.ctrlKey && !e.metaKey && !e.defaultPrevented) {
              e.preventDefault();
              const ta = textareaRef.current;
              if (ta) {
                const start = ta.selectionStart;
                const end = ta.selectionEnd;
                const newSql = sql.slice(0, start) + "  " + sql.slice(end);
                setSql(newSql);
                setTimeout(() => { ta.selectionStart = ta.selectionEnd = start + 2; }, 0);
              }
            }
          }}
          onKeyUp={updateCursor}
          onClick={updateCursor}
          onSelect={updateCursor}
          className="w-full h-full bg-transparent text-transparent caret-slate-200 px-3 py-3 font-mono text-[13px] leading-[22px] outline-none resize-none relative z-10 sql-editor"
          spellCheck={false}
        />
        <SqlAutocomplete textareaRef={textareaRef} sql={sql} setSql={setSql} schema={schema} />
      </div>
    </div>
  );
}
