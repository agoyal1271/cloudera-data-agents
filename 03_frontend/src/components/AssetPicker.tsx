import { useEffect, useRef, useState } from 'react';
import { Search, Loader2 } from 'lucide-react';

/**
 * Shared, search-driven asset picker. Reusable across agents (Quality Guardian,
 * Source Scout, Metadata Curator…) so discovery lives in one place and scales to
 * thousands of tables — it never enumerates the catalog, it queries server-side
 * top-N via /api/catalog/search (semantic when indexed, name-match otherwise).
 *
 * - debounced search (≥2 chars)
 * - keyboard nav (↑/↓/Enter/Esc)
 * - paste-FQN escape hatch: type "db.table" + Enter to use it directly
 */

type Result = {
  name: string;
  type: string;
  namespace?: string;
  field_count?: number;
  similarity?: number | null;
};

interface Props {
  onPick: (name: string) => void;
  type?: string;
  disabled?: boolean;
  placeholder?: string;
}

export function AssetPicker({ onPick, type = 'iceberg_table', disabled, placeholder }: Props) {
  const [q, setQ] = useState('');
  const [results, setResults] = useState<Result[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [active, setActive] = useState(-1);
  const [source, setSource] = useState('');
  const boxRef = useRef<HTMLDivElement>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (timer.current) clearTimeout(timer.current);
    if (q.trim().length < 2) { setResults([]); setOpen(false); return; }
    timer.current = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/catalog/search?q=${encodeURIComponent(q.trim())}&type=${type}&limit=20`);
        const d = await res.json();
        setResults(d.results || []);
        setSource(d.source || '');
        setActive(-1);
        setOpen(true);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 250);
    return () => { if (timer.current) clearTimeout(timer.current); };
  }, [q, type]);

  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);

  const pick = (name: string) => { setQ(name); setOpen(false); onPick(name); };

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setOpen(true); setActive(a => Math.min(a + 1, results.length - 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(a => Math.max(a - 1, 0)); }
    else if (e.key === 'Enter') {
      e.preventDefault();
      if (active >= 0 && results[active]) pick(results[active].name);
      else if (q.includes('.')) pick(q.trim());   // paste-FQN escape hatch
    } else if (e.key === 'Escape') setOpen(false);
  };

  return (
    <div ref={boxRef} className="relative">
      <div className="flex items-center gap-2 px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md focus-within:border-agent-orange">
        <Search size={15} className="text-agent-text-secondary flex-shrink-0" />
        <input
          value={q}
          onChange={e => setQ(e.target.value)}
          onKeyDown={onKey}
          onFocus={() => results.length > 0 && setOpen(true)}
          disabled={disabled}
          placeholder={placeholder || 'Search tables, or paste db.table…'}
          className="flex-1 bg-transparent text-agent-text-primary placeholder-agent-text-secondary focus:outline-none disabled:opacity-50 text-sm"
        />
        {loading && <Loader2 size={14} className="animate-spin text-agent-text-secondary" />}
      </div>

      {open && (results.length > 0 || q.includes('.')) && (
        <div className="absolute z-20 mt-1 w-full max-h-72 overflow-y-auto bg-agent-dark-surface border border-agent-dark-border rounded-md shadow-lg">
          {results.length === 0 && q.includes('.') && (
            <button
              onClick={() => pick(q.trim())}
              className="w-full text-left px-3 py-2 text-xs text-agent-text-secondary hover:bg-agent-dark-border"
            >
              Use “<strong className="text-agent-text-primary">{q.trim()}</strong>” as-is
            </button>
          )}
          {results.map((r, i) => (
            <button
              key={r.name + i}
              onClick={() => pick(r.name)}
              onMouseEnter={() => setActive(i)}
              className={`w-full text-left px-3 py-2 flex items-center justify-between ${i === active ? 'bg-agent-dark-border' : 'hover:bg-agent-dark-border/60'}`}
            >
              <span className="text-sm text-agent-text-primary truncate">{r.name}</span>
              <span className="text-xs text-agent-text-secondary flex-shrink-0 ml-2">
                {r.field_count ? `${r.field_count} cols` : ''}
                {r.similarity != null ? ` · ${Math.round(r.similarity * 100)}%` : ''}
              </span>
            </button>
          ))}
          {results.length > 0 && (
            <div className="px-3 py-1 text-xs text-agent-text-secondary border-t border-agent-dark-border">
              {source === 'semantic' ? 'semantic match' : 'name match'} · top {results.length}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
