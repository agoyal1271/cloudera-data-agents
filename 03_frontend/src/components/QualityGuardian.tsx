import { useEffect, useRef, useState } from 'react';
import {
  Square, RotateCcw, AlertCircle, CheckCircle2, Clock,
  ShieldCheck, Sparkles, Database,
} from 'lucide-react';
import { AssetPicker } from './AssetPicker';

/**
 * Quality Guardian (v2) — profile-first, human-in-the-loop DQ agent UI.
 *
 * Drives the staged backend agent:
 *   POST /api/quality-guardian/profile  → basic checks + sample profile + "what to scan?"
 *   POST /api/quality-guardian/act      → bounded checks from the user's instruction
 *
 * Stays a conversation: profile once, then send instructions; a full-table scan
 * surfaces a Confirm button (the backend's needs_confirm gate).
 */

type Ev = { type: string; [k: string]: any };

async function streamSSE(
  url: string,
  body: any,
  onEvent: (ev: Ev) => void,
  onDone: () => void,
  onError: (e: Error) => void,
) {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const ev = JSON.parse(line.slice(6));
          if (ev.type === 'stream_end') onDone();
          else onEvent(ev);
        } catch { /* ignore partial */ }
      }
    }
    onDone();
  } catch (e) {
    onError(e instanceof Error ? e : new Error('stream failed'));
  }
}

const scoreColor = (s: number) =>
  s >= 90 ? 'text-green-400' : s >= 75 ? 'text-amber-400' : 'text-red-400';

function StatusPill({ status }: { status: string }) {
  const map: Record<string, string> = {
    pass: 'bg-green-500/15 text-green-400',
    warn: 'bg-amber-500/15 text-amber-400',
    fail: 'bg-red-500/15 text-red-400',
    error: 'bg-agent-dark-border text-agent-text-secondary',
  };
  return (
    <span className={`px-1.5 py-0.5 rounded text-xs font-bold uppercase ${map[status] || map.error}`}>
      {status}
    </span>
  );
}

function ScoreCard({ ev }: { ev: Ev }) {
  const score = ev.overall_score ?? 0;
  const c = ev.counts || {};
  return (
    <div className="bg-agent-dark-surface rounded-md p-4 mb-3 border border-agent-dark-border">
      <div className="flex items-center justify-between mb-2">
        <div className="text-xs font-semibold text-agent-text-secondary uppercase">
          {ev.exact ? '✓ Basic checks (exact, full table)' : `Results (${ev.scope})`}
          {ev.cached && <span className="ml-2 text-agent-teal normal-case">· cached</span>}
        </div>
        <div className={`text-2xl font-bold ${scoreColor(score)}`}>{score}</div>
      </div>
      <div className="flex gap-3 text-xs text-agent-text-secondary mb-3">
        {typeof ev.total_rows === 'number' && <span><Database size={11} className="inline mr-1" />{ev.total_rows.toLocaleString()} rows</span>}
        <span className="text-green-400">{c.pass ?? 0} pass</span>
        <span className="text-amber-400">{c.warn ?? 0} warn</span>
        <span className="text-red-400">{c.fail ?? 0} fail</span>
        {ev.driver && <span className="italic">· driver: {ev.driver}</span>}
      </div>
      <div className="space-y-1 max-h-56 overflow-y-auto">
        {(ev.checks || []).map((ch: any, i: number) => (
          <div key={i} className="flex items-center justify-between text-xs text-agent-text-secondary border-l border-agent-dark-border pl-2">
            <span><strong className="text-agent-text-primary">{ch.column}</strong> · {ch.check}</span>
            <span className="flex items-center gap-2">{ch.label} <StatusPill status={ch.status} /></span>
          </div>
        ))}
      </div>
    </div>
  );
}

function SampleProfile({ ev }: { ev: Ev }) {
  return (
    <div className="bg-agent-dark-surface rounded-md p-4 mb-3 border border-agent-teal/30">
      <div className="text-xs font-semibold text-agent-teal uppercase mb-2">
        Sample profile · {ev.sampled_rows?.toLocaleString?.() ?? ev.sampled_rows} rows · {ev.sample_rule}
        <span className="ml-2 text-xs text-agent-text-secondary normal-case">(estimated)</span>
      </div>
      <div className="space-y-1.5 max-h-72 overflow-y-auto">
        {(ev.columns || []).map((c: any, i: number) => (
          <div key={i} className="text-xs text-agent-text-secondary border-l border-agent-dark-border pl-2">
            <strong className="text-agent-text-primary">{c.column}</strong>
            <span className="ml-1 text-agent-text-secondary">: {c.type}</span>
            <span className="ml-2">null {c.null_rate != null ? `${(c.null_rate * 100).toFixed(1)}%` : '—'}</span>
            <span className="ml-2">distinct {c.distinct ?? '—'}</span>
            {c.min != null && <span className="ml-2">min {String(c.min)}</span>}
            {c.max != null && <span className="ml-2">max {String(c.max)}</span>}
            {c.negatives ? <span className="ml-2 text-amber-400">{c.negatives} neg</span> : null}
            {c.future_dates ? <span className="ml-2 text-amber-400">{c.future_dates} future</span> : null}
            {c.looks_like && Object.keys(c.looks_like).length > 0 && (
              <span className="ml-2">
                {Object.entries(c.looks_like).map(([k, v]: any) => (
                  <span key={k} className="inline-block px-1.5 py-0.5 rounded bg-agent-teal/15 text-agent-teal text-xs mr-1">
                    {k} {Math.round(v * 100)}%
                  </span>
                ))}
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function SchemaCard({ ev }: { ev: Ev }) {
  return (
    <div className="bg-agent-dark-surface rounded-md p-4 mb-3 border border-agent-dark-border">
      <div className="text-xs font-semibold text-agent-text-secondary uppercase mb-2">Schema · {ev.columns?.length} columns</div>
      <div className="flex flex-wrap gap-1.5">
        {(ev.columns || []).map((c: any, i: number) => (
          <span key={i} className="text-xs px-1.5 py-0.5 rounded bg-agent-dark-border text-agent-text-secondary">
            {c.name}<span className="text-agent-text-secondary/60"> : {c.type}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

function EventView({ ev, onConfirm, onRescan }: { ev: Ev; onConfirm: () => void; onRescan: () => void }) {
  switch (ev.type) {
    case 'skipped_unchanged':
      return (
        <div className="bg-agent-teal/10 rounded-md p-3 mb-3 border border-agent-teal/40">
          <div className="text-sm font-semibold text-agent-teal mb-1">⏭ Skipped — data unchanged</div>
          <p className="text-xs text-agent-text-secondary mb-2">{ev.message}</p>
          {ev.last_scanned && (
            <p className="text-xs text-agent-text-secondary mb-2">last scanned: {new Date(ev.last_scanned).toLocaleString()}</p>
          )}
          <button onClick={onRescan}
            className="px-3 py-1.5 rounded bg-agent-teal/20 text-agent-teal text-xs font-semibold hover:bg-agent-teal/30">
            Re-scan anyway
          </button>
        </div>
      );
    case 'schema': return <SchemaCard ev={ev} />;
    case 'basic_scorecard': return <ScoreCard ev={ev} />;
    case 'results': return <ScoreCard ev={ev} />;
    case 'sample_profile': return <SampleProfile ev={ev} />;
    case 'semantic_hints':
      return (
        <div className="bg-agent-dark-surface rounded-md p-3 mb-3 border-l-2 border-agent-orange">
          <div className="text-xs font-semibold text-agent-orange mb-2"><Sparkles size={12} className="inline mr-1" />Suggested checks (from evidence — confirm before binding)</div>
          {(ev.hints || []).map((h: any, i: number) => (
            <div key={i} className="text-xs text-agent-text-secondary border-l border-agent-dark-border pl-2 mb-1">
              <strong className="text-agent-text-primary">{h.column}</strong>: {(h.proposed_checks || []).map((p: any) => p.type + (p.pattern ? `(${p.pattern})` : '')).join(', ')}
            </div>
          ))}
        </div>
      );
    case 'thought':
      return <div className="text-sm text-agent-text-secondary italic mb-2"><Clock size={13} className="inline mr-1 text-agent-orange" />{ev.message}</div>;
    case 'proposed_checks':
      return (
        <div className="bg-agent-dark-border/40 rounded-md p-2 mb-2 text-xs font-mono text-agent-text-secondary">
          <div className="text-agent-orange mb-1">proposed ({ev.scope}):</div>
          {JSON.stringify(ev.raw)}
        </div>
      );
    case 'validation':
      return (
        <div className="text-xs mb-2 text-agent-text-secondary">
          {ev.ok ? <span className="text-green-400">✓ {ev.accepted} check(s) accepted ({ev.scope})</span>
                 : <span className="text-red-400">✗ no valid checks</span>}
          {ev.errors?.length > 0 && <div className="text-amber-400 mt-1">⚠ {ev.errors.join('; ')}</div>}
        </div>
      );
    case 'confirm_required':
      return (
        <div className="bg-amber-500/10 rounded-md p-3 mb-2 border border-amber-500/40">
          <div className="text-xs font-semibold text-amber-400 mb-2">⚠ Full-table scan requested</div>
          <p className="text-xs text-agent-text-secondary mb-2">{ev.message}</p>
          <button onClick={onConfirm} className="px-3 py-1.5 rounded bg-amber-500/20 text-amber-300 text-xs font-semibold hover:bg-amber-500/30">
            Confirm full scan
          </button>
        </div>
      );
    case 'executing':
      return <div className="text-sm text-agent-text-secondary mb-2"><Clock size={13} className="inline mr-1 text-agent-orange animate-spin" />Running {ev.check_count} check(s) on {ev.scope}…</div>;
    case 'step':
      return (
        <div className="flex gap-2 text-sm mb-1">
          {ev.status === 'running' ? <Clock size={14} className="text-agent-orange animate-spin mt-0.5" /> : <CheckCircle2 size={14} className="text-green-400 mt-0.5" />}
          <span className="text-agent-text-primary">{ev.name}<span className="text-agent-text-secondary ml-2">{ev.status === 'running' ? 'running…' : 'complete'}</span></span>
        </div>
      );
    case 'question':
      return (
        <div className="bg-purple-500/10 rounded-md p-3 mb-2 border-l-2 border-purple-400">
          <div className="text-sm font-semibold text-purple-300 mb-1">❓ {ev.message}</div>
        </div>
      );
    case 'complete':
      return <div className="flex gap-2 text-sm mb-2"><CheckCircle2 size={14} className="text-green-400 mt-0.5" /><span className="text-green-400 font-semibold">{ev.summary}</span></div>;
    case 'error':
      return <div className="flex gap-2 text-sm mb-2"><AlertCircle size={14} className="text-red-400 mt-0.5" /><span className="text-red-300">{ev.message}</span></div>;
    case 'started':
      return null;
    default:
      return null;
  }
}

export function QualityGuardian() {
  const [asset, setAsset] = useState('');
  const [events, setEvents] = useState<Ev[]>([]);
  const [profile, setProfile] = useState<any>(null);
  const [profiled, setProfiled] = useState(false);
  const [running, setRunning] = useState(false);
  const [instruction, setInstruction] = useState('');
  const [lastAction, setLastAction] = useState('');
  const [error, setError] = useState<string | null>(null);
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [events]);

  const push = (ev: Ev) => {
    if (ev.type === 'question') { setProfile(ev.profile); setProfiled(true); }
    setEvents(prev => [...prev, ev]);
  };

  const startProfile = (name: string, force = false) => {
    const a = name.trim();
    if (!a) return;
    setAsset(a);
    setError(null); setEvents([]); setProfile(null); setProfiled(false); setRunning(true);
    streamSSE('/api/quality-guardian/profile', { asset: a, force },
      push, () => setRunning(false), e => { setError(e.message); setRunning(false); });
  };

  const runAct = (action: string, confirm = false) => {
    if (!action.trim()) return;
    setError(null); setRunning(true); setLastAction(action);
    setEvents(prev => [...prev, { type: 'user_msg', text: action } as Ev]);
    streamSSE('/api/quality-guardian/act',
      { asset: asset.trim(), user_action: action, profile, confirm },
      push, () => setRunning(false), e => { setError(e.message); setRunning(false); });
    setInstruction('');
  };

  const reset = () => { setEvents([]); setProfile(null); setProfiled(false); setError(null); setInstruction(''); };

  return (
    <div className="flex flex-col h-full bg-agent-dark-bg">
      {/* Header */}
      <div className="border-b border-agent-dark-border px-8 py-6">
        <div className="flex items-center gap-3 mb-2">
          <ShieldCheck size={22} className="text-agent-orange" />
          <h1 className="text-xl font-bold text-agent-text-primary">Quality Guardian</h1>
          <span className="px-2 py-0.5 rounded bg-agent-dark-border text-xs text-agent-orange font-semibold">REAL-TIME QA</span>
        </div>
        <p className="text-sm text-agent-text-secondary">
          Profile-first quality on any table: exact basic checks, a 1% sample profile, then you choose what to scan. Bounded, read-only, human-in-the-loop.
        </p>
      </div>

      {/* Asset picker — search-driven, scales to thousands of tables */}
      <div className="flex-shrink-0 border-b border-agent-dark-border px-8 py-5 bg-agent-dark-surface">
        <label className="block text-xs font-semibold text-agent-text-secondary uppercase mb-2">Table</label>
        <div className="flex gap-2 items-start">
          <div className="flex-1">
            <AssetPicker type="iceberg_table" disabled={running} onPick={startProfile}
              placeholder="Search tables, or paste db.table…" />
            {asset && (
              <div className="text-xs text-agent-text-secondary mt-1">
                selected: <strong className="text-agent-text-primary">{asset}</strong>
              </div>
            )}
          </div>
          {running && (
            <button onClick={() => setRunning(false)}
              className="px-4 py-2 rounded-md bg-red-500/20 text-red-400 font-semibold text-sm flex items-center gap-2 hover:bg-red-500/30">
              <Square size={16} />Stop
            </button>
          )}
          <button onClick={reset} disabled={running}
            className="px-3 py-2 rounded-md border border-agent-dark-border text-agent-text-secondary hover:bg-agent-dark-border disabled:opacity-50">
            <RotateCcw size={16} />
          </button>
        </div>
      </div>

      {error && (
        <div className="flex-shrink-0 mx-8 mt-4 p-3 bg-red-500/10 border border-red-500/30 rounded-md flex items-start gap-3">
          <AlertCircle size={16} className="text-red-400 mt-0.5" /><span className="text-sm text-red-300">{error}</span>
        </div>
      )}

      {/* Stream */}
      <div className="flex-1 overflow-y-auto px-8 py-6">
        {events.length === 0 && !running && (
          <div className="flex flex-col items-center justify-center h-full text-agent-text-secondary">
            <ShieldCheck size={40} className="mb-3 opacity-40" />
            <p className="text-sm">Pick a table and click Profile to start.</p>
          </div>
        )}

        {events.map((ev, i) =>
          ev.type === 'user_msg' ? (
            <div key={i} className="flex justify-end mb-2">
              <span className="px-3 py-1.5 rounded-md bg-agent-orange/20 text-agent-text-primary text-sm max-w-[80%]">{ev.text}</span>
            </div>
          ) : (
            <div key={i}><EventView ev={ev} onConfirm={() => runAct(lastAction, true)} onRescan={() => startProfile(asset, true)} /></div>
          )
        )}

        {running && (
          <div className="flex items-center gap-2 text-agent-text-secondary animate-pulse">
            <div className="w-2 h-2 bg-agent-orange rounded-full animate-bounce" /><span className="text-sm">Processing…</span>
          </div>
        )}
        <div ref={endRef} />
      </div>

      {/* Follow-up instruction (after profiling) */}
      {profiled && (
        <div className="flex-shrink-0 border-t border-agent-dark-border px-8 py-4 bg-agent-dark-surface">
          <label className="block text-xs font-semibold text-agent-text-secondary uppercase mb-2">What should I scan?</label>
          <div className="flex gap-2">
            <input
              value={instruction} onChange={e => setInstruction(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && !running && runAct(instruction)}
              disabled={running}
              placeholder='e.g. "validate email format and check risk_score is 0–100"'
              className="flex-1 px-3 py-2 bg-agent-dark-bg border border-agent-dark-border rounded-md text-sm text-agent-text-primary placeholder-agent-text-secondary focus:outline-none focus:border-agent-orange disabled:opacity-50"
            />
            <button onClick={() => runAct(instruction)} disabled={running || !instruction.trim()}
              className="px-4 py-2 rounded-md bg-agent-orange text-white font-semibold text-sm hover:bg-agent-orange/90 disabled:opacity-50">
              Run
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
