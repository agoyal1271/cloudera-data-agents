/**
 * Orchestrator — multi-agent mission control.
 *
 * Adopts the AgenticGenUI pattern: every SSE event from the backend is mapped
 * to a rich inline UI component via the registry (ui-registry.tsx) and rendered
 * directly in the conversation, instead of being summarised as plain text.
 *
 * Layout:
 *   Header  — agent ribbon (live status)
 *   Body    — conversation canvas (full width)
 *               left  : messages + inline UI components
 *               right : dataset inspector (profile cards, clickable)
 *   Composer — sticky input bar
 */

import { type ReactNode, useEffect, useRef, useState } from 'react';
import {
  Radar, Shield, Wrench, Activity, Workflow, ArrowRight, ArrowUp, Square,
  Loader2, Sparkles, MessagesSquare, Check, PenSquare,
} from 'lucide-react';
import { streamSupervisor, type SupEvent } from '../../api/supervisor';
import { renderUIBlock, type UIBlock, AssetGrid, QualityCard, SchemaView, PipelineView, HealthView } from './ui-registry';

// ── Types ─────────────────────────────────────────────────────────────────────

type AgentKey = 'scout' | 'guardian' | 'pipeline' | 'heal' | 'analyst';
type Status = 'idle' | 'queued' | 'running' | 'done' | 'skipped';

const AGENTS: { key: AgentKey; name: string; short: string; Icon: typeof Radar }[] = [
  { key: 'scout', name: 'Source Scout', short: 'Scout', Icon: Radar },
  { key: 'guardian', name: 'Quality Guardian', short: 'Guardian', Icon: Shield },
  { key: 'pipeline', name: 'Pipeline Builder', short: 'Builder', Icon: Wrench },
  { key: 'heal', name: 'Pipeline Healer', short: 'Healer', Icon: Activity },
  { key: 'analyst', name: 'Data Analyst', short: 'Analyst', Icon: MessagesSquare },
];
const NAME_TO_KEY: Record<string, AgentKey> = {
  'Source Scout': 'scout', 'Quality Guardian': 'guardian', 'Pipeline Builder': 'pipeline',
  'Pipeline Healer': 'heal', 'Data Analyst': 'analyst',
};
const IDLE: Record<AgentKey, Status> = { scout: 'idle', guardian: 'idle', pipeline: 'idle', heal: 'idle', analyst: 'idle' };

const STEP_LABEL: Record<string, string> = {
  discover: 'searching the catalog…', resolve_schema: 'reading the schema…',
  generate_sql: 'writing SQL…', run_sql: 'running on Impala…',
  basic_checks: 'running quality checks…', sample_profile: 'profiling the data…',
  lineage: 'tracing lineage…', build_nifi_flow: 'building the pipeline…',
  check_health: 'checking pipeline health…',
};

const SUGGESTIONS = [
  'onboard demo.payments end-to-end',
  'find payment data and check its quality',
  'what is the average amount in demo.payments',
];

const PAGE_BG = 'radial-gradient(140% 100% at 50% -30%, rgba(255,91,0,0.06), transparent 45%), radial-gradient(120% 80% at 100% 0%, rgba(0,163,196,0.04), transparent 42%), #0E131B';

// ── Message model ─────────────────────────────────────────────────────────────

type AssetItem = { name: string; asset_type: string; field_count: number; columns?: { name: string; type?: string }[] };

type Msg =
  | { id: number; kind: 'user'; text: string }
  | { id: number; kind: 'progress'; text: string; done?: boolean }
  | { id: number; kind: 'ui'; block: UIBlock };

// Blackboard: what the inspector panel renders
type Blackboard = {
  asset?: string; assetType?: string;
  schemaCols?: number; schemaReused?: boolean; schemaColumns?: { name: string; type?: string }[];
  quality?: number; qualityChecks?: unknown[]; qualityCounts?: Record<string, number>; qualityRows?: number;
  flow?: string; flowSummary?: { processor_count?: number; parameter_count?: number; processors?: string[]; controller_services?: string[]; parameters_to_fill?: { name: string; sensitive?: boolean }[] };
  health?: string; healthMetrics?: Record<string, unknown>;
};

// ── Main component ─────────────────────────────────────────────────────────────

export function Orchestrator() {
  const [input, setInput] = useState('');
  const [running, setRunning] = useState(false);
  const [intent, setIntent] = useState('');
  const [status, setStatus] = useState<Record<AgentKey, Status>>(IDLE);
  const [active, setActive] = useState<AgentKey | null>(null);
  const [bb, setBb] = useState<Blackboard>({});
  const [discovered, setDiscovered] = useState<AssetItem[]>([]);
  const [focus, setFocus] = useState('');
  const [chat, setChat] = useState<Msg[]>([]);
  // One durable conversation thread. Persisted so a page refresh keeps the session;
  // "New session" mints a fresh id and clears the canvas.
  const [sessionId, setSessionId] = useState<string>(() => {
    const k = 'orchestrator_session_id';
    let id = localStorage.getItem(k);
    if (!id) { id = (crypto.randomUUID?.() ?? `s-${Date.now()}-${Math.random().toString(36).slice(2)}`); localStorage.setItem(k, id); }
    return id;
  });
  const cancelRef = useRef<null | (() => void)>(null);
  const idRef = useRef(0);
  const endRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const nextId = () => (idRef.current += 1);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [chat]);

  // Push a full-replacement progress bubble or turn it into a done state.
  const lastProgressIdx = (c: Msg[]) => { for (let i = c.length - 1; i >= 0; i--) if (c[i].kind === 'progress') return i; return -1; };

  const setProgress = (text: string) =>
    setChat((c) => {
      const i = lastProgressIdx(c);
      if (i >= 0) { const n = [...c]; (n[i] as Extract<Msg, { kind: 'progress' }>).text = text; return n; }
      return [...c, { id: nextId(), kind: 'progress', text } as Msg];
    });

  const resolveProgress = (block?: UIBlock) =>
    setChat((c) => {
      const idx = lastProgressIdx(c);
      const without = idx >= 0 ? c.filter((_, i) => i !== idx) : c;
      return block ? [...without, { id: nextId(), kind: 'ui', block } as Msg] : without;
    });

  const pushUI = (block: UIBlock) =>
    setChat((c) => [...c, { id: nextId(), kind: 'ui', block }]);

  // Focus a discovered asset → fill the inspector + jump to input.
  const focusOn = (name: string) => {
    const a = discovered.find((x) => x.name === name);
    setFocus(name);
    setBb({ asset: name, assetType: a?.asset_type, schemaCols: a?.field_count, schemaColumns: a?.columns });
    setTimeout(() => inputRef.current?.focus(), 0);
  };

  const send = (text: string, ctxAsset?: string) => {
    const m = text.trim();
    if (!m || running) return;
    cancelRef.current?.();
    setInput('');
    setRunning(true);
    setStatus(IDLE);
    setActive(null);
    setChat((c) => [...c,
      { id: nextId(), kind: 'user', text: m },
      { id: nextId(), kind: 'progress', text: 'the team is working…' },
    ]);
    cancelRef.current = streamSupervisor(
      m, handleEvent,
      () => { setRunning(false); setStatus((s) => allDone(s)); setActive(null); resolveProgress(); },
      (err) => { setRunning(false); resolveProgress({ kind: 'answer', text: `⚠ ${err.message}`, grounded: false }); },
      ctxAsset ?? focus ?? undefined,
      sessionId,
    );
  };

  // Mint a fresh thread and clear the canvas — the backend memory is keyed by this id.
  const newSession = () => {
    cancelRef.current?.();
    const id = crypto.randomUUID?.() ?? `s-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    localStorage.setItem('orchestrator_session_id', id);
    setSessionId(id);
    setChat([]); setDiscovered([]); setFocus(''); setBb({}); setIntent('');
    setStatus(IDLE); setActive(null); setRunning(false);
  };

  const stop = () => {
    cancelRef.current?.();
    setRunning(false);
    setStatus((s) => { const o = { ...s }; (Object.keys(o) as AgentKey[]).forEach((k) => { if (o[k] === 'running' || o[k] === 'queued') o[k] = 'idle'; }); return o; });
    setActive(null);
    resolveProgress();
  };

  const handleEvent = (e: SupEvent) => {
    switch (e.type) {
      // ── Routing ──────────────────────────────────────────────────────────────
      case 'plan': {
        if (!Array.isArray(e.plan)) break;
        setIntent(e.intent || '');
        // Lineage runs under the Source Scout pill (it's a discovery capability).
        const plan: string[] = (e.plan || []).map((p: string) => (p === 'lineage' ? 'scout' : p));
        const skip: string[] = (e.skipped || []).filter((s: string) => s !== 'lineage');
        setStatus(() => { const o: Record<AgentKey, Status> = { ...IDLE }; AGENTS.forEach(({ key }) => { if (plan.includes(key)) o[key] = 'queued'; else if (skip.includes(key)) o[key] = 'skipped'; }); return o; });
        break;
      }
      case 'handoff': {
        const to = (e.to === 'lineage' ? 'scout' : e.to) as AgentKey;
        setStatus((s) => { const o = { ...s }; (Object.keys(o) as AgentKey[]).forEach((k) => { if (o[k] === 'running') o[k] = 'done'; }); if (to in o) o[to] = 'running'; return o; });
        setActive((['scout', 'guardian', 'pipeline', 'heal', 'analyst'] as string[]).includes(to) ? to as AgentKey : null);
        break;
      }
      case 'agent_skipped': {
        const k = NAME_TO_KEY[e.agent_name]; if (k) setStatus((s) => ({ ...s, [k]: 'skipped' }));
        break;
      }
      case 'agent_start':
        if (e.asset) setFocus(e.asset);
        if (e.agent_name) setProgress(`${e.agent_name} working…`);
        break;
      case 'step':
        if (e.status === 'running') setProgress(STEP_LABEL[e.name] || `${e.name}…`);
        break;

      // ── Discovery ─────────────────────────────────────────────────────────
      case 'assets': {
        const list = (e.assets || []) as AssetItem[];
        setDiscovered(list);
        setBb((b) => ({ ...b, discovered: list.length }));
        // Resolve the "working…" spinner into the assets component.
        resolveProgress({ kind: 'assets', assets: list, focus });
        break;
      }
      // ── Lineage ───────────────────────────────────────────────────────────
      case 'lineage': {
        if (e.asset) setFocus(e.asset);
        resolveProgress({
          kind: 'lineage',
          asset: e.asset,
          upstream: e.upstream || [],
          downstream: e.downstream || [],
          graph: e.graph || { nodes: [], edges: [] },
          edgeCount: e.edge_count || 0,
        });
        break;
      }

      case 'blackboard':
        if ((e.wrote || []).includes('asset')) {
          setFocus(e.asset);
          setBb((b) => ({ ...b, asset: e.asset, assetType: e.asset_type, schemaCols: e.field_count, schemaColumns: e.columns }));
        }
        if ((e.wrote || []).includes('pipeline')) setBb((b) => ({ ...b, flow: e.flow_name }));
        break;

      // ── Schema ────────────────────────────────────────────────────────────
      case 'schema': {
        const cols = e.columns || [];
        setBb((b) => ({ ...b, schemaCols: cols.length || b.schemaCols, schemaReused: !!e.reused, schemaColumns: cols.length ? cols : b.schemaColumns }));
        // Schema is contextual, not a standalone chat event — inspector shows it.
        break;
      }
      case 'context':
        if (e.asset) setFocus(e.asset);
        if (e.field_count != null) setBb((b) => ({ ...b, asset: e.asset, schemaCols: e.field_count, schemaReused: !!e.reused, schemaColumns: e.columns || b.schemaColumns }));
        break;

      // ── Quality ───────────────────────────────────────────────────────────
      case 'om_context':
        // Compact classification strip shown inline — rendered before the scorecard.
        pushUI({ kind: 'om_context', asset: e.asset, tags: e.tags, owner: e.owner, tier: e.tier });
        break;
      case 'basic_scorecard':
        setBb((b) => ({ ...b, quality: e.overall_score, qualityChecks: e.checks, qualityCounts: e.counts, qualityRows: e.total_rows }));
        // This IS a primary result — render it inline (with any OM fields the guardian attached).
        resolveProgress({
          kind: 'quality', asset: focus || e.asset || '',
          overall_score: e.overall_score, counts: e.counts, checks: e.checks || [], total_rows: e.total_rows,
          om_owner: e.om_owner, om_tier: e.om_tier, om_tags: e.om_tags,
        });
        break;
      case 'quality':
        if (e.overall_score != null) setBb((b) => ({ ...b, quality: e.overall_score }));
        break;

      // ── Pipeline ──────────────────────────────────────────────────────────
      case 'flow_generated':
        setBb((b) => ({ ...b, flow: e.flow_name, flowSummary: e.summary }));
        pushUI({ kind: 'pipeline', asset: focus || '', summary: e.summary });
        break;

      // ── Health ────────────────────────────────────────────────────────────
      case 'health_check':
        setBb((b) => ({ ...b, health: e.state, healthMetrics: e.metrics }));
        pushUI({ kind: 'health', pipeline: bb.flow || focus || 'pipeline', state: e.state, metrics: e.metrics });
        break;

      // ── Analyst ───────────────────────────────────────────────────────────
      case 'sql_result':
        if (e.error) setProgress(`SQL error — ${String(e.error).slice(0, 60)}`);
        else setProgress('summarizing the result…');
        break;
      case 'answer':
        resolveProgress({ kind: 'answer', asset: e.asset, text: e.text, sql: e.sql, grounded: e.grounded });
        break;

      // ── Completion ────────────────────────────────────────────────────────
      case 'complete':
        if (e.agent === 'supervisor') {
          setStatus((s) => allDone(s)); setActive(null);
          // Only show a text summary for multi-agent chains where no primary
          // result card was emitted (e.g. a plain discover with no quality run).
          resolveProgress();
        }
        break;
      case 'text':
        // Conversational responses (smalltalk, errors, guidance).
        resolveProgress({ kind: 'answer', text: e.text || '' });
        break;
      case 'error':
        resolveProgress({ kind: 'answer', text: `⚠ ${e.message || 'error'}` });
        break;
    }
  };

  return (
    <div className="h-full flex flex-col text-agent-text-primary" style={{ background: PAGE_BG }}>
      <style>{KEYFRAMES}</style>

      {/* ── Header: title + agent ribbon ── */}
      <header className="px-7 pt-5 pb-4 border-b border-white/[0.06]">
        <div className="flex items-center gap-3 mb-3.5">
          <div className="w-10 h-10 rounded-2xl bg-agent-orange/15 border border-agent-orange/25 flex items-center justify-center shadow-[0_6px_18px_-6px_rgba(255,91,0,0.5)]">
            <Workflow size={20} className="text-agent-orange" />
          </div>
          <div className="flex-1 min-w-0">
            <h1 className="text-lg font-semibold tracking-tight leading-tight">Orchestrator</h1>
            <p className="text-sm text-agent-text-secondary leading-tight">Your data team — one conversation, the right agents.</p>
          </div>
          {intent && <span className="text-xs px-3 py-1.5 rounded-full bg-agent-teal/10 border border-agent-teal/25 text-agent-teal shrink-0">intent · {intent}</span>}
          <button onClick={newSession} title="Start a new session (clears memory)"
            className="text-xs flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/[0.04] border border-white/[0.1] text-agent-text-secondary hover:text-agent-text-primary hover:border-white/25 transition-all shrink-0">
            <PenSquare size={13} /> New session
          </button>
        </div>
        {/* Agent ribbon */}
        <div className="flex items-center gap-1 flex-wrap">
          {AGENTS.map((a, i) => (
            <div key={a.key} className="flex items-center">
              <AgentPill agent={a} status={status[a.key]} isActive={active === a.key} />
              {i < AGENTS.length - 1 && <ArrowRight size={12} className={`mx-0.5 ${status[a.key] === 'done' ? 'text-agent-teal/60' : 'text-white/[0.12]'}`} />}
            </div>
          ))}
        </div>
      </header>

      {/* ── Body: conversation + inspector ── */}
      <div className="flex-1 min-h-0 grid grid-cols-1 lg:grid-cols-[1fr,360px]">

        {/* Conversation canvas */}
        <main className="flex flex-col min-h-0">
          <div className="flex-1 overflow-y-auto px-7 py-6">
            {chat.length === 0 ? (
              <Welcome onPick={send} />
            ) : (
              <div className="max-w-3xl mx-auto space-y-4">
                {chat.map((m) => <MsgView key={m.id} m={m} onPick={focusOn} focus={focus} />)}
                <div ref={endRef} />
              </div>
            )}
          </div>

          {/* Composer */}
          <div className="px-7 py-4 border-t border-white/[0.06]">
            <div className="max-w-3xl mx-auto flex items-end gap-2.5">
              <div className="flex-1 relative">
                <Sparkles size={17} className="absolute left-4 top-1/2 -translate-y-1/2 text-agent-text-secondary" />
                <input
                  ref={inputRef} value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && send(input)}
                  placeholder={focus ? `Ask about ${focus}, or tell the team what to do…` : 'Ask the team… e.g. "onboard demo.payments end-to-end"'}
                  className="w-full bg-agent-dark-surface/70 border border-white/[0.08] rounded-2xl pl-11 pr-4 py-3.5 text-sm placeholder:text-agent-text-secondary/75 focus:outline-none focus:border-agent-orange/60 focus:ring-2 focus:ring-agent-orange/15 transition-all"
                />
              </div>
              {running
                ? <button onClick={stop} className="w-12 h-12 rounded-2xl bg-agent-dark-surface/80 border border-white/[0.08] hover:border-rose-500/50 flex items-center justify-center transition-colors"><Square size={16} className="text-rose-400" /></button>
                : <button onClick={() => send(input)} disabled={!input.trim()} className="w-12 h-12 rounded-2xl bg-agent-orange text-white flex items-center justify-center shadow-[0_8px_20px_-8px_rgba(255,91,0,0.7)] hover:bg-cloudera-hover disabled:opacity-40 disabled:shadow-none transition-all"><ArrowUp size={18} /></button>}
            </div>
          </div>
        </main>

        {/* Dataset inspector */}
        <aside className="border-t lg:border-t-0 lg:border-l border-white/[0.06] bg-black/20 overflow-y-auto">
          {!bb.asset ? (
            <div className="h-full flex flex-col items-center justify-center text-center px-6 py-16 text-agent-text-secondary">
              <div className="w-14 h-14 rounded-3xl bg-white/[0.03] border border-white/[0.06] flex items-center justify-center mb-4">
                <Workflow size={26} className="opacity-40" />
              </div>
              <p className="text-sm">No dataset in focus.</p>
              <p className="text-xs mt-1 opacity-70 max-w-[200px]">Discover or name a dataset and its profile appears here.</p>
            </div>
          ) : (
            <div className="p-5 space-y-4">
              {/* Identity */}
              <div>
                <div className="text-xs uppercase tracking-wider text-agent-text-secondary mb-1">Dataset in focus</div>
                <div className="text-base font-semibold tracking-tight truncate" title={bb.asset}>{bb.asset}</div>
                <div className="text-sm text-agent-text-secondary">{(bb.assetType || 'dataset').replace('_', ' ')}{bb.schemaCols != null ? ` · ${bb.schemaCols} columns` : ''}</div>
              </div>

              {/* Profile cards */}
              {bb.quality != null && bb.qualityChecks &&
                <QualityCard asset={bb.asset} overall_score={bb.quality} counts={bb.qualityCounts || {}} checks={bb.qualityChecks as never} total_rows={bb.qualityRows} />}
              {bb.schemaColumns?.length &&
                <SchemaView asset={bb.asset} columns={bb.schemaColumns} reused={bb.schemaReused} />}
              {bb.flowSummary &&
                <PipelineView asset={bb.asset} summary={bb.flowSummary} />}
              {bb.health &&
                <HealthView pipeline={bb.flow || bb.asset} state={bb.health} metrics={bb.healthMetrics} />}
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}

// ── Message renderer — dispatches to the registry ─────────────────────────────

function MsgView({ m, onPick, focus }: { m: Msg; onPick: (n: string) => void; focus: string }) {
  if (m.kind === 'user')
    return (
      <div className="flex justify-end animate-[popIn_.2s_ease]">
        <div className="max-w-[80%] rounded-2xl rounded-tr-md bg-agent-orange/15 border border-agent-orange/25 px-4 py-2.5 text-sm">{m.text}</div>
      </div>
    );

  if (m.kind === 'progress')
    return (
      <div className="flex items-center gap-2 text-sm text-agent-text-secondary animate-[popIn_.2s_ease]">
        <Loader2 size={15} className="animate-spin shrink-0" />
        <span>{m.text}</span>
      </div>
    );

  // 'ui' — dispatch to the registry renderer
  const block = m.block;
  // Assets block gets the live focus + picker injected.
  if (block.kind === 'assets')
    return (
      <div className="animate-[popIn_.3s_ease]">
        <AssetGrid assets={block.assets} onPick={onPick} focus={focus} />
      </div>
    );

  return (
    <div className="animate-[popIn_.3s_ease]">
      {renderUIBlock(block, onPick)}
    </div>
  );
}

// ── Welcome ───────────────────────────────────────────────────────────────────

function Welcome({ onPick }: { onPick: (s: string) => void }) {
  return (
    <div className="h-full flex flex-col items-center justify-center text-center max-w-md mx-auto">
      <div className="w-16 h-16 rounded-3xl bg-agent-orange/10 border border-agent-orange/20 flex items-center justify-center mb-5">
        <Workflow size={30} className="text-agent-orange" />
      </div>
      <h2 className="text-xl font-semibold tracking-tight">Your data team, on call</h2>
      <p className="text-sm text-agent-text-secondary mt-2 leading-relaxed max-w-sm">
        Discover data, check quality, build pipelines, and ask questions of it — all in one place.
      </p>
      <div className="mt-6 flex flex-col gap-2 w-full">
        {SUGGESTIONS.map((s) => (
          <button key={s} onClick={() => onPick(s)}
            className="text-sm text-left px-4 py-3 rounded-2xl bg-agent-dark-surface/70 border border-white/[0.07] hover:border-agent-teal/40 hover:bg-agent-dark-surface transition-colors">
            {s}
          </button>
        ))}
      </div>
    </div>
  );
}

// ── Agent ribbon pill ─────────────────────────────────────────────────────────

function AgentPill({ agent, status, isActive }:
  { agent: { short: string; Icon: typeof Radar }; status: Status; isActive: boolean }) {
  const running = status === 'running', done = status === 'done', skipped = status === 'skipped';
  const tone = running ? 'border-agent-orange/60 bg-agent-orange/10'
    : done ? 'border-agent-teal/40 bg-agent-teal/[0.07]'
    : 'border-white/[0.07] bg-white/[0.02]';
  return (
    <div className={`flex items-center gap-1.5 pl-2.5 pr-3 py-1.5 rounded-full border text-sm transition-all ${tone} ${skipped ? 'opacity-30' : status === 'idle' ? 'opacity-55' : 'opacity-100'}`}
      style={isActive ? { animation: 'glowPulse 1.6s ease-in-out infinite' } : undefined}>
      <agent.Icon size={14} className={running ? 'text-agent-orange' : done ? 'text-agent-teal' : 'text-agent-text-secondary'} />
      <span className="font-medium">{agent.short}</span>
      {running && <span className="w-1.5 h-1.5 rounded-full bg-agent-orange animate-pulse" />}
      {done && <Check size={12} className="text-agent-teal" />}
    </div>
  );
}

const allDone = (s: Record<AgentKey, Status>) => {
  const o = { ...s };
  (Object.keys(o) as AgentKey[]).forEach((k) => { if (o[k] === 'running' || o[k] === 'queued') o[k] = 'done'; });
  return o;
};

const KEYFRAMES = `
@keyframes popIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:none; } }
@keyframes glowPulse { 0%,100% { box-shadow:0 0 0 0 rgba(255,91,0,0); } 50% { box-shadow:0 0 16px 1px rgba(255,91,0,.4); } }
`;
