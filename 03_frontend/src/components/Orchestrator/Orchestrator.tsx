import { type ReactNode, useEffect, useRef, useState } from 'react';
import {
  Radar, Shield, Wrench, Activity, Workflow, ArrowRight, ArrowUp, Square,
  Check, Database, Columns3, Gauge, FileCode2, HeartPulse,
  MessagesSquare, ChevronDown, Loader2, Sparkles, Radio, Table2, HardDrive,
} from 'lucide-react';
import { streamSupervisor, type SupEvent } from '../../api/supervisor';

// ── Agents ────────────────────────────────────────────────────────────────────
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

const SUGGESTIONS = [
  'onboard demo.payments end-to-end',
  'find payment data and check its quality',
  'what is the average amount in demo.payments',
];

const PAGE_BG = 'radial-gradient(140% 100% at 50% -30%, rgba(255,91,0,0.06), transparent 45%), radial-gradient(120% 80% at 100% 0%, rgba(0,163,196,0.045), transparent 42%), #0E131B';

type AssetCard = { name: string; asset_type: string; field_count: number; reason?: string; columns?: { name: string; type?: string }[] };
type Msg = { id: number; kind: 'user' | 'answer' | 'system' | 'assets'; text?: string; sql?: string; grounded?: boolean; pending?: boolean; assets?: AssetCard[] };

const TYPE_META: Record<string, { label: string; Icon: typeof Database; color: string }> = {
  iceberg_table: { label: 'Iceberg tables', Icon: Table2, color: 'text-agent-teal' },
  kafka_topic: { label: 'Kafka topics', Icon: Radio, color: 'text-agent-orange' },
  ozone_volume: { label: 'Ozone volumes', Icon: HardDrive, color: 'text-sky-400' },
};

type Blackboard = {
  asset?: string; assetType?: string; discovered?: number;
  schemaCols?: number; schemaReused?: boolean; schemaColumns?: { name: string; type?: string }[];
  quality?: number | null; qualityChecks?: { check?: string; column?: string; label?: string; status?: string }[];
  flow?: string;
  flowSummary?: { processor_count?: number; parameter_count?: number; processors?: string[]; controller_services?: string[]; parameters_to_fill?: { name: string; sensitive?: boolean }[] };
  health?: string;
};

export function Orchestrator() {
  const [input, setInput] = useState('');
  const [running, setRunning] = useState(false);
  const [intent, setIntent] = useState('');
  const [status, setStatus] = useState<Record<AgentKey, Status>>(IDLE);
  const [active, setActive] = useState<AgentKey | null>(null);
  const [bb, setBb] = useState<Blackboard>({});
  const [discovered, setDiscovered] = useState<AssetCard[]>([]);
  const [focus, setFocus] = useState('');
  const [chat, setChat] = useState<Msg[]>([]);
  const cancelRef = useRef<null | (() => void)>(null);
  const idRef = useRef(0);
  const endRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [chat]);

  const lastPending = (c: Msg[]) => { for (let i = c.length - 1; i >= 0; i--) if (c[i].pending) return i; return -1; };
  const resolvePending = (m: Omit<Msg, 'id'>) =>
    setChat((c) => { const i = lastPending(c); if (i < 0) return c; const n = [...c]; n[i] = { ...m, id: c[i].id }; return n; });
  const dropPending = () => setChat((c) => { const i = lastPending(c); if (i < 0) return c; const n = [...c]; n.splice(i, 1); return n; });
  const setPending = (text: string) => setChat((c) => { const i = lastPending(c); if (i < 0) return c; const n = [...c]; n[i] = { ...n[i], text }; return n; });

  const send = (text: string, ctx?: string) => {
    const m = text.trim();
    if (!m || running) return;
    cancelRef.current?.();
    const uid = (idRef.current += 1);
    setInput('');
    setRunning(true);
    setStatus(IDLE);
    setActive(null);
    setChat((c) => [...c, { id: uid, kind: 'user', text: m }, { id: uid + 0.5, kind: 'system', pending: true }]);
    cancelRef.current = streamSupervisor(
      m, handleEvent,
      () => { setRunning(false); setStatus((s) => allDone(s)); setActive(null); dropPending(); },
      (err) => { setRunning(false); resolvePending({ kind: 'system', text: `⚠ ${err.message}` }); },
      ctx ?? focus ?? undefined,
    );
  };

  // Bring a discovered asset into focus for the inspector + chat (optimistic identity).
  const focusOn = (name: string) => {
    const a = discovered.find((x) => x.name === name);
    setFocus(name);
    setBb({ asset: name, assetType: a?.asset_type, schemaCols: a?.field_count, schemaColumns: a?.columns, discovered: discovered.length });
    setTimeout(() => inputRef.current?.focus(), 0);
  };

  const stop = () => {
    cancelRef.current?.();
    setRunning(false);
    setStatus((s) => { const o = { ...s }; (Object.keys(o) as AgentKey[]).forEach((k) => { if (o[k] === 'running' || o[k] === 'queued') o[k] = 'idle'; }); return o; });
    resolvePending({ kind: 'system', text: 'Stopped.' });
    setActive(null);
  };

  const handleEvent = (e: SupEvent) => {
    switch (e.type) {
      case 'plan': {
        if (!Array.isArray(e.plan)) break;   // analyst's tool-plan — ribbon driven by supervisor plan
        setIntent(e.intent || '');
        const plan: string[] = e.plan || [];
        const skip: string[] = e.skipped || [];
        setStatus(() => { const o: Record<AgentKey, Status> = { ...IDLE }; AGENTS.forEach(({ key }) => { if (plan.includes(key)) o[key] = 'queued'; else if (skip.includes(key)) o[key] = 'skipped'; }); return o; });
        break;
      }
      case 'handoff': {
        const to = e.to as AgentKey;
        setStatus((s) => { const o = { ...s }; (Object.keys(o) as AgentKey[]).forEach((k) => { if (o[k] === 'running') o[k] = 'done'; }); if (to in o) o[to] = 'running'; return o; });
        setActive((['scout', 'guardian', 'pipeline', 'heal', 'analyst'] as string[]).includes(to) ? to as AgentKey : null);
        break;
      }
      case 'agent_skipped': { const k = NAME_TO_KEY[e.agent_name]; if (k) setStatus((s) => ({ ...s, [k]: 'skipped' })); break; }
      case 'agent_start': if (e.asset) setFocus(e.asset); if (e.agent_name) setPending(`${e.agent_name} working…`); break;
      case 'step': if (e.status === 'running') setPending(STEP_LABEL[e.name] || `${e.name}…`); break;
      case 'sql_result': if (!e.error) setPending('summarizing the result…'); break;
      case 'assets': {
        const list = (e.assets || []) as AssetCard[];
        setDiscovered(list);
        setBb((b) => ({ ...b, discovered: list.length }));
        resolvePending({ kind: 'assets', assets: list });
        break;
      }
      case 'blackboard':
        if ((e.wrote || []).includes('asset')) { setFocus(e.asset); setBb((b) => ({ ...b, asset: e.asset, assetType: e.asset_type, schemaCols: e.field_count, schemaColumns: e.columns || b.schemaColumns })); }
        if ((e.wrote || []).includes('pipeline')) setBb((b) => ({ ...b, flow: e.flow_name }));
        break;
      case 'schema': setBb((b) => ({ ...b, schemaCols: (e.columns || []).length || b.schemaCols, schemaReused: !!e.reused, schemaColumns: (e.columns?.length) ? e.columns : b.schemaColumns })); break;
      case 'basic_scorecard': setBb((b) => ({ ...b, quality: e.overall_score, qualityChecks: e.checks || b.qualityChecks })); break;
      case 'flow_generated': setBb((b) => ({ ...b, flow: e.flow_name, flowSummary: e.summary })); break;
      case 'health_check': setBb((b) => ({ ...b, health: e.state })); break;
      case 'context': if (e.asset) setFocus(e.asset); if (e.field_count != null) setBb((b) => ({ ...b, asset: e.asset, schemaCols: e.field_count, schemaReused: !!e.reused, schemaColumns: e.columns || b.schemaColumns })); break;
      case 'quality': if (e.overall_score != null) setBb((b) => ({ ...b, quality: e.overall_score })); break;
      case 'answer': resolvePending({ kind: 'answer', text: e.text, sql: e.sql, grounded: e.grounded }); break;
      case 'text': resolvePending({ kind: 'system', text: e.text }); break;
      case 'complete':
        if (e.agent === 'supervisor') {
          setStatus((s) => allDone(s)); setActive(null);
          const txt = `${e.asset ? `${e.asset} — ` : ''}${e.summary || 'done'}`;
          const sid = (idRef.current += 1);
          setChat((c) => {
            const i = lastPending(c);
            // pending bubble still up (e.g. a quality check that produced no chat
            // artifact) → make it the reply; else only add a line for multi-agent chains.
            if (i >= 0) { const n = [...c]; n[i] = { id: c[i].id, kind: 'system', text: txt }; return n; }
            if ((e.agents_run?.length || 0) > 1) return [...c, { id: sid, kind: 'system', text: txt }];
            return c;
          });
        }
        break;
      case 'error': resolvePending({ kind: 'system', text: `⚠ ${e.message || 'error'}` }); break;
    }
  };

  // detail nodes
  const schemaDetail: ReactNode = bb.schemaColumns?.length ? <div className="flex flex-wrap gap-1.5">{bb.schemaColumns.map((c) => <Pill key={c.name} label={c.name} sub={c.type} />)}</div> : undefined;
  const qualityDetail: ReactNode = bb.qualityChecks?.length ? (
    <div className="space-y-1">{bb.qualityChecks.map((c, i) => (
      <div key={i} className="flex items-center justify-between gap-2 text-xs"><span className="text-agent-text-secondary truncate"><span className="text-agent-text-primary">{c.column}</span> · {c.check}{c.label ? ` — ${c.label}` : ''}</span><CheckStatus status={c.status} /></div>
    ))}</div>) : undefined;
  const pipelineDetail: ReactNode = bb.flowSummary ? (
    <div className="space-y-2.5">
      {!!bb.flowSummary.processors?.length && <PillRow label="Processors" items={bb.flowSummary.processors} />}
      {!!bb.flowSummary.controller_services?.length && <PillRow label="Services" items={bb.flowSummary.controller_services} />}
      {!!bb.flowSummary.parameters_to_fill?.length && <div><div className="text-xs text-agent-text-secondary mb-1">Parameters to fill</div><div className="flex flex-wrap gap-1.5">{bb.flowSummary.parameters_to_fill.map((p) => <Pill key={p.name} label={p.name} sub={p.sensitive ? 'secret' : undefined} secret={p.sensitive} />)}</div></div>}
    </div>) : undefined;

  return (
    <div className="h-full flex flex-col text-agent-text-primary" style={{ background: PAGE_BG }}>
      <style>{KEYFRAMES}</style>

      {/* Header + agent ribbon */}
      <header className="px-7 pt-5 pb-4 border-b border-white/[0.06]">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-2xl bg-agent-orange/15 border border-agent-orange/25 flex items-center justify-center shadow-[0_6px_18px_-6px_rgba(255,91,0,0.5)]">
            <Workflow size={20} className="text-agent-orange" />
          </div>
          <div className="flex-1 min-w-0">
            <h1 className="text-lg font-semibold tracking-tight leading-tight">Orchestrator</h1>
            <p className="text-sm text-agent-text-secondary leading-tight">Your data team — one conversation, the right agents.</p>
          </div>
          {intent && <span className="text-xs px-3 py-1.5 rounded-full bg-agent-teal/10 border border-agent-teal/25 text-agent-teal shrink-0">intent · {intent}</span>}
        </div>

        <div className="mt-3.5 flex items-center gap-1 flex-wrap">
          {AGENTS.map((a, i) => (
            <div key={a.key} className="flex items-center">
              <AgentPill agent={a} status={status[a.key]} active={active === a.key} />
              {i < AGENTS.length - 1 && <ArrowRight size={13} className={`mx-0.5 ${status[a.key] === 'done' ? 'text-agent-teal/70' : 'text-white/15'}`} />}
            </div>
          ))}
        </div>
      </header>

      {/* Body: conversation (left) + inspector (right) */}
      <div className="flex-1 min-h-0 grid grid-cols-1 lg:grid-cols-[1fr,380px]">
        {/* Conversation */}
        <main className="flex flex-col min-h-0">
          <div className="flex-1 overflow-y-auto px-7 py-6">
            {chat.length === 0 ? (
              <Welcome onPick={send} />
            ) : (
              <div className="max-w-3xl mx-auto space-y-4">
                {chat.map((m) => m.kind === 'assets'
                  ? <AssetsCard key={m.id} assets={m.assets || []} focus={focus} onPick={focusOn} />
                  : <MessageView key={m.id} m={m} />)}
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
                  ref={inputRef}
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && send(input)}
                  placeholder={focus ? `Ask about ${focus}, or tell the team what to do…` : 'Ask the team…  e.g.  “onboard demo.payments end-to-end”'}
                  className="w-full bg-agent-dark-surface/70 border border-white/[0.08] rounded-2xl pl-11 pr-4 py-3.5 text-sm placeholder:text-agent-text-secondary/75 focus:outline-none focus:border-agent-orange/60 focus:ring-2 focus:ring-agent-orange/15 transition-all"
                />
              </div>
              {running ? (
                <button onClick={stop} className="w-12 h-12 rounded-2xl bg-agent-dark-surface/80 border border-white/[0.08] hover:border-rose-500/50 flex items-center justify-center transition-colors"><Square size={16} className="text-rose-400" /></button>
              ) : (
                <button onClick={() => send(input)} disabled={!input.trim()} className="w-12 h-12 rounded-2xl bg-agent-orange text-white flex items-center justify-center shadow-[0_8px_20px_-8px_rgba(255,91,0,0.7)] hover:bg-cloudera-hover disabled:opacity-40 disabled:shadow-none transition-all"><ArrowUp size={18} /></button>
              )}
            </div>
          </div>
        </main>

        {/* Inspector */}
        <aside className="border-t lg:border-t-0 lg:border-l border-white/[0.06] bg-black/20 overflow-y-auto">
          {!bb.asset ? (
            <div className="h-full flex flex-col items-center justify-center text-center px-6 py-16 text-agent-text-secondary">
              <div className="w-14 h-14 rounded-3xl bg-white/[0.03] border border-white/[0.06] flex items-center justify-center mb-4"><Database size={26} className="opacity-50" /></div>
              <p className="text-sm">No dataset in focus.</p>
              <p className="text-xs mt-1 opacity-70 max-w-[220px]">Discover or name a dataset and its profile shows up here.</p>
            </div>
          ) : (
            <div className="p-5 space-y-4">
              {/* identity */}
              <div>
                <div className="text-xs uppercase tracking-wider text-agent-text-secondary mb-1">Dataset in focus</div>
                <div className="text-lg font-semibold tracking-tight truncate" title={bb.asset}>{bb.asset}</div>
                <div className="text-sm text-agent-text-secondary">{(bb.assetType || 'dataset').replace('_', ' ')}{bb.schemaCols != null ? ` · ${bb.schemaCols} columns` : ''}</div>
              </div>

              {/* profile cards */}
              <div className="space-y-3 pt-1">
                <Card icon={Gauge} label="Quality" accent={qScoreAccent(bb.quality)} big
                  value={bb.quality != null ? String(bb.quality) : undefined} sub={bb.quality != null ? 'overall score' : undefined}
                  empty={bb.quality == null} emptyAction="Check quality" onEmpty={() => send(`check the quality of ${bb.asset}`, bb.asset)} detail={qualityDetail} />
                <Card icon={Columns3} label="Schema" accent="teal"
                  value={bb.schemaCols != null ? `${bb.schemaCols} columns` : undefined} badge={bb.schemaReused ? 'reused ♻' : undefined}
                  empty={bb.schemaCols == null} detail={schemaDetail} />
                <Card icon={FileCode2} label="Pipeline" accent="orange"
                  value={bb.flow} sub={bb.flowSummary ? `${bb.flowSummary.processor_count ?? '?'} processors · ${bb.flowSummary.parameter_count ?? '?'} params` : undefined}
                  empty={!bb.flow} emptyAction="Build pipeline" onEmpty={() => send(`build an ingestion pipeline for ${bb.asset}`, bb.asset)} detail={pipelineDetail} />
                <Card icon={HeartPulse} label="Health" accent={bb.health === 'healthy' ? 'teal' : 'orange'}
                  value={bb.health} sub={bb.health ? 'pipeline status' : undefined} empty={!bb.health} />
              </div>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}

const allDone = (s: Record<AgentKey, Status>) => { const o = { ...s }; (Object.keys(o) as AgentKey[]).forEach((k) => { if (o[k] === 'running' || o[k] === 'queued') o[k] = 'done'; }); return o; };

const STEP_LABEL: Record<string, string> = {
  discover: 'searching the catalog…', resolve_schema: 'reading the schema…',
  generate_sql: 'writing SQL…', run_sql: 'running on Impala…',
  basic_checks: 'checking quality…', sample_profile: 'profiling the data…',
  lineage: 'tracing lineage…', build_nifi_flow: 'building the pipeline…', check_health: 'checking health…',
};

// ── Welcome ───────────────────────────────────────────────────────────────────
function Welcome({ onPick }: { onPick: (s: string) => void }) {
  return (
    <div className="h-full flex flex-col items-center justify-center text-center max-w-md mx-auto">
      <div className="w-16 h-16 rounded-3xl bg-agent-orange/10 border border-agent-orange/20 flex items-center justify-center mb-5"><Workflow size={30} className="text-agent-orange" /></div>
      <h2 className="text-xl font-semibold tracking-tight">Your data team, on call</h2>
      <p className="text-sm text-agent-text-secondary mt-2 leading-relaxed">Discover data, check its quality, build pipelines, and ask questions of it — all in one place. Tell me what you need.</p>
      <div className="mt-6 flex flex-col gap-2 w-full">
        {SUGGESTIONS.map((s) => (
          <button key={s} onClick={() => onPick(s)} className="text-sm text-left px-4 py-3 rounded-2xl bg-agent-dark-surface/70 border border-white/[0.07] hover:border-agent-teal/40 hover:bg-agent-dark-surface transition-colors">
            {s}
          </button>
        ))}
      </div>
    </div>
  );
}

// ── Message ───────────────────────────────────────────────────────────────────
function MessageView({ m }: { m: Msg }) {
  const [showSql, setShowSql] = useState(false);
  if (m.kind === 'user')
    return <div className="flex justify-end animate-[popIn_.2s_ease]"><div className="max-w-[80%] rounded-2xl rounded-tr-md bg-agent-orange/15 border border-agent-orange/25 px-4 py-2.5 text-sm">{m.text}</div></div>;

  if (m.kind === 'system') {
    if (m.pending) return <div className="flex items-center gap-2 text-sm text-agent-text-secondary animate-[popIn_.2s_ease]"><Loader2 size={15} className="animate-spin" /> {m.text || 'the team is working…'}</div>;
    return <div className="flex items-center gap-2 text-sm text-agent-text-secondary animate-[popIn_.2s_ease]"><Check size={15} className="text-agent-teal shrink-0" /><span>{m.text}</span></div>;
  }

  // answer
  return (
    <div className="flex justify-start animate-[popIn_.25s_ease]">
      <div className="max-w-[88%] rounded-2xl rounded-tl-md bg-agent-dark-surface/80 border border-white/[0.06] px-4 py-3">
        <p className="text-sm leading-relaxed">{m.text}</p>
        {m.grounded && <span className="inline-flex items-center gap-1 mt-2 text-xs text-agent-teal"><Check size={12} />grounded in data</span>}
        {m.sql && (
          <div className="mt-2">
            <button onClick={() => setShowSql((v) => !v)} className="text-xs text-agent-text-secondary hover:text-agent-text-primary flex items-center gap-1"><ChevronDown size={12} className={`transition-transform ${showSql ? 'rotate-180' : ''}`} /> {showSql ? 'Hide' : 'Show'} SQL</button>
            {showSql && <pre className="mt-1.5 text-xs bg-black/40 border border-white/[0.06] rounded-xl p-2.5 overflow-x-auto text-agent-text-secondary font-mono leading-relaxed">{m.sql}</pre>}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Discovered assets, grouped by type ────────────────────────────────────────
function AssetsCard({ assets, focus, onPick }: { assets: AssetCard[]; focus: string; onPick: (n: string) => void }) {
  const groups: Record<string, AssetCard[]> = {};
  assets.forEach((a) => { (groups[a.asset_type] = groups[a.asset_type] || []).push(a); });
  const order = Object.keys(groups).sort((a, b) => (TYPE_META[a]?.label || a).localeCompare(TYPE_META[b]?.label || b));
  return (
    <div className="flex justify-start animate-[popIn_.25s_ease]">
      <div className="max-w-[92%] rounded-2xl rounded-tl-md bg-agent-dark-surface/80 border border-white/[0.06] p-4 space-y-3">
        <div className="text-sm text-agent-text-secondary">Found {assets.length} assets — tap one to inspect or ask about it.</div>
        {order.map((type) => {
          const meta = TYPE_META[type] || { label: type, Icon: Database, color: 'text-agent-text-secondary' };
          return (
            <div key={type}>
              <div className="flex items-center gap-1.5 text-xs uppercase tracking-wide text-agent-text-secondary mb-1.5">
                <meta.Icon size={13} className={meta.color} /> {meta.label} · {groups[type].length}
              </div>
              <div className="flex flex-wrap gap-1.5">
                {groups[type].map((a) => (
                  <button key={a.name} onClick={() => onPick(a.name)}
                    className={`text-left px-3 py-1.5 rounded-xl border transition-colors ${a.name === focus ? 'border-agent-teal/50 bg-agent-teal/10' : 'border-white/[0.08] bg-white/[0.02] hover:border-white/20'}`}>
                    <span className="text-sm font-medium">{a.name}</span>
                    <span className="text-xs text-agent-text-secondary ml-1.5">{a.field_count} cols</span>
                  </button>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Agent ribbon pill ─────────────────────────────────────────────────────────
function AgentPill({ agent, status, active }: { agent: { short: string; Icon: typeof Radar }; status: Status; active: boolean }) {
  const running = status === 'running', done = status === 'done', skipped = status === 'skipped';
  const tone = running ? 'border-agent-orange/60 bg-agent-orange/10' : done ? 'border-agent-teal/40 bg-agent-teal/[0.07]' : 'border-white/[0.07] bg-white/[0.02]';
  const op = skipped ? 'opacity-30' : status === 'idle' ? 'opacity-55' : 'opacity-100';
  return (
    <div className={`flex items-center gap-1.5 pl-2.5 pr-3 py-1.5 rounded-full border text-sm transition-all ${tone} ${op}`} style={active ? { animation: 'glowPulse 1.6s ease-in-out infinite' } : undefined}>
      <agent.Icon size={15} className={running ? 'text-agent-orange' : done ? 'text-agent-teal' : 'text-agent-text-secondary'} />
      <span className="font-medium">{agent.short}</span>
      {running && <span className="w-1.5 h-1.5 rounded-full bg-agent-orange animate-pulse" />}
      {done && <Check size={13} className="text-agent-teal" />}
      {skipped && <span className="text-xs text-agent-text-secondary/60">skip</span>}
    </div>
  );
}

// ── Inspector card ────────────────────────────────────────────────────────────
function Card({ icon: Icon, label, value, sub, tag, badge, accent, big, detail, empty, emptyAction, onEmpty }:
  { icon: typeof Database; label: string; value?: string; sub?: string; tag?: string; badge?: string; accent: 'teal' | 'orange' | 'rose' | 'amber'; big?: boolean; detail?: ReactNode; empty?: boolean; emptyAction?: string; onEmpty?: () => void }) {
  const [open, setOpen] = useState(false);
  const clickable = !!detail && !empty;
  const aT = { teal: 'text-agent-teal', orange: 'text-agent-orange', rose: 'text-rose-400', amber: 'text-amber-400' }[accent];
  const aB = empty ? 'border-white/[0.06]' : { teal: 'border-agent-teal/25', orange: 'border-agent-orange/25', rose: 'border-rose-500/25', amber: 'border-amber-500/25' }[accent];
  return (
    <div onClick={clickable ? () => setOpen((o) => !o) : undefined}
      className={`rounded-2xl border ${aB} bg-agent-dark-surface/60 px-4 py-3.5 ${clickable ? 'cursor-pointer hover:border-agent-teal/50 transition-colors' : ''} ${empty ? 'opacity-80' : 'animate-[popIn_.35s_ease]'}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-agent-text-secondary"><Icon size={15} className={empty ? '' : aT} /> <span className="text-xs uppercase tracking-wide">{label}</span></div>
        <div className="flex items-center gap-1.5">
          {badge && <span className={`text-xs px-2 py-0.5 rounded-full font-mono bg-agent-teal/10 ${aT}`}>{badge}</span>}
          {tag && !badge && <span className="text-xs text-agent-text-secondary">{tag}</span>}
          {clickable && <ChevronDown size={14} className={`text-agent-text-secondary transition-transform ${open ? 'rotate-180' : ''}`} />}
        </div>
      </div>
      {empty ? (
        <div className="mt-1.5 flex items-center justify-between">
          <span className="text-sm text-agent-text-secondary/50">not yet</span>
          {emptyAction && onEmpty && <button onClick={(e) => { e.stopPropagation(); onEmpty(); }} className={`text-xs px-2.5 py-1 rounded-full border border-white/[0.1] text-agent-text-secondary hover:text-agent-text-primary hover:border-white/25 transition-colors`}>{emptyAction}</button>}
        </div>
      ) : (
        <>
          <div className={`mt-1 font-semibold truncate ${big ? `text-3xl ${aT}` : 'text-base'}`} title={value}>{value}</div>
          {sub && <div className="text-xs text-agent-text-secondary truncate mt-0.5">{sub}</div>}
          {clickable && !open && <div className="text-xs text-agent-text-secondary/60 mt-1">tap to expand</div>}
          {clickable && open && <div className="mt-3 pt-3 border-t border-white/[0.06]" onClick={(e) => e.stopPropagation()}>{detail}</div>}
        </>
      )}
    </div>
  );
}

// ── bits ──────────────────────────────────────────────────────────────────────
function Pill({ label, sub, secret }: { label: string; sub?: string; secret?: boolean }) {
  return <span className={`text-xs font-mono px-2 py-0.5 rounded-full border ${secret ? 'border-amber-500/30 text-amber-300/90 bg-amber-500/[0.06]' : 'border-white/[0.06] text-agent-text-secondary bg-white/[0.04]'}`}>{label}{sub ? <span className="opacity-50"> · {sub}</span> : null}</span>;
}
function PillRow({ label, items }: { label: string; items?: string[] }) {
  return <div><div className="text-xs text-agent-text-secondary mb-1">{label}</div><div className="flex flex-wrap gap-1.5">{(items || []).map((it) => <Pill key={it} label={it} />)}</div></div>;
}
function CheckStatus({ status }: { status?: string }) {
  const c = status === 'pass' ? 'text-agent-teal bg-agent-teal/10' : status === 'warn' ? 'text-amber-400 bg-amber-500/10' : status === 'fail' ? 'text-rose-400 bg-rose-500/10' : 'text-agent-text-secondary bg-white/[0.04]';
  return <span className={`text-xs px-1.5 py-0.5 rounded-full shrink-0 ${c}`}>{status || '—'}</span>;
}
function qScoreAccent(s?: number | null): 'teal' | 'amber' | 'rose' { if (s == null) return 'teal'; return s >= 90 ? 'teal' : s >= 70 ? 'amber' : 'rose'; }

const KEYFRAMES = `
@keyframes popIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: none; } }
@keyframes glowPulse { 0%,100% { box-shadow: 0 0 0 0 rgba(255,91,0,0); } 50% { box-shadow: 0 0 16px 1px rgba(255,91,0,.4); } }
`;
