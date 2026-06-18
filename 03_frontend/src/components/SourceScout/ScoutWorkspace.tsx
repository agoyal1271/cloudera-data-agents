import { useState, useRef, useEffect, useCallback } from 'react';
import { Radar, ArrowUp, Database, GitBranch, Table2, MessageSquare, Sparkles, Play, Compass, ShieldCheck, AlertTriangle, Check, Cpu, Server, ChevronDown, ChevronRight, Eye } from 'lucide-react';
import { streamChat, type ChatBlock, type ProvenanceSpan, type ProvenanceSummary, type SpanKind } from '../../api/scout';
import { CanvasPanel, type Artifact } from './canvas/CanvasPanel';

type Turn =
  | { role: 'user'; text: string }
  | { role: 'assistant'; blocks: ChatBlock[] };

const STARTERS = [
  { icon: Database,      text: 'Find payment data' },
  { icon: GitBranch,     text: 'Where does customer_360 come from?' },
  { icon: Table2,        text: 'Top 5 merchants by amount in payment_transactions' },
  { icon: MessageSquare, text: 'What is fraud_alerts?' },
];

const isArtifact = (b: ChatBlock): b is Artifact =>
  b.type === 'lineage' || b.type === 'sql_result' || b.type === 'schema' || b.type === 'assets' || b.type === 'quality';

export function ScoutWorkspace() {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [artifact, setArtifact] = useState<Artifact | null>(null);
  const [contextAsset, setContextAsset] = useState<string | undefined>();
  const ctxRef = useRef<{ asset?: string; assetType?: string }>({});
  const cancelRef = useRef<(() => void) | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [turns, streaming]);

  const finishStream = useCallback(() => {
    setStreaming(false);
    setTurns(prev => {
      const next = [...prev];
      const last = next[next.length - 1];
      if (last?.role === 'assistant') {
        next[next.length - 1] = { role: 'assistant', blocks: last.blocks.filter(b => b.type !== 'thinking') };
      }
      return next;
    });
  }, []);

  const send = useCallback((message: string) => {
    const msg = message.trim();
    if (!msg || streaming) return;
    setInput('');
    setStreaming(true);
    setTurns(prev => [...prev, { role: 'user', text: msg }, { role: 'assistant', blocks: [] }]);

    cancelRef.current?.();
    cancelRef.current = streamChat(
      msg,
      ctxRef.current,
      (block) => {
        if (block.type === 'context') {
          ctxRef.current = { asset: block.asset, assetType: block.asset_type };
          setContextAsset(block.asset);
          return;
        }
        // Heavy artifacts open on the canvas (foraging/exploration surface).
        if (isArtifact(block)) setArtifact(block);

        setTurns(prev => {
          const next = [...prev];
          const last = next[next.length - 1];
          if (last?.role !== 'assistant') return prev;
          const blocks: ChatBlock[] = last.blocks.filter(b => b.type !== 'thinking');
          blocks.push(block);
          next[next.length - 1] = { role: 'assistant', blocks };
          return next;
        });
      },
      finishStream,
      finishStream,
    );
  }, [streaming, finishStream]);

  const empty = turns.length === 0;

  return (
    <div className="flex h-full bg-agent-dark-bg">
      {/* LEFT — conversation */}
      <div className="w-[40%] min-w-[380px] max-w-[540px] flex flex-col border-r border-agent-dark-border">
        <div className="flex items-center gap-2.5 px-5 py-4 border-b border-agent-dark-border flex-shrink-0">
          <div className="w-8 h-8 rounded-lg bg-cloudera/15 flex items-center justify-center">
            <Radar size={18} className="text-cloudera" />
          </div>
          <div className="min-w-0">
            <h2 className="text-base font-semibold text-agent-text-primary leading-tight">Source Scout</h2>
            <p className="text-xs text-agent-text-secondary leading-tight truncate">Ask · discover · trace · query</p>
          </div>
          {contextAsset && (
            <div className="ml-auto flex items-center gap-1.5 text-xs text-agent-text-secondary bg-agent-dark-surface border border-agent-dark-border rounded-full px-2.5 py-1 max-w-[45%]">
              <span className="w-1.5 h-1.5 rounded-full bg-cloudera flex-shrink-0" />
              <span className="font-mono truncate">{contextAsset}</span>
            </div>
          )}
        </div>

        <div ref={scrollRef} className="flex-1 overflow-y-auto">
          {empty ? (
            <div className="h-full flex flex-col items-center justify-center px-6 text-center">
              <div className="w-16 h-16 rounded-[1.4rem] bg-cloudera/15 flex items-center justify-center mb-5 shadow-lg shadow-cloudera/10">
                <Sparkles size={28} className="text-cloudera" />
              </div>
              <h1 className="text-xl font-semibold text-agent-text-primary mb-2 tracking-tight">Ask anything about your data</h1>
              <p className="text-sm text-agent-text-secondary max-w-sm mb-7 leading-relaxed">
                Discover assets, trace lineage from OpenMetadata, and get answers by running SQL on Cloudera.
              </p>
              <div className="grid grid-cols-1 gap-2.5 w-full max-w-sm">
                {STARTERS.map(s => (
                  <button key={s.text} onClick={() => send(s.text)}
                    className="flex items-center gap-3 text-left text-sm text-agent-text-primary bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border hover:border-cloudera/50 rounded-2xl px-4 py-3.5 transition-all duration-150 active:scale-[0.98]">
                    <s.icon size={17} className="text-cloudera flex-shrink-0" />
                    <span className="truncate">{s.text}</span>
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div className="px-5 py-5 space-y-5">
              {turns.map((turn, i) =>
                turn.role === 'user' ? (
                  <div key={i} className="flex justify-end">
                    <div className="bg-cloudera text-white rounded-[1.4rem] rounded-br-md px-4 py-2.5 text-sm leading-relaxed max-w-[85%] shadow-sm">{turn.text}</div>
                  </div>
                ) : (
                  <AssistantTurn key={i} blocks={turn.blocks} active={artifact}
                    onOpen={setArtifact} onAsk={send} streaming={streaming && i === turns.length - 1} />
                )
              )}
            </div>
          )}
        </div>

        <div className="flex-shrink-0 border-t border-agent-dark-border px-5 py-4">
          <form onSubmit={e => { e.preventDefault(); send(input); }}
            className="flex items-center gap-2 bg-agent-dark-surface border border-agent-dark-border rounded-full pl-5 pr-1.5 py-1.5 focus-within:border-cloudera/60 focus-within:shadow-lg focus-within:shadow-cloudera/5 transition-all">
            <input value={input} onChange={e => setInput(e.target.value)}
              placeholder={contextAsset ? `Ask about ${contextAsset}…` : 'Ask anything…'}
              disabled={streaming}
              className="flex-1 bg-transparent text-sm text-agent-text-primary placeholder-agent-text-secondary focus:outline-none py-1.5" />
            <button type="submit" disabled={!input.trim() || streaming}
              className="w-10 h-10 rounded-full bg-cloudera hover:bg-cloudera-hover disabled:opacity-30 disabled:scale-100 flex items-center justify-center transition-all duration-150 active:scale-90 flex-shrink-0">
              {streaming
                ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                : <ArrowUp size={19} className="text-white" strokeWidth={2.5} />}
            </button>
          </form>
        </div>
      </div>

      {/* RIGHT — canvas */}
      <div className="flex-1 min-w-0">
        <CanvasPanel artifact={artifact} onAsk={send} />
      </div>
    </div>
  );
}

// ── Conversation turn ─────────────────────────────────────────────────────────
type StepBlock = Extract<ChatBlock, { type: 'step' }>;

function AssistantTurn({ blocks, active, onOpen, onAsk, streaming }: {
  blocks: ChatBlock[]; active: Artifact | null; onOpen: (a: Artifact) => void; onAsk: (q: string) => void; streaming: boolean;
}) {
  const steps = blocks.filter((b): b is StepBlock => b.type === 'step');
  const thinking = blocks.find(b => b.type === 'thinking') as Extract<ChatBlock, { type: 'thinking' }> | undefined;
  const rest = blocks.filter(b => b.type !== 'step' && b.type !== 'thinking');
  const hasResult = rest.length > 0;

  return (
    <div className="flex gap-3">
      <div className="w-8 h-8 rounded-lg bg-cloudera/15 flex items-center justify-center flex-shrink-0 mt-0.5">
        <Radar size={15} className="text-cloudera" />
      </div>
      <div className="flex-1 min-w-0 space-y-3">
        {blocks.length === 0 && streaming && <ThinkingDots />}
        {steps.length > 0 && <StepTrail steps={steps} lastActive={streaming && !hasResult} />}
        {steps.length === 0 && thinking && <ThinkingLine text={thinking.text} />}
        {rest.map((b, i) => {
          if (b.type === 'text') return <p key={i} className="text-sm text-agent-text-primary leading-relaxed">{renderInline(b.text)}</p>;
          if (b.type === 'caveat') return <CaveatBlock key={i} text={b.text} level={b.level} />;
          if (b.type === 'provenance') return <ProvenancePanel key={i} spans={b.spans} summary={b.summary} />;
          if (isArtifact(b)) return <ArtifactChip key={i} block={b} active={active === b} onClick={() => onOpen(b)} />;
          return null;
        })}
      </div>
    </div>
  );
}

// A persistent trail of what the agent is doing — visibility of system status.
function StepTrail({ steps, lastActive }: { steps: StepBlock[]; lastActive: boolean }) {
  return (
    <div className="rounded-xl border border-agent-dark-border bg-agent-dark-surface/40 px-3.5 py-3">
      <ol className="space-y-2.5">
        {steps.map((s, i) => {
          const active = lastActive && i === steps.length - 1;
          return (
            <li key={i} className="flex items-start gap-2.5">
              <span className="mt-[3px] flex-shrink-0">
                {active
                  ? <span className="block w-3.5 h-3.5 border-2 border-cloudera border-t-transparent rounded-full animate-spin" />
                  : <span className="block w-3.5 h-3.5 rounded-full bg-cloudera/15 flex items-center justify-center"><Check size={10} className="text-cloudera" strokeWidth={3} /></span>}
              </span>
              <span className="min-w-0">
                <span className={`block text-sm leading-snug ${active ? 'text-agent-text-primary font-medium' : 'text-agent-text-secondary'}`}>{s.label}</span>
                {s.detail && <span className="block text-xs text-agent-text-secondary/70 font-mono truncate mt-0.5">{s.detail}</span>}
              </span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function ArtifactChip({ block, active, onClick }: { block: Artifact; active: boolean; onClick: () => void }) {
  const meta = chipMeta(block);
  return (
    <button onClick={onClick}
      className={`w-full flex items-center gap-3 text-left rounded-2xl border px-3.5 py-3 transition-all duration-150 active:scale-[0.98] ${
        active ? 'border-cloudera/60 bg-cloudera/10' : 'border-agent-dark-border bg-agent-dark-surface hover:border-cloudera/40'
      }`}>
      <div className="w-8 h-8 rounded-xl bg-agent-dark-bg flex items-center justify-center flex-shrink-0">{meta.icon}</div>
      <div className="min-w-0 flex-1">
        <div className="text-sm font-medium text-agent-text-primary truncate">{meta.title}</div>
        <div className="text-xs text-agent-text-secondary truncate">{meta.sub}</div>
      </div>
      <span className="text-[11px] font-medium text-cloudera flex-shrink-0">{active ? 'on canvas' : 'view →'}</span>
    </button>
  );
}

function chipMeta(b: Artifact): { icon: React.ReactNode; title: string; sub: string } {
  switch (b.type) {
    case 'lineage':    return { icon: <GitBranch size={14} className="text-cloudera" />, title: `Lineage · ${b.asset}`, sub: `${b.upstream.length} upstream · ${b.downstream.length} downstream` };
    case 'sql_result': return { icon: <Play size={13} className="text-green-400" />, title: `Query result · ${b.asset}`, sub: b.error ? 'failed' : `${b.row_count ?? b.rows.length} rows` };
    case 'schema':     return { icon: <Table2 size={14} className="text-teal-400" />, title: `Schema · ${b.asset}`, sub: `${b.fields.length} fields` };
    case 'assets':     return { icon: <Compass size={14} className="text-cloudera" />, title: 'Discovered assets', sub: `${b.assets.length} matches` };
    case 'quality':    return { icon: <ShieldCheck size={14} className="text-cloudera" />, title: `Quality · ${b.asset}`, sub: `score ${b.overall_score}${b.trend && b.trend.direction === 'down' ? ' · degrading' : ''}` };
  }
}

function CaveatBlock({ text, level }: { text: string; level: string }) {
  const tone = level === 'poor' ? 'bg-red-950/30 border-red-800/40 text-red-200'
    : 'bg-amber-950/30 border-amber-800/40 text-amber-200';
  return (
    <div className={`flex items-start gap-2 p-2.5 rounded-xl border text-xs ${tone}`}>
      <AlertTriangle size={13} className="mt-0.5 flex-shrink-0" />
      <span>{text}</span>
    </div>
  );
}

// ── Provenance: "How this answer was made" ────────────────────────────────────
const SPAN_VIS: Record<SpanKind, { Icon: typeof Cpu; label: string; color: string }> = {
  llm:           { Icon: Sparkles,  label: 'AI model',        color: 'text-cloudera' },
  deterministic: { Icon: Cpu,       label: 'Deterministic',   color: 'text-agent-text-secondary' },
  knox:          { Icon: Server,    label: 'Cloudera · Knox', color: 'text-teal-300' },
  openmetadata:  { Icon: GitBranch, label: 'OpenMetadata',    color: 'text-blue-300' },
};

function ProvenancePanel({ spans, summary }: { spans: ProvenanceSpan[]; summary: ProvenanceSummary }) {
  const [open, setOpen] = useState(false);
  const hasKnox = spans.some(s => s.kind === 'knox');
  const hasOM = spans.some(s => s.kind === 'openmetadata');
  const secs = (summary.total_ms / 1000).toFixed(1);

  const parts: string[] = [];
  if (hasKnox) parts.push('computed by SQL on Cloudera');
  if (hasOM) parts.push('grounded in OpenMetadata');
  parts.push(summary.llm_calls ? `model used for ${summary.llm_calls} step${summary.llm_calls > 1 ? 's' : ''}` : 'no model used');
  const badge = parts.join(' · ');

  return (
    <div className="rounded-2xl border border-agent-dark-border bg-agent-dark-surface/40 overflow-hidden">
      <button onClick={() => setOpen(o => !o)}
        className="w-full flex items-start gap-2.5 px-3.5 py-2.5 text-left hover:bg-agent-dark-border/30 transition-colors">
        <ShieldCheck size={15} className="text-cloudera flex-shrink-0 mt-0.5" />
        <div className="min-w-0 flex-1">
          <div className="flex items-baseline gap-2">
            <span className="text-xs font-semibold text-agent-text-primary">How this answer was made</span>
            <span className="text-[11px] text-agent-text-secondary tabular-nums whitespace-nowrap">
              {summary.llm_calls} AI · {summary.deterministic_steps} det · {secs}s{summary.total_tokens ? ` · ${summary.total_tokens} tok` : ''}
            </span>
          </div>
          <div className="text-[11px] text-agent-text-secondary truncate">{badge}</div>
        </div>
        {open ? <ChevronDown size={15} className="text-agent-text-secondary flex-shrink-0 mt-0.5" /> : <ChevronRight size={15} className="text-agent-text-secondary flex-shrink-0 mt-0.5" />}
      </button>
      {open && (
        <div className="px-3.5 pb-3 pt-2 space-y-1.5 border-t border-agent-dark-border">
          {spans.map((s, i) => <ProvenanceRow key={i} s={s} />)}
        </div>
      )}
    </div>
  );
}

function ProvenanceRow({ s }: { s: ProvenanceSpan }) {
  const [showPrompt, setShowPrompt] = useState(false);
  const vis = SPAN_VIS[s.kind];
  const hasPrompt = s.kind === 'llm' && (s.prompt || s.completion);
  return (
    <div className="rounded-xl bg-agent-dark-bg/60 border border-agent-dark-border px-3 py-2">
      <div className="flex items-center gap-2.5">
        <vis.Icon size={14} className={`${vis.color} flex-shrink-0`} />
        <span className="text-sm text-agent-text-primary flex-1 min-w-0 truncate">{s.name}</span>
        <span className={`text-[11px] font-medium ${vis.color} flex-shrink-0`}>{vis.label}</span>
        <span className="text-[11px] text-agent-text-secondary flex-shrink-0 tabular-nums">{s.ms}ms</span>
      </div>
      {(s.note || s.model || s.tokens) && (
        <div className="mt-1 pl-[26px] text-[11px] text-agent-text-secondary">
          {s.model && <span className="font-mono">{s.model}</span>}
          {s.tokens ? <span>{s.model ? ' · ' : ''}{s.tokens} tokens</span> : null}
          {s.note ? <span>{(s.model || s.tokens) ? ' · ' : ''}{s.note}</span> : null}
        </div>
      )}
      {hasPrompt && (
        <div className="mt-1.5 pl-[26px]">
          <button onClick={() => setShowPrompt(v => !v)} className="text-[11px] text-cloudera hover:underline flex items-center gap-1">
            <Eye size={12} /> {showPrompt ? 'Hide' : 'View'} prompt &amp; response
          </button>
          {showPrompt && (
            <div className="mt-1.5 space-y-1.5">
              <div>
                <div className="text-[11px] uppercase tracking-wide text-agent-text-secondary mb-0.5">Prompt → model</div>
                <pre className="text-[11px] font-mono text-agent-text-secondary whitespace-pre-wrap bg-agent-dark-surface rounded-lg p-2 max-h-44 overflow-auto">{s.prompt}</pre>
              </div>
              <div>
                <div className="text-[11px] uppercase tracking-wide text-agent-text-secondary mb-0.5">Model response</div>
                <pre className="text-[11px] font-mono text-cloudera/90 whitespace-pre-wrap bg-agent-dark-surface rounded-lg p-2 max-h-44 overflow-auto">{s.completion}</pre>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ThinkingDots() {
  return (
    <div className="flex items-center gap-1 py-1">
      {[0, 1, 2].map(i => <span key={i} className="w-1.5 h-1.5 rounded-full bg-agent-text-secondary animate-pulse" style={{ animationDelay: `${i * 150}ms` }} />)}
    </div>
  );
}

function ThinkingLine({ text }: { text: string }) {
  return (
    <div className="flex items-center gap-2 text-xs text-agent-text-secondary">
      <div className="w-3 h-3 border-2 border-cloudera border-t-transparent rounded-full animate-spin" />
      {text}
    </div>
  );
}

function renderInline(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`|\*[^*]+\*)/g);
  return parts.map((p, i) => {
    if (p.startsWith('**') && p.endsWith('**')) return <strong key={i} className="font-semibold text-agent-text-primary">{p.slice(2, -2)}</strong>;
    if (p.startsWith('`') && p.endsWith('`')) return <code key={i} className="font-mono text-cloudera bg-agent-dark-surface px-1 py-0.5 rounded text-[0.85em]">{p.slice(1, -1)}</code>;
    if (p.startsWith('*') && p.endsWith('*')) return <em key={i} className="italic">{p.slice(1, -1)}</em>;
    return <span key={i}>{p}</span>;
  });
}
