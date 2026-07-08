import { useState, useRef, useEffect, useCallback } from 'react';
import { Radar, ArrowUp, Database, MessageSquare, GitBranch, Play, ChevronDown, ChevronRight, Table2, Sparkles, ShieldCheck, Workflow, Download } from 'lucide-react';
import { streamChat, type ChatBlock, type AssetCard, type LineageNode, type QualityCheck, type QualityTrend } from '../../api/scout';

// ── Conversation model ────────────────────────────────────────────────────────
type Turn =
  | { role: 'user'; text: string }
  | { role: 'assistant'; blocks: ChatBlock[] };

const STARTERS = [
  { icon: Database,      text: 'Find payment data' },
  { icon: GitBranch,     text: 'Where does customer_360 come from?' },
  { icon: Table2,        text: 'Top 5 merchants by amount in payment_transactions' },
  { icon: MessageSquare, text: 'What is fraud_alerts?' },
];

export function ScoutChat() {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const ctxRef = useRef<{ asset?: string; assetType?: string }>({});
  const cancelRef = useRef<(() => void) | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [turns, streaming]);

  // Clear streaming flag and drop any leftover 'thinking' block on stream end.
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
          return; // context is state, not a rendered block
        }
        setTurns(prev => {
          const next = [...prev];
          const last = next[next.length - 1];
          if (last?.role !== 'assistant') return prev;
          // 'thinking' is transient status: never keep more than the latest one,
          // and clear them all the moment a substantive block arrives.
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
    <div className="flex flex-col h-full bg-agent-dark-bg">
      {/* Header */}
      <div className="flex items-center gap-2.5 px-6 py-4 border-b border-agent-dark-border flex-shrink-0">
        <div className="w-7 h-7 rounded-lg bg-cloudera/15 flex items-center justify-center">
          <Radar size={16} className="text-cloudera" />
        </div>
        <div>
          <h2 className="text-sm font-semibold text-agent-text-primary leading-tight">Source Scout</h2>
          <p className="text-xs text-agent-text-secondary leading-tight">Ask, discover, trace, and query — across your Cloudera platform</p>
        </div>
        {ctxRef.current.asset && !empty && (
          <div className="ml-auto flex items-center gap-1.5 text-xs text-agent-text-secondary bg-agent-dark-surface border border-agent-dark-border rounded-full px-3 py-1">
            <span className="w-1.5 h-1.5 rounded-full bg-cloudera" />
            <span className="font-mono">{ctxRef.current.asset}</span>
          </div>
        )}
      </div>

      {/* Thread / empty state */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {empty ? (
          <div className="h-full flex flex-col items-center justify-center px-6 text-center">
            <div className="w-12 h-12 rounded-2xl bg-cloudera/15 flex items-center justify-center mb-5">
              <Sparkles size={22} className="text-cloudera" />
            </div>
            <h1 className="text-xl font-semibold text-agent-text-primary mb-2">Ask anything about your data</h1>
            <p className="text-sm text-agent-text-secondary max-w-md mb-7">
              Discover assets in plain English, trace lineage from OpenMetadata, and get answers by running SQL on Cloudera — all in one conversation.
            </p>
            <div className="grid grid-cols-2 gap-2.5 w-full max-w-xl">
              {STARTERS.map(s => (
                <button
                  key={s.text}
                  onClick={() => send(s.text)}
                  className="flex items-center gap-2.5 text-left text-sm text-agent-text-primary bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border hover:border-cloudera/50 rounded-xl px-4 py-3 transition-colors"
                >
                  <s.icon size={15} className="text-cloudera flex-shrink-0" />
                  <span className="truncate">{s.text}</span>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="max-w-3xl mx-auto px-6 py-6 space-y-6">
            {turns.map((turn, i) =>
              turn.role === 'user' ? (
                <div key={i} className="flex justify-end">
                  <div className="bg-cloudera text-white rounded-2xl rounded-br-md px-4 py-2.5 text-sm max-w-[80%]">
                    {turn.text}
                  </div>
                </div>
              ) : (
                <AssistantTurn key={i} blocks={turn.blocks} onAsk={send} streaming={streaming && i === turns.length - 1} />
              )
            )}
          </div>
        )}
      </div>

      {/* Composer */}
      <div className="flex-shrink-0 border-t border-agent-dark-border bg-agent-dark-bg px-6 py-4">
        <form
          onSubmit={e => { e.preventDefault(); send(input); }}
          className="max-w-3xl mx-auto flex items-end gap-2 bg-agent-dark-surface border border-agent-dark-border rounded-2xl px-4 py-2.5 focus-within:border-cloudera/60 transition-colors"
        >
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder={ctxRef.current.asset ? `Ask a follow-up about ${ctxRef.current.asset}…` : 'Ask anything about your data…'}
            disabled={streaming}
            className="flex-1 bg-transparent text-sm text-agent-text-primary placeholder-agent-text-secondary focus:outline-none py-1"
          />
          <button
            type="submit"
            disabled={!input.trim() || streaming}
            className="w-8 h-8 rounded-lg bg-cloudera hover:bg-cloudera-hover disabled:opacity-30 flex items-center justify-center transition-colors flex-shrink-0"
          >
            {streaming
              ? <div className="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
              : <ArrowUp size={16} className="text-white" />}
          </button>
        </form>
      </div>
    </div>
  );
}

// ── Assistant turn = sequence of blocks ───────────────────────────────────────
function AssistantTurn({ blocks, onAsk, streaming }: { blocks: ChatBlock[]; onAsk: (q: string) => void; streaming: boolean }) {
  return (
    <div className="flex gap-3">
      <div className="w-7 h-7 rounded-lg bg-cloudera/15 flex items-center justify-center flex-shrink-0 mt-0.5">
        <Radar size={14} className="text-cloudera" />
      </div>
      <div className="flex-1 min-w-0 space-y-3">
        {blocks.length === 0 && streaming && <ThinkingDots />}
        {blocks.map((b, i) => <BlockView key={i} block={b} onAsk={onAsk} />)}
      </div>
    </div>
  );
}

function BlockView({ block, onAsk }: { block: ChatBlock; onAsk: (q: string) => void }) {
  switch (block.type) {
    case 'thinking':   return <ThinkingLine text={block.text} />;
    case 'text':       return <TextBlock text={block.text} />;
    case 'assets':     return <AssetsBlock assets={block.assets} onAsk={onAsk} />;
    case 'lineage':    return <LineageBlock asset={block.asset} upstream={block.upstream} downstream={block.downstream} edgeCount={block.edge_count} graph={block.graph} onAsk={onAsk} />;
    case 'sql_result': return <SqlResultBlock block={block} />;
    case 'schema':     return <SchemaBlock asset={block.asset} fields={block.fields} onAsk={onAsk} />;
    case 'quality':    return <QualityBlock block={block} onAsk={onAsk} />;
    case 'pipeline':   return <PipelineBlock block={block} />;
    default:           return null;
  }
}

// ── Block renderers ───────────────────────────────────────────────────────────
function ThinkingDots() {
  return (
    <div className="flex items-center gap-1 py-1">
      {[0, 1, 2].map(i => (
        <span key={i} className="w-1.5 h-1.5 rounded-full bg-agent-text-secondary animate-pulse" style={{ animationDelay: `${i * 150}ms` }} />
      ))}
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

// Minimal markdown: **bold**, `code`, *italic*
function renderInline(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`|\*[^*]+\*)/g);
  return parts.map((p, i) => {
    if (p.startsWith('**') && p.endsWith('**')) return <strong key={i} className="font-semibold text-agent-text-primary">{p.slice(2, -2)}</strong>;
    if (p.startsWith('`') && p.endsWith('`')) return <code key={i} className="font-mono text-cloudera bg-agent-dark-surface px-1 py-0.5 rounded text-[0.85em]">{p.slice(1, -1)}</code>;
    if (p.startsWith('*') && p.endsWith('*')) return <em key={i} className="italic">{p.slice(1, -1)}</em>;
    return <span key={i}>{p}</span>;
  });
}

function TextBlock({ text }: { text: string }) {
  return <p className="text-sm text-agent-text-primary leading-relaxed">{renderInline(text)}</p>;
}

const TYPE_BADGE: Record<string, { label: string; cls: string }> = {
  iceberg_table: { label: 'Iceberg', cls: 'text-teal-300 bg-teal-500/10 border-teal-500/30' },
  kafka_topic:   { label: 'Kafka',   cls: 'text-blue-300 bg-blue-500/10 border-blue-500/30' },
};

function AssetsBlock({ assets, onAsk }: { assets: AssetCard[]; onAsk: (q: string) => void }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
      {assets.map(a => {
        const badge = TYPE_BADGE[a.asset_type] ?? { label: a.asset_type, cls: 'text-agent-text-secondary bg-agent-dark-surface border-agent-dark-border' };
        return (
          <button
            key={a.name}
            onClick={() => onAsk(a.asset_type === 'iceberg_table' ? `Show me the lineage of ${a.name}` : `What is ${a.name}?`)}
            className="text-left bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border hover:border-cloudera/50 rounded-xl p-3 transition-colors group"
          >
            <div className="flex items-center justify-between mb-1.5">
              <span className="font-mono text-sm text-agent-text-primary truncate">{a.name}</span>
              <span className={`text-xs px-1.5 py-0.5 rounded border font-semibold flex-shrink-0 ml-2 ${badge.cls}`}>{badge.label}</span>
            </div>
            <div className="text-xs text-agent-text-secondary truncate">
              {a.field_count} field{a.field_count !== 1 ? 's' : ''} · {a.fields.slice(0, 4).join(', ')}{a.fields.length > 4 ? '…' : ''}
            </div>
          </button>
        );
      })}
    </div>
  );
}

const ENTITY_STYLE: Record<string, { dot: string; badge: string; label: string }> = {
  topic:     { dot: 'bg-blue-400',   badge: 'text-blue-300 bg-blue-500/10 border-blue-500/30',   label: 'Kafka' },
  table:     { dot: 'bg-teal-400',   badge: 'text-teal-300 bg-teal-500/10 border-teal-500/30',   label: 'Table' },
  pipeline:  { dot: 'bg-orange-400', badge: 'text-orange-300 bg-orange-500/10 border-orange-500/30', label: 'Pipeline' },
  dashboard: { dot: 'bg-purple-400', badge: 'text-purple-300 bg-purple-500/10 border-purple-500/30', label: 'Dashboard' },
};

function nodeStyle(entityType: string) {
  return ENTITY_STYLE[entityType?.toLowerCase()] ?? ENTITY_STYLE.table;
}

function LineageNodeRow({ node, tone }: { node: LineageNode; tone: 'up' | 'down' }) {
  const et = (node as any).entity_type ?? (node.fqn?.startsWith('cdp_kafka') ? 'topic' : 'table');
  const s = nodeStyle(et);
  return (
    <div className="flex items-center gap-2.5 py-1">
      <span className={`w-1.5 h-1.5 rounded-full ${s.dot} flex-shrink-0`} />
      <span className="text-sm text-agent-text-primary font-mono truncate flex-1">{node.name}</span>
      <span className={`text-xs px-1.5 py-0.5 rounded border font-semibold flex-shrink-0 ${s.badge}`}>{s.label}</span>
    </div>
  );
}

function LineageBlock({ asset, upstream, downstream, edgeCount, graph, onAsk }: {
  asset: string; upstream: LineageNode[]; downstream: LineageNode[]; edgeCount: number;
  graph?: { nodes: Array<LineageNode & { depth: number; side: string; entity_type?: string }>; edges: { from: string; to: string }[] };
  onAsk: (q: string) => void;
}) {
  // Group all graph nodes by depth so we can show the full N-hop chain.
  const byDepth = new Map<number, NonNullable<typeof graph>['nodes']>();
  for (const n of graph?.nodes ?? []) {
    if (!byDepth.has(n.depth)) byDepth.set(n.depth, []);
    byDepth.get(n.depth)!.push(n);
  }
  const allDepths   = Array.from(byDepth.keys()).sort((a, b) => a - b);
  const upDepths    = allDepths.filter(d => d < 0).reverse(); // most-distant first (top of card)
  const downDepths  = allDepths.filter(d => d > 0);
  const useGraph    = (graph?.nodes?.length ?? 0) > 0;

  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border">
        <GitBranch size={14} className="text-cloudera" />
        <span className="text-xs font-semibold text-agent-text-primary uppercase tracking-wider">Lineage</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{edgeCount} edge{edgeCount !== 1 ? 's' : ''}</span>
      </div>

      {useGraph ? (
        // ── N-hop pipeline flow ────────────────────────────────────────────
        <div className="px-4 py-3 space-y-1">
          {upDepths.map((d, di) => {
            const nodes = byDepth.get(d)!;
            const hopLabel = Math.abs(d) === 1 ? 'direct source' : `${Math.abs(d)} hops up`;
            return (
              <div key={d}>
                <div className="text-xs text-blue-300/50 mb-1 ml-0.5">{hopLabel}</div>
                <div className="pl-2 border-l-2 border-blue-500/20 space-y-0.5">
                  {nodes.map((n, i) => (
                    <div key={i} className="flex items-center gap-2 py-0.5">
                      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${nodeStyle(n.entity_type ?? '').dot}`} />
                      <span className="font-mono text-sm text-agent-text-primary truncate flex-1">{n.name}</span>
                      <span className={`text-xs px-1.5 py-0.5 rounded border font-semibold flex-shrink-0 ${nodeStyle(n.entity_type ?? '').badge}`}>
                        {nodeStyle(n.entity_type ?? '').label}
                      </span>
                    </div>
                  ))}
                </div>
                {/* connector arrow */}
                <div className="flex items-center ml-2 my-1">
                  <ChevronDown size={12} className="text-agent-dark-border" />
                </div>
              </div>
            );
          })}

          {/* Current asset */}
          <div className="rounded-lg bg-cloudera/15 border border-cloudera/40 px-3 py-2 flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-cloudera flex-shrink-0" />
            <span className="font-mono text-sm font-semibold text-cloudera flex-1">{asset.split('.').pop()}</span>
            <span className="text-xs text-agent-text-secondary">current</span>
          </div>

          {downDepths.map(d => {
            const nodes = byDepth.get(d)!;
            const hopLabel = d === 1 ? 'direct consumer' : `${d} hops down`;
            return (
              <div key={d}>
                <div className="flex items-center ml-2 my-1">
                  <ChevronDown size={12} className="text-agent-dark-border" />
                </div>
                <div className="text-xs text-green-300/50 mb-1 ml-0.5">{hopLabel}</div>
                <div className="pl-2 border-l-2 border-green-500/20 space-y-0.5">
                  {nodes.map((n, i) => (
                    <div key={i} className="flex items-center gap-2 py-0.5">
                      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${nodeStyle(n.entity_type ?? '').dot}`} />
                      <span className="font-mono text-sm text-agent-text-primary truncate flex-1">{n.name}</span>
                      <span className={`text-xs px-1.5 py-0.5 rounded border font-semibold flex-shrink-0 ${nodeStyle(n.entity_type ?? '').badge}`}>
                        {nodeStyle(n.entity_type ?? '').label}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
          {downDepths.length === 0 && upDepths.length > 0 && (
            <div className="text-xs text-agent-text-secondary mt-1 ml-0.5">No downstream consumers</div>
          )}
        </div>
      ) : (
        // ── Fallback: flat 1-hop 3-column layout ──────────────────────────
        <div className="p-4 grid grid-cols-[1fr_auto_1fr] gap-4 items-center">
          <div>
            <div className="text-xs font-semibold text-blue-300/80 uppercase tracking-wider mb-1">Upstream ({upstream.length})</div>
            {upstream.length ? upstream.map((n, i) => <LineageNodeRow key={i} node={n} tone="up" />)
              : <div className="text-xs text-agent-text-secondary py-1.5">— none —</div>}
          </div>
          <div className="flex flex-col items-center">
            <div className="px-3 py-2 rounded-lg bg-cloudera/15 border border-cloudera/40 text-center">
              <div className="text-xs font-mono font-semibold text-cloudera whitespace-nowrap">{asset.split('.').pop()}</div>
              <div className="text-xs text-agent-text-secondary">this asset</div>
            </div>
          </div>
          <div>
            <div className="text-xs font-semibold text-green-300/80 uppercase tracking-wider mb-1">Downstream ({downstream.length})</div>
            {downstream.length ? downstream.map((n, i) => <LineageNodeRow key={i} node={n} tone="down" />)
              : <div className="text-xs text-agent-text-secondary py-1.5">— none —</div>}
          </div>
        </div>
      )}

      <div className="px-4 pb-3 pt-2 flex gap-3 border-t border-agent-dark-border">
        <button onClick={() => onAsk(`Run a query on ${asset}`)} className="text-xs text-cloudera hover:underline">Query →</button>
        <button onClick={() => onAsk(`Check data quality of ${asset}`)} className="text-xs text-cloudera hover:underline">Quality →</button>
      </div>
    </div>
  );
}

function SqlResultBlock({ block }: { block: Extract<ChatBlock, { type: 'sql_result' }> }) {
  const [showSql, setShowSql] = useState(false);
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      {/* SQL toggle */}
      <button onClick={() => setShowSql(s => !s)}
        className="w-full flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border hover:bg-agent-dark-border/40 transition-colors">
        {showSql ? <ChevronDown size={14} className="text-agent-text-secondary" /> : <ChevronRight size={14} className="text-agent-text-secondary" />}
        <Play size={12} className="text-green-400" />
        <span className="text-xs font-semibold text-agent-text-primary">SQL</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{block.executed_on ? `ran on ${block.executed_on}` : ''}</span>
      </button>
      {showSql && (
        <pre className="px-4 py-3 text-xs font-mono text-[#9ab8cc] whitespace-pre-wrap border-b border-agent-dark-border bg-agent-dark-bg overflow-x-auto">{block.sql}</pre>
      )}
      {block.error ? (
        <div className="px-4 py-3 text-xs text-red-300">{block.error}</div>
      ) : block.columns.length === 0 ? (
        <div className="px-4 py-3 text-xs text-agent-text-secondary">No rows.</div>
      ) : (
        <div className="overflow-x-auto max-h-80 overflow-y-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-agent-dark-surface">
              <tr>{block.columns.map((c, i) => (
                <th key={i} className="text-left px-4 py-2 font-semibold text-cloudera border-b border-agent-dark-border whitespace-nowrap">{c}</th>
              ))}</tr>
            </thead>
            <tbody>
              {block.rows.map((row, ri) => (
                <tr key={ri} className="border-b border-agent-dark-border/40 hover:bg-agent-dark-border/30">
                  {row.map((cell, ci) => (
                    <td key={ci} className="px-4 py-1.5 font-mono text-agent-text-primary whitespace-nowrap">
                      {cell === null ? <span className="text-agent-text-secondary italic">null</span> : cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function SchemaBlock({ asset, fields, onAsk }: { asset: string; fields: Array<{ name: string; type?: string }>; onAsk: (q: string) => void }) {
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border">
        <Table2 size={14} className="text-cloudera" />
        <span className="text-xs font-semibold text-agent-text-primary uppercase tracking-wider">Schema</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{fields.length} fields</span>
      </div>
      <div className="p-3 grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-1.5">
        {fields.map((f, i) => (
          <div key={i} className="flex items-baseline gap-2 min-w-0">
            <span className="text-xs font-mono text-agent-text-primary truncate">{f.name}</span>
            <span className="text-xs text-agent-text-secondary ml-auto flex-shrink-0">{f.type ?? ''}</span>
          </div>
        ))}
      </div>
      <div className="px-4 pb-3 flex gap-3">
        <button onClick={() => onAsk(`Show me the lineage of ${asset}`)} className="text-xs text-cloudera hover:underline">Lineage →</button>
        <button onClick={() => onAsk(`Show 10 rows from ${asset}`)} className="text-xs text-cloudera hover:underline">Sample rows →</button>
      </div>
    </div>
  );
}

function PipelineBlock({ block }: { block: Extract<ChatBlock, { type: 'pipeline' }> }) {
  const download = () => {
    const blob = new Blob([JSON.stringify(block.flow, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${block.flow_name}.flow.json`;
    a.click();
    URL.revokeObjectURL(url);
  };
  const sinkLabel = block.sink.table ? `${block.sink.type} · ${block.sink.table}` : block.sink.type;
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border">
        <Workflow size={14} className="text-orange-400" />
        <span className="text-xs font-semibold text-agent-text-primary uppercase tracking-wider">NiFi pipeline</span>
        <span className="text-xs text-agent-text-secondary ml-auto">{block.processors.length} processors · {block.connection_count} connections</span>
      </div>
      <div className="px-4 py-3 space-y-3">
        {/* source → sink flow */}
        <div className="flex items-center gap-2 text-xs">
          <span className="px-2 py-1 rounded font-mono text-blue-300 bg-blue-500/10 border border-blue-500/30 truncate">{block.source.name}</span>
          <ChevronRight size={14} className="text-agent-text-secondary flex-shrink-0" />
          <span className="px-2 py-1 rounded font-mono text-teal-300 bg-teal-500/10 border border-teal-500/30 truncate">{sinkLabel}</span>
        </div>
        {/* processor chain */}
        <div className="flex flex-wrap items-center gap-1">
          {block.processors.map((p, i) => (
            <span key={i} className="flex items-center gap-1">
              <span className="text-xs font-mono text-agent-text-primary bg-agent-dark border border-agent-dark-border rounded px-1.5 py-0.5">{p}</span>
              {i < block.processors.length - 1 && <ChevronRight size={12} className="text-agent-text-secondary" />}
            </span>
          ))}
        </div>
        {/* parameters to fill */}
        {block.parameters_to_fill.length > 0 && (
          <div className="space-y-1">
            <div className="text-xs text-agent-text-secondary">Fill in before starting in NiFi:</div>
            <div className="flex flex-wrap gap-1">
              {block.parameters_to_fill.map((p, i) => (
                <span key={i} className={`text-xs font-mono px-1.5 py-0.5 rounded border ${p.sensitive ? 'text-red-300 bg-red-500/10 border-red-500/30' : 'text-amber-300 bg-amber-500/10 border-amber-500/30'}`} title={p.description}>
                  {p.name}{p.sensitive ? ' 🔒' : ''}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
      <div className="px-4 pb-3">
        <button onClick={download} className="flex items-center gap-1.5 text-xs text-cloudera hover:underline">
          <Download size={13} /> Download {block.flow_name}.flow.json
        </button>
      </div>
    </div>
  );
}

// Inline data-quality insight — rendered both for an explicit "check quality" ask and
// for the ambient signal that travels with a query answer. Same card, one renderer.
function QualityBlock({ block, onAsk }: { block: Extract<ChatBlock, { type: 'quality' }>; onAsk: (q: string) => void }) {
  const score = block.overall_score ?? 0;
  const c = block.counts || { pass: 0, warn: 0, fail: 0 };
  const scoreCls = score >= 90 ? 'text-green-400' : score >= 75 ? 'text-amber-400' : 'text-red-400';
  const issues: QualityCheck[] = (block.checks || [])
    .filter(ch => ch.status !== 'pass')
    .sort((a, b) => (b.metric_value ?? 0) - (a.metric_value ?? 0))
    .slice(0, 3);
  const t: QualityTrend | null = block.trend;
  return (
    <div className="border border-agent-dark-border rounded-xl overflow-hidden bg-agent-dark-surface">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b border-agent-dark-border">
        <ShieldCheck size={14} className="text-agent-teal" />
        <span className="text-xs font-semibold text-agent-text-primary uppercase tracking-wider">Data quality</span>
        {block.ambient && <span className="text-xs text-agent-text-secondary">· auto-checked</span>}
        <span className={`ml-auto text-base font-bold ${scoreCls}`}>{score}<span className="text-xs text-agent-text-secondary">/100</span></span>
      </div>
      <div className="px-4 py-3 space-y-2">
        <div className="flex gap-3 text-xs text-agent-text-secondary">
          <span className="text-green-400">{c.pass} pass</span>
          <span className="text-amber-400">{c.warn} warn</span>
          <span className="text-red-400">{c.fail} fail</span>
          {typeof block.total_rows === 'number' && <span>· {block.total_rows.toLocaleString()} rows</span>}
        </div>
        {issues.length > 0 ? (
          <div className="space-y-1">
            {issues.map((ch, i) => (
              <div key={i} className="flex items-center justify-between text-xs gap-2">
                <span className="text-agent-text-primary truncate"><span className="font-mono">{ch.column}</span> · {ch.check}</span>
                <span className="flex items-center gap-2 text-agent-text-secondary flex-shrink-0">{ch.label}
                  <span className={`px-1.5 py-0.5 rounded text-xs font-bold uppercase ${ch.status === 'fail' ? 'bg-red-500/15 text-red-400' : 'bg-amber-500/15 text-amber-400'}`}>{ch.status}</span>
                </span>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-xs text-green-400">All checks pass — no completeness or uniqueness issues.</div>
        )}
        {t && t.direction === 'down' && (
          <div className="text-xs text-amber-400">↓ Trending down {t.baseline}→{t.current} over {t.window_days}d · {t.driver}</div>
        )}
        {block.root_cause && (
          <div className="text-xs text-agent-text-secondary">Likely upstream cause: <span className="font-mono text-agent-text-primary">{block.root_cause.asset}</span> ({block.root_cause.delta})</div>
        )}
        <div className="flex items-center gap-3 pt-1">
          {block.ambient && (
            <button onClick={() => onAsk(`Check data quality of ${block.asset}`)} className="text-xs text-cloudera hover:underline">Full quality check →</button>
          )}
          {block.written_to_om && <span className="text-xs text-agent-text-secondary">✓ written to OpenMetadata</span>}
        </div>
      </div>
    </div>
  );
}
