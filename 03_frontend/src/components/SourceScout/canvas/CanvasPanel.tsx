import { useState } from 'react';
import { GitBranch, Play, ChevronDown, ChevronRight, Table2, Database, Radio, Compass, ExternalLink, ShieldCheck, TrendingDown, TrendingUp, AlertTriangle } from 'lucide-react';
import { LineageGraph } from './LineageGraph';
import type { ChatBlock, AssetCard, QualityCheck, QualityTrend } from '../../../api/scout';

export type Artifact = Extract<ChatBlock, { type: 'lineage' | 'sql_result' | 'schema' | 'assets' | 'quality' }>;

const OM_BASE = 'http://localhost:8585';

export function CanvasPanel({ artifact, onAsk }: { artifact: Artifact | null; onAsk: (q: string) => void }) {
  if (!artifact) return <EmptyCanvas />;
  switch (artifact.type) {
    case 'lineage':    return <LineageCanvas a={artifact} onAsk={onAsk} />;
    case 'sql_result': return <ResultCanvas a={artifact} />;
    case 'schema':     return <SchemaCanvas a={artifact} onAsk={onAsk} />;
    case 'assets':     return <AssetsCanvas a={artifact} onAsk={onAsk} />;
    case 'quality':    return <QualityCanvas a={artifact} onAsk={onAsk} />;
  }
}

const SCORE_COLOR = (s: number) => s >= 90 ? 'text-green-400' : s >= 75 ? 'text-amber-400' : 'text-red-400';
const STATUS_STYLE: Record<string, string> = {
  pass: 'text-green-300 bg-green-500/10 border-green-500/30',
  warn: 'text-amber-300 bg-amber-500/10 border-amber-500/30',
  fail: 'text-red-300 bg-red-500/10 border-red-500/30',
};

function Sparkline({ points }: { points: number[] }) {
  if (!points || points.length < 2) return null;
  const w = 160, h = 36, pad = 3;
  const min = Math.min(...points), max = Math.max(...points);
  const range = max - min || 1;
  const xs = (i: number) => pad + (i / (points.length - 1)) * (w - 2 * pad);
  const ys = (v: number) => pad + (1 - (v - min) / range) * (h - 2 * pad);
  const d = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${xs(i).toFixed(1)} ${ys(p).toFixed(1)}`).join(' ');
  const down = points[points.length - 1] < points[0];
  const stroke = down ? '#f87171' : '#4ade80';
  return (
    <svg width={w} height={h} className="overflow-visible">
      <path d={d} fill="none" stroke={stroke} strokeWidth={1.5} />
      <circle cx={xs(points.length - 1)} cy={ys(points[points.length - 1])} r={2.5} fill={stroke} />
    </svg>
  );
}

function QualityCanvas({ a, onAsk }: { a: Extract<ChatBlock, { type: 'quality' }>; onAsk: (q: string) => void }) {
  const t = a.trend;
  const failing = a.checks.filter(c => c.status !== 'pass');
  const passing = a.checks.filter(c => c.status === 'pass');
  return (
    <CanvasFrame
      icon={<ShieldCheck size={18} className="text-cloudera" />}
      title={`Quality · ${a.asset}`}
      sub={`${a.total_rows} rows · ${a.counts.pass} pass · ${a.counts.warn} warn · ${a.counts.fail} fail`}
      right={a.written_to_om ? <span className="text-xs text-green-400 flex items-center gap-1"><ShieldCheck size={12} /> in OpenMetadata</span> : undefined}
    >
      <div className="p-4 space-y-4">
        {/* Score + trend header */}
        <div className="flex items-center gap-5 p-4 rounded-xl bg-agent-dark-surface border border-agent-dark-border">
          <div>
            <div className={`text-4xl font-bold ${SCORE_COLOR(a.overall_score)}`}>{a.overall_score}</div>
            <div className="text-xs text-agent-text-secondary">overall / 100</div>
          </div>
          {t && (
            <div className="flex-1">
              <div className="flex items-center gap-1.5 mb-1">
                {t.direction === 'down'
                  ? <TrendingDown size={14} className="text-red-400" />
                  : t.direction === 'up' ? <TrendingUp size={14} className="text-green-400" /> : null}
                <span className={`text-sm font-semibold ${t.direction === 'down' ? 'text-red-300' : t.direction === 'up' ? 'text-green-300' : 'text-agent-text-secondary'}`}>
                  {t.direction === 'down' ? 'Degrading' : t.direction === 'up' ? 'Improving' : 'Stable'} · {t.baseline}→{t.current} over {t.window_days}d
                </span>
              </div>
              <Sparkline points={t.points} />
              {t.driver && <div className="text-xs text-agent-text-secondary mt-1">Driver: {t.driver}</div>}
            </div>
          )}
        </div>

        {/* Root cause */}
        {a.root_cause && (
          <div className="flex items-start gap-2.5 p-3 rounded-xl bg-red-950/30 border border-red-800/40">
            <AlertTriangle size={15} className="text-red-400 mt-0.5 flex-shrink-0" />
            <div className="text-xs text-red-200">
              <span className="font-semibold">Likely root cause is upstream.</span> {a.root_cause.asset} is also degrading
              ({a.root_cause.delta} over the window){a.root_cause.driver ? ` — ${a.root_cause.driver}` : ''}.
              <button onClick={() => onAsk(`Show me the lineage of ${a.asset}`)} className="ml-1 text-cloudera hover:underline">View lineage →</button>
            </div>
          </div>
        )}

        {/* Failing/warn checks first */}
        {failing.length > 0 && (
          <div>
            <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-2">Needs attention ({failing.length})</div>
            <div className="space-y-1.5">
              {failing.map((c, i) => <CheckRow key={i} c={c} />)}
            </div>
          </div>
        )}
        {/* Passing checks */}
        {passing.length > 0 && (
          <div>
            <div className="text-xs font-semibold text-agent-text-secondary uppercase tracking-wider mb-2">Passing ({passing.length})</div>
            <div className="space-y-1.5">
              {passing.map((c, i) => <CheckRow key={i} c={c} />)}
            </div>
          </div>
        )}
      </div>
    </CanvasFrame>
  );
}

function CheckRow({ c }: { c: QualityCheck }) {
  return (
    <div className="flex items-center gap-3 px-3 py-2 rounded-lg bg-agent-dark-surface border border-agent-dark-border">
      <span className={`text-xs px-1.5 py-0.5 rounded border font-semibold uppercase ${STATUS_STYLE[c.status]}`}>{c.status}</span>
      <span className="text-sm font-mono text-agent-text-primary">{c.column}</span>
      <span className="text-xs text-agent-text-secondary ml-auto">{c.check} · {c.label}</span>
    </div>
  );
}

function CanvasFrame({ icon, title, sub, right, children }: {
  icon: React.ReactNode; title: string; sub?: string; right?: React.ReactNode; children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-2.5 px-6 py-4 border-b border-agent-dark-border flex-shrink-0">
        {icon}
        <div className="min-w-0">
          <div className="text-sm font-semibold text-agent-text-primary truncate">{title}</div>
          {sub && <div className="text-xs text-agent-text-secondary truncate">{sub}</div>}
        </div>
        {right && <div className="ml-auto flex items-center gap-2">{right}</div>}
      </div>
      <div className="flex-1 min-h-0 overflow-auto">{children}</div>
    </div>
  );
}

function EmptyCanvas() {
  return (
    <div className="h-full flex flex-col items-center justify-center text-center px-8">
      <div className="w-14 h-14 rounded-2xl bg-agent-dark-surface border border-agent-dark-border flex items-center justify-center mb-4">
        <Compass size={24} className="text-agent-text-secondary" />
      </div>
      <p className="text-sm font-medium text-agent-text-primary">Your canvas</p>
      <p className="text-xs text-agent-text-secondary mt-1 max-w-xs">
        Ask a question on the left. Lineage graphs, query results, and schemas open here — explorable, not buried in chat.
      </p>
    </div>
  );
}

function LineageCanvas({ a, onAsk }: { a: Extract<ChatBlock, { type: 'lineage' }>; onAsk: (q: string) => void }) {
  const fqn = `cdp_hive.demo.default.${a.asset.split('.').pop()}`;
  return (
    <CanvasFrame
      icon={<GitBranch size={18} className="text-cloudera" />}
      title={`Lineage · ${a.asset}`}
      sub={`${a.upstream.length} upstream · ${a.downstream.length} downstream · click a node to expand a hop`}
      right={
        <a href={`${OM_BASE}/table/${fqn}/lineage`} target="_blank" rel="noreferrer"
          className="flex items-center gap-1.5 text-xs text-cloudera hover:underline">
          OpenMetadata <ExternalLink size={12} />
        </a>
      }
    >
      <div className="h-full p-4">
        <LineageGraph asset={a.asset} graph={a.graph} upstream={a.upstream} downstream={a.downstream}
          onNodeClick={(name) => onAsk(`Show me the lineage of ${name}`)} />
      </div>
    </CanvasFrame>
  );
}

function ResultCanvas({ a }: { a: Extract<ChatBlock, { type: 'sql_result' }> }) {
  const [showSql, setShowSql] = useState(true);
  return (
    <CanvasFrame
      icon={<Play size={16} className="text-green-400" />}
      title={`Query result · ${a.asset}`}
      sub={a.error ? 'failed' : `${a.row_count ?? a.rows.length} rows · ran on ${a.executed_on ?? 'impala'}`}
    >
      <div className="p-4 space-y-3">
        <div className="border border-agent-dark-border rounded-xl overflow-hidden">
          <button onClick={() => setShowSql(s => !s)}
            className="w-full flex items-center gap-2 px-4 py-2.5 hover:bg-agent-dark-border/40 transition-colors">
            {showSql ? <ChevronDown size={14} className="text-agent-text-secondary" /> : <ChevronRight size={14} className="text-agent-text-secondary" />}
            <span className="text-xs font-semibold text-agent-text-primary">SQL</span>
          </button>
          {showSql && (
            <pre className="px-4 py-3 text-xs font-mono text-[#9ab8cc] whitespace-pre-wrap border-t border-agent-dark-border bg-agent-dark-bg overflow-x-auto">{a.sql}</pre>
          )}
        </div>

        {a.error ? (
          <div className="p-3 bg-red-950/30 border border-red-800/40 rounded-xl text-xs text-red-200">{a.error}</div>
        ) : a.columns.length === 0 ? (
          <div className="p-3 text-xs text-agent-text-secondary">No rows returned.</div>
        ) : (
          <div className="border border-agent-dark-border rounded-xl overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-agent-dark-surface">
                <tr>{a.columns.map((c, i) => (
                  <th key={i} className="text-left px-4 py-2.5 font-semibold text-cloudera border-b border-agent-dark-border whitespace-nowrap">{c}</th>
                ))}</tr>
              </thead>
              <tbody>
                {a.rows.map((row, ri) => (
                  <tr key={ri} className="border-b border-agent-dark-border/40 hover:bg-agent-dark-border/30">
                    {row.map((cell, ci) => (
                      <td key={ci} className="px-4 py-2 font-mono text-agent-text-primary whitespace-nowrap">
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
    </CanvasFrame>
  );
}

function SchemaCanvas({ a, onAsk }: { a: Extract<ChatBlock, { type: 'schema' }>; onAsk: (q: string) => void }) {
  const isTopic = a.asset_type === 'kafka_topic';
  return (
    <CanvasFrame
      icon={isTopic ? <Radio size={16} className="text-blue-400" /> : <Database size={16} className="text-teal-400" />}
      title={`Schema · ${a.asset}`}
      sub={`${a.fields.length} fields · ${isTopic ? 'Kafka topic' : 'Iceberg table'}`}
      right={
        <button onClick={() => onAsk(`Show me the lineage of ${a.asset}`)}
          className="text-xs text-cloudera hover:underline flex items-center gap-1">
          <GitBranch size={12} /> Lineage
        </button>
      }
    >
      <div className="p-4">
        <div className="border border-agent-dark-border rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-agent-dark-surface">
              <tr>
                <th className="text-left px-4 py-2.5 font-semibold text-agent-text-secondary border-b border-agent-dark-border">Field</th>
                <th className="text-left px-4 py-2.5 font-semibold text-agent-text-secondary border-b border-agent-dark-border">Type</th>
              </tr>
            </thead>
            <tbody>
              {a.fields.map((f, i) => (
                <tr key={i} className="border-b border-agent-dark-border/40 hover:bg-agent-dark-border/30">
                  <td className="px-4 py-2 font-mono text-agent-text-primary">{f.name}</td>
                  <td className="px-4 py-2 font-mono text-xs text-agent-text-secondary">{f.type ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </CanvasFrame>
  );
}

const TYPE_BADGE: Record<string, string> = {
  iceberg_table: 'text-teal-300 bg-teal-500/10 border-teal-500/30',
  kafka_topic:   'text-blue-300 bg-blue-500/10 border-blue-500/30',
};

function AssetsCanvas({ a, onAsk }: { a: Extract<ChatBlock, { type: 'assets' }>; onAsk: (q: string) => void }) {
  return (
    <CanvasFrame
      icon={<Compass size={16} className="text-cloudera" />}
      title="Discovered assets"
      sub={`${a.assets.length} match${a.assets.length !== 1 ? 'es' : ''} · click to explore`}
    >
      <div className="p-4 grid grid-cols-1 lg:grid-cols-2 gap-2.5">
        {a.assets.map((asset: AssetCard) => {
          const badge = TYPE_BADGE[asset.asset_type] ?? 'text-agent-text-secondary bg-agent-dark-surface border-agent-dark-border';
          const label = asset.asset_type === 'kafka_topic' ? 'Kafka' : 'Iceberg';
          return (
            <button key={asset.name}
              onClick={() => onAsk(asset.asset_type === 'iceberg_table' ? `Show me the lineage of ${asset.name}` : `What is ${asset.name}?`)}
              className="text-left bg-agent-dark-surface hover:bg-agent-dark-border border border-agent-dark-border hover:border-cloudera/50 rounded-xl p-3.5 transition-colors">
              <div className="flex items-center justify-between mb-1.5">
                <span className="font-mono text-sm text-agent-text-primary truncate">{asset.name}</span>
                <span className={`text-xs px-1.5 py-0.5 rounded border font-semibold flex-shrink-0 ml-2 ${badge}`}>{label}</span>
              </div>
              <div className="text-xs text-agent-text-secondary truncate">
                {asset.field_count} fields · {asset.fields.slice(0, 5).join(', ')}{asset.fields.length > 5 ? '…' : ''}
              </div>
              {asset.reason && (
                <div className="mt-2 pt-2 border-t border-agent-dark-border text-xs text-cloudera/90 line-clamp-2">
                  {asset.reason}
                </div>
              )}
            </button>
          );
        })}
      </div>
    </CanvasFrame>
  );
}
