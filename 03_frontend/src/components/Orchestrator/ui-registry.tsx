/**
 * Inline UI component registry — AgenticGenUI pattern adapted for our SSE stream.
 *
 * Instead of asking the LLM to emit {"componentType","props"} JSON (the original
 * approach), we derive the component type directly from the SSE event.type that our
 * agents already emit.  The registry maps event types → React components, so every
 * agent "speaks" rich UI without changing a line of backend code.
 *
 * Registry:
 *   discovered_assets  → AssetGrid    (grouped by type, clickable)
 *   quality_scorecard  → QualityCard  (score ring + per-check table)
 *   schema_view        → SchemaView   (column type pills)
 *   pipeline_view      → PipelineView (flow diagram steps + params)
 *   analyst_answer     → AnswerCard   (grounded text + SQL reveal)
 *   health_view        → HealthView   (status badge + metrics)
 */

import { useState } from 'react';
import {
  Check, X, AlertTriangle, Table2, Radio, HardDrive, Database,
  ChevronDown, ChevronRight, FileCode2, HeartPulse, Shield, Layers,
  User, Tag as TagIcon, GitBranch,
} from 'lucide-react';

// ── Shared mini-primitives ────────────────────────────────────────────────────

function Tag({ children, color = 'default' }: { children: React.ReactNode; color?: 'teal' | 'orange' | 'rose' | 'amber' | 'sky' | 'default' }) {
  const cls = {
    teal: 'bg-agent-teal/10 text-agent-teal border-agent-teal/25',
    orange: 'bg-agent-orange/10 text-agent-orange border-agent-orange/25',
    rose: 'bg-rose-500/10 text-rose-400 border-rose-500/25',
    amber: 'bg-amber-500/10 text-amber-400 border-amber-500/25',
    sky: 'bg-sky-400/10 text-sky-400 border-sky-400/25',
    default: 'bg-white/[0.04] text-agent-text-secondary border-white/[0.08]',
  }[color];
  return <span className={`text-xs font-mono px-2 py-0.5 rounded-full border ${cls}`}>{children}</span>;
}

function Section({ title, icon: Icon, children }: { title: string; icon?: typeof Shield; children: React.ReactNode }) {
  return (
    <div>
      {title && (
        <div className="flex items-center gap-1.5 text-xs uppercase tracking-wider text-agent-text-secondary mb-2">
          {Icon && <Icon size={12} />} {title}
        </div>
      )}
      {children}
    </div>
  );
}

// ── Score ring (SVG) ─────────────────────────────────────────────────────────

function ScoreRing({ score, size = 64 }: { score: number; size?: number }) {
  const r = size * 0.38;
  const circ = 2 * Math.PI * r;
  const arc = (score / 100) * circ;
  const color = score >= 90 ? '#00A3C4' : score >= 70 ? '#F59E0B' : '#F87171';
  return (
    <svg width={size} height={size} style={{ transform: 'rotate(-90deg)' }}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth={size * 0.1} />
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={color} strokeWidth={size * 0.1}
        strokeDasharray={`${arc} ${circ - arc}`} strokeLinecap="round" />
      <text x="50%" y="50%" textAnchor="middle" dominantBaseline="middle" fill={color}
        fontSize={size * 0.26} fontWeight="700" style={{ transform: 'rotate(90deg)', transformOrigin: '50% 50%' }}>
        {score}
      </text>
    </svg>
  );
}

// ── Bar (for completeness / distinctness metrics) ────────────────────────────

function Bar({ value, max = 100, color }: { value: number; max?: number; color?: string }) {
  const pct = Math.min(100, (value / max) * 100);
  const bg = color || (pct >= 90 ? '#00A3C4' : pct >= 70 ? '#F59E0B' : '#F87171');
  return (
    <div className="relative h-1.5 rounded-full bg-white/[0.06] overflow-hidden">
      <div className="absolute left-0 top-0 bottom-0 rounded-full transition-all" style={{ width: `${pct}%`, background: bg }} />
    </div>
  );
}

// ── 1. Discovered assets ───────────────────────────────────────────────────────

type AssetItem = { name: string; asset_type: string; field_count: number; columns?: { name: string; type?: string }[] };

const TYPE_META: Record<string, { label: string; Icon: typeof Table2; color: string; accent: 'teal' | 'orange' | 'sky' }> = {
  iceberg_table: { label: 'Iceberg tables', Icon: Table2, color: 'text-agent-teal', accent: 'teal' },
  kafka_topic: { label: 'Kafka topics', Icon: Radio, color: 'text-agent-orange', accent: 'orange' },
  ozone_volume: { label: 'Ozone volumes', Icon: HardDrive, color: 'text-sky-400', accent: 'sky' },
};

export function AssetGrid({ assets, onPick, focus }: { assets: AssetItem[]; onPick: (n: string) => void; focus?: string }) {
  const groups: Record<string, AssetItem[]> = {};
  assets.forEach((a) => { (groups[a.asset_type] = groups[a.asset_type] || []).push(a); });
  const order = Object.keys(groups).sort();
  const total = assets.length;

  return (
    <div className="rounded-2xl border border-white/[0.08] bg-agent-dark-surface/60 p-4 space-y-4">
      <div className="text-sm text-agent-text-secondary">
        Found <span className="font-semibold text-agent-text-primary">{total}</span> assets — tap one to inspect it or ask questions.
      </div>
      {order.map((type) => {
        const meta = TYPE_META[type] || { label: type, Icon: Database, color: 'text-agent-text-secondary', accent: 'default' };
        return (
          <Section key={type} title={`${meta.label} · ${groups[type].length}`} icon={meta.Icon}>
            <div className="flex flex-wrap gap-2">
              {groups[type].map((a) => (
                <button key={a.name} onClick={() => onPick(a.name)}
                  className={`text-left px-3.5 py-2.5 rounded-xl border transition-all ${a.name === focus
                    ? 'border-agent-teal/50 bg-agent-teal/[0.08]'
                    : 'border-white/[0.08] bg-white/[0.02] hover:border-white/20 hover:bg-white/[0.04]'}`}>
                  <div className="text-sm font-medium">{a.name}</div>
                  <div className="text-xs text-agent-text-secondary mt-0.5">{a.field_count} columns</div>
                </button>
              ))}
            </div>
          </Section>
        );
      })}
    </div>
  );
}

// ── 2. Quality scorecard ──────────────────────────────────────────────────────

type Check = { check?: string; column?: string; label?: string; status?: string; metric_value?: number };

function statusIcon(s?: string) {
  if (s === 'pass') return <Check size={13} className="text-agent-teal" />;
  if (s === 'fail') return <X size={13} className="text-rose-400" />;
  return <AlertTriangle size={13} className="text-amber-400" />;
}
function statusColor(s?: string): 'teal' | 'rose' | 'amber' {
  return s === 'pass' ? 'teal' : s === 'fail' ? 'rose' : 'amber';
}

export function QualityCard({ asset, overall_score, counts, checks, total_rows, om_owner, om_tier, om_tags }:
  { asset: string; overall_score: number; counts: Record<string, number>; checks: Check[];
    total_rows?: number; om_owner?: string; om_tier?: string; om_tags?: string[] }) {
  const [open, setOpen] = useState(false);
  const scoreColor = overall_score >= 90 ? 'teal' : overall_score >= 70 ? 'amber' : 'rose';
  return (
    <div className="rounded-2xl border border-white/[0.08] bg-agent-dark-surface/60 p-4 space-y-3">
      <div className="flex items-start gap-4">
        <ScoreRing score={overall_score} size={72} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <Shield size={15} className="text-agent-text-secondary" />
            <span className="text-xs uppercase tracking-wider text-agent-text-secondary">Quality</span>
            <Tag color={scoreColor}>{overall_score >= 90 ? 'Excellent' : overall_score >= 70 ? 'Good' : 'Needs attention'}</Tag>
          </div>
          <div className="text-sm font-semibold truncate">{asset}</div>
          {total_rows != null && <div className="text-xs text-agent-text-secondary mt-0.5">{total_rows.toLocaleString()} rows scanned</div>}
          <div className="flex gap-3 mt-2">
            {Object.entries(counts || {}).map(([k, v]) => (
              <div key={k} className="flex items-center gap-1 text-xs">
                {statusIcon(k)}
                <span className="text-agent-text-secondary">{v} {k}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* OM classification strip — shown when the guardian enriched from OpenMetadata */}
      {(om_owner || om_tier || (om_tags && om_tags.length > 0)) && (
        <div className="flex flex-wrap items-center gap-2 pt-2 border-t border-white/[0.06]">
          <TagIcon size={12} className="text-violet-400 shrink-0" />
          {om_owner && (
            <span className="flex items-center gap-1 text-xs text-agent-text-secondary">
              <User size={11} />{om_owner}
            </span>
          )}
          {om_tier && <Tag color="default">{om_tier}</Tag>}
          {(om_tags || []).filter(t => !t?.includes('Tier')).slice(0, 5).map(t => (
            <span key={t} className="text-xs px-2 py-0.5 rounded-full bg-violet-500/10 border border-violet-500/20 text-violet-300 font-mono">{t}</span>
          ))}
        </div>
      )}

      {checks?.length > 0 && (
        <>
          <button onClick={() => setOpen((o) => !o)}
            className="flex items-center gap-1 text-xs text-agent-text-secondary hover:text-agent-text-primary transition-colors">
            {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {open ? 'Hide' : 'Show'} {checks.length} checks
          </button>
          {open && (
            <div className="border border-white/[0.06] rounded-xl overflow-hidden">
              {checks.map((c, i) => (
                <div key={i} className={`flex items-center gap-3 px-3 py-2 text-sm ${i % 2 === 0 ? 'bg-white/[0.02]' : ''}`}>
                  {statusIcon(c.status)}
                  <span className="flex-1 min-w-0">
                    <span className="font-medium">{c.column}</span>
                    <span className="text-agent-text-secondary"> · {c.check}</span>
                  </span>
                  <span className="text-xs text-agent-text-secondary shrink-0">{c.label}</span>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── 2b. OM classification card (standalone om_context event) ──────────────────

export function OmContextCard({ asset, tags, owner, tier }: { asset: string; tags?: string[]; owner?: string; tier?: string }) {
  if (!owner && !tier && !(tags && tags.length > 0)) return null;
  return (
    <div className="rounded-xl border border-violet-500/20 bg-violet-500/[0.04] px-4 py-2.5 flex flex-wrap items-center gap-3">
      <div className="flex items-center gap-1.5 text-xs uppercase tracking-wider text-violet-400 shrink-0">
        <TagIcon size={12} />OpenMetadata
      </div>
      <span className="text-xs font-mono text-agent-text-secondary shrink-0">{asset}</span>
      {owner && (
        <span className="flex items-center gap-1 text-xs text-agent-text-secondary">
          <User size={11} className="text-violet-400" />{owner}
        </span>
      )}
      {tier && (
        <span className="text-xs px-2 py-0.5 rounded-full bg-violet-500/15 border border-violet-500/25 text-violet-300 font-semibold">{tier}</span>
      )}
      {(tags || []).filter(t => !t?.includes('Tier')).slice(0, 6).map(t => (
        <span key={t} className="text-xs px-2 py-0.5 rounded-full bg-violet-500/10 border border-violet-500/20 text-violet-300 font-mono">{t}</span>
      ))}
    </div>
  );
}

// ── 3. Schema view ────────────────────────────────────────────────────────────

type ColDef = { name: string; type?: string };

const TYPE_COLOR: Record<string, 'teal' | 'orange' | 'sky' | 'amber' | 'default'> = {
  string: 'default', varchar: 'default', text: 'default',
  long: 'sky', int: 'sky', bigint: 'sky', double: 'sky', float: 'sky',
  'decimal(12, 2)': 'amber', decimal: 'amber',
  timestamp: 'orange', date: 'orange',
  boolean: 'teal',
};
function typeColor(t?: string): 'teal' | 'orange' | 'sky' | 'amber' | 'default' {
  if (!t) return 'default';
  for (const [k, v] of Object.entries(TYPE_COLOR)) { if (t.toLowerCase().startsWith(k)) return v; }
  return 'default';
}

export function SchemaView({ asset, columns, reused }: { asset: string; columns: ColDef[]; reused?: boolean }) {
  return (
    <div className="rounded-2xl border border-white/[0.08] bg-agent-dark-surface/60 p-4 space-y-3">
      <div className="flex items-center gap-2">
        <Layers size={15} className="text-agent-teal" />
        <span className="text-xs uppercase tracking-wider text-agent-text-secondary">Schema</span>
        <span className="text-sm font-medium">{asset}</span>
        {reused && <Tag color="teal">reused ♻</Tag>}
      </div>
      <div className="border border-white/[0.06] rounded-xl overflow-hidden">
        {columns.map((c, i) => (
          <div key={c.name} className={`flex items-center justify-between px-3 py-2 text-sm ${i % 2 === 0 ? 'bg-white/[0.02]' : ''}`}>
            <span className="font-mono font-medium">{c.name}</span>
            <Tag color={typeColor(c.type)}>{c.type || '?'}</Tag>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── 4. Pipeline view ──────────────────────────────────────────────────────────

type PipelineSummary = {
  flow_name?: string;
  processor_count?: number;
  parameter_count?: number;
  processors?: string[];
  controller_services?: string[];
  parameters_to_fill?: { name: string; sensitive?: boolean; description?: string }[];
};

export function PipelineView({ asset, summary }: { asset: string; summary: PipelineSummary }) {
  const [open, setOpen] = useState(false);
  const procs = summary.processors || [];
  return (
    <div className="rounded-2xl border border-white/[0.08] bg-agent-dark-surface/60 p-4 space-y-3">
      <div className="flex items-center gap-2">
        <FileCode2 size={15} className="text-agent-orange" />
        <span className="text-xs uppercase tracking-wider text-agent-text-secondary">Pipeline</span>
        <span className="text-sm font-semibold">{summary.flow_name || asset}</span>
      </div>

      <div className="flex gap-3">
        <div className="text-center px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/[0.06]">
          <div className="text-lg font-bold text-agent-orange">{summary.processor_count ?? '?'}</div>
          <div className="text-xs text-agent-text-secondary">processors</div>
        </div>
        <div className="text-center px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/[0.06]">
          <div className="text-lg font-bold text-amber-400">{summary.parameter_count ?? '?'}</div>
          <div className="text-xs text-agent-text-secondary">params to fill</div>
        </div>
        {(summary.controller_services?.length || 0) > 0 && (
          <div className="text-center px-3 py-1.5 rounded-xl bg-white/[0.03] border border-white/[0.06]">
            <div className="text-lg font-bold text-sky-400">{summary.controller_services!.length}</div>
            <div className="text-xs text-agent-text-secondary">services</div>
          </div>
        )}
      </div>

      {procs.length > 0 && (
        <div className="space-y-1.5">
          {procs.map((p, i) => (
            <div key={i} className="flex items-center gap-2.5">
              <div className="w-5 h-5 rounded-full bg-agent-orange/15 border border-agent-orange/30 flex items-center justify-center text-xs font-bold text-agent-orange">{i + 1}</div>
              <div className="flex-1 h-px bg-white/[0.06]" />
              <div className="text-xs font-mono text-agent-text-secondary max-w-[200px] truncate">{p}</div>
            </div>
          ))}
        </div>
      )}

      {(summary.parameters_to_fill?.length || 0) > 0 && (
        <>
          <button onClick={() => setOpen((o) => !o)}
            className="flex items-center gap-1 text-xs text-agent-text-secondary hover:text-agent-text-primary transition-colors">
            {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {summary.parameters_to_fill!.length} parameters to configure
          </button>
          {open && (
            <div className="border border-white/[0.06] rounded-xl overflow-hidden">
              {summary.parameters_to_fill!.map((p, i) => (
                <div key={i} className={`flex items-center justify-between px-3 py-2 text-sm ${i % 2 === 0 ? 'bg-white/[0.02]' : ''}`}>
                  <span className="font-mono text-agent-text-primary">{p.name}</span>
                  {p.sensitive && <Tag color="amber">secret</Tag>}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── 5. Data Analyst answer ────────────────────────────────────────────────────

export function AnswerCard({ asset, text, sql, grounded }: { asset?: string; text: string; sql?: string; grounded?: boolean }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-2xl border border-sky-400/25 bg-sky-400/[0.05] p-4 space-y-2.5">
      <div className="flex items-center gap-2">
        <div className="w-6 h-6 rounded-full bg-sky-400/15 flex items-center justify-center">
          <Database size={13} className="text-sky-400" />
        </div>
        <span className="text-xs uppercase tracking-wider text-sky-400">Answer</span>
        {asset && <span className="text-xs font-mono text-agent-text-secondary">· {asset}</span>}
        {grounded && (
          <span className="ml-auto text-xs flex items-center gap-1 text-agent-teal">
            <Check size={12} />grounded in data
          </span>
        )}
      </div>
      <p className="text-sm leading-relaxed">{text}</p>
      {sql && (
        <>
          <button onClick={() => setOpen((o) => !o)}
            className="flex items-center gap-1 text-xs text-agent-text-secondary hover:text-agent-text-primary transition-colors">
            {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {open ? 'Hide' : 'Show'} SQL
          </button>
          {open && (
            <pre className="text-xs bg-black/40 border border-white/[0.06] rounded-xl p-3 overflow-x-auto text-agent-text-secondary font-mono leading-relaxed">{sql}</pre>
          )}
        </>
      )}
    </div>
  );
}

// ── 6. Health view ────────────────────────────────────────────────────────────

export function HealthView({ pipeline, state, metrics }: { pipeline: string; state: string; metrics?: Record<string, unknown> }) {
  const healthy = state === 'healthy';
  return (
    <div className={`rounded-2xl border p-4 space-y-3 ${healthy ? 'border-agent-teal/25 bg-agent-teal/[0.05]' : 'border-rose-500/25 bg-rose-500/[0.04]'}`}>
      <div className="flex items-center gap-2">
        <HeartPulse size={15} className={healthy ? 'text-agent-teal' : 'text-rose-400'} />
        <span className="text-xs uppercase tracking-wider text-agent-text-secondary">Health</span>
        <span className="text-sm font-medium">{pipeline}</span>
        <span className={`ml-auto text-xs font-semibold px-2.5 py-1 rounded-full ${healthy ? 'bg-agent-teal/15 text-agent-teal' : 'bg-rose-500/15 text-rose-400'}`}>
          {state}
        </span>
      </div>
      {metrics && Object.keys(metrics).length > 0 && (
        <div className="grid grid-cols-2 gap-2">
          {Object.entries(metrics).slice(0, 6).map(([k, v]) => (
            <div key={k} className="text-xs bg-white/[0.03] border border-white/[0.06] rounded-xl px-3 py-2">
              <div className="text-agent-text-secondary">{k.replace(/_/g, ' ')}</div>
              <div className="font-mono font-semibold mt-0.5">{String(v)}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Lineage (how an asset is formed) ──────────────────────────────────────────

export type LineageGraphNode = { id: string; name: string; depth: number; side: string; entity_type?: string };
export type LineageGraph = { nodes: LineageGraphNode[]; edges: { from: string; to: string }[] };

const ENTITY_META: Record<string, { label: string; color: string }> = {
  table:     { label: 'Table',     color: 'text-agent-teal border-agent-teal/30 bg-agent-teal/[0.08]' },
  topic:     { label: 'Kafka',     color: 'text-agent-orange border-agent-orange/30 bg-agent-orange/[0.08]' },
  pipeline:  { label: 'Pipeline',  color: 'text-amber-400 border-amber-500/30 bg-amber-500/[0.08]' },
  dashboard: { label: 'Dashboard', color: 'text-violet-400 border-violet-500/30 bg-violet-500/[0.08]' },
};

function shortName(n: string) { return n.includes('.') ? n.split('.').slice(-2).join('.') : n; }

export function LineageView({ asset, graph, edgeCount, onPick }:
  { asset: string; graph: LineageGraph; edgeCount: number; onPick?: (n: string) => void }) {
  const nodes = graph?.nodes || [];
  if (!nodes.length) {
    return (
      <div className="rounded-2xl border border-white/[0.08] bg-agent-dark-surface/60 p-4 text-sm text-agent-text-secondary">
        No lineage recorded for <span className="font-mono text-agent-text-primary">{asset}</span> in OpenMetadata yet.
      </div>
    );
  }
  const byDepth = new Map<number, LineageGraphNode[]>();
  for (const n of nodes) { if (!byDepth.has(n.depth)) byDepth.set(n.depth, []); byDepth.get(n.depth)!.push(n); }
  const depths = Array.from(byDepth.keys()).sort((a, b) => a - b); // upstream (neg) → current (0) → downstream (pos)

  const Pill = ({ n }: { n: LineageGraphNode }) => {
    const meta = ENTITY_META[n.entity_type || 'table'] || ENTITY_META.table;
    const isCurrent = n.depth === 0;
    return (
      <button onClick={() => onPick?.(n.name)}
        className={`text-left px-3 py-1.5 rounded-lg border text-xs font-mono transition-all hover:brightness-125 ${
          isCurrent ? 'text-agent-orange border-agent-orange/50 bg-agent-orange/[0.12] font-semibold' : meta.color}`}>
        {shortName(n.name)}
        <span className="ml-1.5 opacity-60 not-italic">{isCurrent ? 'this' : meta.label}</span>
      </button>
    );
  };

  return (
    <div className="rounded-2xl border border-white/[0.08] bg-agent-dark-surface/60 p-4">
      <div className="flex items-center gap-2 mb-3 text-xs uppercase tracking-wider text-agent-text-secondary">
        <GitBranch size={13} /> How <span className="font-mono text-agent-text-primary normal-case">{shortName(asset)}</span> is formed
        <span className="ml-auto normal-case">{nodes.length} assets · {edgeCount} edges</span>
      </div>
      <div className="flex flex-col items-center gap-1">
        {depths.map((d, i) => (
          <div key={d} className="flex flex-col items-center gap-1">
            <div className="flex flex-wrap justify-center gap-1.5">
              {byDepth.get(d)!.map((n) => <Pill key={n.id || n.name} n={n} />)}
            </div>
            {i < depths.length - 1 && <ChevronDown size={14} className="text-white/20" />}
          </div>
        ))}
      </div>
      <div className="mt-3 text-xs text-agent-text-secondary text-center">Upstream sources flow down into this asset · tap any to inspect</div>
    </div>
  );
}

// ── Registry ─────────────────────────────────────────────────────────────────

export type UIBlock =
  | { kind: 'assets'; assets: AssetItem[]; focus?: string }
  | { kind: 'lineage'; asset: string; upstream?: unknown[]; downstream?: unknown[]; graph: LineageGraph; edgeCount: number }
  | { kind: 'quality'; asset: string; overall_score: number; counts: Record<string, number>; checks: Check[];
      total_rows?: number; om_owner?: string; om_tier?: string; om_tags?: string[] }
  | { kind: 'schema'; asset: string; columns: ColDef[]; reused?: boolean }
  | { kind: 'pipeline'; asset: string; summary: PipelineSummary }
  | { kind: 'answer'; asset?: string; text: string; sql?: string; grounded?: boolean }
  | { kind: 'health'; pipeline: string; state: string; metrics?: Record<string, unknown> }
  | { kind: 'om_context'; asset: string; tags?: string[]; owner?: string; tier?: string };

export function renderUIBlock(block: UIBlock, onPick?: (n: string) => void): React.ReactNode {
  switch (block.kind) {
    case 'assets': return <AssetGrid assets={block.assets} onPick={onPick || (() => {})} focus={block.focus} />;
    case 'lineage': return <LineageView asset={block.asset} graph={block.graph} edgeCount={block.edgeCount} onPick={onPick} />;
    case 'quality': return <QualityCard asset={block.asset} overall_score={block.overall_score} counts={block.counts}
                             checks={block.checks} total_rows={block.total_rows}
                             om_owner={block.om_owner} om_tier={block.om_tier} om_tags={block.om_tags} />;
    case 'schema': return <SchemaView asset={block.asset} columns={block.columns} reused={block.reused} />;
    case 'pipeline': return <PipelineView asset={block.asset} summary={block.summary} />;
    case 'answer': return <AnswerCard asset={block.asset} text={block.text} sql={block.sql} grounded={block.grounded} />;
    case 'health': return <HealthView pipeline={block.pipeline} state={block.state} metrics={block.metrics} />;
    case 'om_context': return <OmContextCard asset={block.asset} tags={block.tags} owner={block.owner} tier={block.tier} />;
  }
}
