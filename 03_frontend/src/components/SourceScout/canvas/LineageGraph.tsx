import { useMemo, useState, useEffect } from 'react';
import { Database, Radio, LayoutDashboard, BrainCircuit, Plus } from 'lucide-react';
import type { LineageNode, GraphNode, LineageGraphData } from '../../../api/scout';

type Kind = 'table' | 'topic' | 'dashboard' | 'model';

function kindOf(node: { fqn?: string; service?: string }): Kind {
  const f = (node.fqn ?? '').toLowerCase();
  const s = (node.service ?? '').toLowerCase();
  if (f.startsWith('cdp_kafka') || s.includes('kafka')) return 'topic';
  if (f.startsWith('cdp_superset') || s.includes('superset')) return 'dashboard';
  if (f.startsWith('cdp_mlflow') || s.includes('mlflow')) return 'model';
  return 'table';
}

const KIND_META: Record<Kind, { label: string; Icon: typeof Database; ring: string; text: string }> = {
  table:     { label: 'table',     Icon: Database,        ring: 'border-teal-500/40',   text: 'text-teal-200' },
  topic:     { label: 'kafka topic', Icon: Radio,         ring: 'border-blue-500/40',   text: 'text-blue-200' },
  dashboard: { label: 'dashboard', Icon: LayoutDashboard, ring: 'border-amber-500/40',  text: 'text-amber-200' },
  model:     { label: 'ml model',  Icon: BrainCircuit,    ring: 'border-violet-500/40', text: 'text-violet-200' },
};

const NODE_W = 196;
const NODE_H = 64;
const COL_GAP = 118;
const ROW_GAP = 22;
const PAD = 32;

interface Positioned extends GraphNode {
  x: number;
  y: number;
}

export function LineageGraph({
  asset, graph, upstream, downstream, onNodeClick,
}: {
  asset: string;
  graph?: LineageGraphData;
  upstream?: LineageNode[];
  downstream?: LineageNode[];
  onNodeClick?: (name: string) => void;
}) {
  // Build a graph; if the backend didn't send one, synthesise a 1-hop graph from
  // the direct upstream/downstream lists so older payloads still render.
  const data: LineageGraphData = useMemo(() => {
    if (graph && graph.nodes?.length) return graph;
    const cur: GraphNode = { id: '__cur__', name: asset, fqn: '', entity_type: 'table', description: '', service: '', depth: 0, side: 'cur' };
    const ups = (upstream ?? []).map<GraphNode>(n => ({ ...n, depth: -1, side: 'up' }));
    const downs = (downstream ?? []).map<GraphNode>(n => ({ ...n, depth: 1, side: 'down' }));
    const edges = [
      ...ups.map(u => ({ from: u.id, to: cur.id })),
      ...downs.map(d => ({ from: cur.id, to: d.id })),
    ];
    return { nodes: [cur, ...ups, ...downs], edges };
  }, [graph, asset, upstream, downstream]);

  const currentId = useMemo(
    () => data.nodes.find(n => n.side === 'cur')?.id ?? data.nodes[0]?.id ?? '',
    [data],
  );

  // adjacency (undirected) for expansion + visibility
  const adj = useMemo(() => {
    const m = new Map<string, Set<string>>();
    for (const n of data.nodes) m.set(n.id, new Set());
    for (const e of data.edges) {
      m.get(e.from)?.add(e.to);
      m.get(e.to)?.add(e.from);
    }
    return m;
  }, [data]);

  // expanded nodes whose neighbours are revealed; current is always expanded → 1 hop by default
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set([currentId]));
  useEffect(() => { setExpanded(new Set([currentId])); }, [currentId]);

  // visible = current + neighbours of every expanded, visible node (closure)
  const visible = useMemo(() => {
    const vis = new Set<string>([currentId]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const id of expanded) {
        if (!vis.has(id)) continue;
        for (const nb of adj.get(id) ?? []) {
          if (!vis.has(nb)) { vis.add(nb); changed = true; }
        }
      }
    }
    return vis;
  }, [expanded, adj, currentId]);

  const hiddenCount = (id: string) => {
    let c = 0;
    for (const nb of adj.get(id) ?? []) if (!visible.has(nb)) c++;
    return c;
  };

  // layout visible nodes into columns keyed by depth
  const { placed, edges, width, height } = useMemo(() => {
    const vnodes = data.nodes.filter(n => visible.has(n.id));
    const byDepth = new Map<number, GraphNode[]>();
    for (const n of vnodes) {
      if (!byDepth.has(n.depth)) byDepth.set(n.depth, []);
      byDepth.get(n.depth)!.push(n);
    }
    const depths = [...byDepth.keys()].sort((a, b) => a - b);
    for (const d of depths) byDepth.get(d)!.sort((a, b) => a.name.localeCompare(b.name));

    const maxRows = Math.max(1, ...depths.map(d => byDepth.get(d)!.length));
    const height = PAD * 2 + maxRows * NODE_H + (maxRows - 1) * ROW_GAP;
    const width = PAD * 2 + depths.length * NODE_W + Math.max(0, depths.length - 1) * COL_GAP;

    const pos = new Map<string, Positioned>();
    depths.forEach((d, ci) => {
      const col = byDepth.get(d)!;
      const colH = col.length * NODE_H + (col.length - 1) * ROW_GAP;
      const startY = (height - colH) / 2;
      const x = PAD + ci * (NODE_W + COL_GAP);
      col.forEach((n, i) => pos.set(n.id, { ...n, x, y: startY + i * (NODE_H + ROW_GAP) }));
    });

    const placed = [...pos.values()];
    const edges = data.edges
      .filter(e => pos.has(e.from) && pos.has(e.to))
      .map(e => ({ from: pos.get(e.from)!, to: pos.get(e.to)! }));

    return { placed, edges, width, height };
  }, [data, visible]);

  const handleClick = (n: Positioned) => {
    if (hiddenCount(n.id) > 0) {
      setExpanded(prev => new Set(prev).add(n.id));            // reveal next hop
    } else if (n.side !== 'cur') {
      onNodeClick?.(n.name);                                    // leaf → explore from here
    }
  };

  const edgePath = (from: Positioned, to: Positioned) => {
    const x1 = from.x + NODE_W, y1 = from.y + NODE_H / 2;
    const x2 = to.x, y2 = to.y + NODE_H / 2;
    const mx = (x1 + x2) / 2;
    return `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`;
  };

  return (
    <div className="w-full h-full overflow-auto flex items-center justify-center">
      <div className="relative" style={{ width, height, minWidth: width }}>
        <svg className="absolute inset-0" width={width} height={height} style={{ overflow: 'visible' }}>
          <defs>
            <marker id="lin-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
              <path d="M0,0 L10,5 L0,10 z" fill="#6B7480" />
            </marker>
          </defs>
          {edges.map((e, i) => (
            <path key={i} d={edgePath(e.from, e.to)} fill="none"
              stroke="#6B7480" strokeWidth={1.75} strokeOpacity={0.6} markerEnd="url(#lin-arrow)" />
          ))}
        </svg>

        {placed.map(n => {
          const meta = KIND_META[kindOf(n)];
          const current = n.side === 'cur';
          const hidden = hiddenCount(n.id);
          const expandable = hidden > 0;
          return (
            <button
              key={n.id}
              onClick={() => handleClick(n)}
              title={expandable ? `Expand — ${hidden} more` : n.name}
              className={`absolute text-left rounded-2xl border bg-agent-dark-surface px-3.5 py-2.5 transition-all duration-200 ${
                current
                  ? 'border-cloudera ring-2 ring-cloudera/30 cursor-default'
                  : `${meta.ring} ${expandable ? 'hover:border-cloudera/60 hover:-translate-y-0.5 cursor-pointer' : 'cursor-pointer opacity-90'}`
              }`}
              style={{ left: n.x, top: n.y, width: NODE_W, height: NODE_H }}
            >
              <div className="flex items-center gap-1.5 mb-1">
                <meta.Icon size={13} className={current ? 'text-cloudera' : meta.text} />
                <span className={`text-xs uppercase tracking-wider ${current ? 'text-cloudera' : 'text-agent-text-secondary'}`}>
                  {current ? 'this asset' : meta.label}
                </span>
              </div>
              <div className={`text-sm font-mono font-semibold truncate ${current ? 'text-cloudera' : 'text-agent-text-primary'}`}>
                {n.name}
              </div>

              {expandable && (
                <span
                  className="absolute -right-2.5 top-1/2 -translate-y-1/2 w-6 h-6 rounded-full bg-cloudera text-white flex items-center justify-center shadow-md shadow-cloudera/30"
                  title={`${hidden} more`}
                >
                  <Plus size={14} strokeWidth={3} />
                </span>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}
