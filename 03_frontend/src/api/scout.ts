// Scout Chat — streaming client for the conversational orchestrator.

export interface QualityCheck {
  check: string;
  column: string;
  metric_value: number;
  label: string;
  status: 'pass' | 'warn' | 'fail';
}

export interface QualityTrend {
  direction: 'up' | 'down' | 'stable';
  current: number;
  baseline: number;
  delta: number;
  level: 'good' | 'fair' | 'poor';
  driver: string;
  points: number[];
  window_days: number;
}

export interface RootCause {
  asset: string;
  current: number;
  direction: string;
  delta: number;
  driver: string;
}

export type ChatBlock =
  | { type: 'thinking'; text: string }
  | { type: 'step'; label: string; detail?: string }
  | { type: 'text'; text: string }
  | { type: 'assets'; assets: AssetCard[] }
  | { type: 'lineage'; asset: string; upstream: LineageNode[]; downstream: LineageNode[]; edge_count: number; graph?: LineageGraphData }
  | { type: 'sql_result'; asset: string; sql: string; columns: string[]; rows: string[][]; row_count?: number; executed_on?: string; error?: string }
  | { type: 'schema'; asset: string; asset_type: string; fields: Array<{ name: string; type?: string }> }
  | { type: 'quality'; asset: string; overall_score: number; counts: { pass: number; warn: number; fail: number }; checks: QualityCheck[]; total_rows: number; trend: QualityTrend | null; root_cause: RootCause | null; written_to_om: boolean; ambient?: boolean }
  | { type: 'caveat'; asset: string; level: string; direction: string; text: string }
  | { type: 'context'; asset: string; asset_type?: string }
  | { type: 'provenance'; spans: ProvenanceSpan[]; summary: ProvenanceSummary }
  | { type: 'done' };

export type SpanKind = 'llm' | 'deterministic' | 'knox' | 'openmetadata';

export interface ProvenanceSpan {
  name: string;
  kind: SpanKind;
  ms: number;
  model?: string;
  tokens?: number;
  temperature?: number;
  prompt?: string;
  completion?: string;
  note?: string;
}

export interface ProvenanceSummary {
  llm_calls: number;
  deterministic_steps: number;
  total_tokens: number;
  total_ms: number;
}

export interface AssetCard {
  name: string;
  asset_type: string;
  field_count: number;
  fields: string[];
  reason?: string;
}

export interface LineageNode {
  id: string;
  name: string;
  fqn: string;
  entity_type: string;
  description: string;
  service: string;
}

export interface GraphNode extends LineageNode {
  depth: number;             // signed hop distance: -2,-1 upstream · 0 current · +1,+2 downstream
  side: 'up' | 'cur' | 'down';
}

export interface LineageGraphData {
  nodes: GraphNode[];
  edges: { from: string; to: string }[];
}

export function streamChat(
  message: string,
  context: { asset?: string; assetType?: string },
  onBlock: (block: ChatBlock) => void,
  onDone?: () => void,
  onError?: (err: Error) => void,
): () => void {
  let aborted = false;
  (async () => {
    try {
      const res = await fetch('/api/scout/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message,
          context_asset: context.asset ?? null,
          context_asset_type: context.assetType ?? null,
        }),
      });
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (!aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          try {
            const block = JSON.parse(line.slice(6)) as ChatBlock;
            if (block.type === 'done') onDone?.();
            else onBlock(block);
          } catch { /* skip malformed */ }
        }
      }
    } catch (e) {
      if (!aborted) onError?.(e as Error);
    }
  })();
  return () => { aborted = true; };
}
