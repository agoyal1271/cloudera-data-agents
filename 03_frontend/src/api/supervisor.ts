// Supervisor (multi-agent orchestrator) + Analyst SSE clients.
// POST a request, stream back raw event objects.

export type SupEvent = {
  type: string;
  agent?: string;
  [k: string]: any;
};

function streamSSE(
  url: string,
  body: unknown,
  onEvent: (e: SupEvent) => void,
  onDone?: () => void,
  onError?: (err: Error) => void,
): () => void {
  let aborted = false;
  (async () => {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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
          if (!line.startsWith('data:')) continue;
          const payload = line.slice(5).trim();
          if (!payload) continue;
          try {
            const e = JSON.parse(payload) as SupEvent;
            if (e.type === 'stream_end') onDone?.();
            else onEvent(e);
          } catch {
            /* skip malformed */
          }
        }
      }
      if (!aborted) onDone?.();
    } catch (e) {
      if (!aborted) onError?.(e as Error);
    }
  })();
  return () => {
    aborted = true;
  };
}

export function streamSupervisor(
  message: string,
  onEvent: (e: SupEvent) => void,
  onDone?: () => void,
  onError?: (err: Error) => void,
  contextAsset?: string,
  sessionId?: string,
): () => void {
  return streamSSE('/api/supervisor/chat',
    { message, context_asset: contextAsset ?? null, session_id: sessionId ?? null },
    onEvent, onDone, onError);
}

export function streamAnalyst(
  asset: string,
  question: string,
  onEvent: (e: SupEvent) => void,
  onDone?: () => void,
  onError?: (err: Error) => void,
): () => void {
  return streamSSE('/api/analyst/ask', { asset, question }, onEvent, onDone, onError);
}
