import zlib from 'node:zlib';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type { FastifyBaseLogger } from 'fastify';
import type { Session } from './session.js';

let seqCounter = 0;
const KEEPALIVE_MS = 15_000;
/** Slow-consumer cap: if a sink queues more than this many frames, the client
 *  is too slow — drop the connection (it will reconnect + replay). */
const MAX_PENDING_FRAMES = 300;
const MAX_PENDING_BYTES = 8 * 1024 * 1024;

/**
 * Server-Sent Events connection. Writes `event:/id:/data:` frames through a
 * gzip stream with proper backpressure: when the socket is congested
 * (gzip.write returns false) frames are queued in a bounded buffer and
 * flushed on 'drain'; a client that outruns the cap is disconnected rather
 * than unboundedly buffering in memory.
 */
export function attachSse(
  req: FastifyRequest,
  reply: FastifyReply,
  session: Session,
  log: FastifyBaseLogger,
): void {
  reply.hijack();
  const raw = reply.raw;
  const gzip = zlib.createGzip({ level: zlib.constants.Z_BEST_SPEED });
  gzip.pipe(raw);

  raw.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'Content-Encoding': 'gzip',
    'X-Accel-Buffering': 'no',
  });

  let closed = false;
  let paused = false;
  let pausedAt = 0;
  let pendingBytes = 0;
  let replaying = true;
  const replayPending: Array<{ type: string; seq: number; data: unknown }> = [];
  const pending: string[] = [];

  const flush = (): void => {
    // drain means gzip can accept data again. The old implementation forgot
    // this reset and permanently wedged every congested SSE connection.
    paused = false;
    pausedAt = 0;
    while (!paused && pending.length > 0) {
      const frame = pending.shift()!;
      pendingBytes -= Buffer.byteLength(frame);
      if (!gzip.write(frame)) {
        paused = true;
        pausedAt = Date.now();
        gzip.once('drain', flush);
        break;
      }
      gzip.flush();
    }
  };

  const sendFrame = (frame: string): void => {
    if (closed) return;
    if (paused) {
      pending.push(frame);
      pendingBytes += Buffer.byteLength(frame);
      if (pending.length > MAX_PENDING_FRAMES || pendingBytes > MAX_PENDING_BYTES) cleanup();
      return;
    }
    const ok = gzip.write(frame);
    gzip.flush();
    if (!ok) {
      paused = true;
      pausedAt = Date.now();
      gzip.once('drain', flush);
    }
  };

  const lastIdHeader = req.headers['last-event-id'];
  const lastId = lastIdHeader ? Number.parseInt(String(lastIdHeader), 10) : -1;
  const replay = lastId >= 0 ? session.replayAfter(lastId) : [];

  const skeleton = (req.query as { skeleton?: string }).skeleton === '1';

  // Delta coalescing: text/thinking deltas stream at token rate; shipping
  // each one is 5-8x more frames than needed. Buffer them per-sink and flush
  // every 60ms as a single merged delta.
  let deltaBuf: { kind: 'text' | 'thinking'; contentIndex: number; text: string; seq: number } | null = null;
  let deltaTimer: NodeJS.Timeout | null = null;

  function flushDeltas(): void {
    deltaTimer = null;
    const buf = deltaBuf;
    if (!buf) return;
    deltaBuf = null;
    const kind = buf.kind, contentIndex = buf.contentIndex, text = buf.text;
    // Use the seq of the LAST buffered delta so Last-Event-ID stays correct.
    const frame = {
      type: 'message_update',
      seq: buf.seq,
      data: {
        type: 'message_update',
        assistantMessageEvent: { type: kind + '_delta', contentIndex, delta: text },
      },
    };
    sendFrame(encodeFrame(frame.type, frame.seq, frame.data));
  }

  const sink = {
    skeleton,
    send(record: { type: string; seq: number; data: unknown }): void {
      // Skeleton clients: stream ONLY user input + assistant response.
      // Tool events (calls, outputs, execution progress) are dropped — the
      // assistant's prose already describes what it did.
      const d = record.data as { message?: { role?: string; toolName?: string } };
      if (
        record.type === 'tool_execution_start' ||
        record.type === 'tool_execution_update' ||
        record.type === 'tool_execution_end' ||
        (record.type === 'message_start' && d?.message?.role === 'tool') ||
        (record.type === 'message_end' && d?.message?.role === 'tool') ||
        (record.type === 'message_update' && d?.message?.role === 'tool') ||
        (record.type === 'file_update' && (d?.message?.role === 'tool' || !!d?.message?.toolName))
      ) {
        return; // seq NOT advanced: reconnect replays nothing for dropped frames
      }
      if (replaying) { replayPending.push(record); return; }
      // Coalesce streaming deltas
      if (record.type === 'message_update') {
        const ev = (record.data as { assistantMessageEvent?: { type?: string; contentIndex?: number; delta?: string } }).assistantMessageEvent;
        if (ev && (ev.type === 'text_delta' || ev.type === 'thinking_delta')) {
          const kind = ev.type === 'text_delta' ? 'text' as const : 'thinking' as const;
          if (deltaBuf && deltaBuf.kind === kind && deltaBuf.contentIndex === ev.contentIndex) {
            deltaBuf.text += ev.delta ?? '';
            deltaBuf.seq = record.seq;
          } else {
            flushDeltas();
            deltaBuf = { kind, contentIndex: ev.contentIndex ?? 0, text: ev.delta ?? '', seq: record.seq };
          }
          if (!deltaTimer) deltaTimer = setTimeout(flushDeltas, 60);
          return;
        }
        // Non-delta event: flush pending deltas FIRST to preserve order
        flushDeltas();
      }
      sendFrame(encodeFrame(record.type, record.seq, record.data));
    },
    close(): void {
      if (deltaTimer) clearTimeout(deltaTimer);
      flushDeltas();
      cleanup();
    },
  };

  session.subscribe(sink);
  sendFrame(': connected\n\n');
  for (const record of replay) {
    if (skeleton && session.isToolEvent(record)) continue; // focus: drop tool frames
    const data = skeleton && session.isToolHeavyEvent(record)
      ? session.skeletonizeRecordData(record.data) : record.data;
    sendFrame(encodeFrame(record.type, record.seq, data));
  }
  replaying = false;
  for (const record of replayPending.splice(0).sort((a, b) => a.seq - b.seq)) {
    if (record.seq > lastId) sendFrame(encodeFrame(record.type, record.seq, record.data));
  }

  const keepalive = setInterval(() => {
    try {
      sendFrame(': keepalive\n\n');
    } catch {
      cleanup();
    }
  }, KEEPALIVE_MS);
  keepalive.unref();

  function cleanup(): void {
    if (closed) return;
    closed = true;
    clearInterval(keepalive);
    session.unsubscribe(sink);
    try {
      gzip.end();
    } catch { /* already closed */ }
  }

  gzip.on('error', cleanup);
  raw.on('error', cleanup);
  req.raw.on('close', cleanup);
  log.debug({ sessionId: session.id }, 'sse client connected');
}

function encodeFrame(event: string, seq: number, data: unknown): string {
  return (
    `event: ${event}\n` +
    `id: ${seq}\n` +
    `data: ${JSON.stringify(data)}\n\n`
  );
}
