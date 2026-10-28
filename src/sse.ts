import zlib from 'node:zlib';
import type { FastifyBaseLogger, FastifyReply, FastifyRequest } from 'fastify';
import { isToolEvent, type EventSink, type RingRecord, type Session } from './session.js';

const KEEPALIVE_MS = 15_000;
/** Slow-consumer cap: a sink queuing more than this is dropped (it reconnects + replays). */
const MAX_PENDING_FRAMES = 300;
const MAX_PENDING_BYTES = 8 * 1024 * 1024;
const DELTA_FLUSH_MS = 60;
const RETRY_MS = 3_000;

/* ---------------- CONTRACT §1: ids + reset ---------------- */

export interface ParsedEventId {
  epoch: string;
  seq: number;
}

/** `<epoch>-<seq>` → parts; null when unparsable (legacy numeric ids included). */
export function parseEventId(raw: string | undefined | null): ParsedEventId | null {
  if (!raw) return null;
  const at = raw.lastIndexOf('-');
  if (at <= 0) return null;
  const epoch = raw.slice(0, at);
  const seqText = raw.slice(at + 1);
  if (!/^\d+$/.test(seqText)) return null;
  const seq = Number(seqText);
  return Number.isSafeInteger(seq) ? { epoch, seq } : null;
}

export type ReplayDecision =
  | { kind: 'live' }
  | { kind: 'replay'; records: RingRecord[] }
  | { kind: 'reset'; reason: 'epoch' | 'gap' | 'ahead' };

export interface ReplaySource {
  epoch: string;
  seq: number;
  /** Highest seq evicted from the ring. */
  ringFloor: number;
  ringSnapshot(): readonly RingRecord[];
}

/**
 * Decide what a (re)connecting client gets:
 *  - no Last-Event-ID → live;
 *  - same epoch, seq inside the ring → replay frames after it;
 *  - different epoch / unparsable → reset(epoch); seq beyond current → reset(ahead);
 *    seq older than the ring's retained window → reset(gap).
 */
export function resolveReplay(lastEventId: string | undefined, src: ReplaySource): ReplayDecision {
  if (!lastEventId) return { kind: 'live' };
  const parsed = parseEventId(lastEventId);
  if (!parsed || parsed.epoch !== src.epoch) return { kind: 'reset', reason: 'epoch' };
  if (parsed.seq > src.seq) return { kind: 'reset', reason: 'ahead' };
  if (parsed.seq < src.ringFloor) return { kind: 'reset', reason: 'gap' };
  return { kind: 'replay', records: src.ringSnapshot().filter((r) => r.seq > parsed.seq) };
}

export function encodeFrame(event: string, id: string, json: string): string {
  return `event: ${event}\nid: ${id}\ndata: ${json}\n\n`;
}

export interface SseExtras {
  /** Current pending queue (CONTRACT §2) — sent as a snapshot on connect. */
  queueSnapshot?: () => unknown[];
}

/**
 * Server-Sent Events connection: gzip with real backpressure (bounded
 * buffer, drop slow consumers), per-sink delta coalescing, focus-mode tool
 * filtering, epoch-seq ids with replay/reset, and a status snapshot.
 */
export function attachSse(
  req: FastifyRequest,
  reply: FastifyReply,
  session: Session,
  log: FastifyBaseLogger,
  extras: SseExtras = {},
): void {
  reply.hijack();
  const raw = reply.raw;
  // Negotiate compression: gzip only when the client advertises it
  // (URLSession always does); plain text otherwise (curl, EventSource polyfills).
  const acceptsGzip = /\bgzip\b/i.test(String(req.headers['accept-encoding'] ?? ''));
  const gz = acceptsGzip ? zlib.createGzip({ level: zlib.constants.Z_BEST_SPEED }) : null;
  if (gz) gz.pipe(raw);
  const out: NodeJS.WritableStream = gz ?? raw;
  const flushOut = (): void => { gz?.flush(); };

  raw.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    ...(gz ? { 'Content-Encoding': 'gzip' } : {}),
    Vary: 'Accept-Encoding',
    'X-Accel-Buffering': 'no',
  });

  let closed = false;
  let paused = false;
  let pendingBytes = 0;
  const pending: string[] = [];

  const flush = (): void => {
    paused = false;
    while (!paused && pending.length > 0) {
      const frame = pending.shift()!;
      pendingBytes -= frame.length;
      if (!out.write(frame)) {
        paused = true;
        out.once('drain', flush);
        break;
      }
      flushOut();
    }
  };

  const sendFrame = (frame: string): void => {
    if (closed) return;
    if (paused) {
      pending.push(frame);
      pendingBytes += frame.length;
      if (pending.length > MAX_PENDING_FRAMES || pendingBytes > MAX_PENDING_BYTES) cleanup();
      return;
    }
    const ok = out.write(frame);
    flushOut();
    if (!ok) {
      paused = true;
      out.once('drain', flush);
    }
  };

  const skeleton = (req.query as { skeleton?: string }).skeleton === '1';

  // Delta coalescing: text/thinking deltas stream at token rate; merge them
  // per sink and flush every 60 ms as one delta carrying the LAST id.
  // §13: only deltas of the SAME message key merge (and text deltas only when
  // contiguous by offset); the merged frame carries the FIRST delta's offset.
  let deltaBuf: { kind: 'text' | 'thinking'; contentIndex: number; text: string; id: string; key: string | null; offset: number | undefined } | null = null;
  let deltaTimer: NodeJS.Timeout | null = null;

  function flushDeltas(): void {
    if (deltaTimer) clearTimeout(deltaTimer);
    deltaTimer = null;
    const buf = deltaBuf;
    if (!buf) return;
    deltaBuf = null;
    const data: Record<string, unknown> = {
      type: 'message_update',
      key: buf.key,
      assistantMessageEvent: buf.offset === undefined
        ? { type: `${buf.kind}_delta`, contentIndex: buf.contentIndex, delta: buf.text }
        : { type: `${buf.kind}_delta`, contentIndex: buf.contentIndex, delta: buf.text, offset: buf.offset },
    };
    if (buf.offset !== undefined) data.offset = buf.offset;
    sendFrame(encodeFrame('message_update', buf.id, JSON.stringify(data)));
  }

  const write = (record: RingRecord): void => {
    // Focus clients stream ONLY user input + assistant prose (+ status).
    if (skeleton && isToolEvent(record)) return; // incl. file_update rows of role tool
    if (record.type === 'message_update') {
      const ev = (record.data as { assistantMessageEvent?: { type?: string; contentIndex?: number; delta?: string } }).assistantMessageEvent;
      if (ev && (ev.type === 'text_delta' || ev.type === 'thinking_delta')) {
        const kind = ev.type === 'text_delta' ? 'text' as const : 'thinking' as const;
        const contentIndex = ev.contentIndex ?? 0;
        const d = record.data as { key?: unknown; offset?: unknown };
        const key = typeof d.key === 'string' ? d.key : null;
        const offset = typeof d.offset === 'number' ? d.offset : undefined;
        const delta = ev.delta ?? '';
        const contiguous = deltaBuf !== null && (deltaBuf.offset === undefined
          ? offset === undefined
          : offset !== undefined && deltaBuf.offset + deltaBuf.text.length === offset);
        if (deltaBuf && deltaBuf.kind === kind && deltaBuf.contentIndex === contentIndex && deltaBuf.key === key && contiguous) {
          deltaBuf.text += delta;
          deltaBuf.id = record.id;
        } else {
          flushDeltas();
          deltaBuf = { kind, contentIndex, text: delta, id: record.id, key, offset };
        }
        if (!deltaTimer) deltaTimer = setTimeout(flushDeltas, DELTA_FLUSH_MS);
        return;
      }
    }
    flushDeltas(); // preserve order: pending deltas go before any other frame
    sendFrame(encodeFrame(record.type, record.id, record.json));
  };

  const sink: EventSink = {
    skeleton,
    send: write,
    close(): void {
      flushDeltas();
      cleanup();
    },
  };

  // Subscribe and replay synchronously — no event can interleave.
  session.subscribe(sink);
  // `retry:` tells EventSource-style clients how fast to reconnect (WHATWG).
  sendFrame(`retry: ${RETRY_MS}\n: connected\n\n`);
  const currentId = (): string => `${session.epoch}-${session.seq}`;
  const lastId = req.headers['last-event-id'];
  const decision = resolveReplay(Array.isArray(lastId) ? lastId[0] : lastId, session);
  if (decision.kind === 'reset') {
    sendFrame(encodeFrame('reset', currentId(), JSON.stringify({ type: 'reset', reason: decision.reason })));
  } else if (decision.kind === 'replay') {
    for (const record of decision.records) write(record);
    flushDeltas();
  }
  // Authoritative status snapshot (agent_status frames are live-only).
  sendFrame(encodeFrame('agent_status', currentId(), JSON.stringify(session.statusFrame())));
  if (session.pendingQuestion) {
    sendFrame(encodeFrame('agent_question', currentId(), JSON.stringify({
      type: 'agent_question',
      requestId: session.pendingQuestion.requestId,
      questions: session.pendingQuestion.questions,
    })));
  }
  // CONTRACT-V2 §13: the in-progress assistant message, authoritative up to now
  // (after the status snapshot; also covers the reset path above).
  const inProgress = session.inProgress;
  if (inProgress) {
    sendFrame(encodeFrame('message_snapshot', currentId(), JSON.stringify({
      type: 'message_snapshot',
      key: inProgress.key,
      role: 'assistant',
      timestamp: inProgress.timestamp,
      text: inProgress.text,
      streaming: true,
    })));
  }
  if (extras.queueSnapshot) {
    sendFrame(encodeFrame('queue_update', currentId(), JSON.stringify({ type: 'queue_update', items: extras.queueSnapshot() })));
  }

  const keepalive = setInterval(() => sendFrame(': keepalive\n\n'), KEEPALIVE_MS);
  keepalive.unref();

  function cleanup(): void {
    if (closed) return;
    closed = true;
    clearInterval(keepalive);
    if (deltaTimer) clearTimeout(deltaTimer);
    session.unsubscribe(sink);
    try {
      out.end();
    } catch { /* already closed */ }
  }

  gz?.on('error', cleanup);
  raw.on('error', cleanup);
  req.raw.on('close', cleanup);
  log.debug({ sessionId: session.id }, 'sse client connected');
}
