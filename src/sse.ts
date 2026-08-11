import type { FastifyReply, FastifyRequest } from 'fastify';
import type { FastifyBaseLogger } from 'fastify';
import type { Session } from './session.js';

const KEEPALIVE_MS = 15_000;

/**
 * Server-Sent Events connection. Writes `event:/id:/data:` frames directly to
 * the raw socket. Sends keepalive comments while idle so proxies/NAT don't
 * drop the connection. Respects Last-Event-ID replay via the session ring.
 */
export function attachSse(
  req: FastifyRequest,
  reply: FastifyReply,
  session: Session,
  log: FastifyBaseLogger,
): void {
  reply.hijack();
  const raw = reply.raw;
  raw.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  raw.write(': connected\n\n');

  // Replay missed events if the client sends Last-Event-ID.
  const lastIdHeader = req.headers['last-event-id'];
  const lastId = lastIdHeader ? Number.parseInt(String(lastIdHeader), 10) : -1;
  if (lastId >= 0) {
    for (const record of session.replayAfter(lastId)) {
      raw.write(encodeFrame(record.type, record.seq, record.data));
    }
  }

  const sink = {
    send(record: { type: string; seq: number; data: unknown }): void {
      raw.write(encodeFrame(record.type, record.seq, record.data));
    },
  };

  const keepalive = setInterval(() => {
    try {
      raw.write(': keepalive\n\n');
    } catch {
      cleanup();
    }
  }, KEEPALIVE_MS);
  keepalive.unref();

  session.subscribe(sink);

  function cleanup(): void {
    clearInterval(keepalive);
    session.unsubscribe(sink);
    try {
      raw.end();
    } catch { /* already closed */ }
  }

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
