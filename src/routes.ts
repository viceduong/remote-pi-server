import crypto from 'node:crypto';
import os from 'node:os';
import type { FastifyBaseLogger, FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { BusyError, type SessionManager } from './manager.js';
import { attachSse } from './sse.js';
import type { Env } from './config.js';
import type { ServerConfig } from './types.js';

const CreateSessionSchema = z.object({
  name: z.string().max(80).optional(),
});

const TurnSchema = z.object({
  message: z.string().min(1).max(50_000),
  streamingBehavior: z.enum(['steer', 'followUp']).optional(),
});

// App-created ids are 16-hex; discovered pi session ids are UUIDs or <ts>_<uuid>.
const ID_PARAM = z.string().regex(/^[0-9a-fA-F]{16}$|^[0-9A-Za-z_.-]+$/);

export function registerRoutes(
  fastify: FastifyInstance,
  opts: { config: Env; manager: SessionManager; piVersion: string | null; log: FastifyBaseLogger },
): void {
  const { config, manager, piVersion, log } = opts;

  fastify.addHook('onRequest', async (req, reply) => {
    if (req.url === '/api/health') return;
    if (!authorized(req, config.REMOTE_PI_TOKEN)) {
      reply.code(401).send({ error: 'Unauthorized' });
    }
  });

  const publicConfig: ServerConfig = {
    name: config.REMOTE_PI_NAME ?? os.hostname(),
    piVersion,
    api: '1.0',
    workdir: config.REMOTE_PI_WORKDIR,
    features: ['chat', 'sessions', 'abort', 'history', 'sse'],
  };

  fastify.get('/api/health', async () => ({ ok: true, name: publicConfig.name }));

  fastify.get('/api/config', async () => publicConfig);

  fastify.get('/api/status', async () => {
    const liveInstances = manager.getLiveInstances();
    return {
      ...publicConfig,
      maxAgents: config.REMOTE_PI_MAX_AGENTS,
      runningAgents: manager.runningCount,
      liveInstances,
      externalAgents: liveInstances.length,
      sessions: manager.list(),
    };
  });

  fastify.get('/api/sessions', async (req) => {
    // Cursor pagination: ?limit=N (default 50, max 200) and ?before=<ts>.
    const limit = Math.min(Math.max(parseInt(String((req.query as { limit?: string }).limit ?? '50'), 10) || 50, 1), 200);
    const before = parseInt(String((req.query as { before?: string }).before ?? ''), 10) || undefined;
    return manager.listPage(limit, before);
  });

  fastify.post('/api/sessions', async (req, reply) => {
    const body = CreateSessionSchema.safeParse(req.body);
    if (!body.success) return reply.code(400).send({ error: 'Invalid body', issues: body.error.issues });
    try {
      const session = await manager.create(body.data.name ?? '');
      return reply.code(201).send({ session: session.toSummary() });
    } catch (err) {
      if (err instanceof BusyError) return reply.code(503).send({ error: err.message });
      return reply.code(409).send({ error: (err as Error).message });
    }
  });

  fastify.get('/api/sessions/:id', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const summary = manager.summary(id);
    if (!summary) return reply.code(404).send({ error: 'Session not found' });
    return { session: summary };
  });

  fastify.post('/api/sessions/:id/turn', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const body = TurnSchema.safeParse(req.body);
    if (!body.success) return reply.code(400).send({ error: 'Invalid body', issues: body.error.issues });

    // Write guard (pi-threads external-writer pattern): an external pi process
    // owns this session — refusing prevents interleaved JSONL writes. force=1
    // only bypasses while the host agent is IDLE (not actively writing), so
    // app and host turns can never run concurrently.
    const force = (req.query as { force?: string }).force === '1';
    if (manager.isExternallyLive(id)) {
      const hostWriting = manager.isHostWriting(id);
      if (!force || hostWriting) {
        return reply.code(409).send({
          error: hostWriting
            ? 'The host terminal is actively working on this session right now. Let it finish, then retry.'
            : 'Session is live on the host (an external pi process is using it). Stop it there, or resume anyway with ?force=1.',
          code: 'session_live',
        });
      }
    }

    try {
      await manager.ensureRunning(id);
    } catch (err) {
      return reply.code((err as { code?: number }).code ?? 409).send({ error: (err as Error).message });
    }
    const session = manager.get(id) ?? null;
    if (!session) return reply.code(404).send({ error: 'Session not found' });

    let queued = false;
    let behavior = body.data.streamingBehavior;
    if (session.busy && !behavior) {
      // The busy flag can go stale (process respawned mid-turn, missed events).
      // Verify against the live process before queueing — otherwise we queue
      // behind an idle agent and the follow-up never fires.
      try {
        const st = await session.request<{ isStreaming?: boolean }>({ type: 'get_state' });
        session.busy = st.isStreaming === true;
      } catch {
        /* keep busy as-is */
      }
      if (session.busy) {
        behavior = 'followUp'; // queue until the agent finishes
        queued = true;
      }
    }
    try {
      const cmd: Record<string, unknown> = { type: 'prompt', message: body.data.message };
      if (behavior) cmd.streamingBehavior = behavior;
      await session.request(cmd, 8000);
      return reply.code(202).send({ accepted: true, queued });
    } catch (err) {
      return reply.code(409).send({ error: (err as Error).message });
    }
  });

  fastify.get('/api/sessions/:id/messages', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const session = manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    // Pagination: ?limit=N (default 100, max 500) and ?before=<ts> (load older).
    const limit = Math.min(Math.max(parseInt(String((req.query as { limit?: string }).limit ?? '100'), 10) || 100, 1), 500);
    const before = parseInt(String((req.query as { before?: string }).before ?? ''), 10) || undefined;
    try {
      const all = await manager.history(session);
      const page = before
        ? all.filter((m) => (m.timestamp ?? 0) < before).slice(-limit)
        : all.slice(-limit);
      return {
        messages: page,
        hasMore: before
          ? all.some((m) => (m.timestamp ?? 0) < (page[0]?.timestamp ?? 0))
          : all.length > limit,
        total: all.length,
      };
    } catch (err) {
      return reply.code(500).send({ error: (err as Error).message });
    }
  });

  fastify.get('/api/sessions/:id/state', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const session = manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    if (!session.running) return { session: session.toSummary(), state: { running: false } };
    try {
      const state = await session.request({ type: 'get_state' });
      return { session: session.toSummary(), state };
    } catch (err) {
      return { session: session.toSummary(), state: { error: (err as Error).message } };
    }
  });

  fastify.post('/api/sessions/:id/abort', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const session = manager.find(id);
    if (session) session.send({ type: 'abort' });
    return { ok: true };
  });

  const ForkSchema = z.object({
    entryId: z.string().min(1).optional(),
    name: z.string().max(80).optional(),
  });

  fastify.post('/api/sessions/:id/fork', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const body = ForkSchema.safeParse(req.body ?? {});
    if (!body.success) return reply.code(400).send({ error: 'Invalid body', issues: body.error.issues });

    // Default to the last user message entry in the session file.
    let entryId = body.data.entryId;
    if (!entryId) {
      const messages = manager.historyFromFileSafe(id);
      const lastUser = [...messages].reverse().find((m) => m.role === 'user' && m.id);
      if (lastUser?.id) entryId = lastUser.id;
    }
    if (!entryId) return reply.code(400).send({ error: 'No forkable user message found' });

    try {
      const result = await manager.fork(id, entryId, body.data.name);
      if (!result) return reply.code(404).send({ error: 'Session not found' });
      return reply.code(201).send({ session: result.session, forkedFrom: result.text });
    } catch (err) {
      return reply.code(409).send({ error: (err as Error).message });
    }
  });

  fastify.delete('/api/sessions/:id', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const { purge } = (req.query as { purge?: string });
    const removed = manager.delete(id, purge === '1');
    if (!removed) return reply.code(404).send({ error: 'Session not found' });
    return { ok: true };
  });

  fastify.get('/api/sessions/:id/events', async (req, reply) => {
    const id = parseId(req, reply);
    if (!id) return;
    const session = await manager.ensureRunning(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    attachSse(req, reply, session, log);
  });
}

function parseId(req: FastifyRequest, reply: {
  code: (code: number) => { send: (body: unknown) => unknown };
}): string | null {
  const id = ID_PARAM.safeParse((req.params as { id?: string }).id);
  if (!id.success) {
    reply.code(400).send({ error: 'Invalid session id' });
    return null;
  }
  return id.data;
}

function authorized(req: FastifyRequest, token: string): boolean {
  if (!token) return true;
  const header = req.headers.authorization ?? '';
  const match = /^Bearer\s+(.+)$/i.exec(header);
  if (!match) return false;
  const a = Buffer.from(match[1]!);
  const b = Buffer.from(token);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
