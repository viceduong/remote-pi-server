import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { FastifyBaseLogger, FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { AmbiguousModelError, BusyError, ModelNotFoundError, ModelTransportError, SessionLiveError, SpawnError, type SessionManager } from './manager.js';
import { LeaseHeldError, type Session } from './session.js';
import { attachSse } from './sse.js';
import { readTailLines, parseEntry } from './jsonl.js';
import { isVisibleMessage } from './visibility.js';
import { isActive } from './queue.js';
import type { Env } from './config.js';
import type { ServerConfig } from './types.js';
import { listDir, rootPaths, WorkdirError } from './workdir.js';

/* ---------------- request schemas (zod on every body/query) ---------------- */

const IDEMPOTENCY_KEY = z.string().regex(/^[A-Za-z0-9._:-]{1,128}$/);
const flag = z.enum(['0', '1', 'true', 'false']).optional().transform((v) => v === '1' || v === 'true');
const intQ = (def: number, min: number, max: number) => z.coerce.number().int().min(min).max(max).optional().default(def);
const tsQ = z.coerce.number().int().positive().optional();

const CreateSessionSchema = z.object({
  name: z.string().max(80).optional(),
  /** Idempotency key: a repeat within 10 min returns the same session. */
  clientRequestId: IDEMPOTENCY_KEY.optional(),
  /** CONTRACT §12: absolute dir inside the configured roots (validated by workdir.ts → 403/404/400). */
  workdir: z.string().min(1).max(4096).optional(),
  /** §12: `mkdir -p` a missing workdir (default false). */
  createDir: z.boolean().optional(),
});
const DirsQuery = z.object({ path: z.string().min(1).max(4096).optional() });

const TurnSchema = z.object({
  message: z.string().min(1).max(50_000),
  streamingBehavior: z.enum(['steer', 'followUp']).optional(),
  // Client-generated idempotency key; legacy clients may omit it.
  clientMessageId: IDEMPOTENCY_KEY.optional(),
});
const TurnQuery = z.object({ force: flag });
const ListQuery = z.object({ limit: intQ(50, 1, 200), before: tsQ });
const MessagesQuery = z.object({
  limit: intQ(100, 1, 500),
  before: tsQ,
  since: z.string().min(1).max(128).optional(),
  visible: flag,
});
const DeleteQuery = z.object({ purge: flag, force: flag });
const EventsQuery = z.object({ skeleton: flag });
const AnswerQuestionSchema = z.object({
  answers: z.array(z.object({
    questionIndex: z.number().int().min(0),
    question: z.string(),
    kind: z.enum(['option', 'custom', 'chat', 'multi']),
    answer: z.string().nullable(),
    selected: z.array(z.string()).optional(),
    notes: z.string().optional(),
    preview: z.string().optional(),
  })).optional(),
  cancelled: z.boolean().optional(),
});
const RenameSchema = z.object({ name: z.string().min(1).max(80) });
const SetModelSchema = z.object({
  provider: z.string().min(1).max(200).optional(),
  modelId: z.string().min(1).max(200),
});
const ForkSchema = z.object({
  entryId: z.string().min(1).max(128).optional(),
  name: z.string().max(80).optional(),
  clientRequestId: IDEMPOTENCY_KEY.optional(),
});
const EmptyBody = z.union([z.object({}).passthrough(), z.null(), z.undefined()]);

// App-created ids are 16-hex; discovered pi session ids are UUIDs or <ts>_<uuid>.
const ID_PARAM = z.string().regex(/^[0-9A-Za-z_.-]{1,128}$/);
const PARAM_TOKEN = z.string().regex(/^[0-9A-Za-z_.:-]{1,256}$/);

type Reply = FastifyReply;

function parse<T extends z.ZodTypeAny>(schema: T, value: unknown, reply: Reply, what: string): z.infer<T> | null {
  const r = schema.safeParse(value ?? undefined);
  if (!r.success) {
    void reply.code(400).send({ error: `Invalid ${what}`, issues: r.error.issues });
    return null;
  }
  return r.data as z.infer<T>;
}

/** One mapping from domain errors to HTTP (shape: `{error, code?}`). */
function sendError(reply: Reply, err: unknown): Reply {
  const e = err as Error;
  if (err instanceof WorkdirError) {
    return reply.code(err.status).send({ error: e.message, code: err.apiCode, ...(err.roots ? { roots: err.roots } : {}) });
  }
  if (err instanceof BusyError) return reply.code(503).send({ error: e.message, code: 'busy' });
  if (err instanceof SessionLiveError || err instanceof LeaseHeldError) {
    return reply.code(409).send({ error: e.message, code: 'session_live' });
  }
  if (err instanceof SpawnError) return reply.code(409).send({ error: e.message, code: 'spawn_failed' });
  if (err instanceof ModelNotFoundError) return reply.code(404).send({ error: e.message });
  if (err instanceof AmbiguousModelError) return reply.code(400).send({ error: e.message, code: 'ambiguous_model' });
  if (err instanceof ModelTransportError) return reply.code(502).send({ error: e.message });
  return reply.code(409).send({ error: e?.message ?? String(err) });
}

export function registerRoutes(
  fastify: FastifyInstance,
  opts: { config: Env; manager: SessionManager; piVersion: string | null; log: FastifyBaseLogger },
): void {
  const { config, manager, piVersion, log } = opts;

  fastify.addHook('onRequest', async (req, reply) => {
    if (req.url === '/api/health') return;
    if (!authorized(req, config.REMOTE_PI_TOKEN)) {
      return reply.code(401).send({ error: 'Unauthorized' });
    }
  });

  const publicConfig: ServerConfig = {
    name: config.REMOTE_PI_NAME ?? os.hostname(),
    piVersion,
    api: '1.0',
    workdir: config.REMOTE_PI_WORKDIR,
    features: ['chat', 'sessions', 'abort', 'history', 'sse', 'contract-v2'],
  };

  const idOf = (req: FastifyRequest, reply: Reply): string | null =>
    parse(ID_PARAM, (req.params as { id?: string }).id, reply, 'session id');

  fastify.get('/api/health', async () => ({ ok: true, name: publicConfig.name }));

  fastify.get('/api/config', async () => publicConfig);

  fastify.get('/api/status', async () => {
    const liveInstances = await manager.getLiveInstances();
    const mem = process.memoryUsage();
    return {
      ...publicConfig,
      maxAgents: config.REMOTE_PI_MAX_AGENTS,
      runningAgents: manager.runningCount,
      liveInstances,
      externalAgents: liveInstances.length,
      memory: { heapUsedMB: Math.round(mem.heapUsed / 1048576), rssMB: Math.round(mem.rss / 1048576) },
      sinks: manager.sinkCounts(),
      sessions: await manager.list(),
    };
  });

  fastify.get('/api/sessions', async (req, reply) => {
    const q = parse(ListQuery, req.query, reply, 'query');
    if (!q) return;
    return manager.listPage(q.limit, q.before);
  });

  fastify.post('/api/sessions', async (req, reply) => {
    const body = parse(CreateSessionSchema, req.body ?? {}, reply, 'body');
    if (!body) return;
    try {
      const session = await manager.create(body.name ?? '', body.clientRequestId, {
        workdir: body.workdir,
        createDir: body.createDir,
      });
      return reply.code(201).send({ session: await manager.summaryOf(session) });
    } catch (err) {
      return sendError(reply, err);
    }
  });

  // CONTRACT §12: directory picker. No `path` → roots/recent/default; with
  // `path` → one level of subdirectories (403 outside the roots).
  fastify.get('/api/dirs', async (req, reply) => {
    const q = parse(DirsQuery, req.query, reply, 'query');
    if (!q) return;
    if (q.path === undefined) {
      return {
        roots: rootPaths(manager.workdirRoots),
        recent: await manager.recentWorkdirs(20),
        default: manager.defaultWorkdir,
      };
    }
    try {
      return listDir(q.path, manager.workdirRoots);
    } catch (err) {
      if (err instanceof WorkdirError) return sendError(reply, err);
      throw err;
    }
  });

  fastify.get('/api/sessions/:id', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const summary = await manager.summary(id);
    if (!summary) return reply.code(404).send({ error: 'Session not found' });
    return { session: summary };
  });

  fastify.post('/api/sessions/:id/question/:requestId', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const requestId = parse(PARAM_TOKEN, (req.params as { requestId?: string }).requestId, reply, 'requestId');
    if (!requestId) return;
    const body = parse(AnswerQuestionSchema, req.body ?? {}, reply, 'body');
    if (!body) return;
    const session = await manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    const res = await session.answerQuestion(requestId, body.answers ?? [], body.cancelled === true);
    if (!res.ok) return reply.code(res.status).send({ error: res.error });
    return { ok: true };
  });

  /*
   * Turn write-guard (leases are the only ownership signal):
   *  - host TUI holds the lease  → proxied to it (202); ?force=1 = graceful
   *    hello+abort+release handshake, then a bridge child;
   *  - lease unreadable          → 409 session_live (retry);
   *  - no lease but the file was written < 30 s ago by a non-lease holder
   *    → 409 session_live unless ?force=1;
   *  - otherwise                 → bridge child (spawned on demand).
   */
  fastify.post('/api/sessions/:id/turn', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const q = parse(TurnQuery, req.query, reply, 'query');
    if (!q) return;
    const body = parse(TurnSchema, req.body, reply, 'body');
    if (!body) return;

    const own = await manager.ownership(id);
    if (!own.session) return reply.code(404).send({ error: 'Session not found' });
    if (own.owner === 'tui' && q.force) {
      const released = await manager.handshakeRelease(id);
      if (!released) {
        return reply.code(409).send({ error: 'Host Pi did not release the session in time', code: 'session_live' });
      }
    } else if (own.owner === 'unknown') {
      return reply.code(409).send({ error: 'Session ownership is temporarily unreadable — retry', code: 'session_live' });
    } else if (own.owner === 'none' && !q.force && await manager.hasUnownedRecentWriter(id)) {
      return reply.code(409).send({
        error: 'Another pi process (without the ownership lease) is writing this session right now. Let it finish, or retry with force.',
        code: 'session_live',
      });
    }

    let session: Session | null;
    try {
      // Use the RETURNED session: ensureRunning can re-key to pi's canonical id.
      session = await manager.ensureRunning(id);
    } catch (err) {
      return sendError(reply, err);
    }
    if (!session) return reply.code(404).send({ error: 'Session not found' });

    try {
      if (body.streamingBehavior) {
        // §10: a brand-new message sent as steer/followUp. Enqueue first so
        // clientMessageId idempotency + the clientMessageId echo reconciliation
        // stay intact, then promote it to immediate delivery with the requested
        // behavior (bypassing the busy gate). A duplicate clientMessageId that
        // already ran/finished returns the same item without re-sending.
        const item = manager.enqueuePrompt(session, body.message, body.clientMessageId);
        if (item.status === 'failed') {
          return reply.code(409).send({ error: item.error ?? 'Previously failed idempotent turn', code: 'turn_failed' });
        }
        if (item.status === 'queued') {
          const result = await manager.steerQueueItem(session.id, item.id, body.streamingBehavior);
          if (result === 'session_live') {
            return reply.code(409).send({ error: 'Session is driven by another writer — retry', code: 'session_live' });
          }
          if (result === 'failed') {
            return reply.code(502).send({ error: 'Steer delivery failed — message left queued', queueItemId: item.id });
          }
        }
        const dispatched = item.status === 'running' || item.status === 'done';
        return reply.code(202).send({
          accepted: true,
          queued: body.clientMessageId ? isActive(item) : !dispatched,
          dispatched,
          queueItemId: item.id,
          queueDepth: session.queue.filter(isActive).length,
        });
      }
      // Everything else goes through the durable queue: dispatched at once
      // when the agent is idle, held (never lost, never doubled) when busy.
      const item = manager.enqueuePrompt(session, body.message, body.clientMessageId);
      if (item.status === 'failed') {
        return reply.code(409).send({ error: item.error ?? 'Previously failed idempotent turn', code: 'turn_failed' });
      }
      manager.dispatchQueuedNow(session);
      const dispatched = item.status === 'running' || item.status === 'done';
      return reply.code(202).send({
        accepted: true,
        // Legacy clients (no clientMessageId) read `queued` as "held behind a busy agent".
        queued: body.clientMessageId ? isActive(item) : !dispatched,
        dispatched,
        queueItemId: item.id,
        queueDepth: session.queue.filter(isActive).length,
      });
    } catch (err) {
      return sendError(reply, err);
    }
  });

  fastify.get('/api/sessions/:id/queue', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const items = await manager.getQueueItems(id);
    if (!items) return reply.code(404).send({ error: 'Session not found' });
    return { items };
  });

  fastify.delete('/api/sessions/:id/queue/:itemId', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const itemId = parse(PARAM_TOKEN, (req.params as { itemId?: string }).itemId, reply, 'itemId');
    if (!itemId) return;
    const ok = await manager.cancelQueueItem(id, itemId);
    if (!ok) return reply.code(404).send({ error: 'Queued item not found' });
    return { ok: true };
  });

  /*
   * §10 steer / send-now: promote a queued item into the running turn NOW
   * (streamingBehavior:"steer"), bypassing the busy gate. Same session_live
   * ownership rule as /turn (a host TUI is fine — proxied; an unleased external
   * writer or an unreadable lease is 409). 404 when it is not a currently
   * `queued` item; 502 when delivery fails (item stays queued).
   */
  fastify.post('/api/sessions/:id/queue/:itemId/steer', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const itemId = parse(PARAM_TOKEN, (req.params as { itemId?: string }).itemId, reply, 'itemId');
    if (!itemId) return;

    const own = await manager.ownership(id);
    if (!own.session) return reply.code(404).send({ error: 'Session not found' });
    if (own.owner === 'unknown') {
      return reply.code(409).send({ error: 'Session ownership is temporarily unreadable — retry', code: 'session_live' });
    }
    if (own.owner === 'none' && await manager.hasUnownedRecentWriter(id)) {
      return reply.code(409).send({
        error: 'Another pi process (without the ownership lease) is writing this session right now. Let it finish, or retry with force.',
        code: 'session_live',
      });
    }

    try {
      const result = await manager.steerQueueItem(id, itemId);
      if (result === 'not_found') return reply.code(404).send({ error: 'Queued item not found' });
      if (result === 'session_live') return reply.code(409).send({ error: 'Session is driven by another writer — retry', code: 'session_live' });
      if (result === 'failed') return reply.code(502).send({ error: 'Steer delivery failed — item left queued' });
      return reply.code(200).send({ ok: true, dispatched: true });
    } catch (err) {
      return sendError(reply, err);
    }
  });

  // Full tool result for a toolCallId (skeleton mode fetch-on-expand).
  fastify.get('/api/sessions/:id/toolresult/:toolCallId', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const toolCallId = parse(PARAM_TOKEN, (req.params as { toolCallId?: string }).toolCallId, reply, 'toolCallId');
    if (!toolCallId) return;
    const session = await manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    try {
      const lines = await readTailLines(session.file, 8 * 1024 * 1024);
      for (let i = lines.length - 1; i >= 0; i--) {
        const line = lines[i]!;
        if (!line.includes(toolCallId)) continue;
        const entry = parseEntry(line);
        const msg = entry?.message as { role?: string; content?: unknown; isError?: boolean; toolCallId?: string } | undefined;
        if (entry?.type !== 'message' || !msg || (msg.role !== 'toolResult' && msg.role !== 'tool')) continue;
        const text = typeof msg.content === 'string'
          ? msg.content
          : Array.isArray(msg.content)
            ? (msg.content as { type?: string; text?: string }[]).filter((b) => b.type === 'text').map((b) => b.text ?? '').join('')
            : '';
        return { toolCallId, text, isError: Boolean(msg.isError) };
      }
      return reply.code(404).send({ error: 'Tool result not found' });
    } catch (err) {
      return reply.code(502).send({ error: (err as Error).message });
    }
  });

  /* ---------------- stats (pi get_session_stats + cached sidecar) ---------------- */

  // Last-known stats per session FILE: in-memory cache + <file>.stats.json
  // sidecar (ignored by the dir watcher). The sidecar is written only when the
  // stats actually changed — never on every GET.
  const statsCache = new Map<string, { stats: Record<string, unknown>; json: string }>();
  const statsSidecar = (file: string): string => `${file}.stats.json`;
  const loadStats = async (file: string): Promise<Record<string, unknown> | null> => {
    const mem = statsCache.get(file);
    if (mem) return mem.stats;
    try {
      const json = await fs.promises.readFile(statsSidecar(file), 'utf8');
      const stats = JSON.parse(json) as Record<string, unknown>;
      statsCache.set(file, { stats, json });
      return stats;
    } catch {
      return null;
    }
  };
  const storeStats = async (file: string, stats: Record<string, unknown>): Promise<void> => {
    const json = JSON.stringify(stats);
    if (statsCache.get(file)?.json === json) return;
    statsCache.set(file, { stats, json });
    try {
      await fs.promises.writeFile(statsSidecar(file), json);
    } catch { /* best effort */ }
  };
  // Verified context windows for models reported by id only.
  const KNOWN_MODEL_CONTEXT_WINDOWS: Record<string, number> = {
    'merge-gateway/zai/glm-5.3-flash': 1_000_000,
    'zai/glm-5.3-flash': 1_000_000,
    'glm-5.3-flash': 1_000_000,
  };
  const windowFor = (modelId: string | null): number => (modelId
    ? KNOWN_MODEL_CONTEXT_WINDOWS[modelId] ?? KNOWN_MODEL_CONTEXT_WINDOWS[modelId.split('/').pop() ?? ''] ?? 0
    : 0);
  /** Context usage from get_state, else from the LAST assistant usage in the file tail. */
  const estimateContextUsage = async (session: Session): Promise<Record<string, unknown> | null> => {
    let contextWindow = 0;
    let modelId: string | null = null;
    if (session.running) {
      try {
        const st = await session.request<{
          model?: unknown;
          contextUsage?: { tokens?: number | null; contextWindow?: number | null; percent?: number | null } | null;
        }>({ type: 'get_state' }, 8_000);
        const cu = st.contextUsage;
        if (cu && typeof cu.percent === 'number' && typeof cu.tokens === 'number'
            && typeof cu.contextWindow === 'number' && cu.contextWindow > 0) {
          return { tokens: cu.tokens, contextWindow: cu.contextWindow, percent: cu.percent };
        }
        const model = st.model;
        if (typeof model === 'string') modelId = model;
        else if (model && typeof model === 'object') {
          const m = model as { id?: string; provider?: string; modelId?: string; contextWindow?: number };
          if (typeof m.contextWindow === 'number' && m.contextWindow > 0) contextWindow = m.contextWindow;
          if (typeof m.id === 'string') modelId = m.id;
          else if (typeof m.provider === 'string' && typeof m.modelId === 'string') modelId = `${m.provider}/${m.modelId}`;
        }
      } catch { /* fall through to the file */ }
    }
    if (!contextWindow) contextWindow = windowFor(modelId);
    try {
      const lines = await readTailLines(session.file, 4 * 1024 * 1024);
      let usageTokens = 0;
      for (let i = lines.length - 1; i >= 0; i--) {
        const entry = parseEntry(lines[i]!) as {
          type?: string;
          provider?: string;
          modelId?: string;
          message?: { role?: string; stopReason?: string; usage?: { totalTokens?: number; input?: number; output?: number; cacheRead?: number; cacheWrite?: number } };
        } | null;
        if (!entry) continue;
        if (!usageTokens && entry.type === 'message' && entry.message?.role === 'assistant'
            && entry.message.stopReason !== 'aborted' && entry.message.stopReason !== 'error' && entry.message.usage) {
          const u = entry.message.usage;
          usageTokens = u.totalTokens ?? (u.input ?? 0) + (u.output ?? 0) + (u.cacheRead ?? 0) + (u.cacheWrite ?? 0);
        }
        if (!modelId && entry.type === 'model_change' && typeof entry.modelId === 'string') {
          modelId = entry.provider ? `${entry.provider}/${entry.modelId}` : entry.modelId;
        }
        if (usageTokens && modelId) break;
      }
      if (!contextWindow) contextWindow = windowFor(modelId);
      if (contextWindow > 0 && usageTokens > 0) {
        return { tokens: usageTokens, contextWindow, percent: Math.round((usageTokens / contextWindow) * 10_000) / 100 };
      }
    } catch { /* file unreadable */ }
    return null;
  };
  // The iOS stats decode is strict: drop null-valued contextUsage keys; omit
  // contextUsage entirely when unusable.
  const sanitizeStats = (stats: Record<string, unknown>): Record<string, unknown> => {
    const cu = stats.contextUsage;
    if (!cu || typeof cu !== 'object') return stats;
    const clean: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(cu as Record<string, unknown>)) {
      if (v !== null && v !== undefined) clean[k] = v;
    }
    const out = { ...stats };
    if (typeof clean.percent !== 'number') delete out.contextUsage;
    else out.contextUsage = clean;
    return out;
  };
  const EMPTY_STATS = { tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 }, cost: 0 };
  fastify.get('/api/sessions/:id/stats', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const session = await manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    const fileKey = path.resolve(session.file);
    if (session.running) {
      try {
        const clean = sanitizeStats(await session.request<Record<string, unknown>>({ type: 'get_session_stats' }, 15_000));
        await storeStats(fileKey, clean);
        return { stats: clean };
      } catch { /* older owner extension etc. — estimate below */ }
    }
    const base = (await loadStats(fileKey)) ?? null;
    const estimated = await estimateContextUsage(session);
    if (!base && !estimated) return reply.code(409).send({ error: 'Session is not running' });
    const clean = sanitizeStats(estimated ? { ...(base ?? EMPTY_STATS), contextUsage: estimated } : (base ?? EMPTY_STATS));
    await storeStats(fileKey, clean);
    return { stats: clean, stale: true };
  });

  /* ---------------- messages ---------------- */

  fastify.get('/api/sessions/:id/messages', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const q = parse(MessagesQuery, req.query, reply, 'query');
    if (!q) return;
    const session = await manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    const pendingRows = () => manager.pendingItems(session).map((item) => ({
      id: item.id,
      clientMessageId: item.clientMessageId ?? null,
      text: item.message,
      message: item.message,
      status: item.status,
      queuedAt: item.queuedAt,
    }));
    // CONTRACT §3: user rows the queue delivered carry the sender's
    // clientMessageId (matched by the echo timestamp recorded at delivery), so
    // clients reconcile their chips by id instead of by text.
    const cidByTs = new Map<number, string>();
    for (const item of session.queue) {
      if (item.clientMessageId && typeof item.messageTimestamp === 'number') {
        cidByTs.set(item.messageTimestamp, item.clientMessageId);
      }
    }
    const withIds = <T extends { role?: string; timestamp?: number | null }>(rows: T[]): T[] =>
      cidByTs.size === 0 ? rows : rows.map((m) => {
        const cid = m.role === 'user' && typeof m.timestamp === 'number' ? cidByTs.get(m.timestamp) : undefined;
        return cid ? { ...m, clientMessageId: cid } : m;
      });
    try {
      const all = withIds(await manager.history(session));
      const pool = q.visible ? all.filter(isVisibleMessage) : all;
      if (q.since) {
        // Durable cursor: only rows strictly after the entry id the client saw.
        const at = all.findIndex((m) => m.id === q.since);
        if (at >= 0) {
          const tail = all.slice(at + 1);
          return {
            messages: (q.visible ? tail.filter(isVisibleMessage) : tail).slice(-q.limit),
            pending: pendingRows(),
            working: await manager.working(session, all),
            hasMore: false,
            cursorMode: true,
          };
        }
        // Unknown cursor (compaction/branch switch) — full fetch below.
      }
      const before = q.before;
      const older = before ? pool.filter((m) => (m.timestamp ?? 0) < before) : pool;
      const page = older.slice(-q.limit);
      return {
        messages: page,
        // CONTRACT §2: queued/running items only, with status + clientMessageId.
        pending: before ? undefined : pendingRows(),
        working: before ? undefined : await manager.working(session, all),
        hasMore: older.length > page.length,
        cursorMode: false,
        total: all.length,
        ...(q.visible ? { visibleTotal: pool.length } : {}),
      };
    } catch (err) {
      return reply.code(500).send({ error: (err as Error).message });
    }
  });

  fastify.get('/api/sessions/:id/state', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const session = await manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    const summary = await manager.summaryOf(session);
    if (!session.running) return { session: summary, state: { running: false }, status: session.statusFrame() };
    try {
      const state = await session.request({ type: 'get_state' });
      return { session: summary, state, status: session.statusFrame() };
    } catch (err) {
      return { session: summary, state: { error: (err as Error).message }, status: session.statusFrame() };
    }
  });

  /** Mutations a host TUI cannot serve over IPC: refuse while it owns the session. */
  const rejectHostOwned = async (id: string, reply: Reply): Promise<boolean> => {
    if (!(await manager.isHostOwned(id))) return false;
    await reply.code(409).send({ error: 'Session is owned by a host pi TUI — do this in the terminal', code: 'session_live' });
    return true;
  };

  fastify.post('/api/sessions/:id/name', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const body = parse(RenameSchema, req.body, reply, 'body');
    if (!body) return;
    if (await rejectHostOwned(id, reply)) return;
    try {
      const session = await manager.rename(id, body.name);
      if (!session) return reply.code(404).send({ error: 'Session not found' });
      return { session };
    } catch (err) {
      return sendError(reply, err);
    }
  });

  /**
   * §11 model picker. A host TUI is fine (proxied to its owner extension);
   * an unleased external writer or an unreadable lease is 409 `session_live`.
   */
  const rejectExternalWriter = async (id: string, reply: Reply): Promise<boolean> => {
    const own = await manager.ownership(id);
    if (!own.session) {
      await reply.code(404).send({ error: 'Session not found' });
      return true;
    }
    if (own.owner === 'unknown') {
      await reply.code(409).send({ error: 'Session ownership is temporarily unreadable — retry', code: 'session_live' });
      return true;
    }
    if (own.owner === 'none' && await manager.hasUnownedRecentWriter(id)) {
      await reply.code(409).send({
        error: 'Another pi process (without the ownership lease) is writing this session right now. Let it finish, then retry.',
        code: 'session_live',
      });
      return true;
    }
    return false;
  };

  fastify.get('/api/sessions/:id/models', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    if (await rejectExternalWriter(id, reply)) return;
    try {
      return await manager.listModels(id);
    } catch (err) {
      return sendError(reply, err);
    }
  });

  fastify.post('/api/sessions/:id/models', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const body = parse(SetModelSchema, req.body, reply, 'body');
    if (!body) return;
    if (await rejectExternalWriter(id, reply)) return;
    try {
      const model = await manager.setModel(id, body.modelId, body.provider);
      return { ok: true, model };
    } catch (err) {
      return sendError(reply, err);
    }
  });

  fastify.post('/api/sessions/:id/models/cycle', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    if (!EmptyBody.safeParse(req.body).success) return reply.code(400).send({ error: 'Invalid body' });
    if (await rejectExternalWriter(id, reply)) return;
    try {
      return await manager.cycleModel(id);
    } catch (err) {
      return sendError(reply, err);
    }
  });

  // Abort reaches whichever transport owns the session (child → `abort`,
  // TUI owner → `op:"abort"`) and reports the REAL result.
  fastify.post('/api/sessions/:id/abort', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const session = await manager.find(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    let target: Session | null = session;
    if (!session.running && await manager.getLiveLeaseInfo(id)) {
      try {
        target = await manager.ensureRunning(id); // attach to the TUI owner
      } catch (err) {
        return sendError(reply, err);
      }
    }
    if (!target?.running) return { ok: false, error: 'Agent not running' };
    const ok = await target.abort();
    return ok ? { ok: true } : { ok: false, error: 'Abort was not acknowledged by the agent' };
  });

  fastify.post('/api/sessions/:id/fork', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const body = parse(ForkSchema, req.body ?? {}, reply, 'body');
    if (!body) return;
    // Fork runs in a dedicated child that only READS the source file, so it
    // is safe for TUI-owned sessions too; never mid-turn.
    const src = await manager.find(id);
    if (!src) return reply.code(404).send({ error: 'Session not found' });
    if (src.isBusy) return reply.code(409).send({ error: 'Agent is busy — fork when it finishes' });
    const entryId = body.entryId ?? await manager.lastUserEntryId(id);
    if (!entryId) return reply.code(400).send({ error: 'No forkable user message found' });
    try {
      const result = await manager.fork(id, entryId, body.name, body.clientRequestId);
      if (!result) return reply.code(404).send({ error: 'Session not found' });
      return reply.code(201).send({ session: result.session, forkedFrom: result.text });
    } catch (err) {
      return sendError(reply, err);
    }
  });

  fastify.delete('/api/sessions/:id', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    const q = parse(DeleteQuery, req.query, reply, 'query');
    if (!q) return;
    if (q.purge) {
      // Never delete a file another writer owns — not even with force.
      if (await manager.isHostOwned(id)) {
        return reply.code(409).send({ error: 'Session is owned by a host pi TUI — close it there before deleting', code: 'session_live' });
      }
      if (!q.force && await manager.hasUnownedRecentWriter(id)) {
        return reply.code(409).send({ error: 'Session file is being written by another pi process', code: 'session_live' });
      }
    }
    const removed = await manager.delete(id, q.purge);
    if (!removed) return reply.code(404).send({ error: 'Session not found' });
    return { ok: true };
  });

  fastify.get('/api/sessions/:id/events', async (req, reply) => {
    const id = idOf(req, reply);
    if (!id) return;
    if (!parse(EventsQuery, req.query, reply, 'query')) return;
    // Never spawns a child just to watch: TUI-owned → owner proxy; otherwise
    // the session's own transport, or a read-only file tailer.
    const session = await manager.sessionForSSE(id);
    if (!session) return reply.code(404).send({ error: 'Session not found' });
    attachSse(req, reply, session, log, { queueSnapshot: () => manager.pendingItems(session) });
  });
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
