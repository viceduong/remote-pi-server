import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify';
import { pino } from 'pino';
import { SessionManager } from '../src/manager.js';
import { registerRoutes } from '../src/routes.js';
import { acquireLease } from '../src/lease.js';
import { loadConfig } from '../src/config.js';
import { normalizeModelList, resolveModel } from '../src/models.js';
import type { RingRecord } from '../src/session.js';

const log = pino({ enabled: false });
const tick = (ms = 40) => new Promise((r) => setTimeout(r, ms));

/** pi-ai Model<Api>-shaped objects (extra fields must be dropped). */
const piModel = (provider: string, id: string, name: string, extra: Record<string, unknown> = {}) => ({
  id, name, provider, api: 'anthropic-messages', baseUrl: 'https://x', reasoning: true,
  input: ['text'], cost: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 64000, ...extra,
});
const CATALOG = [
  piModel('zai', 'glm-4.6', 'GLM 4.6', { reasoning: false, contextWindow: 128000 }),
  piModel('anthropic', 'claude-sonnet-4-5', 'Claude Sonnet 4.5'),
  piModel('anthropic', 'claude-opus-4-1', 'Claude Opus 4.1'),
  piModel('openrouter', 'claude-sonnet-4-5', 'Claude Sonnet 4.5 (OR)'),
  piModel('anthropic', 'claude-opus-4-1', 'Claude Opus 4.1 dup'),
];

describe('§11 normalizer', () => {
  it('normalizes pi Model objects: slim fields, dedupe provider+id, sort provider then name', () => {
    const out = normalizeModelList({ models: CATALOG });
    expect(out).toEqual([
      { provider: 'anthropic', id: 'claude-opus-4-1', name: 'Claude Opus 4.1', contextWindow: 200000, reasoning: true },
      { provider: 'anthropic', id: 'claude-sonnet-4-5', name: 'Claude Sonnet 4.5', contextWindow: 200000, reasoning: true },
      { provider: 'openrouter', id: 'claude-sonnet-4-5', name: 'Claude Sonnet 4.5 (OR)', contextWindow: 200000, reasoning: true },
      { provider: 'zai', id: 'glm-4.6', name: 'GLM 4.6', contextWindow: 128000, reasoning: false },
    ]);
    expect(normalizeModelList(CATALOG)).toHaveLength(4); // bare array accepted too
    expect(normalizeModelList([{ id: 'x' }, null, { provider: 'p', id: 'y' }])).toEqual([{ provider: 'p', id: 'y', name: 'y' }]);
  });

  it('resolves provider+id exactly; legacy id only when unambiguous; provider/id string form', () => {
    const list = normalizeModelList(CATALOG);
    expect(resolveModel(list, 'claude-sonnet-4-5', 'openrouter')).toMatchObject({ ok: true, model: { provider: 'openrouter' } });
    expect(resolveModel(list, 'glm-4.6')).toMatchObject({ ok: true, model: { provider: 'zai' } });
    expect(resolveModel(list, 'claude-sonnet-4-5')).toMatchObject({ ok: false, reason: 'ambiguous' });
    expect(resolveModel(list, 'anthropic/claude-sonnet-4-5')).toMatchObject({ ok: true, model: { provider: 'anthropic' } });
    expect(resolveModel(list, 'nope')).toMatchObject({ ok: false, reason: 'not_found' });
    expect(resolveModel(list, 'glm-4.6', 'anthropic')).toMatchObject({ ok: false, reason: 'not_found' });
  });
});

/* ---------------- fake owner (TUI) ---------------- */

function pipePath(): string {
  const name = `rpi-models-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  return process.platform === 'win32' ? `\\\\.\\pipe\\${name}` : path.join(os.tmpdir(), `${name}.sock`);
}

interface FakeOwner {
  ipc: string;
  received: Record<string, unknown>[];
  sockets: net.Socket[];
  current: { provider: string; id: string };
  close(): Promise<void>;
}

/** v6 owner by default; `protocol: 5` mimics the old extension (no get_available_models). */
async function fakeOwner(opts: { protocol?: number; echoEventFirst?: boolean } = {}): Promise<FakeOwner> {
  const protocol = opts.protocol ?? 6;
  const ipc = pipePath();
  const received: Record<string, unknown>[] = [];
  const sockets: net.Socket[] = [];
  const self = { current: { provider: 'anthropic', id: 'claude-opus-4-1' } } as FakeOwner;
  const entry = (m: (typeof CATALOG)[number]) => ({ provider: m.provider, id: m.id, name: m.name, contextWindow: m.contextWindow, reasoning: m.reasoning });
  let seq = 0;
  const server = net.createServer((socket) => {
    sockets.push(socket);
    let buf = '';
    const send = (o: unknown) => socket.write(`${JSON.stringify(o)}\n`);
    const ok = (id: unknown, data: unknown) => send({ type: 'response', id, success: true, data });
    const fail = (id: unknown, error: string) => send({ type: 'response', id, success: false, error });
    socket.on('data', (d) => {
      buf += d.toString('utf8');
      let i: number;
      while ((i = buf.indexOf('\n')) >= 0) {
        const msg = JSON.parse(buf.slice(0, i)) as Record<string, unknown>;
        buf = buf.slice(i + 1);
        received.push(msg);
        const cur = CATALOG.find((m) => m.provider === self.current.provider && m.id === self.current.id)!;
        if (msg.op === 'hello') {
          ok('hello', protocol >= 6
            ? { eventSeq: 0, isStreaming: false, protocolVersion: 6, capabilities: ['models'], model: cur.id, currentModel: entry(cur) }
            : { eventSeq: 0, isStreaming: false, protocolVersion: 5, model: cur.id });
        } else if (msg.op === 'get_state') {
          ok(msg.id, protocol >= 6 ? { isStreaming: false, model: cur.id, currentModel: entry(cur) } : { isStreaming: false, model: cur.id });
        } else if (msg.op === 'get_available_models' && protocol >= 6) {
          ok(msg.id, { models: CATALOG.map(entry), current: { ...self.current } });
        } else if (msg.op === 'set_model') {
          const t = CATALOG.find((m) => m.id === msg.modelId && (!msg.provider || m.provider === msg.provider));
          if (!t) { fail(msg.id, `unknown model ${String(msg.modelId)}`); continue; }
          self.current = { provider: t.provider, id: t.id };
          const ev = { type: 'event', seq: ++seq, eventType: 'model_changed', event: { type: 'model_changed', provider: t.provider, id: t.id, name: t.name, source: 'set' } };
          if (protocol >= 6 && opts.echoEventFirst) send(ev); // pi's model_select fires inside setModel
          ok(msg.id, protocol >= 6 ? { model: t.id, selected: entry(t) } : { model: t.id });
        } else if (msg.op === 'get_available_models') {
          fail(msg.id, 'unknown op get_available_models');
        } else {
          ok(msg.id, {});
        }
      }
    });
    socket.on('error', () => undefined);
  });
  await new Promise<void>((r) => server.listen(ipc, r));
  return Object.assign(self, {
    ipc, received, sockets,
    close: () => new Promise<void>((r) => { for (const s of sockets) s.destroy(); server.close(() => r()); }),
  });
}

/* ---------------- fake RPC child (bridge) ---------------- */

/** A `pi --mode rpc` stand-in that implements pi 0.85's model commands verbatim. */
function writeFakePi(dir: string): string {
  const script = path.join(dir, 'fake-pi.mjs');
  fs.writeFileSync(script, `
import readline from 'node:readline';
const CATALOG = ${JSON.stringify(CATALOG)};
let current = CATALOG[1];
const out = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  const c = JSON.parse(line);
  const ok = (data) => out({ id: c.id, type: 'response', command: c.type, success: true, ...(data === undefined ? {} : { data }) });
  const err = (error) => out({ id: c.id, type: 'response', command: c.type, success: false, error });
  switch (c.type) {
    case 'get_state': return ok({ model: current, isStreaming: false, messageCount: 0 });
    case 'get_available_models': return ok({ models: CATALOG });
    case 'set_model': {
      const m = CATALOG.find((x) => x.provider === c.provider && x.id === c.modelId);
      if (!m) return err('Model not found: ' + c.provider + '/' + c.modelId);
      current = m; return ok(m);
    }
    case 'cycle_model': { const i = CATALOG.indexOf(current); current = CATALOG[(i + 1) % CATALOG.length]; return ok({ model: current, thinkingLevel: 'off', isScoped: false }); }
    default: return ok();
  }
});
`);
  if (process.platform === 'win32') {
    const cmd = path.join(dir, 'fake-pi.cmd');
    fs.writeFileSync(cmd, `@"${process.execPath}" "${script}" %*\r\n`);
    return cmd;
  }
  const sh = path.join(dir, 'fake-pi');
  fs.writeFileSync(sh, `#!/bin/sh\nexec "${process.execPath}" "${script}" "$@"\n`, { mode: 0o755 });
  return sh;
}

/* ---------------- harness ---------------- */

let root: string;
let owner: FakeOwner | null;
let app: FastifyInstance | null;
let manager: SessionManager | null;
const auth = { authorization: 'Bearer tok' };

const writeSession = (id: string): string => {
  const dir = path.join(root, '--proj--');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${id}.jsonl`);
  fs.writeFileSync(file, `${JSON.stringify({ type: 'session', id, cwd: root })}\n`);
  return file;
};

async function startApp(bin = 'pi-not-installed-xyz'): Promise<void> {
  manager = new SessionManager({ sessionDir: root, workdir: root, bin, maxAgents: 2, idleKillMs: 60_000, extraArgs: [], log, noTimers: true });
  app = Fastify();
  registerRoutes(app, {
    config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: root }),
    manager, piVersion: 'test', log: log as unknown as FastifyBaseLogger,
  });
  await app.ready();
}

async function tuiSession(id: string, ownerOpts: Parameters<typeof fakeOwner>[0] = {}) {
  owner = await fakeOwner(ownerOpts);
  const file = writeSession(id);
  acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
  await startApp();
  const session = (await manager!.ensureRunning(id))!;
  expect(session.isProxy).toBe(true);
  const frames: RingRecord[] = [];
  session.subscribe({ send: (r) => frames.push(r), skeleton: true });
  return { session, frames, modelFrames: () => frames.filter((f) => f.type === 'model_changed').map((f) => f.data) };
}

beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-models-')); owner = null; app = null; manager = null; });
afterEach(async () => {
  manager?.stopAll();
  await app?.close();
  await owner?.close();
  await tick(100);
  fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

describe('§11 model picker — TUI owner', () => {
  const ID = '0199eeee-ffff-7aaa-8bbb-ccccdddd0001';

  it('GET /models (v6): normalized sorted list + current, via owner op get_available_models', async () => {
    await tuiSession(ID);
    const res = await app!.inject({ method: 'GET', url: `/api/sessions/${ID}/models`, headers: auth });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { models: { provider: string; id: string }[]; current: unknown };
    expect(body.current).toEqual({ provider: 'anthropic', id: 'claude-opus-4-1' });
    expect(body.models.map((m) => `${m.provider}/${m.id}`)).toEqual([
      'anthropic/claude-opus-4-1', 'anthropic/claude-sonnet-4-5', 'openrouter/claude-sonnet-4-5', 'zai/glm-4.6',
    ]);
    expect(body.models[0]).toEqual({ provider: 'anthropic', id: 'claude-opus-4-1', name: 'Claude Opus 4.1', contextWindow: 200000, reasoning: true });
    expect(owner!.received.some((m) => m.op === 'get_available_models')).toBe(true);
  });

  it('POST /models sends {provider, modelId}; one model_changed even when the owner echoes model_select first', async () => {
    const { session, modelFrames } = await tuiSession(ID, { echoEventFirst: true });
    const res = await app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models`, headers: auth, payload: { provider: 'zai', modelId: 'glm-4.6' } });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true, model: { provider: 'zai', id: 'glm-4.6', name: 'GLM 4.6' } });
    expect(owner!.received.find((m) => m.op === 'set_model')).toMatchObject({ provider: 'zai', modelId: 'glm-4.6' });
    await tick();
    expect(modelFrames()).toEqual([{ type: 'model_changed', provider: 'zai', id: 'glm-4.6', name: 'GLM 4.6' }]);
    expect(session.model).toBe('zai/glm-4.6');
    expect((await manager!.summary(ID))?.model).toBe('zai/glm-4.6');
  });

  it('TUI-side switch (owner model_changed event) → model_changed frame + summary model', async () => {
    const { session, modelFrames } = await tuiSession(ID);
    owner!.sockets[0]!.write(`${JSON.stringify({ type: 'event', seq: 1, eventType: 'model_changed', event: { type: 'model_changed', provider: 'openrouter', id: 'claude-sonnet-4-5', name: 'Claude Sonnet 4.5 (OR)', source: 'cycle' } })}\n`);
    await tick();
    expect(modelFrames()).toEqual([{ type: 'model_changed', provider: 'openrouter', id: 'claude-sonnet-4-5', name: 'Claude Sonnet 4.5 (OR)' }]);
    expect(session.model).toBe('openrouter/claude-sonnet-4-5');
    // Not in the replay ring.
    expect(session.ringSnapshot().some((r) => r.type === 'model_changed')).toBe(false);
  });

  it('legacy {modelId}: unique id accepted, ambiguous → 400, unknown → 404', async () => {
    await tuiSession(ID);
    const post = (payload: unknown) => app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models`, headers: auth, payload: payload as object });
    const ok = await post({ modelId: 'glm-4.6' });
    expect(ok.statusCode).toBe(200);
    expect(owner!.received.filter((m) => m.op === 'set_model').at(-1)).toMatchObject({ provider: 'zai', modelId: 'glm-4.6' });
    const amb = await post({ modelId: 'claude-sonnet-4-5' });
    expect(amb.statusCode).toBe(400);
    expect(amb.json()).toMatchObject({ code: 'ambiguous_model' });
    expect((await post({ modelId: 'nope' })).statusCode).toBe(404);
    expect((await post({ provider: 'zai', modelId: 'nope' })).statusCode).toBe(404);
  });

  it('v5 owner: GET → 502 telling to update the owner extension; POST with provider still works', async () => {
    const { modelFrames } = await tuiSession(ID, { protocol: 5 });
    const res = await app!.inject({ method: 'GET', url: `/api/sessions/${ID}/models`, headers: auth });
    expect(res.statusCode).toBe(502);
    expect(res.json().error).toMatch(/update the host pi owner extension/);
    expect(owner!.received.some((m) => m.op === 'get_available_models')).toBe(false);

    const set = await app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models`, headers: auth, payload: { provider: 'zai', modelId: 'glm-4.6' } });
    expect(set.statusCode).toBe(200);
    expect(set.json()).toEqual({ ok: true, model: { provider: 'zai', id: 'glm-4.6', name: 'glm-4.6' } });
    await tick();
    expect(modelFrames()).toHaveLength(1);
  });

  it('cycle on a TUI owner sets the next available model', async () => {
    await tuiSession(ID);
    const res = await app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models/cycle`, headers: auth });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ modelId: 'claude-sonnet-4-5', model: { provider: 'anthropic', id: 'claude-sonnet-4-5', name: 'Claude Sonnet 4.5' } });
  });
});

describe('§11 model picker — bridge RPC child', () => {
  const ID = '0199eeee-ffff-7aaa-8bbb-ccccdddd0002';

  it('GET lists via get_available_models + get_state; POST sends provider; cycle reads data.model', async () => {
    writeSession(ID);
    await startApp(writeFakePi(root));
    const session = (await manager!.ensureRunning(ID))!;
    expect(session.hasChild).toBe(true);
    const frames: RingRecord[] = [];
    session.subscribe({ send: (r) => frames.push(r) });
    expect(session.model).toBe('anthropic/claude-sonnet-4-5');

    const list = await app!.inject({ method: 'GET', url: `/api/sessions/${ID}/models`, headers: auth });
    expect(list.statusCode).toBe(200);
    expect(list.json().current).toEqual({ provider: 'anthropic', id: 'claude-sonnet-4-5' });
    expect(list.json().models).toHaveLength(4);

    const set = await app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models`, headers: auth, payload: { provider: 'openrouter', modelId: 'claude-sonnet-4-5' } });
    expect(set.statusCode).toBe(200);
    expect(set.json()).toEqual({ ok: true, model: { provider: 'openrouter', id: 'claude-sonnet-4-5', name: 'Claude Sonnet 4.5 (OR)' } });
    expect(session.model).toBe('openrouter/claude-sonnet-4-5');

    const bad = await app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models`, headers: auth, payload: { provider: 'zai', modelId: 'claude-sonnet-4-5' } });
    expect(bad.statusCode).toBe(404);

    const cyc = await app!.inject({ method: 'POST', url: `/api/sessions/${ID}/models/cycle`, headers: auth });
    expect(cyc.statusCode).toBe(200);
    expect(cyc.json()).toMatchObject({ modelId: 'claude-opus-4-1', model: { provider: 'anthropic', id: 'claude-opus-4-1' } });

    expect(frames.filter((f) => f.type === 'model_changed').map((f) => `${f.data.provider}/${f.data.id}`))
      .toEqual(['openrouter/claude-sonnet-4-5', 'anthropic/claude-opus-4-1']);
  }, 20_000);
});
