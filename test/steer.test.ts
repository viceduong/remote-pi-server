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

const log = pino({ enabled: false });

function pipePath(): string {
  const name = `rpi-steer-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  return process.platform === 'win32' ? `\\\\.\\pipe\\${name}` : path.join(os.tmpdir(), `${name}.sock`);
}

interface FakeOwner {
  ipc: string;
  received: Record<string, unknown>[];
  sockets: net.Socket[];
  close(): Promise<void>;
}

/**
 * Minimal owner stand-in. `acceptPrompt=false` makes it reject the
 * prompt/steer/enqueue op (reproduces an owner rejecting a steer → 502).
 */
async function fakeOwner(acceptPrompt = true, settleOnAbort = true): Promise<FakeOwner> {
  let seq = 100;
  const ipc = pipePath();
  const received: Record<string, unknown>[] = [];
  const sockets: net.Socket[] = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    let buf = '';
    socket.on('data', (d) => {
      buf += d.toString('utf8');
      let i: number;
      while ((i = buf.indexOf('\n')) >= 0) {
        const msg = JSON.parse(buf.slice(0, i)) as Record<string, unknown>;
        buf = buf.slice(i + 1);
        received.push(msg);
        if (msg.op === 'hello') {
          socket.write(`${JSON.stringify({ type: 'response', id: 'hello', success: true, data: { eventSeq: 0, isStreaming: false } })}\n`);
        } else if (msg.op === 'get_state') {
          socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: { isStreaming: false } })}\n`);
        } else if (msg.op === 'abort') {
          socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: {} })}
`);
          // Like pi: an interrupted turn ends and settles shortly after.
          if (settleOnAbort) {
            setTimeout(() => {
              socket.write(evLine(++seq, 'agent_end'));
              socket.write(evLine(++seq, 'agent_settled'));
            }, 30);
          }
        } else if ((msg.op === 'steer' || msg.op === 'enqueue' || msg.op === 'prompt') && !acceptPrompt) {
          socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: false, error: 'owner refused the steer' })}\n`);
        } else {
          socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: {} })}\n`);
        }
      }
    });
    socket.on('error', () => undefined);
  });
  await new Promise<void>((r) => server.listen(ipc, r));
  return {
    ipc,
    received,
    sockets,
    close: () => new Promise((r) => { for (const s of sockets) s.destroy(); server.close(() => r()); }),
  };
}

const evLine = (seq: number, type: string): string =>
  `${JSON.stringify({ type: 'event', seq, eventType: type, event: { type } })}\n`;
const tick = (ms = 40) => new Promise((r) => setTimeout(r, ms));

let root: string;
let owner: FakeOwner | null;

const writeSession = (id: string): string => {
  const dir = path.join(root, '--proj--');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${id}.jsonl`);
  fs.writeFileSync(file, `${JSON.stringify({ type: 'session', id, cwd: root })}\n`);
  return file;
};
const makeManager = (): SessionManager => new SessionManager({
  sessionDir: root, workdir: root, bin: 'pi-not-installed-xyz', maxAgents: 2, idleKillMs: 60_000,
  extraArgs: [], log, noTimers: true,
});

beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-steer-')); owner = null; });
afterEach(async () => { await owner?.close(); owner = null; fs.rmSync(root, { recursive: true, force: true }); });

describe('§10 steer / send-now — manager.steerQueueItem', () => {
  const ID = '0199dddd-eeee-7fff-8aaa-bbbbcccc0001';

  async function attachedBusySession(manager: SessionManager): Promise<void> {
    owner = await fakeOwner();
    const file = writeSession(ID);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    const s = await manager.ensureRunning(ID);
    expect(s?.isProxy).toBe(true);
    // Drive it busy so the normal queue gate (dispatchQueued → isBusy) holds items.
    owner.sockets[0]!.write(evLine(1, 'agent_start'));
    await tick();
    expect(s!.isBusy).toBe(true);
  }

  it('send-now interrupts the turn, then delivers the item as a FRESH prompt; other items untouched', async () => {
    const manager = makeManager();
    await attachedBusySession(manager);
    const session = (await manager.find(ID))!;

    const a = manager.enqueuePrompt(session, 'steer me', 'cm-a');
    const b = manager.enqueuePrompt(session, 'leave me', 'cm-b');
    manager.dispatchQueuedNow(session); // busy → nothing dispatches
    await tick();
    expect(manager.pendingItems(session).map((i) => i.status)).toEqual(['queued', 'queued']);

    const result = await manager.steerQueueItem(ID, a.id);
    await tick();
    expect(result).toBe('dispatched');

    // Like the host's Alt+Enter: abort first, then a plain prompt (no steer).
    const ops = owner!.received.map((m) => m.op);
    const abortAt = ops.indexOf('abort');
    const promptAt = owner!.received.findIndex((m) => m.op === 'prompt' && m.message === 'steer me');
    expect(abortAt).toBeGreaterThanOrEqual(0);
    expect(promptAt).toBeGreaterThan(abortAt);
    expect(owner!.received.some((m) => m.op === 'steer')).toBe(false);
    // Only the promoted item moved; the other is still queued and never sent.
    expect(a.status).toBe('running');
    expect(b.status).toBe('queued');
    expect(owner!.received.some((m) => m.message === 'leave me')).toBe(false);
    manager.stopAll();
  });

  it('404 (not_found) for an absent id and for an already-running item', async () => {
    const manager = makeManager();
    await attachedBusySession(manager);
    const session = (await manager.find(ID))!;
    expect(await manager.steerQueueItem(ID, 'no-such-item')).toBe('not_found');

    const a = manager.enqueuePrompt(session, 'once', 'cm-once');
    expect(await manager.steerQueueItem(ID, a.id)).toBe('dispatched');
    expect(a.status).toBe('running');
    // Already running → no longer a `queued` item → 404.
    expect(await manager.steerQueueItem(ID, a.id)).toBe('not_found');
    manager.stopAll();
  });

  it('falls back to a steer when the interrupted turn does not settle in time', async () => {
    const manager = makeManager();
    owner = await fakeOwner(true, false); // abort acknowledged, turn never settles
    const file = writeSession(ID);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    await manager.ensureRunning(ID);
    owner.sockets[0]!.write(evLine(1, 'agent_start'));
    await tick();
    const session = (await manager.find(ID))!;
    const a = manager.enqueuePrompt(session, 'late', 'cm-late');
    expect(await manager.steerQueueItem(ID, a.id)).toBe('dispatched');
    expect(owner!.received.find((m) => m.op === 'steer')).toMatchObject({ message: 'late' });
    manager.stopAll();
  }, 15_000);

  it('502 (failed) when the owner rejects the steer — item stays queued, attempt not consumed', async () => {
    const manager = makeManager();
    owner = await fakeOwner(false); // rejects prompt/steer ops
    const file = writeSession(ID);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    await manager.ensureRunning(ID);
    owner.sockets[0]!.write(evLine(1, 'agent_start'));
    await tick();
    const session = (await manager.find(ID))!;

    const a = manager.enqueuePrompt(session, 'will fail', 'cm-fail');
    const result = await manager.steerQueueItem(ID, a.id);
    await tick();
    expect(result).toBe('failed');
    expect(a.status).toBe('queued'); // left queued
    expect(a.attempts ?? 0).toBe(0); // steer did not consume a retry attempt
    manager.stopAll();
  });
});

describe('§10 steer / send-now — HTTP routes', () => {
  let app: FastifyInstance;
  let manager: SessionManager;
  let base: string;
  const auth = { authorization: 'Bearer tok' };
  const ID = '0199dddd-eeee-7fff-8aaa-bbbbcccc0002';

  async function startApp(m: SessionManager): Promise<void> {
    manager = m;
    app = Fastify();
    registerRoutes(app, {
      config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: root }),
      manager, piVersion: 'test', log: log as unknown as FastifyBaseLogger,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    base = `http://127.0.0.1:${(app.server.address() as { port: number }).port}`;
  }

  afterEach(async () => { manager?.stopAll(); await app?.close(); });

  it('POST /queue/:itemId/steer interrupts and sends the queued item now', async () => {
    owner = await fakeOwner();
    const file = writeSession(ID);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    await startApp(makeManager());
    const session = (await manager.ensureRunning(ID))!;
    owner.sockets[0]!.write(evLine(1, 'agent_start'));
    await tick();
    const item = manager.enqueuePrompt(session, 'promote me', 'cm-http');

    const res = await app.inject({ method: 'POST', url: `/api/sessions/${ID}/queue/${item.id}/steer`, headers: auth });
    await tick();
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true, dispatched: true });
    expect(owner.received.some((m) => m.op === 'abort')).toBe(true);
    expect(owner.received.find((m) => m.op === 'prompt' && m.message === 'promote me')).toBeTruthy();

    // Absent item → 404.
    const miss = await app.inject({ method: 'POST', url: `/api/sessions/${ID}/queue/nope/steer`, headers: auth });
    expect(miss.statusCode).toBe(404);
  });

  it('POST /queue/:itemId/steer → 409 session_live when the lease is unreadable', async () => {
    const file = writeSession(ID);
    fs.mkdirSync(`${file}.lease`); // a directory lease = unreadable → ownership unknown
    await startApp(makeManager());
    const res = await app.inject({ method: 'POST', url: `/api/sessions/${ID}/queue/anything/steer`, headers: auth });
    expect(res.statusCode).toBe(409);
    expect(res.json()).toMatchObject({ code: 'session_live' });
  });

  it('POST /turn {streamingBehavior:"steer"} threads through as a steer op and is idempotent', async () => {
    owner = await fakeOwner();
    const file = writeSession(ID);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    await startApp(makeManager());

    const res = await app.inject({
      method: 'POST', url: `/api/sessions/${ID}/turn`, headers: auth,
      payload: { message: 'interrupt now', streamingBehavior: 'steer', clientMessageId: 'cm-turn' },
    });
    await tick();
    expect(res.statusCode).toBe(202);
    const body = res.json() as { dispatched: boolean; queueItemId: string };
    expect(body.dispatched).toBe(true);
    expect(body.queueItemId).toBeTruthy();
    expect(owner.received.filter((m) => m.op === 'steer')).toHaveLength(1);
    expect(owner.received.find((m) => m.op === 'steer')).toMatchObject({ message: 'interrupt now' });

    // Same clientMessageId again → same item, NOT re-sent.
    const again = await app.inject({
      method: 'POST', url: `/api/sessions/${ID}/turn`, headers: auth,
      payload: { message: 'interrupt now', streamingBehavior: 'steer', clientMessageId: 'cm-turn' },
    });
    await tick();
    expect((again.json() as { queueItemId: string }).queueItemId).toBe(body.queueItemId);
    expect(owner.received.filter((m) => m.op === 'steer')).toHaveLength(1);
  });
});
