/**
 * CONTRACT-V2 §15 — cancel pending messages, incl. ones already steered into pi.
 * pi can only clear its WHOLE queue: clear_queue → drop the first occurrence of
 * the item's text → re-queue the rest (incl. terminal-typed ones) in order,
 * with their original kind.
 */
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
import { pendingView } from '../src/queue.js';
import type { RingRecord, Session } from '../src/session.js';

const log = pino({ enabled: false });
const tick = (ms = 40) => new Promise((r) => setTimeout(r, ms));
const auth = { authorization: 'Bearer tok' };

function pipePath(): string {
  const name = `rpi-cancel-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  return process.platform === 'win32' ? `\\\\.\\pipe\\${name}` : path.join(os.tmpdir(), `${name}.sock`);
}

/* ---------------- fake owner (host TUI) with pi's queue semantics ---------------- */

interface FakeOwner {
  ipc: string;
  received: Record<string, unknown>[];
  sockets: net.Socket[];
  steering: string[];
  followUp: string[];
  /** Runs inside clear_queue BEFORE pi clears (e.g. pi delivering a message just before). */
  beforeClear: ((socket: net.Socket) => void) | null;
  emit(type: string, event?: Record<string, unknown>): void;
  close(): Promise<void>;
}

async function fakeOwner(opts: { v7?: boolean } = {}): Promise<FakeOwner> {
  const v7 = opts.v7 ?? true;
  let seq = 100;
  const ipc = pipePath();
  const self = { ipc, received: [], sockets: [], steering: [], followUp: [], beforeClear: null } as unknown as FakeOwner;
  const server = net.createServer((socket) => {
    self.sockets.push(socket);
    let buf = '';
    const ok = (id: unknown, data: unknown = {}) => socket.write(`${JSON.stringify({ type: 'response', id, success: true, data })}\n`);
    const fail = (id: unknown, error: string) => socket.write(`${JSON.stringify({ type: 'response', id, success: false, error })}\n`);
    socket.on('data', (d) => {
      buf += d.toString('utf8');
      let i: number;
      while ((i = buf.indexOf('\n')) >= 0) {
        const msg = JSON.parse(buf.slice(0, i)) as Record<string, unknown>;
        buf = buf.slice(i + 1);
        self.received.push(msg);
        const text = String(msg.message ?? '');
        switch (msg.op) {
          case 'hello':
            socket.write(`${JSON.stringify({
              type: 'response', id: 'hello', success: true,
              data: {
                eventSeq: 0, isStreaming: false, protocolVersion: v7 ? 7 : 6,
                capabilities: ['prompt.streamingBehavior', 'models', ...(v7 ? ['clear_queue'] : [])],
              },
            })}\n`);
            break;
          case 'steer': self.steering.push(text); ok(msg.id, { steered: true }); break;
          case 'enqueue': self.followUp.push(text); ok(msg.id, { queued: true }); break;
          case 'prompt': ok(msg.id, { queued: false, deliverAs: null }); break; // starts a turn
          case 'clear_queue':
            if (!v7) { fail(msg.id, 'unknown op clear_queue'); break; }
            self.beforeClear?.(socket);
            ok(msg.id, { steering: self.steering.splice(0), followUp: self.followUp.splice(0) });
            break;
          default: ok(msg.id);
        }
      }
    });
    socket.on('error', () => undefined);
  });
  await new Promise<void>((r) => server.listen(ipc, r));
  return Object.assign(self, {
    emit: (type: string, event: Record<string, unknown> = {}) => {
      for (const s of self.sockets) s.write(`${JSON.stringify({ type: 'event', seq: ++seq, eventType: type, event: { type, ...event } })}\n`);
    },
    close: () => new Promise<void>((r) => { for (const s of self.sockets) s.destroy(); server.close(() => r()); }),
  });
}

/** pi delivering a queued user message: message_start/_end of the user row. */
function userEcho(o: FakeOwner, text: string, socket?: net.Socket): void {
  const message = { role: 'user', content: [{ type: 'text', text }], timestamp: Date.now() };
  // Like pi: the text leaves the mirror lists at message_start.
  const s = o.steering.indexOf(text);
  if (s >= 0) o.steering.splice(s, 1);
  else { const f = o.followUp.indexOf(text); if (f >= 0) o.followUp.splice(f, 1); }
  const write = (type: string) => {
    const line = `${JSON.stringify({ type: 'event', seq: Date.now(), eventType: type, event: { type, message } })}\n`;
    for (const sk of socket ? [socket] : o.sockets) sk.write(line);
  };
  write('message_start');
  write('message_end');
}

/* ---------------- fake RPC child with pi's queue semantics ---------------- */

function writeFakePi(dir: string): string {
  const script = path.join(dir, 'fake-pi.mjs');
  fs.writeFileSync(script, `
import readline from 'node:readline';
const out = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
let steering = [], followUp = [];
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  const c = JSON.parse(line);
  const ok = (data) => out({ id: c.id, type: 'response', command: c.type, success: true, ...(data === undefined ? {} : { data }) });
  switch (c.type) {
    case 'get_state': return ok({ isStreaming: false, messageCount: 0 });
    case 'prompt':
      if (c.streamingBehavior === 'steer') steering.push(c.message);
      else if (c.streamingBehavior === 'followUp') followUp.push(c.message);
      return ok();
    case 'steer': steering.push(c.message); return ok();
    case 'follow_up': followUp.push(c.message); return ok();
    case 'clear_queue': { const r = { steering, followUp }; steering = []; followUp = []; return ok(r); }
    case 'peek_queue': return ok({ steering, followUp });
    case '__emit': for (const e of c.events) out(e); return ok();
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

/** Busy TUI session (owner proxy); returns the session + its queue_update frames. */
async function busyTui(id: string, v7 = true): Promise<{ session: Session; queueFrames: () => unknown[] }> {
  owner = await fakeOwner({ v7 });
  const file = writeSession(id);
  acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
  await startApp();
  const session = (await manager!.ensureRunning(id))!;
  expect(session.isProxy).toBe(true);
  const frames: RingRecord[] = [];
  session.subscribe({ send: (r) => frames.push(r), skeleton: true });
  owner.emit('agent_start');
  await tick();
  expect(session.isBusy).toBe(true);
  return { session, queueFrames: () => frames.filter((f) => f.type === 'queue_update').map((f) => f.data) };
}

/** Enqueue app messages while busy → the server steers them into pi. */
async function steerIn(session: Session, texts: [string, string][]): Promise<void> {
  for (const [text, cmid] of texts) manager!.enqueuePrompt(session, text, cmid);
  manager!.dispatchQueuedNow(session);
  await tick(80);
}

const del = (id: string, itemId: string) => app!.inject({ method: 'DELETE', url: `/api/sessions/${id}/queue/${itemId}`, headers: auth });
const delByClient = (id: string, cmid: string) => app!.inject({ method: 'DELETE', url: `/api/sessions/${id}/queue/by-client/${cmid}`, headers: auth });
const opsAfterClear = () => {
  const at = owner!.received.findIndex((m) => m.op === 'clear_queue');
  return owner!.received.slice(at + 1).map((m) => [m.op, m.message]);
};

beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-cancel-')); owner = null; app = null; manager = null; });
afterEach(async () => {
  manager?.stopAll();
  await app?.close();
  await owner?.close();
  await tick(100);
  fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

describe('§15 cancel a steered item — TUI owner v7', () => {
  const ID = '0199ffff-1111-7aaa-8bbb-ccccdddd0001';

  it('clears pi queue, drops only that text, re-queues the rest (terminal-typed included) in order', async () => {
    const { session, queueFrames } = await busyTui(ID);
    owner!.steering.push('typed in terminal'); // already in pi's queue, unknown to the server
    owner!.followUp.push('terminal follow-up');
    await steerIn(session, [['app one', 'cm-1'], ['app two', 'cm-2'], ['app three', 'cm-3']]);
    expect(owner!.steering).toEqual(['typed in terminal', 'app one', 'app two', 'app three']);
    const view = manager!.pendingItems(session);
    expect(view.map((v) => [v.message, v.status, v.steered])).toEqual([
      ['app one', 'running', true], ['app two', 'running', true], ['app three', 'running', true],
    ]);

    const two = session.queue.find((i) => i.clientMessageId === 'cm-2')!;
    const framesBefore = queueFrames().length;
    const res = await del(ID, two.id);
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true });

    // Re-queued in the original order with the original kinds.
    expect(opsAfterClear()).toEqual([
      ['steer', 'typed in terminal'], ['steer', 'app one'], ['steer', 'app three'], ['enqueue', 'terminal follow-up'],
    ]);
    expect(owner!.steering).toEqual(['typed in terminal', 'app one', 'app three']);
    expect(owner!.followUp).toEqual(['terminal follow-up']);
    expect(two.status).toBe('cancelled');
    expect(manager!.pendingItems(session).map((v) => v.message)).toEqual(['app one', 'app three']);
    const last = queueFrames().slice(framesBefore).pop() as { items: { message: string }[] };
    expect(last.items.map((i) => i.message)).toEqual(['app one', 'app three']);

    // The others still reconcile by their echo; repeat DELETE is idempotent.
    userEcho(owner!, 'app one');
    await tick();
    expect(session.queue.find((i) => i.clientMessageId === 'cm-1')!.echoedAt).toBeTruthy();
    // Delivered into the running turn → finished, no longer pending.
    expect(pendingView(session.queue).find((v) => v.clientMessageId === 'cm-1')).toBeUndefined();
    expect((await del(ID, two.id)).statusCode).toBe(200);
  });

  it('a steered item is done as soon as pi echoes it, while the long turn keeps running', async () => {
    const { session, queueFrames } = await busyTui(ID);
    await steerIn(session, [['mid-turn note', 'cm-mid']]);
    const item = session.queue.find((i) => i.clientMessageId === 'cm-mid')!;
    expect(item.status).toBe('running');
    const before = queueFrames().length;
    userEcho(owner!, 'mid-turn note');
    await tick();
    expect(session.isBusy).toBe(true); // the agent's turn is still going
    expect(item.status).toBe('done');
    expect(manager!.pendingItems(session).some((v) => v.clientMessageId === 'cm-mid')).toBe(false);
    const last = queueFrames().slice(before).pop() as { items: { clientMessageId?: string }[] };
    expect(last.items.some((i) => i.clientMessageId === 'cm-mid')).toBe(false);
  });

  it('a follow-up item keeps its kind; cancelling the follow-up itself removes it from followUp', async () => {
    const { session } = await busyTui(ID);
    const fu = manager!.enqueuePrompt(session, 'later please', 'cm-fu');
    expect(await manager!.steerQueueItem(ID, fu.id, 'followUp')).toBe('dispatched');
    await steerIn(session, [['steer me', 'cm-s']]);
    expect(owner!.followUp).toEqual(['later please']);
    expect(fu.deliveredAs).toBe('followUp');
    expect(pendingView(session.queue).find((v) => v.id === fu.id)!.steered).toBe(true);

    const s = session.queue.find((i) => i.clientMessageId === 'cm-s')!;
    expect((await del(ID, s.id)).statusCode).toBe(200);
    expect(opsAfterClear()).toEqual([['enqueue', 'later please']]);
    expect(owner!.followUp).toEqual(['later please']);

    owner!.received.length = 0;
    expect((await delByClient(ID, 'cm-fu')).statusCode).toBe(200);
    expect(owner!.followUp).toEqual([]);
    expect(fu.status).toBe('cancelled');
  });

  it('echo race: pi delivers the item during clear_queue → 409 already_delivered, nothing lost', async () => {
    const { session } = await busyTui(ID);
    owner!.steering.push('typed in terminal');
    await steerIn(session, [['racer', 'cm-r'], ['other', 'cm-o']]);
    const racer = session.queue.find((i) => i.clientMessageId === 'cm-r')!;
    owner!.beforeClear = (socket) => userEcho(owner!, 'racer', socket);

    const res = await del(ID, racer.id);
    expect(res.statusCode).toBe(409);
    expect(res.json().code).toBe('already_delivered');
    expect(owner!.steering).toEqual(['typed in terminal', 'other']); // everything else re-queued
    expect(racer.status).toBe('done'); // pi delivered it into the turn → finished
    expect(racer.echoedAt).toBeTruthy();
  });

  it('echo race with a duplicate text: the echoed item never removes the identical terminal message', async () => {
    const { session } = await busyTui(ID);
    await steerIn(session, [['continue', 'cm-c']]);
    owner!.steering.push('continue'); // typed in the terminal after the app's
    const c = session.queue.find((i) => i.clientMessageId === 'cm-c')!;
    owner!.beforeClear = (socket) => userEcho(owner!, 'continue', socket);
    const res = await del(ID, c.id);
    expect(res.statusCode).toBe(409);
    expect(owner!.steering).toEqual(['continue']);
  });

  it('already echoed → 409 without touching pi; text not in the queue → 409 with everything re-queued', async () => {
    const { session } = await busyTui(ID);
    await steerIn(session, [['gone', 'cm-g'], ['raced', 'cm-x'], ['keep', 'cm-k']]);
    userEcho(owner!, 'gone');
    await tick();
    const g = session.queue.find((i) => i.clientMessageId === 'cm-g')!;
    const r1 = await del(ID, g.id);
    expect(r1.statusCode).toBe(409);
    expect(r1.json().code).toBe('already_delivered');
    expect(owner!.received.some((m) => m.op === 'clear_queue')).toBe(false);

    // pi already took "raced" out of its queue but the echo is not here yet.
    owner!.steering.splice(owner!.steering.indexOf('raced'), 1);
    const x = session.queue.find((i) => i.clientMessageId === 'cm-x')!;
    const r2 = await del(ID, x.id);
    expect(r2.statusCode).toBe(409);
    expect(r2.json().code).toBe('already_delivered');
    expect(owner!.steering).toEqual(['keep']);
    expect(x.status).toBe('running');
  });

  it('an item that started a turn as a normal prompt → 409 already_delivered', async () => {
    owner = await fakeOwner();
    const file = writeSession(ID);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    await startApp();
    const session = (await manager!.ensureRunning(ID))!;
    const item = manager!.enqueuePrompt(session, 'fresh turn', 'cm-p');
    manager!.dispatchQueuedNow(session);
    await tick(80);
    expect(item.deliveredAs).toBe('prompt');
    expect(item.status).toBe('running');
    const res = await del(ID, item.id);
    expect(res.statusCode).toBe(409);
    expect(res.json().code).toBe('already_delivered');
    expect(owner.received.some((m) => m.op === 'clear_queue')).toBe(false);
  });

  it('by-client route: cancels a queued item, 404 for an unknown clientMessageId', async () => {
    owner = await fakeOwner();
    writeSession(ID);
    await startApp();
    const session = (await manager!.find(ID))!;
    manager!.enqueuePrompt(session, 'not sent yet', 'cm-q');
    expect((await delByClient(ID, 'cm-q')).statusCode).toBe(200);
    expect(session.queue).toHaveLength(0);
    expect((await delByClient(ID, 'cm-q')).statusCode).toBe(404);
    expect((await delByClient(ID, 'nope')).statusCode).toBe(404);
    expect((await delByClient(ID, 'bad id!')).statusCode).toBe(400);
  });

  it('owner v6 (no clear_queue) → 502, item untouched, pi queue untouched', async () => {
    const { session } = await busyTui(ID, false);
    await steerIn(session, [['stuck', 'cm-v6']]);
    const item = session.queue.find((i) => i.clientMessageId === 'cm-v6')!;
    const res = await del(ID, item.id);
    expect(res.statusCode).toBe(502);
    expect(res.json().code).toBe('clear_queue_unsupported');
    expect(res.json().error).toMatch(/older than v7/);
    expect(owner!.received.some((m) => m.op === 'clear_queue')).toBe(false);
    expect(owner!.steering).toEqual(['stuck']);
    expect(item.status).toBe('running');
  });

  it('the cancel holds the dispatch slot: messages queued meanwhile are steered only after the re-queue', async () => {
    const { session } = await busyTui(ID);
    await steerIn(session, [['first', 'cm-a'], ['second', 'cm-b']]);
    owner!.beforeClear = () => {
      manager!.enqueuePrompt(session, 'while cancelling', 'cm-w');
      manager!.dispatchQueuedNow(session);
    };
    const a = session.queue.find((i) => i.clientMessageId === 'cm-a')!;
    expect((await del(ID, a.id)).statusCode).toBe(200);
    await tick(80);
    expect(owner!.steering).toEqual(['second', 'while cancelling']);
  });
});

describe('§15 cancel a steered item — bridge RPC child', () => {
  const ID = '0199ffff-2222-7aaa-8bbb-ccccdddd0002';

  async function busyChild(): Promise<Session> {
    writeSession(ID);
    await startApp(writeFakePi(root));
    const session = (await manager!.ensureRunning(ID))!;
    expect(session.hasChild).toBe(true);
    await session.request({ type: '__emit', events: [{ type: 'agent_start' }] });
    await tick();
    expect(session.isBusy).toBe(true);
    return session;
  }
  const emit = (s: Session, events: Record<string, unknown>[]) => s.request({ type: '__emit', events });
  const peek = (s: Session) => s.request<{ steering: string[]; followUp: string[] }>({ type: 'peek_queue' });

  it('waits for a safe point (tool running), then clear_queue + re-queue via prompt+streamingBehavior', async () => {
    const session = await busyChild();
    await session.request({ type: 'steer', message: 'typed elsewhere' });
    await session.request({ type: 'follow_up', message: 'fu elsewhere' });
    await steerIn(session, [['x1', 'c-1'], ['x2', 'c-2']]);
    expect((await peek(session)).steering).toEqual(['typed elsewhere', 'x1', 'x2']);

    // Between turn_end and the next stream pi may have drained steering already:
    // the cancel must not clear until a tool runs / an assistant message streams.
    await emit(session, [{ type: 'turn_end' }]);
    await tick();
    expect(session.queueClearSafe).toBe(false);
    const x1 = session.queue.find((i) => i.clientMessageId === 'c-1')!;
    const pending = del(ID, x1.id);
    await tick(200);
    expect((await peek(session)).steering).toEqual(['typed elsewhere', 'x1', 'x2']); // not cleared yet
    await emit(session, [{ type: 'tool_execution_start', toolCallId: 't1', toolName: 'bash' }]);
    const res = await pending;
    expect(res.statusCode).toBe(200);
    expect(await peek(session)).toEqual({ steering: ['typed elsewhere', 'x2'], followUp: ['fu elsewhere'] });
    expect(x1.status).toBe('cancelled');
  });

  it('echo while waiting for the safe point → 409 already_delivered, pi queue never cleared', async () => {
    const session = await busyChild();
    await steerIn(session, [['soon', 'c-s']]);
    await emit(session, [{ type: 'turn_end' }]);
    const item = session.queue.find((i) => i.clientMessageId === 'c-s')!;
    const pending = del(ID, item.id);
    await tick(150);
    const message = { role: 'user', content: [{ type: 'text', text: 'soon' }], timestamp: Date.now() };
    await emit(session, [{ type: 'turn_start' }, { type: 'message_start', message }, { type: 'message_end', message }]);
    const res = await pending;
    expect(res.statusCode).toBe(409);
    expect(res.json().code).toBe('already_delivered');
    expect((await peek(session)).steering).toEqual(['soon']); // fake pi never "delivered" it; untouched
  });
});
