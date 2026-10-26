import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { pino } from 'pino';
import { Session, type RingRecord } from '../src/session.js';
import { SessionManager } from '../src/manager.js';
import { acquireLease, inspectLease, readLease, type Lease } from '../src/lease.js';

const log = pino({ enabled: false });

function pipePath(): string {
  const name = `rpi-test-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  return process.platform === 'win32' ? `\\\\.\\pipe\\${name}` : path.join(os.tmpdir(), `${name}.sock`);
}

interface FakeOwner {
  ipc: string;
  received: Record<string, unknown>[];
  sockets: net.Socket[];
  close(): Promise<void>;
}

/**
 * Minimal owner-extension stand-in. `onHello` decides what bytes to write
 * back (lets tests reproduce the v3 "events before hello in one chunk" bug).
 */
async function fakeOwner(onHello: (socket: net.Socket, msg: Record<string, unknown>) => void): Promise<FakeOwner> {
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
        if (msg.op === 'hello') onHello(socket, msg);
        else if (msg.op === 'get_state') socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: { isStreaming: false, sessionName: 'tui-session' } })}\n`);
        else socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: {} })}\n`);
      }
    });
    socket.on('error', () => undefined);
  });
  await new Promise<void>((r) => server.listen(ipc, r));
  return {
    ipc,
    received,
    sockets,
    close: () => new Promise((r) => {
      for (const s of sockets) s.destroy();
      server.close(() => r());
    }),
  };
}

const helloOk = (socket: net.Socket, extra = ''): void => {
  socket.write(`${extra}${JSON.stringify({ type: 'response', id: 'hello', success: true, data: { eventSeq: 7, isStreaming: false } })}\n`);
};

describe('owner proxy handshake (H2) and op translation (H3/H5)', () => {
  let owner: FakeOwner | null = null;
  let session: Session;
  const lease = (ipc: string): Lease => ({
    v: 1, sessionId: 's', epoch: 1, holder: { pid: process.pid, nonce: 'n', startedAtMs: 0 },
    ipc, acquiredAt: Date.now(), renewedAt: Date.now(), leaseDurationMs: 15_000, kind: 'tui',
  });

  beforeEach(() => {
    session = new Session('own1', 'owner', path.join(os.tmpdir(), 'nope.jsonl'), 'pi', os.tmpdir(), os.tmpdir(), [], log);
  });
  afterEach(async () => {
    session.stop('test end');
    await owner?.close();
    owner = null;
  });

  it('line-buffers: raw events BEFORE the hello reply in the same chunk do not wedge the handshake', async () => {
    owner = await fakeOwner((socket) => {
      // v3 owners broadcast raw events to un-helloed sockets.
      const early = `${JSON.stringify({ type: 'agent_start' })}\n${JSON.stringify({ type: 'turn_start' })}\n`;
      helloOk(socket, early);
    });
    await session.attachOwner(lease(owner.ipc));
    expect(session.isProxy).toBe(true);
    expect(session.owner).toBe('tui');
    // Early events were processed, in order, after the attach.
    expect(session.ringSnapshot().map((r) => r.type)).toEqual(['agent_start', 'turn_start']);
    expect(owner.received[0]).toMatchObject({ op: 'hello', type: 'bridge', lastSeq: 0 });
    expect(typeof owner.received[0]!.clientId).toBe('string');
  });

  it('handles a hello reply split across chunks', async () => {
    owner = await fakeOwner((socket) => {
      const line = `${JSON.stringify({ type: 'response', id: 'hello', success: true, data: {} })}\n`;
      socket.write(line.slice(0, 10));
      setTimeout(() => socket.write(line.slice(10)), 20);
    });
    await session.attachOwner(lease(owner.ipc));
    expect(session.isProxy).toBe(true);
  });

  it('a rejected hello destroys the socket and leaves no half-attached state', async () => {
    owner = await fakeOwner((socket) => {
      socket.write(`${JSON.stringify({ type: 'response', id: 'hello', success: false, error: 'nope' })}\n`);
    });
    await expect(session.attachOwner(lease(owner.ipc))).rejects.toThrow(/hello rejected/);
    expect(session.isProxy).toBe(false);
    expect(session.running).toBe(false);
    await expect(session.request({ type: 'get_state' })).rejects.toThrow(/Agent not running/);
  });

  it('translates abort and streamingBehavior to owner ops and reports real success', async () => {
    owner = await fakeOwner((socket) => helloOk(socket));
    await session.attachOwner(lease(owner.ipc));
    expect(await session.abort()).toBe(true);
    await session.request({ type: 'prompt', message: 'later', streamingBehavior: 'followUp' });
    await session.request({ type: 'prompt', message: 'now', streamingBehavior: 'steer' });
    await session.request({ type: 'prompt', message: 'plain' });
    const ops = owner.received.slice(1).filter((m) => m.op !== 'get_state').map((m) => m.op);
    expect(ops).toEqual(['abort', 'enqueue', 'steer', 'prompt']);
    expect(owner.received.every((m) => m.type === undefined || m.op === 'hello')).toBe(true);
  });

  it('owner disconnect emits agent_exited + agent_status and detaches', async () => {
    owner = await fakeOwner((socket) => helloOk(socket));
    await session.attachOwner(lease(owner.ipc));
    const frames: RingRecord[] = [];
    session.subscribe({ send: (r) => frames.push(r) });
    for (const s of owner.sockets) s.destroy();
    await new Promise((r) => setTimeout(r, 50));
    expect(session.isProxy).toBe(false);
    expect(frames.map((f) => f.type)).toEqual(expect.arrayContaining(['agent_exited', 'agent_status']));
    const status = frames.filter((f) => f.type === 'agent_status').pop()!.data as { phase: string; busy: boolean };
    expect(status).toMatchObject({ phase: 'exited', busy: false });
  });

  it('re-attach to the same owner passes the last seen owner seq', async () => {
    owner = await fakeOwner((socket) => {
      helloOk(socket);
      socket.write(`${JSON.stringify({ type: 'event', seq: 8, eventType: 'agent_start', event: { type: 'agent_start' } })}\n`);
    });
    const l = lease(owner.ipc);
    await session.attachOwner(l);
    await new Promise((r) => setTimeout(r, 30));
    for (const s of owner.sockets) s.destroy();
    await new Promise((r) => setTimeout(r, 30));
    await session.attachOwner(l);
    const hellos = owner.received.filter((m) => m.op === 'hello');
    expect(hellos.map((h) => h.lastSeq)).toEqual([0, 8]);
  });
});

describe('owner proxy questionnaires (CONTRACT §8, owner protocol v5)', () => {
  let srv: net.Server | null = null;
  let session: Session;
  const sockets: net.Socket[] = [];
  const received: Record<string, unknown>[] = [];
  const ipcLease = (ipc: string): Lease => ({
    v: 1, sessionId: 's', epoch: 1, holder: { pid: process.pid, nonce: 'q', startedAtMs: 0 },
    ipc, acquiredAt: Date.now(), renewedAt: Date.now(), leaseDurationMs: 15_000, kind: 'tui',
  });
  const questions = [{ index: 0, header: 'H', question: 'Q?', multiSelect: false, options: [{ label: 'A', description: 'a' }] }];
  const evLine = (seq: number, event: Record<string, unknown>): string =>
    `${JSON.stringify({ type: 'event', seq, eventType: event.type, event })}\n`;
  const tick = (ms = 40) => new Promise((r) => setTimeout(r, ms));

  /** `answerOp` decides the reply to answer_question (v4 owners answer "unknown op"). */
  async function start(answerOp: (socket: net.Socket, msg: Record<string, unknown>) => void, onHello?: (socket: net.Socket) => void): Promise<string> {
    const ipc = pipePath();
    srv = net.createServer((socket) => {
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
            helloOk(socket);
            onHello?.(socket);
          } else if (msg.op === 'answer_question') answerOp(socket, msg);
          else socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: { isStreaming: true } })}\n`);
        }
      });
      socket.on('error', () => undefined);
    });
    await new Promise<void>((r) => srv!.listen(ipc, r));
    return ipc;
  }

  beforeEach(() => {
    session = new Session('ownq', 'owner-q', path.join(os.tmpdir(), 'nope-q.jsonl'), 'pi', os.tmpdir(), os.tmpdir(), [], log);
    received.length = 0;
  });
  afterEach(async () => {
    session.stop('test end');
    for (const s of sockets.splice(0)) s.destroy();
    await new Promise<void>((r) => (srv ? srv.close(() => r()) : r()));
    srv = null;
  });

  it('owner agent_question → pendingQuestion + one card; answer routes to answer_question; owner resolved is not re-broadcast', async () => {
    const ipc = await start((socket, msg) => {
      socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: { requestId: msg.requestId } })}\n`);
      socket.write(evLine(12, { type: 'agent_question_resolved', requestId: msg.requestId }));
    });
    await session.attachOwner(ipcLease(ipc));
    const frames: RingRecord[] = [];
    session.subscribe({ send: (r) => frames.push(r) });
    sockets[0]!.write(evLine(10, { type: 'agent_start' }));
    sockets[0]!.write(evLine(11, { type: 'agent_question', requestId: 'rq-1', questions }));
    sockets[0]!.write(evLine(11, { type: 'agent_question', requestId: 'rq-1', questions })); // replay dup
    await tick();
    expect(session.pendingQuestion).toEqual({ requestId: 'rq-1', questions });
    expect(session.statusFrame()).toMatchObject({ busy: true, questionId: 'rq-1', activity: { kind: 'question' } });
    expect(frames.filter((f) => f.type === 'agent_question')).toHaveLength(1);
    expect(session.ringSnapshot().some((r) => r.type.startsWith('agent_question'))).toBe(false);

    const answers = [{ questionIndex: 0, question: 'Q?', kind: 'option', answer: 'A' }];
    expect(await session.answerQuestion('rq-1', answers, false)).toEqual({ ok: true });
    expect(received.find((m) => m.op === 'answer_question')).toMatchObject({ op: 'answer_question', requestId: 'rq-1', answers, cancelled: false });
    expect(received.find((m) => m.op === 'answer_question')!.type).toBeUndefined();
    await tick();
    expect(session.pendingQuestion).toBeNull();
    expect(frames.filter((f) => f.type === 'agent_question_resolved')).toHaveLength(1);
    expect((frames.filter((f) => f.type === 'agent_status').at(-1)!.data as { questionId: unknown }).questionId).toBeNull();
  });

  it('terminal answer: owner pushes agent_question_resolved → cleared; a late remote answer is a 409', async () => {
    const ipc = await start((socket, msg) => {
      socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: false, error: `unknown requestId ${String(msg.requestId)}` })}\n`);
    });
    await session.attachOwner(ipcLease(ipc));
    sockets[0]!.write(evLine(10, { type: 'agent_question', requestId: 'rq-2', questions }));
    await tick();
    sockets[0]!.write(evLine(11, { type: 'agent_question_resolved', requestId: 'rq-2' }));
    await tick();
    expect(session.pendingQuestion).toBeNull();
    expect(await session.answerQuestion('rq-2', [], true)).toMatchObject({ ok: false, status: 409 });
    // Owner says the id is unknown (raced with the terminal) → 409 and cleared.
    sockets[0]!.write(evLine(12, { type: 'agent_question', requestId: 'rq-3', questions }));
    await tick();
    expect(await session.answerQuestion('rq-3', [], true)).toMatchObject({ ok: false, status: 409 });
    expect(session.pendingQuestion).toBeNull();
  });

  it('a v4 owner (unknown op answer_question) yields a clear 502 and keeps the question', async () => {
    const ipc = await start((socket, msg) => {
      socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: false, error: 'unknown op answer_question' })}\n`);
    });
    await session.attachOwner(ipcLease(ipc));
    sockets[0]!.write(evLine(10, { type: 'agent_question', requestId: 'rq-4', questions }));
    await tick();
    const res = await session.answerQuestion('rq-4', [], true);
    expect(res).toMatchObject({ ok: false, status: 502 });
    expect((res as { error: string }).error).toMatch(/older than v5/);
    expect(session.pendingQuestion?.requestId).toBe('rq-4');
  });

  it('question replayed after hello is picked up; owner detach clears it (resolved + status)', async () => {
    const ipc = await start(() => undefined, (socket) => {
      socket.write(evLine(7, { type: 'agent_question', requestId: 'rq-5', questions }));
    });
    const frames: RingRecord[] = [];
    session.subscribe({ send: (r) => frames.push(r) });
    await session.attachOwner(ipcLease(ipc));
    await tick();
    expect(session.pendingQuestion?.requestId).toBe('rq-5');
    for (const s of sockets) s.destroy();
    await tick();
    expect(session.isProxy).toBe(false);
    expect(session.pendingQuestion).toBeNull();
    expect(frames.filter((f) => f.type === 'agent_question_resolved').map((f) => (f.data as { requestId: string }).requestId)).toEqual(['rq-5']);
    expect(frames.filter((f) => f.type === 'agent_status').at(-1)!.data).toMatchObject({ phase: 'exited', busy: false, activity: null, questionId: null });
  });

  it('owner compaction events (v5 spelling) drive activity', async () => {
    const ipc = await start(() => undefined);
    await session.attachOwner(ipcLease(ipc));
    sockets[0]!.write(evLine(10, { type: 'agent_start' }));
    await tick();
    const busySince = session.statusFrame().activity!.busySince;
    expect(busySince).toBeGreaterThan(0);
    sockets[0]!.write(evLine(11, { type: 'compaction_start', reason: 'threshold' }));
    await tick();
    expect(session.statusFrame().activity).toMatchObject({ kind: 'compacting', reason: 'threshold', busySince });
    // A live proxy's summary (runtime) carries the same activity object incl. busySince.
    expect(session.runtime().activity).toMatchObject({ kind: 'compacting', busySince });
    sockets[0]!.write(evLine(12, { type: 'compaction_end', reason: 'threshold', aborted: false, willRetry: false }));
    await tick();
    expect(session.statusFrame().activity).toMatchObject({ kind: 'waiting', busySince }); // unchanged across the compaction
  });
});

describe('SessionManager ownership (leases only)', () => {
  let root: string;
  let manager: SessionManager;
  let owner: FakeOwner | null = null;

  const makeManager = (maxAgents = 1): SessionManager => new SessionManager({
    sessionDir: root, workdir: root, bin: 'pi-definitely-not-installed-xyz', maxAgents, idleKillMs: 60_000,
    extraArgs: [], log, noTimers: true,
  });
  const writeSession = (id: string, name = `${id}.jsonl`): string => {
    const dir = path.join(root, '--proj--');
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, name);
    fs.writeFileSync(file, `${JSON.stringify({ type: 'session', id, cwd: root })}\n`);
    return file;
  };

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-own-'));
  });
  afterEach(async () => {
    manager?.stopAll();
    await owner?.close();
    owner = null;
    fs.rmSync(root, { recursive: true, force: true });
  });

  it('a TUI lease → proxy attach; proxies do not consume agent slots', async () => {
    owner = await fakeOwner((socket) => helloOk(socket));
    const file = writeSession('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0001');
    acquireLease(file, 'x', owner.ipc, Date.now(), { kind: 'tui' });
    manager = makeManager(1);
    const s = await manager.ensureRunning('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0001');
    expect(s?.isProxy).toBe(true);
    expect(manager.runningCount).toBe(0);
    const sum = await manager.summary('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0001');
    expect(sum).toMatchObject({ owner: 'tui', live: true, running: true, phase: 'idle', busy: false });
  });

  it('list status for an unattached TUI-leased session is truthful (owner tui, phase idle)', async () => {
    const file = writeSession('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0002');
    acquireLease(file, 'x', '\\\\.\\pipe\\nobody', Date.now(), { kind: 'tui' });
    manager = makeManager();
    const [sum] = (await manager.list());
    expect(sum).toMatchObject({ owner: 'tui', live: true, livePid: process.pid, phase: 'idle', busy: false });
  });

  it('find(): exact id, or a UNIQUE prefix of ≥ 12 chars — never an 8-char guess', async () => {
    writeSession('0199aaaa-bbbb-7ccc-8ddd-000000000001', 'a.jsonl');
    writeSession('0199aaaa-bbbb-7ccc-9eee-000000000002', 'b.jsonl');
    manager = makeManager();
    expect((await manager.find('0199aaaa-bbbb-7ccc-8ddd-000000000001'))?.id).toBe('0199aaaa-bbbb-7ccc-8ddd-000000000001');
    expect(await manager.find('0199aaaa')).toBeNull(); // 8-char prefix: refused
    expect(await manager.find('0199aaaa-bbbb-7ccc')).toBeNull(); // ambiguous
    expect((await manager.find('0199aaaa-bbbb-7ccc-9e'))?.id).toBe('0199aaaa-bbbb-7ccc-9eee-000000000002');
  });

  it('cap reached → BusyError (503), not a silent null/404', async () => {
    writeSession('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0003');
    manager = makeManager(1);
    Object.defineProperty(manager, 'runningCount', { get: () => 1 });
    await expect(manager.ensureRunning('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0003')).rejects.toMatchObject({ code: 503 });
  });

  it('an unreadable lease is never treated as unowned', async () => {
    const file = writeSession('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0004');
    fs.mkdirSync(`${file}.lease`);
    manager = makeManager();
    await expect(manager.ensureRunning('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0004')).rejects.toThrow(/ownership could not be determined/);
    expect(await manager.isHostOwned('0199aaaa-bbbb-7ccc-8ddd-eeeeffff0004')).toBe(true);
  });
});

describe('bridge lease fencing (H11)', () => {
  it('a TUI pre-empting the bridge lease fences the bridge on its next renew', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-fence-'));
    const file = path.join(dir, 'f.jsonl');
    fs.writeFileSync(file, '');
    const s = new Session('f1', 'fence', file, 'pi', dir, dir, [], log);
    let fenced = 0;
    s.onFenced = () => { fenced++; };
    (s as unknown as { takeBridgeLease(): void }).takeBridgeLease();
    const ours = readLease(file)!;
    expect(ours.kind).toBe('bridge');
    expect(ours.bridgePid).toBe(process.pid);
    s.renewTick();
    expect(fenced).toBe(0);
    // Host TUI opens the file: pre-empts with epoch+1.
    const tui = acquireLease(file, 'f1', 'pipe', Date.now(), { kind: 'tui', preemptKind: 'bridge' });
    expect(tui.ok).toBe(true);
    s.renewTick();
    expect(fenced).toBe(1);
    expect(inspectLease(file).lease?.kind).toBe('tui');
    (s as unknown as { stopRenew(): void }).stopRenew();
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
