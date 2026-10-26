import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { pino } from 'pino';
import { SessionManager } from '../src/manager.js';
import { Session, type RingRecord } from '../src/session.js';
import {
  QUEUE_MAX_ATTEMPTS,
  backoffMs,
  pendingView,
  pruneQueue,
  queueFilePath,
  restoreItems,
  type QueueItem,
} from '../src/queue.js';

const item = (p: Partial<QueueItem>): QueueItem => ({
  id: 'q', message: 'm', status: 'queued', queuedAt: Date.now(), startedAt: null, completedAt: null, error: null, ...p,
});

describe('queue model (CONTRACT §2)', () => {
  it('pendingView returns only queued/running items, with status + clientMessageId', () => {
    const view = pendingView([
      item({ id: 'a', status: 'queued', clientMessageId: 'c1' }),
      item({ id: 'b', status: 'running' }),
      item({ id: 'c', status: 'done' }),
      item({ id: 'd', status: 'failed' }),
    ]);
    expect(view.map((v) => [v.id, v.status, v.clientMessageId])).toEqual([['a', 'queued', 'c1'], ['b', 'running', undefined]]);
  });

  it('prunes finished items older than 1 h', () => {
    const now = Date.now();
    const kept = pruneQueue([
      item({ id: 'old', status: 'done', completedAt: now - 2 * 3_600_000 }),
      item({ id: 'recent', status: 'failed', completedAt: now - 60_000 }),
      item({ id: 'active', status: 'queued', queuedAt: now - 5 * 3_600_000 }),
    ], now);
    expect(kept.map((i) => i.id)).toEqual(['recent', 'active']);
  });

  it('restore: delivered running items are done (never re-sent), undelivered go back to queued', () => {
    const restored = restoreItems([
      item({ id: 'delivered', status: 'running', deliveredAt: 1, startedAt: 1 }),
      item({ id: 'lost', status: 'running', startedAt: 1 }),
      { bogus: true },
    ]);
    expect(restored.map((i) => [i.id, i.status])).toEqual([['delivered', 'done'], ['lost', 'queued']]);
  });

  it('backoff is exponential, capped at 30 s, with equal jitter', () => {
    expect(backoffMs(1, () => 0)).toBe(500);
    expect(backoffMs(1, () => 1)).toBe(1000);
    expect(backoffMs(3, () => 1)).toBe(4000);
    expect(backoffMs(20, () => 1)).toBe(30_000);
    expect(backoffMs(20, () => 0)).toBe(15_000);
  });
});

describe('SessionManager queue delivery', () => {
  let root: string;
  let manager: SessionManager;
  let session: Session;
  let frames: RingRecord[];
  const sink = { send: (r: RingRecord) => frames.push(r) };

  beforeEach(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-queue-'));
    manager = new SessionManager({
      sessionDir: root, workdir: root, bin: 'pi', maxAgents: 2, idleKillMs: 60_000, extraArgs: [],
      log: pino({ enabled: false }), noTimers: true,
    });
    // A real discovered session, registered (and queue-wired) by the manager.
    const dir = path.join(root, '--work--');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'q1.jsonl'), '{"type":"session","id":"queue-session-0001"}\n');
    session = (await manager.find('queue-session-0001'))!;
    frames = [];
    session.subscribe(sink);
  });

  afterEach(() => {
    session.unsubscribe(sink);
    vi.useRealTimers();
    vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  });

  /** Pretend a transport is attached and answer RPCs with `respond`. */
  function fakeTransport(respond: (cmd: Record<string, unknown>) => Promise<unknown>): Array<Record<string, unknown>> {
    const sent: Array<Record<string, unknown>> = [];
    Object.defineProperty(session, 'running', { get: () => true });
    vi.spyOn(session, 'request').mockImplementation(async (cmd: Record<string, unknown>) => {
      sent.push(cmd);
      return respond(cmd) as never;
    });
    vi.spyOn(manager, 'ensureRunning').mockResolvedValue(session);
    return sent;
  }

  it('does NOT dedupe by text: two identical "continue" prompts are both delivered', async () => {
    const sent = fakeTransport(async () => ({}));
    const a = manager.enqueuePrompt(session, 'continue');
    const b = manager.enqueuePrompt(session, 'continue');
    expect(a.id).not.toBe(b.id);
    manager.dispatchQueuedNow(session);
    await vi.waitFor(() => expect(sent).toHaveLength(1));
    expect(a.status).toBe('running');
    // Turn ends → first completes, second dispatches.
    session.handleEvent({ type: 'agent_start' });
    session.handleEvent({ type: 'agent_end' });
    await vi.waitFor(() => expect(sent).toHaveLength(2));
    expect(sent.map((c) => c.message)).toEqual(['continue', 'continue']);
    expect(a.status).toBe('done');
  });

  it('dedupes only by clientMessageId', () => {
    const a = manager.enqueuePrompt(session, 'hello', 'cm-1');
    const again = manager.enqueuePrompt(session, 'different text', 'cm-1');
    expect(again.id).toBe(a.id);
    expect(session.queue).toHaveLength(1);
  });

  it('marks running synchronously when the transport is up (dispatched)', () => {
    fakeTransport(() => new Promise(() => undefined));
    const a = manager.enqueuePrompt(session, 'go');
    manager.dispatchQueuedNow(session);
    expect(a.status).toBe('running');
    expect(session.statusFrame().phase).toBe('waiting');
  });

  it('backs off on failure and fails the item after the attempt limit, surfacing the error', async () => {
    vi.useFakeTimers();
    const sent = fakeTransport(async () => { throw new Error('Agent not running'); });
    const a = manager.enqueuePrompt(session, 'will fail');
    manager.dispatchQueuedNow(session);
    await vi.advanceTimersByTimeAsync(0);
    expect(sent).toHaveLength(1);
    expect(a.status).toBe('queued');
    expect(a.attempts).toBe(1);
    expect(a.nextAttemptAt! - Date.now()).toBeGreaterThanOrEqual(400);
    // Nothing is re-sent before the backoff elapses (no hot loop).
    await vi.advanceTimersByTimeAsync(300);
    expect(sent).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(sent).toHaveLength(QUEUE_MAX_ATTEMPTS);
    expect(a.status).toBe('failed');
    expect(a.error).toMatch(/Agent not running/);
    expect(session.statusFrame()).toMatchObject({ phase: 'error' });
    expect(session.statusFrame().error).toMatch(/Queued message failed after 6 attempts/);
    // Clients only ever see active items.
    expect(pendingView(session.queue)).toEqual([]);
    const lastQueueFrame = frames.filter((f) => f.type === 'queue_update').pop()!;
    expect((lastQueueFrame.data as { items: unknown[] }).items).toEqual([]);
  });

  it('persists the queue and restores it into a freshly found session', async () => {
    const file = path.join(root, '--w2--', 'p.jsonl');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, '{"type":"session","id":"persist-1234567"}\n');
    const qf = queueFilePath(root, file);
    fs.mkdirSync(path.dirname(qf), { recursive: true });
    fs.writeFileSync(qf, JSON.stringify([item({ id: 'x', status: 'running', deliveredAt: 5 }), item({ id: 'y', status: 'done', completedAt: Date.now() })]));
    const items = await manager.getQueueItems('persist-1234567');
    expect(items).toEqual([]); // delivered running → done; done is never "pending"
    const s = await manager.find('persist-1234567');
    expect(s!.queue.map((i) => [i.id, i.status])).toEqual([['x', 'done'], ['y', 'done']]);
  });

  it('echoes clientMessageId on the user message_end and in history', async () => {
    fakeTransport(async () => ({}));
    manager.enqueuePrompt(session, 'tracked', 'cm-echo');
    manager.dispatchQueuedNow(session);
    await vi.waitFor(() => expect(session.queue[0]!.deliveredAt).toBeTruthy());
    session.handleEvent({ type: 'message_end', message: { role: 'user', content: [{ type: 'text', text: 'tracked' }], timestamp: 4242 } });
    const end = frames.filter((f) => f.type === 'message_end').pop()!;
    expect((end.data as { clientMessageId?: string }).clientMessageId).toBe('cm-echo');
    expect(session.queue[0]!.messageTimestamp).toBe(4242);
    fs.writeFileSync(session.file, `${JSON.stringify({ type: 'message', id: 'e1', message: { role: 'user', content: 'tracked', timestamp: 4242 } })}\n`);
    const rows = await manager.history(session);
    expect(rows[0]!.clientMessageId).toBe('cm-echo');
  });
});
