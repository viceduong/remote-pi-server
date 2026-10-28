import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { PassThrough } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { pino } from 'pino';
import type { FastifyBaseLogger, FastifyReply, FastifyRequest } from 'fastify';
import {
  KeyCounter,
  LiveKeyTracker,
  countsAt,
  messageKey,
  scanKeySnapshot,
  withSuffix,
} from '../src/keys.js';
import { SessionFileCache } from '../src/filecache.js';
import { FileTailer, type TailBatch } from '../src/tailer.js';
import { Session, type RingRecord } from '../src/session.js';
import { attachSse } from '../src/sse.js';
import { canonicalText, mapAgentMessage } from '../src/history.js';
import type { Lease } from '../src/lease.js';
import type { AgentMessage, ChatMessage, RpcEvent } from '../src/types.js';

const log = pino({ enabled: false });

/* ---------------- fixtures ---------------- */

const user = (ts: number, text: string): AgentMessage => ({ role: 'user', content: [{ type: 'text', text }], timestamp: ts });
const assistant = (ts: number, ...texts: string[]): AgentMessage => ({
  role: 'assistant',
  content: [{ type: 'thinking', thinking: 'hmm' }, ...texts.map((text) => ({ type: 'text' as const, text }))],
  timestamp: ts,
  provider: 'p',
  model: 'm',
});
const toolResult = (ts: number, toolCallId: string, text: string): AgentMessage => ({
  role: 'toolResult', toolCallId, toolName: 'bash', content: [{ type: 'text', text }], isError: false, timestamp: ts,
});

let entrySeq = 0;
const line = (message: AgentMessage): string =>
  `${JSON.stringify({ type: 'message', id: `e${++entrySeq}`, parentId: null, timestamp: new Date(message.timestamp ?? 0).toISOString(), message })}\n`;
const header = `${JSON.stringify({ type: 'session', id: 'sess', cwd: '/w', timestamp: '2026-10-28T00:00:00.000Z' })}\n`;

let dir: string;
let file: string;
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'keys-'));
  file = path.join(dir, 's.jsonl');
});
afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

function newSession(f = file): Session {
  return new Session('k1', 'keys', f, 'pi', dir, dir, [], log);
}

/** Feed one complete pi message through the live path (start → end). */
function live(s: Session, msg: AgentMessage, deltas: string[] = []): void {
  s.handleEvent({ type: 'message_start', message: { ...msg, content: msg.role === 'assistant' ? [] : msg.content } });
  for (const d of deltas) {
    s.handleEvent({ type: 'message_update', message: { role: msg.role }, assistantMessageEvent: { type: 'text_delta', contentIndex: 1, delta: d } });
  }
  s.handleEvent({ type: 'message_end', message: msg });
}

const wire = (s: Session, type: string): RpcEvent[] => s.ringSnapshot().filter((r) => r.type === type).map((r) => r.data);

/* ---------------- key function ---------------- */

describe('messageKey (CONTRACT-V2 §13)', () => {
  it('role:timestamp for user/assistant/custom; tool:<toolCallId> for tool results', () => {
    expect(messageKey(user(1700000000001, 'hi'))).toBe('user:1700000000001');
    expect(messageKey(assistant(1700000000002, 'yo'))).toBe('assistant:1700000000002');
    expect(messageKey({ role: 'custom', customType: 'note', content: 'x', timestamp: 5 })).toBe('custom:5');
    expect(messageKey(toolResult(9, 'call_ab', 'out'))).toBe('tool:call_ab');
    // Parallel tool results sharing a millisecond still differ.
    expect(messageKey(toolResult(9, 'call_cd', 'out'))).toBe('tool:call_cd');
    // Degenerate inputs never throw.
    expect(messageKey({ role: 'user', content: 'x' })).toBe('user:?');
    expect(messageKey({ role: 'toolResult', content: 'x', timestamp: 3 })).toBe('toolResult:3');
    expect(messageKey(null)).toBeNull();
  });

  it('collisions get #2, #3 … in assignment order', () => {
    expect(withSuffix('tool:call_1', 1)).toBe('tool:call_1');
    expect(withSuffix('tool:call_1', 3)).toBe('tool:call_1#3');
    const c = new KeyCounter();
    expect(c.assign(toolResult(1, 'call_1', 'a'))).toBe('tool:call_1');
    expect(c.assign(toolResult(2, 'call_1', 'b'))).toBe('tool:call_1#2');
    expect(c.assign(user(1, 'x'))).toBe('user:1');
    expect(c.assign(toolResult(3, 'call_1', 'c'))).toBe('tool:call_1#3');
    expect(c.assign(user(1, 'x again'))).toBe('user:1#2');
  });
});

/* ---------------- live ↔ history agreement ---------------- */

/**
 * A realistic session: a provider that reuses `call_1` every turn, a steered
 * user message stamped at queue time (older than the rows persisted before
 * it), and a message pi persisted twice.
 */
const T = 1_790_000_000_000;
const prefix: AgentMessage[] = [
  user(T, 'first question'),
  assistant(T + 10, 'Let me ', 'check.'),
  toolResult(T + 20, 'call_1', 'ok 1'),
  assistant(T + 30, 'Done.'),
  user(T + 40, 'dup me'),
  user(T + 40, 'dup me'), // re-persisted (same object twice in the file)
];
const liveTail: AgentMessage[] = [
  user(T + 100, 'second question'),
  assistant(T + 110, 'Running again'),
  toolResult(T + 120, 'call_1', 'ok 2'), // base key already in history → #2
  user(T + 115, 'steered: stop after this'), // stamped at queue time, persisted later
  assistant(T + 130, 'Stopping ', 'now.'),
  toolResult(T + 140, 'call_1', 'ok 3'), // → #3
];

describe('live keys agree with the history mapping', () => {
  it('seeded live keys (incl. collisions and an out-of-order steer) equal /messages keys', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const files = new SessionFileCache();
    const s = newSession();
    s.keySource = (f) => files.keySnapshot(f);
    await s.seedKeys(); // transport attach point = current end of file

    const liveKeys: string[] = [];
    for (const m of liveTail) {
      live(s, m);
      liveKeys.push(String(wire(s, 'message_end').pop()!.key));
      fs.appendFileSync(file, line(m)); // pi persists after emitting message_end
    }
    const history = await files.messages(file);
    const keys = history.map((m) => m.key);
    expect(keys).toEqual([
      `user:${T}`, `assistant:${T + 10}`, 'tool:call_1', `assistant:${T + 30}`, `user:${T + 40}`, `user:${T + 40}#2`,
      `user:${T + 100}`, `assistant:${T + 110}`, 'tool:call_1#2', `user:${T + 115}`, `assistant:${T + 130}`, 'tool:call_1#3',
    ]);
    expect(liveKeys).toEqual(keys.slice(prefix.length));
    // message_start / message_end of one message share the key.
    const starts = wire(s, 'message_start').map((d) => d.key);
    expect(starts).toEqual(liveKeys);
    // The standalone scanner (audits / tailer default) agrees too.
    const snap = await scanKeySnapshot(file);
    expect(snap.counts.get('tool:call_1')).toBe(3);
    expect(snap.counts.get(`user:${T + 40}`)).toBe(2);
  });

  it('a message persisted after the attach point but before the seed read is claimed, not double-counted', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const attachAt = fs.statSync(file).size;
    const raced = toolResult(T + 200, 'call_1', 'raced');
    fs.appendFileSync(file, line(raced)); // persisted before we read the seed
    const s = newSession();
    s.keySource = (f) => scanKeySnapshot(f);
    await s.seedKeys(attachAt);
    live(s, raced); // its events reach us (delivered after attach)
    const next = toolResult(T + 210, 'call_1', 'next');
    live(s, next);
    fs.appendFileSync(file, line(next));
    const ends = wire(s, 'message_end').map((d) => d.key);
    const history = (await new SessionFileCache().messages(file)).map((m) => m.key);
    expect(history.slice(-2)).toEqual(['tool:call_1#2', 'tool:call_1#3']);
    expect(ends).toEqual(['tool:call_1#2', 'tool:call_1#3']);
  });

  it('rows from before the attach point are never claimed (a re-persisted old message gets #n)', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const s = newSession();
    s.keySource = (f) => scanKeySnapshot(f);
    await s.seedKeys();
    live(s, user(T, 'first question')); // same object streamed again (e.g. pi re-appends it)
    expect(wire(s, 'message_end').pop()!.key).toBe(`user:${T}#2`);
  });

  it('events arriving while the seed is read are held and keyed in order', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const s = newSession();
    s.keySource = async (f) => { await gate; return scanKeySnapshot(f); };
    const seeded = s.seedKeys();
    live(s, toolResult(T + 300, 'call_1', 'held'));
    expect(s.ringSnapshot()).toHaveLength(0); // held, not keyed early
    release();
    await seeded;
    expect(wire(s, 'message_end').map((d) => d.key)).toEqual(['tool:call_1#2']);
  });

  it('countsAt rewinds a snapshot to an earlier offset', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const mid = fs.statSync(file).size;
    fs.appendFileSync(file, line(toolResult(T + 1, 'call_1', 'x')));
    const snap = await scanKeySnapshot(file);
    expect(snap.counts.get('tool:call_1')).toBe(2);
    expect(countsAt(snap, mid).get('tool:call_1')).toBe(1);
    expect((await scanKeySnapshot(file, mid)).counts).toEqual(countsAt(snap, mid));
  });

  it('LiveKeyTracker: message_end without message_start (attached mid-message) gets a fresh key', () => {
    const t = new LiveKeyTracker();
    t.seed({ offset: 0, counts: new Map([['tool:call_1', 1]]), recent: [] });
    expect(t.end(toolResult(5, 'call_1', 'x'))).toBe('tool:call_1#2');
    expect(t.start(assistant(6))).toBe('assistant:6');
    expect(t.current).toBe('assistant:6');
    expect(t.end(assistant(6, 'done'))).toBe('assistant:6');
    expect(t.current).toBeNull();
  });
});

/* ---------------- canonical text ---------------- */

describe('message_end text (one canonical text function)', () => {
  it('equals the history row text (text blocks joined with no separator; thinking excluded)', async () => {
    const msgs = [assistant(T + 1, 'Hello ', 'world', '\n\nsecond block'), user(T + 2, 'plain'), toolResult(T + 3, 'call_x', 'tool out')];
    msgs.push({ role: 'user', content: [{ type: 'text', text: 'a' }, { type: 'image' }, { type: 'text', text: 'b' }], timestamp: T + 4 });
    fs.writeFileSync(file, header + msgs.map(line).join(''));
    const s = newSession();
    for (const m of msgs) live(s, m);
    const history = await new SessionFileCache().messages(file);
    const ends = wire(s, 'message_end');
    expect(ends.map((d) => d.text)).toEqual(history.map((m) => m.text));
    expect(ends[0]!.text).toBe('Hello world\n\nsecond block');
    expect(history[3]!.text).toBe('ab');
    expect(mapAgentMessage(msgs[0]!)!.text).toBe(canonicalText(msgs[0]!));
  });

  it('message_update done carrying the message gets text; owner-slimmed updates do not', () => {
    const s = newSession();
    const m = assistant(T, 'final');
    s.handleEvent({ type: 'message_start', message: { ...m, content: [] } });
    s.handleEvent({ type: 'message_update', message: m, assistantMessageEvent: { type: 'done', reason: 'stop' } });
    s.handleEvent({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'done', reason: 'stop' } });
    const ups = wire(s, 'message_update');
    expect(ups[0]).toMatchObject({ key: `assistant:${T}`, text: 'final' });
    expect(ups[1]!.text).toBeUndefined();
  });
});

/* ---------------- offsets + in-progress ---------------- */

describe('text_delta offsets and the in-progress message', () => {
  it('offset = UTF-16 length of the streamed text before the delta; in-progress tracks it', () => {
    const s = newSession();
    const m = assistant(T);
    s.handleEvent({ type: 'message_start', message: { ...m, content: [] } });
    for (const d of ['Hé', '😀 ', 'there']) {
      s.handleEvent({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', contentIndex: 1, delta: d } });
    }
    s.handleEvent({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'thinking_delta', contentIndex: 0, delta: 'zz' } });
    const ups = wire(s, 'message_update');
    expect(ups.map((u) => u.offset)).toEqual([0, 2, 5, undefined]);
    expect(ups.map((u) => u.key)).toEqual(Array(4).fill(`assistant:${T}`));
    expect((ups[1]!.assistantMessageEvent as { offset?: number }).offset).toBe(2);
    expect(s.inProgress).toEqual({ key: `assistant:${T}`, timestamp: T, text: 'Hé😀 there' });
    s.handleEvent({ type: 'message_end', message: assistant(T, 'Hé😀 there') });
    expect(s.inProgress).toBeNull();
  });

  it('in-progress is cleared by an abort settle (agent_end) and capped at 1 MB', () => {
    const s = newSession();
    s.handleEvent({ type: 'agent_start' });
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T } });
    const big = 'x'.repeat(700 * 1024);
    for (let i = 0; i < 2; i++) {
      s.handleEvent({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', contentIndex: 0, delta: big } });
    }
    s.handleEvent({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', contentIndex: 0, delta: 'y' } });
    expect(s.inProgress!.text.length).toBe(1024 * 1024);
    expect(wire(s, 'message_update').pop()!.offset).toBe(1400 * 1024); // offsets stay exact past the cap
    s.handleEvent({ type: 'agent_end', messages: [] });
    expect(s.inProgress).toBeNull();
  });
});

/* ---------------- SSE ---------------- */

interface FakeConn {
  frames: () => { event: string; id: string; data: Record<string, unknown> }[];
  close: () => void;
}

function connect(s: Session, lastEventId?: string): FakeConn {
  const raw = Object.assign(new PassThrough(), { writeHead: () => undefined });
  let text = '';
  raw.on('data', (d: Buffer) => { text += d.toString('utf8'); });
  const reqRaw = new EventEmitter();
  const req = { headers: lastEventId ? { 'last-event-id': lastEventId } : {}, query: {}, raw: reqRaw } as unknown as FastifyRequest;
  const reply = { hijack: () => undefined, raw } as unknown as FastifyReply;
  attachSse(req, reply, s, log as unknown as FastifyBaseLogger);
  return {
    frames: () => text.split('\n\n').filter((b) => b.includes('data: ')).map((b) => {
      const get = (k: string): string => b.split('\n').find((l) => l.startsWith(`${k}: `))?.slice(k.length + 2) ?? '';
      return { event: get('event'), id: get('id'), data: JSON.parse(get('data')) as Record<string, unknown> };
    }),
    close: () => reqRaw.emit('close'),
  };
}

const delta = (s: Session, d: string, type = 'text_delta'): void =>
  s.handleEvent({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type, contentIndex: 1, delta: d } });
const tick = (ms = 90): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe('SSE coalescing, replay and message_snapshot', () => {
  it('coalesced deltas carry the FIRST offset + key; different keys never merge', async () => {
    const s = newSession();
    const c = connect(s);
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T } });
    delta(s, 'Hel');
    delta(s, 'lo');
    delta(s, ' you');
    // A new message starts inside the same 60 ms window.
    s.handleEvent({ type: 'message_end', message: assistant(T, 'Hello you') });
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T + 1 } });
    delta(s, 'Next');
    await tick();
    const ups = c.frames().filter((f) => f.event === 'message_update');
    expect(ups.map((f) => [f.data.key, f.data.offset, (f.data.assistantMessageEvent as { delta: string }).delta])).toEqual([
      [`assistant:${T}`, 0, 'Hello you'],
      [`assistant:${T + 1}`, 0, 'Next'],
    ]);
    c.close();
  });

  it('coalescing never merges deltas of different keys even without a frame in between', async () => {
    const s = newSession();
    const c = connect(s);
    // Simulate two keyed streams back-to-back by forging ring-less records.
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T } });
    delta(s, 'a');
    const recs = s.ringSnapshot().slice();
    const forged: RingRecord = { ...recs[recs.length - 1]!, id: `${s.epoch}-999`, seq: 999, data: { ...recs[recs.length - 1]!.data, key: 'assistant:other', offset: 1 } };
    (s as unknown as { fanout(r: RingRecord): void }).fanout(forged);
    await tick();
    const ups = c.frames().filter((f) => f.event === 'message_update');
    expect(ups.map((f) => f.data.key)).toEqual([`assistant:${T}`, 'assistant:other']);
    c.close();
  });

  it('replay re-coalesces with the original offsets', async () => {
    const s = newSession();
    s.handleEvent({ type: 'agent_start' });
    const firstId = s.ringSnapshot()[0]!.id;
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T } });
    delta(s, 'abc');
    delta(s, 'de');
    const c = connect(s, firstId);
    await tick();
    const ups = c.frames().filter((f) => f.event === 'message_update');
    expect(ups).toHaveLength(1);
    expect(ups[0]!.data).toMatchObject({ key: `assistant:${T}`, offset: 0 });
    expect((ups[0]!.data.assistantMessageEvent as { delta: string }).delta).toBe('abcde');
    // The snapshot follows the status snapshot and is authoritative up to now.
    const types = c.frames().map((f) => f.event);
    expect(types.indexOf('message_snapshot')).toBeGreaterThan(types.indexOf('agent_status'));
    c.close();
  });

  it('message_snapshot on connect mid-message; none when idle', async () => {
    const s = newSession();
    const idle = connect(s);
    expect(idle.frames().some((f) => f.event === 'message_snapshot')).toBe(false);
    idle.close();
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T } });
    delta(s, 'partial ');
    delta(s, 'text');
    const c = connect(s);
    const snap = c.frames().find((f) => f.event === 'message_snapshot')!;
    expect(snap.data).toEqual({ type: 'message_snapshot', key: `assistant:${T}`, role: 'assistant', timestamp: T, text: 'partial text', streaming: true });
    // Live deltas continue from the snapshot length.
    delta(s, '!');
    await tick();
    expect(c.frames().filter((f) => f.event === 'message_update').pop()!.data.offset).toBe('partial text'.length);
    c.close();
  });

  it('message_snapshot follows a reset frame', () => {
    const s = newSession();
    s.handleEvent({ type: 'message_start', message: { role: 'assistant', content: [], timestamp: T } });
    delta(s, 'so far');
    const c = connect(s, 'stale.1-3');
    const evs = c.frames().map((f) => f.event);
    expect(evs[0]).toBe('reset');
    expect(evs).toContain('message_snapshot');
    expect(evs.indexOf('message_snapshot')).toBeGreaterThan(evs.indexOf('reset'));
    expect(c.frames().find((f) => f.event === 'message_snapshot')!.data.text).toBe('so far');
    c.close();
  });
});

/* ---------------- owner (TUI) path ---------------- */

function pipePath(): string {
  const name = `rpi-keys-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  return process.platform === 'win32' ? `\\\\.\\pipe\\${name}` : path.join(os.tmpdir(), `${name}.sock`);
}

describe('owner proxy: deltas before any message_start', () => {
  let server: net.Server | null = null;
  const sockets: net.Socket[] = [];
  afterEach(async () => {
    for (const so of sockets) so.destroy();
    await new Promise<void>((r) => (server ? server.close(() => r()) : r()));
    server = null;
  });

  it('key:null (no offset) for mid-message deltas, then a keyed message_end with text; next message fully keyed', async () => {
    const ipc = pipePath();
    const ev = (seq: number, event: Record<string, unknown>): string => `${JSON.stringify({ type: 'event', seq, eventType: event.type, event })}\n`;
    const msgA = assistant(T + 50, 'mid ', 'message');
    const msgB = assistant(T + 60, 'next');
    server = net.createServer((socket) => {
      sockets.push(socket);
      let buf = '';
      socket.on('data', (d) => {
        buf += d.toString('utf8');
        let i: number;
        while ((i = buf.indexOf('\n')) >= 0) {
          const msg = JSON.parse(buf.slice(0, i)) as Record<string, unknown>;
          buf = buf.slice(i + 1);
          if (msg.op === 'hello') {
            socket.write(`${JSON.stringify({ type: 'response', id: 'hello', success: true, data: { eventSeq: 1, isStreaming: true } })}\n`
              + ev(2, { type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', contentIndex: 1, delta: 'mid ' } })
              + ev(3, { type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', contentIndex: 2, delta: 'message' } })
              + ev(4, { type: 'message_end', message: msgA })
              + ev(5, { type: 'message_start', message: { ...msgB, content: [] } })
              + ev(6, { type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', contentIndex: 1, delta: 'next' } })
              + ev(7, { type: 'message_end', message: msgB }));
          } else {
            socket.write(`${JSON.stringify({ type: 'response', id: msg.id, success: true, data: { isStreaming: true } })}\n`);
          }
        }
      });
      socket.on('error', () => undefined);
    });
    await new Promise<void>((r) => server!.listen(ipc, r));
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const s = newSession();
    s.keySource = (f) => scanKeySnapshot(f); // seeded: events are held until the seed lands
    const lease: Lease = {
      v: 1, sessionId: 's', epoch: 1, holder: { pid: process.pid, nonce: 'n', startedAtMs: 0 },
      ipc, acquiredAt: Date.now(), renewedAt: Date.now(), leaseDurationMs: 15_000, kind: 'tui',
    };
    await s.attachOwner(lease);
    await tick(100);
    const msgs = s.ringSnapshot().filter((r) => r.type.startsWith('message_')).map((r) => r.data);
    expect(msgs.map((d) => [d.type, d.key, d.offset])).toEqual([
      ['message_update', null, undefined],
      ['message_update', null, undefined],
      ['message_end', `assistant:${T + 50}`, undefined],
      ['message_start', `assistant:${T + 60}`, undefined],
      ['message_update', `assistant:${T + 60}`, 0],
      ['message_end', `assistant:${T + 60}`, undefined],
    ]);
    expect(msgs[2]!.text).toBe('mid message');
    // No in-progress snapshot was fabricated for the key-less message.
    expect(s.inProgress).toBeNull();
    s.stop('test end');
  });
});

/* ---------------- tailer / file_update ---------------- */

describe('tailer rows carry history keys', () => {
  it('file_update messages get the same keys as /messages (seeded from the prefix)', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const batches: TailBatch[] = [];
    const tailer = new FileTailer(file, (b) => batches.push(b), 5);
    tailer.start();
    for (const m of liveTail) fs.appendFileSync(file, line(m));
    await tailer.poll();
    tailer.stop();
    const tailed = batches.flatMap((b) => b.messages.map((m: ChatMessage) => m.key));
    const history = (await new SessionFileCache().messages(file)).map((m) => m.key);
    expect(tailed).toEqual(history.slice(prefix.length));
    expect(tailed).toContain('tool:call_1#3');
  });

  it('session file_update frames carry key (file cache seed)', async () => {
    fs.writeFileSync(file, header + prefix.map(line).join(''));
    const files = new SessionFileCache();
    const s = newSession();
    s.keySource = (f) => files.keySnapshot(f);
    const frames: RingRecord[] = [];
    s.subscribe({ send: (r) => frames.push(r) });
    fs.appendFileSync(file, line(toolResult(T + 500, 'call_1', 'tailed')));
    await s.pollTail();
    const fu = frames.filter((f) => f.type === 'file_update');
    expect((fu[0]!.data.message as ChatMessage).key).toBe('tool:call_1#2');
    s.closeSinks();
  });
});
