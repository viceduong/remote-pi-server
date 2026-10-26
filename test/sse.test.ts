import { describe, expect, it } from 'vitest';
import { pino } from 'pino';
import { parseEventId, resolveReplay, encodeFrame } from '../src/sse.js';
import { Session, computeStatus, trimEventForWire, isToolEvent, type RingRecord } from '../src/session.js';

function makeSession(): Session {
  return new Session('sse1', 'sse', '/tmp/none.jsonl', 'pi', '/tmp', '/tmp', [], pino({ enabled: false }));
}

describe('SSE event ids (CONTRACT §1)', () => {
  it('parses <epoch>-<seq> and rejects legacy/garbage ids', () => {
    expect(parseEventId('lx2k1.3-42')).toEqual({ epoch: 'lx2k1.3', seq: 42 });
    expect(parseEventId('123')).toBeNull();
    expect(parseEventId('abc-')).toBeNull();
    expect(parseEventId('-5')).toBeNull();
    expect(parseEventId(undefined)).toBeNull();
  });

  it('ring records carry `<epoch>-<seq>` ids', () => {
    const s = makeSession();
    s.handleEvent({ type: 'agent_start' });
    const rec = s.ringSnapshot()[0]!;
    expect(rec.id).toBe(`${s.epoch}-${rec.seq}`);
    expect(JSON.parse(rec.json)).toEqual(rec.data);
  });

  it('no Last-Event-ID → live; same epoch → replay after it', () => {
    const s = makeSession();
    for (let i = 0; i < 5; i++) s.handleEvent({ type: 'turn_end', i });
    const ring = s.ringSnapshot();
    expect(resolveReplay(undefined, s)).toEqual({ kind: 'live' });
    const d = resolveReplay(ring[1]!.id, s);
    expect(d.kind).toBe('replay');
    expect((d as { records: RingRecord[] }).records.map((r) => r.seq)).toEqual(ring.slice(2).map((r) => r.seq));
    expect((resolveReplay(`${s.epoch}-${s.seq}`, s) as { records: RingRecord[] }).records).toEqual([]);
  });

  it('different epoch / unparsable → reset(epoch); ahead → reset(ahead)', () => {
    const s = makeSession();
    s.handleEvent({ type: 'agent_start' });
    expect(resolveReplay('other.1-1', s)).toEqual({ kind: 'reset', reason: 'epoch' });
    expect(resolveReplay('17', s)).toEqual({ kind: 'reset', reason: 'epoch' });
    expect(resolveReplay(`${s.epoch}-${s.seq + 5}`, s)).toEqual({ kind: 'reset', reason: 'ahead' });
  });

  it('seq older than the ring window → reset(gap)', () => {
    const s = makeSession();
    s.handleEvent({ type: 'turn_end' });
    const firstId = s.ringSnapshot()[0]!.id;
    for (let i = 0; i < 600; i++) s.handleEvent({ type: 'turn_end', i }); // ring cap 500
    expect(s.ringFloor).toBeGreaterThan(1);
    expect(resolveReplay(firstId, s)).toEqual({ kind: 'reset', reason: 'gap' });
  });

  it('a new epoch (respawn/attach/re-key/fork) invalidates old ids', () => {
    const s = makeSession();
    s.handleEvent({ type: 'agent_start' });
    const old = `${s.epoch}-${s.seq}`;
    s.newEpoch();
    expect(resolveReplay(old, s)).toEqual({ kind: 'reset', reason: 'epoch' });
    expect(s.ringSnapshot()).toHaveLength(0);
  });

  it('encodes frames per the WHATWG format', () => {
    expect(encodeFrame('reset', 'e-1', '{"type":"reset"}')).toBe('event: reset\nid: e-1\ndata: {"type":"reset"}\n\n');
  });
});

describe('agent_status (CONTRACT §5)', () => {
  const base = { phase: 'idle' as const, busy: false, promptReserved: false, error: null, owner: 'none' as const };
  it('computes phase from real state', () => {
    expect(computeStatus(base)).toEqual({ type: 'agent_status', phase: 'idle', busy: false, working: false, owner: 'none' });
    expect(computeStatus({ ...base, promptReserved: true }).phase).toBe('waiting');
    expect(computeStatus({ ...base, busy: true, phase: 'streaming' })).toMatchObject({ phase: 'streaming', busy: true });
    expect(computeStatus({ ...base, error: 'boom' })).toMatchObject({ phase: 'error', error: 'boom', busy: false });
    expect(computeStatus({ ...base, phase: 'terminated' }).phase).toBe('exited');
  });

  it('is emitted to every sink on transitions (deduplicated)', () => {
    const s = makeSession();
    const frames: RingRecord[] = [];
    const sink = { send: (r: RingRecord) => frames.push(r) };
    s.subscribe(sink);
    s.handleEvent({ type: 'agent_start' });
    s.handleEvent({ type: 'turn_start' }); // same status → no duplicate frame
    s.handleEvent({ type: 'agent_end' });
    const statuses = frames.filter((f) => f.type === 'agent_status').map((f) => (f.data as { phase: string }).phase);
    expect(statuses).toEqual(['streaming', 'idle']);
    s.unsubscribe(sink);
  });

  it('agent_end with willRetry keeps the session busy', () => {
    const s = makeSession();
    s.handleEvent({ type: 'agent_start' });
    s.handleEvent({ type: 'agent_end', willRetry: true });
    expect(s.busy).toBe(true);
    s.handleEvent({ type: 'agent_settled' });
    expect(s.busy).toBe(false);
  });
});

describe('wire trimming (P3)', () => {
  it('strips partial messages and toolcall_delta payloads; keeps done message', () => {
    const delta = trimEventForWire({
      type: 'message_update',
      message: { role: 'assistant', content: [{ type: 'text', text: 'huge partial' }] },
      assistantMessageEvent: { type: 'text_delta', delta: 'x', partial: { content: 'huge' } },
    });
    expect(delta).toEqual({ type: 'message_update', message: { role: 'assistant' }, assistantMessageEvent: { type: 'text_delta', delta: 'x' } });
    const tc = trimEventForWire({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_delta', delta: '{"path":"..."}', contentIndex: 1 } });
    expect(tc.assistantMessageEvent).toEqual({ type: 'toolcall_delta', contentIndex: 1 });
    const done = trimEventForWire({ type: 'message_update', message: { role: 'assistant', content: 'final' }, assistantMessageEvent: { type: 'done' } });
    expect(done.message).toEqual({ role: 'assistant', content: 'final' });
  });

  it('isToolEvent recognises pi roles toolResult and tool, tool_execution_*, file_update tool rows', () => {
    expect(isToolEvent({ type: 'tool_execution_end', data: {} })).toBe(true);
    expect(isToolEvent({ type: 'message_end', data: { message: { role: 'toolResult' } } })).toBe(true);
    expect(isToolEvent({ type: 'file_update', data: { message: { role: 'tool' } } })).toBe(true);
    expect(isToolEvent({ type: 'message_end', data: { message: { role: 'assistant' } } })).toBe(false);
  });
});
