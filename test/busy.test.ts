import { describe, expect, it } from 'vitest';
import { Session } from '../src/session.js';
import { pino } from 'pino';

function makeSession() {
  return new Session(
    'test1234567890ab',
    'busy-test',
    '/tmp/nonexistent.jsonl',
    'pi',
    '/tmp',
    '/tmp',
    [],
    pino({ enabled: false }),
  );
}

describe('session busy tracking (mid-turn safety)', () => {
  it('stays busy across per-message done events (tool loops)', () => {
    const s = makeSession();
    s.handleEvent({ type: 'agent_start' } as never);
    s.handleEvent({ type: 'turn_start' } as never);
    expect(s.busy).toBe(true);

    // Tool-loop messages: each emits a 'done' assistantMessageEvent.
    s.handleEvent({ type: 'message_update', assistantMessageEvent: { type: 'text_delta' } } as never);
    s.handleEvent({ type: 'message_update', assistantMessageEvent: { type: 'done' } } as never);
    expect(s.busy).toBe(true);

    s.handleEvent({ type: 'message_update', assistantMessageEvent: { type: 'done' } } as never);
    expect(s.busy).toBe(true);

    // Only turn_end / agent_end clears it.
    s.handleEvent({ type: 'turn_end' } as never);
    expect(s.busy).toBe(false);
  });

  it('turn_start sets busy true and records lastTurnStartAt', () => {
    const s = makeSession();
    s.handleEvent({ type: 'turn_start' } as never);
    expect(s.busy).toBe(true);
    expect(s.lastTurnStartAt).toBeGreaterThan(0);
  });
});

describe('session phase state machine', () => {
  it('idle -> streaming -> awaitingInput -> streaming cycle', () => {
    const s = makeSession();
    expect(s.phase).toBe('idle');
    s.handleEvent({ type: 'turn_start' } as never);
    expect(s.phase).toBe('streaming');
    // per-message done does NOT end the phase (tool loops)
    s.handleEvent({ type: 'message_update', assistantMessageEvent: { type: 'done' } } as never);
    expect(s.phase).toBe('streaming');
    s.handleEvent({ type: 'turn_end' } as never);
    expect(s.phase).toBe('awaitingInput');
    s.handleEvent({ type: 'turn_start' } as never);
    expect(s.phase).toBe('streaming');
    s.handleEvent({ type: 'agent_end' } as never);
    expect(s.phase).toBe('awaitingInput');
  });

});

describe('mid-turn protection (phase-authoritative busy)', () => {
  it('a prompt during a tool gap is treated as busy (never raw-sent)', () => {
    const s = makeSession();
    s.handleEvent({ type: 'turn_start' } as never);
    // Tool gap: no deltas flowing, but phase stays streaming.
    expect(s.phase).toBe('streaming');
    // The turn-route guard would see busyNow = busy || phase==='streaming'.
    expect(s.busy || s.phase === 'streaming').toBe(true);
    s.handleEvent({ type: 'turn_end' } as never);
    expect(s.busy || s.phase === 'streaming').toBe(false);
  });
});
