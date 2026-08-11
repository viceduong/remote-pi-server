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
