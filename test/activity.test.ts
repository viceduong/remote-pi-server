import { describe, expect, it, vi, afterEach } from 'vitest';
import { pino } from 'pino';
import { Session, type RingRecord } from '../src/session.js';
import type { AgentStatusFrame } from '../src/types.js';

function makeSession(): { s: Session; frames: RingRecord[]; statuses: () => AgentStatusFrame[] } {
  const s = new Session('act1', 'activity', '/tmp/nonexistent-activity.jsonl', 'pi', '/tmp', '/tmp', [], pino({ enabled: false }));
  const frames: RingRecord[] = [];
  s.subscribe({ send: (r) => frames.push(r) });
  // No transport: subscribe started a tailer on a missing file — harmless, stop it.
  (s as unknown as { stopTailer(): void }).stopTailer();
  return { s, frames, statuses: () => frames.filter((f) => f.type === 'agent_status').map((f) => f.data as AgentStatusFrame) };
}

const ev = (type: string, extra: Record<string, unknown> = {}) => ({ type, ...extra }) as never;
const upd = (type: string, extra: Record<string, unknown> = {}) =>
  ev('message_update', { message: { role: 'assistant' }, assistantMessageEvent: { type, contentIndex: 0, ...extra } });

afterEach(() => {
  vi.useRealTimers();
});

describe('CONTRACT §7 activity', () => {
  it('idle sessions report activity null + questionId null', () => {
    const { s } = makeSession();
    expect(s.statusFrame()).toMatchObject({ phase: 'idle', busy: false, activity: null, questionId: null });
    expect(s.runtime().activity).toBeNull();
  });

  it('agent_start → waiting → thinking → writing → toolcall{tool} → tool → waiting → null', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const { s, statuses } = makeSession();
    s.handleEvent(ev('agent_start'));
    // busySince is pinned to the start of the run and stays put across every transition.
    expect(s.statusFrame().activity).toEqual({ kind: 'waiting', since: 1_000, busySince: 1_000 });
    expect(s.statusFrame()).toMatchObject({ phase: 'streaming', busy: true });

    vi.setSystemTime(2_000);
    s.handleEvent(upd('thinking_start'));
    expect(s.statusFrame().activity).toEqual({ kind: 'thinking', since: 2_000, busySince: 1_000 });

    vi.setSystemTime(3_000);
    s.handleEvent(upd('text_start'));
    s.handleEvent(upd('text_delta', { delta: 'a' }));
    expect(s.statusFrame().activity).toEqual({ kind: 'writing', since: 3_000, busySince: 1_000 });

    vi.setSystemTime(4_000);
    // RPC wire: toolcall_start carries toolName (partial stripped)
    s.handleEvent(upd('toolcall_start', { toolName: 'bash', id: 'c1' }));
    s.handleEvent(upd('toolcall_delta', { delta: '{"cmd"' }));
    expect(s.statusFrame().activity).toEqual({ kind: 'toolcall', since: 4_000, busySince: 1_000, tool: 'bash' });

    vi.setSystemTime(5_000);
    s.handleEvent(ev('tool_execution_start', { toolCallId: 'c1', toolName: 'bash', args: {} }));
    expect(s.statusFrame().activity).toEqual({ kind: 'tool', since: 5_000, busySince: 1_000, tool: 'bash', toolCount: 1 });

    vi.setSystemTime(6_000);
    s.handleEvent(ev('tool_execution_end', { toolCallId: 'c1', toolName: 'bash', result: {}, isError: false }));
    expect(s.statusFrame().activity).toEqual({ kind: 'waiting', since: 6_000, busySince: 1_000 });

    s.handleEvent(ev('agent_end', { messages: [] }));
    expect(s.statusFrame()).toMatchObject({ phase: 'idle', busy: false, activity: null });
    expect(statuses().at(-1)).toMatchObject({ busy: false, activity: null });
  });

  it('toolcall name falls back to the partial (owner/raw events)', () => {
    const { s } = makeSession();
    s.handleEvent(ev('agent_start'));
    s.handleEvent(upd('toolcall_start', { partial: { content: [{ type: 'toolCall', id: 'x', name: 'read' }] } }));
    expect(s.statusFrame().activity).toMatchObject({ kind: 'toolcall', tool: 'read' });
  });

  it('token-rate deltas do not produce a status frame each (transitions only)', () => {
    const { s, statuses } = makeSession();
    s.handleEvent(ev('agent_start'));
    const before = statuses().length;
    s.handleEvent(upd('thinking_start'));
    for (let i = 0; i < 200; i++) s.handleEvent(upd('thinking_delta', { delta: 'x' }));
    s.handleEvent(upd('thinking_end', { content: 'x' }));
    s.handleEvent(upd('text_start'));
    for (let i = 0; i < 200; i++) s.handleEvent(upd('text_delta', { delta: 'y' }));
    expect(statuses().length - before).toBe(2); // → thinking, → writing
  });

  it('parallel tools: toolCount tracks the running set, tool = first running one', () => {
    vi.useFakeTimers();
    vi.setSystemTime(10_000);
    const { s } = makeSession();
    s.handleEvent(ev('agent_start'));
    s.handleEvent(ev('tool_execution_start', { toolCallId: 'a', toolName: 'read' }));
    vi.setSystemTime(10_500);
    s.handleEvent(ev('tool_execution_start', { toolCallId: 'b', toolName: 'grep' }));
    expect(s.statusFrame().activity).toEqual({ kind: 'tool', since: 10_000, busySince: 10_000, tool: 'read', toolCount: 2 });
    s.handleEvent(ev('tool_execution_end', { toolCallId: 'a', toolName: 'read' }));
    expect(s.statusFrame().activity).toEqual({ kind: 'tool', since: 10_500, busySince: 10_000, tool: 'grep', toolCount: 1 });
    vi.setSystemTime(11_000);
    s.handleEvent(ev('tool_execution_end', { toolCallId: 'b', toolName: 'grep' }));
    expect(s.statusFrame().activity).toEqual({ kind: 'waiting', since: 11_000, busySince: 10_000 });
  });

  it('compaction (RPC spelling): overrides, carries reason + integer percent, restores the previous activity', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const { s, statuses } = makeSession();
    s.handleEvent(ev('agent_start'));
    s.handleEvent(upd('text_start'));
    vi.setSystemTime(2_000);
    s.handleEvent(ev('compaction_start', { reason: 'overflow' }));
    expect(s.statusFrame().activity).toEqual({ kind: 'compacting', since: 2_000, busySince: 1_000, reason: 'overflow' });
    s.handleEvent(ev('compaction_progress', { percent: 41.6, summaryChars: 10, tokensBefore: 5 }));
    const n = statuses().length;
    s.handleEvent(ev('compaction_progress', { percent: 41.9, summaryChars: 12, tokensBefore: 5 })); // same integer
    expect(statuses().length).toBe(n);
    expect(s.statusFrame().activity).toMatchObject({ kind: 'compacting', percent: 42 });
    s.handleEvent(ev('compaction_end', { reason: 'overflow', aborted: false, willRetry: true }));
    expect(s.statusFrame().activity).toEqual({ kind: 'writing', since: 1_000, busySince: 1_000 });
  });

  it('compaction while idle (manual /compact) is busy, then idle again — and kicks the queue', () => {
    const { s } = makeSession();
    let idle = 0;
    s.onIdle = () => { idle++; };
    s.handleEvent(ev('compaction_start', { reason: 'manual' }));
    expect(s.statusFrame()).toMatchObject({ busy: true, phase: 'streaming', activity: { kind: 'compacting', reason: 'manual' } });
    expect(s.isBusy).toBe(true);
    expect(s.reservePrompt()).toBe(false); // queue must not dispatch into a compaction
    s.handleEvent(ev('compaction_end', { reason: 'manual', aborted: false, willRetry: false }));
    expect(s.statusFrame()).toMatchObject({ busy: false, activity: null });
    expect(idle).toBe(1);
  });

  it('compaction (v4 owner raw hook spelling) is understood and its payload never hits the wire', () => {
    const { s, frames } = makeSession();
    s.handleEvent(ev('session_before_compact', { reason: 'threshold', willRetry: false, preparation: { big: 'x'.repeat(10_000) }, branchEntries: [1, 2] }));
    expect(s.statusFrame().activity).toMatchObject({ kind: 'compacting', reason: 'threshold' });
    const rec = frames.find((f) => f.type === 'session_before_compact')!;
    expect(rec.json).not.toContain('preparation');
    expect(rec.json).not.toContain('branchEntries');
    s.handleEvent(ev('session_compact_failed', { reason: 'threshold', aborted: true, willRetry: false }));
    expect(s.statusFrame().activity).toBeNull();
  });

  it('retry: auto_retry_start → retrying{attempt,maxAttempts,delayMs}; agent_end willRetry keeps it; auto_retry_end → waiting', () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const { s } = makeSession();
    s.handleEvent(ev('agent_start'));
    vi.setSystemTime(2_000);
    s.handleEvent(ev('auto_retry_start', { attempt: 1, maxAttempts: 3, delayMs: 2000, errorMessage: '529' }));
    s.handleEvent(ev('agent_end', { messages: [], willRetry: true }));
    expect(s.statusFrame()).toMatchObject({ busy: true, activity: { kind: 'retrying', since: 2_000, attempt: 1, maxAttempts: 3, delayMs: 2000 } });
    expect(s.isBusy).toBe(true);
    s.handleEvent(ev('agent_start'));
    s.handleEvent(upd('text_delta', { delta: 'ok' }));
    expect(s.statusFrame().activity?.kind).toBe('writing');
    s.handleEvent(ev('auto_retry_end', { success: true, attempt: 1 }));
    expect(s.statusFrame().activity?.kind).toBe('writing'); // only resets a retrying activity
    s.handleEvent(ev('agent_end', { messages: [] }));
    expect(s.statusFrame().activity).toBeNull();
  });

  it('agent_end willRetry without auto_retry_start → retrying', () => {
    const { s } = makeSession();
    s.handleEvent(ev('agent_start'));
    s.handleEvent(ev('agent_end', { messages: [], willRetry: true }));
    expect(s.statusFrame()).toMatchObject({ busy: true, activity: { kind: 'retrying' } });
    s.handleEvent(ev('agent_settled'));
    expect(s.statusFrame()).toMatchObject({ busy: false, activity: null });
  });

  it('a reserved prompt is busy and has a waiting activity (busy ⇔ activity)', () => {
    vi.useFakeTimers();
    vi.setSystemTime(500);
    const { s } = makeSession();
    expect(s.reservePrompt()).toBe(true);
    expect(s.statusFrame()).toMatchObject({ phase: 'waiting', busy: true, activity: { kind: 'waiting', busySince: 500 } });
    s.releasePrompt();
    expect(s.statusFrame()).toMatchObject({ busy: false, activity: null });
  });
});

describe('CONTRACT §7 busySince (continuous busy-period start)', () => {
  it('is pinned at idle→busy, stable across every sub-activity transition, cleared on idle', () => {
    vi.useFakeTimers();
    vi.setSystemTime(100_000);
    const { s } = makeSession();
    // A prompt is reserved first (idle→busy begins here), then the run flows through it.
    expect(s.reservePrompt()).toBe(true);
    expect(s.statusFrame().activity?.busySince).toBe(100_000);

    vi.setSystemTime(100_100);
    s.handleEvent(ev('agent_start')); // reservation → run, SAME busy period
    vi.setSystemTime(100_200);
    s.handleEvent(upd('thinking_start'));
    vi.setSystemTime(100_300);
    s.handleEvent(upd('text_start'));
    vi.setSystemTime(100_400);
    s.handleEvent(ev('tool_execution_start', { toolCallId: 'a', toolName: 'bash' }));
    vi.setSystemTime(100_500);
    s.handleEvent(ev('compaction_start', { reason: 'threshold' }));
    vi.setSystemTime(100_600);
    s.handleEvent(ev('compaction_end', { reason: 'threshold', aborted: false, willRetry: false }));
    // Through waiting→thinking→writing→tool→compacting and back, busySince never moved.
    for (const _ of [0]) void _;
    expect(s.statusFrame().activity?.busySince).toBe(100_000);
    expect(s.statusFrame().activity?.since).toBe(100_400); // the tool (still running) started later

    vi.setSystemTime(100_700);
    s.handleEvent(ev('tool_execution_end', { toolCallId: 'a', toolName: 'bash' }));
    expect(s.statusFrame().activity?.busySince).toBe(100_000); // still the same period

    s.handleEvent(ev('agent_end', { messages: [] }));
    expect(s.statusFrame().activity).toBeNull();

    // A NEW run starts a fresh period (old busySince is not reused).
    vi.setSystemTime(200_000);
    s.handleEvent(ev('agent_start'));
    expect(s.statusFrame().activity?.busySince).toBe(200_000);
  });

  it('survives a reconnect snapshot: a mid-run client gets the ORIGINAL busySince, not "now"', () => {
    vi.useFakeTimers();
    vi.setSystemTime(300_000);
    const { s } = makeSession();
    s.handleEvent(ev('agent_start'));
    vi.setSystemTime(305_000);
    s.handleEvent(upd('thinking_start'));

    // A client reconnects 10s into the run: the snapshot statusFrame() is a pure read.
    vi.setSystemTime(310_000);
    const snap1 = s.statusFrame();
    expect(snap1.activity).toMatchObject({ kind: 'thinking', since: 305_000, busySince: 300_000 });
    // Re-reading it again (another reconnect) must not drift busySince to "now".
    vi.setSystemTime(320_000);
    expect(s.statusFrame().activity?.busySince).toBe(300_000);
    expect(snap1.activity?.busySince).toBe(300_000); // the earlier snapshot object is unchanged too
  });

  it('busySince is 0/absent when idle and the session has no transport', () => {
    const { s } = makeSession();
    expect(s.statusFrame().activity).toBeNull();
    expect(s.runtime().activity).toBeNull();
  });
});

describe('CONTRACT §8 pending question (RPC marker transport)', () => {
  const ask = (id: string) => ev('extension_ui_request', {
    id,
    method: 'select',
    title: `pi-questionnaire:${JSON.stringify({ questions: [{ index: 0, header: 'H', question: 'Q?', multiSelect: false, options: [{ label: 'A', description: '' }] }] })}`,
    options: ['A'],
  });
  const withStdin = (s: Session): string[] => {
    const lines: string[] = [];
    (s as unknown as { proc: unknown }).proc = { stdin: { write: (l: string) => { lines.push(l); return true; } } };
    return lines;
  };

  it('question overrides activity, questionId mirrors it; answer clears + one resolved frame', async () => {
    const { s, frames } = makeSession();
    const lines = withStdin(s);
    s.handleEvent(ev('agent_start'));
    s.handleEvent(ev('tool_execution_start', { toolCallId: 't1', toolName: 'ask_user_question' }));
    s.handleEvent(ask('req-1'));
    expect(s.statusFrame()).toMatchObject({ busy: true, questionId: 'req-1', activity: { kind: 'question' } });
    expect(frames.filter((f) => f.type === 'agent_question')).toHaveLength(1);
    s.handleEvent(ask('req-1')); // duplicate → no second card
    expect(frames.filter((f) => f.type === 'agent_question')).toHaveLength(1);

    expect(await s.answerQuestion('nope', [], true)).toMatchObject({ ok: false, status: 409 });
    const answers = [{ questionIndex: 0, question: 'Q?', kind: 'option', answer: 'A' }];
    expect(await s.answerQuestion('req-1', answers, false)).toEqual({ ok: true });
    expect(JSON.parse(lines[0]!)).toEqual({ type: 'extension_ui_response', id: 'req-1', value: `pi-answers:${JSON.stringify({ answers, cancelled: false })}` });
    expect(s.statusFrame()).toMatchObject({ questionId: null, activity: { kind: 'tool', tool: 'ask_user_question' } });
    // tool end of the same questionnaire: no second resolved frame
    s.handleEvent(ev('tool_execution_end', { toolCallId: 't1', toolName: 'ask_user_question' }));
    expect(frames.filter((f) => f.type === 'agent_question_resolved')).toHaveLength(1);
    (s as unknown as { proc: unknown }).proc = null;
  });

  it.each([
    ['tool_execution_end of ask_user_question', ev('tool_execution_end', { toolCallId: 't1', toolName: 'ask_user_question' })],
    ['final agent_end', ev('agent_end', { messages: [] })],
    ['agent_settled', ev('agent_settled')],
  ])('clears on %s (select timeout resolves silently in RPC mode)', (_label, trigger) => {
    const { s, frames } = makeSession();
    s.handleEvent(ev('agent_start'));
    s.handleEvent(ask('req-2'));
    s.handleEvent(trigger);
    expect(s.pendingQuestion).toBeNull();
    expect(s.statusFrame().questionId).toBeNull();
    expect(frames.filter((f) => f.type === 'agent_question_resolved').map((f) => (f.data as { requestId: string }).requestId)).toEqual(['req-2']);
    const last = frames.filter((f) => f.type === 'agent_status').at(-1)!.data as AgentStatusFrame;
    expect(last.questionId).toBeNull();
  });

  it('does not clear on agent_end willRetry or an unrelated tool end', () => {
    const { s } = makeSession();
    s.handleEvent(ev('agent_start'));
    s.handleEvent(ask('req-3'));
    s.handleEvent(ev('tool_execution_end', { toolCallId: 'x', toolName: 'bash' }));
    s.handleEvent(ev('agent_end', { messages: [], willRetry: true }));
    expect(s.pendingQuestion?.requestId).toBe('req-3');
  });

  it('abort declines the child select first, then clears the question', async () => {
    const { s, frames } = makeSession();
    const lines = withStdin(s);
    s.handleEvent(ask('req-4'));
    // Answer the abort RPC from the fake child.
    const realWrite = (s as unknown as { proc: { stdin: { write(l: string): boolean } } }).proc.stdin;
    realWrite.write = (l: string) => {
      lines.push(l);
      const obj = JSON.parse(l) as { id?: string; type?: string };
      if (obj.type === 'abort') setImmediate(() => (s as unknown as { handleResponse(o: unknown): void }).handleResponse({ type: 'response', id: obj.id, command: 'abort', success: true }));
      return true;
    };
    expect(await s.abort()).toBe(true);
    expect(JSON.parse(lines[0]!)).toEqual({ type: 'extension_ui_response', id: 'req-4', cancelled: true });
    expect(JSON.parse(lines[1]!)).toMatchObject({ type: 'abort' });
    expect(s.pendingQuestion).toBeNull();
    expect(frames.filter((f) => f.type === 'agent_question_resolved')).toHaveLength(1);
    expect((frames.filter((f) => f.type === 'agent_status').at(-1)!.data as AgentStatusFrame).questionId).toBeNull();
    (s as unknown as { proc: unknown }).proc = null;
  });

  it('answering with no transport is a 409', async () => {
    const { s } = makeSession();
    s.handleEvent(ask('req-5'));
    expect(await s.answerQuestion('req-5', [], true)).toMatchObject({ ok: false, status: 409 });
  });
});
