import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { pino } from 'pino';
import { formatQaAnswer, qaFromToolResult } from '../src/qa.js';
import { entryToChatMessage, parseEntry } from '../src/jsonl.js';
import { isVisibleMessage } from '../src/visibility.js';
import { Session, isToolEvent, trimEventForWire, type RingRecord } from '../src/session.js';

/** Real pi 0.85 JSONL toolResult entry shape (from ~/.pi/agent/sessions). */
const askEntry = (details: unknown, id = 'ce420665', isError = false) => ({
  type: 'message',
  id,
  parentId: 'p1',
  timestamp: '2026-06-04T16:55:00.000Z',
  message: {
    role: 'toolResult',
    toolCallId: 'toolu_1',
    toolName: 'ask_user_question',
    content: [{ type: 'text', text: 'User has answered your questions: "Which?"="v26". You can now continue with the user\'s answers in mind.' }],
    details,
    isError,
    timestamp: 1_780_000_000_000,
  },
});

describe('CONTRACT §9 qa formatting', () => {
  it('formats each answer kind', () => {
    expect(formatQaAnswer({ kind: 'option', answer: 'v26.3.0 Current' })).toBe('v26.3.0 Current');
    expect(formatQaAnswer({ kind: 'multi', answer: null, selected: ['A', 'C'] })).toBe('A, C');
    expect(formatQaAnswer({ kind: 'custom', answer: 'typed text' })).toBe('typed text');
    expect(formatQaAnswer({ kind: 'chat', answer: 'Chat about this' })).toBe('Chat about this');
    expect(formatQaAnswer({ kind: 'option', answer: 'B', notes: 'because' })).toBe('B — because');
    expect(formatQaAnswer({ kind: 'custom', answer: '' })).toBe('(no input)');
  });

  it('builds items in question order; cancelled → empty items', () => {
    const qa = qaFromToolResult(askEntry({
      answers: [
        { questionIndex: 1, question: 'Second?', kind: 'custom', answer: 'free' },
        { questionIndex: 0, question: 'First?', kind: 'option', answer: 'A', notes: 'n' },
      ],
      cancelled: false,
    }).message);
    expect(qa).toEqual({ cancelled: false, items: [{ question: 'First?', answer: 'A — n' }, { question: 'Second?', answer: 'free' }] });
    expect(qaFromToolResult(askEntry({ answers: [], cancelled: true }).message)).toEqual({ cancelled: true, items: [] });
    expect(qaFromToolResult(askEntry({ answers: [], cancelled: true, error: 'no_ui' }).message)).toEqual({ cancelled: true, items: [] });
  });

  it('no qa for other tools, validation failures (details {}) or non-results', () => {
    expect(qaFromToolResult(askEntry({}, 'x', true).message)).toBeNull();
    expect(qaFromToolResult({ role: 'toolResult', toolName: 'bash', details: { answers: [], cancelled: false } })).toBeNull();
    expect(qaFromToolResult({ role: 'assistant', toolName: 'ask_user_question', details: { answers: [], cancelled: false } })).toBeNull();
  });
});

describe('CONTRACT §9 Q&A rows are visible tool rows', () => {
  it('JSONL → ChatMessage carries qa and passes the visibility rule', () => {
    const line = JSON.stringify(askEntry({ answers: [{ questionIndex: 0, question: 'Which?', kind: 'option', answer: 'v26' }], cancelled: false }));
    const m = entryToChatMessage(parseEntry(line)!)!;
    expect(m).toMatchObject({ id: 'ce420665', role: 'tool', toolName: 'ask_user_question', qa: { cancelled: false, items: [{ question: 'Which?', answer: 'v26' }] } });
    expect(isVisibleMessage(m)).toBe(true);
    const declined = entryToChatMessage(askEntry({ answers: [], cancelled: true }))!;
    expect(isVisibleMessage(declined)).toBe(true);
    const invalid = entryToChatMessage(askEntry({}, 'v', true))!;
    expect(invalid.qa).toBeUndefined();
    expect(isVisibleMessage(invalid)).toBe(false);
    const bash = entryToChatMessage({ type: 'message', id: 'b', message: { role: 'toolResult', toolName: 'bash', content: 'out' } })!;
    expect(isVisibleMessage(bash)).toBe(false);
  });

  it('message_start/_end wire events carry qa (top level + message) and pass the skeleton filter', () => {
    for (const type of ['message_start', 'message_end']) {
      const wire = trimEventForWire({ type, message: askEntry({ answers: [], cancelled: true }).message });
      expect(wire.qa).toEqual({ cancelled: true, items: [] });
      expect((wire.message as { qa?: unknown }).qa).toEqual({ cancelled: true, items: [] });
      expect(isToolEvent({ type, data: wire })).toBe(false);
    }
    // Other tool results are still dropped by focus clients.
    const bash = trimEventForWire({ type: 'message_end', message: { role: 'toolResult', toolName: 'bash', content: [] } });
    expect(bash.qa).toBeUndefined();
    expect(isToolEvent({ type: 'message_end', data: bash })).toBe(true);
    expect(isToolEvent({ type: 'tool_execution_end', data: { type: 'tool_execution_end', toolName: 'ask_user_question' } })).toBe(true);
  });

  it('tailer file_update rows carry qa and pass the skeleton filter', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'qa-tail-'));
    const file = path.join(dir, 's.jsonl');
    fs.writeFileSync(file, '{"type":"session","id":"s"}\n');
    const s = new Session('qa1', 'qa', file, 'pi', dir, dir, [], pino({ enabled: false }));
    const frames: RingRecord[] = [];
    s.subscribe({ send: (r) => frames.push(r), skeleton: true });
    try {
      fs.appendFileSync(file, `${JSON.stringify(askEntry({ answers: [{ questionIndex: 0, question: 'Q?', kind: 'multi', answer: null, selected: ['x', 'y'] }], cancelled: false }, 'e9'))}\n`);
      await s.pollTail();
      const upd = frames.find((f) => f.type === 'file_update')!;
      const msg = (upd.data as { message: { qa?: unknown } }).message;
      expect(msg.qa).toEqual({ cancelled: false, items: [{ question: 'Q?', answer: 'x, y' }] });
      expect(isToolEvent(upd)).toBe(false);
    } finally {
      s.closeSinks();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
