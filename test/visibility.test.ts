import { describe, expect, it } from 'vitest';
import { isVisibleMessage } from '../src/visibility.js';
import type { ChatMessage } from '../src/types.js';

const row = (p: Partial<ChatMessage>): ChatMessage => ({
  id: 'e1', role: 'assistant', text: '', thinking: null, toolCalls: [], toolName: null,
  isError: false, system: false, model: null, timestamp: 1, errorMessage: null, ...p,
});

describe('visibility rule (CONTRACT §4)', () => {
  it('user rows need non-empty trimmed text', () => {
    expect(isVisibleMessage(row({ role: 'user', text: 'hi' }))).toBe(true);
    expect(isVisibleMessage(row({ role: 'user', text: '   ' }))).toBe(false);
  });
  it('assistant rows need text excluding thinking', () => {
    expect(isVisibleMessage(row({ text: 'answer' }))).toBe(true);
    expect(isVisibleMessage(row({ text: '', thinking: 'only thinking' }))).toBe(false);
    expect(isVisibleMessage(row({ text: '', toolCalls: [{ id: 't', name: 'bash', arguments: {} }] }))).toBe(false);
  });
  it('error rows are visible', () => {
    expect(isVisibleMessage(row({ text: '', isError: true }))).toBe(true);
    expect(isVisibleMessage(row({ text: '', errorMessage: 'rate limited' }))).toBe(true);
  });
  it('tool results and custom/extension notes are hidden', () => {
    expect(isVisibleMessage(row({ role: 'tool', text: 'output', toolName: 'bash' }))).toBe(false);
    expect(isVisibleMessage(row({ role: 'tool', text: 'failed', isError: true }))).toBe(false);
    expect(isVisibleMessage(row({ role: 'user', text: '## Tool Call Summary', system: true }))).toBe(false);
  });
});
