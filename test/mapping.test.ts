import { describe, expect, it } from 'vitest';
import { mapAgentMessage } from '../src/history.js';

describe('mapAgentMessage', () => {
  it('maps plain user text', () => {
    const m = mapAgentMessage({ role: 'user', content: 'hello', timestamp: 1 });
    expect(m).toEqual({
      id: null, role: 'user', text: 'hello', thinking: null, toolCalls: [],
      toolName: null, isError: false, system: false, model: null, timestamp: 1, errorMessage: null,
    });
  });

  it('maps assistant text/thinking/toolCall blocks', () => {
    const m = mapAgentMessage({
      role: 'assistant',
      content: [
        { type: 'thinking', thinking: 'hmm' },
        { type: 'text', text: 'Answer' },
        { type: 'toolCall', id: 't1', name: 'bash', arguments: { command: 'ls' } },
      ],
      model: 'claude-3-5',
      timestamp: 2,
    });
    expect(m?.role).toBe('assistant');
    expect(m?.text).toBe('Answer');
    expect(m?.thinking).toBe('hmm');
    expect(m?.toolCalls).toEqual([{ id: 't1', name: 'bash', arguments: { command: 'ls' } }]);
    expect(m?.model).toBe('claude-3-5');
  });

  it('parses string tool arguments as JSON', () => {
    const m = mapAgentMessage({
      role: 'assistant',
      content: [{ type: 'toolCall', name: 'edit', arguments: '{"path":"a.ts"}' }],
    });
    expect(m?.toolCalls[0]?.arguments).toEqual({ path: 'a.ts' });
  });

  it('marks tool result messages', () => {
    const m = mapAgentMessage({
      role: 'tool',
      toolName: 'bash',
      isError: true,
      content: [{ type: 'text', text: 'exit 1' }],
    });
    expect(m?.role).toBe('tool');
    expect(m?.toolName).toBe('bash');
    expect(m?.isError).toBe(true);
    expect(m?.text).toBe('exit 1');
  });

  it('joins multiple text blocks', () => {
    const m = mapAgentMessage({
      role: 'assistant',
      content: [{ type: 'text', text: 'a' }, { type: 'text', text: 'b' }],
    });
    expect(m?.text).toBe('ab');
  });

  it('marks context-prune summaries as system notes', () => {
    const m = mapAgentMessage({
      role: 'user',
      content: '### Tool Call 1: bash\n- Listed the files.\n- **Success** — done.',
    });
    expect(m?.system).toBe(true);
    expect(mapAgentMessage({ role: 'user', content: '## Tool Call Summary\n\n**Tool: read**' })?.system).toBe(true);
  });

  it('does not mark ordinary user prompts as system', () => {
    const m = mapAgentMessage({ role: 'user', content: 'what is a tool call?' });
    expect(m?.system).toBeFalsy();
  });

  it('returns null for empty input', () => {
    expect(mapAgentMessage(null)).toBeNull();
    expect(mapAgentMessage(undefined)).toBeNull();
  });
});
