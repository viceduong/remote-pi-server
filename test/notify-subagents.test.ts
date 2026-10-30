import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { applyPut, defaultSettings, isSubagentHeader, publicSettings, SubagentClassifier } from '../src/notify.js';

const TYPES = new Set(['general-purpose', 'explore', 'plan', 'worker']);

describe('subagent session detection', () => {
  it('parentSession + "<type>#<id8>" name is a subagent', () => {
    expect(isSubagentHeader('C:/s/parent.jsonl', 'general-purpose#0814fce3', TYPES)).toBe(true);
    expect(isSubagentHeader('C:/s/parent.jsonl', 'my-custom#AbC12345', TYPES)).toBe(true);
  });

  it('parentSession + a bare agent type name is a subagent', () => {
    expect(isSubagentHeader('C:/s/parent.jsonl', 'Explore', TYPES)).toBe(true);
    expect(isSubagentHeader('C:/s/parent.jsonl', 'worker', TYPES)).toBe(true);
  });

  it('forks (parentSession, ordinary name) and main sessions are not', () => {
    expect(isSubagentHeader('C:/s/parent.jsonl', 'Fix sign-out bug', TYPES)).toBe(false);
    expect(isSubagentHeader('C:/s/parent.jsonl', null, TYPES)).toBe(false);
    expect(isSubagentHeader(undefined, 'general-purpose#0814fce3', TYPES)).toBe(false);
  });

  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-subagent-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  const write = (name: string, lines: unknown[]): string => {
    const f = path.join(dir, name);
    fs.writeFileSync(f, lines.map((l) => JSON.stringify(l)).join('\n') + '\n');
    return f;
  };

  it('classifies real-shaped session files (header + session_info) and caches', async () => {
    fs.mkdirSync(path.join(dir, 'agents'));
    fs.writeFileSync(path.join(dir, 'agents', 'reviewer.md'), '# reviewer');
    const c = new SubagentClassifier(path.join(dir, 'agents'));
    const sub = write('sub.jsonl', [
      { type: 'session', version: 3, id: 'a', parentSession: 'C:/p.jsonl' },
      { type: 'model_change', provider: 'p', modelId: 'm' },
      { type: 'session_info', name: 'general-purpose#0814fce3' },
    ]);
    const custom = write('custom.jsonl', [
      { type: 'session', version: 3, id: 'b', parentSession: 'C:/p.jsonl' },
      { type: 'session_info', name: 'reviewer' },
    ]);
    const fork = write('fork.jsonl', [
      { type: 'session', version: 3, id: 'c', parentSession: 'C:/p.jsonl' },
      { type: 'session_info', name: 'Try the other approach' },
    ]);
    const main = write('main.jsonl', [{ type: 'session', version: 3, id: 'd' }]);
    expect(await c.isSubagent(sub)).toBe(true);
    expect(await c.isSubagent(custom)).toBe(true); // custom agent from agents/*.md
    expect(await c.isSubagent(fork)).toBe(false);
    expect(await c.isSubagent(main)).toBe(false);
    expect(await c.isSubagent(path.join(dir, 'missing.jsonl'))).toBe(false);
  });

  it('skipSubagents defaults on, is public and settable', () => {
    const s = defaultSettings();
    expect(s.skipSubagents).toBe(true);
    expect(publicSettings(s).skipSubagents).toBe(true);
    expect(applyPut(s, { skipSubagents: false }).skipSubagents).toBe(false);
  });
});
