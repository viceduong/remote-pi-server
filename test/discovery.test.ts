import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/manager.js';
import { pino } from 'pino';

let dir: string;
let manager: SessionManager;

function makeManager() {
  return new SessionManager({
    sessionDir: dir,
    workdir: '/work/proj',
    bin: 'pi',
    maxAgents: 2,
    idleKillMs: 60_000,
    extraArgs: [],
    log: pino({ enabled: false }),
  });
}

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-test-'));
});

afterEach(() => {
  manager?.stopAll();
  fs.rmSync(dir, { recursive: true, force: true });
});

describe('SessionManager discovery', () => {
  it('discovers nested pi session files with metadata', () => {
    // Simulate pi's layout: <sessiondir>/--C--Users-Admin--/<id>.jsonl
    const projDir = path.join(dir, '--C--Users-Admin--');
    fs.mkdirSync(projDir, { recursive: true });
    const file = path.join(projDir, '2026-08-07T10-00-00-000Z_abc123.jsonl');
    fs.writeFileSync(file, [
      '{"type":"session","version":3,"id":"abc123","timestamp":"2026-08-07T10:00:00.000Z","cwd":"C:\\\\Users\\\\Admin"}',
      '{"type":"message","id":"m1","role":"user","content":"list the files please"}',
      '{"type":"message","id":"m2","role":"assistant","content":"[{\\"type\\":\\"text\\",\\"text\\":\\"done\\"}]"}',
      '',
    ].join('\n'));
    // Touch mtime to the past so it is not "active".
    const past = new Date(Date.now() - 3_600_000);
    fs.utimesSync(file, past, past);

    manager = makeManager();
    const sessions = manager.list();

    expect(sessions).toHaveLength(1);
    const s = sessions[0]!;
    expect(s.id).toBe('abc123');
    expect(s.source).toBe('pi');
    expect(s.active).toBe(false);
    expect(s.messageCount).toBe(2);
    expect(s.name).toContain('list the files please');
    expect(s.running).toBe(false);
  });

  it('marks recently-modified sessions as active', () => {
    const projDir = path.join(dir, '--C--Users-Admin--');
    fs.mkdirSync(projDir, { recursive: true });
    const file = path.join(projDir, 'abc.jsonl');
    fs.writeFileSync(file, JSON.stringify({ type: 'session', id: 'abc' })); // fresh mtime

    manager = makeManager();
    const s = manager.list()[0]!;
    expect(s.active).toBe(true);
    expect(s.lastActivityAt).toBeGreaterThan(0);
  });

  it('places app session files inside the mangled workdir dir', () => {
    manager = makeManager();
    // No real pi spawn here (CI has none) — pure path placement check.
    const file = manager.sessionFile('aabbccddeeff0011');
    expect(file).toBe(path.join(dir, '--work-proj--', 'aabbccddeeff0011.jsonl'));
  });
});
