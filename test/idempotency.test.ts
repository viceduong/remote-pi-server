import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { pino } from 'pino';
import { Session } from '../src/session.js';
import { SessionManager } from '../src/manager.js';

function makeSession(file: string): Session {
  return new Session(
    'idem1234567890ab',
    'idempotency-test',
    file,
    'pi',
    path.dirname(file),
    path.dirname(file),
    [],
    pino({ enabled: false }),
  );
}

describe('turn idempotency', () => {
  it('returns the existing durable queue item for a repeated client key', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-pi-idem-'));
    const manager = new SessionManager({
      sessionDir: root,
      workdir: root,
      bin: 'pi',
      maxAgents: 1,
      idleKillMs: 60_000,
      extraArgs: [],
      log: pino({ enabled: false }),
    });
    const sessionFile = path.join(root, 'session.jsonl');
    const session = makeSession(sessionFile);

    const first = manager.enqueuePrompt(session, 'hello', 'client-1');
    const second = manager.enqueuePrompt(session, 'hello', 'client-1');

    expect(second.id).toBe(first.id);
    expect(session.queue).toHaveLength(1);
    expect(second.clientMessageId).toBe('client-1');
  });
});
