import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { BusyError, SessionManager } from '../src/manager.js';
import { pino } from 'pino';

let dir: string;
let manager: SessionManager;

function makeManager(bin = 'pi') {
  return new SessionManager({
    sessionDir: dir,
    workdir: '/work/proj',
    bin,
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
  vi.restoreAllMocks();
});

describe('live write guard', () => {
  it('flags sessions owned by an external pi process', async () => {
    // One external pi process on the host (probe mocked).
    vi.mock('../src/live.js', () => ({
      probeLivePiInstances: vi.fn(async () => [
        { pid: 999, cwd: null, startedAt: Date.now(), args: 'pi-agent' },
      ]),
    }));
    const { probeLivePiInstances } = await import('../src/live.js');

    // Fresh external session file -> candidate for the recency pass.
    const projDir = path.join(dir, '--C--Users-Admin--');
    fs.mkdirSync(projDir, { recursive: true });
    const file = path.join(projDir, 'ext1.jsonl');
    fs.writeFileSync(file, '{"type":"session","id":"ext1","cwd":"C:\\\\Users\\\\Admin"}\n');

    manager = makeManager();
    // Force a live probe through the (mocked) module.
    await manager['refreshLive']();
    expect(probeLivePiInstances).toHaveBeenCalled();

    const sessions = manager.list();
    const s = sessions.find((x) => x.id === 'ext1');
    expect(s?.live).toBe(true);
    expect(s?.livePid).toBe(999);
    expect(manager.isExternallyLive('ext1')).toBe(true);
    expect(manager.isExternallyLive('nope')).toBe(false);
  });
});

describe('spawn failure surfacing', () => {
  it('create() rejects with a SpawnError when pi cannot boot', async () => {
    manager = makeManager('pi-definitely-not-installed-xyz');
    await expect(manager.create('boom')).rejects.toThrow(/failed to start|exited before/);
  }, 30_000);

  it('create() rejects BusyError at the hard capacity (no eviction)', async () => {
    // Ownership model: no idle-eviction — the hard cap is a plain 503.
    manager = makeManager();
    const spy = vi.spyOn(manager as unknown as { runningCount: number }, 'runningCount', 'get');
    spy.mockReturnValue(2);
    await expect(manager.create('x')).rejects.toBeInstanceOf(BusyError);
  });
});
