import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  acquireLease,
  gcLeases,
  inspectLease,
  inspectLeaseAsync,
  isLeaseHeld,
  leasePath,
  readLease,
  releaseLease,
  renewLease,
  writeAtomic,
  type Lease,
} from '../src/lease.js';

const DEAD_PID = 999_999_999; // implausible pid → ESRCH

describe('session ownership leases', () => {
  let dir: string;
  let file: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-test-'));
    file = path.join(dir, 's.jsonl');
    fs.writeFileSync(file, '');
  });
  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  const rewrite = (patch: (l: Lease) => void): void => {
    const lease = JSON.parse(fs.readFileSync(leasePath(file), 'utf8')) as Lease;
    patch(lease);
    fs.writeFileSync(leasePath(file), JSON.stringify(lease));
  };

  it('acquires on an unowned session (kind defaults to tui)', () => {
    const r = acquireLease(file, 's1', '\\\\.\\pipe\\t1', Date.now());
    expect(r.ok).toBe(true);
    expect(r.lease?.epoch).toBe(1);
    expect(readLease(file)?.sessionId).toBe('s1');
    expect(readLease(file)?.kind).toBe('tui');
    expect(inspectLease(file).state).toBe('held');
  });

  it('refuses while held by a live process', () => {
    expect(acquireLease(file, 's1', 'p1', Date.now()).ok).toBe(true);
    const r = acquireLease(file, 's1', 'p2', Date.now());
    expect(r.ok).toBe(false);
    expect(r.reason).toBe('held');
  });

  it('pid liveness is decisive: a stalled heartbeat (>20 s) is still held', () => {
    const t = Date.now();
    acquireLease(file, 's1', 'p1', t, t);
    // Holder (this process) is alive but has not renewed for a minute.
    expect(isLeaseHeld(readLease(file), t + 60_000)).toBe(true);
    expect(acquireLease(file, 's1', 'p2', { now: t + 60_000 }).ok).toBe(false);
  });

  it('claims immediately when the recorded holder process is dead (epoch fenced)', () => {
    const a = acquireLease(file, 's1', 'pA', Date.now()).lease!;
    rewrite((l) => { l.holder.pid = DEAD_PID; });
    expect(inspectLease(file).state).toBe('stale');
    const b = acquireLease(file, 's1', 'pB', Date.now());
    expect(b.ok).toBe(true);
    expect(b.lease!.epoch).toBe(2);
    // The old holder wakes up: its renew reports the fence, release is a no-op.
    expect(renewLease(file, a)).toBe('lost');
    releaseLease(file, a);
    expect(readLease(file)?.epoch).toBe(2);
  });

  it('TUIs pre-empt a held bridge lease; bridges never pre-empt a TUI', () => {
    const bridge = acquireLease(file, 's1', '', Date.now(), { kind: 'bridge', bridgePid: process.pid });
    expect(bridge.ok).toBe(true);
    // A second bridge claim is refused…
    expect(acquireLease(file, 's1', '', Date.now(), { kind: 'bridge' }).ok).toBe(false);
    // …but a TUI pre-empts it with epoch+1.
    const tui = acquireLease(file, 's1', 'pipe', Date.now(), { kind: 'tui', preemptKind: 'bridge' });
    expect(tui.ok).toBe(true);
    expect(tui.lease!.epoch).toBe(2);
    expect(renewLease(file, bridge.lease!)).toBe('lost');
    // A bridge cannot take it back.
    expect(acquireLease(file, 's1', '', Date.now(), { kind: 'bridge', preemptKind: 'bridge' }).ok).toBe(false);
  });

  it('a bridge lease whose server process died is stale even if the child pid lives', () => {
    acquireLease(file, 's1', '', Date.now(), { kind: 'bridge', bridgePid: DEAD_PID, holderPid: process.pid });
    expect(inspectLease(file).state).toBe('stale');
  });

  it('an unreadable lease is `unknown` (treated as held), never unowned', async () => {
    fs.mkdirSync(leasePath(file)); // reading a directory → EISDIR, not ENOENT
    expect(inspectLease(file).state).toBe('unknown');
    expect((await inspectLeaseAsync(file)).state).toBe('unknown');
    const r = acquireLease(file, 's1', 'p', Date.now());
    expect(r.ok).toBe(false);
    expect(r.reason).toBe('unknown');
  });

  it('corrupt lease content counts as absent', () => {
    fs.writeFileSync(leasePath(file), '{nope');
    expect(inspectLease(file).state).toBe('absent');
  });

  it('renew updates renewedAt and can patch the holder pid (bridge after spawn)', () => {
    const t = Date.now();
    const lease = acquireLease(file, 's1', '', t, { kind: 'bridge', now: t }).lease!;
    expect(renewLease(file, lease, t + 1000, { holder: { ...lease.holder, pid: process.pid, startedAtMs: t } })).toBe('renewed');
    expect(readLease(file)!.renewedAt).toBe(t + 1000);
    fs.unlinkSync(leasePath(file));
    expect(renewLease(file, lease)).toBe('absent');
  });

  it('release removes own lease; tombstone release leaves a non-held marker', () => {
    const a = acquireLease(file, 's1', 'pA', Date.now()).lease!;
    releaseLease(file, a);
    expect(readLease(file)).toBeNull();
    const b = acquireLease(file, 's1', 'pB', Date.now()).lease!;
    releaseLease(file, b, { tombstone: true, now: 123 });
    expect(readLease(file)?.releasedAt).toBe(123);
    expect(inspectLease(file).state).toBe('stale');
    // A tombstone does not block the next claim and its epoch continues.
    expect(acquireLease(file, 's1', 'pC', Date.now()).lease?.epoch).toBe(b.epoch + 1);
  });

  it('writeAtomic never leaks its tmp file when the rename fails', () => {
    const target = path.join(dir, 'blocked');
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, 'x'), 'keep'); // non-empty dir: rename cannot replace it
    expect(() => writeAtomic(target, 'data')).toThrow();
    expect(fs.readdirSync(dir).filter((f) => f.endsWith('.tmp'))).toEqual([]);
  });

  it('gcLeases removes stale leases and old tmp files, keeps held leases and fresh tombstones', async () => {
    const live = path.join(dir, 'live.jsonl');
    const dead = path.join(dir, 'sub', 'dead.jsonl');
    const tomb = path.join(dir, 'tomb.jsonl');
    fs.mkdirSync(path.dirname(dead), { recursive: true });
    acquireLease(live, 'live', 'p', Date.now());
    acquireLease(dead, 'dead', 'p', Date.now());
    const dl = JSON.parse(fs.readFileSync(leasePath(dead), 'utf8')) as Lease;
    dl.holder.pid = DEAD_PID;
    fs.writeFileSync(leasePath(dead), JSON.stringify(dl));
    const t = acquireLease(tomb, 'tomb', 'p', Date.now()).lease!;
    releaseLease(tomb, t, { tombstone: true });
    const oldTmp = path.join(dir, 's.jsonl.lease.deadbeef.tmp');
    const freshTmp = path.join(dir, 's.jsonl.lease.cafebabe.tmp');
    fs.writeFileSync(oldTmp, '');
    fs.writeFileSync(freshTmp, '');
    const past = new Date(Date.now() - 10 * 60_000);
    fs.utimesSync(oldTmp, past, past);

    const r = await gcLeases(dir);
    expect(r.staleLeases).toBe(1);
    expect(r.tmpFiles).toBe(1);
    expect(fs.existsSync(leasePath(live))).toBe(true);
    expect(fs.existsSync(leasePath(dead))).toBe(false);
    expect(fs.existsSync(leasePath(tomb))).toBe(true);
    expect(fs.existsSync(oldTmp)).toBe(false);
    expect(fs.existsSync(freshTmp)).toBe(true);
    // Tombstones expire.
    const later = await gcLeases(dir, { now: Date.now() + 10 * 60_000, tmpMaxAgeMs: Infinity });
    expect(later.staleLeases).toBe(1);
    expect(fs.existsSync(leasePath(tomb))).toBe(false);
  });
});
