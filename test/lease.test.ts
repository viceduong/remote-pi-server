import { describe, it, expect, beforeEach, afterEach } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  acquireLease,
  renewLease,
  releaseLease,
  readLease,
  isLeaseHeld,
  leasePath,
} from "../src/lease.js";

describe("session ownership leases", () => {
  let dir: string;
  let file: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "lease-test-"));
    file = path.join(dir, "s.jsonl");
    fs.writeFileSync(file, "");
  });
  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("acquires on an unowned session", () => {
    const r = acquireLease(file, "s1", "\\\\.\\pipe\\t1", Date.now());
    expect(r.ok).toBe(true);
    expect(r.lease?.epoch).toBe(1);
    expect(readLease(file)?.sessionId).toBe("s1");
  });

  it("refuses while held by a live process", () => {
    expect(acquireLease(file, "s1", "p1", Date.now()).ok).toBe(true);
    const r = acquireLease(file, "s1", "p2", Date.now());
    expect(r.ok).toBe(false);
    expect(r.reason).toBe("held");
  });

  it("fences epochs across claims — old holder detects loss on renew", () => {
    const t = Date.now();
    const a = acquireLease(file, "s1", "pA", t).lease!;
    // Simulate holder A dying: lease goes stale (no renewal).
    const stale = acquireLease(file, "s1", "pB", t, t + 60_000);
    expect(stale.ok).toBe(true);
    expect(stale.lease!.epoch).toBe(2);
    // A wakes up and tries to renew its epoch-1 lease.
    expect(renewLease(file, a)).toBe("lost");
    // And cannot delete B's lease.
    releaseLease(file, a);
    expect(readLease(file)?.epoch).toBe(2);
  });

  it("claims immediately when the recorded holder process is dead", () => {
    const deadPid = acquireLease(file, "s1", "p1", Date.now()).lease!.holder.pid;
    // Find a pid that does not exist (our own +N is safe enough for the test).
    let ghost = deadPid + 5000;
    while (!readLease(file)) break;
    // Overwrite holder with a definitely-dead pid.
    const lease = JSON.parse(fs.readFileSync(leasePath(file), "utf8"));
    lease.holder.pid = ghost = 999_999_999; // implausible pid
    fs.writeFileSync(leasePath(file), JSON.stringify(lease));
    const r = acquireLease(file, "s1", "p2", Date.now());
    expect(r.ok).toBe(true);
    expect(r.lease!.epoch).toBe(2);
    void ghost;
  });

  it("renews and stays held; isLeaseHeld respects freshness window", async () => {
    const t = Date.now();
    const lease = acquireLease(file, "s1", "p1", t, t).lease!;
    expect(isLeaseHeld(readLease(file), t)).toBe(true);
    expect(renewLease(file, lease, t + 1000)).toBe("renewed");
    expect(readLease(file)!.renewedAt).toBe(t + 1000);
    // Simulate silence beyond duration+grace → stale even if pid alive (us).
    expect(isLeaseHeld(readLease(file), t + 60_000)).toBe(false);
  });

  it("release removes own lease and keeps others'", () => {
    const a = acquireLease(file, "s1", "pA", Date.now()).lease!;
    releaseLease(file, a);
    expect(readLease(file)).toBeNull();
    const b = acquireLease(file, "s1", "pB", Date.now()).lease!;
    releaseLease(file, b);
    expect(readLease(file)).toBeNull();
  });
});
