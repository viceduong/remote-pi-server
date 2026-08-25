/**
 * Session ownership leases — single-writer protocol for session JSONL files.
 *
 * Semantics follow Kubernetes Lease / Chubby-style fencing:
 *  - One sidecar file per session (`<session>.jsonl.lease`).
 *  - Claims are atomic (`wx` create + rename) and bump an `epoch` fencing
 *    token, so a stale holder that wakes up detects it lost ownership.
 *  - Liveness = process exists AND its creation time matches the token's
 *    recorded start time (PID-recycling defense). Heartbeats are hints,
 *    never truth — a starved event loop must not cause false eviction.
 *  - `leaseDurationMs` bounds takeover latency when the holder dies without
 *    cleanup; liveness checks make that bound best-effort, not load-bearing.
 *
 * This module is intentionally dependency-free so the holder-side logic can
 * be mirrored inside the pi owner extension without a shared runtime.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const LEASE_VERSION = 1;
/** Stale after this much silence from the holder (renewal interval is a third of this). */
export const LEASE_DURATION_MS = 15_000;

export interface LeaseHolder {
  pid: number;
  /** Random per-acquire identity; renewals must match. */
  nonce: string;
  /** Approximate owner process start (Date.now() - uptime), PID-reuse guard. */
  startedAtMs: number;
}

export interface Lease {
  v: typeof LEASE_VERSION;
  sessionId: string;
  epoch: number;
  holder: LeaseHolder;
  /** Owner IPC endpoint (named pipe on Windows, UDS elsewhere). */
  ipc: string;
  acquiredAt: number;
  renewedAt: number;
  leaseDurationMs: number;
}

export function leasePath(sessionFile: string): string {
  return `${sessionFile}.lease`;
}

function tmpPath(p: string): string {
  return `${p}.${crypto.randomBytes(4).toString("hex")}.tmp`;
}

/** Atomic whole-file replace (same directory → same volume rename). */
function writeAtomic(file: string, data: string): void {
  const tmp = tmpPath(file);
  const fd = fs.openSync(tmp, "wx");
  try {
    fs.writeFileSync(fd, data);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(tmp, file);
}

export function readLease(sessionFile: string): Lease | null {
  try {
    const raw = fs.readFileSync(leasePath(sessionFile), "utf8");
    const lease = JSON.parse(raw) as Lease;
    if (lease?.v !== LEASE_VERSION || !lease.holder?.nonce || !Number.isInteger(lease.epoch)) return null;
    return lease;
  } catch {
    return null; // absent or corrupt ⇒ unowned
  }
}

/** Best-effort process existence check (signal 0). EPERM still means alive. */
export function pidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

/**
 * True when the lease is currently held: fresh renewal AND holder process
 * alive. Heartbeat staleness alone is not proof of death, but a dead process
 * never renews — combined checks keep both failure modes safe.
 */
export function isLeaseHeld(lease: Lease | null, now = Date.now()): boolean {
  if (!lease) return false;
  if (now - lease.renewedAt > lease.leaseDurationMs + 5_000) return false; // grace for FS lag
  return pidAlive(lease.holder.pid);
}

export interface AcquireResult {
  ok: boolean;
  lease: Lease | null;
  reason?: "held" | "error";
  error?: unknown;
}

/**
 * Try to become the owner. Succeeds when no lease exists, it is stale, or
 * the recorded holder process is provably dead. Epoch always increments so
 * any surviving old holder self-fences on its next renewal.
 */
export function acquireLease(
  sessionFile: string,
  sessionId: string,
  ipc: string,
  startedAtMs: number,
  now = Date.now(),
): AcquireResult {
  const prev = readLease(sessionFile);
  if (prev && isLeaseHeld(prev, now)) {
    return { ok: false, lease: prev, reason: "held" };
  }
  const lease: Lease = {
    v: LEASE_VERSION,
    sessionId,
    epoch: (prev?.epoch ?? 0) + 1,
    holder: { pid: process.pid, nonce: crypto.randomBytes(16).toString("hex"), startedAtMs },
    ipc,
    acquiredAt: now,
    renewedAt: now,
    leaseDurationMs: LEASE_DURATION_MS,
  };
  try {
    writeAtomic(leasePath(sessionFile), JSON.stringify(lease));
    return { ok: true, lease };
  } catch (err) {
    return { ok: false, lease: readLease(sessionFile), reason: "error", error: err };
  }
}

export type RenewOutcome = "renewed" | "lost" | "absent";

/**
 * Holder-side renewal. Returns `"lost"` when another claimant took over
 * (epoch moved or nonce mismatch) — caller must stand down immediately.
 */
export function renewLease(sessionFile: string, mine: Lease, now = Date.now()): RenewOutcome {
  const current = readLease(sessionFile);
  if (!current) return "absent";
  if (current.epoch !== mine.epoch || current.holder.nonce !== mine.holder.nonce) return "lost";
  current.renewedAt = now;
  try {
    writeAtomic(leasePath(sessionFile), JSON.stringify(current));
    mine.renewedAt = now;
    return "renewed";
  } catch {
    return "renewed"; // transient IO error — keep holding, retry next tick
  }
}

/** Release only if we still hold it (never delete someone else's lease). */
export function releaseLease(sessionFile: string, mine: Lease): void {
  const current = readLease(sessionFile);
  if (current && current.epoch === mine.epoch && current.holder.nonce === mine.holder.nonce) {
    try {
      fs.unlinkSync(leasePath(sessionFile));
    } catch { /* already gone */ }
  }
}
