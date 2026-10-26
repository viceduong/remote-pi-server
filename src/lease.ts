/**
 * Session ownership leases — the single-writer protocol for session JSONL files.
 *
 * Leases are the ONLY ownership/liveness signal the bridge uses (the old host
 * process probe is gone). One sidecar per session (`<session>.jsonl.lease`):
 *
 *  - `kind: "tui"`    — a host pi TUI (remote-pi-owner extension) owns the file;
 *                       the bridge proxies turns to it over `ipc`.
 *  - `kind: "bridge"` — the bridge spawned a `pi --mode rpc` child for the file
 *                       (holder.pid = child pid, `bridgePid` = server pid).
 *                       A host TUI opening the file PRE-EMPTS it (epoch+1); the
 *                       bridge sees the fence on its next renew and stands down.
 *  - absent kind      — written by a pre-v4 owner extension; treated as "tui".
 *
 * Held = holder pid alive (+ process start time matches `startedAtMs` where the
 * platform exposes it cheaply, i.e. Linux /proc). Heartbeat staleness is only a
 * fallback when pid liveness cannot be determined. A lease that cannot be READ
 * (EPERM/EBUSY mid-rename on Windows, ...) is `unknown` and must be treated as
 * held — never as "unowned".
 *
 * Dependency-free on purpose: the holder side is mirrored in the pi extension
 * `~/.pi/agent/extensions/remote-pi-owner.ts`. Keep both in sync.
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const LEASE_VERSION = 1;
/** Fallback staleness window (only used when pid liveness is undeterminable). */
export const LEASE_DURATION_MS = 15_000;
/** Holders renew this often; a fenced bridge child notices within one tick. */
export const RENEW_INTERVAL_MS = 5_000;
/** Tolerance between recorded and observed process start time. */
const START_TIME_TOLERANCE_MS = 30_000;

export type LeaseKind = 'tui' | 'bridge';

export interface LeaseHolder {
  pid: number;
  /** Random per-acquire identity; renewals must match. */
  nonce: string;
  /** Approximate holder process start (epoch ms) — PID-reuse guard. */
  startedAtMs: number;
}

export interface Lease {
  v: typeof LEASE_VERSION;
  sessionId: string;
  epoch: number;
  holder: LeaseHolder;
  /** Owner IPC endpoint (named pipe on Windows, UDS elsewhere). '' for bridge leases. */
  ipc: string;
  acquiredAt: number;
  renewedAt: number;
  leaseDurationMs: number;
  /** v4+: who holds it. Absent = legacy owner extension (a TUI). */
  kind?: LeaseKind;
  /** Bridge leases only: pid of the bridge server that spawned the child. */
  bridgePid?: number;
  /**
   * Tombstone: the holder released cleanly at this time (v4 extension). Never
   * held; tells the bridge that recent file writes came from a lease holder,
   * not from an unowned writer. GC removes tombstones after a few minutes.
   */
  releasedAt?: number;
}

/** Result of inspecting a lease file. `unknown` must be treated as held. */
export type LeaseState = 'absent' | 'held' | 'stale' | 'unknown';

export interface LeaseInspection {
  state: LeaseState;
  lease: Lease | null;
  error?: string;
}

export function leasePath(sessionFile: string): string {
  return `${sessionFile}.lease`;
}

export function leaseKind(lease: Lease): LeaseKind {
  return lease.kind === 'bridge' ? 'bridge' : 'tui';
}

/** True for any state other than absent/stale (unknown counts as held). */
export function isOwned(i: LeaseInspection): boolean {
  return i.state === 'held' || i.state === 'unknown';
}

function sleepSync(ms: number): void {
  try {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
  } catch {
    /* Atomics.wait unavailable — retry immediately */
  }
}

const RETRYABLE_RENAME = new Set(['EPERM', 'EBUSY', 'EACCES', 'EEXIST', 'ENOTEMPTY']);

/**
 * Atomic whole-file replace (tmp in the same directory, then rename). Windows
 * renames fail transiently with EPERM/EBUSY while a reader has the target
 * open — retry a few times, and never leak the tmp file on final failure.
 */
export function writeAtomic(file: string, data: string): void {
  const tmp = `${file}.${crypto.randomBytes(4).toString('hex')}.tmp`;
  fs.writeFileSync(tmp, data, { flag: 'wx' });
  for (let attempt = 0; ; attempt++) {
    try {
      fs.renameSync(tmp, file);
      return;
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code ?? '';
      if (attempt >= 4 || !RETRYABLE_RENAME.has(code)) {
        try { fs.unlinkSync(tmp); } catch { /* already gone */ }
        throw err;
      }
      sleepSync(10 * (attempt + 1));
    }
  }
}

function validLease(x: unknown): x is Lease {
  const l = x as Lease | null;
  return !!l && l.v === LEASE_VERSION && typeof l.holder?.nonce === 'string'
    && Number.isInteger(l.epoch) && Number.isInteger(l.holder?.pid);
}

/** Raw read: absent (ENOENT / corrupt), ok, or unknown (any other IO error). */
export function readLeaseRaw(sessionFile: string): { state: 'absent' } | { state: 'ok'; lease: Lease } | { state: 'unknown'; error: string } {
  let raw: string;
  try {
    raw = fs.readFileSync(leasePath(sessionFile), 'utf8');
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ENOENT' || code === 'ENOTDIR') return { state: 'absent' };
    return { state: 'unknown', error: code ?? (err as Error).message };
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    // Corrupt content cannot come from an atomic writer — treat as absent.
    return validLease(parsed) ? { state: 'ok', lease: parsed } : { state: 'absent' };
  } catch {
    return { state: 'absent' };
  }
}

/** Non-blocking variant of readLeaseRaw (request handlers / list summaries). */
export async function readLeaseRawAsync(sessionFile: string): Promise<ReturnType<typeof readLeaseRaw>> {
  let raw: string;
  try {
    raw = await fs.promises.readFile(leasePath(sessionFile), 'utf8');
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ENOENT' || code === 'ENOTDIR') return { state: 'absent' };
    return { state: 'unknown', error: code ?? (err as Error).message };
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    return validLease(parsed) ? { state: 'ok', lease: parsed } : { state: 'absent' };
  } catch {
    return { state: 'absent' };
  }
}

/** Back-compat helper: the lease if readable, else null. Prefer inspectLease. */
export function readLease(sessionFile: string): Lease | null {
  const r = readLeaseRaw(sessionFile);
  return r.state === 'ok' ? r.lease : null;
}

/** pid liveness: true / false / null (cannot determine). EPERM = alive. */
export function pidLiveness(pid: number): boolean | null {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'EPERM') return true;
    if (code === 'ESRCH') return false;
    return null;
  }
}

export function pidAlive(pid: number): boolean {
  return pidLiveness(pid) !== false;
}

let bootTimeMs: number | null | undefined;
let clockTicks = 100;

/**
 * Process start time in epoch ms when cheaply available (Linux /proc only).
 * Windows/macOS would need WMI/ps per call — not cheap, so null.
 */
export function processStartMs(pid: number): number | null {
  if (process.platform !== 'linux') return null;
  try {
    if (bootTimeMs === undefined) {
      const stat = fs.readFileSync('/proc/stat', 'utf8');
      const m = /^btime\s+(\d+)/m.exec(stat);
      bootTimeMs = m ? Number(m[1]) * 1000 : null;
    }
    if (bootTimeMs === null) return null;
    const raw = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = raw.slice(raw.lastIndexOf(')') + 2).split(' ');
    const ticks = Number(fields[19]);
    if (!Number.isFinite(ticks)) return null;
    return bootTimeMs + (ticks / clockTicks) * 1000;
  } catch {
    return null;
  }
}

/** Test hook. */
export function _setClockTicks(n: number): void {
  clockTicks = n;
}

/**
 * Held = holder pid alive (and start time consistent when observable). For
 * bridge leases the spawning server must be alive too — a lease left behind by
 * a dead bridge instance is stale even if an orphaned child lingers.
 * Heartbeat freshness only decides when liveness is undeterminable.
 */
export function isLeaseHeld(lease: Lease | null, now = Date.now()): boolean {
  if (!lease) return false;
  if (typeof lease.releasedAt === 'number') return false;
  if (lease.kind === 'bridge' && typeof lease.bridgePid === 'number' && pidLiveness(lease.bridgePid) === false) {
    return false;
  }
  const alive = pidLiveness(lease.holder.pid);
  if (alive === false) return false;
  if (alive === true) {
    const started = processStartMs(lease.holder.pid);
    if (started !== null && lease.holder.startedAtMs > 0
        && started > lease.holder.startedAtMs + START_TIME_TOLERANCE_MS) {
      return false; // pid recycled by a younger process
    }
    return true;
  }
  return now - lease.renewedAt <= (lease.leaseDurationMs || LEASE_DURATION_MS) + 5_000;
}

export function inspectLease(sessionFile: string, now = Date.now()): LeaseInspection {
  const r = readLeaseRaw(sessionFile);
  if (r.state === 'absent') return { state: 'absent', lease: null };
  if (r.state === 'unknown') return { state: 'unknown', lease: null, error: r.error };
  return { state: isLeaseHeld(r.lease, now) ? 'held' : 'stale', lease: r.lease };
}

export async function inspectLeaseAsync(sessionFile: string, now = Date.now()): Promise<LeaseInspection> {
  const r = await readLeaseRawAsync(sessionFile);
  if (r.state === 'absent') return { state: 'absent', lease: null };
  if (r.state === 'unknown') return { state: 'unknown', lease: null, error: r.error };
  return { state: isLeaseHeld(r.lease, now) ? 'held' : 'stale', lease: r.lease };
}

export interface AcquireOptions {
  kind?: LeaseKind;
  /** Holder pid (defaults to this process). */
  holderPid?: number;
  /** Bridge leases: the server pid. */
  bridgePid?: number;
  /** Pre-empt a held lease of this kind (TUIs pre-empt bridge leases). */
  preemptKind?: LeaseKind;
  now?: number;
}

export interface AcquireResult {
  ok: boolean;
  lease: Lease | null;
  reason?: 'held' | 'unknown' | 'error' | 'race';
  error?: unknown;
}

/**
 * Try to become the owner. Succeeds when no lease exists, it is stale, or it
 * is held by a pre-emptable kind. The epoch always increments, so a surviving
 * old holder self-fences on its next renewal. Verifies after writing so two
 * racing claimants cannot both believe they won.
 */
export function acquireLease(
  sessionFile: string,
  sessionId: string,
  ipc: string,
  startedAtMs: number,
  nowOrOpts: number | AcquireOptions = Date.now(),
): AcquireResult {
  const opts: AcquireOptions = typeof nowOrOpts === 'number' ? { now: nowOrOpts } : nowOrOpts;
  const now = opts.now ?? Date.now();
  const prev = inspectLease(sessionFile, now);
  if (prev.state === 'unknown') return { ok: false, lease: null, reason: 'unknown' };
  if (prev.state === 'held') {
    const preemptable = opts.preemptKind && prev.lease && leaseKind(prev.lease) === opts.preemptKind;
    if (!preemptable) return { ok: false, lease: prev.lease, reason: 'held' };
  }
  const lease: Lease = {
    v: LEASE_VERSION,
    sessionId,
    epoch: (prev.lease?.epoch ?? 0) + 1,
    holder: { pid: opts.holderPid ?? process.pid, nonce: crypto.randomBytes(16).toString('hex'), startedAtMs },
    ipc,
    acquiredAt: now,
    renewedAt: now,
    leaseDurationMs: LEASE_DURATION_MS,
    kind: opts.kind ?? 'tui',
    ...(opts.bridgePid !== undefined ? { bridgePid: opts.bridgePid } : {}),
  };
  try {
    writeAtomic(leasePath(sessionFile), JSON.stringify(lease));
  } catch (err) {
    return { ok: false, lease: readLease(sessionFile), reason: 'error', error: err };
  }
  const check = readLease(sessionFile);
  if (!check || check.holder.nonce !== lease.holder.nonce) {
    return { ok: false, lease: check, reason: 'race' };
  }
  return { ok: true, lease };
}

export type RenewOutcome = 'renewed' | 'lost' | 'absent' | 'unknown';

/**
 * Holder-side renewal. `lost` = another claimant took over (epoch moved or
 * nonce mismatch) — the caller must stand down immediately. `patch` lets the
 * bridge update holder.pid once its child has spawned.
 */
export function renewLease(
  sessionFile: string,
  mine: Lease,
  now = Date.now(),
  patch?: Partial<Pick<Lease, 'ipc' | 'holder'>>,
): RenewOutcome {
  const r = readLeaseRaw(sessionFile);
  if (r.state === 'absent') return 'absent';
  if (r.state === 'unknown') return 'unknown'; // keep holding, retry next tick
  const current = r.lease;
  if (current.epoch !== mine.epoch || current.holder.nonce !== mine.holder.nonce) return 'lost';
  const next: Lease = { ...current, ...(patch ?? {}), renewedAt: now };
  try {
    writeAtomic(leasePath(sessionFile), JSON.stringify(next));
    mine.renewedAt = now;
    if (patch?.holder) mine.holder = patch.holder;
    if (patch?.ipc !== undefined) mine.ipc = patch.ipc;
  } catch {
    /* transient IO error — keep holding, retry next tick */
  }
  return 'renewed';
}

/**
 * Release only if we still hold it (never touch someone else's lease).
 * `tombstone` leaves a `releasedAt` marker instead of deleting the file.
 */
export function releaseLease(sessionFile: string, mine: Lease, opts: { tombstone?: boolean; now?: number } = {}): void {
  const current = readLease(sessionFile);
  if (!current || current.epoch !== mine.epoch || current.holder.nonce !== mine.holder.nonce) return;
  if (opts.tombstone) {
    try {
      writeAtomic(leasePath(sessionFile), JSON.stringify({ ...current, ipc: '', releasedAt: opts.now ?? Date.now() }));
      return;
    } catch { /* fall back to unlink */ }
  }
  try {
    fs.unlinkSync(leasePath(sessionFile));
  } catch { /* already gone */ }
}

/** Tombstones older than this are garbage-collected. */
export const TOMBSTONE_TTL_MS = 5 * 60_000;

/** Tmp files from writeAtomic (lease + queue sidecars, hex or pid suffix). */
const SIDECAR_TMP_RE = /\.(?:lease|json)\.[0-9a-f]+\.tmp$/;

export interface GcResult {
  staleLeases: number;
  tmpFiles: number;
}

/**
 * Garbage-collect stale leases (holder dead) and orphaned tmp files left by
 * failed atomic writes. Async + sequential so it never stalls the event loop.
 */
export async function gcLeases(root: string, opts: { tmpMaxAgeMs?: number; now?: number } = {}): Promise<GcResult> {
  const tmpMaxAgeMs = opts.tmpMaxAgeMs ?? 60_000;
  const result: GcResult = { staleLeases: 0, tmpFiles: 0 };
  const walk = async (dir: string, depth: number): Promise<void> => {
    if (depth > 4) return;
    let children: fs.Dirent[];
    try {
      children = await fs.promises.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const child of children) {
      const full = path.join(dir, child.name);
      if (child.isDirectory()) {
        await walk(full, depth + 1);
        continue;
      }
      if (!child.isFile()) continue;
      if (SIDECAR_TMP_RE.test(child.name)) {
        try {
          const st = await fs.promises.stat(full);
          if ((opts.now ?? Date.now()) - st.mtimeMs > tmpMaxAgeMs) {
            await fs.promises.unlink(full);
            result.tmpFiles++;
          }
        } catch { /* raced */ }
        continue;
      }
      if (child.name.endsWith('.jsonl.lease')) {
        const sessionFile = full.slice(0, -'.lease'.length);
        const i = await inspectLeaseAsync(sessionFile, opts.now);
        if (i.state !== 'stale' || !i.lease) continue;
        const releasedAt = i.lease.releasedAt;
        if (typeof releasedAt === 'number' && (opts.now ?? Date.now()) - releasedAt < TOMBSTONE_TTL_MS) continue;
        // Re-check identity right before deleting (a new holder may have
        // claimed it between inspect and unlink).
        const againRaw = await readLeaseRawAsync(sessionFile);
        const again = againRaw.state === 'ok' ? againRaw.lease : null;
        if (again && again.epoch === i.lease.epoch && again.holder.nonce === i.lease.holder.nonce) {
          try {
            await fs.promises.unlink(full);
            result.staleLeases++;
          } catch { /* raced */ }
        }
      }
    }
  };
  await walk(root, 0);
  return result;
}
