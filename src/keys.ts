/**
 * CONTRACT-V2 §13 — transcript identity.
 *
 * pi stamps every message object once at creation (`message.timestamp`, ms)
 * and the SAME object is streamed (message_start/_end) and persisted (JSONL
 * `type:"message"` entry). So:
 *
 *   key = toolResult → `tool:<toolCallId>`
 *         otherwise  → `<role>:<timestamp>`
 *
 * is stable live and in history. When a session has several rows with the
 * same base key (e.g. providers that reuse `call_1` tool-call ids every turn,
 * or a message re-persisted by pi), the 2nd, 3rd… occurrence in FILE ORDER get
 * `#2`, `#3`… Every producer of keys (history mapping, file cache, tailer,
 * live event path) assigns through the same {@link KeyCounter}, so they agree.
 *
 * Live consistency: a live transport seeds its {@link LiveKeyTracker} from a
 * {@link KeySnapshot} of the file (counts per base key at a byte offset). A live
 * message then gets the NEXT occurrence number for its base — exactly what the
 * history mapping will assign once pi appends it. The one race (a message that
 * was persisted after the transport attached but before the snapshot was read,
 * whose events are delivered live as well — or an owner ring replay of events
 * we missed while detached) is handled by "claims": snapshot rows that ended
 * after the attach point are claimable by a live message with the identical
 * identity (base key + timestamp), which then reuses that row's key instead of
 * counting it twice. pi emits events BEFORE persisting, so rows that ended
 * before the attach point can never be seen live (except via replay, for
 * which the window starts at the detach point).
 */
import fs from 'node:fs';
import { LineSplitter, parseEntry, type SessionEntry } from './jsonl.js';
import type { AgentMessage } from './types.js';

/** Base (un-suffixed) key of a pi message; null when it is not an object. */
export function messageKey(msg: AgentMessage | Record<string, unknown> | null | undefined): string | null {
  if (!msg || typeof msg !== 'object') return null;
  const role = typeof msg.role === 'string' && msg.role ? msg.role : 'unknown';
  if ((role === 'toolResult' || role === 'tool') && typeof msg.toolCallId === 'string' && msg.toolCallId) {
    return `tool:${msg.toolCallId}`;
  }
  const ts = typeof msg.timestamp === 'number' && Number.isFinite(msg.timestamp) ? String(msg.timestamp) : '?';
  return `${role}:${ts}`;
}

/** `base` for the 1st occurrence, `base#n` for the n-th (n ≥ 2). */
export function withSuffix(base: string, occurrence: number): string {
  return occurrence <= 1 ? base : `${base}#${occurrence}`;
}

/** Timestamp part of a message identity (claims compare base + timestamp). */
function stampOf(msg: { timestamp?: unknown } | null | undefined): number | null {
  const ts = msg?.timestamp;
  return typeof ts === 'number' && Number.isFinite(ts) ? ts : null;
}

function identity(base: string, ts: number | null): string {
  return `${base}|${ts ?? '?'}`;
}

/** Occurrence counter per base key (file-order assignment). */
export class KeyCounter {
  readonly counts: Map<string, number>;

  constructor(seed?: ReadonlyMap<string, number>) {
    this.counts = new Map(seed ?? []);
  }

  /** Count one more row with `base`; returns its (suffixed) key. */
  next(base: string): string {
    const n = (this.counts.get(base) ?? 0) + 1;
    this.counts.set(base, n);
    return withSuffix(base, n);
  }

  /** Assign the key of the next row for `msg` (null for non-objects). */
  assign(msg: AgentMessage | null | undefined): string | null {
    const base = messageKey(msg);
    return base === null ? null : this.next(base);
  }
}

/** A keyed row of a snapshot (only the most recent rows are kept). */
export interface KeyRow {
  base: string;
  ts: number | null;
  key: string;
  /** Absolute byte offset just after this row's line. */
  end: number;
}

/** Key state of a session file at a byte offset (all complete lines before it). */
export interface KeySnapshot {
  offset: number;
  counts: Map<string, number>;
  /** The most recent rows in file order (bounded). */
  recent: KeyRow[];
}

export const RECENT_ROWS = 512;

/** Rolling key state of a file being parsed in order (file cache / scanner). */
export class FileKeyState {
  readonly counter = new KeyCounter();
  readonly recent: KeyRow[] = [];

  /** Count a `type:"message"` entry whose line ends at `end`; returns its key. */
  add(msg: AgentMessage, end: number): string {
    const base = messageKey(msg) ?? 'unknown:?';
    const key = this.counter.next(base);
    this.recent.push({ base, ts: stampOf(msg), key, end });
    if (this.recent.length > RECENT_ROWS) this.recent.splice(0, this.recent.length - RECENT_ROWS);
    return key;
  }

  snapshot(offset: number): KeySnapshot {
    return { offset, counts: new Map(this.counter.counts), recent: this.recent.slice() };
  }
}

/** True for the entries the history mapping turns into rows (and counts). */
export function isMessageEntry(entry: SessionEntry | null): entry is SessionEntry & { message: AgentMessage } {
  return !!entry && entry.type === 'message' && !!entry.message && typeof entry.message === 'object';
}

/**
 * Counts as they were at `offset` (≤ snapshot.offset): rows that ended after it
 * are taken back out. Exact while those rows are within `recent`.
 */
export function countsAt(snap: KeySnapshot, offset: number): Map<string, number> {
  const counts = new Map(snap.counts);
  for (let i = snap.recent.length - 1; i >= 0; i--) {
    const row = snap.recent[i]!;
    if (row.end <= offset) break;
    const n = (counts.get(row.base) ?? 0) - 1;
    if (n > 0) counts.set(row.base, n);
    else counts.delete(row.base);
  }
  return counts;
}

const SCAN_CHUNK = 1024 * 1024;

/**
 * Standalone key scan of a session file (read-only), up to `limit` bytes.
 * Used when no file cache is wired (tests, tailer default) and for audits.
 */
export async function scanKeySnapshot(file: string, limit = Infinity): Promise<KeySnapshot> {
  const state = new FileKeyState();
  let fh: fs.promises.FileHandle;
  try {
    fh = await fs.promises.open(file, 'r');
  } catch {
    return state.snapshot(0);
  }
  try {
    const size = Math.min((await fh.stat()).size, limit);
    const splitter = new LineSplitter();
    const buf = Buffer.alloc(Math.min(SCAN_CHUNK, Math.max(1, size)));
    let pos = 0;
    while (pos < size) {
      const { bytesRead } = await fh.read(buf, 0, Math.min(buf.length, size - pos), pos);
      if (!bytesRead) break;
      const base = pos - splitter.pendingBytes;
      pos += bytesRead;
      const lines = splitter.push(buf.subarray(0, bytesRead));
      const ends = splitter.lastEnds;
      for (let i = 0; i < lines.length; i++) {
        const entry = parseEntry(lines[i]!);
        if (isMessageEntry(entry)) state.add(entry.message, base + ends[i]!);
      }
    }
    // A trailing line without '\n' is not a complete row yet (pi always
    // terminates lines) — the offset stops before it.
    return state.snapshot(pos - splitter.pendingBytes);
  } finally {
    await fh.close().catch(() => undefined);
  }
}

/**
 * Per-session live key assignment (message_start / message_end of whichever
 * transport is attached). See the module comment for the seeding rule.
 */
export class LiveKeyTracker {
  private counter = new KeyCounter();
  /** identity → keys of snapshot rows that a live message may still claim. */
  private claimable = new Map<string, string[]>();
  /** base → key of messages that saw message_start but not message_end yet. */
  private readonly open = new Map<string, string>();
  /** Key of the message the last message_start was for (deltas use it). */
  current: string | null = null;

  /**
   * Seed from a file snapshot. Rows that ended after `windowStart` may still
   * arrive live (race / replay) and are claimable instead of re-counted.
   * Open/current messages are kept (a re-seed never interrupts a stream).
   */
  seed(snap: KeySnapshot | null, windowStart = Infinity): void {
    this.counter = new KeyCounter(snap?.counts);
    this.claimable = new Map();
    for (const row of snap?.recent ?? []) {
      if (row.end <= windowStart) continue;
      const id = identity(row.base, row.ts);
      const list = this.claimable.get(id);
      if (list) list.push(row.key);
      else this.claimable.set(id, [row.key]);
    }
    // Keys already handed out live for still-open messages stay counted.
    for (const [base, key] of this.open) {
      const n = key === base ? 1 : Number(key.slice(base.length + 1)) || 1;
      if ((this.counter.counts.get(base) ?? 0) < n) this.counter.counts.set(base, n);
    }
  }

  /** Transport went away: nothing is open any more. */
  detach(): void {
    this.open.clear();
    this.current = null;
  }

  private assign(base: string, ts: number | null): string {
    const id = identity(base, ts);
    const list = this.claimable.get(id);
    if (list && list.length) {
      const key = list.shift()!;
      if (!list.length) this.claimable.delete(id);
      return key;
    }
    return this.counter.next(base);
  }

  /** message_start: assign (or reuse while open) and make it current. */
  start(msg: AgentMessage | null | undefined): string | null {
    const base = messageKey(msg);
    if (base === null) return null;
    let key = this.open.get(base);
    if (!key) {
      key = this.assign(base, stampOf(msg));
      this.open.set(base, key);
    }
    this.current = key;
    return key;
  }

  /** message_end: the open key for this message, else a fresh assignment. */
  end(msg: AgentMessage | null | undefined): string | null {
    const base = messageKey(msg);
    if (base === null) return null;
    const key = this.open.get(base) ?? this.assign(base, stampOf(msg));
    this.open.delete(base);
    if (this.current === key) this.current = null;
    return key;
  }

  /** Test/diagnostic view of the counts. */
  countOf(base: string): number {
    return this.counter.counts.get(base) ?? 0;
  }
}
