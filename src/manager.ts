import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import type { Logger } from 'pino';
import { LeaseHeldError, Session, type EventSink } from './session.js';
import { mapAgentMessage } from './history.js';
import { manglePath } from './paths.js';
import {
  gcLeases,
  inspectLease,
  inspectLeaseAsync,
  leaseKind,
  leasePath,
  readLeaseRawAsync,
  writeAtomic,
  type Lease,
  type LeaseInspection,
} from './lease.js';
import { SessionFileCache, type FileSummary } from './filecache.js';
import { stripLoneSurrogates } from './jsonl.js';
import {
  QUEUE_MAX_ACTIVE,
  QUEUE_MAX_ATTEMPTS,
  backoffMs,
  isActive,
  newQueueId,
  pendingView,
  pruneQueue,
  queueFilePath,
  restoreItems,
  type PendingItemView,
  type QueueItem,
} from './queue.js';
import type { AgentMessage, AgentState, ChatMessage, LiveInstance, SessionOwner, SessionSummary } from './types.js';

export interface SessionManagerOptions {
  sessionDir: string;
  workdir: string;
  bin: string;
  maxAgents: number;
  idleKillMs: number;
  extraArgs: string[];
  log: Logger;
  /** Test hook: disable timers (sweep, lease GC, dir watcher). */
  noTimers?: boolean;
}

/** Agent-slot capacity reached (503). */
export class BusyError extends Error {
  readonly code = 503;
  constructor(max?: number) {
    super(max
      ? `All ${max} agent slots are busy — stop an idle session or retry shortly`
      : 'All agents busy');
  }
}

/** pi failed to start (409, carries pi's startup stderr). */
export class SpawnError extends Error {
  readonly code = 409;
  constructor(message: string) {
    super(message);
  }
}

/** Another writer owns the session (409 `session_live`). */
export class SessionLiveError extends Error {
  readonly code = 409;
  readonly apiCode = 'session_live';
  constructor(message: string) {
    super(message);
  }
}

/** Lease-derived ownership of a session file. `unknown` = treat as owned. */
export type LeaseOwner = SessionOwner | 'unknown';

/** Metadata of one pi session file (from the incremental file cache). */
interface SessionFileMeta {
  id: string;
  file: string;
  name: string;
  workdir: string | null;
  createdAt: number;
  /** File mtime (rounded — iOS decodes Int strictly). */
  lastActivityAt: number;
  messageCount: number;
  lastMessageAt: number | null;
  /** A `<file>.lease` sidecar was present at the last directory walk. */
  hasLease: boolean;
}

const ACTIVE_WINDOW_MS = 5 * 60_000;
const WRITING_WINDOW_MS = 30_000;
const INDEX_TTL_MS = 5_000;
/** Serve a stale walk while refreshing in the background, up to this age. */
const INDEX_MAX_STALE_MS = 60_000;
const WALK_CONCURRENCY = 16;
const MAX_LISTED = 250;
const MIN_PREFIX = 12;
const UNLOAD_AFTER_MS = 15 * 60_000;
const CREATE_IDEMPOTENCY_MS = 10 * 60_000;
const LEASE_GC_INTERVAL_MS = 10 * 60_000;
/** A file written this recently by nobody holding a lease = unowned live writer. */
const UNOWNED_WRITE_WINDOW_MS = 30_000;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function shorten(text: string): string {
  return text.length > 48 ? `${text.slice(0, 48)}…` : text;
}

/**
 * Owns the session lifecycle and discovers ALL pi sessions on the host (the
 * session dir defaults to pi's own storage). Ownership comes from leases only:
 *  - TUI lease held → proxy to the host TUI over its IPC endpoint;
 *  - no lease       → spawn a `pi --mode rpc` child under a bridge lease;
 *  - SSE viewers of unowned sessions get a read-only file tailer.
 */
export class SessionManager {
  private readonly sessions = new Map<string, Session>();
  private readonly options: SessionManagerOptions;
  /** Last directory walk (disk metas only; loaded sessions are overlaid per read). */
  private walkCache: { at: number; generation: number; entries: Map<string, SessionFileMeta> } | null = null;
  private indexGeneration = 0;
  private walkInflight: Promise<Map<string, SessionFileMeta>> | null = null;
  private readonly startLocks = new Map<string, Promise<Session | null>>();
  /** Idempotency keys for mutating POSTs (create/fork): key → first result for 10 min. */
  private readonly idempotency = new Map<string, { at: number; promise: Promise<unknown> }>();
  /** Last time a bridge transport for a file ended (unowned-writer heuristic). */
  private readonly lastBridgeExit = new Map<string, number>();
  readonly files: SessionFileCache;

  constructor(options: SessionManagerOptions) {
    this.options = options;
    fs.mkdirSync(options.sessionDir, { recursive: true });
    this.files = new SessionFileCache({ onAsyncUpdate: () => this.invalidateIndex() });
    if (!options.noTimers) {
      setInterval(() => this.sweep(), 60_000).unref();
      setTimeout(() => void this.collectLeaseGarbage(), 5_000).unref();
      setInterval(() => void this.collectLeaseGarbage(), LEASE_GC_INTERVAL_MS).unref();
      this.watchSessionDir();
    }
  }

  /** Recursive fs.watch; only session JSONL changes invalidate the index —
   *  lease/tmp/queue/stats sidecar writes are ignored (no invalidation storms). */
  private watchSessionDir(): void {
    try {
      const watcher = fs.watch(this.options.sessionDir, { recursive: true }, (_ev, name) => {
        const n = name ? name.toString() : '';
        if (!n || n.endsWith('.jsonl')) this.invalidateIndex();
      });
      watcher.on('error', () => this.invalidateIndex());
    } catch {
      this.options.log.debug('recursive fs.watch unavailable — using TTL only');
    }
  }

  async collectLeaseGarbage(): Promise<void> {
    try {
      const r = await gcLeases(this.options.sessionDir);
      if (r.staleLeases || r.tmpFiles) this.options.log.info(r, 'lease GC removed stale sidecars');
    } catch (err) {
      this.options.log.warn({ err: (err as Error).message }, 'lease GC failed');
    }
  }

  /* ---------------- ids & paths ---------------- */

  private nextId(): string {
    return crypto.randomUUID().replace(/-/g, '').slice(0, 16);
  }

  /** App-created sessions live inside the mangled workdir dir — where pi itself stores them. */
  sessionFile(id: string): string {
    return path.join(this.options.sessionDir, manglePath(this.options.workdir), `${id}.jsonl`);
  }

  /* ---------------- discovery ---------------- */

  invalidateIndex(): void {
    this.indexGeneration++;
  }

  /** Last walk + overlay (possibly stale) — for background maintenance only. */
  private peekIndex(): Map<string, SessionFileMeta> {
    return this.overlay(this.walkCache?.entries ?? new Map());
  }

  /**
   * Session index with stale-while-revalidate semantics. The directory walk is
   * async (fs.promises + incremental file summaries) and shared by concurrent
   * callers. A dirty (watcher-invalidated) or expired walk is refreshed in the
   * background while callers get the previous one — unless it is very old or
   * `fresh` is requested (e.g. an id lookup that missed).
   */
  private async getIndex(fresh = false): Promise<Map<string, SessionFileMeta>> {
    const cache = this.walkCache;
    const age = cache ? Date.now() - cache.at : Infinity;
    const clean = cache !== null && cache.generation === this.indexGeneration && age < INDEX_TTL_MS;
    if (cache && clean && !fresh) return this.overlay(cache.entries);
    const walk = this.refreshWalk();
    if (cache && !fresh && age < INDEX_MAX_STALE_MS) {
      walk.catch(() => undefined);
      return this.overlay(cache.entries);
    }
    return this.overlay(await walk);
  }

  private refreshWalk(): Promise<Map<string, SessionFileMeta>> {
    if (this.walkInflight) return this.walkInflight;
    const generation = this.indexGeneration;
    const p = (async () => {
      const entries = new Map<string, SessionFileMeta>();
      await this.walkSessionDir(this.options.sessionDir, entries, 0);
      this.walkCache = { at: Date.now(), generation, entries };
      return entries;
    })().finally(() => {
      if (this.walkInflight === p) this.walkInflight = null;
    });
    this.walkInflight = p;
    return p;
  }

  /**
   * Loaded sessions win: re-key any meta pointing at the same FILE under the
   * session's canonical id (pre-rekey placeholder ids never list twice) and
   * add running sessions whose JSONL pi has not written yet.
   */
  private overlay(walk: Map<string, SessionFileMeta>): Map<string, SessionFileMeta> {
    if (this.sessions.size === 0) return walk;
    const entries = new Map(walk);
    const byFile = new Map<string, string>();
    for (const [k, m] of entries) byFile.set(path.resolve(m.file), k);
    for (const s of this.sessions.values()) {
      const key = byFile.get(path.resolve(s.file));
      const meta = key ? entries.get(key) : undefined;
      if (meta && key) {
        if (key !== s.id) {
          entries.delete(key);
          entries.set(s.id, { ...meta, id: s.id });
        }
      } else if (s.running) {
        entries.set(s.id, {
          id: s.id,
          file: s.file,
          name: s.name,
          workdir: s.workdir,
          createdAt: s.createdAt,
          lastActivityAt: Math.round(s.lastActivityAt),
          messageCount: s.messageCount,
          lastMessageAt: null,
          hasLease: true,
        });
      }
    }
    return entries;
  }

  private async walkSessionDir(dir: string, out: Map<string, SessionFileMeta>, depth: number): Promise<void> {
    if (depth > 3) return;
    let children: fs.Dirent[];
    try {
      children = await fs.promises.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    const names = new Set(children.map((c) => c.name));
    const files = children.filter((c) => c.isFile() && c.name.endsWith('.jsonl'));
    // Bounded parallelism: enough to keep the libuv pool busy, never a flood.
    for (let i = 0; i < files.length; i += WALK_CONCURRENCY) {
      await Promise.all(files.slice(i, i + WALK_CONCURRENCY).map(async (child) => {
        const full = path.join(dir, child.name);
        let stat: fs.Stats;
        try {
          stat = await fs.promises.stat(full);
        } catch {
          return;
        }
        const sum = await this.files.summaryFast(full, stat);
        const meta = this.metaFor(full, stat, sum, names.has(`${child.name}.lease`));
        const prev = out.get(meta.id);
        if (!prev || prev.lastActivityAt < meta.lastActivityAt) out.set(meta.id, meta);
      }));
    }
    for (const child of children) {
      if (child.isDirectory() && !child.name.startsWith('.')) {
        await this.walkSessionDir(path.join(dir, child.name), out, depth + 1);
      }
    }
  }

  private metaFor(file: string, stat: fs.Stats, sum: FileSummary, hasLease: boolean): SessionFileMeta {
    const base = path.basename(file, '.jsonl');
    let name = sum.sessionName ?? (sum.firstUserText ? shorten(sum.firstUserText) : null);
    if (!name && sum.cwd) {
      const dirName = sum.cwd.split(/[\\/]/).filter(Boolean).pop();
      const date = stat.mtime.toISOString().slice(0, 10);
      name = dirName ? `${dirName} · ${date}` : date;
    }
    return {
      id: sum.headerId ?? base,
      file,
      name: name ?? `pi session · ${base.slice(0, 8)}`,
      workdir: sum.cwd,
      createdAt: sum.createdAt ?? Math.round(stat.birthtimeMs || stat.mtimeMs),
      lastActivityAt: Math.round(stat.mtimeMs),
      messageCount: sum.messageCount,
      lastMessageAt: sum.lastMessageAt,
      hasLease,
    };
  }

  /** Exact id, else a unique prefix of ≥ 12 chars (UUIDv7 share time prefixes). */
  private async resolveMeta(id: string): Promise<SessionFileMeta | null> {
    const lookup = (index: Map<string, SessionFileMeta>): SessionFileMeta | null | undefined => {
      const exact = index.get(id);
      if (exact) return exact;
      if (id.length < MIN_PREFIX) return undefined;
      let hit: SessionFileMeta | undefined;
      for (const meta of index.values()) {
        if (!meta.id.startsWith(id)) continue;
        if (hit) return null; // ambiguous — never guess
        hit = meta;
      }
      return hit;
    };
    const first = lookup(await this.getIndex());
    if (first !== undefined) return first;
    // Miss on a possibly stale index (a session created seconds ago): re-walk once.
    return lookup(await this.getIndex(true)) ?? null;
  }

  private sessionByFile(file: string): Session | undefined {
    const key = path.resolve(file);
    for (const s of this.sessions.values()) if (path.resolve(s.file) === key) return s;
    return undefined;
  }

  /** Get-or-create the registered Session for a meta (queue restored once). */
  private sessionFor(meta: SessionFileMeta): Session {
    const existing = this.sessions.get(meta.id) ?? this.sessionByFile(meta.file);
    if (existing) return existing;
    const session = new Session(
      meta.id,
      meta.name,
      meta.file,
      this.options.bin,
      meta.workdir ?? this.options.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log,
      'pi',
    );
    session.messageCount = meta.messageCount;
    session.createdAt = meta.createdAt;
    session.lastActivityAt = meta.lastActivityAt;
    this.register(session);
    return session;
  }

  private register(session: Session): void {
    this.sessions.set(session.id, session);
    this.wireQueue(session);
    session.onFenced = () => this.handleFenced(session);
  }

  get(id: string): Session | null {
    return this.sessions.get(id) ?? null;
  }

  /** Registered Session for an id (exact or unique ≥12-char prefix); loads its queue. */
  async find(id: string): Promise<Session | null> {
    const loaded = this.sessions.get(id);
    if (loaded) return loaded;
    const meta = await this.resolveMeta(id);
    return meta ? this.sessionFor(meta) : null;
  }

  /* ---------------- ownership ---------------- */

  /** Fresh (uncached) lease-derived ownership of a session. */
  async ownership(id: string): Promise<{ owner: LeaseOwner; lease: Lease | null; session: Session | null }> {
    const session = await this.find(id);
    if (!session) return { owner: 'none', lease: null, session: null };
    if (session.hasChild) return { owner: 'bridge', lease: null, session };
    const insp = await inspectLeaseAsync(session.file);
    if (insp.state === 'unknown') return { owner: 'unknown', lease: null, session };
    if (insp.state === 'held' && insp.lease) return { owner: leaseKind(insp.lease), lease: insp.lease, session };
    return { owner: 'none', lease: null, session };
  }

  /** Lease held by a host TUI (proxy target), if any. */
  async getLiveLeaseInfo(id: string): Promise<Lease | null> {
    const o = await this.ownership(id);
    return o.owner === 'tui' ? o.lease : null;
  }

  /** True when a host TUI (or an undeterminable owner) holds the session. */
  async isHostOwned(id: string): Promise<boolean> {
    const o = (await this.ownership(id)).owner;
    return o === 'tui' || o === 'unknown';
  }

  /**
   * No lease, no bridge transport, yet the file was written in the last 30 s
   * by someone other than our own (now stopped) child: an unowned writer (an
   * old TUI that lost the lease race, a print-mode pi…). Spawning a child now
   * would make two writers.
   */
  async hasUnownedRecentWriter(id: string): Promise<boolean> {
    const o = await this.ownership(id);
    if (o.owner !== 'none') return false;
    const s = o.session;
    if (!s || s.running) return false;
    try {
      const mtime = (await fs.promises.stat(s.file)).mtimeMs;
      if (Date.now() - mtime >= UNOWNED_WRITE_WINDOW_MS) return false;
      // Writes up to our own transport's end, or a holder's clean release
      // (tombstone), were made by the single writer of the time.
      const ours = this.lastBridgeExit.get(path.resolve(s.file)) ?? 0;
      const tomb = await readLeaseRawAsync(s.file);
      const released = tomb.state === 'ok' ? (tomb.lease.releasedAt ?? 0) : 0;
      return mtime > Math.max(ours, released) + 2_000;
    } catch {
      return false;
    }
  }

  /**
   * Graceful takeover handshake (turn ?force=1): hello first (line-buffered),
   * then abort + release; wait for the lease to go away. Never force-kills.
   */
  async handshakeRelease(id: string, timeoutMs = 8000): Promise<boolean> {
    const s = await this.find(id);
    if (!s) return false;
    const lease = await this.getLiveLeaseInfo(id);
    if (!lease) return true;
    if (s.isProxy) s.stop('takeover');
    const helper = new Session(`${s.id}~release`, s.name, s.file, this.options.bin, s.workdir,
      this.options.sessionDir, [], this.options.log.child({ release: true }));
    try {
      await helper.attachOwner(lease);
      await helper.abort();
      await helper.request({ type: 'release' }, 3_000).catch(() => undefined);
    } catch (err) {
      this.options.log.warn({ sessionId: id, err: (err as Error).message }, 'release handshake failed');
    } finally {
      helper.stop('release done');
    }
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const insp = await inspectLeaseAsync(s.file);
      if (insp.state === 'absent' || insp.state === 'stale') return true;
      await sleep(250);
    }
    return false;
  }

  /* ---------------- lifecycle ---------------- */

  /** Re-key a started session under pi's canonical id (JSONL header id). */
  private canonicalRekey(session: Session): void {
    const piId = session.piId;
    const key = session.id;
    if (!piId || piId === key || this.sessions.get(key) !== session) return;
    this.sessions.delete(key);
    session.id = piId;
    this.sessions.set(piId, session);
    session.newEpoch();
    try {
      const placeholder = this.sessionFile(key);
      if (path.resolve(placeholder) !== path.resolve(session.file) && fs.existsSync(placeholder)
          && fs.statSync(placeholder).size === 0) {
        fs.unlinkSync(placeholder);
      }
    } catch { /* best effort */ }
    this.invalidateIndex();
    this.options.log.info({ from: key, to: piId }, 'session re-keyed to canonical pi id');
  }

  /** Bridge children only — owner proxies never consume a slot. */
  get runningCount(): number {
    let n = 0;
    for (const s of this.sessions.values()) if (s.hasChild) n++;
    return n;
  }

  /** Free a slot by stopping the stalest idle child (no viewers first). */
  private ensureAgentSlot(): boolean {
    if (this.runningCount < this.options.maxAgents) return true;
    const now = Date.now();
    const evictable = [...this.sessions.values()]
      .filter((x) => x.hasChild && !x.isBusy && !x.error && !x.queue.some(isActive)
        && now - x.lastActivityAt > 60_000)
      .sort((a, b) => (a.sinkCount() - b.sinkCount()) || (a.lastActivityAt - b.lastActivityAt));
    for (const victim of evictable) {
      if (this.runningCount < this.options.maxAgents) break;
      this.options.log.info({ sessionId: victim.id }, 'evicting idle agent to free a slot');
      victim.stop('evicted for capacity');
    }
    return this.runningCount < this.options.maxAgents;
  }

  /** Run `fn` once per idempotency key (10 min window); failures are not cached. */
  idempotent<T>(key: string | undefined, fn: () => Promise<T>): Promise<T> {
    if (!key) return fn();
    const prior = this.idempotency.get(key);
    if (prior && Date.now() - prior.at < CREATE_IDEMPOTENCY_MS) return prior.promise as Promise<T>;
    const promise = fn();
    this.idempotency.set(key, { at: Date.now(), promise });
    promise.catch(() => {
      if (this.idempotency.get(key)?.promise === promise) this.idempotency.delete(key);
    });
    return promise;
  }

  /**
   * Create a session. `clientRequestId` makes it idempotent: a repeat within
   * 10 min returns the same session (lost HTTP response retries).
   */
  create(name: string, clientRequestId?: string): Promise<Session> {
    return this.idempotent(clientRequestId ? `create:${clientRequestId}` : undefined, () => this.createUnlocked(name));
  }

  private async createUnlocked(name: string): Promise<Session> {
    if (!this.ensureAgentSlot()) throw new BusyError(this.options.maxAgents);
    const id = this.nextId();
    const session = new Session(
      id,
      name,
      this.sessionFile(id),
      this.options.bin,
      this.options.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log,
    );
    session.passNameOnce = name.trim().length > 0;
    this.register(session);
    this.invalidateIndex();
    try {
      await session.start();
      this.throwIfDead(session);
    } catch (err) {
      session.stop('create failed');
      this.sessions.delete(session.id);
      throw err;
    }
    this.canonicalRekey(session);
    return session;
  }

  private throwIfDead(session: Session): void {
    if (session.closed || !session.running) {
      throw new SpawnError(session.error ? `pi failed to start: ${session.error}` : 'pi exited before the session was ready');
    }
  }

  /** Bring a session's transport up (proxy to the TUI owner, or spawn a child). */
  async ensureRunning(id: string): Promise<Session | null> {
    const existing = this.startLocks.get(id);
    if (existing) return existing;
    const pending = this.ensureRunningUnlocked(id);
    this.startLocks.set(id, pending);
    try {
      return await pending;
    } finally {
      if (this.startLocks.get(id) === pending) this.startLocks.delete(id);
    }
  }

  private async ensureRunningUnlocked(id: string): Promise<Session | null> {
    const session = await this.find(id);
    if (!session) return null;
    for (let attempt = 0; attempt < 2; attempt++) {
      const insp = inspectLease(session.file);
      if (insp.state === 'unknown') throw new LeaseHeldError('unknown', null);
      const lease = insp.state === 'held' ? insp.lease : null;
      const oursForThisFile = lease !== null && leaseKind(lease) === 'bridge' && lease.bridgePid === process.pid;
      if (oursForThisFile && !session.hasChild && !this.anyChildFor(session.file)) {
        // Orphaned sidecar from one of OUR children that is gone — reclaimable.
        try { fs.unlinkSync(leasePath(session.file)); } catch { /* raced */ }
        continue;
      }
      if (lease && !oursForThisFile) {
        if (leaseKind(lease) === 'bridge') {
          session.externalOwner = 'bridge';
          throw new SessionLiveError('Session is driven by another Remote Pi bridge process');
        }
        // Host TUI is the single writer → proxy to it.
        session.externalOwner = 'tui';
        if (session.hasChild) await session.standDownChild();
        if (session.isProxy && session.ownerLease?.epoch === lease.epoch
            && session.ownerLease.holder.nonce === lease.holder.nonce) {
          return session;
        }
        await this.attachOwner(session, lease);
        return session;
      }
      session.externalOwner = 'none';
      if (session.isProxy) session.stop('owner released the lease');
      if (session.hasChild) return session;
      if (!this.ensureAgentSlot()) throw new BusyError(this.options.maxAgents);
      try {
        await session.start();
      } catch (err) {
        if (err instanceof LeaseHeldError && attempt === 0) continue; // a TUI claimed it meanwhile
        throw err;
      }
      this.throwIfDead(session);
      this.canonicalRekey(session);
      return session;
    }
    return session;
  }

  private anyChildFor(file: string): boolean {
    const key = path.resolve(file);
    for (const s of this.sessions.values()) if (s.hasChild && path.resolve(s.file) === key) return true;
    return false;
  }

  /** Attach as owner proxy; waits briefly for a just-written lease to publish its ipc. */
  private async attachOwner(session: Session, lease: Lease): Promise<void> {
    let current = lease;
    const deadline = Date.now() + 3_000;
    while (!current.ipc && Date.now() < deadline) {
      await sleep(250);
      const insp = inspectLease(session.file);
      if (insp.state !== 'held' || !insp.lease) break;
      current = insp.lease;
    }
    if (!current.ipc) {
      throw new SessionLiveError('The host TUI owns this session but its control channel is unavailable — restart that pi TUI to load the updated remote-pi-owner extension');
    }
    try {
      await session.attachOwner(current);
    } catch (err) {
      throw new SessionLiveError(`Host TUI owns this session but did not answer (${(err as Error).message})`);
    }
  }

  /** Our bridge lease was pre-empted by a host TUI: stand the child down and proxy instead. */
  private handleFenced(session: Session): void {
    void (async () => {
      await session.standDownChild();
      for (let i = 0; i < 10; i++) {
        const insp = inspectLease(session.file);
        if (insp.state === 'held' && insp.lease && leaseKind(insp.lease) === 'tui') {
          session.externalOwner = 'tui';
          if (insp.lease.ipc) {
            try {
              await session.attachOwner(insp.lease);
              this.options.log.info({ sessionId: session.id }, 'fenced child replaced by owner proxy');
              this.dispatchQueued(session);
              return;
            } catch (err) {
              this.options.log.debug({ sessionId: session.id, err: (err as Error).message }, 'proxy attach after fence failed');
            }
          }
        } else if (insp.state !== 'held' && insp.state !== 'unknown') {
          return; // owner already gone; next turn spawns/attaches lazily
        }
        await sleep(1_000);
      }
    })();
  }

  /** Session for an SSE attach: never spawns a child just to watch. */
  async sessionForSSE(id: string): Promise<Session | null> {
    const session = await this.find(id);
    if (!session) return null;
    if (session.running) return session;
    const insp = await inspectLeaseAsync(session.file);
    if (insp.state === 'held' && insp.lease && leaseKind(insp.lease) === 'tui') {
      session.externalOwner = 'tui';
      try {
        await this.ensureRunning(session.id); // cheap: proxy attach, no slot
      } catch (err) {
        this.options.log.debug({ sessionId: id, err: (err as Error).message }, 'proxy attach for SSE failed — tailing file');
      }
    } else {
      session.externalOwner = insp.state === 'held' && insp.lease ? leaseKind(insp.lease) : insp.state === 'unknown' ? 'tui' : 'none';
    }
    return session;
  }

  async delete(id: string, purge: boolean): Promise<boolean> {
    const session = await this.find(id);
    if (!session) return false;
    const file = session.file;
    const closed = session.waitClosed(5_000);
    session.stop('deleted');
    session.closeSinks();
    if (session.queueTimer) clearTimeout(session.queueTimer);
    this.sessions.delete(session.id);
    if (purge) {
      await closed; // never unlink under a still-writing child
      for (const f of [file, queueFilePath(this.options.sessionDir, file), `${file}.stats.json`]) {
        await fs.promises.unlink(f).catch(() => undefined);
      }
      const insp = await inspectLeaseAsync(file);
      if (insp.state === 'stale' || (insp.lease && leaseKind(insp.lease) === 'bridge' && insp.lease.bridgePid === process.pid)) {
        await fs.promises.unlink(leasePath(file)).catch(() => undefined);
      }
      this.files.delete(file);
    }
    this.invalidateIndex();
    return true;
  }

  /* ---------------- summaries (ONE builder) ---------------- */

  /** Lease ownership for a summary (only files that have a sidecar are read). */
  private async leaseFor(meta: SessionFileMeta | null, s: Session | undefined): Promise<LeaseInspection | null> {
    const file = meta?.file ?? s?.file;
    if (!file || (!meta?.hasLease && !s)) return null;
    return inspectLeaseAsync(file);
  }

  private summarize(meta: SessionFileMeta | null, s: Session | undefined, lease: LeaseInspection | null): SessionSummary {
    const now = Date.now();
    let leaseOwner: SessionOwner = 'none';
    let leasePid: number | null = null;
    if (lease?.state === 'unknown') leaseOwner = 'tui';
    else if (lease?.state === 'held' && lease.lease) {
      leaseOwner = leaseKind(lease.lease);
      leasePid = lease.lease.holder.pid;
    }
    const owner: SessionOwner = s?.running ? s.owner : leaseOwner;
    if (s && !s.running) s.externalOwner = leaseOwner;
    const mtime = meta?.lastActivityAt ?? 0;
    const lastActivityAt = Math.round(Math.max(mtime, s?.lastActivityAt ?? 0));
    const busy = s ? s.isBusy : false;
    return {
      id: s?.id ?? meta!.id,
      name: (s?.running ? s.name : meta?.name) ?? s?.name ?? 'pi session',
      running: owner !== 'none',
      busy,
      model: s?.model ?? null,
      messageCount: meta?.messageCount ?? s?.messageCount ?? 0,
      createdAt: meta?.createdAt ?? s?.createdAt ?? now,
      lastActivityAt,
      lastMessageAt: meta?.lastMessageAt ?? null,
      error: s?.error ?? null,
      phase: s?.phase ?? 'idle',
      owner,
      source: s?.source ?? 'pi',
      workdir: meta?.workdir ?? s?.workdir ?? null,
      active: busy || now - lastActivityAt < ACTIVE_WINDOW_MS,
      live: owner === 'tui',
      livePid: owner === 'tui' ? (s?.ownerLease?.holder.pid ?? leasePid) : null,
      writing: mtime > 0 && now - mtime < WRITING_WINDOW_MS,
      // CONTRACT §7: only sessions with an attached transport have activity.
      activity: s?.running ? s.activity() : null,
    };
  }

  private sortKey(meta: SessionFileMeta, s: Session | undefined): number {
    return meta.lastMessageAt ?? Math.max(meta.lastActivityAt, s?.lastActivityAt ?? 0);
  }

  /** Cursor-paginated session list: newest first, `before` = older than ts. */
  async listPage(limit: number, before?: number): Promise<{ sessions: SessionSummary[]; hasMore: boolean; total: number }> {
    const rows: { meta: SessionFileMeta; s: Session | undefined; key: number }[] = [];
    const seen = new Set<string>();
    for (const meta of (await this.getIndex()).values()) {
      const s = this.sessions.get(meta.id) ?? this.sessionByFile(meta.file);
      const id = s?.id ?? meta.id;
      if (seen.has(id)) continue;
      seen.add(id);
      rows.push({ meta, s, key: this.sortKey(meta, s) });
    }
    rows.sort((a, b) => b.key - a.key);
    const filtered = before ? rows.filter((r) => r.key < before) : rows;
    const page = filtered.slice(0, limit);
    const sessions = await Promise.all(page.map(async (r) => this.summarize(r.meta, r.s, await this.leaseFor(r.meta, r.s))));
    return { sessions, hasMore: filtered.length > limit, total: rows.length };
  }

  async list(): Promise<SessionSummary[]> {
    return (await this.listPage(MAX_LISTED)).sessions;
  }

  async summary(id: string): Promise<SessionSummary | null> {
    const loaded = this.sessions.get(id);
    const meta = loaded ? ((await this.getIndex()).get(loaded.id) ?? null) : await this.resolveMeta(id);
    const s = loaded ?? (meta ? this.sessionByFile(meta.file) : undefined);
    if (!s && !meta) return null;
    return this.summarize(meta, s, await this.leaseFor(meta, s));
  }

  async summaryOf(session: Session): Promise<SessionSummary> {
    const meta = (await this.getIndex()).get(session.id) ?? null;
    return this.summarize(meta, session, await this.leaseFor(meta, session));
  }

  /** /api/status `liveInstances`: host TUIs holding leases. */
  async getLiveInstances(): Promise<LiveInstance[]> {
    const metas = [...(await this.getIndex()).values()].filter((m) => m.hasLease);
    const out: LiveInstance[] = [];
    await Promise.all(metas.map(async (meta) => {
      const insp = await inspectLeaseAsync(meta.file);
      if (insp.state !== 'held' || !insp.lease || leaseKind(insp.lease) !== 'tui') return;
      out.push({
        pid: insp.lease.holder.pid,
        cwd: meta.workdir,
        startedAt: insp.lease.holder.startedAtMs || null,
        args: insp.lease.ipc,
        sessionId: meta.id,
      });
    }));
    return out;
  }

  stopAll(): void {
    for (const s of this.sessions.values()) {
      if (s.queueTimer) clearTimeout(s.queueTimer);
      s.stop('shutdown');
    }
  }

  /**
   * Graceful shutdown: end every SSE stream (so the HTTP server can close),
   * stop children and wait for them to exit — their close handlers release
   * the bridge leases — and drop owner-proxy pipes (never the TUIs).
   */
  async shutdown(timeoutMs = 5_000): Promise<void> {
    this.shuttingDown = true;
    const sessions = [...this.sessions.values()];
    for (const s of sessions) {
      if (s.queueTimer) clearTimeout(s.queueTimer);
      s.closeSinks();
    }
    const waits = sessions.map((s) => s.waitClosed(timeoutMs));
    for (const s of sessions) s.stop('shutdown');
    await Promise.all(waits);
  }

  private shuttingDown = false;

  sinkCounts(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [id, s] of this.sessions) if (s.sinkCount()) out[id] = s.sinkCount();
    return out;
  }

  /* ---------------- rename / models ---------------- */

  async rename(id: string, name: string): Promise<SessionSummary | null> {
    const session = await this.ensureRunning(id);
    if (!session) return null;
    session.name = name;
    await session.request({ type: 'set_session_name', name });
    this.invalidateIndex();
    return this.summaryOf(session);
  }

  async listModels(id: string): Promise<unknown> {
    const session = await this.ensureRunning(id);
    if (!session) throw new Error('Session not found');
    return session.request({ type: 'get_available_models' });
  }

  async setModel(id: string, modelId: string): Promise<void> {
    const session = await this.ensureRunning(id);
    if (!session) throw new Error('Session not found');
    await session.request({ type: 'set_model', modelId });
  }

  async cycleModel(id: string): Promise<string | null> {
    const session = await this.ensureRunning(id);
    if (!session) throw new Error('Session not found');
    const data = await session.request<{ modelId?: string }>({ type: 'cycle_model' });
    return data.modelId ?? null;
  }

  /* ---------------- durable prompt queue ---------------- */

  /**
   * Enqueue a prompt (idempotent by clientMessageId). Lone UTF-16 surrogates
   * are stripped (Node's encoder crashed on them); valid pairs — emoji —
   * are preserved.
   */
  enqueuePrompt(session: Session, rawMessage: string, clientMessageId?: string): QueueItem {
    const message = stripLoneSurrogates(rawMessage);
    if (clientMessageId) {
      const existing = session.queue.find((item) => item.clientMessageId === clientMessageId);
      if (existing) return existing;
    }
    if (session.queue.filter(isActive).length >= QUEUE_MAX_ACTIVE) throw new Error('Prompt queue is full');
    const item: QueueItem = {
      id: newQueueId(),
      ...(clientMessageId ? { clientMessageId } : {}),
      message,
      status: 'queued',
      queuedAt: Date.now(),
      startedAt: null,
      completedAt: null,
      error: null,
      attempts: 0,
    };
    session.queue.push(item);
    this.persistQueue(session);
    this.broadcastQueue(session);
    return item;
  }

  pendingItems(session: Session): PendingItemView[] {
    return pendingView(session.queue);
  }

  private broadcastQueue(session: Session): void {
    session.broadcast('queue_update', { items: pendingView(session.queue) });
  }

  /** Trigger queue delivery (after an enqueue). */
  dispatchQueuedNow(session: Session): void {
    this.dispatchQueued(session);
  }

  private scheduleDispatch(session: Session, delayMs: number): void {
    if (session.queueTimer) clearTimeout(session.queueTimer);
    session.queueTimer = setTimeout(() => {
      session.queueTimer = null;
      this.dispatchQueued(session);
    }, Math.max(50, delayMs));
    session.queueTimer.unref?.();
  }

  /** Dispatch the next ready queued prompt when the agent can take it. */
  private dispatchQueued(session: Session): void {
    if (session.dispatching || this.shuttingDown) return;
    if (session.queue.some((i) => i.status === 'running')) return; // one at a time
    const queued = session.queue.filter((i) => i.status === 'queued');
    if (!queued.length) return;
    const now = Date.now();
    const ready = queued.find((i) => (i.nextAttemptAt ?? 0) <= now);
    if (!ready) {
      this.scheduleDispatch(session, Math.min(...queued.map((i) => i.nextAttemptAt ?? now)) - now);
      return;
    }
    if (session.isBusy) return; // onIdle re-triggers
    if (session.running) {
      // Transport is up: mark running SYNCHRONOUSLY so the turn response can
      // report `dispatched: true` (no queued chip for an immediate send).
      if (!session.reservePrompt()) return;
      this.markRunning(session, ready);
      session.dispatching = true;
      void this.send(session, ready).finally(() => {
        session.dispatching = false;
      });
      return;
    }
    session.dispatching = true;
    void this.startAndDeliver(session, ready).finally(() => {
      session.dispatching = false;
    });
  }

  /** No transport yet: bring one up (proxy or child), then deliver. */
  private async startAndDeliver(session: Session, item: QueueItem): Promise<void> {
    let live: Session | null;
    try {
      live = await this.ensureRunning(session.id);
      if (!live) throw new Error('Session not found');
    } catch (err) {
      if (err instanceof BusyError) {
        this.scheduleDispatch(session, 5_000); // capacity is transient — no attempt consumed
        return;
      }
      item.attempts = (item.attempts ?? 0) + 1;
      this.failAttempt(session, item, err as Error);
      return;
    }
    if (item.status !== 'queued') return; // cancelled meanwhile
    if (!live.reservePrompt()) return; // became busy — onIdle retries
    this.markRunning(live, item);
    await this.send(live, item);
  }

  private markRunning(session: Session, item: QueueItem): void {
    item.status = 'running';
    item.startedAt = Date.now();
    item.attempts = (item.attempts ?? 0) + 1;
    item.error = null;
    delete item.nextAttemptAt;
    this.persistQueue(session);
    this.broadcastQueue(session);
    session.resetCrashBudget();
    session.expectEcho(item.message, item.clientMessageId, (ts) => {
      if (ts !== null) {
        item.messageTimestamp = ts;
        this.persistQueue(session);
      }
    });
  }

  private async send(session: Session, item: QueueItem): Promise<void> {
    this.options.log.info({ sessionId: session.id, itemId: item.id, attempt: item.attempts }, 'dispatching queued prompt');
    try {
      await session.request({ type: 'prompt', message: item.message }, 30_000);
      item.deliveredAt = Date.now();
      if (session.error?.startsWith('Queued message')) session.setError(null);
      // The run may already have ended before the acceptance reply arrived
      // (owner extensions emit events synchronously): settle it now.
      if (item.status === 'running' && !session.isBusy && session.lastIdleAt >= (item.startedAt ?? 0)) {
        item.status = 'done';
        item.completedAt = Date.now();
        this.persistQueue(session);
        this.broadcastQueue(session);
        setImmediate(() => this.dispatchQueued(session));
        return;
      }
      this.persistQueue(session);
    } catch (err) {
      session.releasePrompt();
      session.cancelEcho(item.message);
      this.failAttempt(session, item, err as Error);
    }
  }

  /** Backoff (1 s … 30 s); after QUEUE_MAX_ATTEMPTS the item fails visibly. */
  private failAttempt(session: Session, item: QueueItem, err: Error): void {
    const attempts = item.attempts ?? 1;
    const msg = err.message || String(err);
    if (attempts >= QUEUE_MAX_ATTEMPTS) {
      item.status = 'failed';
      item.error = msg;
      item.completedAt = Date.now();
      this.options.log.error({ sessionId: session.id, itemId: item.id, err: msg }, 'queued prompt failed permanently');
      session.setError(`Queued message failed after ${attempts} attempts: ${msg}`);
    } else {
      item.status = 'queued';
      item.startedAt = null;
      item.error = msg;
      const delay = backoffMs(attempts);
      item.nextAttemptAt = Date.now() + delay;
      this.options.log.warn({ sessionId: session.id, itemId: item.id, attempts, delay, err: msg }, 'queue dispatch failed, backing off');
      this.scheduleDispatch(session, delay);
    }
    this.persistQueue(session);
    this.broadcastQueue(session);
  }

  private static readonly STUCK_RUNNING_MS = 10 * 60_000;

  /** Watchdog for `running` items whose completion signal never arrived. */
  recoverStuckRunning(session: Session): void {
    const now = Date.now();
    let changed = false;
    for (const item of session.queue) {
      if (item.status !== 'running') continue;
      const age = now - (item.startedAt ?? now);
      if (item.deliveredAt) {
        if (!session.isBusy && (now - item.deliveredAt > 180_000 || age > SessionManager.STUCK_RUNNING_MS)) {
          item.status = 'done';
          item.completedAt = now;
          changed = true;
        }
      } else if (age > 120_000 && !session.dispatching) {
        item.status = 'queued'; // the prompt RPC died without settling
        item.startedAt = null;
        changed = true;
      }
    }
    if (changed) {
      if (!session.isBusy) session.releasePrompt();
      this.persistQueue(session);
      this.broadcastQueue(session);
      this.dispatchQueued(session);
    }
  }

  private completeRunning(session: Session): void {
    let changed = false;
    for (const item of session.queue) {
      if (item.status === 'running' && item.deliveredAt) {
        item.status = 'done';
        item.completedAt = Date.now();
        changed = true;
      }
    }
    if (changed) {
      this.persistQueue(session);
      this.broadcastQueue(session);
    }
  }

  /** Wire queue hooks and restore the persisted queue (exactly once per Session). */
  private wireQueue(session: Session): void {
    if (session.queueWired) return;
    session.queueWired = true;
    this.restoreQueue(session);
    session.onIdle = () => {
      this.completeRunning(session);
      this.dispatchQueued(session);
    };
    session.onExit = () => {
      this.lastBridgeExit.set(path.resolve(session.file), Date.now());
      let changed = false;
      for (const item of session.queue) {
        if (item.status !== 'running') continue;
        if (item.deliveredAt) {
          item.status = 'done'; // accepted by the agent — never re-send
          item.completedAt = Date.now();
        } else {
          item.status = 'queued';
          item.startedAt = null;
        }
        changed = true;
      }
      if (changed) {
        this.persistQueue(session);
        this.broadcastQueue(session);
      }
      if (session.queue.some((i) => i.status === 'queued')) this.scheduleDispatch(session, 2_000);
    };
    if (session.queue.some((i) => i.status === 'queued')) this.scheduleDispatch(session, 500);
  }

  private persistQueue(session: Session): void {
    session.queue = pruneQueue(session.queue);
    const file = queueFilePath(this.options.sessionDir, session.file);
    try {
      if (session.queue.length === 0) {
        try { fs.unlinkSync(file); } catch { /* none */ }
        return;
      }
      fs.mkdirSync(path.dirname(file), { recursive: true });
      writeAtomic(file, JSON.stringify(session.queue));
    } catch (err) {
      this.options.log.warn({ err: (err as Error).message }, 'queue persist failed');
    }
  }

  private restoreQueue(session: Session): void {
    const file = queueFilePath(this.options.sessionDir, session.file);
    let raw: unknown;
    try {
      raw = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      return; // absent or corrupt
    }
    const before = Array.isArray(raw) ? raw.length : 0;
    session.queue = restoreItems(raw);
    if (session.queue.length !== before
        || (raw as QueueItem[]).some((i) => i?.status === 'running')) {
      this.persistQueue(session);
    }
  }

  /** CONTRACT §2: pending (queued/running) items only. Null = unknown session. */
  async getQueueItems(id: string): Promise<PendingItemView[] | null> {
    const session = await this.find(id);
    return session ? pendingView(session.queue) : null;
  }

  /** Cancel a queued item. Returns false if unknown/not queued. */
  async cancelQueueItem(id: string, itemId: string): Promise<boolean> {
    const session = await this.find(id);
    if (!session) return false;
    const idx = session.queue.findIndex((i) => i.id === itemId && i.status === 'queued');
    if (idx < 0) return false;
    session.queue.splice(idx, 1);
    this.persistQueue(session);
    this.broadcastQueue(session);
    return true;
  }

  /* ---------------- history ---------------- */

  /**
   * Full history parsed from the session JSONL via the incremental cache
   * (only appended bytes are parsed after the first read). No temp pi
   * processes: an empty/unwritten file of a running child falls back to
   * get_messages on THAT child, otherwise it is simply empty.
   */
  async history(session: Session): Promise<ChatMessage[]> {
    let messages: ChatMessage[] = [];
    try {
      messages = await this.files.messages(session.file);
    } catch { /* not written yet */ }
    if (messages.length === 0 && session.hasChild) {
      try {
        const data = await session.request<{ messages?: AgentMessage[] }>({ type: 'get_messages' }, 15_000);
        messages = (data.messages ?? []).map(mapAgentMessage).filter((m): m is ChatMessage => m !== null);
      } catch { /* agent busy/booting — empty */ }
    }
    return this.decorateEchoes(session, messages);
  }

  /** CONTRACT §3: attach clientMessageId to user rows mapped to delivered queue items. */
  private decorateEchoes(session: Session, messages: ChatMessage[]): ChatMessage[] {
    const byTs = new Map<number, string>();
    for (const item of session.queue) {
      if (item.clientMessageId && typeof item.messageTimestamp === 'number') byTs.set(item.messageTimestamp, item.clientMessageId);
    }
    if (!byTs.size) return messages;
    return messages.map((m) => {
      const cmid = m.role === 'user' && m.timestamp !== null ? byTs.get(m.timestamp) : undefined;
      return cmid ? { ...m, clientMessageId: cmid } : m;
    });
  }

  /**
   * Working flag for /messages: lease/transport state when attached; for
   * unowned files a display heuristic (fresh file whose last row is a user
   * prompt).
   */
  async working(s: Session, messages?: ChatMessage[]): Promise<boolean> {
    if (s.running) return s.isBusy;
    try {
      const stat = await fs.promises.stat(s.file);
      if (Date.now() - stat.mtimeMs > 120_000) return false;
      const rows = messages ?? await this.history(s);
      const last = rows[rows.length - 1];
      return !!last && last.role === 'user';
    } catch {
      return false;
    }
  }

  /** Entry id of the last user message (default fork point). */
  async lastUserEntryId(id: string): Promise<string | null> {
    const s = await this.find(id);
    if (!s) return null;
    const rows = await this.history(s);
    for (let i = rows.length - 1; i >= 0; i--) {
      const m = rows[i]!;
      if (m.role === 'user' && !m.system && m.id) return m.id;
    }
    return null;
  }

  /* ---------------- fork ---------------- */

  /**
   * Fork at a user entry. pi 0.85 RPC `fork` switches the PROCESS to a brand
   * new session file. So: spawn a dedicated child on the source file (it only
   * reads it and never takes the source lease), fork there, read get_state
   * for the new sessionFile/sessionId, and keep that child as the NEW session
   * keyed by its real pi id. The source session (and its owner/child/viewers)
   * is never touched.
   */
  fork(id: string, entryId: string, name?: string, clientRequestId?: string): Promise<{ session: SessionSummary; text: string | null } | null> {
    return this.idempotent(clientRequestId ? `fork:${clientRequestId}` : undefined, () => this.forkUnlocked(id, entryId, name));
  }

  private async forkUnlocked(id: string, entryId: string, name?: string): Promise<{ session: SessionSummary; text: string | null } | null> {
    const src = await this.find(id);
    if (!src) return null;
    if (src.isBusy) throw new SpawnError('Agent is busy — fork when the current turn finishes');
    if (!this.ensureAgentSlot()) throw new BusyError(this.options.maxAgents);
    const child = new Session(
      `${this.nextId()}`,
      name || `${src.name} (fork)`,
      src.file,
      this.options.bin,
      src.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log.child({ fork: true }),
      'app',
    );
    try {
      await child.start({ noLease: true, passName: false });
      this.throwIfDead(child);
      const data = await child.request<{ text?: string; cancelled?: boolean }>({ type: 'fork', entryId }, 30_000);
      if (data.cancelled) throw new SpawnError('Fork cancelled by an extension');
      const st = await child.request<AgentState>({ type: 'get_state' }, 15_000);
      if (!st.sessionFile || !st.sessionId || path.resolve(st.sessionFile) === path.resolve(src.file)) {
        throw new SpawnError('pi did not switch to a new session file after fork');
      }
      const { sessionFile: _ignored, ...rest } = st;
      child.applyState(rest);
      child.adoptIdentity(st.sessionId, st.sessionFile, name || `${src.name} (fork)`);
      if (name) await child.request({ type: 'set_session_name', name }).catch(() => undefined);
      this.register(child);
      this.invalidateIndex();
      return { session: await this.summaryOf(child), text: data.text ?? null };
    } catch (err) {
      child.stop('fork failed');
      throw err;
    }
  }

  /* ---------------- maintenance ---------------- */

  private sweep(): void {
    const now = Date.now();
    for (const [key, s] of [...this.sessions]) {
      const hasActive = s.queue.some(isActive);
      // H1: never idle-stop proxies or sessions somebody is watching.
      if (s.hasChild && !s.isBusy && !hasActive && s.sinkCount() === 0
          && now - s.lastActivityAt > this.options.idleKillMs) {
        this.options.log.info({ sessionId: s.id }, 'idle timeout, stopping agent');
        s.stop('idle');
      }
      if (s.queue.some((i) => i.status === 'running')) this.recoverStuckRunning(s);
      if (s.queue.some((i) => i.status === 'queued') && !s.dispatching && !s.queueTimer) this.dispatchQueued(s);
      // Unload dormant registry entries (memory), keeping anything with state.
      if (!s.running && !hasActive && s.sinkCount() === 0 && !s.restarting && !s.queueTimer
          && !s.dispatching && now - s.lastActivityAt > UNLOAD_AFTER_MS) {
        this.sessions.delete(key);
      }
    }
    for (const [k, v] of this.idempotency) if (now - v.at > CREATE_IDEMPOTENCY_MS) this.idempotency.delete(k);
    for (const [k, t] of this.lastBridgeExit) if (now - t > 10 * 60_000) this.lastBridgeExit.delete(k);
    this.wakeDiskQueues();
  }

  /** Post-restart: sessions not loaded yet may still have durable queued prompts. */
  private wakeDiskQueues(): void {
    const queueDir = path.join(this.options.sessionDir, '.queue');
    let names: string[];
    try {
      names = fs.readdirSync(queueDir).filter((f) => f.endsWith('.json'));
    } catch {
      return;
    }
    if (!names.length) return;
    const wanted = new Set(names.map((f) => path.join(queueDir, f)));
    for (const meta of this.peekIndex().values()) {
      const qf = queueFilePath(this.options.sessionDir, meta.file);
      if (!wanted.has(qf) || this.sessions.has(meta.id)) continue;
      try {
        const items = restoreItems(JSON.parse(fs.readFileSync(qf, 'utf8')));
        if (items.some((i) => i.status === 'queued')) this.sessionFor(meta); // wireQueue schedules dispatch
      } catch { /* corrupt queue file */ }
    }
  }
}

/** Re-export for routes that attach SSE sinks. */
export type { EventSink };
