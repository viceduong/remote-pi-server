import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import type { Logger } from 'pino';
import { Session, type EventSink } from './session.js';
import { mapAgentMessage } from './history.js';
import { manglePath } from './paths.js';
import type { AgentMessage, AgentState, ChatMessage, LiveInstance, SessionSummary } from './types.js';
import { probeLivePiInstances, type LivePiInstance } from './live.js';
import { queueFilePath, type QueueItem } from './queue.js';

export interface SessionManagerOptions {
  sessionDir: string;
  workdir: string;
  bin: string;
  maxAgents: number;
  idleKillMs: number;
  extraArgs: string[];
  log: Logger;
}

/** Busy-capacity reached (503). */
export class BusyError extends Error {
  readonly code = 503;
  constructor() {
    super('All agents busy');
  }
}

/** pi failed to start (409, carries pi's startup stderr). */
export class SpawnError extends Error {
  readonly code = 409;
  constructor(message: string) {
    super(message);
  }
}

/** Metadata extracted from a pi session file. */
interface SessionFileMeta {
  id: string;
  file: string;
  name: string;
  workdir: string | null;
  createdAt: number;
  lastActivityAt: number;
  messageCount: number;
  /** Timestamp of the actual last message entry (not file mtime). */
  lastMessageAt: number | null;
  /** True when the file was modified recently (live pi process on the host). */
  active: boolean;
  /** True when an external (non-bridge) pi process owns this session. */
  live: boolean;
  livePid: number | null;
  /** True when the session file was written in the last 30s (agent working now). */
  writing: boolean;
}

const META_READ_LIMIT = 4096;
const MESSAGE_SCAN_LIMIT = 2 * 1024 * 1024;
const ACTIVE_WINDOW_MS = 5 * 60_000;
/** A session file touched within this window means the agent is working NOW. */
const WRITING_WINDOW_MS = 30_000;
const INDEX_TTL_MS = 3_000;
const PROBE_TTL_MS = 5_000;
const MAX_LISTED = 250;

/**
 * Owns the session lifecycle and — importantly — discovers **all** pi sessions
 * on the host (not just ones created through this bridge). The session dir
 * defaults to pi's real storage (`~/.pi/agent/sessions`), so sessions created
 * in the CLI/TUI and sessions created from the app are one and the same pool.
 */
export class SessionManager {
  private readonly sessions = new Map<string, Session>();
  private readonly options: SessionManagerOptions;
  private indexCache: { at: number; entries: Map<string, SessionFileMeta> } | null = null;
  /** Read-only mirrors for host-owned sessions — kept OUT of `sessions` so
   *  they never pollute the session list/index metadata. */
  private readonly mirrors = new Map<string, Session>();
  private readonly fileCache = new Map<string, { mtimeMs: number; size: number; meta: SessionFileMeta }>();
  private liveCache: { at: number; instances: LiveInstance[]; mapped: Map<string, number> } = {
    at: 0,
    instances: [],
    mapped: new Map(),
  };

  constructor(options: SessionManagerOptions) {
    this.options = options;
    fs.mkdirSync(options.sessionDir, { recursive: true });
    setInterval(() => this.sweepIdle(), 60_000).unref();
    // Live pi-instance probe (external agents on the host).
    void this.refreshLive();
    setInterval(() => void this.refreshLive(), PROBE_TTL_MS).unref();
    // Watch the session dir for changes (new sessions / active updates).
    this.watchSessionDir();
  }

  /** Recursive fs.watch where supported (Windows/macOS); the 3s index TTL
   *  covers platforms without recursive watch. */
  private watchSessionDir(): void {
    try {
      const watcher = fs.watch(this.options.sessionDir, { recursive: true }, () => {
        this.invalidateIndex();
      });
      watcher.on('error', () => this.invalidateIndex());
      this.options.log.debug('session dir watcher active');
    } catch {
      this.options.log.debug('recursive fs.watch unavailable — using TTL only');
    }
  }

  /** Probe host processes; map external pi agents to sessions by cwd, then by
   *  recency. Never maps bridge-managed children (excluded by pid). */
  private async refreshLive(): Promise<void> {
    try {
      const instances = await probeLivePiInstances();
      const children = new Set<number>();
      for (const s of this.sessions.values()) {
        if (s.pid !== null) children.add(s.pid);
      }
      const external = instances.filter((i) => !children.has(i.pid));

      const mapped = new Map<string, number>();
      const used = new Set<string>();
      // Candidate external sessions, freshest first.
      const candidates = this.walkAllMetas()
        .filter((m) => !this.sessions.has(m.id) || !this.sessions.get(m.id)!.running)
        .sort((a, b) => b.lastActivityAt - a.lastActivityAt);

      // Pass 1: exact cwd match (POSIX: /proc/<pid>/cwd; Windows: P/Invoke).
      for (const inst of external) {
        if (!inst.cwd) continue;
        const hit = candidates.find((c) => c.workdir === inst.cwd && !used.has(c.id));
        if (hit) {
          mapped.set(hit.id, inst.pid);
          used.add(hit.id);
        }
      }
      // Pass 2: unmatched agents -> freshest session touched in the last 2 min.
      for (const inst of external) {
        if ([...mapped.values()].includes(inst.pid)) continue;
        const hit = candidates.find(
          (c) => !used.has(c.id) && Date.now() - c.lastActivityAt < 120_000,
        );
        if (hit) {
          mapped.set(hit.id, inst.pid);
          used.add(hit.id);
        }
      }
      this.liveCache = {
        at: Date.now(),
        instances: external.map((i) => ({
          pid: i.pid,
          cwd: i.cwd,
          startedAt: i.startedAt,
          args: i.args,
          sessionId: [...mapped.entries()].find(([, p]) => p === i.pid)?.[0] ?? null,
        })),
        mapped,
      };
    } catch (err) {
      this.options.log.warn({ err: (err as Error).message }, 'live probe failed');
    }
  }

  /**
   * Session for SSE attachment: host-owned sessions get a READ-ONLY mirror
   * (file watcher only — NO second pi process, so the agent context never
   * diverges). Bridge-owned sessions spawn normally. A turn later converts
   * the mirror into a real agent (explicit takeover).
   */
  async sessionForSSE(id: string): Promise<Session | null> {
    const existing = this.sessions.get(id);
    if (existing) {
      if (!existing.running && !existing.readOnly) {
        await existing.start();
      }
      return existing;
    }
    const oldMirror = this.mirrors.get(id);
    if (oldMirror) return oldMirror;
    const meta = this.buildIndex().get(id);
    if (!meta) return null;
    if (this.liveCache.mapped.has(id)) {
      const mirror = new Session(
        id,
        meta.name,
        meta.file,
        this.options.bin,
        meta.workdir ?? this.options.workdir,
        this.options.sessionDir,
        this.options.extraArgs,
        this.options.log.child({ mirror: true }),
        'pi',
        true, // readOnly
      );
      this.mirrors.set(id, mirror);
      await mirror.start();
      return mirror;
    }
    return this.ensureRunning(id);
  }

  /* ---------------- rename ---------------- */

  /** Rename a session: pi persists it via set_session_name (session_renamed
   *  entry) so the file — and every surface reading it — syncs. */
  async rename(id: string, name: string): Promise<SessionSummary | null> {
    const session = await this.ensureRunning(id);
    if (!session) return null;
    session.name = name;
    await session.request({ type: 'set_session_name', name });
    this.invalidateIndex();
    return session.toSummary();
  }

  /* ---------------- models ---------------- */

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

  /** Raw external pi instances (for /api/status). */
  getLiveInstances(): LiveInstance[] {
    return this.liveCache.instances;
  }

  /** True when an external (non-bridge) pi process currently owns this session. */
  isExternallyLive(id: string): boolean {
    return this.liveCache.mapped.has(id);
  }

  /** True when the host agent is actively writing to the session file (<10s). */
  isHostWriting(id: string): boolean {
    const meta = this.liveCache.mapped.has(id) ? this.buildIndex().get(id) : undefined;
    if (!meta) return false;
    try {
      return Date.now() - fs.statSync(meta.file).mtimeMs < 10_000;
    } catch {
      return false;
    }
  }

  private walkAllMetas(): SessionFileMeta[] {
    const entries = new Map<string, SessionFileMeta>();
    this.walkSessionDir(this.options.sessionDir, entries);
    for (const s of this.sessions.values()) entries.set(s.id, this.metaFromLive(s));
    return [...entries.values()];
  }

  /* ---------------- ids & paths ---------------- */

  private nextId(): string {
    return crypto.randomUUID().replace(/-/g, '').slice(0, 16);
  }

  /** App-created sessions live inside the mangled workdir dir — exactly where
   *  pi itself stores them, so the CLI sees them too. */
  sessionFile(id: string): string {
    return path.join(
      this.options.sessionDir,
      manglePath(this.options.workdir),
      `${id}.jsonl`,
    );
  }

  /* ---------------- discovery ---------------- */

  /** Recursively scan the session dir for pi session files (cached 10s). */
  private buildIndex(): Map<string, SessionFileMeta> {
    const now = Date.now();
    if (this.indexCache && now - this.indexCache.at < INDEX_TTL_MS) {
      return this.indexCache.entries;
    }
    const entries = new Map<string, SessionFileMeta>();
    this.walkSessionDir(this.options.sessionDir, entries);
    // Live (managed) sessions win over their disk metadata — EXCEPT
    // lastMessageAt, which must reflect the real last message, not the
    // open/spawn time (activity timestamps move on every SSE event).
    for (const s of this.sessions.values()) {
      const live = this.metaFromLive(s);
      const disk = entries.get(s.id);
      if (disk && disk.lastMessageAt !== null) {
        live.lastMessageAt = disk.lastMessageAt;
      }
      entries.set(s.id, live);
    }
    // External pi agents currently owning a session (from the last probe).
    for (const [id, pid] of this.liveCache.mapped) {
      const meta = entries.get(id);
      if (meta) {
        meta.live = true;
        meta.livePid = pid;
      }
    }
    this.indexCache = { at: now, entries };
    return entries;
  }

  private walkSessionDir(dir: string, out: Map<string, SessionFileMeta>): void {
    let children: fs.Dirent[];
    try {
      children = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const child of children) {
      const full = path.join(dir, child.name);
      if (child.isDirectory()) {
        this.walkSessionDir(full, out);
      } else if (child.isFile() && child.name.endsWith('.jsonl')) {
        // Reuse parsed meta for unchanged files (stat is ~free vs reading 2MB).
        let stat: fs.Stats | null = null;
        try {
          stat = fs.statSync(full);
        } catch {
          continue;
        }
        const cached = this.fileCache.get(full);
        if (cached && cached.mtimeMs === stat.mtimeMs && cached.size === stat.size) {
          out.set(cached.meta.id, cached.meta);
          continue;
        }
        const meta = this.readMeta(full, stat);
        if (meta) {
          this.fileCache.set(full, { mtimeMs: stat.mtimeMs, size: stat.size, meta });
          out.set(meta.id, meta);
        }
      }
    }
  }

  private readMeta(file: string, statArg?: fs.Stats): SessionFileMeta | null {
    const base = path.basename(file, '.jsonl');
    let head: Buffer;
    let stat: fs.Stats;
    try {
      const fd = fs.openSync(file, 'r');
      head = Buffer.alloc(META_READ_LIMIT);
      const n = fs.readSync(fd, head, 0, META_READ_LIMIT, 0);
      fs.closeSync(fd);
      head = head.subarray(0, n);
      stat = statArg ?? fs.statSync(file);
    } catch {
      return null;
    }

    let id = base;
    let cwd: string | null = null;
    let sessionName: string | null = null;
    let createdAt = Math.round(stat.mtimeMs);
    let messageCount = 0;
    let scanned = 0;
    let lastMessageAt: number | null = null;

    const text = head.toString('utf8');
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      try {
        const obj = JSON.parse(line) as Record<string, unknown>;
        if (obj.type === 'session') {
          if (typeof obj.id === 'string') id = obj.id;
          if (typeof obj.cwd === 'string') cwd = obj.cwd;
          if (typeof obj.timestamp === 'string') {
            const ts = Date.parse(obj.timestamp);
            if (!Number.isNaN(ts)) createdAt = ts;
          }
        } else if (obj.type === 'session_renamed' && typeof obj.name === 'string') {
          sessionName = obj.name;
        } else if (obj.type === 'session_info' && typeof obj.name === 'string') {
          sessionName = obj.name;
        }
      } catch { /* skip unparseable line */ }
    }
    // Count messages across the WHOLE file (2MB windows undercount big
    // sessions). Cached per-file, so the scan cost is amortized.
    {
      const fd = fs.openSync(file, 'r');
      const rest = Buffer.alloc(stat.size);
      const n = fs.readSync(fd, rest, 0, rest.length, 0);
      fs.closeSync(fd);
      const hay = rest.subarray(0, n).toString('utf8');
      let idx = hay.indexOf('"type":"message"');
      while (idx >= 0) {
        messageCount++;
        idx = hay.indexOf('"type":"message"', idx + 1);
        if (++scanned > 100_000) break;
      }
    }
    // Last message timestamp: parse the tail line-by-line (a regex can match
    // `"type":"message"` inside message CONTENT and report a stale time).
    {
      const fd = fs.openSync(file, 'r');
      const tailStart = Math.max(0, stat.size - 512 * 1024);
      const tail = Buffer.alloc(stat.size - tailStart);
      const n = fs.readSync(fd, tail, 0, tail.length, tailStart);
      fs.closeSync(fd);
      const tailText = tail.subarray(0, n).toString('utf8');
      for (const line of tailText.split('\n')) {
        if (!line.trim()) continue;
        try {
          const entry = JSON.parse(line) as { type?: string; timestamp?: number | string };
          if (entry.type === 'message' && entry.timestamp) {
            const ts = typeof entry.timestamp === 'number'
              ? entry.timestamp
              : Date.parse(entry.timestamp as string);
            if (!Number.isNaN(ts)) lastMessageAt = ts;
          }
        } catch { /* skip */ }
      }
    }

    const name = sessionName
      ?? this.fallbackName(text, cwd, stat)
      ?? `pi session · ${base.slice(0, 8)}`;

    return {
      id,
      file,
      name,
      workdir: cwd ?? null,
      createdAt,
      // fs.Stats.mtimeMs is fractional — the iOS app decodes Int strictly,
      // so round to integer epoch ms before serializing.
      lastActivityAt: Math.round(stat.mtimeMs),
      messageCount,
      lastMessageAt,
      active: Date.now() - stat.mtimeMs < ACTIVE_WINDOW_MS,
      live: false,
      livePid: null,
      writing: Date.now() - stat.mtimeMs < WRITING_WINDOW_MS,
    };
  }

  /** Name from the first user message, else "cwd — date". */
  private fallbackName(head: string, cwd: string | null, stat: fs.Stats): string | null {
    // pi 0.79: user content is a block array: content:[{"type":"text","text":"..."}]
    const blocks = /"role":"user","content":\[\{"type":"text","text":"([^"]{1,60})/.exec(head);
    if (blocks) return this.shorten(blocks[1]!);
    // Older flat shape: content:"..."
    const flat = /"role":"user","content":"([^"]{1,60})/.exec(head);
    if (flat) return this.shorten(flat[1]!);
    if (cwd) {
      const base = cwd.split(/[\\/]/).filter(Boolean).pop();
      const date = stat.mtime.toISOString().slice(0, 10);
      return base ? `${base} · ${date}` : date;
    }
    return null;
  }

  private shorten(text: string): string {
    const clean = text.replace(/\\n/g, ' ');
    return clean.length > 48 ? `${clean.slice(0, 48)}…` : clean;
  }

  private metaFromLive(s: Session): SessionFileMeta {
    return {
      id: s.id,
      file: s.file,
      name: s.name,
      workdir: s.workdir,
      createdAt: s.createdAt,
      lastActivityAt: s.lastActivityAt,
      messageCount: s.messageCount,
      lastMessageAt: s.lastActivityAt,
      active: s.busy,
      live: false,
      livePid: null,
      writing: s.busy,
    };
  }

  private invalidateIndex(): void {
    this.indexCache = null;
  }

  /* ---------------- lifecycle ---------------- */

  async create(name: string): Promise<Session> {
    if (this.runningCount >= this.options.maxAgents) {
      // No idle-eviction: a single user's attached sessions must never be
      // killed (mid-turn eviction was a bug source). Hard cap only.
      throw new BusyError();
    }
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
    this.sessions.set(id, session);
    this.invalidateIndex();
    this.wireQueue(session);
    await session.start();
    this.throwIfDead(session);
    return session;
  }

  /** Surface a failed pi boot (stderr text) like piface/pi-threads do. */
  private throwIfDead(session: Session): void {
    if (session.closed || session.error || !session.running) {
      throw new SpawnError(
        session.error ? `pi failed to start: ${session.error}` : 'pi exited before the session was ready',
      );
    }
  }

  /** Resume any session (managed or discovered) by id. */
  async ensureRunning(id: string): Promise<Session | null> {
    const live = this.sessions.get(id);
    if (live) {
      // Read-only mirror -> explicit takeover: convert in place so the SSE
      // subscribers (app) keep receiving agent events on the same Session.
      if (live.readOnly) {
        live.readOnly = false;
        this.options.log.info({ sessionId: id }, 'takeover: mirror -> real agent');
      }
      if (!live.running) await live.start();
      return live;
    }
    const mirror = this.mirrors.get(id);
    if (mirror) {
      // Takeover of a host-owned mirror (kept out of `sessions`): convert it
      // in place and move it into the managed map — SSE continuity preserved.
      mirror.readOnly = false;
      this.mirrors.delete(id);
      this.sessions.set(id, mirror);
      this.options.log.info({ sessionId: id }, 'takeover: mirror -> real agent');
      if (!mirror.running) await mirror.start();
      return mirror;
    }
    const meta = this.buildIndex().get(id);
    if (!meta) return null;
    if (this.runningCount >= this.options.maxAgents) {
      return null; // hard cap only — never evict an attached session
    }
    const session = this.sessionFromMeta(meta, 'pi');
    this.sessions.set(id, session);
    this.wireQueue(session);
    await session.start();
    this.throwIfDead(session);
    return session;
  }

  get(id: string): Session | null {
    return this.sessions.get(id) ?? null;
  }

  find(id: string): Session | null {
    if (this.sessions.has(id)) return this.sessions.get(id)!;
    const meta = this.buildIndex().get(id);
    if (!meta) return null;
    return this.sessionFromMeta(meta, 'pi');
  }

  private sessionFromMeta(meta: SessionFileMeta, source: 'app' | 'pi'): Session {
    const session = new Session(
      meta.id,
      meta.name,
      meta.file,
      this.options.bin,
      meta.workdir ?? this.options.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log,
      source,
    );
    session.messageCount = meta.messageCount;
    session.createdAt = meta.createdAt;
    session.lastActivityAt = meta.lastActivityAt;
    return session;
  }

  delete(id: string, purge: boolean): boolean {
    const session = this.sessions.get(id);
    let file = session?.file;
    if (session) {
      session.stop();
      this.sessions.delete(id);
    } else if (!file) {
      file = this.buildIndex().get(id)?.file;
    }
    if (purge && file) {
      try { fs.unlinkSync(file); } catch { /* already gone */ }
    }
    this.invalidateIndex();
    return session !== undefined || file !== undefined;
  }

  list(): SessionSummary[] {
    const index = this.buildIndex();
    const out: SessionSummary[] = [];
    for (const meta of index.values()) {
      const live = this.sessions.get(meta.id);
      if (live) {
        const sum = live.toSummary();
        if (meta.lastMessageAt !== null) sum.lastMessageAt = meta.lastMessageAt;
        out.push(sum);
      } else {
        out.push({
          id: meta.id,
          name: meta.name,
          running: false,
          busy: false,
          model: null,
          messageCount: meta.messageCount,
          createdAt: meta.createdAt,
          lastActivityAt: meta.lastActivityAt,
          lastMessageAt: meta.lastMessageAt,
          error: null,
          phase: meta.live ? 'streaming' : 'idle',
          owner: meta.live ? 'terminal' : 'none',
          source: 'pi',
          workdir: meta.workdir,
          active: meta.active,
          live: meta.live,
          livePid: meta.livePid,
          writing: meta.writing,
        });
      }
    }
    return this.allSorted(out).slice(0, MAX_LISTED);
  }

  /** Cursor-paginated session list: newest first, `before` = older than ts. */
  listPage(limit: number, before?: number): { sessions: SessionSummary[]; hasMore: boolean; total: number } {
    const index = this.buildIndex();
    const out: SessionSummary[] = [];
    for (const meta of index.values()) {
      const live = this.sessions.get(meta.id);
      if (live) {
        const sum = live.toSummary();
        if (meta.lastMessageAt !== null) sum.lastMessageAt = meta.lastMessageAt;
        out.push(sum);
      } else {
        out.push({
          id: meta.id,
          name: meta.name,
          running: false,
          busy: false,
          model: null,
          messageCount: meta.messageCount,
          createdAt: meta.createdAt,
          lastActivityAt: meta.lastActivityAt,
          lastMessageAt: meta.lastMessageAt,
          error: null,
          phase: meta.live ? 'streaming' : 'idle',
          owner: meta.live ? 'terminal' : 'none',
          source: 'pi',
          workdir: meta.workdir,
          active: meta.active,
          live: meta.live,
          livePid: meta.livePid,
          writing: meta.writing,
        });
      }
    }
    const sorted = this.allSorted(out);
    const filtered = before ? sorted.filter((s) => (s.lastMessageAt ?? s.lastActivityAt) < before) : sorted;
    const page = filtered.slice(0, limit);
    return { sessions: page, hasMore: filtered.length > limit, total: sorted.length };
  }

  private allSorted(list: SessionSummary[]): SessionSummary[] {
    return list.sort((a, b) => (b.lastMessageAt ?? b.lastActivityAt) - (a.lastMessageAt ?? a.lastActivityAt));
  }

  /** Summary for one session — live flag from the probe for external ones. */
  summary(id: string): SessionSummary | null {
    const live = this.sessions.get(id);
    if (live) {
      const out = live.toSummary();
      // Honor the real last-message time even for managed sessions
      // (toSummary uses activity time, which moves on open/spawn).
      const meta = this.buildIndex().get(id);
      if (meta && meta.lastMessageAt !== null) out.lastMessageAt = meta.lastMessageAt;
      return out;
    }
    const meta = this.buildIndex().get(id);
    if (!meta) return null;
    return {
      id: meta.id,
      name: meta.name,
      running: false,
      busy: false,
      model: null,
      messageCount: meta.messageCount,
      createdAt: meta.createdAt,
      lastActivityAt: meta.lastActivityAt,
      lastMessageAt: meta.lastMessageAt,
      error: null,
      phase: meta.live ? 'streaming' : 'idle',
      owner: meta.live ? 'terminal' : 'none',
      source: 'pi',
      workdir: meta.workdir,
      active: meta.active,
      live: meta.live,
      livePid: meta.livePid,
      writing: meta.writing,
    };
  }

  get runningCount(): number {
    let n = 0;
    for (const s of this.sessions.values()) if (s.running) n++;
    return n;
  }

  stopAll(): void {
    for (const s of this.sessions.values()) s.stop();
    for (const m of this.mirrors.values()) m.stop();
    this.mirrors.clear();
  }

  /**
   * Server-owned prompt queue. Queued messages are held here (never only
   * optimistic on the client), dispatched as a direct prompt the moment the
   * current turn ends, and surfaced as `pending` in /messages — so they can
   * never vanish on reload and never double-send.
   */
  enqueuePrompt(session: Session, message: string): QueueItem {
    const item: QueueItem = {
      id: crypto.randomUUID().replace(/-/g, '').slice(0, 16),
      message,
      status: 'queued',
      queuedAt: Date.now(),
      startedAt: null,
      completedAt: null,
      error: null,
    };
    session.queue.push(item);
    if (session.queue.filter((i) => i.status === 'queued' || i.status === 'running').length > 50) {
      // Bound the durable file: drop finished items older than a day.
      const cutoff = Date.now() - 86_400_000;
      session.queue = session.queue.filter(
        (i) => i.status === 'queued' || i.status === 'running' || (i.completedAt ?? 0) > cutoff,
      );
    }
    this.persistQueue(session);
    session.broadcast('queue_update', { items: session.queue });
    return item;
  }

  /** Dispatch the next queued prompt when the agent is ready. */
  private dispatchQueued(session: Session): void {
    const item = session.queue.shift();
    if (!item || session.busy || session.phase === 'streaming') return;
    item.status = 'running';
    item.startedAt = Date.now();
    this.persistQueue(session);
    session.broadcast('queue_update', { items: session.queue });
    this.options.log.info({ sessionId: session.id }, 'dispatching queued prompt');
    void session.request({ type: 'prompt', message: item.message }).catch((err) => {
      // Put it back if the dispatch failed — never lose a queued message —
      // and retry shortly (the agent may not be fully ready yet).
      this.options.log.warn({ sessionId: session.id, err: (err as Error).message }, 'queue dispatch failed, retrying');
      item.status = 'queued';
      item.startedAt = null;
      session.queue.unshift(item);
      this.persistQueue(session);
      session.broadcast('queue_update', { items: session.queue });
      setTimeout(() => this.dispatchQueued(session), 2000);
    });
  }

  /** Wire a session's queue to its turn_end events + restore persisted state. */
  private wireQueue(session: Session): void {
    this.restoreQueue(session);
    session.onIdle = () => this.dispatchQueued(session);
    if (session.queue.length) this.dispatchQueued(session);
  }

  private persistQueue(session: Session): void {
    try {
      const file = queueFilePath(this.options.sessionDir, session.file);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, JSON.stringify(session.queue));
    } catch (err) {
      this.options.log.warn({ err: (err as Error).message }, 'queue persist failed');
    }
  }

  private restoreQueue(session: Session): void {
    try {
      const file = queueFilePath(this.options.sessionDir, session.file);
      if (!fs.existsSync(file)) return;
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8')) as QueueItem[];
      if (Array.isArray(parsed)) {
        session.queue = parsed.filter((i) => i && typeof i.id === 'string' && typeof i.message === 'string');
      }
    } catch { /* corrupt -> ignore */ }
  }

  /** Queue contents for a session (API). File-first: works even before the
   *  session is loaded into memory (e.g. right after a server restart). */
  getQueueItems(id: string): QueueItem[] | null {
    const session = this.find(id);
    if (!session) return null;
    const file = queueFilePath(this.options.sessionDir, session.file);
    if (!fs.existsSync(file)) return session.queue;
    try {
      return JSON.parse(fs.readFileSync(file, 'utf8')) as QueueItem[];
    } catch {
      return [];
    }
  }

  /** Cancel a queued item. Returns false if unknown/not queued. */
  cancelQueueItem(id: string, itemId: string): boolean {
    const items = this.getQueueItems(id);
    if (!items) return false;
    const idx = items.findIndex((i) => i.id === itemId && i.status === 'queued');
    if (idx < 0) return false;
    items.splice(idx, 1);
    // Persist to disk, then sync the in-memory session queue if loaded.
    try {
      const session = this.find(id);
      const file = session ? queueFilePath(this.options.sessionDir, session.file) : null;
      if (file) {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, JSON.stringify(items));
      }
    } catch (err) {
      this.options.log.warn({ err: (err as Error).message }, 'queue persist failed');
    }
    const session = this.sessions.get(id);
    if (session) {
      const i2 = session.queue.findIndex((i) => i.id === itemId);
      if (i2 >= 0) session.queue.splice(i2, 1);
      session.broadcast('queue_update', { items: session.queue });
    }
    return true;
  }

  /* ---------------- history ---------------- */

  /**
   * Fork a session at a user message. pi's `fork` branches the *current*
   * session in place, so we clone first (new session + file), fork the clone,
   * then restore the source session on its original file. Source stays intact.
   */
  async fork(id: string, entryId: string, name?: string): Promise<{ session: SessionSummary; text: string | null } | null> {
    const src = await this.ensureRunning(id);
    if (!src) return null;
    if (src.busy || src.phase === 'streaming') {
      throw new SpawnError('Agent is busy — fork when the current turn finishes');
    }
    const srcFile = src.file;
    const srcName = src.name;

    // 1) pi `clone`: duplicate the current branch into a new session/file.
    await src.request({ type: 'clone' });
    const st = await src.request<AgentState>({ type: 'get_state' });
    const cloneFile = st.sessionFile && st.sessionFile !== srcFile
      ? st.sessionFile
      : srcFile;

    // 2) stop the process (it now serves the clone) and detach the source.
    src.stop();
    this.sessions.delete(src.id);

    // 3) boot the clone on its own file.
    const cloneId = this.nextId();
    const clone = new Session(
      cloneId,
      name || `${srcName} (fork)`,
      cloneFile,
      this.options.bin,
      src.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log.child({ fork: true }),
    );
    this.sessions.set(cloneId, clone);
    await clone.start();
    this.throwIfDead(clone);

    // 4) truncate the clone at the fork point.
    const data = await clone.request<{ text?: string; cancelled?: boolean }>({ type: 'fork', entryId });
    if (data.cancelled) throw new SpawnError('Fork cancelled by an extension');

    // 5) restore the source on its original file.
    const restored = new Session(
      src.id,
      srcName,
      srcFile,
      this.options.bin,
      src.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log,
    );
    this.sessions.set(src.id, restored);
    await restored.start();
    this.throwIfDead(restored);

    this.invalidateIndex();
    return { session: clone.toSummary(), text: data.text ?? null };
  }


  /**
   * Full history for a session. Fast path: parse the session JSONL directly
   * (ms-scale — pi's get_messages takes ~8s on large sessions because it
   * re-serializes every message). Falls back to RPC/temp-pi if the file is
   * unreadable.
   */
  /**
   * Server-derived working flag: our own process is mid-turn, or the file's
   * last entry is a user prompt awaiting a reply and the file changed within
   * the working window. Works for mirrors (TUI-owned sessions) where RPC
   * events never reach clients.
   */
  async working(s: Session): Promise<boolean> {
    if (s.busy || s.phase === 'streaming') return true;
    try {
      const stat = await fs.promises.stat(s.file);
      if (Date.now() - stat.mtimeMs > 120_000) return false;
      const messages = await this.history(s);
      const last = messages[messages.length - 1];
      return !!last && last.role === 'user';
    } catch {
      return false;
    }
  }

  /** Per-session SSE sink counts (zombie-connection forensics). */
  sinkCounts(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [id, s] of this.sessions) out[id] = s.sinkCount();
    for (const [id, s] of this.mirrors) out[id] = s.sinkCount();
    return out;
  }

  async history(session: Session): Promise<ChatMessage[]> {
    try {
      const messages = this.historyFromFile(session.file);
      if (messages.length > 0) return messages;
    } catch {
      /* fall through to RPC */
    }
    if (session.running) {
      const data = await session.request<{ messages?: AgentMessage[] }>({ type: 'get_messages' });
      return (data.messages ?? []).map(mapAgentMessage).filter((m): m is ChatMessage => m !== null);
    }
    const tmp = new Session(
      `${session.id}h`,
      session.name,
      session.file,
      this.options.bin,
      session.workdir,
      this.options.sessionDir,
      this.options.extraArgs,
      this.options.log.child({ temp: true }),
    );
    try {
      await tmp.start();
      await new Promise((r) => setTimeout(r, 800));
      if (!tmp.running) return [];
      const data = await tmp.request<{ messages?: AgentMessage[] }>({ type: 'get_messages' });
      return (data.messages ?? []).map(mapAgentMessage).filter((m): m is ChatMessage => m !== null);
    } finally {
      tmp.stop();
    }
  }

  /** Public wrapper for route-level id lookups (fork default entry). */
  historyFromFileSafe(id: string): ChatMessage[] {
    const meta = this.buildIndex().get(id);
    if (!meta) return [];
    try {
      return this.historyFromFile(meta.file);
    } catch {
      return [];
    }
  }

  private historyCache = new Map<string, { mtimeMs: number; size: number; messages: ChatMessage[] }>();
  private historyCacheBytes = 0;
  private static readonly HISTORY_CACHE_MAX_BYTES = 100 * 1024 * 1024;

  /** Parse the session JSONL tail directly (same entry shape pi serves).
   *  Cached by (mtime,size) so polling/pagination don't re-parse unchanged
   *  files (a 2MB file parse per request adds up). */
  private historyFromFile(file: string): ChatMessage[] {
    const stat = fs.statSync(file);
    const cached = this.historyCache.get(file);
    if (cached && cached.mtimeMs === stat.mtimeMs && cached.size === stat.size) {
      return cached.messages;
    }
    const messages = this.parseHistoryFile(file);
    // Byte-cap the cache, not just entry count: parsed arrays of big sessions
    // are MBs each, and 200 of them would balloon past the heap limit.
    let bytes = 0;
    for (const m of messages) bytes += (m.text?.length ?? 0) + 64;
    this.historyCacheBytes += bytes;
    this.historyCache.set(file, { mtimeMs: stat.mtimeMs, size: stat.size, messages });
    while (this.historyCache.size > 0 && this.historyCacheBytes > SessionManager.HISTORY_CACHE_MAX_BYTES) {
      const oldest = this.historyCache.keys().next().value as string;
      const old = this.historyCache.get(oldest);
      if (!old) break;
      this.historyCacheBytes -= old.messages.reduce((acc, m) => acc + (m.text?.length ?? 0) + 64, 0);
      this.historyCache.delete(oldest);
    }
    return messages;
  }

  private parseHistoryFile(file: string): ChatMessage[] {
    const stat = fs.statSync(file);
    const TAIL = 16 * 1024 * 1024;
    const start = Math.max(0, stat.size - TAIL);
    const fd = fs.openSync(file, 'r');
    const buf = Buffer.alloc(stat.size - start);
    try {
      fs.readSync(fd, buf, 0, buf.length, start);
    } finally {
      fs.closeSync(fd);
    }
    const out: ChatMessage[] = [];
    const lines = buf.toString('utf8').split('\n');
    for (const line of lines) {
      if (!line.trim()) continue;
      let entry: { type?: string; id?: string; message?: AgentMessage };
      try {
        entry = JSON.parse(line) as { type?: string; message?: AgentMessage };
      } catch {
        continue;
      }
      if (entry.type !== 'message' || !entry.message) continue;
      const msgWithId = { ...entry.message } as AgentMessage;
      if (entry.id) msgWithId.id = entry.id;
      const mapped = mapAgentMessage(msgWithId);
      if (mapped) out.push(mapped);
    }
    return out;
  }

  /* ---------------- maintenance ---------------- */

  private sweepIdle(): void {
    const now = Date.now();
    for (const s of this.sessions.values()) {
      if (s.running && !s.busy && s.phase !== 'streaming'
          && now - s.lastActivityAt > this.options.idleKillMs) {
        this.options.log.info({ sessionId: s.id }, 'idle timeout, stopping agent');
        s.stop();
      }
    }
  }
}

/** Re-export for routes that attach SSE sinks. */
export type { EventSink };
