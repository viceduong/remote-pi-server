/**
 * CONTRACT-V2 §14 — turn-end notifications (Bark).
 *
 *  - {@link TurnWatcher}: file-based detection over the whole pi sessions dir
 *    (recursive fs.watch + periodic rescan fallback). Per file it keeps the
 *    byte offset consumed and a byte-level {@link LineSplitter}, so only
 *    APPENDED bytes are parsed. Rows are keyed with the §13 {@link KeyCounter}
 *    (seeded lazily from the shared file cache / a key scan at the start
 *    offset, exactly like the tailer). An assistant row whose `stopReason` is
 *    not "toolUse" is a turn end. Files present when watching starts begin at
 *    EOF (pre-existing rows never notify); files that appear later are read
 *    from 0 but rows older than NEW_FILE_GRACE_MS before discovery are
 *    ignored (forks copy old rows). Per file, turn ends within 3 s collapse
 *    into the last one. Notified keys + offsets are persisted (dedupe across
 *    restarts and rewrites).
 *  - {@link NotifySettingsStore}: settings file (secrets inside, 0600) —
 *    {@link publicSettings} is the only view that leaves the process.
 *  - {@link NotifyService}: glues both to the Bark client.
 */
import fs from 'node:fs';
import path from 'node:path';
import { z } from 'zod';
import type { Logger } from 'pino';
import {
  BARK_ALGORITHMS, BARK_LEVELS, BARK_MODES, buildPushBody, cryptoProblem, sendBark,
  type BarkAlgorithm, type BarkClientOptions, type BarkCrypto, type BarkLevel, type BarkMessage, type BarkMode,
  type BarkRequestBody, type BarkResult,
} from './bark.js';
import { canonicalText } from './history.js';
import { LineSplitter, parseEntry } from './jsonl.js';
import { KeyCounter, countsAt, isMessageEntry, scanKeySnapshot, type KeySnapshot } from './keys.js';
import { writeAtomic } from './lease.js';
import { demangleDir } from './paths.js';
import type { AgentMessage } from './types.js';

/* ======================= settings ======================= */

export const DEFAULT_BARK_SERVER = 'https://api.day.app';

export interface NotifySettings {
  /** null = default (on once a device key is set). */
  enabled: boolean | null;
  barkServer: string;
  deviceKey: string | null;
  encryption: {
    /** null = default (on once a key is set). */
    enabled: boolean | null;
    algorithm: BarkAlgorithm;
    mode: BarkMode;
    key: string | null;
    iv: string | null;
  };
  level: BarkLevel;
}

/** GET /api/notify — secrets never included. */
export interface PublicNotifySettings {
  enabled: boolean;
  barkServer: string;
  hasDeviceKey: boolean;
  encryption: { enabled: boolean; algorithm: BarkAlgorithm; mode: BarkMode; hasKey: boolean };
  level: BarkLevel;
}

export function defaultSettings(): NotifySettings {
  return {
    enabled: null,
    barkServer: DEFAULT_BARK_SERVER,
    deviceKey: null,
    encryption: { enabled: null, algorithm: 'AES128', mode: 'CBC', key: null, iv: null },
    level: 'active',
  };
}

export const notifyEnabled = (s: NotifySettings): boolean => (s.enabled ?? true) && !!s.deviceKey;
export const encryptionEnabled = (s: NotifySettings): boolean => s.encryption.enabled ?? !!s.encryption.key;

export function publicSettings(s: NotifySettings): PublicNotifySettings {
  return {
    enabled: notifyEnabled(s),
    barkServer: s.barkServer,
    hasDeviceKey: !!s.deviceKey,
    encryption: {
      enabled: encryptionEnabled(s),
      algorithm: s.encryption.algorithm,
      mode: s.encryption.mode,
      hasKey: !!s.encryption.key,
    },
    level: s.level,
  };
}

/** "aes-256" / "AES256" / "aes_256" → "AES256". */
const algorithmSchema = z.preprocess(
  (v) => (typeof v === 'string' ? v.toUpperCase().replace(/[^A-Z0-9]/g, '') : v),
  z.enum(BARK_ALGORITHMS),
);
const modeSchema = z.preprocess((v) => (typeof v === 'string' ? v.trim().toUpperCase() : v), z.enum(BARK_MODES));
/** Empty string or null clears a secret. */
const secret = (max: number, re?: RegExp) => z.union([
  z.null(),
  z.literal(''),
  re ? z.string().max(max).regex(re) : z.string().max(max).regex(/^[\x20-\x7E]+$/, 'printable ASCII only'),
]);

export const NotifyPutSchema = z.object({
  enabled: z.boolean().optional(),
  barkServer: z.string().max(500).url().refine((u) => /^https?:\/\//i.test(u), 'http(s) URL required').optional(),
  deviceKey: secret(256, /^[A-Za-z0-9_-]+$/).optional(),
  encryption: z.object({
    enabled: z.boolean().optional(),
    algorithm: algorithmSchema.optional(),
    mode: modeSchema.optional(),
    key: secret(64).optional(),
    iv: secret(64).optional(),
  }).strict().optional(),
  level: z.enum(BARK_LEVELS).optional(),
}).strict();
export type NotifyPut = z.infer<typeof NotifyPutSchema>;

const StoredSchema = z.object({
  enabled: z.boolean().nullable().catch(null),
  barkServer: z.string().catch(DEFAULT_BARK_SERVER),
  deviceKey: z.string().nullable().catch(null),
  encryption: z.object({
    enabled: z.boolean().nullable().catch(null),
    algorithm: z.enum(BARK_ALGORITHMS).catch('AES128'),
    mode: z.enum(BARK_MODES).catch('CBC'),
    key: z.string().nullable().catch(null),
    iv: z.string().nullable().catch(null),
  }).catch(defaultSettings().encryption),
  level: z.enum(BARK_LEVELS).catch('active'),
});

export class NotifySettingsError extends Error {}

/** Merge a validated PUT into settings; throws NotifySettingsError on an inconsistent result. */
export function applyPut(cur: NotifySettings, put: NotifyPut): NotifySettings {
  const clear = (v: string | null | undefined, prev: string | null): string | null =>
    v === undefined ? prev : v === null || v === '' ? null : v;
  const next: NotifySettings = {
    enabled: put.enabled ?? cur.enabled,
    barkServer: put.barkServer ? put.barkServer.trim().replace(/\/+$/, '') : cur.barkServer,
    deviceKey: clear(put.deviceKey, cur.deviceKey),
    encryption: {
      enabled: put.encryption?.enabled ?? cur.encryption.enabled,
      algorithm: put.encryption?.algorithm ?? cur.encryption.algorithm,
      mode: put.encryption?.mode ?? cur.encryption.mode,
      key: clear(put.encryption?.key, cur.encryption.key),
      iv: clear(put.encryption?.iv, cur.encryption.iv),
    },
    level: put.level ?? cur.level,
  };
  const e = next.encryption;
  if (e.key || e.iv) {
    // A key that cannot work with the chosen algorithm/mode is rejected up front.
    const problem = e.key ? cryptoProblem(e.algorithm, e.mode, e.key, e.iv) : null;
    if (problem) throw new NotifySettingsError(problem);
  }
  return next;
}

export class NotifySettingsStore {
  private current: NotifySettings;

  constructor(readonly file: string, private readonly log?: Logger) {
    this.current = this.load();
  }

  private load(): NotifySettings {
    try {
      const raw = JSON.parse(fs.readFileSync(this.file, 'utf8')) as unknown;
      const parsed = StoredSchema.safeParse(raw);
      if (parsed.success) return parsed.data;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') this.log?.warn({ err: (err as Error).message }, 'notify settings unreadable — using defaults');
    }
    return defaultSettings();
  }

  get(): NotifySettings {
    return this.current;
  }

  update(put: NotifyPut): NotifySettings {
    const next = applyPut(this.current, put);
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    writeAtomic(this.file, JSON.stringify(next, null, 2));
    try { fs.chmodSync(this.file, 0o600); } catch { /* best effort (Windows) */ }
    this.current = next;
    return next;
  }
}

/* ======================= turn detection ======================= */

export type TurnKind = 'stop' | 'error' | 'aborted';

export interface TurnEnd {
  file: string;
  key: string;
  kind: TurnKind;
  stopReason: string | null;
  text: string;
  errorMessage: string | null;
  timestamp: number | null;
  /** Byte offset just after the row's line. */
  end: number;
}

/** Turn-end classification of a persisted message (null = not a turn end). */
export function turnEndOf(msg: AgentMessage): { kind: TurnKind; stopReason: string | null } | null {
  if (msg.role !== 'assistant') return null;
  const sr = typeof msg.stopReason === 'string' ? msg.stopReason : null;
  if (sr === 'toolUse') return null;
  if (sr === null) {
    // Legacy rows without stopReason: a tool-call message is mid-turn.
    const blocks = Array.isArray(msg.content) ? msg.content : [];
    if (blocks.some((b) => b && b.type === 'toolCall')) return null;
  }
  return { kind: sr === 'error' ? 'error' : sr === 'aborted' ? 'aborted' : 'stop', stopReason: sr };
}

interface PersistEntry { offset: number; keys: string[]; at: number }
interface PersistFile { v: 1; files: Record<string, PersistEntry> }

const KEYS_PER_FILE = 50;
const STATE_MAX_FILES = 5_000;
const STATE_MAX_AGE_MS = 30 * 24 * 3600_000;

/** Persisted dedupe: per session file → recently notified keys + offset. */
export class NotifyState {
  private readonly files = new Map<string, PersistEntry>();
  private timer: NodeJS.Timeout | null = null;

  constructor(readonly file: string | null, private readonly now: () => number = Date.now) {
    if (!file) return;
    try {
      const raw = JSON.parse(fs.readFileSync(file, 'utf8')) as PersistFile;
      for (const [k, v] of Object.entries(raw?.files ?? {})) {
        if (v && Array.isArray(v.keys)) {
          this.files.set(k, { offset: Number(v.offset) || 0, keys: v.keys.filter((x) => typeof x === 'string').slice(-KEYS_PER_FILE), at: Number(v.at) || 0 });
        }
      }
    } catch { /* missing / corrupt → empty */ }
  }

  private id(file: string): string {
    const r = path.resolve(file);
    return process.platform === 'win32' ? r.toLowerCase() : r;
  }

  has(file: string, key: string): boolean {
    return this.files.get(this.id(file))?.keys.includes(key) ?? false;
  }

  mark(file: string, keys: string[], offset: number): void {
    const id = this.id(file);
    const e = this.files.get(id) ?? { offset: 0, keys: [], at: 0 };
    for (const k of keys) if (!e.keys.includes(k)) e.keys.push(k);
    if (e.keys.length > KEYS_PER_FILE) e.keys.splice(0, e.keys.length - KEYS_PER_FILE);
    e.offset = Math.max(e.offset, offset);
    e.at = this.now();
    this.files.set(id, e);
    this.scheduleSave();
  }

  entry(file: string): PersistEntry | undefined {
    return this.files.get(this.id(file));
  }

  private scheduleSave(): void {
    if (!this.file || this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.saveNow();
    }, 500);
    this.timer.unref?.();
  }

  saveNow(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (!this.file) return;
    const cutoff = this.now() - STATE_MAX_AGE_MS;
    const rows = [...this.files.entries()].filter(([, v]) => v.at >= cutoff).sort((a, b) => b[1].at - a[1].at).slice(0, STATE_MAX_FILES);
    const out: PersistFile = { v: 1, files: Object.fromEntries(rows) };
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true });
      writeAtomic(this.file, JSON.stringify(out));
    } catch { /* next mark retries */ }
  }
}

interface Watched {
  file: string;
  consumed: number;
  splitter: LineSplitter;
  keys: KeyCounter | null;
  /** Rows with an older message timestamp are never notified (−∞ for append-only reads from EOF). */
  minTs: number;
  /** When this process started watching the file (rewrite guard). */
  watchStart: number;
  reading: Promise<void> | null;
  again: boolean;
  pollTimer: NodeJS.Timeout | null;
  lastActive: number;
  pending: { turn: TurnEnd; superseded: string[]; firstAt: number; timer: NodeJS.Timeout } | null;
}

export interface TurnWatcherOptions {
  sessionDir: string;
  onTurnEnd: (turn: TurnEnd) => void | Promise<void>;
  state: NotifyState;
  /** Shared key source (file cache) — default: standalone scan. */
  keySource?: (file: string) => Promise<KeySnapshot>;
  debounceMs?: number;
  maxDebounceMs?: number;
  pollDelayMs?: number;
  rescanMs?: number;
  newFileGraceMs?: number;
  /** Disable fs.watch (tests drive via poll / rescan). */
  noFsWatch?: boolean;
  log?: Logger;
  now?: () => number;
}

const READ_CHUNK = 1024 * 1024;
const MAX_DEPTH = 3;
const KEYS_IDLE_DROP_MS = 10 * 60_000;

export class TurnWatcher {
  private readonly files = new Map<string, Watched>();
  private watcher: fs.FSWatcher | null = null;
  private rescanTimer: NodeJS.Timeout | null = null;
  private scanning: Promise<void> | null = null;
  private started = false;
  private ready = false;
  private readonly now: () => number;

  constructor(private readonly opts: TurnWatcherOptions) {
    this.now = opts.now ?? Date.now;
  }

  /** Register every existing file at EOF, then watch. Resolves after the initial scan. */
  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.scan(true);
    this.ready = true;
    if (!this.opts.noFsWatch) {
      try {
        this.watcher = fs.watch(this.opts.sessionDir, { recursive: true }, (_ev, name) => {
          const n = name ? name.toString() : '';
          if (!n.endsWith('.jsonl') || n.split(/[\\/]/).some((seg) => seg.startsWith('.'))) return;
          this.touch(path.join(this.opts.sessionDir, n));
        });
        this.watcher.on('error', (err) => {
          this.opts.log?.warn({ err: err.message }, 'notify: dir watcher failed — rescan only');
          try { this.watcher?.close(); } catch { /* ignore */ }
          this.watcher = null;
        });
      } catch {
        this.opts.log?.debug('notify: recursive fs.watch unavailable — rescan only');
      }
    }
    const every = this.opts.rescanMs ?? 30_000;
    if (every > 0) {
      this.rescanTimer = setInterval(() => void this.rescan(), every);
      this.rescanTimer.unref?.();
    }
  }

  stop(): void {
    this.started = false;
    try { this.watcher?.close(); } catch { /* ignore */ }
    this.watcher = null;
    if (this.rescanTimer) clearInterval(this.rescanTimer);
    this.rescanTimer = null;
    for (const w of this.files.values()) {
      if (w.pollTimer) clearTimeout(w.pollTimer);
      if (w.pending) clearTimeout(w.pending.timer);
      w.pollTimer = null;
      w.pending = null;
    }
  }

  /** Walk the dir: new files are registered, grown/shrunk files polled. */
  rescan(): Promise<void> {
    if (!this.ready) return Promise.resolve();
    return this.scan(false);
  }

  private scan(initial: boolean): Promise<void> {
    if (this.scanning) return this.scanning;
    const p = this.walk(this.opts.sessionDir, 0, initial).finally(() => {
      if (this.scanning === p) this.scanning = null;
    });
    this.scanning = p;
    return p;
  }

  private async walk(dir: string, depth: number, initial: boolean): Promise<void> {
    if (depth > MAX_DEPTH) return;
    let children: fs.Dirent[];
    try {
      children = await fs.promises.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const c of children) {
      if (c.name.startsWith('.')) continue;
      const full = path.join(dir, c.name);
      if (c.isDirectory()) await this.walk(full, depth + 1, initial);
      else if (c.isFile() && c.name.endsWith('.jsonl')) {
        let size: number;
        try {
          size = (await fs.promises.stat(full)).size;
        } catch {
          continue;
        }
        const w = this.files.get(this.id(full));
        if (!w) this.register(full, initial ? size : 0, !initial);
        else {
          if (size !== w.consumed) void this.poll(full);
          else this.maybeDropKeys(w);
        }
      }
    }
  }

  private id(file: string): string {
    const r = path.resolve(file);
    return process.platform === 'win32' ? r.toLowerCase() : r;
  }

  private register(file: string, offset: number, isNew: boolean): Watched {
    const now = this.now();
    const w: Watched = {
      file,
      consumed: offset,
      splitter: new LineSplitter(),
      keys: offset === 0 ? new KeyCounter() : null,
      minTs: isNew ? now - (this.opts.newFileGraceMs ?? 10 * 60_000) : -Infinity,
      watchStart: now,
      reading: null,
      again: false,
      pollTimer: null,
      lastActive: now,
      pending: null,
    };
    this.files.set(this.id(file), w);
    if (isNew) void this.poll(file);
    return w;
  }

  private maybeDropKeys(w: Watched): void {
    if (w.keys && !w.pending && !w.reading && w.splitter.pendingBytes === 0 && this.now() - w.lastActive > KEYS_IDLE_DROP_MS) {
      w.keys = null; // re-seeded from the shared cache on the next append
    }
  }

  /** fs.watch hit: (register and) poll the file shortly. */
  touch(file: string): void {
    if (!this.ready || !this.started) return;
    let w = this.files.get(this.id(file));
    if (!w) {
      let exists = true;
      try { fs.statSync(file); } catch { exists = false; }
      if (!exists) return;
      w = this.register(file, 0, true);
      return;
    }
    if (w.pollTimer) return;
    w.pollTimer = setTimeout(() => {
      w.pollTimer = null;
      void this.poll(file);
    }, this.opts.pollDelayMs ?? 200);
    w.pollTimer.unref?.();
  }

  /** Read whatever was appended to `file` (serialized per file). */
  poll(file: string): Promise<void> {
    const w = this.files.get(this.id(file));
    if (!w) return Promise.resolve();
    if (w.reading) {
      w.again = true;
      return w.reading;
    }
    w.reading = this.readOnce(w)
      .catch((err) => this.opts.log?.debug({ err: (err as Error).message }, 'notify: read failed'))
      .finally(() => {
        w.reading = null;
        if (w.again) {
          w.again = false;
          void this.poll(file);
        }
      });
    return w.reading;
  }

  private async seedKeys(w: Watched): Promise<KeyCounter> {
    if (w.keys) return w.keys;
    const at = w.consumed - w.splitter.pendingBytes;
    let snap: KeySnapshot;
    try {
      snap = this.opts.keySource ? await this.opts.keySource(w.file) : await scanKeySnapshot(w.file, at);
      if (snap.offset < at) snap = await scanKeySnapshot(w.file, at);
    } catch {
      snap = await scanKeySnapshot(w.file, at);
    }
    w.keys ??= new KeyCounter(countsAt(snap, at));
    return w.keys;
  }

  private async readOnce(w: Watched): Promise<void> {
    let fh: fs.promises.FileHandle;
    try {
      fh = await fs.promises.open(w.file, 'r');
    } catch {
      return;
    }
    try {
      let size = (await fh.stat()).size;
      if (size < w.consumed) {
        // Rewritten / truncated: re-read it whole, but only rows stamped after
        // we started watching (and never already-notified keys) may notify.
        w.consumed = 0;
        w.splitter.reset();
        w.keys = new KeyCounter();
        w.minTs = Math.max(w.minTs, w.watchStart);
        size = (await fh.stat()).size;
      }
      if (size === w.consumed) return;
      const keys = await this.seedKeys(w);
      w.lastActive = this.now();
      let pos = w.consumed;
      const buf = Buffer.alloc(Math.min(READ_CHUNK, size - pos));
      while (pos < size) {
        const { bytesRead } = await fh.read(buf, 0, Math.min(buf.length, size - pos), pos);
        if (!bytesRead) break;
        const base = pos - w.splitter.pendingBytes;
        pos += bytesRead;
        const lines = w.splitter.push(buf.subarray(0, bytesRead));
        const ends = w.splitter.lastEnds;
        for (let i = 0; i < lines.length; i++) {
          const entry = parseEntry(lines[i]!);
          if (!isMessageEntry(entry)) continue;
          const key = keys.assign(entry.message);
          if (!key) continue;
          const t = turnEndOf(entry.message);
          if (!t) continue;
          const ts = typeof entry.message.timestamp === 'number' ? entry.message.timestamp : null;
          const fresh = ts !== null ? ts >= w.minTs : w.minTs === -Infinity;
          if (!fresh || this.opts.state.has(w.file, key)) continue;
          this.enqueue(w, {
            file: w.file,
            key,
            kind: t.kind,
            stopReason: t.stopReason,
            text: canonicalText(entry.message),
            errorMessage: typeof entry.message.errorMessage === 'string' ? entry.message.errorMessage : null,
            timestamp: ts,
            end: base + ends[i]!,
          });
        }
      }
      w.consumed = pos;
    } finally {
      await fh.close().catch(() => undefined);
    }
  }

  /** Per-session debounce: several turn ends within `debounceMs` → only the last is delivered. */
  private enqueue(w: Watched, turn: TurnEnd): void {
    const debounce = this.opts.debounceMs ?? 3_000;
    const maxWait = this.opts.maxDebounceMs ?? 10_000;
    const now = this.now();
    if (w.pending) {
      clearTimeout(w.pending.timer);
      w.pending.superseded.push(w.pending.turn.key);
      w.pending.turn = turn;
    } else {
      w.pending = { turn, superseded: [], firstAt: now, timer: null as unknown as NodeJS.Timeout };
    }
    const delay = Math.max(0, Math.min(debounce, w.pending.firstAt + maxWait - now));
    w.pending.timer = setTimeout(() => this.flush(w), delay);
    w.pending.timer.unref?.();
  }

  private flush(w: Watched): void {
    const p = w.pending;
    if (!p) return;
    w.pending = null;
    // Marked BEFORE delivery: a failed send is never retried by the watcher (no storms, no dupes).
    this.opts.state.mark(w.file, [...p.superseded, p.turn.key], p.turn.end);
    try {
      void Promise.resolve(this.opts.onTurnEnd(p.turn)).catch((err) =>
        this.opts.log?.warn({ err: (err as Error).message }, 'notify: delivery failed'));
    } catch (err) {
      this.opts.log?.warn({ err: (err as Error).message }, 'notify: delivery failed');
    }
  }

  /** Test/diagnostic: number of watched files. */
  get watchedCount(): number {
    return this.files.size;
  }
}

/* ======================= payload ======================= */

const BODY_MAX = 180;

function clip(s: string, max = BODY_MAX): string {
  const cps = Array.from(s);
  return cps.length <= max ? s : `${cps.slice(0, max - 1).join('')}…`;
}

/** Body per §14: first non-empty line (≤180), "Error: …", or "Stopped". */
export function notificationBody(turn: Pick<TurnEnd, 'kind' | 'text' | 'errorMessage'>): string {
  if (turn.kind === 'aborted') return 'Stopped';
  if (turn.kind === 'error') {
    const firstErr = (turn.errorMessage ?? '').split(/\r?\n/).map((l) => l.trim()).find(Boolean);
    return clip(`Error: ${firstErr ?? 'unknown error'}`);
  }
  const first = turn.text.split(/\r?\n/).map((l) => l.trim()).find(Boolean);
  return first ? clip(first) : 'Turn finished';
}

export interface SessionRef { id: string; name: string | null; workdir?: string | null }

/** Fallback session identity when the index does not know the file. */
export function fallbackSession(file: string): { id: string; name: string } {
  const id = path.basename(file, '.jsonl');
  const dir = path.basename(path.dirname(file));
  const demangled = demangleDir(dir);
  const folder = demangled ? demangled.split('/').filter(Boolean).pop() ?? dir : dir;
  return { id, name: folder || id };
}

export function buildMessage(turn: TurnEnd, session: SessionRef | null, level: BarkLevel): BarkMessage {
  const fb = fallbackSession(turn.file);
  const folder = session?.workdir ? session.workdir.split(/[\\/]/).filter(Boolean).pop() ?? null : null;
  const id = session?.id ?? fb.id;
  return {
    title: clip(session?.name || folder || fb.name, 120),
    body: notificationBody(turn),
    group: id,
    url: `remotepi://session/${encodeURIComponent(id)}`,
    level,
  };
}

/** The Bark body for settings (encrypted iff encryption is effectively on). Throws on misconfig. */
export function pushBodyFor(settings: NotifySettings, msg: BarkMessage): BarkRequestBody {
  if (!settings.deviceKey) throw new NotifySettingsError('device key not set');
  let enc: BarkCrypto | null = null;
  if (encryptionEnabled(settings)) {
    const e = settings.encryption;
    const problem = cryptoProblem(e.algorithm, e.mode, e.key, e.iv);
    if (problem) throw new NotifySettingsError(problem);
    enc = { algorithm: e.algorithm, mode: e.mode, key: e.key! };
  }
  return buildPushBody(settings.deviceKey, msg, enc);
}

/** Copy of a push body safe to print/log (device key + ciphertext redacted). */
export function redactBody(body: BarkRequestBody): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(body)) {
    if (k === 'device_key') out[k] = '<redacted>';
    else if (k === 'ciphertext') out[k] = `<${v.length} base64 chars>`;
    else if (k === 'iv') out[k] = `<${v.length} chars, random per push>`;
    else out[k] = v;
  }
  return out;
}

/* ======================= service ======================= */

export interface NotifyServiceOptions {
  dataDir: string;
  sessionDir: string;
  resolveSession: (file: string) => Promise<SessionRef | null>;
  keySource?: (file: string) => Promise<KeySnapshot>;
  log: Logger;
  bark?: BarkClientOptions;
  watcher?: Partial<Omit<TurnWatcherOptions, 'sessionDir' | 'onTurnEnd' | 'state' | 'keySource' | 'log'>>;
}

export class NotifyService {
  readonly settings: NotifySettingsStore;
  readonly state: NotifyState;
  readonly watcher: TurnWatcher;
  /** Last delivery (diagnostics/tests). */
  lastResult: (BarkResult & { key: string }) | null = null;
  private inflight: Promise<unknown> = Promise.resolve();

  constructor(private readonly opts: NotifyServiceOptions) {
    this.settings = new NotifySettingsStore(path.join(opts.dataDir, 'notify.json'), opts.log);
    this.state = new NotifyState(path.join(opts.dataDir, 'notify-state.json'));
    this.watcher = new TurnWatcher({
      ...opts.watcher,
      sessionDir: opts.sessionDir,
      state: this.state,
      ...(opts.keySource ? { keySource: opts.keySource } : {}),
      log: opts.log,
      onTurnEnd: (t) => this.deliver(t),
    });
  }

  start(): Promise<void> {
    return this.watcher.start();
  }

  stop(): void {
    this.watcher.stop();
    this.state.saveNow();
  }

  /** Settled when every delivery started so far finished (tests). */
  idle(): Promise<unknown> {
    return this.inflight;
  }

  async messageFor(turn: TurnEnd): Promise<BarkMessage> {
    const session = await this.opts.resolveSession(turn.file).catch(() => null);
    return buildMessage(turn, session, this.settings.get().level);
  }

  private deliver(turn: TurnEnd): Promise<void> {
    const run = (async () => {
      const s = this.settings.get();
      if (!notifyEnabled(s)) return;
      const msg = await this.messageFor(turn);
      let body: BarkRequestBody;
      try {
        body = pushBodyFor(s, msg);
      } catch (err) {
        this.opts.log.warn({ err: (err as Error).message }, 'notify: not sent (settings)');
        return;
      }
      const r = await sendBark(s.barkServer, body, this.opts.bark);
      this.lastResult = { ...r, key: turn.key };
      if (r.ok) this.opts.log.info({ session: msg.group, kind: turn.kind, status: r.status }, 'notify: sent');
      else this.opts.log.warn({ session: msg.group, kind: turn.kind, status: r.status, err: r.error }, 'notify: Bark push failed');
    })();
    this.inflight = Promise.all([this.inflight, run.catch(() => undefined)]);
    return run;
  }

  /** POST /api/notify/test — works even when `enabled` is false (needs a device key). */
  async test(): Promise<BarkResult> {
    const s = this.settings.get();
    let body: BarkRequestBody;
    try {
      body = pushBodyFor(s, {
        title: 'Remote Pi',
        body: 'Test notification — turn-end alerts are working.',
        group: 'remote-pi',
        url: 'remotepi://',
        level: s.level,
      });
    } catch (err) {
      return { ok: false, status: 0, error: (err as Error).message };
    }
    const r = await sendBark(s.barkServer, body, this.opts.bark);
    if (!r.ok) this.opts.log.warn({ status: r.status, err: r.error }, 'notify: test push failed');
    return r;
  }
}
