/**
 * Incremental, async session-file cache.
 *
 * Session JSONL files are append-only in practice but can be huge (90 MB+).
 * Re-reading and re-parsing a whole file synchronously on every change
 * stalled the event loop (slow opens, keepalive gaps, tunnel 502s). This
 * cache keeps, per file, the byte offset consumed so far plus the partial
 * trailing line, and on growth parses ONLY the appended bytes. A shrink or a
 * rewritten head triggers a full re-parse, done asynchronously in chunks.
 *
 * Two tiers per file:
 *  - summary (always kept, tiny): header id/cwd/created, name, first user
 *    text, message entry count, last message time;
 *  - messages (optional, budgeted by approximate bytes, LRU-evicted).
 */
import fs from 'node:fs';
import path from 'node:path';
import { LineSplitter, contentText, entryTimestamp, entryToChatMessage, parseEntry, type SessionEntry } from './jsonl.js';
import { FileKeyState, isMessageEntry, type KeySnapshot } from './keys.js';
import type { ChatMessage } from './types.js';

export interface FileSummary {
  headerId: string | null;
  cwd: string | null;
  createdAt: number | null;
  sessionName: string | null;
  firstUserText: string | null;
  messageCount: number;
  lastMessageAt: number | null;
  size: number;
  mtimeMs: number;
  /** False while an async (re)parse is still pending for this file. */
  complete: boolean;
}

interface FileState extends FileSummary {
  file: string;
  /** Bytes read so far (the splitter holds any trailing partial line). */
  consumed: number;
  splitter: LineSplitter;
  head: Buffer;
  messages: ChatMessage[] | null;
  msgBytes: number;
  /** CONTRACT-V2 §13 key state (always maintained, even without the messages tier). */
  keys: FileKeyState;
}

export interface FileCacheOptions {
  maxMessageBytes?: number;
  maxFiles?: number;
  /** Reads up to this many new bytes synchronously; larger work goes async. */
  syncLimitBytes?: number;
  /** Called after an async update changed a file's summary. */
  onAsyncUpdate?: (file: string) => void;
}

const HEAD_BYTES = 256;
const CHUNK = 1024 * 1024;

function emptyState(file: string): FileState {
  return {
    file,
    headerId: null,
    cwd: null,
    createdAt: null,
    sessionName: null,
    firstUserText: null,
    messageCount: 0,
    lastMessageAt: null,
    size: 0,
    mtimeMs: 0,
    complete: false,
    consumed: 0,
    splitter: new LineSplitter(),
    head: Buffer.alloc(0),
    messages: null,
    msgBytes: 0,
    keys: new FileKeyState(),
  };
}

/** Fast-path: pi writes `{"type":"<kind>",...` — skip JSON.parse for kinds we ignore. */
function interesting(line: string): boolean {
  if (!line.startsWith('{"type":"')) return true; // unknown layout — parse it
  const rest = line.slice(9, 30);
  // message, session, session_info, session_renamed
  return rest.startsWith('message"') || rest.startsWith('session');
}

export class SessionFileCache {
  private readonly states = new Map<string, FileState>();
  private readonly inflight = new Map<string, Promise<FileState | null>>();
  private messageBytes = 0;
  private readonly maxMessageBytes: number;
  private readonly maxFiles: number;
  private readonly syncLimitBytes: number;
  private readonly onAsyncUpdate: ((file: string) => void) | undefined;

  constructor(opts: FileCacheOptions = {}) {
    this.maxMessageBytes = opts.maxMessageBytes ?? 100 * 1024 * 1024;
    this.maxFiles = opts.maxFiles ?? 5_000;
    this.syncLimitBytes = opts.syncLimitBytes ?? 2 * 1024 * 1024;
    this.onAsyncUpdate = opts.onAsyncUpdate;
  }

  /** Approximate bytes held in parsed message lists (for telemetry/tests). */
  get cachedMessageBytes(): number {
    return this.messageBytes;
  }

  private key(file: string): string {
    return path.resolve(file);
  }

  private touch(key: string, st: FileState): void {
    this.states.delete(key);
    this.states.set(key, st);
    while (this.states.size > this.maxFiles) {
      const oldest = this.states.keys().next().value as string;
      this.drop(oldest);
    }
  }

  private drop(key: string): void {
    const st = this.states.get(key);
    if (st?.messages) this.messageBytes -= st.msgBytes;
    this.states.delete(key);
  }

  delete(file: string): void {
    this.drop(this.key(file));
  }

  private enforceBudget(protect: string): void {
    if (this.messageBytes <= this.maxMessageBytes) return;
    for (const [k, st] of this.states) {
      if (this.messageBytes <= this.maxMessageBytes) break;
      if (k === protect || !st.messages) continue;
      this.messageBytes -= st.msgBytes;
      st.messages = null; // keep the summary; messages re-parse on demand
      st.msgBytes = 0;
    }
  }

  private applyLine(st: FileState, line: string, end: number): void {
    if (!interesting(line)) return;
    const entry = parseEntry(line);
    if (!entry) return;
    this.applyEntry(st, entry, line.length, end);
  }

  private applyEntry(st: FileState, entry: SessionEntry, lineBytes: number, end = 0): void {
    switch (entry.type) {
      case 'session':
        if (st.headerId === null && typeof entry.id === 'string') st.headerId = entry.id;
        if (st.cwd === null && typeof entry.cwd === 'string') st.cwd = entry.cwd;
        if (st.createdAt === null) st.createdAt = entryTimestamp(entry);
        return;
      case 'session_info':
      case 'session_renamed': {
        const name = typeof entry.name === 'string' ? entry.name.trim() : '';
        st.sessionName = name || null;
        return;
      }
      case 'message': {
        st.messageCount++;
        const ts = entryTimestamp(entry);
        if (ts !== null) st.lastMessageAt = ts;
        if (st.firstUserText === null) {
          const m = (entry.message ?? entry) as { role?: string; content?: unknown };
          if (m.role === 'user') {
            const t = contentText(m.content).replace(/\s+/g, ' ').trim();
            if (t) st.firstUserText = t;
          }
        }
        // §13: every message row is counted in file order (same rule as the tailer/live path).
        const key = isMessageEntry(entry) ? st.keys.add(entry.message, end) : null;
        if (st.messages) {
          const mapped = entryToChatMessage(entry, key);
          if (mapped) {
            st.messages.push(mapped);
            st.msgBytes += lineBytes + 128;
            this.messageBytes += lineBytes + 128;
          }
        }
        return;
      }
      default:
    }
  }

  private applyChunk(st: FileState, chunk: Buffer): void {
    if (st.head.length < HEAD_BYTES && st.consumed < HEAD_BYTES) {
      st.head = Buffer.concat([st.head, chunk.subarray(0, HEAD_BYTES - st.head.length)]);
    }
    const base = st.consumed - st.splitter.pendingBytes;
    st.consumed += chunk.length;
    const lines = st.splitter.push(chunk);
    const ends = st.splitter.lastEnds;
    for (let i = 0; i < lines.length; i++) this.applyLine(st, lines[i]!, base + ends[i]!);
  }


  /** What has to happen to bring `st` up to date with `stat`. */
  private plan(st: FileState | undefined, stat: fs.Stats, wantMessages: boolean): 'none' | 'grow' | 'full' {
    if (!st) return 'full';
    if (wantMessages && st.messages === null) return 'full';
    if (stat.size < st.consumed) return 'full';
    if (stat.size === st.consumed && stat.mtimeMs === st.mtimeMs) return 'none';
    return 'grow';
  }

  /**
   * Summary for the session index (non-blocking). Brings the file up to date
   * when the work is bounded (≤ syncLimitBytes of new data, parsed async);
   * bigger work (a never-seen 90 MB file) runs in the background — the caller
   * gets the last known summary (or a quick head-only one) and
   * `onAsyncUpdate` fires when the full parse lands.
   */
  async summaryFast(file: string, stat: fs.Stats): Promise<FileSummary> {
    const key = this.key(file);
    const st = this.states.get(key);
    const plan = this.plan(st, stat, false);
    if (plan === 'none' && st) {
      this.touch(key, st);
      return st;
    }
    if (this.inflight.has(key)) return st ?? this.headOnly(file, stat);
    const bytes = plan === 'full' ? stat.size : stat.size - (st?.consumed ?? 0);
    if (bytes <= this.syncLimitBytes) {
      const fresh = await this.update(file, false);
      return fresh ?? st ?? this.headOnly(file, stat);
    }
    void this.update(file, false).then((s) => {
      if (s) this.onAsyncUpdate?.(file);
    });
    return st ?? this.headOnly(file, stat);
  }

  private async headOnly(file: string, stat: fs.Stats): Promise<FileSummary> {
    const st = emptyState(file);
    try {
      const fh = await fs.promises.open(file, 'r');
      try {
        const head = Buffer.alloc(Math.min(stat.size, 8192));
        const { bytesRead } = await fh.read(head, 0, head.length, 0);
        const splitter = new LineSplitter();
        for (const line of splitter.push(head.subarray(0, bytesRead))) {
          const e = parseEntry(line);
          if (e) this.applyEntry(st, e, 0);
        }
      } finally {
        await fh.close().catch(() => undefined);
      }
    } catch { /* unreadable */ }
    st.messageCount = 0;
    st.lastMessageAt = null;
    st.size = stat.size;
    st.mtimeMs = stat.mtimeMs;
    st.complete = false;
    return st;
  }


  /** Async update (deduplicated per file). Returns null when the file is gone. */
  private update(file: string, wantMessages: boolean): Promise<FileState | null> {
    const key = this.key(file);
    const running = this.inflight.get(key);
    if (running) {
      // A count-only parse in flight cannot satisfy a messages request.
      return running.then((st) => (st && (!wantMessages || st.messages) ? st : this.update(file, wantMessages)));
    }
    const p = this.updateAsync(key, file, wantMessages).finally(() => {
      if (this.inflight.get(key) === p) this.inflight.delete(key);
    });
    this.inflight.set(key, p);
    return p;
  }

  private async updateAsync(key: string, file: string, wantMessages: boolean): Promise<FileState | null> {
    let fh: fs.promises.FileHandle;
    try {
      fh = await fs.promises.open(file, 'r');
    } catch {
      this.drop(key);
      return null;
    }
    try {
      const stat = await fh.stat();
      const prev = this.states.get(key);
      let plan = this.plan(prev, stat, wantMessages);
      if (plan === 'none' && prev) return prev;
      if (plan === 'grow' && prev && prev.head.length) {
        const head = Buffer.alloc(prev.head.length);
        const { bytesRead } = await fh.read(head, 0, head.length, 0);
        if (!head.subarray(0, bytesRead).equals(prev.head)) plan = 'full';
      }
      // Full parses build a fresh state and swap it in at the end so readers
      // never observe a half-built message list.
      const st = plan === 'full' || !prev ? emptyState(file) : prev;
      if (st !== prev && (wantMessages || prev?.messages)) st.messages = [];
      let pos = st.consumed;
      const buf = Buffer.alloc(Math.min(CHUNK, Math.max(1, stat.size - pos)));
      while (pos < stat.size) {
        const want = Math.min(buf.length, stat.size - pos);
        const { bytesRead } = await fh.read(buf, 0, want, pos);
        if (!bytesRead) break;
        this.applyChunk(st, Buffer.from(buf.subarray(0, bytesRead)));
        pos += bytesRead;
      }
      st.size = stat.size;
      st.mtimeMs = stat.mtimeMs;
      st.complete = true;
      if (st !== prev) {
        if (prev?.messages) this.messageBytes -= prev.msgBytes;
        this.states.set(key, st);
      }
      this.touch(key, st);
      this.enforceBudget(key);
      return st;
    } finally {
      await fh.close().catch(() => undefined);
    }
  }

  /** Up-to-date summary (async; may parse the whole file the first time). */
  async summary(file: string): Promise<FileSummary | null> {
    return this.update(file, false);
  }

  /**
   * Parsed messages for a file, brought up to date incrementally. The array
   * is shared — callers must not mutate it (copy rows before decorating).
   */
  async messages(file: string): Promise<ChatMessage[]> {
    const st = await this.update(file, true);
    if (!st) throw Object.assign(new Error(`ENOENT: ${file}`), { code: 'ENOENT' });
    return st.messages ?? [];
  }

  /**
   * CONTRACT-V2 §13: key state of the file brought up to date — counts per
   * base key over every complete line, at the byte offset where the pending
   * partial line (if any) starts. Seeds live key tracking and the tailer.
   */
  async keySnapshot(file: string): Promise<KeySnapshot> {
    const st = await this.update(file, false);
    if (!st) return new FileKeyState().snapshot(0);
    return st.keys.snapshot(st.consumed - st.splitter.pendingBytes);
  }
}
