/**
 * Read-only live tailer for a session JSONL the bridge does not own (written
 * by an unowned host TUI, or idle with SSE viewers attached). Emits newly
 * appended `message` entries as ChatMessages.
 *
 * Byte-exact: reads raw bytes from `consumed` (= every byte read so far), the
 * LineSplitter keeps the partial trailing line as bytes, and only complete
 * lines are decoded — a chunk boundary inside a multibyte char or a partial
 * line can never duplicate or corrupt output. On shrink/rewrite it re-syncs
 * and suppresses everything it already emitted (dedupe by entry id).
 */
import fs from 'node:fs';
import path from 'node:path';
import { LineSplitter, entryToChatMessage, parseEntry } from './jsonl.js';
import { KeyCounter, countsAt, isMessageEntry, scanKeySnapshot, type KeySnapshot } from './keys.js';
import type { ChatMessage } from './types.js';

export interface TailBatch {
  messages: ChatMessage[];
  mtimeMs: number;
}

const SEEN_LIMIT = 5_000;
const READ_CHUNK = 1024 * 1024;

export class FileTailer {
  private consumed = 0;
  private readonly splitter = new LineSplitter();
  private readonly seen = new Set<string>();
  private readonly seenOrder: string[] = [];
  private lastId: string | null = null;
  private watcher: fs.FSWatcher | null = null;
  private timer: NodeJS.Timeout | null = null;
  private reading: Promise<void> | null = null;
  private again = false;
  private stopped = true;
  /** CONTRACT-V2 §13: key counter for rows from `consumed` on (null until seeded). */
  private keys: KeyCounter | null = null;

  constructor(
    readonly file: string,
    private readonly onBatch: (batch: TailBatch) => void,
    private readonly debounceMs = 250,
    /** Key state of the file (counts of every row before the start offset are derived from it). */
    private readonly keySource: ((file: string) => Promise<KeySnapshot>) | null = null,
  ) {}

  /** Seed the key counter with the counts of all rows before `consumed` (once per start). */
  private async seedKeys(): Promise<void> {
    if (this.keys) return;
    const at = this.consumed;
    let snap: KeySnapshot;
    try {
      snap = this.keySource ? await this.keySource(this.file) : await scanKeySnapshot(this.file, at);
    } catch {
      snap = await scanKeySnapshot(this.file, at);
    }
    if (!this.keys) this.keys = new KeyCounter(countsAt(snap, at));
  }

  get running(): boolean {
    return !this.stopped;
  }

  /** Start at the current end of file (history is served by /messages). */
  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    try {
      this.consumed = fs.statSync(this.file).size;
    } catch {
      this.consumed = 0; // pi creates the JSONL lazily
    }
    this.splitter.reset();
    this.keys = null;
    try {
      // Watch the directory: the file may not exist yet.
      const dir = path.dirname(this.file);
      const base = path.basename(this.file);
      this.watcher = fs.watch(dir, (_ev, name) => {
        if (name && name.toString() !== base) return;
        this.schedule();
      });
      this.watcher.on('error', () => this.closeWatcher());
    } catch {
      this.watcher = null;
    }
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.closeWatcher();
  }

  private closeWatcher(): void {
    try { this.watcher?.close(); } catch { /* ignore */ }
    this.watcher = null;
  }

  schedule(): void {
    if (this.stopped || this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.poll();
    }, this.debounceMs);
    this.timer.unref?.();
  }

  /** Read whatever was appended since the last poll (serialized). */
  poll(): Promise<void> {
    if (this.reading) {
      this.again = true;
      return this.reading;
    }
    this.reading = this.readOnce()
      .catch(() => undefined)
      .finally(() => {
        this.reading = null;
        if (this.again && !this.stopped) {
          this.again = false;
          void this.poll();
        }
      });
    return this.reading;
  }

  private remember(id: string): void {
    if (this.seen.has(id)) return;
    this.seen.add(id);
    this.seenOrder.push(id);
    if (this.seenOrder.length > SEEN_LIMIT) {
      const old = this.seenOrder.shift();
      if (old) this.seen.delete(old);
    }
  }

  private async readOnce(): Promise<void> {
    let fh: fs.promises.FileHandle;
    try {
      fh = await fs.promises.open(this.file, 'r');
    } catch {
      return;
    }
    try {
      await this.seedKeys();
      const stat = await fh.stat();
      if (stat.size < this.consumed) {
        await this.resync(fh, stat);
        return;
      }
      if (stat.size === this.consumed) return;
      const out: ChatMessage[] = [];
      let pos = this.consumed;
      const buf = Buffer.alloc(Math.min(READ_CHUNK, stat.size - pos));
      while (pos < stat.size) {
        const { bytesRead } = await fh.read(buf, 0, Math.min(buf.length, stat.size - pos), pos);
        if (!bytesRead) break;
        pos += bytesRead;
        for (const line of this.splitter.push(buf.subarray(0, bytesRead))) {
          const m = this.lineToMessage(line);
          if (m) out.push(m);
        }
      }
      this.consumed = pos;
      this.emit(out, stat.mtimeMs);
    } finally {
      await fh.close().catch(() => undefined);
    }
  }

  private lineToMessage(line: string): ChatMessage | null {
    const entry = parseEntry(line);
    if (!isMessageEntry(entry)) return null;
    // Count EVERY row in file order (even ones deduped below) so keys match history.
    const key = (this.keys ??= new KeyCounter()).assign(entry.message);
    const mapped = entryToChatMessage(entry, key);
    if (!mapped) return null;
    if (mapped.id) {
      if (this.seen.has(mapped.id)) return null;
      this.remember(mapped.id);
      this.lastId = mapped.id;
    }
    return mapped;
  }

  /**
   * File shrank (rewrite/compaction by another writer): re-read it and emit
   * only entries AFTER the last one we emitted, minus anything already seen.
   * If our anchor vanished we cannot place new rows — re-sync silently (the
   * client reloads history on reconnect/reset).
   */
  private async resync(fh: fs.promises.FileHandle, stat: fs.Stats): Promise<void> {
    // Re-parse from byte 0 with OUR splitter so its trailing partial line
    // (raw bytes) carries over to the next poll untouched.
    this.splitter.reset();
    const counter = new KeyCounter(); // whole file re-read: recount from scratch
    const entries: ChatMessage[] = [];
    let pos = 0;
    const buf = Buffer.alloc(Math.min(READ_CHUNK, Math.max(1, stat.size)));
    while (pos < stat.size) {
      const { bytesRead } = await fh.read(buf, 0, Math.min(buf.length, stat.size - pos), pos);
      if (!bytesRead) break;
      pos += bytesRead;
      for (const line of this.splitter.push(buf.subarray(0, bytesRead))) {
        const e = parseEntry(line);
        const m = isMessageEntry(e) ? entryToChatMessage(e, counter.assign(e.message)) : null;
        if (m) entries.push(m);
      }
    }
    this.consumed = pos;
    this.keys = counter;
    const anchor = this.lastId ? entries.findIndex((m) => m.id === this.lastId) : -1;
    const fresh = anchor >= 0
      ? entries.slice(anchor + 1).filter((m) => !m.id || !this.seen.has(m.id))
      : [];
    for (const m of entries) if (m.id) this.remember(m.id);
    const last = entries[entries.length - 1];
    if (last?.id) this.lastId = last.id;
    this.emit(fresh, stat.mtimeMs);
  }

  private emit(messages: ChatMessage[], mtimeMs: number): void {
    if (!messages.length || this.stopped) return;
    this.onBatch({ messages, mtimeMs });
  }
}
