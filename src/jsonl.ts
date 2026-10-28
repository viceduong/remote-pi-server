/**
 * JSONL primitives shared by every reader of pi session files and NDJSON
 * pipes: byte-level line splitting (multibyte-safe), crash-safe decoding,
 * lone-surrogate stripping, and the single "read the tail and parse" helper.
 */
import fs from 'node:fs';
import { mapAgentMessage, textOf } from './history.js';
import type { AgentMessage, ChatMessage } from './types.js';

/**
 * Matches ONLY lone UTF-16 surrogates (a high not followed by a low, or a low
 * not preceded by a high). Valid pairs (emoji) are untouched. No `u` flag on
 * purpose: we operate on code units.
 */
const LONE_SURROGATE_RE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;

export function stripLoneSurrogates(s: string): string {
  return s.replace(LONE_SURROGATE_RE, '');
}

// TextDecoder replaces invalid UTF-8 instead of tripping Node's StringBytes
// assertion (Buffer.toString on malformed input crashed the service before).
const decoder = new TextDecoder('utf-8');

export function decodeUtf8(buf: Uint8Array): string {
  return decoder.decode(buf);
}

const EMPTY = Buffer.alloc(0);
const LF = 0x0a;
const CR = 0x0d;

/**
 * Byte-level NDJSON splitter. Splits on 0x0A only (never inside a multibyte
 * UTF-8 sequence, never on U+2028/2029), strips a trailing \r, and keeps the
 * partial trailing line as BYTES until it completes — so a chunk boundary in
 * the middle of an emoji can never corrupt or duplicate data.
 */
export class LineSplitter {
  private remainder: Buffer = EMPTY;
  private skipping = false;
  /**
   * For the lines returned by the last push(): byte offset just after each
   * line's '
', relative to the start of the bytes held BEFORE that push
   * (i.e. absolute end = (bytesFedBefore - pendingBytesBefore) + lastEnds[i]).
   */
  lastEnds: number[] = [];

  constructor(private readonly maxLineBytes = 64 * 1024 * 1024) {}

  /** Bytes of the incomplete trailing line currently held. */
  get pendingBytes(): number {
    return this.remainder.length;
  }

  reset(): void {
    this.remainder = EMPTY;
    this.skipping = false;
  }

  /** Feed bytes; returns every COMPLETE line (decoded, \r stripped). */
  push(chunk: Uint8Array): string[] {
    const buf = this.remainder.length ? Buffer.concat([this.remainder, chunk]) : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    const lines: string[] = [];
    const ends: number[] = [];
    this.lastEnds = ends;
    let start = 0;
    for (;;) {
      const nl = buf.indexOf(LF, start);
      if (nl < 0) break;
      let end = nl;
      if (end > start && buf[end - 1] === CR) end--;
      if (this.skipping) {
        this.skipping = false; // tail of an oversized line — discard
      } else if (end > start) {
        lines.push(decodeUtf8(buf.subarray(start, end)));
        ends.push(nl + 1);
      }
      start = nl + 1;
    }
    const rest = buf.subarray(start);
    if (rest.length > this.maxLineBytes) {
      this.remainder = EMPTY;
      this.skipping = true; // drop the monster line up to its newline
    } else {
      // Copy so we do not pin a large chunk buffer for a small remainder.
      this.remainder = rest.length ? Buffer.from(rest) : EMPTY;
    }
    return lines;
  }

  /** Flush the trailing partial line (end of file reads). */
  flush(): string | null {
    if (!this.remainder.length || this.skipping) {
      this.reset();
      return null;
    }
    let end = this.remainder.length;
    if (this.remainder[end - 1] === CR) end--;
    const line = decodeUtf8(this.remainder.subarray(0, end));
    this.reset();
    return line;
  }
}

export interface SessionEntry {
  type?: string;
  id?: string;
  timestamp?: string | number;
  message?: AgentMessage;
  [key: string]: unknown;
}

export function parseEntry(line: string): SessionEntry | null {
  if (!line || line.charCodeAt(0) !== 0x7b /* { */) {
    const t = line.trim();
    if (!t.startsWith('{')) return null;
    line = t;
  }
  try {
    const obj = JSON.parse(line) as unknown;
    return obj && typeof obj === 'object' ? (obj as SessionEntry) : null;
  } catch {
    return null;
  }
}

/** Timestamp of an entry (message.timestamp ms, else entry ISO timestamp). */
export function entryTimestamp(entry: SessionEntry): number | null {
  const mts = entry.message?.timestamp;
  if (typeof mts === 'number' && Number.isFinite(mts)) return mts;
  const ts = entry.timestamp;
  if (typeof ts === 'number') return ts;
  if (typeof ts === 'string') {
    const n = Date.parse(ts);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}

/**
 * Map a `type:"message"` entry to the wire ChatMessage (entry id attached).
 * `key` (CONTRACT-V2 §13) is assigned by the caller in file order.
 */
export function entryToChatMessage(entry: SessionEntry, key?: string | null): ChatMessage | null {
  if (entry.type !== 'message' || !entry.message) return null;
  const msg = { ...entry.message } as AgentMessage;
  if (typeof entry.id === 'string') msg.id = entry.id;
  const mapped = mapAgentMessage(msg);
  if (mapped && key) mapped.key = key;
  return mapped;
}

/** Plain text of a content field — THE canonical text function (history.textOf). */
export function contentText(content: unknown): string {
  return textOf(content);
}

/**
 * THE "read the tail and parse" helper: complete lines of the last `maxBytes`
 * of a file (async, one read). When the window starts mid-file the first
 * (partial) line is dropped.
 */
export async function readTailLines(file: string, maxBytes: number): Promise<string[]> {
  const fh = await fs.promises.open(file, 'r');
  try {
    const size = (await fh.stat()).size;
    const start = Math.max(0, size - maxBytes);
    const buf = Buffer.alloc(size - start);
    const { bytesRead } = buf.length ? await fh.read(buf, 0, buf.length, start) : { bytesRead: 0 };
    return splitWindow(buf.subarray(0, bytesRead), start > 0);
  } finally {
    await fh.close();
  }
}

function splitWindow(buf: Buffer, dropFirst: boolean): string[] {
  const splitter = new LineSplitter();
  const lines = splitter.push(buf);
  const last = splitter.flush();
  if (last !== null) lines.push(last);
  if (dropFirst) lines.shift();
  return lines;
}
