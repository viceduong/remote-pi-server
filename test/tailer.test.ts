import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { FileTailer, type TailBatch } from '../src/tailer.js';

const entry = (id: string, role: string, text: string): string =>
  `${JSON.stringify({ type: 'message', id, timestamp: '2026-10-26T10:00:00.000Z', message: { role, content: [{ type: 'text', text }], timestamp: 1 } })}\n`;

let dir: string;
let file: string;
let batches: TailBatch[];
let tailer: FileTailer;

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tailer-'));
  file = path.join(dir, 's.jsonl');
  fs.writeFileSync(file, `{"type":"session","id":"s"}\n${entry('e0', 'user', 'old history')}`);
  batches = [];
  tailer = new FileTailer(file, (b) => batches.push(b), 5);
  tailer.start();
});

afterEach(() => {
  tailer.stop();
  fs.rmSync(dir, { recursive: true, force: true });
});

const ids = (): string[] => batches.flatMap((b) => b.messages.map((m) => m.id ?? '?'));

describe('FileTailer', () => {
  it('starts at end of file (history is not re-emitted)', async () => {
    await tailer.poll();
    expect(ids()).toEqual([]);
  });

  it('emits a partial line exactly once, after it completes (no duplicate)', async () => {
    const line = entry('e1', 'assistant', 'hello world');
    fs.appendFileSync(file, line.slice(0, 20));
    await tailer.poll();
    expect(ids()).toEqual([]);
    fs.appendFileSync(file, line.slice(20));
    await tailer.poll();
    await tailer.poll();
    expect(ids()).toEqual(['e1']);
    expect(batches[0]!.messages[0]!.text).toBe('hello world');
  });

  it('handles a multibyte character split across two appends', async () => {
    const bytes = Buffer.from(entry('e2', 'assistant', 'emoji 😀 and 世界'), 'utf8');
    const cut = bytes.indexOf(Buffer.from('😀', 'utf8')) + 2; // inside the 4-byte emoji
    fs.appendFileSync(file, bytes.subarray(0, cut));
    await tailer.poll();
    fs.appendFileSync(file, bytes.subarray(cut));
    await tailer.poll();
    expect(batches.flatMap((b) => b.messages.map((m) => m.text))).toEqual(['emoji 😀 and 世界']);
  });

  it('on shrink re-syncs without re-emitting what it already sent', async () => {
    fs.appendFileSync(file, entry('e1', 'user', 'one') + entry('e2', 'assistant', 'two'));
    await tailer.poll();
    expect(ids()).toEqual(['e1', 'e2']);
    // Another writer rewrites the file shorter (old history dropped) and adds e3.
    const before = fs.statSync(file).size;
    fs.writeFileSync(file, entry('e2', 'assistant', 'two') + entry('e3', 'user', 'three'));
    expect(fs.statSync(file).size).toBeLessThan(before);
    await tailer.poll();
    expect(ids()).toEqual(['e1', 'e2', 'e3']);
  });

  it('a shrink that loses the anchor re-syncs silently', async () => {
    fs.appendFileSync(file, entry('e1', 'user', 'one'));
    await tailer.poll();
    fs.writeFileSync(file, entry('x1', 'user', 'unrelated'));
    await tailer.poll();
    expect(ids()).toEqual(['e1']);
    fs.appendFileSync(file, entry('x2', 'assistant', 'next'));
    await tailer.poll();
    expect(ids()).toEqual(['e1', 'x2']);
  });
});
