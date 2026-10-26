import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { SessionFileCache } from '../src/filecache.js';

const msg = (id: string, role: string, text: string, ts: number): string =>
  `${JSON.stringify({ type: 'message', id, message: { role, content: [{ type: 'text', text }], timestamp: ts } })}\n`;
const header = '{"type":"session","version":3,"id":"hdr-1","timestamp":"2026-10-26T10:00:00.000Z","cwd":"/w"}\n';

let dir: string;
let file: string;

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fcache-'));
  file = path.join(dir, 's.jsonl');
});
afterEach(() => fs.rmSync(dir, { recursive: true, force: true }));

describe('SessionFileCache', () => {
  it('parses header, name, counts and messages; then only appended bytes', async () => {
    fs.writeFileSync(file, header + msg('m1', 'user', 'first prompt', 1) + '{"type":"session_info","name":"Named"}\n');
    const cache = new SessionFileCache();
    const sum = await cache.summary(file);
    expect(sum).toMatchObject({ headerId: 'hdr-1', cwd: '/w', sessionName: 'Named', firstUserText: 'first prompt', messageCount: 1, lastMessageAt: 1 });
    const first = await cache.messages(file);
    expect(first.map((m) => m.id)).toEqual(['m1']);

    // Append a partial line: not counted until complete.
    const next = msg('m2', 'assistant', 'answer 😀', 2);
    fs.appendFileSync(file, next.slice(0, 15));
    expect((await cache.messages(file)).map((m) => m.id)).toEqual(['m1']);
    fs.appendFileSync(file, next.slice(15));
    const second = await cache.messages(file);
    expect(second.map((m) => m.id)).toEqual(['m1', 'm2']);
    expect(second[1]!.text).toBe('answer 😀');
    expect((await cache.summary(file))!.messageCount).toBe(2);
  });

  it('full re-parse on shrink and on a rewritten head', async () => {
    fs.writeFileSync(file, header + msg('m1', 'user', 'a', 1) + msg('m2', 'user', 'b', 2));
    const cache = new SessionFileCache();
    expect((await cache.messages(file)).length).toBe(2);
    fs.writeFileSync(file, header + msg('m9', 'user', 'z', 9));
    expect((await cache.messages(file)).map((m) => m.id)).toEqual(['m9']);
    // Same length, different head → detected via head signature.
    const other = header.replace('hdr-1', 'hdr-2') + msg('m9', 'user', 'z', 9) + msg('m10', 'user', 'y', 10);
    fs.writeFileSync(file, other);
    const sum = await cache.summary(file);
    expect(sum!.headerId).toBe('hdr-2');
    expect(sum!.messageCount).toBe(2);
  });

  it('summaryFast returns a head-only summary for big unseen files and finishes async', async () => {
    fs.writeFileSync(file, header + msg('m1', 'user', 'x'.repeat(5000), 1) + msg('m2', 'user', 'y', 2));
    let updated = 0;
    const cache = new SessionFileCache({ syncLimitBytes: 100, onAsyncUpdate: () => { updated++; } });
    const quick = await cache.summaryFast(file, fs.statSync(file));
    expect(quick.headerId).toBe('hdr-1');
    expect(quick.complete).toBe(false);
    await new Promise((r) => setTimeout(r, 50));
    expect(updated).toBe(1);
    const full = await cache.summaryFast(file, fs.statSync(file));
    expect(full.messageCount).toBe(2);
    expect(full.complete).toBe(true);
  });

  it('evicts parsed messages beyond the byte budget but keeps summaries', async () => {
    const a = path.join(dir, 'a.jsonl');
    const b = path.join(dir, 'b.jsonl');
    fs.writeFileSync(a, header + msg('a1', 'user', 'x'.repeat(2000), 1));
    fs.writeFileSync(b, header + msg('b1', 'user', 'y'.repeat(2000), 1));
    const cache = new SessionFileCache({ maxMessageBytes: 3000 });
    await cache.messages(a);
    await cache.messages(b);
    expect(cache.cachedMessageBytes).toBeLessThanOrEqual(3000);
    expect((await cache.summary(a))!.messageCount).toBe(1);
    expect((await cache.messages(a)).map((m) => m.id)).toEqual(['a1']);
  });
});
