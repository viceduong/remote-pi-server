import { describe, expect, it } from 'vitest';
import { LineSplitter, stripLoneSurrogates, readTailLines } from '../src/jsonl.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

describe('LineSplitter (byte-level NDJSON)', () => {
  it('keeps a partial trailing line until it completes', () => {
    const s = new LineSplitter();
    expect(s.push(Buffer.from('{"a":1}\n{"b":'))).toEqual(['{"a":1}']);
    expect(s.pendingBytes).toBe(5);
    expect(s.push(Buffer.from('2}\n'))).toEqual(['{"b":2}']);
    expect(s.pendingBytes).toBe(0);
  });

  it('never corrupts a multibyte character split across chunks', () => {
    const line = Buffer.from('{"t":"héllo 😀 世界"}\n', 'utf8');
    const s = new LineSplitter();
    const out: string[] = [];
    // Feed one byte at a time — every split point inside every code point.
    for (const byte of line) out.push(...s.push(Buffer.from([byte])));
    expect(out).toEqual(['{"t":"héllo 😀 世界"}']);
  });

  it('splits on LF only (U+2028 inside JSON strings is data), strips CR', () => {
    const s = new LineSplitter();
    expect(s.push(Buffer.from('{"x":"a b"}\r\n{"y":1}\n'))).toEqual(['{"x":"a b"}', '{"y":1}']);
  });

  it('drops an oversized line up to its newline and recovers', () => {
    const s = new LineSplitter(10);
    expect(s.push(Buffer.from('x'.repeat(50)))).toEqual([]);
    expect(s.push(Buffer.from('yyy\n{"ok":1}\n'))).toEqual(['{"ok":1}']);
  });

  it('flush returns the trailing partial line', () => {
    const s = new LineSplitter();
    s.push(Buffer.from('{"a":1}\n{"tail":true}'));
    expect(s.flush()).toBe('{"tail":true}');
    expect(s.flush()).toBeNull();
  });
});

describe('stripLoneSurrogates', () => {
  it('keeps emoji (valid pairs) intact', () => {
    expect(stripLoneSurrogates('hi 😀👍🏽 there')).toBe('hi 😀👍🏽 there');
  });
  it('removes lone high and lone low surrogates only', () => {
    expect(stripLoneSurrogates('a\uD800b')).toBe('ab');
    expect(stripLoneSurrogates('a\uDC00b')).toBe('ab');
    expect(stripLoneSurrogates('\uD800😀')).toBe('😀');
    expect(stripLoneSurrogates('😀\uDE00')).toBe('😀');
  });
});

describe('readTailLines', () => {
  it('drops the partial first line of a mid-file window', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'jsonl-'));
    const file = path.join(dir, 'f.jsonl');
    fs.writeFileSync(file, '{"n":1}\n{"n":2}\n{"n":3}\n');
    expect(await readTailLines(file, 1024)).toEqual(['{"n":1}', '{"n":2}', '{"n":3}']);
    expect(await readTailLines(file, 12)).toEqual(['{"n":3}']);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
