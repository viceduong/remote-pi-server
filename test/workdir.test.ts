import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  MAX_WORKDIR_LENGTH,
  WorkdirError,
  checkWorkdir,
  isInside,
  listDir,
  parseRoots,
  prepareWorkdir,
  skipDirName,
  syntaxError,
  type WorkdirRoot,
} from '../src/workdir.js';

const IS_WIN = process.platform === 'win32';
const winIt = IS_WIN ? it : it.skip;

let tmp: string;
let rootDir: string;
let outside: string;
let roots: WorkdirRoot[];

beforeEach(() => {
  tmp = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-workdir-')));
  rootDir = path.join(tmp, 'Root');
  outside = path.join(tmp, 'Outside');
  fs.mkdirSync(rootDir);
  fs.mkdirSync(outside);
  roots = parseRoots(rootDir, '/unused');
});

afterEach(() => {
  fs.rmSync(tmp, { recursive: true, force: true });
});

function link(target: string, at: string): void {
  fs.symlinkSync(target, at, IS_WIN ? 'junction' : 'dir');
}

function rejectCode(fn: () => unknown): { status: number; apiCode: string; roots?: string[] } {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(WorkdirError);
    const e = err as WorkdirError;
    return { status: e.status, apiCode: e.apiCode, ...(e.roots ? { roots: e.roots } : {}) };
  }
  throw new Error('expected a WorkdirError');
}

describe('syntaxError (Windows rules)', () => {
  const bad: Array<[string, RegExp]> = [
    ['', /empty/],
    ['relative\\dir', /absolute/],
    ['.\\dir', /absolute/],
    ['..\\dir', /absolute/],
    ['D:', /absolute/],
    ['D:foo', /absolute/],
    ['\\foo', /absolute/],
    ['\\\\server\\share\\x', /UNC/],
    ['//server/share/x', /UNC/],
    ['\\\\?\\C:\\x', /UNC/],
    ['\\\\.\\pipe\\x', /UNC/],
    ['\\\\.\\C:\\x', /UNC/],
    ['D:\\a\u0000b', /control/],
    ['D:\\a\nb', /control/],
    ['D:\\a\u001fb', /control/],
    ['D:\\CON', /reserved/],
    ['D:\\proj\\con', /reserved/],
    ['D:\\nul.txt', /reserved/],
    ['D:\\x\\COM1', /reserved/],
    ['D:\\x\\lpt9.log', /reserved/],
    ['D:\\x\\AUX', /reserved/],
    ['D:\\x\\PRN', /reserved/],
    ['D:\\foo.', /dot or space/],
    ['D:\\foo \\bar', /dot or space/],
    ['D:\\foo\\bar...', /dot or space/],
    ['D:\\a?b', /invalid character/],
    ['D:\\a*b', /invalid character/],
    ['D:\\a:stream', /invalid character/],
    ['D:\\a<b', /invalid character/],
    ['D:\\a|b', /invalid character/],
    ['D:\\a"b', /invalid character/],
    [`D:\\${'a'.repeat(MAX_WORKDIR_LENGTH)}`, /longer than/],
  ];
  for (const [input, re] of bad) {
    it(`rejects ${JSON.stringify(input.length > 40 ? `${input.slice(0, 40)}…` : input)}`, () => {
      expect(syntaxError(input, true)).toMatch(re);
    });
  }

  it('accepts ordinary absolute paths (incl. `.` segments, forward slashes, root)', () => {
    for (const ok of ['D:\\', 'D:/', 'd:\\Projects\\my app', 'D:\\a\\.\\b', 'C:/x/y', 'D:\\con-tools', 'D:\\console', 'D:\\com10', 'D:\\.hidden', 'D:\\a.b']) {
      expect(syntaxError(ok, true), ok).toBeNull();
    }
  });

  it('rejects any `..` segment, even one that resolves inside the drive', () => {
    for (const bad of ['D:\\a\\..\\b', 'D:\\..\\Windows', 'D:/x/../y']) {
      expect(syntaxError(bad, true), bad).toMatch(/".." segments/);
    }
  });

  it('accepts exactly 260 chars, rejects 261', () => {
    const at = `D:\\${'a'.repeat(MAX_WORKDIR_LENGTH - 3)}`;
    expect(at.length).toBe(260);
    expect(syntaxError(at, true)).toBeNull();
    expect(syntaxError(`${at}b`, true)).toMatch(/longer than/);
  });
});

describe('syntaxError (POSIX rules)', () => {
  it('requires absolute non-network paths and no control chars', () => {
    expect(syntaxError('/home/u/proj', false)).toBeNull();
    expect(syntaxError('/home/u/CON', false)).toBeNull(); // not reserved on POSIX
    expect(syntaxError('home/u', false)).toMatch(/absolute/);
    expect(syntaxError('//net/share', false)).toMatch(/network/);
    expect(syntaxError('/a\u0000b', false)).toMatch(/control/);
  });
});

describe('isInside', () => {
  it('is case-insensitive on Windows and segment-aware', () => {
    expect(isInside('D:\\Root\\a', 'd:\\root', true)).toBe(true);
    expect(isInside('d:\\ROOT', 'D:\\Root', true)).toBe(true);
    expect(isInside('D:\\Root2', 'D:\\Root', true)).toBe(false);
    expect(isInside('D:\\Ro', 'D:\\Root', true)).toBe(false);
    expect(isInside('D:\\anything', 'D:\\', true)).toBe(true);
    expect(isInside('E:\\x', 'D:\\', true)).toBe(false);
  });

  it('is case-sensitive on POSIX', () => {
    expect(isInside('/work/a', '/work', false)).toBe(true);
    expect(isInside('/Work/a', '/work', false)).toBe(false);
    expect(isInside('/workshop', '/work', false)).toBe(false);
    expect(isInside('/x', '/', false)).toBe(true);
  });
});

describe('parseRoots', () => {
  it('splits on `;`, trims, dedupes (case-insensitively on Windows), falls back to the workdir', () => {
    const r = parseRoots(` ${rootDir} ; ${outside};;${IS_WIN ? rootDir.toUpperCase() : rootDir}`, '/unused');
    expect(r.map((x) => x.path)).toEqual([rootDir, outside]);
    expect(parseRoots(undefined, rootDir).map((x) => x.path)).toEqual([rootDir]);
    expect(parseRoots('  ', rootDir).map((x) => x.path)).toEqual([rootDir]);
  });

  it('throws on relative roots', () => {
    expect(() => parseRoots('relative', rootDir)).toThrow(/not absolute/);
  });

  it('records the realpath of a root that is itself a link', () => {
    const viaLink = path.join(tmp, 'rootlink');
    link(rootDir, viaLink);
    const [r] = parseRoots(viaLink, '/unused');
    expect(r!.path).toBe(viaLink);
    expect(r!.real.toLowerCase()).toBe(rootDir.toLowerCase());
  });
});

describe('checkWorkdir', () => {
  it('allows the root itself and paths below it (existing or not)', () => {
    expect(checkWorkdir(rootDir, roots)).toMatchObject({ path: rootDir, exists: true, isDir: true });
    fs.mkdirSync(path.join(rootDir, 'proj'));
    expect(checkWorkdir(path.join(rootDir, 'proj'), roots)).toMatchObject({ exists: true, isDir: true });
    expect(checkWorkdir(path.join(rootDir, 'new', 'deeper'), roots)).toMatchObject({
      path: path.join(rootDir, 'new', 'deeper'), exists: false, isDir: false,
    });
  });

  it('forbids paths outside every root with 403 workdir_forbidden + roots', () => {
    expect(rejectCode(() => checkWorkdir(outside, roots))).toEqual({ status: 403, apiCode: 'workdir_forbidden', roots: [rootDir] });
    expect(rejectCode(() => checkWorkdir(tmp, roots)).status).toBe(403);
    // Sibling sharing the root's prefix.
    fs.mkdirSync(`${rootDir}2`);
    expect(rejectCode(() => checkWorkdir(`${rootDir}2`, roots)).status).toBe(403);
  });

  it('forbids `..` segments, escaping or not', () => {
    expect(rejectCode(() => checkWorkdir(`${rootDir}${path.sep}..${path.sep}Outside`, roots)).status).toBe(403);
    expect(rejectCode(() => checkWorkdir(`${rootDir}${path.sep}a${path.sep}..${path.sep}..${path.sep}Outside`, roots)).status).toBe(403);
    expect(rejectCode(() => checkWorkdir(`${rootDir}${path.sep}a${path.sep}..${path.sep}b`, roots)).status).toBe(403);
  });

  winIt('is case-insensitive and returns the on-disk casing', () => {
    fs.mkdirSync(path.join(rootDir, 'MyProj'));
    const r = checkWorkdir(path.join(rootDir.toLowerCase(), 'myproj'), roots);
    expect(r.path).toBe(path.join(rootDir, 'MyProj'));
    expect(r.isDir).toBe(true);
    // Missing tail keeps the requested spelling below the real ancestor.
    expect(checkWorkdir(path.join(rootDir.toUpperCase(), 'NewOne'), roots).path).toBe(path.join(rootDir, 'NewOne'));
  });

  it('forbids a junction/symlink inside a root that points outside (and anything below it)', () => {
    const jn = path.join(rootDir, 'escape');
    link(outside, jn);
    expect(rejectCode(() => checkWorkdir(jn, roots)).status).toBe(403);
    expect(rejectCode(() => checkWorkdir(path.join(jn, 'missing', 'deep'), roots)).status).toBe(403);
    fs.mkdirSync(path.join(outside, 'sub'));
    expect(rejectCode(() => checkWorkdir(path.join(jn, 'sub'), roots)).status).toBe(403);
  });

  it('allows a junction that stays inside the roots (canonical path = target)', () => {
    fs.mkdirSync(path.join(rootDir, 'target'));
    const jn = path.join(rootDir, 'alias');
    link(path.join(rootDir, 'target'), jn);
    expect(checkWorkdir(jn, roots)).toMatchObject({ path: path.join(rootDir, 'target'), isDir: true });
  });

  it('forbids a link outside the roots that points inside', () => {
    const jn = path.join(outside, 'into-root');
    link(rootDir, jn);
    expect(rejectCode(() => checkWorkdir(jn, roots)).status).toBe(403);
  });

  it('forbids a dangling link', () => {
    const target = path.join(rootDir, 'gone');
    fs.mkdirSync(target);
    const jn = path.join(rootDir, 'dangling');
    link(target, jn);
    fs.rmdirSync(target);
    expect(rejectCode(() => checkWorkdir(jn, roots)).status).toBe(403);
  });

  winIt('maps every syntactic rejection to 403', () => {
    for (const p of ['relative', '\\\\server\\share', '\\\\?\\' + rootDir, `${rootDir}\\CON`, `${rootDir}\\x.`, `${rootDir}\\x `, `${rootDir}\\${'z'.repeat(300)}`]) {
      expect(rejectCode(() => checkWorkdir(p, roots)).status, p).toBe(403);
    }
  });

  it('honours multiple roots', () => {
    const two = parseRoots(`${rootDir};${outside}`, '/unused');
    expect(checkWorkdir(outside, two).isDir).toBe(true);
    expect(rejectCode(() => checkWorkdir(tmp, two)).status).toBe(403);
  });
});

describe('prepareWorkdir', () => {
  it('404 workdir_missing when absent and createDir is false — nothing is created', () => {
    const p = path.join(rootDir, 'nope');
    expect(rejectCode(() => prepareWorkdir(p, roots, false))).toEqual({ status: 404, apiCode: 'workdir_missing' });
    expect(fs.existsSync(p)).toBe(false);
  });

  it('mkdir -p of nested missing dirs, idempotent on repeat', () => {
    const p = path.join(rootDir, 'a', 'b', 'c');
    expect(prepareWorkdir(p, roots, true)).toBe(p);
    expect(fs.statSync(p).isDirectory()).toBe(true);
    expect(prepareWorkdir(p, roots, true)).toBe(p);
    expect(prepareWorkdir(p, roots, false)).toBe(p);
  });

  it('400 workdir_not_dir for a file, or a file in the middle of the path', () => {
    const f = path.join(rootDir, 'file.txt');
    fs.writeFileSync(f, 'x');
    expect(rejectCode(() => prepareWorkdir(f, roots, false))).toEqual({ status: 400, apiCode: 'workdir_not_dir' });
    expect(rejectCode(() => prepareWorkdir(f, roots, true))).toEqual({ status: 400, apiCode: 'workdir_not_dir' });
    expect(rejectCode(() => prepareWorkdir(path.join(f, 'sub'), roots, true))).toEqual({ status: 400, apiCode: 'workdir_not_dir' });
  });

  it('never creates anything outside the roots (403 before mkdir)', () => {
    const jn = path.join(rootDir, 'escape');
    link(outside, jn);
    expect(rejectCode(() => prepareWorkdir(path.join(jn, 'x', 'y'), roots, true)).status).toBe(403);
    expect(fs.existsSync(path.join(outside, 'x'))).toBe(false);
    expect(rejectCode(() => prepareWorkdir(path.join(outside, 'z'), roots, true)).status).toBe(403);
    expect(fs.existsSync(path.join(outside, 'z'))).toBe(false);
  });
});

describe('listDir', () => {
  it('lists subdirectories only, sorted case-insensitively, skipping hidden/system/noise dirs', () => {
    for (const d of ['beta', 'Alpha', 'gamma', '.git', '.hidden', 'node_modules', '$RECYCLE.BIN', 'System Volume Information']) {
      fs.mkdirSync(path.join(rootDir, d));
    }
    fs.writeFileSync(path.join(rootDir, 'file.txt'), 'x');
    const l = listDir(rootDir, roots);
    expect(l).toMatchObject({ path: rootDir, exists: true, isDir: true, allowed: true, parent: null, truncated: false });
    expect(l.entries).toEqual([
      { name: 'Alpha', path: path.join(rootDir, 'Alpha') },
      { name: 'beta', path: path.join(rootDir, 'beta') },
      { name: 'gamma', path: path.join(rootDir, 'gamma') },
    ]);
  });

  it('parent is null at a root and the containing dir below it', () => {
    fs.mkdirSync(path.join(rootDir, 'a', 'b'), { recursive: true });
    expect(listDir(path.join(rootDir, 'a', 'b'), roots).parent).toBe(path.join(rootDir, 'a'));
    expect(listDir(path.join(rootDir, 'a'), roots).parent).toBe(rootDir);
    if (IS_WIN) expect(listDir(rootDir.toLowerCase(), roots).parent).toBeNull();
  });

  it('includes junctions inside the roots, never ones escaping them', () => {
    fs.mkdirSync(path.join(rootDir, 'target'));
    link(path.join(rootDir, 'target'), path.join(rootDir, 'alias'));
    link(outside, path.join(rootDir, 'escape'));
    const names = listDir(rootDir, roots).entries.map((e) => e.name);
    expect(names).toEqual(['alias', 'target']);
  });

  it('truncates at the limit', () => {
    for (let i = 0; i < 7; i++) fs.mkdirSync(path.join(rootDir, `d${i}`));
    const l = listDir(rootDir, roots, 5);
    expect(l.entries).toHaveLength(5);
    expect(l.truncated).toBe(true);
    expect(listDir(rootDir, roots, 7).truncated).toBe(false);
  });

  it('reports missing paths and files without a listing', () => {
    expect(listDir(path.join(rootDir, 'missing'), roots)).toMatchObject({ exists: false, isDir: false, entries: [], parent: rootDir });
    fs.writeFileSync(path.join(rootDir, 'f'), 'x');
    expect(listDir(path.join(rootDir, 'f'), roots)).toMatchObject({ exists: true, isDir: false, entries: [] });
  });

  it('403 outside the roots (no listing)', () => {
    expect(rejectCode(() => listDir(outside, roots)).status).toBe(403);
    link(outside, path.join(rootDir, 'escape'));
    expect(rejectCode(() => listDir(path.join(rootDir, 'escape'), roots)).status).toBe(403);
  });

  it('skip rules', () => {
    for (const n of ['.git', 'node_modules', 'NODE_MODULES', '$Recycle.Bin', '.vscode', 'System Volume Information']) expect(skipDirName(n), n).toBe(true);
    for (const n of ['src', 'git', 'modules', 'Program Files']) expect(skipDirName(n), n).toBe(false);
  });
});
