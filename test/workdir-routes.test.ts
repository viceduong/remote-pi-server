import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify';
import { pino } from 'pino';
import { SessionManager } from '../src/manager.js';
import { registerRoutes } from '../src/routes.js';
import { loadConfig } from '../src/config.js';
import { manglePath } from '../src/paths.js';
import { inspectLease } from '../src/lease.js';
import { parseRoots } from '../src/workdir.js';

/* CONTRACT §12 — POST /api/sessions {workdir, createDir} and GET /api/dirs. */

const log = pino({ enabled: false });
const IS_WIN = process.platform === 'win32';
const auth = { authorization: 'Bearer tok' };

let tmp: string;
let sessionDir: string;
let work: string;
let outside: string;
let spawnLog: string;
let bin: string;
let app: FastifyInstance;
let manager: SessionManager;
const extraManagers: SessionManager[] = [];

/**
 * `pi --mode rpc` stand-in: writes pi's session header (cwd = its real
 * process cwd) to `--session`, logs each spawn, answers get_state like pi.
 */
function writeFakePi(dir: string, logFile: string): string {
  const script = path.join(dir, 'fake-pi.mjs');
  fs.writeFileSync(script, `
import fs from 'node:fs';
import crypto from 'node:crypto';
import readline from 'node:readline';
const args = process.argv.slice(2);
const file = args[args.indexOf('--session') + 1];
let id;
try { id = JSON.parse(fs.readFileSync(file, 'utf8').split('\\n')[0]).id; } catch {}
if (!id) {
  id = crypto.randomUUID();
  fs.writeFileSync(file, JSON.stringify({ type: 'session', version: 3, id, timestamp: new Date().toISOString(), cwd: process.cwd() }) + '\\n');
}
fs.appendFileSync(${JSON.stringify(logFile)}, JSON.stringify({ cwd: process.cwd(), file }) + '\\n');
const out = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  const c = JSON.parse(line);
  const ok = (data) => out({ id: c.id, type: 'response', command: c.type, success: true, ...(data === undefined ? {} : { data }) });
  if (c.type === 'get_state') return ok({ sessionId: id, sessionFile: file, isStreaming: false, messageCount: 0, model: null });
  return ok();
});
`);
  if (IS_WIN) {
    const cmd = path.join(dir, 'fake-pi.cmd');
    fs.writeFileSync(cmd, `@"${process.execPath}" "${script}" %*\r\n`);
    return cmd;
  }
  const sh = path.join(dir, 'fake-pi');
  fs.writeFileSync(sh, `#!/bin/sh\nexec "${process.execPath}" "${script}" "$@"\n`, { mode: 0o755 });
  return sh;
}

function newManager(): SessionManager {
  return new SessionManager({
    sessionDir, workdir: work, workdirRoots: parseRoots(work, work), bin, maxAgents: 4, idleKillMs: 60_000,
    extraArgs: [], log, noTimers: true,
  });
}

function spawns(): { cwd: string; file: string }[] {
  if (!fs.existsSync(spawnLog)) return [];
  return fs.readFileSync(spawnLog, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l) as { cwd: string; file: string });
}

function writeSessionFile(cwd: string, id: string, mtimeMs: number): void {
  const dir = path.join(sessionDir, manglePath(cwd));
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${id}.jsonl`);
  fs.writeFileSync(file, `${JSON.stringify({ type: 'session', id, cwd })}\n`);
  fs.utimesSync(file, mtimeMs / 1000, mtimeMs / 1000);
}

beforeEach(async () => {
  tmp = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-wdroutes-')));
  sessionDir = path.join(tmp, 'sessions');
  work = path.join(tmp, 'work');
  outside = path.join(tmp, 'outside');
  fs.mkdirSync(work);
  fs.mkdirSync(outside);
  spawnLog = path.join(tmp, 'spawns.log');
  bin = writeFakePi(tmp, spawnLog);
  manager = newManager();
  app = Fastify();
  registerRoutes(app, {
    config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: work }),
    manager, piVersion: 'test', log: log as unknown as FastifyBaseLogger,
  });
  await app.ready();
});

afterEach(async () => {
  manager.stopAll();
  for (const m of extraManagers.splice(0)) m.stopAll();
  await app.close();
  // Children are killed asynchronously (taskkill) — retry the cleanup briefly.
  for (let i = 0; i < 20; i++) {
    try {
      fs.rmSync(tmp, { recursive: true, force: true });
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 250));
    }
  }
});

const post = (payload: Record<string, unknown>) => app.inject({ method: 'POST', url: '/api/sessions', headers: auth, payload });

describe('POST /api/sessions with workdir (§12)', () => {
  it('403 workdir_forbidden (+roots) outside the roots, for relative/UNC/reserved paths and junction escapes', async () => {
    const jn = path.join(work, 'escape');
    fs.symlinkSync(outside, jn, IS_WIN ? 'junction' : 'dir');
    const cases = [outside, 'relative/dir', path.join(work, '..', 'outside'), path.join(jn, 'new'),
      ...(IS_WIN ? ['\\\\server\\share\\x', `${work}\\CON`, `${work}\\trailing.`] : [])];
    for (const workdir of cases) {
      const res = await post({ workdir, createDir: true });
      expect(res.statusCode, workdir).toBe(403);
      expect(res.json()).toMatchObject({ code: 'workdir_forbidden', roots: [work] });
    }
    expect(fs.existsSync(path.join(outside, 'new'))).toBe(false);
    expect(spawns()).toHaveLength(0);
  });

  it('404 workdir_missing without createDir; 400 workdir_not_dir for a file; 400 on a non-boolean createDir', async () => {
    const missing = await post({ workdir: path.join(work, 'nope') });
    expect(missing.statusCode).toBe(404);
    expect(missing.json()).toMatchObject({ code: 'workdir_missing' });
    expect(fs.existsSync(path.join(work, 'nope'))).toBe(false);

    fs.writeFileSync(path.join(work, 'file.txt'), 'x');
    const notDir = await post({ workdir: path.join(work, 'file.txt'), createDir: true });
    expect(notDir.statusCode).toBe(400);
    expect(notDir.json()).toMatchObject({ code: 'workdir_not_dir' });

    const badFlag = await post({ workdir: work, createDir: 'yes' });
    expect(badFlag.statusCode).toBe(400);
    expect(spawns()).toHaveLength(0);
  });

  it('createDir → mkdir -p, pi spawned in that cwd, file in the mangled dir, discoverable after a rescan', async () => {
    const dir = path.join(work, 'apps', 'new-proj');
    const res = await post({ name: 'fresh', workdir: dir, createDir: true });
    expect(res.statusCode).toBe(201);
    const { session } = res.json() as { session: { id: string; workdir: string; running: boolean } };
    expect(session.workdir).toBe(dir);
    expect(session.running).toBe(true);
    expect(fs.statSync(dir).isDirectory()).toBe(true);

    const [spawn] = spawns();
    expect(spawn!.cwd.toLowerCase()).toBe(dir.toLowerCase());
    expect(path.dirname(spawn!.file)).toBe(path.join(sessionDir, manglePath(dir)));
    const header = JSON.parse(fs.readFileSync(spawn!.file, 'utf8').split('\n')[0]!) as { id: string; cwd: string };
    expect(header.cwd.toLowerCase()).toBe(dir.toLowerCase());
    // Re-keyed to pi's canonical id; bridge lease sits next to the file in the mangled dir.
    expect(session.id).toBe(header.id);
    expect(inspectLease(spawn!.file)).toMatchObject({ state: 'held' });

    // A fresh manager (server restart) finds it purely by walking the session dir.
    manager.stopAll();
    await new Promise((r) => setTimeout(r, 300));
    const m2 = newManager();
    extraManagers.push(m2);
    const sum = await m2.summary(header.id);
    expect(sum).toMatchObject({ id: header.id, workdir: header.cwd });
    expect((await m2.list()).map((s) => s.id)).toContain(header.id);

    // Resume: ownership is free, the child respawns in the session's own cwd on the same file.
    expect((await m2.ownership(header.id)).owner).toBe('none');
    const resumed = await m2.ensureRunning(header.id);
    expect(resumed).not.toBeNull();
    expect(resumed!.file).toBe(spawn!.file);
    expect(resumed!.workdir.toLowerCase()).toBe(dir.toLowerCase());
    const second = spawns()[1]!;
    expect(second.cwd.toLowerCase()).toBe(dir.toLowerCase());
    expect(second.file).toBe(spawn!.file);
    expect((await m2.ownership(header.id)).owner).toBe('bridge');
    expect(inspectLease(spawn!.file)).toMatchObject({ state: 'held' });

    // It now shows up as a recent dir.
    const dirs = await (async () => {
      const a = Fastify();
      registerRoutes(a, { config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: work }), manager: m2, piVersion: 't', log: log as unknown as FastifyBaseLogger });
      const r = await a.inject({ url: '/api/dirs', headers: auth });
      await a.close();
      return r.json() as { recent: { path: string; sessions: number }[] };
    })();
    expect(dirs.recent[0]).toMatchObject({ path: header.cwd, sessions: 1 });
  }, 30_000);

  it('existing dir without createDir works; absent workdir keeps the default', async () => {
    fs.mkdirSync(path.join(work, 'existing'));
    const a = await post({ workdir: path.join(work, 'existing') });
    expect(a.statusCode).toBe(201);
    expect((a.json() as { session: { workdir: string } }).session.workdir).toBe(path.join(work, 'existing'));
    const b = await post({ name: 'default' });
    expect(b.statusCode).toBe(201);
    expect((b.json() as { session: { workdir: string } }).session.workdir.toLowerCase()).toBe(work.toLowerCase());
    expect(spawns().map((s) => path.dirname(s.file))).toEqual([
      path.join(sessionDir, manglePath(path.join(work, 'existing'))),
      path.join(sessionDir, manglePath(work)),
    ]);
  }, 30_000);

  it('idempotent create (clientRequestId) makes one dir + one session, even concurrently', async () => {
    const dir = path.join(work, 'idem', 'x');
    const payload = { workdir: dir, createDir: true, clientRequestId: 'req-1' };
    const [r1, r2] = await Promise.all([post(payload), post(payload)]);
    const r3 = await post(payload);
    const ids = [r1, r2, r3].map((r) => {
      expect(r.statusCode).toBe(201);
      return (r.json() as { session: { id: string } }).session.id;
    });
    expect(new Set(ids).size).toBe(1);
    expect(spawns()).toHaveLength(1);
    expect(fs.readdirSync(path.join(sessionDir, manglePath(dir))).filter((f) => f.endsWith('.jsonl'))).toHaveLength(1);
  }, 30_000);

  it('a failed (forbidden) attempt is not cached under its clientRequestId', async () => {
    const dir = path.join(work, 'later');
    const first = await post({ workdir: dir, clientRequestId: 'req-2' });
    expect(first.statusCode).toBe(404);
    const second = await post({ workdir: dir, createDir: true, clientRequestId: 'req-2' });
    expect(second.statusCode).toBe(201);
  }, 30_000);
});

describe('GET /api/dirs (§12)', () => {
  it('no path → roots, recent (from the index, inside roots only, newest first, counted) and default', async () => {
    const now = Date.now();
    const a = path.join(work, 'a');
    const b = path.join(work, 'b');
    writeSessionFile(a, 'sa1', now - 60_000);
    writeSessionFile(a, 'sa2', now - 10_000);
    writeSessionFile(b, 'sb1', now - 30_000);
    writeSessionFile(outside, 'so1', now);
    if (IS_WIN) writeSessionFile(a.toUpperCase(), 'sa3', now - 90_000); // same dir, other spelling
    const res = await app.inject({ url: '/api/dirs', headers: auth });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { roots: string[]; recent: { path: string; lastUsedAt: number; sessions: number }[]; default: string };
    expect(body.roots).toEqual([work]);
    expect(body.default).toBe(work);
    expect(body.recent.map((r) => r.path)).toEqual([a, b]);
    expect(body.recent[0]).toMatchObject({ sessions: IS_WIN ? 3 : 2 });
    expect(Math.abs(body.recent[0]!.lastUsedAt - (now - 10_000))).toBeLessThan(2_000);
    expect(body.recent[1]).toMatchObject({ sessions: 1 });
  });

  it('recent is capped at 20', async () => {
    for (let i = 0; i < 25; i++) writeSessionFile(path.join(work, `p${i}`), `s${i}`, Date.now() - i * 1000);
    const body = (await app.inject({ url: '/api/dirs', headers: auth })).json() as { recent: { path: string }[] };
    expect(body.recent).toHaveLength(20);
    expect(body.recent[0]!.path).toBe(path.join(work, 'p0'));
  });

  it('path → subdirectory listing with skip rules, sorting and parent', async () => {
    for (const d of ['Zeta', 'alpha', 'Beta', '.git', 'node_modules', '.cache']) fs.mkdirSync(path.join(work, d));
    fs.mkdirSync(path.join(work, 'alpha', 'inner'));
    fs.writeFileSync(path.join(work, 'readme.md'), 'x');
    const res = await app.inject({ url: `/api/dirs?path=${encodeURIComponent(work)}`, headers: auth });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({
      path: work, exists: true, isDir: true, allowed: true, parent: null, truncated: false,
      entries: ['alpha', 'Beta', 'Zeta'].map((name) => ({ name, path: path.join(work, name) })),
    });
    const sub = (await app.inject({ url: `/api/dirs?path=${encodeURIComponent(path.join(work, 'alpha'))}`, headers: auth })).json();
    expect(sub).toMatchObject({ parent: work, entries: [{ name: 'inner', path: path.join(work, 'alpha', 'inner') }] });
    const missing = (await app.inject({ url: `/api/dirs?path=${encodeURIComponent(path.join(work, 'nope'))}`, headers: auth })).json();
    expect(missing).toMatchObject({ exists: false, isDir: false, allowed: true, entries: [], parent: work });
  });

  it('truncates at 500 entries', async () => {
    for (let i = 0; i < 503; i++) fs.mkdirSync(path.join(work, `d${String(i).padStart(3, '0')}`));
    const body = (await app.inject({ url: `/api/dirs?path=${encodeURIComponent(work)}`, headers: auth })).json() as { entries: unknown[]; truncated: boolean };
    expect(body.entries).toHaveLength(500);
    expect(body.truncated).toBe(true);
  });

  it('403 (no listing) outside the roots and through escaping junctions; escaping junctions are not listed', async () => {
    fs.symlinkSync(outside, path.join(work, 'escape'), IS_WIN ? 'junction' : 'dir');
    for (const p of [outside, path.join(work, 'escape'), path.join(work, '..'), 'relative']) {
      const res = await app.inject({ url: `/api/dirs?path=${encodeURIComponent(p)}`, headers: auth });
      expect(res.statusCode, p).toBe(403);
      expect(res.json()).toMatchObject({ code: 'workdir_forbidden', roots: [work] });
      expect(res.json()).not.toHaveProperty('entries');
    }
    const root = (await app.inject({ url: `/api/dirs?path=${encodeURIComponent(work)}`, headers: auth })).json() as { entries: unknown[] };
    expect(root.entries).toEqual([]);
  });

  it('requires auth', async () => {
    expect((await app.inject({ url: '/api/dirs' })).statusCode).toBe(401);
  });
});
