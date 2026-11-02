/**
 * CONTRACT-V2 §16 — background running bash (Ctrl+B) + background shells.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify';
import { pino } from 'pino';
import { SessionManager } from '../src/manager.js';
import { registerRoutes } from '../src/routes.js';
import { acquireLease } from '../src/lease.js';
import { loadConfig } from '../src/config.js';
import {
  defaultShellDirs, isValidShellId, normalizeFile, pidAlive, resolveShellDirs, ShellStore, utf8SafeSlice,
  type ShellRecord,
} from '../src/shells.js';

const log = pino({ enabled: false });
const tick = (ms = 40) => new Promise((r) => setTimeout(r, ms));
const auth = { authorization: 'Bearer tok' };

const ID_A = '0199ffff-1616-7aaa-8bbb-ccccdddd0001';
const ID_B = '0199ffff-1616-7aaa-8bbb-ccccdddd0002';

function pipePath(): string {
  const name = `rpi-shells-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  return process.platform === 'win32' ? `\\\\.\\pipe\\${name}` : path.join(os.tmpdir(), `${name}.sock`);
}

/* ---------------- fake owner (host TUI) ---------------- */

interface FakeOwner {
  ipc: string;
  received: Record<string, unknown>[];
  emit(type: string, event?: Record<string, unknown>): void;
  close(): Promise<void>;
}

async function fakeOwner(opts: { caps?: string[]; reply?: Record<string, unknown> } = {}): Promise<FakeOwner> {
  const caps = opts.caps ?? ['prompt.streamingBehavior', 'models', 'clear_queue', 'background_bash'];
  let seq = 100;
  const ipc = pipePath();
  const received: Record<string, unknown>[] = [];
  const sockets: net.Socket[] = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    let buf = '';
    socket.on('data', (d) => {
      buf += d.toString('utf8');
      let i: number;
      while ((i = buf.indexOf('\n')) >= 0) {
        const msg = JSON.parse(buf.slice(0, i)) as Record<string, unknown>;
        buf = buf.slice(i + 1);
        received.push(msg);
        const respond = (success: boolean, data: unknown, error?: string) =>
          socket.write(`${JSON.stringify({ type: 'response', id: msg.op === 'hello' ? 'hello' : msg.id, success, data, ...(error ? { error } : {}) })}\n`);
        if (msg.op === 'hello') {
          respond(true, { eventSeq: 0, isStreaming: false, protocolVersion: caps.includes('background_bash') ? 8 : 7, capabilities: caps });
        } else if (msg.op === 'background_bash') {
          if (!caps.includes('background_bash')) respond(false, undefined, 'unknown op background_bash');
          else respond(true, opts.reply ?? { detached: true });
        } else {
          respond(true, {});
        }
      }
    });
    socket.on('error', () => undefined);
  });
  await new Promise<void>((r) => server.listen(ipc, r));
  return {
    ipc,
    received,
    emit: (type, event = {}) => {
      for (const s of sockets) s.write(`${JSON.stringify({ type: 'event', seq: ++seq, eventType: type, event: { type, ...event } })}\n`);
    },
    close: () => new Promise<void>((r) => { for (const s of sockets) s.destroy(); server.close(() => r()); }),
  };
}

/* ---------------- fake RPC child ---------------- */

function writeFakePi(dir: string): string {
  const script = path.join(dir, 'fake-pi.mjs');
  fs.writeFileSync(script, `
import readline from 'node:readline';
const out = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
const prompts = [];
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  const c = JSON.parse(line);
  const ok = (data) => out({ id: c.id, type: 'response', command: c.type, success: true, ...(data === undefined ? {} : { data }) });
  switch (c.type) {
    case 'get_state': return ok({ isStreaming: false, messageCount: 0 });
    case 'prompt': prompts.push({ message: c.message, streamingBehavior: c.streamingBehavior ?? null }); return ok();
    case '__prompts': return ok({ prompts });
    case '__emit': for (const e of c.events) out(e); return ok();
    default: return ok();
  }
});
`);
  if (process.platform === 'win32') {
    const cmd = path.join(dir, 'fake-pi.cmd');
    fs.writeFileSync(cmd, `@"${process.execPath}" "${script}" %*\r\n`);
    return cmd;
  }
  const sh = path.join(dir, 'fake-pi');
  fs.writeFileSync(sh, `#!/bin/sh\nexec "${process.execPath}" "${script}" "$@"\n`, { mode: 0o755 });
  return sh;
}

/* ---------------- harness ---------------- */

let root: string;
let shellDir: string;
let owner: FakeOwner | null;
let app: FastifyInstance | null;
let manager: SessionManager | null;
const spawned: ChildProcess[] = [];

const writeSession = (id: string): string => {
  const dir = path.join(root, '--proj--');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${id}.jsonl`);
  fs.writeFileSync(file, `${JSON.stringify({ type: 'session', id, cwd: root })}\n`);
  return file;
};

function writeShell(rec: Partial<ShellRecord> & { id: string; sessionFile: string | null }, log?: string | Buffer): void {
  const full: ShellRecord = {
    command: 'sleep 100', status: 'running', pid: process.pid, startTime: Date.now(),
    outputFile: path.join(shellDir, `${rec.id}.log`), cwd: root, ...rec,
  };
  fs.writeFileSync(path.join(shellDir, `${rec.id}.json`), JSON.stringify(full));
  if (log !== undefined) fs.writeFileSync(path.join(shellDir, `${rec.id}.log`), log);
}

const readShell = (id: string): ShellRecord => JSON.parse(fs.readFileSync(path.join(shellDir, `${id}.json`), 'utf8')) as ShellRecord;

async function startApp(bin = 'pi-not-installed-xyz', store?: ShellStore): Promise<void> {
  manager = new SessionManager({ sessionDir: root, workdir: root, bin, maxAgents: 2, idleKillMs: 60_000, extraArgs: [], log, noTimers: true });
  app = Fastify();
  registerRoutes(app, {
    config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: root, REMOTE_PI_BG_BASH_DIRS: shellDir }),
    manager, piVersion: 'test', log: log as unknown as FastifyBaseLogger,
    ...(store ? { shells: store } : {}),
  });
  await app.ready();
}

const get = (url: string) => app!.inject({ method: 'GET', url, headers: auth });
const post = (url: string) => app!.inject({ method: 'POST', url, headers: auth });

/** A long-running process we own (stands in for a detached bash). */
function longProcess(): ChildProcess {
  const c = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore', windowsHide: true });
  spawned.push(c);
  return c;
}

beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-shells-'));
  shellDir = path.join(root, 'pi-bg-bash');
  fs.mkdirSync(shellDir);
  owner = null; app = null; manager = null;
});
afterEach(async () => {
  manager?.stopAll();
  await app?.close();
  await owner?.close();
  for (const c of spawned.splice(0)) { try { c.kill(); } catch { /* gone */ } }
  await tick(100);
  fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

/* ---------------- units ---------------- */

describe('§16 shell helpers', () => {
  it('validates ids strictly', () => {
    expect(isValidShellId('bg-0123abcd')).toBe(true);
    for (const bad of ['bg-0123ABCD', 'bg-0123abc', 'bg-0123abcde', '../bg-0123abcd', 'bg-0123abcd.json', 'mon-0123abcd', '', null]) {
      expect(isValidShellId(bad)).toBe(false);
    }
  });

  it('resolves dirs like pi-bash (tmpdir/pi-bg-bash) + the user temp on Windows; env override wins', () => {
    const d = defaultShellDirs({ USERPROFILE: 'C:\\Users\\X' }, 'win32');
    expect(d[0]).toBe(path.join(os.tmpdir(), 'pi-bg-bash'));
    expect(d).toContain(path.join('C:\\Users\\X', 'AppData', 'Local', 'Temp', 'pi-bg-bash'));
    expect(defaultShellDirs({}, 'linux')).toEqual([path.join(os.tmpdir(), 'pi-bg-bash')]);
    expect(resolveShellDirs(`${root};${root}`)).toEqual([path.resolve(root)]);
  });

  it('utf8SafeSlice drops a cut leading sequence and an incomplete trailing one', () => {
    const full = Buffer.from('aé€😀z', 'utf8'); // 1 + 2 + 3 + 4 + 1 bytes
    expect(utf8SafeSlice(full.subarray(2), true).toString('utf8')).toBe('€😀z'); // cut inside é
    expect(utf8SafeSlice(full.subarray(4), true).toString('utf8')).toBe('😀z'); // cut inside €
    expect(utf8SafeSlice(full.subarray(0, 9), false).toString('utf8')).toBe('aé€'); // writer mid-😀
    expect(utf8SafeSlice(full, false).toString('utf8')).toBe('aé€😀z');
  });
});

/* ---------------- shells routes ---------------- */

describe('§16 GET /shells, /output, /kill', () => {
  it('lists only this session\'s shells (path-normalized), newest first, dead running pid → failed', async () => {
    const fileA = writeSession(ID_A);
    const fileB = writeSession(ID_B);
    const variant = process.platform === 'win32' ? fileA.toUpperCase().replace(/\\/g, '/') : `${path.dirname(fileA)}/./${path.basename(fileA)}`;
    const now = Date.now();
    writeShell({ id: 'bg-00000001', sessionFile: fileA, status: 'completed', exitCode: 0, startTime: now - 5000, endTime: now - 4000 }, 'done\n');
    writeShell({ id: 'bg-00000002', sessionFile: variant, status: 'running', pid: process.pid, startTime: now - 1000 }, 'abc');
    writeShell({ id: 'bg-00000003', sessionFile: fileA, status: 'running', pid: 999_999_9, startTime: now - 3000 });
    writeShell({ id: 'bg-00000004', sessionFile: fileB, startTime: now });
    writeShell({ id: 'bg-00000005', sessionFile: null, startTime: now });
    fs.writeFileSync(path.join(shellDir, 'bg-00000006.json'), '{broken');
    fs.writeFileSync(path.join(shellDir, 'notashell.json'), JSON.stringify({ id: 'notashell', sessionFile: fileA }));
    await startApp();

    const res = await get(`/api/sessions/${ID_A}/shells`);
    expect(res.statusCode).toBe(200);
    const { shells } = res.json() as { shells: Record<string, unknown>[] };
    expect(shells.map((s) => [s.id, s.status])).toEqual([
      ['bg-00000002', 'running'], ['bg-00000003', 'failed'], ['bg-00000001', 'completed'],
    ]);
    expect(shells[0]).toMatchObject({ command: 'sleep 100', pid: process.pid, outputBytes: 3 });
    expect(shells[0]!.durationMs).toBeGreaterThanOrEqual(1000);
    expect(shells[2]).toMatchObject({ exitCode: 0, endTime: now - 4000, durationMs: 1000, outputBytes: 5 });
    expect(shells[1]!.outputBytes).toBe(0);
    // Liveness downgrade is reported, not written back (pi-bash owns the file).
    expect(readShell('bg-00000003').status).toBe('running');

    const b = (await get(`/api/sessions/${ID_B}/shells`)).json() as { shells: { id: string }[] };
    expect(b.shells.map((s) => s.id)).toEqual(['bg-00000004']);
    expect((await get('/api/sessions/0199ffff-1616-7aaa-8bbb-ccccdddd9999/shells')).statusCode).toBe(404);
  });

  it('rejects malformed / traversal shell ids and other sessions\' shells', async () => {
    const fileA = writeSession(ID_A);
    const fileB = writeSession(ID_B);
    writeShell({ id: 'bg-0000000b', sessionFile: fileB }, 'secret');
    writeShell({ id: 'bg-0000000a', sessionFile: fileA }, 'mine');
    await startApp();
    for (const bad of ['bg-0000000A', 'bg-0000000a.json', 'bg-1', '..%5Cbg-0000000b', 'bg-0000000b%00']) {
      expect((await get(`/api/sessions/${ID_A}/shells/${bad}/output`)).statusCode).toBe(400);
      expect((await post(`/api/sessions/${ID_A}/shells/${bad}/kill`)).statusCode).toBe(400);
    }
    // An encoded '/' never reaches the handler (router 404) — either way no content.
    for (const bad of ['..%2F..%2Fx', '%2E%2E', '..%2Fbg-0000000b']) {
      const r = await get(`/api/sessions/${ID_A}/shells/${bad}/output`);
      expect([400, 404]).toContain(r.statusCode);
      expect(r.body).not.toContain('secret');
    }
    expect((await get(`/api/sessions/${ID_A}/shells/bg-0000000b/output`)).statusCode).toBe(404);
    expect((await get(`/api/sessions/${ID_A}/shells/bg-0000000c/output`)).statusCode).toBe(404);
    expect((await get(`/api/sessions/${ID_A}/shells/bg-0000000a/output`)).json()).toEqual({ text: 'mine', truncated: false, size: 4 });
    expect((await get(`/api/sessions/${ID_A}/shells/bg-0000000a/output?tail=0`)).statusCode).toBe(400);
    expect((await get(`/api/sessions/${ID_A}/shells/bg-0000000a/output?tail=262145`)).statusCode).toBe(400);
  });

  it('tails the log UTF-8 safely with ANSI kept; default 16 KiB', async () => {
    const fileA = writeSession(ID_A);
    const body = `\x1b[32mstart\x1b[0m\n${'€'.repeat(10_000)}\nend 😀\n`; // > 16 KiB
    writeShell({ id: 'bg-000000aa', sessionFile: fileA }, body);
    writeShell({ id: 'bg-000000ab', sessionFile: fileA }, '\x1b[31mred\x1b[0m é');
    await startApp();
    const size = Buffer.byteLength(body);

    const def = (await get(`/api/sessions/${ID_A}/shells/bg-000000aa/output`)).json() as { text: string; truncated: boolean; size: number };
    expect(def.size).toBe(size);
    expect(def.truncated).toBe(true);
    expect(def.text).not.toContain('\ufffd');
    expect(def.text.endsWith('end 😀\n')).toBe(true);
    expect(Buffer.byteLength(def.text)).toBeLessThanOrEqual(16_384);
    expect(Buffer.byteLength(def.text)).toBeGreaterThan(16_384 - 3);

    for (const tail of [1, 2, 3, 4, 5, 7, 8, 100]) {
      const r = (await get(`/api/sessions/${ID_A}/shells/bg-000000aa/output?tail=${tail}`)).json() as { text: string };
      expect(r.text).not.toContain('\ufffd');
      expect(body.endsWith(r.text)).toBe(true);
    }
    const all = (await get(`/api/sessions/${ID_A}/shells/bg-000000aa/output?tail=262144`)).json() as { text: string; truncated: boolean };
    expect(all).toMatchObject({ text: body, truncated: false });
    expect((await get(`/api/sessions/${ID_A}/shells/bg-000000ab/output`)).json()).toMatchObject({ text: '\x1b[31mred\x1b[0m é', truncated: false });
  });

  it('kills only a running shell of this session (real tree kill), json → killed', async () => {
    const fileA = writeSession(ID_A);
    const fileB = writeSession(ID_B);
    const proc = longProcess();
    await tick(200);
    expect(pidAlive(proc.pid!)).toBe(true);
    const exited = new Promise<void>((r) => proc.once('exit', () => r()));
    writeShell({ id: 'bg-0000c0de', sessionFile: fileA, pid: proc.pid! }, 'x');
    writeShell({ id: 'bg-0000c0df', sessionFile: fileA, status: 'completed', exitCode: 0, endTime: Date.now() });
    await startApp();

    // Another session cannot see or kill it.
    expect((await post(`/api/sessions/${ID_B}/shells/bg-0000c0de/kill`)).statusCode).toBe(404);
    expect(pidAlive(proc.pid!)).toBe(true);
    expect(readShell('bg-0000c0de').status).toBe('running');
    void fileB;

    const res = await post(`/api/sessions/${ID_A}/shells/bg-0000c0de/kill`);
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true });
    await exited;
    expect(pidAlive(proc.pid!)).toBe(false);
    const rec = readShell('bg-0000c0de');
    expect(rec).toMatchObject({ status: 'killed', exitCode: -1, sessionFile: fileA, command: 'sleep 100' });
    expect(typeof rec.endTime).toBe('number');
    expect(fs.readdirSync(shellDir).filter((n) => n.endsWith('.tmp'))).toEqual([]);

    const again = await post(`/api/sessions/${ID_A}/shells/bg-0000c0de/kill`);
    expect(again.statusCode).toBe(409);
    expect(again.json()).toMatchObject({ code: 'not_running', status: 'killed' });
    expect((await post(`/api/sessions/${ID_A}/shells/bg-0000c0df/kill`)).statusCode).toBe(409);
  }, 30_000);

  it('kill failure on a live pid restores the record → 502', async () => {
    const fileA = writeSession(ID_A);
    writeShell({ id: 'bg-0000fa11', sessionFile: fileA, pid: process.pid });
    const store = new ShellStore([shellDir], { kill: () => Promise.reject(new Error('access denied')) });
    await startApp(undefined, store);
    const res = await post(`/api/sessions/${ID_A}/shells/bg-0000fa11/kill`);
    expect(res.statusCode).toBe(502);
    expect(readShell('bg-0000fa11').status).toBe('running');
  });

  it('normalizeFile is case-insensitive on Windows only', () => {
    expect(normalizeFile('C:\\A\\b.jsonl', 'win32')).toBe(normalizeFile('c:/a/B.JSONL', 'win32'));
  });
});

/* ---------------- POST /background ---------------- */

describe('§16 POST /background', () => {
  async function tui(opts: { caps?: string[]; reply?: Record<string, unknown> } = {}): Promise<void> {
    owner = await fakeOwner(opts);
    const file = writeSession(ID_A);
    acquireLease(file, 'nonce', owner.ipc, Date.now(), { kind: 'tui' });
    await startApp();
    const session = (await manager!.ensureRunning(ID_A))!;
    expect(session.isProxy).toBe(true);
  }

  it('owner v8: 409 while no tool runs, then op background_bash → {ok, detached}', async () => {
    await tui();
    const idle = await post(`/api/sessions/${ID_A}/background`);
    expect(idle.statusCode).toBe(409);
    expect(idle.json()).toMatchObject({ code: 'not_running' });
    expect(owner!.received.some((m) => m.op === 'background_bash')).toBe(false);

    owner!.emit('agent_start');
    owner!.emit('tool_execution_start', { toolCallId: 't1', toolName: 'bash', args: { command: 'sleep 100' } });
    await tick();
    const res = await post(`/api/sessions/${ID_A}/background`);
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true, detached: true });
    expect(owner!.received.filter((m) => m.op === 'background_bash')).toHaveLength(1);
  });

  it('owner reports detached:false (bash already finished) → 200 detached:false', async () => {
    await tui({ reply: { detached: false } });
    owner!.emit('agent_start');
    owner!.emit('tool_execution_start', { toolCallId: 't1', toolName: 'bash' });
    await tick();
    expect((await post(`/api/sessions/${ID_A}/background`)).json()).toEqual({ ok: true, detached: false });
  });

  it('owner without the capability (v7) → 502 "reload the TUI", op never sent', async () => {
    await tui({ caps: ['prompt.streamingBehavior', 'models', 'clear_queue'] });
    owner!.emit('agent_start');
    owner!.emit('tool_execution_start', { toolCallId: 't1', toolName: 'bash' });
    await tick();
    const res = await post(`/api/sessions/${ID_A}/background`);
    expect(res.statusCode).toBe(502);
    expect(res.json().error).toMatch(/reload the TUI/);
    expect(owner!.received.some((m) => m.op === 'background_bash')).toBe(false);
  });

  it('owner whose pi lacks signalBashDetach → 502', async () => {
    await tui({ reply: { detached: false, unsupported: true } });
    owner!.emit('agent_start');
    owner!.emit('tool_execution_start', { toolCallId: 't1', toolName: 'bash' });
    await tick();
    const res = await post(`/api/sessions/${ID_A}/background`);
    expect(res.statusCode).toBe(502);
    expect(res.json()).toMatchObject({ code: 'background_unsupported' });
  });

  it('no transport → 409 (never spawns); unknown session → 404', async () => {
    writeSession(ID_A);
    await startApp();
    expect((await post(`/api/sessions/${ID_A}/background`)).statusCode).toBe(409);
    expect(manager!.get(ID_A)?.running ?? false).toBe(false);
    expect((await post('/api/sessions/0199ffff-1616-7aaa-8bbb-ccccdddd9999/background')).statusCode).toBe(404);
  });

  it('RPC child: sends prompt "/remote-background" (no streamingBehavior) → detached null', async () => {
    writeSession(ID_A);
    await startApp(writeFakePi(root));
    const session = (await manager!.ensureRunning(ID_A))!;
    expect(session.hasChild).toBe(true);
    expect((await post(`/api/sessions/${ID_A}/background`)).statusCode).toBe(409);

    await session.request({ type: '__emit', events: [
      { type: 'agent_start' },
      { type: 'tool_execution_start', toolCallId: 'c1', toolName: 'bash', args: { command: 'sleep 100' } },
    ] });
    await tick();
    const res = await post(`/api/sessions/${ID_A}/background`);
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ ok: true, detached: null });
    const { prompts } = await session.request<{ prompts: { message: string; streamingBehavior: string | null }[] }>({ type: '__prompts' });
    expect(prompts).toEqual([{ message: '/remote-background', streamingBehavior: null }]);
    // Not a queued/delivered user message.
    expect(session.queue).toEqual([]);
  });
});
