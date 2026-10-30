import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify';
import { pino } from 'pino';
import { barkDecrypt, barkEncrypt, buildPushBody, sendBark, type BarkMessage } from '../src/bark.js';
import {
  NotifyService, NotifyState, TurnWatcher, applyPut, buildMessage, defaultSettings, notificationBody, publicSettings, redactBody,
  type TurnEnd,
} from '../src/notify.js';
import { SessionManager } from '../src/manager.js';
import { registerRoutes } from '../src/routes.js';
import { loadConfig } from '../src/config.js';

const log = pino({ enabled: false });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

let seq = 0;
const row = (role: string, extra: Record<string, unknown> = {}, ts = Date.now() + seq): string =>
  `${JSON.stringify({ type: 'message', id: `e${++seq}`, message: { role, content: [{ type: 'text', text: `${role} text ${seq}` }], timestamp: ts, ...extra } })}\n`;
const assistant = (stopReason: string, text = 'All done.\nsecond line', extra: Record<string, unknown> = {}, ts?: number): string =>
  `${JSON.stringify({ type: 'message', id: `e${++seq}`, message: { role: 'assistant', content: [{ type: 'thinking', thinking: 'hm' }, { type: 'text', text }], stopReason, timestamp: ts ?? Date.now() + seq, ...extra } })}\n`;

/* ---------------- encryption ---------------- */

describe('Bark encryption', () => {
  it('matches the documented known-answer vectors (docs/en-us + docs/encryption.md)', () => {
    const c = { algorithm: 'AES128' as const, mode: 'CBC' as const, key: '1234567890123456' };
    const json = '{"body": "test", "sound": "birdsong"}';
    expect(barkEncrypt(json, c, '1111111111111111')).toBe('d3QhjQjP5majvNt5CjsvFWwqqj2gKl96RFj5OO+u6ynTt7lkyigDYNA3abnnCLpr');
    expect(barkEncrypt(json, c, '1234567890123456')).toBe('+aPt5cwN9GbTLLSFri60l3h1X00u/9j1FENfWiTxhNHVLGU+XoJ15JJG5W/d/yf0');
  });

  it('round-trips every algorithm × mode; GCM appends a 16-byte tag', () => {
    const keys = { AES128: 'k'.repeat(16), AES192: 'k'.repeat(24), AES256: 'k'.repeat(32) } as const;
    for (const algorithm of ['AES128', 'AES192', 'AES256'] as const) {
      for (const mode of ['CBC', 'ECB', 'GCM'] as const) {
        const c = { algorithm, mode, key: keys[algorithm] };
        const iv = mode === 'CBC' ? 'i'.repeat(16) : mode === 'GCM' ? 'i'.repeat(12) : '';
        const pt = JSON.stringify({ title: 'Tiêu đề 😀', body: 'x' });
        const ct = barkEncrypt(pt, c, iv);
        expect(ct).toMatch(/^[A-Za-z0-9+/]+=*$/); // standard base64
        expect(barkDecrypt(ct, c, iv)).toBe(pt);
        if (mode === 'GCM') expect(Buffer.from(ct, 'base64').length).toBe(Buffer.byteLength(pt) + 16);
      }
    }
  });

  it('encrypted push body carries only device_key/ciphertext/iv/level; plaintext JSON decrypts back', () => {
    const msg: BarkMessage = { title: 'My session', body: 'Hello', group: 'sid', url: 'remotepi://session/sid', level: 'active' };
    const enc = { algorithm: 'AES256' as const, mode: 'CBC' as const, key: 'a'.repeat(32) };
    const body = buildPushBody('DEVKEY', msg, enc);
    expect(Object.keys(body).sort()).toEqual(['ciphertext', 'device_key', 'iv', 'level']);
    expect(body.iv).toMatch(/^[A-Za-z0-9]{16}$/);
    expect(JSON.parse(barkDecrypt(body.ciphertext!, enc, body.iv!))).toEqual(msg);
    const gcm = buildPushBody('DEVKEY', msg, { ...enc, mode: 'GCM' });
    expect(gcm.iv).toHaveLength(12);
    const ecb = buildPushBody('DEVKEY', msg, { ...enc, mode: 'ECB' });
    expect(ecb.iv).toBeUndefined();
    expect(JSON.stringify(ecb)).not.toContain('Hello');
    // Two pushes never share an IV / ciphertext.
    expect(buildPushBody('DEVKEY', msg, enc).ciphertext).not.toBe(body.ciphertext);
    expect(buildPushBody('DEVKEY', msg, null)).toEqual({ device_key: 'DEVKEY', ...msg });
    expect(JSON.stringify(redactBody(body))).not.toContain('DEVKEY');
  });
});

/* ---------------- payload ---------------- */

describe('payload', () => {
  const turn = (over: Partial<TurnEnd>): TurnEnd => ({
    file: path.join('x', '--D--proj--', 'abc.jsonl'), key: 'assistant:1', kind: 'stop', stopReason: 'stop',
    text: '', errorMessage: null, timestamp: 1, end: 0, ...over,
  });

  it('body: first non-empty line ≤180; error; aborted', () => {
    expect(notificationBody(turn({ text: '\n\n  First line  \nsecond' }))).toBe('First line');
    const long = notificationBody(turn({ text: 'y'.repeat(500) }));
    expect(Array.from(long)).toHaveLength(180);
    expect(long.endsWith('…')).toBe(true);
    expect(notificationBody(turn({ kind: 'error', errorMessage: 'rate limited\nmore' }))).toBe('Error: rate limited');
    expect(notificationBody(turn({ kind: 'aborted', text: 'partial' }))).toBe('Stopped');
  });

  it('title = session name, group = id, url scheme; fallback to folder name', () => {
    const m = buildMessage(turn({ text: 'hi' }), { id: 'S1', name: 'Refactor' }, 'timeSensitive');
    expect(m).toEqual({ title: 'Refactor', body: 'hi', group: 'S1', url: 'remotepi://session/S1', level: 'timeSensitive' });
    const fb = buildMessage(turn({ text: 'hi' }), null, 'active');
    expect(fb).toMatchObject({ title: 'proj', group: 'abc', url: 'remotepi://session/abc' });
  });
});

/* ---------------- watcher ---------------- */

describe('TurnWatcher', () => {
  let dir: string;
  let file: string;
  let got: TurnEnd[];
  let watcher: TurnWatcher;
  let state: NotifyState;

  const make = (stateFile: string | null = null, debounceMs = 20): TurnWatcher => {
    state = new NotifyState(stateFile);
    return new TurnWatcher({
      sessionDir: dir, state, onTurnEnd: (t) => { got.push(t); }, debounceMs, rescanMs: 0, noFsWatch: true,
    });
  };

  beforeEach(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'notify-'));
    fs.mkdirSync(path.join(dir, '--proj--'));
    file = path.join(dir, '--proj--', 's.jsonl');
    // Pre-existing history incl. a finished turn: must never notify.
    fs.writeFileSync(file, `{"type":"session","id":"S"}\n${row('user')}${assistant('stop', 'old answer', {}, 1000)}`);
    got = [];
    watcher = make();
    await watcher.start();
  });

  afterEach(() => {
    watcher.stop();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('ignores pre-existing rows and toolUse; notifies stop with §13 key and text', async () => {
    await watcher.poll(file);
    await sleep(50);
    expect(got).toEqual([]);
    fs.appendFileSync(file, row('user') + assistant('toolUse', 'calling', { content: [{ type: 'toolCall', name: 'bash' }] }));
    fs.appendFileSync(file, `${JSON.stringify({ type: 'message', message: { role: 'toolResult', toolCallId: 'c1', toolName: 'bash', content: 'ok', timestamp: Date.now() } })}\n`);
    await watcher.poll(file);
    await sleep(50);
    expect(got).toEqual([]);
    const ts = Date.now() + 999;
    fs.appendFileSync(file, assistant('stop', 'Final answer\nmore', {}, ts));
    await watcher.poll(file);
    await sleep(60);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ kind: 'stop', key: `assistant:${ts}`, text: 'Final answer\nmore' });
  });

  it('error and aborted turns notify (separate windows)', async () => {
    fs.appendFileSync(file, assistant('error', '', { errorMessage: 'boom' }));
    await watcher.poll(file);
    await sleep(60);
    fs.appendFileSync(file, assistant('aborted', 'half'));
    await watcher.poll(file);
    await sleep(60);
    fs.appendFileSync(file, assistant('length', 'cut off'));
    await watcher.poll(file);
    await sleep(60);
    expect(got.map((t) => t.kind)).toEqual(['error', 'aborted', 'stop']);
    expect(got[0]!.errorMessage).toBe('boom');
  });

  it('keys continue the file count (seeded at the start offset): duplicate base gets #2', async () => {
    fs.appendFileSync(file, assistant('stop', 'again', {}, 1000)); // same base key as the pre-existing row
    await watcher.poll(file);
    await sleep(60);
    expect(got.map((t) => t.key)).toEqual(['assistant:1000#2']);
  });

  it('a partial line notifies once, when complete', async () => {
    const line = assistant('stop', 'split');
    fs.appendFileSync(file, line.slice(0, 30));
    await watcher.poll(file);
    fs.appendFileSync(file, line.slice(30));
    await watcher.poll(file);
    await sleep(60);
    expect(got).toHaveLength(1);
  });

  it('debounce: several turn ends within the window → only the last', async () => {
    watcher.stop();
    watcher = make(null, 150);
    await watcher.start();
    fs.appendFileSync(file, assistant('stop', 'one'));
    await watcher.poll(file);
    await sleep(40);
    fs.appendFileSync(file, assistant('stop', 'two'));
    await watcher.poll(file);
    await sleep(40);
    fs.appendFileSync(file, assistant('stop', 'three'));
    await watcher.poll(file);
    expect(got).toEqual([]);
    await sleep(250);
    expect(got.map((t) => t.text)).toEqual(['three']);
  });

  it('recursive fs.watch picks up appends without an explicit poll', async () => {
    watcher.stop();
    state = new NotifyState(null);
    watcher = new TurnWatcher({ sessionDir: dir, state, onTurnEnd: (t) => { got.push(t); }, debounceMs: 20, rescanMs: 0, pollDelayMs: 20 });
    await watcher.start();
    fs.appendFileSync(file, assistant('stop', 'watched'));
    for (let i = 0; i < 100 && !got.length; i++) await sleep(20);
    expect(got.map((t) => t.text)).toEqual(['watched']);
  });

  it('new files found by rescan are read from 0 (recent rows only)', async () => {
    const f2 = path.join(dir, '--proj--', 'new.jsonl');
    fs.writeFileSync(f2, `{"type":"session","id":"N"}\n${assistant('stop', 'copied from a fork', {}, Date.now() - 3600_000)}${assistant('stop', 'fresh')}`);
    await watcher.rescan();
    await watcher.poll(f2);
    await sleep(60);
    expect(got.map((t) => t.text)).toEqual(['fresh']);
    // Dot dirs (server data) are never watched.
    fs.mkdirSync(path.join(dir, '.remote-pi'));
    fs.writeFileSync(path.join(dir, '.remote-pi', 'x.jsonl'), assistant('stop', 'nope'));
    await watcher.rescan();
    await sleep(60);
    expect(got).toHaveLength(1);
  });

  it('dedupe persists across restart (rewrite after restart does not re-notify)', async () => {
    watcher.stop();
    const stateFile = path.join(dir, '.remote-pi', 'notify-state.json');
    watcher = make(stateFile);
    await watcher.start();
    const future = Date.now() + 60_000; // stamped "after" any watch start
    const k1 = assistant('stop', 'first', {}, future);
    fs.appendFileSync(file, k1);
    await watcher.poll(file);
    await sleep(60);
    expect(got.map((t) => t.text)).toEqual(['first']);
    watcher.stop();
    state.saveNow();
    expect(JSON.stringify(JSON.parse(fs.readFileSync(stateFile, 'utf8')))).toContain(`assistant:${future}`);

    // "Restart": a new watcher with the persisted state.
    got = [];
    watcher = make(stateFile);
    await watcher.start();
    // pi rewrites the file (shrink observed first) with the same rows + one new turn end.
    fs.truncateSync(file, 10);
    await watcher.poll(file);
    fs.writeFileSync(file, `{"type":"session","id":"S"}\n${k1}${assistant('stop', 'second', {}, future + 1)}`);
    await watcher.poll(file);
    await sleep(60);
    expect(got.map((t) => t.text)).toEqual(['second']);
  });
});

/* ---------------- Bark client (local stub) ---------------- */

interface Stub { url: string; hits: { body: Record<string, string>; path: string }[]; close: () => Promise<void> }

async function stubServer(handler: (n: number, res: http.ServerResponse) => void): Promise<Stub> {
  const hits: Stub['hits'] = [];
  const server = http.createServer((req, res) => {
    let data = '';
    req.on('data', (c) => { data += c; });
    req.on('end', () => {
      hits.push({ body: data ? JSON.parse(data) as Record<string, string> : {}, path: req.url ?? '' });
      handler(hits.length, res);
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}`,
    hits,
    close: () => new Promise((r) => { server.closeAllConnections(); server.close(() => r()); }),
  };
}

const okJson = (res: http.ServerResponse): void => {
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ code: 200, message: 'success', timestamp: 1 }));
};

describe('sendBark', () => {
  it('POSTs JSON to <server>/push', async () => {
    const s = await stubServer((_n, res) => okJson(res));
    const r = await sendBark(`${s.url}/`, { device_key: 'K', ciphertext: 'c' });
    expect(r).toEqual({ ok: true, status: 200 });
    expect(s.hits[0]).toEqual({ path: '/push', body: { device_key: 'K', ciphertext: 'c' } });
    await s.close();
  });

  it('retries once on 5xx, never on 4xx', async () => {
    const s = await stubServer((n, res) => { if (n === 1) { res.writeHead(503); res.end(); } else okJson(res); });
    expect(await sendBark(s.url, { device_key: 'K' }, { retryDelayMs: 5 })).toEqual({ ok: true, status: 200 });
    expect(s.hits).toHaveLength(2);
    await s.close();
    const bad = await stubServer((_n, res) => { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ code: 400, message: 'failed to get device token' })); });
    expect(await sendBark(bad.url, { device_key: 'K' }, { retryDelayMs: 5 })).toEqual({ ok: false, status: 400, error: 'failed to get device token' });
    expect(bad.hits).toHaveLength(1);
    await bad.close();
    const always = await stubServer((_n, res) => { res.writeHead(500); res.end(); });
    const r = await sendBark(always.url, { device_key: 'K' }, { retryDelayMs: 5 });
    expect(r.ok).toBe(false);
    expect(always.hits).toHaveLength(2); // exactly one retry
    await always.close();
  });

  it('timeout → status 0, one retry, no secrets in the error', async () => {
    const s = await stubServer(() => { /* never answers */ });
    const r = await sendBark(s.url, { device_key: 'SECRETKEY' }, { timeoutMs: 100, retryDelayMs: 5 });
    expect(r).toMatchObject({ ok: false, status: 0, error: 'timeout' });
    expect(s.hits).toHaveLength(2);
    await s.close();
    const refused = await sendBark('http://127.0.0.1:1', { device_key: 'SECRETKEY' }, { timeoutMs: 1000, retryDelayMs: 5 });
    expect(refused.ok).toBe(false);
    expect(JSON.stringify(refused)).not.toContain('SECRETKEY');
  });
});

/* ---------------- settings + routes + end-to-end ---------------- */

describe('settings', () => {
  it('defaults: disabled until a device key; encryption follows the key; validation', () => {
    const d = defaultSettings();
    expect(publicSettings(d)).toEqual({
      enabled: false, barkServer: 'https://api.day.app', hasDeviceKey: false,
      encryption: { enabled: false, algorithm: 'AES128', mode: 'CBC', hasKey: false }, level: 'active',
    });
    const s = applyPut(d, { deviceKey: 'abc', encryption: { key: '1234567890123456' } });
    expect(publicSettings(s)).toMatchObject({ enabled: true, hasDeviceKey: true, encryption: { enabled: true, hasKey: true } });
    expect(() => applyPut(s, { encryption: { algorithm: 'AES256' } })).toThrow(/32 characters/);
    expect(() => applyPut(s, { encryption: { mode: 'GCM', iv: '1234567890123456' } })).toThrow(/12 characters/);
    expect(publicSettings(applyPut(s, { deviceKey: null })).enabled).toBe(false);
  });
});

describe('notify routes + service', () => {
  let root: string;
  let app: FastifyInstance;
  let manager: SessionManager;
  let notify: NotifyService;
  let stub: Stub;
  let sfile: string;
  const ID = '0199bbbb-cccc-7ddd-8eee-ffff00000009';
  const auth = { authorization: 'Bearer tok' };
  const DEVICE = 'DeviceKeySecret123';
  const KEY = 'KeyKeyKeyKeyKeyKeyKeyKeyKeyKey32';

  beforeEach(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-notify-'));
    fs.mkdirSync(path.join(root, '--proj--'));
    sfile = path.join(root, '--proj--', 's.jsonl');
    fs.writeFileSync(sfile, `${JSON.stringify({ type: 'session', id: ID, cwd: root })}\n${JSON.stringify({ type: 'session_info', name: 'My Session' })}\n${row('user')}`);
    manager = new SessionManager({ sessionDir: root, workdir: root, bin: 'pi', maxAgents: 2, idleKillMs: 60_000, extraArgs: [], log, noTimers: true });
    stub = await stubServer((_n, res) => okJson(res));
    notify = new NotifyService({
      dataDir: path.join(root, '.remote-pi'), sessionDir: root, log,
      resolveSession: (f) => manager.describeFile(f),
      keySource: (f) => manager.files.keySnapshot(f),
      bark: { retryDelayMs: 5 },
      watcher: { debounceMs: 20, rescanMs: 0, noFsWatch: true },
    });
    await notify.start();
    app = Fastify();
    registerRoutes(app, { config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: root }), manager, piVersion: 'test', log: log as unknown as FastifyBaseLogger, notify });
    await app.ready();
  });

  afterEach(async () => {
    notify.stop();
    manager.stopAll();
    await app.close();
    await stub.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it('requires auth; GET/PUT never return secrets; secrets persisted to the data dir', async () => {
    expect((await app.inject({ url: '/api/notify' })).statusCode).toBe(401);
    const put = await app.inject({
      method: 'PUT', url: '/api/notify', headers: auth,
      payload: { barkServer: stub.url, deviceKey: DEVICE, encryption: { algorithm: 'aes-256', mode: 'cbc', key: KEY, iv: 'IvIvIvIvIvIvIvIv' }, level: 'timeSensitive' },
    });
    expect(put.statusCode).toBe(200);
    expect(put.json()).toEqual({
      enabled: true, barkServer: stub.url, hasDeviceKey: true,
      encryption: { enabled: true, algorithm: 'AES256', mode: 'CBC', hasKey: true }, level: 'timeSensitive',
    });
    const get = await app.inject({ url: '/api/notify', headers: auth });
    for (const res of [put, get]) {
      expect(res.body).not.toContain(DEVICE);
      expect(res.body).not.toContain(KEY);
      expect(res.body).not.toContain('IvIvIvIv');
    }
    const stored = fs.readFileSync(path.join(root, '.remote-pi', 'notify.json'), 'utf8');
    expect(stored).toContain(DEVICE);
    // Bad values are rejected without echoing them.
    const bad = await app.inject({ method: 'PUT', url: '/api/notify', headers: auth, payload: { encryption: { key: 'short-secret-value' } } });
    expect(bad.statusCode).toBe(400);
    expect(bad.body).not.toContain('short-secret-value');
    const bad2 = await app.inject({ method: 'PUT', url: '/api/notify', headers: auth, payload: { deviceKey: 'has spaces secret' } });
    expect(bad2.statusCode).toBe(400);
    expect(bad2.body).not.toContain('has spaces secret');
    const unknown = await app.inject({ method: 'PUT', url: '/api/notify', headers: auth, payload: { level: 'loud' } });
    expect(unknown.statusCode).toBe(400);
  });

  it('POST /api/notify/test sends an encrypted push; without a device key → ok:false', async () => {
    const none = await app.inject({ method: 'POST', url: '/api/notify/test', headers: auth });
    expect(none.json()).toEqual({ ok: false, status: 0, error: 'device key not set' });
    await app.inject({ method: 'PUT', url: '/api/notify', headers: auth, payload: { barkServer: stub.url, deviceKey: DEVICE, encryption: { algorithm: 'AES256', mode: 'GCM', key: KEY } } });
    const res = await app.inject({ method: 'POST', url: '/api/notify/test', headers: auth });
    expect(res.json()).toEqual({ ok: true, status: 200 });
    const hit = stub.hits[0]!.body;
    expect(Object.keys(hit).sort()).toEqual(['ciphertext', 'device_key', 'iv', 'level']);
    const plain = JSON.parse(barkDecrypt(hit.ciphertext!, { algorithm: 'AES256', mode: 'GCM', key: KEY }, hit.iv!)) as Record<string, string>;
    expect(plain.title).toBe('Remote Pi');
  });

  it('end-to-end: appended turn end → one encrypted Bark push with session name/body/url', async () => {
    await app.inject({ method: 'PUT', url: '/api/notify', headers: auth, payload: { barkServer: stub.url, deviceKey: DEVICE, encryption: { key: '1234567890123456' } } });
    fs.appendFileSync(sfile, assistant('toolUse', '', { content: [{ type: 'toolCall', name: 'bash' }] }) + assistant('stop', '\nThe build passes.\nDetails…'));
    await notify.watcher.poll(sfile);
    await sleep(60);
    await notify.idle();
    expect(stub.hits).toHaveLength(1);
    const hit = stub.hits[0]!.body;
    expect(hit.device_key).toBe(DEVICE);
    expect(JSON.stringify(hit)).not.toContain('build passes');
    const plain = JSON.parse(barkDecrypt(hit.ciphertext!, { algorithm: 'AES128', mode: 'CBC', key: '1234567890123456' }, hit.iv!)) as Record<string, string>;
    expect(plain).toEqual({ title: 'My Session', body: 'The build passes.', group: ID, url: `remotepi://session/${ID}`, level: 'active' });
  });

  it('disabled → turn ends are consumed but nothing is sent', async () => {
    await app.inject({ method: 'PUT', url: '/api/notify', headers: auth, payload: { enabled: false, barkServer: stub.url, deviceKey: DEVICE } });
    fs.appendFileSync(sfile, assistant('stop', 'x'));
    await notify.watcher.poll(sfile);
    await sleep(60);
    await notify.idle();
    expect(stub.hits).toHaveLength(0);
  });
});
