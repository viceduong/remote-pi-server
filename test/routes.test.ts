import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify';
import { pino } from 'pino';
import { SessionManager } from '../src/manager.js';
import { registerRoutes } from '../src/routes.js';
import { rateLimitKey } from '../src/ratelimit.js';
import { loadConfig } from '../src/config.js';

const log = pino({ enabled: false });
const ID = '0199bbbb-cccc-7ddd-8eee-ffff00000001';

let root: string;
let app: FastifyInstance;
let manager: SessionManager;
let base: string;

beforeEach(async () => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'rpi-routes-'));
  const dir = path.join(root, '--proj--');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 's.jsonl'), [
    JSON.stringify({ type: 'session', id: ID, cwd: root }),
    JSON.stringify({ type: 'message', id: 'u1', message: { role: 'user', content: 'hello', timestamp: 1 } }),
    JSON.stringify({ type: 'message', id: 'a1', message: { role: 'assistant', content: [{ type: 'thinking', thinking: 'hm' }], timestamp: 2 } }),
    JSON.stringify({ type: 'message', id: 't1', message: { role: 'toolResult', toolName: 'bash', content: 'out', timestamp: 3 } }),
    JSON.stringify({ type: 'message', id: 'a2', message: { role: 'assistant', content: [{ type: 'text', text: 'done' }], timestamp: 4 } }),
    '',
  ].join('\n'));
  manager = new SessionManager({
    sessionDir: root, workdir: root, bin: 'pi', maxAgents: 2, idleKillMs: 60_000, extraArgs: [], log, noTimers: true,
  });
  app = Fastify();
  registerRoutes(app, {
    config: loadConfig({ REMOTE_PI_TOKEN: 'tok', REMOTE_PI_WORKDIR: root }),
    manager,
    piVersion: 'test',
    log: log as unknown as FastifyBaseLogger,
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const addr = app.server.address() as { port: number };
  base = `http://127.0.0.1:${addr.port}`;
});

afterEach(async () => {
  manager.stopAll();
  await app.close();
  fs.rmSync(root, { recursive: true, force: true });
});

const auth = { authorization: 'Bearer tok' };

describe('routes', () => {
  it('?visible=1 applies the single visibility rule', async () => {
    const res = await app.inject({ url: `/api/sessions/${ID}/messages?visible=1`, headers: auth });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { messages: { id: string }[]; pending: unknown[]; total: number };
    expect(body.messages.map((m) => m.id)).toEqual(['u1', 'a2']);
    expect(body.total).toBe(4);
    expect(body.pending).toEqual([]);
  });

  it('validates queries with zod (400 on garbage)', async () => {
    const res = await app.inject({ url: `/api/sessions/${ID}/messages?limit=abc`, headers: auth });
    expect(res.statusCode).toBe(400);
    const list = await app.inject({ url: '/api/sessions?limit=0', headers: auth });
    expect(list.statusCode).toBe(400);
  });

  it('refuses purge of a TUI-owned session even with force', async () => {
    const file = path.join(root, '--proj--', 's.jsonl');
    fs.writeFileSync(`${file}.lease`, JSON.stringify({
      v: 1, sessionId: ID, epoch: 1, holder: { pid: process.pid, nonce: 'n', startedAtMs: 0 },
      ipc: 'x', acquiredAt: Date.now(), renewedAt: Date.now(), leaseDurationMs: 15000, kind: 'tui',
    }));
    const res = await app.inject({ method: 'DELETE', url: `/api/sessions/${ID}?purge=1&force=1`, headers: auth });
    expect(res.statusCode).toBe(409);
    expect(res.json()).toMatchObject({ code: 'session_live' });
    expect(fs.existsSync(file)).toBe(true);
  });

  it('SSE: an unknown Last-Event-ID yields one reset frame, then a status snapshot', async () => {
    const text = await new Promise<string>((resolve, reject) => {
      const req = http.get(`${base}/api/sessions/${ID}/events?skeleton=1`, {
        headers: { ...auth, 'last-event-id': 'stale.9-123', 'accept-encoding': 'gzip' },
      }, (res) => {
        expect(res.headers['content-type']).toMatch(/text\/event-stream/);
        const gunzip = zlib.createGunzip();
        let acc = '';
        res.pipe(gunzip);
        gunzip.on('data', (d: Buffer) => {
          acc += d.toString('utf8');
          if (acc.includes('event: queue_update')) {
            req.destroy();
            resolve(acc);
          }
        });
        gunzip.on('error', reject);
      });
      req.on('error', () => undefined);
      setTimeout(() => reject(new Error('timeout')), 5000);
    });
    expect(text).toMatch(/^retry: \d+\n/);
    const events = [...text.matchAll(/event: (\S+)\nid: (\S+)\ndata: (.*)\n/g)].map((m) => ({ event: m[1], id: m[2], data: JSON.parse(m[3]!) }));
    expect(events.map((e) => e.event)).toEqual(['reset', 'agent_status', 'queue_update']);
    expect(events[0]!.data).toEqual({ type: 'reset', reason: 'epoch' });
    expect(events[0]!.id).toMatch(/^[0-9a-z]+\.[0-9a-z]+-\d+$/);
    expect(events[1]!.data).toMatchObject({ type: 'agent_status', phase: 'idle', busy: false, owner: 'none' });
    expect(events[2]!.data).toEqual({ type: 'queue_update', items: [] });
  });
});

describe('rate-limit key (P4)', () => {
  it('uses CF-Connecting-IP behind cloudflared and separates tokens', () => {
    const a = rateLimitKey({ ip: '127.0.0.1', headers: { 'cf-connecting-ip': '1.2.3.4', authorization: 'Bearer a' } } as never);
    const b = rateLimitKey({ ip: '127.0.0.1', headers: { 'cf-connecting-ip': '5.6.7.8', authorization: 'Bearer a' } } as never);
    const c = rateLimitKey({ ip: '127.0.0.1', headers: { 'cf-connecting-ip': '1.2.3.4', authorization: 'Bearer b' } } as never);
    expect(a).not.toBe(b);
    expect(a).not.toBe(c);
    expect(a.endsWith(':1.2.3.4')).toBe(true);
    expect(rateLimitKey({ ip: '9.9.9.9', headers: {} } as never)).toBe('anon:9.9.9.9');
  });
});

describe('spawn failure surfacing', () => {
  it('create() rejects with a SpawnError when pi cannot boot', async () => {
    const m = new SessionManager({
      sessionDir: root, workdir: root, bin: 'pi-definitely-not-installed-xyz', maxAgents: 2, idleKillMs: 60_000,
      extraArgs: [], log, noTimers: true,
    });
    await expect(m.create('boom')).rejects.toThrow(/failed to start|exited before/);
    m.stopAll();
  }, 40_000);
});
