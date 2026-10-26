import Fastify, { type FastifyBaseLogger, type FastifyError } from 'fastify';
import compress from '@fastify/compress';
import helmet from '@fastify/helmet';
import rateLimit from '@fastify/rate-limit';
import { defaultSessionDir, loadConfig } from './config.js';
import { logger } from './logger.js';
import { SessionManager } from './manager.js';
import { detectPiVersion } from './pi.js';
import { registerRoutes } from './routes.js';
import { rateLimitKey } from './ratelimit.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const piVersion = await detectPiVersion(config.PI_BIN);

  const sessionDir = config.REMOTE_PI_SESSION_DIR ?? defaultSessionDir();

  const manager = new SessionManager({
    sessionDir,
    workdir: config.REMOTE_PI_WORKDIR,
    bin: config.PI_BIN,
    maxAgents: config.REMOTE_PI_MAX_AGENTS,
    idleKillMs: config.REMOTE_PI_IDLE_KILL_MS,
    extraArgs: config.REMOTE_PI_EXTRA_ARGS.split(/\s+/).filter(Boolean),
    log: logger,
  });

  const app = Fastify({
    logger: false, // request logs off; app errors go through our pino logger
    bodyLimit: 1_048_576,
    // cloudflared / local reverse proxies connect from loopback: trust only
    // those hops for X-Forwarded-For so req.ip is the real client.
    trustProxy: ['127.0.0.1', '::1'],
  });

  // gzip for JSON (history payloads are multi-MB). SSE does its own per-frame
  // flushed gzip and is hijacked, so this never buffers event frames.
  await app.register(compress, { threshold: 1024 });
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(rateLimit, {
    max: 300, // per token+client per minute (SSE connections are long-lived)
    timeWindow: '1 minute',
    keyGenerator: rateLimitKey,
  });

  app.setErrorHandler((err: FastifyError, req, reply) => {
    logger.error({ err: err.message, url: req.url }, 'request error');
    void reply.code(err.statusCode ?? 500).send({ error: err.message ?? 'Internal error' });
  });

  registerRoutes(app, { config, manager, piVersion, log: logger as unknown as FastifyBaseLogger });

  await app.listen({ host: config.REMOTE_PI_HOST, port: config.REMOTE_PI_PORT });

  // Memory + sink telemetry (growth curve proves a leak or a fix).
  const memTimer = setInterval(() => {
    const mem = process.memoryUsage();
    const sinks = manager.sinkCounts();
    logger.info({
      heapMB: Math.round(mem.heapUsed / 1048576),
      rssMB: Math.round(mem.rss / 1048576),
      sinkTotal: Object.values(sinks).reduce((a, b) => a + b, 0),
      sessions: Object.keys(sinks).length,
      runningAgents: manager.runningCount,
      historyCacheMB: Math.round(manager.files.cachedMessageBytes / 1048576),
    }, 'telemetry');
  }, 5 * 60_000);
  memTimer.unref();

  logger.info({
    pi: piVersion ?? 'NOT FOUND (check PI_BIN / PATH)',
    url: `http://${config.REMOTE_PI_HOST}:${config.REMOTE_PI_PORT}`,
    workdir: config.REMOTE_PI_WORKDIR,
    sessions: sessionDir,
    maxAgents: config.REMOTE_PI_MAX_AGENTS,
    auth: config.REMOTE_PI_TOKEN ? 'Bearer token' : 'DISABLED (open LAN)',
  }, 'Remote Pi server started');

  // Graceful shutdown: drain SSE, stop children (their close handlers release
  // bridge leases), close the HTTP server, exit. NSSM stops the Windows
  // service with Ctrl+C (SIGINT) / Ctrl+Break (SIGBREAK); systemd uses SIGTERM.
  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');
    const hardExit = setTimeout(() => process.exit(1), 10_000);
    hardExit.unref();
    try {
      await manager.shutdown(5_000);
      await app.close();
    } catch (err) {
      logger.error({ err: (err as Error).message }, 'shutdown error');
    }
    process.exit(0);
  };
  for (const sig of ['SIGINT', 'SIGTERM', 'SIGBREAK', 'SIGHUP'] as const) {
    process.on(sig, () => void shutdown(sig));
  }
}

main().catch((err) => {
  logger.error({ err: (err as Error).message }, 'fatal');
  process.exit(1);
});
