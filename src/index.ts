import Fastify, { type FastifyBaseLogger, type FastifyError } from 'fastify';
import compress from '@fastify/compress';
import helmet from '@fastify/helmet';
import rateLimit from '@fastify/rate-limit';
import { defaultSessionDir, loadConfig } from './config.js';
import { logger } from './logger.js';
import { SessionManager } from './manager.js';
import { detectPiVersion } from './pi.js';
import { registerRoutes } from './routes.js';

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
  });

  // gzip: session history payloads are multi-MB JSON — compression cuts
  // transfer ~10x (URLSession decompresses transparently).
  await app.register(compress, { threshold: 1024 });
  // Industry-standard hardening for an internet-exposed token service.
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(rateLimit, {
    max: 300,        // per IP per minute (SSE connections are long-lived)
    timeWindow: '1 minute',
    keyGenerator: (req) => req.ip,
  });

  app.setErrorHandler((err: FastifyError, req, reply) => {
    logger.error({ err: err.message, url: req.url }, 'request error');
    reply.code(err.statusCode ?? 500).send({ error: err.message ?? 'Internal error' });
  });

  registerRoutes(app, { config, manager, piVersion, log: logger as unknown as FastifyBaseLogger });

  await app.listen({ host: config.REMOTE_PI_HOST, port: config.REMOTE_PI_PORT });

  logger.info({
    pi: piVersion ?? 'NOT FOUND (check PI_BIN / PATH)',
    url: `http://${config.REMOTE_PI_HOST}:${config.REMOTE_PI_PORT}`,
    workdir: config.REMOTE_PI_WORKDIR,
    sessions: sessionDir,
    maxAgents: config.REMOTE_PI_MAX_AGENTS,
    auth: config.REMOTE_PI_TOKEN ? 'Bearer token' : 'DISABLED (open LAN)',
  }, 'Remote Pi server started');

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'shutting down');
    manager.stopAll();
    await app.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  logger.error({ err: (err as Error).message }, 'fatal');
  process.exit(1);
});
