import { z } from 'zod';
import path from 'node:path';

/**
 * Environment configuration, validated at boot with zod.
 * Fails fast on invalid values instead of misbehaving at runtime.
 */
const EnvSchema = z.object({
  REMOTE_PI_PORT: z.coerce.number().int().min(1).max(65535).default(8787),
  // Bind loopback by default. Network exposure requires explicit host + token.
  REMOTE_PI_HOST: z.string().min(1).default('127.0.0.1'),
  /** Empty string is allowed only for loopback development. */
  REMOTE_PI_TOKEN: z.string().default(''),
  REMOTE_PI_ALLOW_ANONYMOUS: z.string().default('0').transform((v) => v === '1' || v.toLowerCase() === 'true'),
  REMOTE_PI_NAME: z.string().min(1).optional(),
  REMOTE_PI_WORKDIR: z.string().min(1).default(process.cwd()),
  REMOTE_PI_SESSION_DIR: z.string().min(1).optional(),
  REMOTE_PI_MAX_AGENTS: z.coerce.number().int().min(1).max(16).default(4),
  REMOTE_PI_IDLE_KILL_MS: z.coerce.number().int().min(60_000).default(45 * 60_000),
  REMOTE_PI_EXTRA_ARGS: z.string().default(''),
  PI_BIN: z.string().min(1).default('pi'),
});

export type Env = z.infer<typeof EnvSchema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Env {
  const parsed = EnvSchema.safeParse(env);
  if (!parsed.success) {
    const details = parsed.error.issues
      .map((i) => `  ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid configuration:\n${details}`);
  }
  const config = parsed.data;
  const loopback = config.REMOTE_PI_HOST === '127.0.0.1'
    || config.REMOTE_PI_HOST === 'localhost'
    || config.REMOTE_PI_HOST === '::1';
  if (config.REMOTE_PI_TOKEN === 'CHANGE_ME') {
    throw new Error('REMOTE_PI_TOKEN must be replaced; refusing placeholder secret');
  }
  if (!loopback && !config.REMOTE_PI_TOKEN && !config.REMOTE_PI_ALLOW_ANONYMOUS) {
    throw new Error('REMOTE_PI_TOKEN is required when REMOTE_PI_HOST is not loopback');
  }
  return config;
}

/** pi's real default session storage (~/.pi/agent/sessions). */
export function defaultSessionDir(env: NodeJS.ProcessEnv = process.env): string {
  const home = env.HOME ?? env.USERPROFILE;
  if (!home) return path.resolve('.pi/agent/sessions');
  return path.join(home, '.pi', 'agent', 'sessions');
}
