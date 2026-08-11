import { pino } from 'pino';

/**
 * Structured JSON logger. Logs go to stdout; JSONL RPC traffic never touches
 * the logger, so machine-readable output stays clean.
 */
export const logger = pino({
  name: 'remote-pi-server',
  level: process.env.LOG_LEVEL ?? 'info',
  base: null,
  timestamp: pino.stdTimeFunctions.isoTime,
});
