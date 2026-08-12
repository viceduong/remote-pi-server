/**
 * Server-owned prompt queue — durable outbox.
 *
 * Queued user prompts live on the SERVER (persisted to disk next to the
 * sessions), dispatched as direct prompts the moment the agent goes idle,
 * and surfaced to clients via /queue + queue_update SSE events. Messages
 * can never vanish on reload and never double-send.
 */
import path from 'node:path';

export type QueueStatus = 'queued' | 'running' | 'done' | 'failed';

export interface QueueItem {
  id: string;
  message: string;
  status: QueueStatus;
  queuedAt: number;
  startedAt: number | null;
  completedAt: number | null;
  error: string | null;
}

/**
 * Resolve the queue file path for a session. Keyed on the session FILE path
 * (stable across restarts) — the manager's session id is ephemeral and can
 * change when pi re-identifies the session from disk.
 */
export function queueFilePath(sessionDir: string, sessionFile: string): string {
  const base = path.basename(sessionFile).replace(/\.jsonl$/, '');
  return path.join(sessionDir, '.queue', `${base}.json`);
}
