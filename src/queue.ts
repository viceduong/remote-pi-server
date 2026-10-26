/**
 * Server-owned prompt queue — durable outbox.
 *
 * Queued user prompts live on the SERVER (persisted next to the sessions),
 * are dispatched when the agent is ready, and are surfaced to clients via
 * /queue, `pending` in /messages and `queue_update` SSE frames.
 *
 * CONTRACT §2: clients only ever see `queued`/`running` items; `done`/`failed`
 * items are kept for 1 h (idempotency for repeated clientMessageIds) and then
 * pruned. Delivery dedupe is ONLY by clientMessageId + the `deliveredAt`
 * marker — never by searching the session for identical text (that dropped
 * legit repeats like "continue").
 */
import path from 'node:path';
import crypto from 'node:crypto';

export type QueueStatus = 'queued' | 'running' | 'done' | 'failed';

export interface QueueItem {
  id: string;
  /** Client-generated idempotency key; optional for legacy API clients. */
  clientMessageId?: string;
  message: string;
  status: QueueStatus;
  queuedAt: number;
  startedAt: number | null;
  completedAt: number | null;
  error: string | null;
  /** Dispatch attempts so far (backoff + give-up). */
  attempts?: number;
  /** Earliest time the next dispatch attempt may run. */
  nextAttemptAt?: number;
  /** Set once the agent ACCEPTED the prompt — a crash after this must not re-send. */
  deliveredAt?: number;
  /** Timestamp of the user message pi wrote for this item (clientMessageId echo). */
  messageTimestamp?: number;
}

/** Finished items are retained this long, then pruned from the durable file. */
export const QUEUE_RETENTION_MS = 60 * 60_000;
export const QUEUE_MAX_ACTIVE = 50;
export const QUEUE_MAX_ATTEMPTS = 6;
export const QUEUE_BACKOFF_CAP_MS = 30_000;

export function isActive(item: QueueItem): boolean {
  return item.status === 'queued' || item.status === 'running';
}

/** Wire view (CONTRACT §2): only queued/running, with status + clientMessageId. */
export interface PendingItemView {
  id: string;
  clientMessageId?: string;
  message: string;
  status: 'queued' | 'running';
  queuedAt: number;
  startedAt: number | null;
  completedAt: null;
  error: string | null;
  attempts: number;
}

export function pendingView(items: readonly QueueItem[]): PendingItemView[] {
  return items.filter(isActive).map((i) => ({
    id: i.id,
    ...(i.clientMessageId ? { clientMessageId: i.clientMessageId } : {}),
    message: i.message,
    status: i.status as 'queued' | 'running',
    queuedAt: i.queuedAt,
    startedAt: i.startedAt,
    completedAt: null,
    error: i.error,
    attempts: i.attempts ?? 0,
  }));
}

/** Drop done/failed items older than the retention window. */
export function pruneQueue(items: QueueItem[], now = Date.now()): QueueItem[] {
  const cutoff = now - QUEUE_RETENTION_MS;
  return items.filter((i) => isActive(i) || (i.completedAt ?? i.queuedAt) > cutoff);
}

/**
 * Backoff before attempt `attempts + 1`: exponential (1 s, 2 s, 4 s … capped
 * at 30 s) with "equal jitter" — half fixed, half random — so many sessions
 * retrying after a shared failure do not stampede in lockstep.
 */
export function backoffMs(attempts: number, random: () => number = Math.random): number {
  const base = Math.min(QUEUE_BACKOFF_CAP_MS, 1000 * 2 ** Math.max(0, attempts - 1));
  return Math.round(base / 2 + random() * (base / 2));
}

/**
 * Normalize items loaded from disk after a restart: a `running` item that the
 * agent had already accepted is done (re-sending would duplicate it); one that
 * was never accepted goes back to `queued`.
 */
export function restoreItems(raw: unknown, now = Date.now()): QueueItem[] {
  if (!Array.isArray(raw)) return [];
  const items = (raw as QueueItem[])
    .filter((i) => i && typeof i.id === 'string' && typeof i.message === 'string')
    .map((i): QueueItem => {
      if (i.status !== 'running') return i;
      return i.deliveredAt
        ? { ...i, status: 'done', completedAt: i.completedAt ?? now }
        : { ...i, status: 'queued', startedAt: null };
    });
  return pruneQueue(items, now);
}

export function newQueueId(): string {
  return crypto.randomUUID().replace(/-/g, '').slice(0, 16);
}

/**
 * Resolve the queue file path for a session. Keyed on the session FILE path
 * (stable across restarts and id re-keys).
 */
export function queueFilePath(sessionDir: string, sessionFile: string): string {
  const base = path.basename(sessionFile).replace(/\.jsonl$/, '');
  const key = crypto.createHash('sha256').update(path.resolve(sessionFile)).digest('hex').slice(0, 16);
  return path.join(sessionDir, '.queue', `${base}-${key}.json`);
}
