import { once } from 'node:events';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import type { ChildProcess } from 'node:child_process';
import type { Logger } from 'pino';
import { killPiProcess, spawnPiProcess } from './pi.js';
import {
  acquireLease,
  releaseLease,
  renewLease,
  RENEW_INTERVAL_MS,
  type Lease,
} from './lease.js';
import { LineSplitter, contentText } from './jsonl.js';
import { FileTailer, type TailBatch } from './tailer.js';
import { ASK_USER_QUESTION_TOOL, qaFromToolResult } from './qa.js';
import type {
  Activity,
  ActivityKind,
  AgentState,
  AgentStatusFrame,
  RpcEvent,
  RpcResponse,
  SessionOwner,
  SessionPhase,
  StatusPhase,
} from './types.js';
import type { QueueItem } from './queue.js';

const EVENT_RING_CAPACITY = 500;
/** Ring is byte-capped too — 500 × 2MB payloads would OOM the server. */
const EVENT_RING_MAX_BYTES = 8 * 1024 * 1024;
const RPC_TIMEOUT_MS = 10_000;
/** Extension-heavy pi startups can take >10s before RPC answers. */
const READY_TIMEOUT_MS = 30_000;
const OWNER_CONNECT_TIMEOUT_MS = 3_000;
const OWNER_HELLO_TIMEOUT_MS = 4_000;
const MAX_RPC_LINE_BYTES = 16 * 1024 * 1024;
/** Crash restart policy: exponential backoff, give up after 5 crashes in 2 min. */
const CRASH_WINDOW_MS = 2 * 60_000;
const CRASH_LIMIT = 5;
const CRASH_BACKOFF_CAP_MS = 16_000;

/** Markers the ask_user_question extension uses when it must fall back to
 *  select() (pi's RPC UI adapter has no custom components). */
export const RPC_QUESTION_PREFIX = 'pi-questionnaire:';
const RPC_ANSWER_PREFIX = 'pi-answers:';

/* ---------------- SSE stream identity (CONTRACT §1) ---------------- */

const BOOT_EPOCH = Date.now().toString(36);
let epochGeneration = 0;

/** `<base36 boot time>.<generation>` — new on every (re)spawn/attach/re-key/fork. */
export function nextStreamEpoch(): string {
  epochGeneration += 1;
  return `${BOOT_EPOCH}.${epochGeneration.toString(36)}`;
}

/** One event record (ring entry or live-only frame). */
export interface RingRecord {
  /** Opaque SSE id `<epoch>-<seq>`. */
  id: string;
  seq: number;
  type: string;
  data: RpcEvent;
  /** Pre-serialized `data` (stringified exactly once). */
  json: string;
}

/** Destination for streamed events (SSE connection). */
export interface EventSink {
  send(record: RingRecord): void;
  close?(): void;
  /** Focus-mode (iOS) client: tool frames are dropped / truncated. */
  skeleton?: boolean;
}

interface PendingRpc {
  resolve: (data: unknown) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

/** A queued prompt awaiting its user-message echo (CONTRACT §3). */
interface PendingEcho {
  text: string;
  clientMessageId: string | undefined;
  onEcho: (timestamp: number | null) => void;
}

/** Result of answering a questionnaire (route maps status → HTTP code). */
export type AnswerResult = { ok: true } | { ok: false; status: 409 | 502; error: string };

/** Thrown when the bridge cannot take the ownership lease for a spawn. */
export class LeaseHeldError extends Error {
  readonly code = 409;
  constructor(readonly reason: string, readonly lease: Lease | null) {
    super(reason === 'unknown'
      ? 'Session ownership could not be determined (lease unreadable) — retry shortly'
      : 'Session is owned by another process (lease held)');
  }
}

/** CONTRACT §5/§7/§8: the one function computing client-visible status. */
export function computeStatus(s: {
  phase: SessionPhase;
  busy: boolean;
  promptReserved: boolean;
  error: string | null;
  owner: SessionOwner;
  /** §7 — the caller guarantees: non-null ⇔ busy || promptReserved. */
  activity?: Activity | null;
  questionId?: string | null;
}): AgentStatusFrame {
  let phase: StatusPhase;
  if (s.busy) phase = 'streaming';
  else if (s.promptReserved) phase = 'waiting';
  else if (s.error) phase = 'error';
  else if (s.phase === 'terminated') phase = 'exited';
  else phase = 'idle';
  const busy = phase === 'streaming' || phase === 'waiting';
  const frame: AgentStatusFrame = { type: 'agent_status', phase, busy, working: busy, owner: s.owner };
  if (s.error) frame.error = s.error;
  if (s.activity !== undefined) frame.activity = s.activity;
  if (s.questionId !== undefined) frame.questionId = s.questionId;
  return frame;
}

/** compaction_progress percent → integer 0-100 (undefined when unknown). */
function percentOf(v: unknown): number | undefined {
  if (typeof v !== 'number' || !Number.isFinite(v)) return undefined;
  return Math.max(0, Math.min(100, Math.round(v)));
}

/** Tool name announced by a toolcall_start (RPC: ev.toolName; else from the partial). */
function toolcallName(ev: Record<string, unknown>): string | undefined {
  if (typeof ev.toolName === 'string' && ev.toolName) return ev.toolName;
  const partial = ev.partial as { content?: { name?: unknown }[] } | undefined;
  const idx = typeof ev.contentIndex === 'number' ? ev.contentIndex : -1;
  const block = Array.isArray(partial?.content) ? partial!.content[idx] : undefined;
  return typeof block?.name === 'string' && block.name ? block.name : undefined;
}

/**
 * Bandwidth trim for streamed events. pi's (and the owner extension's)
 * message_update can carry the growing partial message — quadratic bytes.
 * Ship the delta only: keep `message.role` for routing, drop
 * `assistantMessageEvent.partial` and toolcall_delta payloads. `done`/`error`
 * keep the final message (clients finalize the bubble from it).
 */
export function trimEventForWire(obj: RpcEvent): RpcEvent {
  if (obj.type === 'message_start' || obj.type === 'message_end') {
    // CONTRACT §9: Q&A rows carry `qa` (top level + on the message copy).
    const qa = qaFromToolResult(obj.message);
    if (qa) return { ...obj, qa, message: { ...(obj.message as Record<string, unknown>), qa } };
    return obj;
  }
  if (obj.type === 'session_before_compact') {
    // v4 owners forward this raw — never ship preparation/branchEntries (huge).
    return { type: obj.type, reason: obj.reason, willRetry: obj.willRetry };
  }
  if (obj.type !== 'message_update') return obj;
  const ev = obj.assistantMessageEvent as Record<string, unknown> | undefined;
  const evType = typeof ev?.type === 'string' ? ev.type : '';
  const out: RpcEvent = { ...obj };
  if (evType !== 'done' && evType !== 'error' && obj.message && typeof obj.message === 'object') {
    out.message = { role: (obj.message as { role?: string }).role };
  }
  if (ev) {
    const e2: Record<string, unknown> = { ...ev };
    delete e2.partial;
    if (evType === 'toolcall_delta') delete e2.delta;
    out.assistantMessageEvent = e2;
  }
  return out;
}

/** True for tool-call/output events (focus clients drop them entirely). */
export function isToolEvent(record: { type: string; data: unknown }): boolean {
  if (record.type.startsWith('tool_execution')) return true;
  const data = record.data as { qa?: unknown; message?: { role?: string; toolName?: string; qa?: unknown } } | null;
  // CONTRACT §9: Q&A rows (message_start/_end, file_update) stay in focus streams.
  if (data?.qa || data?.message?.qa) return false;
  const msg = data?.message;
  return msg?.role === 'tool' || msg?.role === 'toolResult' || !!msg?.toolName;
}

/** Options for a Session that do not fit the positional constructor. */
export interface SessionStartOptions {
  /** Pass `--name` on this spawn (only for brand-new sessions — pi appends a
   *  session_info entry for every `--name`). */
  passName?: boolean;
  /** Spawn WITHOUT taking the bridge lease (fork helper that never writes the source file). */
  noLease?: boolean;
}

/**
 * A Session is the bridge-side handle of one pi session file. At most ONE
 * transport is active:
 *  - a `pi --mode rpc` child the bridge spawned (owner 'bridge', lease kind
 *    'bridge', renewed every 5 s — fenced ⇒ stand down);
 *  - an owner-proxy socket to the host TUI that holds the lease ('tui');
 *  - none: SSE viewers get a read-only file tailer (`file_update` frames).
 * Events from either transport flow through handleEvent → ring → SSE sinks.
 */
export class Session {
  id: string;
  name: string;
  file: string;
  workdir: string;
  readonly source: 'app' | 'pi';
  createdAt = Date.now();
  lastActivityAt = Date.now();
  busy = false;
  /** Server-owned durable prompt queue (persisted by the manager). */
  queue: QueueItem[] = [];
  queueWired = false;
  /** True while the manager is dispatching a queue item for this session. */
  dispatching = false;
  queueTimer: NodeJS.Timeout | null = null;
  /** Called when a run settles and the agent is ready for the next prompt. */
  onIdle: (() => void) | null = null;
  /** Called whenever the active transport goes away. */
  onExit: (() => void) | null = null;
  /** Called when our bridge lease was taken by someone else (TUI pre-emption). */
  onFenced: (() => void) | null = null;
  /** pi's own session id (JSONL header id, from get_state). */
  piId: string | null = null;
  phase: SessionPhase = 'idle';
  model: string | null = null;
  messageCount = 0;
  error: string | null = null;
  /** Ownership seen in the lease while no transport is attached. */
  externalOwner: SessionOwner = 'none';
  /** Unanswered questionnaire request (replayed to late subscribers). */
  pendingQuestion: { requestId: string; questions: unknown } | null = null;

  /* CONTRACT §7 activity (derived from the active transport's events) */
  /** Run-level activity (waiting/thinking/writing/toolcall/retrying) while a run is live. */
  private run: { kind: ActivityKind; since: number; tool?: string; attempt?: number; maxAttempts?: number; delayMs?: number } | null = null;
  /** Running tool calls (insertion order = start order): toolCallId → name/start. */
  private readonly runningTools = new Map<string, { name: string; since: number }>();
  /** Compaction in progress (overrides the run activity; busy for dispatch). */
  private compaction: { since: number; reason?: string; percent?: number } | null = null;
  private questionSince = 0;
  private busySince = 0;
  private reservedSince = 0;

  /* SSE stream identity */
  epoch = nextStreamEpoch();
  seq = 0;
  private readonly ring: RingRecord[] = [];
  private ringBytes = 0;
  /** Highest seq evicted from the ring (gap detection). */
  ringFloor = 0;
  private readonly sinks = new Set<EventSink>();

  /* transports */
  private proc: ChildProcess | null = null;
  private rpcSplitter = new LineSplitter(MAX_RPC_LINE_BYTES);
  private readonly stderrTail: string[] = [];
  private ownerSocket: net.Socket | null = null;
  ownerLease: Lease | null = null;
  /** Last owner event seq seen + the owner identity it belongs to (hello lastSeq). */
  private ownerSeq = 0;
  private ownerIdentity = '';
  private bridgeLease: Lease | null = null;
  private renewTimer: NodeJS.Timeout | null = null;
  private tailer: FileTailer | null = null;

  private readonly pending = new Map<string, PendingRpc>();
  private rpcCounter = 0;
  private stopping = false;
  private exitEmitted = true;
  private restartTimer: NodeJS.Timeout | null = null;
  private crashTimes: number[] = [];
  private promptReserved = false;
  private pendingEcho: PendingEcho[] = [];
  /** Pass --name on the next spawn (set for brand-new sessions only). */
  passNameOnce = false;
  /** True once the child process has terminated (spawn failure or exit). */
  closed = false;
  /** Last time a run settled (agent_end / agent_settled). */
  lastIdleAt = 0;
  /** The current child answered its first RPC (crash restarts only make sense after that). */
  private childReady = false;

  constructor(
    id: string,
    name: string,
    file: string,
    private readonly bin: string,
    workdir: string,
    private readonly sessionDir: string,
    private readonly extraArgs: string[],
    private readonly log: Logger,
    source: 'app' | 'pi' = 'app',
  ) {
    this.id = id;
    this.name = name || 'New session';
    this.file = file;
    this.workdir = workdir;
    this.source = source;
  }

  /* ---------------- state accessors ---------------- */

  get running(): boolean {
    return this.proc !== null || this.ownerSocket !== null;
  }

  /** A bridge-spawned child is running (counts toward maxAgents). */
  get hasChild(): boolean {
    return this.proc !== null;
  }

  /** Proxied to a live host TUI via IPC (never counts toward maxAgents). */
  get isProxy(): boolean {
    return this.ownerSocket !== null;
  }

  get pid(): number | null {
    return this.proc?.pid ?? null;
  }

  get owner(): SessionOwner {
    if (this.proc) return 'bridge';
    if (this.ownerSocket) return 'tui';
    return this.externalOwner;
  }

  /** Busy for dispatch purposes: mid-run or a prompt is in flight. */
  get isBusy(): boolean {
    return this.busy || this.phase === 'streaming' || this.promptReserved || this.hasLiveActivity();
  }

  /** A run, tool, compaction or questionnaire is in progress (§7: busy). */
  private hasLiveActivity(): boolean {
    return this.run !== null || this.runningTools.size > 0 || this.compaction !== null || this.pendingQuestion !== null;
  }

  /** CONTRACT §7: what the agent is doing right now (null = idle / no transport). */
  activity(): Activity | null {
    if (this.pendingQuestion) return { kind: 'question', since: this.questionSince };
    if (this.compaction) {
      const a: Activity = { kind: 'compacting', since: this.compaction.since };
      if (this.compaction.reason) a.reason = this.compaction.reason;
      if (this.compaction.percent !== undefined) a.percent = this.compaction.percent;
      return a;
    }
    if (this.runningTools.size > 0) {
      const first = this.runningTools.values().next().value!;
      return { kind: 'tool', since: first.since, tool: first.name, toolCount: this.runningTools.size };
    }
    if (this.run) {
      const a: Activity = { kind: this.run.kind, since: this.run.since };
      if (this.run.tool) a.tool = this.run.tool;
      if (this.run.attempt !== undefined) a.attempt = this.run.attempt;
      if (this.run.maxAttempts !== undefined) a.maxAttempts = this.run.maxAttempts;
      if (this.run.delayMs !== undefined) a.delayMs = this.run.delayMs;
      return a;
    }
    // A run with no activity seen yet (attach mid-run) or a prompt in flight.
    if (this.busy) return { kind: 'waiting', since: this.busySince };
    if (this.promptReserved) return { kind: 'waiting', since: this.reservedSince };
    return null;
  }

  /** Set the run activity; `since` only moves when the kind/tool changes. Returns true on change. */
  private setRun(kind: ActivityKind, extra: { tool?: string | undefined; attempt?: number | undefined; maxAttempts?: number | undefined; delayMs?: number | undefined } = {}): boolean {
    const prev = this.run;
    if (prev && prev.kind === kind && (extra.tool === undefined || prev.tool === extra.tool)) {
      const next = { ...prev };
      for (const [k, v] of Object.entries(extra)) if (v !== undefined) (next as Record<string, unknown>)[k] = v;
      const changed = JSON.stringify(next) !== JSON.stringify(prev);
      this.run = next;
      return changed;
    }
    this.run = { kind, since: Date.now() };
    for (const [k, v] of Object.entries(extra)) if (v !== undefined) (this.run as Record<string, unknown>)[k] = v;
    return true;
  }

  /** Forget all activity (run ended / transport gone). */
  private resetActivity(): void {
    this.run = null;
    this.runningTools.clear();
    this.compaction = null;
  }

  /** Publish a questionnaire (both transports) — at most one broadcast per requestId. */
  private setQuestion(requestId: string, questions: unknown): void {
    if (this.pendingQuestion?.requestId === requestId) return; // replay (owner hello) — already known
    this.pendingQuestion = { requestId, questions };
    this.questionSince = Date.now();
    this.broadcast('agent_question', { requestId, questions });
    this.emitStatus();
  }

  /** Resolve the pending questionnaire if it is `requestId` (single resolved broadcast). */
  private resolveQuestion(requestId: string, emit = true): boolean {
    if (!this.pendingQuestion || this.pendingQuestion.requestId !== requestId) return false;
    this.pendingQuestion = null;
    this.broadcast('agent_question_resolved', { requestId });
    if (emit) this.emitStatus();
    return true;
  }

  /** CONTRACT §8 clearing triggers (abort, run end, exit, detach…). */
  private clearQuestion(emit = true): void {
    if (this.pendingQuestion) this.resolveQuestion(this.pendingQuestion.requestId, emit);
  }

  get restarting(): boolean {
    return this.restartTimer !== null;
  }

  sinkCount(): number {
    return this.sinks.size;
  }

  reservePrompt(): boolean {
    if (this.isBusy) return false; // incl. compaction / pending question (§7)
    this.promptReserved = true;
    this.reservedSince = Date.now();
    this.emitStatus();
    return true;
  }

  releasePrompt(): void {
    if (!this.promptReserved) return;
    this.promptReserved = false;
    this.emitStatus();
  }

  /** Record a queue-dispatch problem so it reaches clients via agent_status. */
  setError(message: string | null): void {
    this.error = message;
    this.emitStatus();
  }

  /* ---------------- SSE fan-out ---------------- */

  /** New stream epoch: ring cleared, ids restart (clients get `reset`). */
  newEpoch(): void {
    this.epoch = nextStreamEpoch();
    this.ring.length = 0;
    this.ringBytes = 0;
    this.ringFloor = this.seq;
  }

  private makeRecord(type: string, data: RpcEvent): RingRecord {
    const seq = ++this.seq;
    return { id: `${this.epoch}-${seq}`, seq, type, data, json: JSON.stringify(data) };
  }

  private pushRing(record: RingRecord): void {
    this.ring.push(record);
    this.ringBytes += record.json.length;
    while (this.ring.length > EVENT_RING_CAPACITY || this.ringBytes > EVENT_RING_MAX_BYTES) {
      const oldest = this.ring.shift();
      if (!oldest) break;
      this.ringBytes -= oldest.json.length;
      this.ringFloor = oldest.seq;
    }
  }

  private fanout(record: RingRecord): void {
    for (const sink of [...this.sinks]) {
      try {
        sink.send(record);
      } catch { /* sink handles its own teardown */ }
    }
  }

  /** Live-only synthetic frame (queue_update, agent_status, …) — not in the ring. */
  broadcast(type: string, data: Record<string, unknown>): void {
    if (this.sinks.size === 0) {
      this.seq++;
      return;
    }
    this.fanout(this.makeRecord(type, { type, ...data } as RpcEvent));
  }

  statusFrame(): AgentStatusFrame {
    return computeStatus({
      phase: this.phase,
      busy: this.busy || this.hasLiveActivity(),
      promptReserved: this.promptReserved,
      error: this.error,
      owner: this.owner,
      activity: this.activity(),
      questionId: this.pendingQuestion?.requestId ?? null,
    });
  }

  private lastStatusJson = '';

  /** CONTRACT §5: emit agent_status (deduplicated against the last emission). */
  emitStatus(force = false): void {
    const frame = this.statusFrame();
    const json = JSON.stringify(frame);
    if (!force && json === this.lastStatusJson) return;
    this.lastStatusJson = json;
    this.broadcast('agent_status', frame);
  }

  ringSnapshot(): readonly RingRecord[] {
    return this.ring;
  }

  subscribe(sink: EventSink): void {
    this.sinks.add(sink);
    if (!this.running) this.ensureTailer();
  }

  unsubscribe(sink: EventSink): void {
    this.sinks.delete(sink);
    if (this.sinks.size === 0) this.stopTailer();
  }

  /** Drop all subscribers (session deleted). */
  closeSinks(): void {
    for (const sink of [...this.sinks]) {
      this.sinks.delete(sink);
      try { sink.close?.(); } catch { /* ignore */ }
    }
    this.stopTailer();
  }

  /* ---------------- read-only tailer (no transport) ---------------- */

  private ensureTailer(): void {
    if (this.sinks.size === 0 || this.running) return;
    if (this.tailer && this.tailer.file === this.file && this.tailer.running) return;
    this.stopTailer();
    this.tailer = new FileTailer(this.file, (batch) => this.onTailBatch(batch));
    this.tailer.start();
  }

  private stopTailer(): void {
    this.tailer?.stop();
    this.tailer = null;
  }

  private onTailBatch(batch: TailBatch): void {
    if (this.running) return; // a transport attached meanwhile — it streams canonical events
    this.lastActivityAt = Date.now();
    // Display hint only: an unowned writer's last row is a user prompt and
    // the file is fresh ⇒ probably working. Status stays lease-derived.
    const last = batch.messages[batch.messages.length - 1];
    const working = Date.now() - batch.mtimeMs < 120_000 && last?.role === 'user';
    for (const message of batch.messages) {
      this.fanout(this.makeRecord('file_update', { type: 'file_update', message, working } as RpcEvent));
    }
  }

  /** Test hook: force a tail poll now. */
  async pollTail(): Promise<void> {
    await this.tailer?.poll();
  }

  /* ---------------- RPC plumbing ---------------- */

  /** Translate a pi-rpc command into an owner-IPC op (old v3 + new owners). */
  static toOwnerOp(obj: Record<string, unknown>): Record<string, unknown> {
    const { type, ...rest } = obj;
    let op = String(type ?? '');
    if (op === 'prompt') {
      const sb = rest.streamingBehavior;
      if (sb === 'steer') op = 'steer';
      else if (sb === 'followUp') op = 'enqueue';
    } else if (op === 'follow_up') {
      op = 'enqueue';
    }
    return { ...rest, op };
  }

  /** Send a command and await its `response` record. */
  request<T = unknown>(obj: Record<string, unknown>, timeoutMs = RPC_TIMEOUT_MS): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const proc = this.proc;
      const socket = this.ownerSocket;
      if (!proc?.stdin && !socket) {
        reject(new Error('Agent not running'));
        return;
      }
      const id = `r${++this.rpcCounter}`;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC timeout for ${String(obj.type)}`));
      }, timeoutMs);
      this.pending.set(id, { resolve: (d) => resolve(d as T), reject, timer });
      try {
        if (proc?.stdin) {
          proc.stdin.write(`${JSON.stringify({ ...obj, id })}\n`);
        } else {
          socket!.write(`${JSON.stringify({ ...Session.toOwnerOp(obj), id })}\n`);
        }
      } catch (err) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(err as Error);
      }
    });
  }

  /** Abort the current run on whichever transport is active. Real result. */
  async abort(): Promise<boolean> {
    if (!this.running) return false;
    // A child blocked in the questionnaire select() would sit out its 10-min
    // timeout — decline it first so the tool returns and the abort lands.
    if (this.proc?.stdin && this.pendingQuestion) {
      try {
        this.proc.stdin.write(`${JSON.stringify({ type: 'extension_ui_response', id: this.pendingQuestion.requestId, cancelled: true })}\n`);
      } catch { /* abort below still runs */ }
    }
    try {
      await this.request({ type: 'abort' }, 15_000);
      this.clearQuestion(false);
      this.emitStatus(true);
      return true;
    } catch (err) {
      this.log.warn({ sessionId: this.id, err: (err as Error).message }, 'abort failed');
      return false;
    }
  }

  private handleResponse(obj: RpcResponse): void {
    const entry = this.pending.get(obj.id ?? '');
    if (!entry) return;
    clearTimeout(entry.timer);
    this.pending.delete(obj.id ?? '');
    if (obj.success) entry.resolve(obj.data ?? {});
    else entry.reject(new Error(obj.error ?? `RPC error: ${obj.command}`));
  }

  private rejectPending(reason: string): void {
    for (const p of this.pending.values()) {
      clearTimeout(p.timer);
      p.reject(new Error(reason));
    }
    this.pending.clear();
    for (const echo of this.pendingEcho.splice(0)) echo.onEcho(null);
  }

  /**
   * Answer a pending questionnaire (CONTRACT §8) and fan out the resolved
   * frame so every client dismisses its card.
   *  - bridge child: extension_ui_response with the `pi-answers:` marker;
   *  - host TUI (owner protocol >= 5): op `answer_question` → the owner calls
   *    the overlay's done({answers, cancelled}).
   * 409 = no such pending question; 502 = the transport refused/failed.
   */
  async answerQuestion(requestId: string, answers: unknown[], cancelled: boolean): Promise<AnswerResult> {
    if (!this.pendingQuestion || this.pendingQuestion.requestId !== requestId) {
      return { ok: false, status: 409, error: 'No pending question with that id' };
    }
    if (this.proc?.stdin) {
      const frame: Record<string, unknown> = { type: 'extension_ui_response', id: requestId };
      if (cancelled) frame.cancelled = true;
      else frame.value = `${RPC_ANSWER_PREFIX}${JSON.stringify({ answers, cancelled: false })}`;
      try {
        this.proc.stdin.write(`${JSON.stringify(frame)}\n`);
      } catch (err) {
        return { ok: false, status: 502, error: `Could not deliver the answer to the agent: ${(err as Error).message}` };
      }
    } else if (this.ownerSocket) {
      try {
        await this.request({ type: 'answer_question', requestId, answers, cancelled }, RPC_TIMEOUT_MS);
      } catch (err) {
        const msg = (err as Error).message;
        if (/unknown op/i.test(msg)) {
          return { ok: false, status: 502, error: 'The host TUI cannot take remote answers (Remote Pi owner extension older than v5) — answer in the terminal' };
        }
        if (/unknown requestId|no pending question/i.test(msg)) {
          // Answered in the terminal meanwhile (its resolved event may still be in flight).
          this.resolveQuestion(requestId);
          return { ok: false, status: 409, error: 'No pending question with that id' };
        }
        return { ok: false, status: 502, error: `Host TUI rejected the answer: ${msg}` };
      }
    } else {
      return { ok: false, status: 409, error: 'Agent not running' };
    }
    this.resolveQuestion(requestId);
    return { ok: true };
  }

  /** Register a dispatched queue item so its user-message echo carries the clientMessageId. */
  expectEcho(text: string, clientMessageId: string | undefined, onEcho: (timestamp: number | null) => void): void {
    this.pendingEcho.push({ text: text.trim(), clientMessageId, onEcho });
  }

  cancelEcho(text: string): void {
    const t = text.trim();
    const idx = this.pendingEcho.findIndex((e) => e.text === t);
    if (idx >= 0) this.pendingEcho.splice(idx, 1)[0]!.onEcho(null);
  }

  /** Mark a run live; it always has an activity (at least "waiting"). */
  private setBusy(): void {
    if (!this.busy) this.busySince = Date.now();
    this.busy = true;
    if (!this.run) this.setRun('waiting');
  }

  handleEvent(obj: RpcEvent): void {
    this.lastActivityAt = Date.now();
    // Questionnaire transport: capture as a first-class agent_question frame.
    if (obj.type === 'extension_ui_request') {
      const req = obj as unknown as { id?: string; method?: string; title?: string };
      if (req.method === 'select' && typeof req.title === 'string' && req.title.startsWith(RPC_QUESTION_PREFIX)) {
        let questions: unknown = [];
        try {
          questions = (JSON.parse(req.title.slice(RPC_QUESTION_PREFIX.length)) as { questions?: unknown }).questions ?? [];
        } catch {
          questions = [];
        }
        this.setQuestion(String(req.id ?? ''), questions);
        return; // never forward the raw transport frame
      }
    }
    // Owner (TUI, protocol >= 5) questionnaire bridge: the same first-class frames.
    if (obj.type === 'agent_question') {
      const requestId = typeof obj.requestId === 'string' ? obj.requestId : '';
      if (requestId) this.setQuestion(requestId, obj.questions ?? []);
      return;
    }
    if (obj.type === 'agent_question_resolved') {
      if (typeof obj.requestId === 'string') this.resolveQuestion(obj.requestId);
      return;
    }

    let wire = trimEventForWire(obj);
    /** Activity/status may have changed → emitStatus (deduped) after the fan-out. */
    let dirty = false;
    let settledNow = false;
    switch (obj.type) {
      case 'agent_start':
      case 'turn_start':
        this.setBusy();
        this.phase = 'streaming';
        this.error = null;
        this.promptReserved = false;
        if (this.runningTools.size === 0) this.setRun('waiting');
        dirty = true;
        break;
      case 'turn_end':
        // pi can still be processing between turn_end and agent_end — keep busy.
        this.phase = 'awaitingInput';
        break;
      case 'agent_end': {
        const msgs = obj.messages as unknown[] | undefined;
        if (Array.isArray(msgs)) this.messageCount = msgs.length;
        dirty = true;
        if (obj.willRetry === true) {
          // An automatic retry (or overflow compaction + retry) follows.
          this.runningTools.clear();
          this.setRun('retrying');
          break;
        }
        this.busy = false;
        this.phase = 'awaitingInput';
        this.promptReserved = false;
        this.error = Session.runError(msgs);
        this.run = null;
        this.runningTools.clear();
        this.clearQuestion(false);
        settledNow = true;
        break;
      }
      case 'agent_settled':
        this.busy = false;
        this.promptReserved = false;
        if (this.phase === 'streaming') this.phase = 'awaitingInput';
        this.resetActivity();
        this.clearQuestion(false);
        dirty = true;
        settledNow = true;
        break;
      case 'error':
        this.error = String((obj.error as { message?: string } | undefined)?.message ?? obj.error ?? 'agent error');
        dirty = true;
        break;
      case 'message_update': {
        if (!this.busy) break;
        const ev = obj.assistantMessageEvent as Record<string, unknown> | undefined;
        const t = typeof ev?.type === 'string' ? ev.type : '';
        // Only transitions emit: setRun is a no-op for repeated deltas of one kind.
        if (t === 'thinking_start' || t === 'thinking_delta') dirty = this.setRun('thinking');
        else if (t === 'text_start' || t === 'text_delta') dirty = this.setRun('writing');
        else if (t === 'toolcall_start') dirty = this.setRun('toolcall', { tool: toolcallName(ev!) });
        else if (t === 'toolcall_delta' && this.run?.kind !== 'toolcall') dirty = this.setRun('toolcall');
        break;
      }
      case 'tool_execution_start': {
        const id = typeof obj.toolCallId === 'string' ? obj.toolCallId : `anon-${this.seq}`;
        if (!this.runningTools.has(id)) {
          this.runningTools.set(id, { name: typeof obj.toolName === 'string' ? obj.toolName : 'tool', since: Date.now() });
        }
        if (!this.busy) this.setBusy();
        dirty = true;
        break;
      }
      case 'tool_execution_end': {
        if (typeof obj.toolCallId === 'string') this.runningTools.delete(obj.toolCallId);
        // Tools done → a fresh "waiting" (since = now: the shown kind changed).
        if (this.runningTools.size === 0 && this.busy) this.run = { kind: 'waiting', since: Date.now() };
        if (obj.toolName === ASK_USER_QUESTION_TOOL) this.clearQuestion(false);
        dirty = true;
        break;
      }
      case 'compaction_start':
      case 'session_before_compact': { // v4 owners forward the raw hook event
        const reason = typeof obj.reason === 'string' ? obj.reason : undefined;
        this.compaction = { since: Date.now(), ...(reason ? { reason } : {}) };
        dirty = true;
        break;
      }
      case 'compaction_progress': {
        if (!this.compaction) this.compaction = { since: Date.now() };
        const p = percentOf(obj.percent);
        if (p !== undefined) this.compaction.percent = p;
        dirty = true;
        break;
      }
      case 'compaction_end':
      case 'session_compact':
      case 'session_compact_failed':
        if (this.compaction) {
          this.compaction = null;
          dirty = true;
          // Restores the run activity — or idle when no run is live (manual /compact).
          if (!this.isBusy) settledNow = true;
        }
        break;
      case 'auto_retry_start': {
        const num = (v: unknown): number | undefined => (typeof v === 'number' && Number.isFinite(v) ? v : undefined);
        if (!this.busy) this.setBusy();
        this.runningTools.clear();
        this.setRun('retrying', { attempt: num(obj.attempt), maxAttempts: num(obj.maxAttempts), delayMs: num(obj.delayMs) });
        dirty = true;
        break;
      }
      case 'auto_retry_end':
        if (this.run?.kind === 'retrying' && this.busy) this.setRun('waiting');
        dirty = true;
        break;
      case 'message_start':
      case 'message_end': {
        const msg = obj.message as { role?: string; content?: unknown; timestamp?: number } | undefined;
        if (msg?.role === 'user' && this.pendingEcho.length) {
          const text = contentText(msg.content).trim();
          const idx = this.pendingEcho.findIndex((e) => e.text === text);
          if (idx >= 0) {
            const echo = this.pendingEcho[idx]!;
            if (echo.clientMessageId) wire = { ...wire, clientMessageId: echo.clientMessageId };
            if (obj.type === 'message_end') {
              this.pendingEcho.splice(idx, 1);
              echo.onEcho(typeof msg.timestamp === 'number' ? msg.timestamp : null);
            }
          }
        }
        break;
      }
      default:
    }

    const record = this.makeRecord(wire.type, wire);
    this.pushRing(record);
    this.fanout(record);

    if (dirty) this.emitStatus();
    if (settledNow) {
      this.lastIdleAt = Date.now();
      this.onIdle?.();
    }
  }

  /** Error text when the run's last assistant message ended in an error. */
  private static runError(messages: unknown[] | undefined): string | null {
    if (!Array.isArray(messages)) return null;
    for (let i = messages.length - 1; i >= 0; i--) {
      const m = messages[i] as { role?: string; stopReason?: string; errorMessage?: string } | null;
      if (m?.role !== 'assistant') continue;
      return m.stopReason === 'error' ? (m.errorMessage || 'agent error') : null;
    }
    return null;
  }

  private onStdoutChunk(chunk: Buffer): void {
    for (const line of this.rpcSplitter.push(chunk)) this.routeLine(line, false);
  }

  private routeLine(line: string, fromOwner: boolean): void {
    if (!line.trim()) return;
    let obj: unknown;
    try {
      obj = JSON.parse(line);
    } catch {
      if (!fromOwner) this.log.warn({ sessionId: this.id, line: line.slice(0, 200) }, 'non-JSON pi stdout');
      return;
    }
    if (fromOwner) this.onOwnerMessage(obj as Record<string, unknown>);
    else if ((obj as { type?: string }).type === 'response') this.handleResponse(obj as RpcResponse);
    else this.handleEvent(obj as RpcEvent);
  }

  /* ---------------- child lifecycle ---------------- */

  /**
   * Spawn `pi --mode rpc` for this file under a bridge lease and wait for its
   * first response (get_state) instead of a fixed sleep.
   */
  async start(opts: SessionStartOptions = {}): Promise<void> {
    if (this.proc || this.ownerSocket) return;
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }
    if (!opts.noLease) this.takeBridgeLease();
    const passName = opts.passName ?? this.passNameOnce;
    this.passNameOnce = false;
    const args = [
      '--mode', 'rpc',
      '--session', this.file,
      '--session-dir', this.sessionDir,
      ...(passName && this.name ? ['--name', this.name] : []),
      ...this.extraArgs,
    ];
    this.log.info({ sessionId: this.id }, `spawn pi ${this.bin} ${args.join(' ')}`);

    const child = spawnPiProcess(this.bin, args, this.workdir);
    this.proc = child;
    this.rpcSplitter = new LineSplitter(MAX_RPC_LINE_BYTES);
    this.stopping = false;
    this.promptReserved = false;
    this.closed = false;
    this.exitEmitted = false;
    this.childReady = false;
    this.busy = false;
    this.resetActivity();
    this.clearQuestion(false);
    this.phase = 'idle';
    this.lastActivityAt = Date.now();
    this.newEpoch();
    this.stopTailer();

    child.stdout?.on('data', (d: Buffer) => this.onStdoutChunk(d));
    const errDecoder = new TextDecoder('utf8');
    child.stderr?.on('data', (d: Buffer) => {
      const line = errDecoder.decode(d, { stream: true }).trim();
      if (!line) return;
      this.stderrTail.push(line);
      if (this.stderrTail.length > 40) this.stderrTail.shift();
      this.log.debug({ sessionId: this.id, chunk: line }, 'pi stderr');
    });
    child.on('error', (err) => {
      this.error = err.message;
      this.log.error({ sessionId: this.id, err: err.message }, 'pi spawn failed');
    });
    child.on('close', (code, signal) => this.onChildClose(child, code, signal));

    try {
      await once(child, 'spawn');
    } catch (err) {
      this.error = (err as Error).message;
      return; // close handler cleans up
    }
    if (this.bridgeLease && child.pid) {
      // Holder = the child (contract §6); the server pid fences orphans.
      const holder = { ...this.bridgeLease.holder, pid: child.pid, startedAtMs: Date.now() };
      if (renewLease(this.file, this.bridgeLease, Date.now(), { holder }) === 'lost') this.handleFenced();
    }
    try {
      await this.refreshState();
      this.childReady = true;
      this.crashTimesPrune();
    } catch (err) {
      this.log.warn({ sessionId: this.id, err: (err as Error).message }, 'state priming failed');
    }
    this.emitStatus(true);
  }

  private crashTimesPrune(): void {
    const cutoff = Date.now() - CRASH_WINDOW_MS;
    this.crashTimes = this.crashTimes.filter((t) => t > cutoff);
  }

  private takeBridgeLease(): void {
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true });
    } catch { /* surfaced by acquire */ }
    const res = acquireLease(this.file, this.piId ?? this.id, '', Date.now(), {
      kind: 'bridge',
      holderPid: process.pid,
      bridgePid: process.pid,
    });
    if (!res.ok || !res.lease) {
      if (res.reason === 'error') {
        // Unwritable sidecar (read-only dir…): run unleased rather than not at all.
        this.log.warn({ sessionId: this.id, err: String(res.error) }, 'bridge lease write failed — running unleased');
        return;
      }
      throw new LeaseHeldError(res.reason ?? 'held', res.lease);
    }
    this.bridgeLease = res.lease;
    this.startRenew();
  }

  private startRenew(): void {
    this.stopRenew();
    this.renewTimer = setInterval(() => this.renewTick(), RENEW_INTERVAL_MS);
    this.renewTimer.unref?.();
  }

  private stopRenew(): void {
    if (this.renewTimer) clearInterval(this.renewTimer);
    this.renewTimer = null;
  }

  /** Exposed for tests; normally driven by the renew interval. */
  renewTick(): void {
    const mine = this.bridgeLease;
    if (!mine) return;
    const outcome = renewLease(this.file, mine);
    if (outcome === 'lost') {
      this.handleFenced();
    } else if (outcome === 'absent' && this.proc) {
      // Someone removed our sidecar — re-assert ownership for the live child.
      const res = acquireLease(this.file, this.piId ?? this.id, '', mine.holder.startedAtMs, {
        kind: 'bridge',
        holderPid: this.proc.pid ?? process.pid,
        bridgePid: process.pid,
      });
      if (res.ok && res.lease) this.bridgeLease = res.lease;
      else if (res.reason === 'held') this.handleFenced();
    }
  }

  private handleFenced(): void {
    if (!this.bridgeLease) return;
    this.log.warn({ sessionId: this.id }, 'bridge lease fenced (host TUI pre-empted) — standing down');
    this.bridgeLease = null;
    this.stopRenew();
    this.onFenced?.();
  }

  /** Stop our child because another owner took the lease (abort first if busy). */
  async standDownChild(): Promise<void> {
    const child = this.proc;
    if (!child) return;
    if (this.busy || this.phase === 'streaming') {
      await this.abort().catch(() => false);
    }
    const closed = once(child, 'close').catch(() => undefined);
    this.stop('fenced');
    await Promise.race([closed, new Promise((r) => setTimeout(r, 5_000))]);
  }

  private onChildClose(child: ChildProcess, code: number | null, signal: NodeJS.Signals | null): void {
    if (this.proc !== child) return;
    const midTurn = this.phase === 'streaming' || this.busy;
    if (midTurn) {
      this.log.error({ sessionId: this.id, code, signal, stopping: this.stopping }, 'agent stopped MID-TURN');
      this.broadcast('agent_crashed', { code, midTurn: true });
    }
    this.stopRenew();
    if (this.bridgeLease) releaseLease(this.file, this.bridgeLease);
    this.bridgeLease = null;

    const unexpected = !this.stopping && (code !== 0 || signal !== null);
    this.proc = null;
    this.closed = true;
    this.busy = false;
    this.promptReserved = false;
    this.phase = 'terminated';
    this.resetActivity();
    this.clearQuestion(false);
    this.rejectPending('Agent exited');
    this.log.info({ sessionId: this.id, code, signal }, 'pi exited');

    if (unexpected) {
      this.log.error({ sessionId: this.id, code, stderr: this.stderrTail.slice(-20) }, 'pi exited unexpectedly');
      this.broadcast('agent_crashed', { code, stderr: this.stderrTail.slice(-20) });
      // A child that never became ready failed to boot: surfaced to the
      // caller (SpawnError), not auto-restarted.
      if (this.childReady) this.scheduleCrashRestart();
      else this.error = this.stderrTail.slice(-3).join(' ').slice(0, 500) || `pi exited with code ${code}`;
    }
    this.onExit?.();
    this.emitExited(code);
    this.ensureTailer();
  }

  /** Exponential backoff with jitter (≈1,2,4,8,16 s); give up after 5 crashes in 2 min. */
  private scheduleCrashRestart(): void {
    const now = Date.now();
    this.crashTimes.push(now);
    this.crashTimesPrune();
    if (this.crashTimes.length >= CRASH_LIMIT) {
      this.error = `pi crashed ${this.crashTimes.length} times in 2 minutes — automatic restart stopped`;
      this.log.error({ sessionId: this.id }, 'crash loop — giving up on auto-restart');
      return;
    }
    const base = Math.min(CRASH_BACKOFF_CAP_MS, 1000 * 2 ** (this.crashTimes.length - 1));
    const delay = Math.round(base / 2 + Math.random() * (base / 2)); // equal jitter
    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      if (this.proc || this.ownerSocket || this.stopping) return;
      this.start().catch((err) => {
        this.log.warn({ sessionId: this.id, err: (err as Error).message }, 'auto-restart failed');
        this.error = `restart failed: ${(err as Error).message}`;
        this.emitStatus();
      });
    }, delay);
    this.restartTimer.unref?.();
  }

  /** Reset the crash budget (explicit user action, e.g. a new turn). */
  resetCrashBudget(): void {
    this.crashTimes = [];
  }

  /** agent_exited + agent_status exactly once per transport lifetime. */
  private emitExited(code: number | null): void {
    if (this.exitEmitted) return;
    this.exitEmitted = true;
    this.broadcast('agent_exited', { code });
    this.emitStatus(true);
  }

  private async refreshState(): Promise<void> {
    const st = await this.request<AgentState>({ type: 'get_state' }, READY_TIMEOUT_MS);
    this.applyState(st);
  }

  applyState(st: AgentState): void {
    if (st.sessionId) this.piId = st.sessionId;
    if (st.sessionFile && path.resolve(st.sessionFile) !== path.resolve(this.file) && this.proc) {
      this.rebindFile(st.sessionFile);
    }
    if (st.sessionName) this.name = st.sessionName;
    const m = st.model;
    if (m && typeof m === 'object') this.model = `${m.provider ?? ''}/${m.modelId ?? m.id ?? ''}`;
    else if (typeof m === 'string') this.model = m;
    if (typeof st.messageCount === 'number') this.messageCount = st.messageCount;
    if (st.isStreaming) {
      this.setBusy();
      this.phase = 'streaming';
    }
    if (st.isCompacting === true && !this.compaction) this.compaction = { since: Date.now() };
  }

  /** pi resolved a different file than we asked for — move our lease with it. */
  private rebindFile(newFile: string): void {
    const old = this.file;
    this.file = newFile;
    if (this.bridgeLease) {
      releaseLease(old, this.bridgeLease);
      this.bridgeLease = null;
      this.stopRenew();
      try {
        this.takeBridgeLease();
        if (this.bridgeLease && this.proc?.pid) {
          const lease = this.bridgeLease as Lease;
          renewLease(this.file, lease, Date.now(), { holder: { ...lease.holder, pid: this.proc.pid } });
        }
      } catch (err) {
        this.log.warn({ sessionId: this.id, err: (err as Error).message }, 'lease rebind failed');
        this.handleFencedLater();
      }
    }
  }

  private handleFencedLater(): void {
    setImmediate(() => this.onFenced?.());
  }

  /** Adopt a new identity after pi switched this process to another file (fork). */
  adoptIdentity(sessionId: string, file: string, name?: string): void {
    this.id = sessionId;
    this.piId = sessionId;
    this.file = file;
    if (name) this.name = name;
    this.newEpoch();
    if (this.proc) {
      try {
        this.takeBridgeLease();
        const lease = this.bridgeLease as Lease | null;
        if (lease && this.proc.pid) {
          renewLease(this.file, lease, Date.now(), { holder: { ...lease.holder, pid: this.proc.pid } });
        }
      } catch (err) {
        this.log.warn({ sessionId: this.id, err: (err as Error).message }, 'fork lease failed');
      }
    }
  }

  /**
   * Stop the active transport. Children are killed (exit is reported by the
   * close handler); owner proxies only drop our pipe (never the host TUI).
   * Every path emits agent_exited + agent_status.
   */
  stop(reason = 'stop'): void {
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }
    this.stopping = true;
    this.promptReserved = false;
    if (this.ownerSocket) {
      this.detachOwner(this.ownerSocket, `stopped (${reason})`);
      return;
    }
    const proc = this.proc;
    if (!proc) return;
    this.log.info({ sessionId: this.id, reason }, 'stopping agent');
    try { proc.stdin?.end(); } catch { /* ignore */ }
    killPiProcess(proc);
  }

  /** Resolve once the child (if any) has exited, or after `timeoutMs`. */
  async waitClosed(timeoutMs: number): Promise<void> {
    const child = this.proc;
    if (!child) return;
    await Promise.race([once(child, 'close').catch(() => undefined), new Promise((r) => setTimeout(r, timeoutMs))]);
  }

  /* ---------------- owner proxy (host TUI holds the lease) ---------------- */

  /**
   * Attach to the live host TUI holding the lease. Line-buffered handshake:
   * the hello reply is the first `{"type":"response","id":"hello"}` LINE —
   * events the owner pushes before it (old v3 owners broadcast raw events to
   * un-helloed sockets) are buffered and processed after attach. On failure
   * the socket is destroyed and NOT kept, so later calls retry cleanly.
   */
  async attachOwner(lease: Lease): Promise<void> {
    if (this.proc) throw new Error('attachOwner: bridge child still running');
    if (!lease.ipc) throw new Error('attachOwner: lease has no ipc endpoint yet');
    if (this.ownerSocket) this.detachOwner(this.ownerSocket, 'reattach', true);
    const identity = `${lease.holder.pid}:${lease.holder.nonce}`;
    const lastSeq = identity === this.ownerIdentity ? this.ownerSeq : 0;

    const socket = await new Promise<net.Socket>((resolve, reject) => {
      const s = net.connect(lease.ipc);
      const timer = setTimeout(() => {
        s.destroy();
        reject(new Error('owner connect timeout'));
      }, OWNER_CONNECT_TIMEOUT_MS);
      timer.unref?.();
      s.once('connect', () => {
        clearTimeout(timer);
        s.removeAllListeners('error');
        resolve(s);
      });
      s.once('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });

    const splitter = new LineSplitter(MAX_RPC_LINE_BYTES);
    // Everything except the hello reply is queued until we are installed, so
    // pre-hello and post-hello frames are processed in arrival order.
    const early: Record<string, unknown>[] = [];
    let attached = false;
    let installed = false;
    const hello = await new Promise<Record<string, unknown>>((resolve, reject) => {
      let settled = false;
      const fail = (err: Error): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        socket.destroy();
        reject(err);
      };
      const timer = setTimeout(() => fail(new Error('owner hello timeout')), OWNER_HELLO_TIMEOUT_MS);
      timer.unref?.();
      socket.on('data', (chunk: Buffer) => {
        for (const line of splitter.push(chunk)) {
          if (installed) {
            this.routeLine(line, true);
            continue;
          }
          let obj: Record<string, unknown>;
          try {
            obj = JSON.parse(line) as Record<string, unknown>;
          } catch {
            continue;
          }
          if (attached) {
            early.push(obj);
          } else if (obj.type === 'response' && obj.id === 'hello') {
            if (obj.success === false) {
              fail(new Error(`owner hello rejected: ${String(obj.error ?? 'unknown')}`));
              return;
            }
            settled = true;
            attached = true;
            clearTimeout(timer);
            resolve((obj.data as Record<string, unknown>) ?? {});
          } else if (obj.type === 'response') {
            continue; // stray reply for another socket's id — ignore
          } else {
            early.push(obj);
          }
        }
      });
      socket.on('error', (err) => {
        if (!attached) fail(err);
        else this.log.warn({ sessionId: this.id, err: err.message }, 'owner ipc error');
      });
      socket.on('close', () => {
        if (!attached) fail(new Error('owner closed during hello'));
        else this.detachOwner(socket, 'owner disconnected');
      });
      socket.write(`${JSON.stringify({
        id: 'hello',
        op: 'hello',
        type: 'bridge',
        lastSeq,
        clientId: `remote-pi-bridge:${path.resolve(this.file)}`,
      })}\n`);
    });

    if (this.proc || socket.destroyed) {
      // A child appeared (never run both) or the owner hung up mid-attach.
      socket.destroy();
      throw new Error(this.proc ? 'attachOwner: bridge child started during handshake' : 'owner closed during attach');
    }
    this.ownerSocket = socket;
    this.ownerLease = lease;
    if (identity !== this.ownerIdentity) {
      this.ownerIdentity = identity;
      this.ownerSeq = 0;
      this.newEpoch();
    }
    // Owner could not replay what we missed → clients must reload (reset).
    if (hello.rehydrateFromFile === true) this.newEpoch();
    if (typeof hello.eventSeq === 'number' && this.ownerSeq === 0) this.ownerSeq = hello.eventSeq;
    this.stopping = false;
    this.closed = false;
    this.exitEmitted = false;
    this.error = null;
    this.busy = false;
    this.resetActivity();
    if (hello.isStreaming === true) this.setBusy();
    this.phase = this.busy ? 'streaming' : 'idle';
    this.externalOwner = 'tui';
    this.lastActivityAt = Date.now();
    this.stopTailer();
    this.log.info({ sessionId: this.id, ipc: lease.ipc, lastSeq }, 'attached to live owner');
    for (const obj of early.splice(0)) this.onOwnerMessage(obj);
    installed = true;
    try {
      const st = await this.request<AgentState & { isStreaming?: boolean }>({ type: 'get_state' }, 5_000);
      if (st.isStreaming === true) {
        if (!this.busy) this.setBusy();
      } else {
        this.busy = false;
        this.run = null;
        this.runningTools.clear();
      }
      this.phase = this.busy ? 'streaming' : 'idle';
      if (st.sessionName) this.name = st.sessionName;
      if (typeof st.model === 'string') this.model = st.model;
    } catch { /* snapshot from hello is good enough */ }
    this.emitStatus(true);
  }

  private onOwnerMessage(obj: Record<string, unknown>): void {
    switch (obj.type) {
      case 'response':
        this.handleResponse(obj as unknown as RpcResponse);
        return;
      case 'event': {
        // Attached-socket frame: {type:'event', seq, eventType, event}.
        if (typeof obj.seq === 'number') this.ownerSeq = obj.seq;
        const ev = obj.event as RpcEvent | undefined;
        if (ev && typeof ev.type === 'string') this.handleEvent(ev);
        return;
      }
      case 'evicted':
        this.log.warn({ sessionId: this.id, reason: obj.reason }, 'owner evicted this attachment');
        this.ownerSocket?.destroy();
        return;
      default:
        if (typeof obj.type === 'string') this.handleEvent(obj as RpcEvent); // legacy raw event
    }
  }

  private detachOwner(socket: net.Socket, reason: string, quiet = false): void {
    if (this.ownerSocket !== socket) {
      try { socket.destroy(); } catch { /* ignore */ }
      return;
    }
    this.ownerSocket = null;
    try { socket.destroy(); } catch { /* ignore */ }
    this.busy = false;
    this.promptReserved = false;
    this.phase = 'terminated';
    this.resetActivity();
    this.clearQuestion(false);
    this.rejectPending(`Owner connection closed: ${reason}`);
    this.log.info({ sessionId: this.id, reason }, 'owner detached');
    this.onExit?.();
    if (!quiet) {
      this.emitExited(0);
      this.ensureTailer();
    }
  }

  /* ---------------- summary ---------------- */

  /** Bare runtime facts; the manager builds the wire summary (one builder). */
  runtime(): { running: boolean; busy: boolean; phase: SessionPhase; owner: SessionOwner; error: string | null; activity: Activity | null } {
    return {
      running: this.running,
      busy: this.isBusy,
      phase: this.phase,
      owner: this.owner,
      error: this.error,
      activity: this.running ? this.activity() : null,
    };
  }
}
