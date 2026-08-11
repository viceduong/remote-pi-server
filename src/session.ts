import { once } from 'node:events';
import path from 'node:path';
import fs from 'node:fs';
import type { ChildProcess } from 'node:child_process';
import type { Logger } from 'pino';
import { spawnPiProcess } from './pi.js';
import { mapAgentMessage } from './history.js';
import type { AgentMessage, AgentState, RpcEvent, RpcResponse, SessionPhase, SessionSummary } from './types.js';

const EVENT_RING_CAPACITY = 500;
const RPC_TIMEOUT_MS = 10_000;
const FILE_WATCH_DEBOUNCE_MS = 250;

/** One event record in the replay ring. */
export interface RingRecord {
  seq: number;
  type: string;
  data: RpcEvent;
}

/** Destination for streamed events (SSE connection). */
export interface EventSink {
  send(record: RingRecord): void;
}

interface PendingRpc {
  resolve: (data: unknown) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * A Session wraps one `pi --mode rpc` child process:
 *  - JSONL framing on stdout (strict \n delimiter, strip \r)
 *  - request/response correlation for commands
 *  - event ring buffer + fan-out for SSE subscribers
 *  - busy tracking from turn/message lifecycle events
 */
export class Session {
  readonly id: string;
  name: string;
  file: string;
  /** Working directory the agent runs in (from session metadata when resuming). */
  workdir: string;
  /** 'app' = created through the bridge; 'pi' = pre-existing host session. */
  readonly source: 'app' | 'pi';
  createdAt = Date.now();
  lastActivityAt = Date.now();
  busy = false;
  /** Last turn_start timestamp (queue-delivery watchdog uses it). */
  lastTurnStartAt = 0;
  /** Server-owned prompt queue (dispatched on turn_end — messages never
   *  vanish and never double-send). */
  queue: string[] = [];
  /** Called when a turn completes and the agent is ready for the next prompt. */
  onIdle: (() => void) | null = null;
  /** Runtime phase derived from the owner's event stream. */
  phase: SessionPhase = 'idle';
  /** Read-only mirror mode: file watcher only, no pi process (convertible). */
  readOnly = false;
  model: string | null = null;
  messageCount = 0;
  error: string | null = null;

  private proc: ChildProcess | null = null;
  private buffer = '';
  private readonly ring: RingRecord[] = [];
  private readonly stderrTail: string[] = [];
  private readonly sinks = new Set<EventSink>();
  private readonly pending = new Map<string, PendingRpc>();
  private seq = 0;
  private rpcCounter = 0;
  private stopping = false;
  /** True once the child process has terminated (spawn failure or exit). */
  closed = false;

  /* ----- file-tail live push (host/other-client activity) ----- */
  private fileWatcher: fs.FSWatcher | null = null;
  private fileOffset = 0;
  private watchTimer: NodeJS.Timeout | null = null;

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
    readOnly = false,
  ) {
    this.id = id;
    this.name = name || 'New session';
    this.file = file;
    this.workdir = workdir;
    this.source = source;
    this.readOnly = readOnly;
  }

  get running(): boolean {
    return this.proc !== null;
  }

  /** Child pid of the live pi process (null when not running). */
  get pid(): number | null {
    return this.proc?.pid ?? null;
  }

  /* ---------------- RPC plumbing ---------------- */

  /** Send a fire-and-forget command (e.g. abort). */
  send(obj: Record<string, unknown>): boolean {
    if (!this.proc?.stdin || this.proc.stdin.destroyed) return false;
    this.proc.stdin.write(`${JSON.stringify(obj)}\n`);
    return true;
  }

  /** Send a command and await its `response` record. */
  request<T = unknown>(obj: Record<string, unknown>, timeoutMs = RPC_TIMEOUT_MS): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const proc = this.proc;
      if (!proc?.stdin) {
        reject(new Error('Agent not running'));
        return;
      }
      const id = `r${++this.rpcCounter}`;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC timeout for ${String(obj.type)}`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (d) => resolve(d as T),
        reject,
        timer,
      });
      proc.stdin.write(`${JSON.stringify({ ...obj, id })}\n`);
    });
  }

  private handleResponse(obj: RpcResponse): void {
    const entry = this.pending.get(obj.id ?? '');
    if (!entry) return;
    clearTimeout(entry.timer);
    this.pending.delete(obj.id ?? '');
    if (obj.success) entry.resolve(obj.data ?? {});
    else entry.reject(new Error(obj.error ?? `RPC error: ${obj.command}`));
  }

  handleEvent(obj: RpcEvent): void {
    this.lastActivityAt = Date.now();
    if (obj.type === 'turn_start') {
      this.busy = true;
      this.lastTurnStartAt = Date.now();
      this.phase = 'streaming';
    }
    else if (obj.type === 'turn_end' || obj.type === 'agent_end') {
      this.busy = false;
      this.phase = 'awaitingInput';
      if (this.onIdle) this.onIdle();
    }
    else if (obj.type === 'message_update') {
      // NOTE: do NOT clear busy on per-message 'done'/'error' — a turn with
      // tool loops emits several messages before turn_end; clearing busy here
      // made mid-turn agents look idle and get evicted/killed.
    }
    if (obj.type === 'agent_end') {
      this.error = null;
      const msgs = obj.messages as unknown[] | undefined;
      if (Array.isArray(msgs)) this.messageCount = msgs.length;
    }
    if (obj.type === 'error') this.error = String((obj.error as { message?: string } | string) ?? 'agent error');

    const record: RingRecord = { seq: ++this.seq, type: obj.type, data: obj };
    this.ring.push(record);
    if (this.ring.length > EVENT_RING_CAPACITY) {
      this.ring.splice(0, this.ring.length - EVENT_RING_CAPACITY);
    }
    for (const sink of this.sinks) sink.send(record);
  }

  private onStdoutChunk(chunk: Buffer): void {
    this.buffer += chunk.toString('utf8');
    let idx: number;
    while ((idx = this.buffer.indexOf('\n')) >= 0) {
      const line = this.buffer.slice(0, idx).replace(/\r$/, '');
      this.buffer = this.buffer.slice(idx + 1);
      if (!line.trim()) continue;
      let obj: unknown;
      try {
        obj = JSON.parse(line);
      } catch {
        this.log.warn({ sessionId: this.id, line: line.slice(0, 200) }, 'non-JSON pi stdout');
        continue;
      }
      const r = obj as { type?: string };
      if (r.type === 'response') this.handleResponse(obj as RpcResponse);
      else this.handleEvent(obj as RpcEvent);
    }
  }

  /* ---------------- lifecycle ---------------- */

  /** Read-only mirror: watch the file for host activity, spawn nothing. */
  async startReadOnly(): Promise<void> {
    if (this.proc) return;
    this.fileOffset = 0;
    this.startFileWatch();
  }

  /** Spawn the pi process and prime session metadata via get_state. */
  async start(): Promise<void> {
    if (this.readOnly) return this.startReadOnly();
    if (this.proc) return;
    const args = [
      '--mode', 'rpc',
      '--session', this.file,
      '--session-dir', this.sessionDir,
      '--name', this.name,
      ...this.extraArgs,
    ];
    this.log.info({ sessionId: this.id }, `spawn pi ${this.bin} ${args.join(' ')}`);

    const child = spawnPiProcess(this.bin, args, this.workdir);
    this.proc = child;
    this.buffer = '';
    this.stopping = false;

    child.stdout?.on('data', (d: Buffer) => this.onStdoutChunk(d));
    child.stderr?.on('data', (d: Buffer) => {
      const line = d.toString().trim();
      this.stderrTail.push(line);
      if (this.stderrTail.length > 40) this.stderrTail.shift();
      this.log.debug({ sessionId: this.id, chunk: line }, 'pi stderr');
    });

    child.on('error', (err) => {
      this.error = err.message;
      this.log.error({ sessionId: this.id, err: err.message }, 'pi spawn failed');
    });

    child.on('close', (code) => {
      const unexpected = code !== 0 && !this.stopping;
      if (unexpected) {
        this.log.error(
          { sessionId: this.id, code, stderr: this.stderrTail.slice(-20) },
          'pi exited unexpectedly',
        );
        for (const sink of this.sinks) {
          sink.send({
            seq: ++this.seq,
            type: 'agent_crashed',
            data: { type: 'agent_crashed', code, stderr: this.stderrTail.slice(-20) },
          });
        }
        // Auto-recovery: respawn (resume) shortly after an unexpected exit.
        setTimeout(() => {
          if (!this.proc) void this.start().catch(() => {});
        }, 1000);
      }
      this.log.info({ sessionId: this.id, code }, 'pi exited');
      this.proc = null;
      this.closed = true;
      this.busy = false;
      this.phase = 'terminated';
      for (const p of this.pending.values()) {
        clearTimeout(p.timer);
        p.reject(new Error('Agent exited'));
      }
      this.pending.clear();
      for (const sink of this.sinks) sink.send({ seq: ++this.seq, type: 'agent_exited', data: { type: 'agent_exited', code } });
      this.sinks.clear();
    });

    // Give the RPC endpoint a beat to boot, then prime metadata.
    try {
      await once(child, 'spawn');
      await sleep(1500);
      await this.refreshState();
    } catch (err) {
      this.log.warn({ sessionId: this.id, err: (err as Error).message }, 'state priming failed');
    }
  }

  private async refreshState(): Promise<void> {
    const st = await this.request<AgentState>({ type: 'get_state' });
    if (st.sessionId) this.file = st.sessionFile ?? this.file;
    if (st.sessionName) this.name = st.sessionName;
    if (st.model) this.model = `${st.model.provider}/${st.model.modelId ?? st.model.id ?? ''}`;
    if (typeof st.messageCount === 'number') this.messageCount = st.messageCount;
    if (st.isStreaming) this.busy = true;
  }

  /** Terminate the child process. Safe to call multiple times. */
  stop(): void {
    if (!this.proc || this.stopping) return;
    this.stopping = true;
    try {
      this.proc.stdin?.end();
    } catch { /* ignore */ }
    try {
      this.proc.kill();
    } catch { /* ignore */ }
  }

  /* ---------------- SSE fan-out ---------------- */

  subscribe(sink: EventSink): void {
    this.sinks.add(sink);
    if (this.sinks.size === 1) this.startFileWatch();
  }

  unsubscribe(sink: EventSink): void {
    this.sinks.delete(sink);
    if (this.sinks.size === 0) this.stopFileWatch();
  }

  /** Replay events after `lastSeq` (for Last-Event-ID). */
  replayAfter(lastSeq: number): RingRecord[] {
    return this.ring.filter((r) => r.seq > lastSeq);
  }

  /** Point the file watcher at a new branch file (after fork). */
  retargetFile(newFile: string): void {
    this.stopFileWatch();
    this.file = newFile;
    this.fileOffset = 0;
    if (this.sinks.size > 0) this.startFileWatch();
  }

  /* ----- file-watch live push ----- */

  /**
   * Watch the session JSONL: whenever the file grows (host terminal, other
   * clients), parse the appended message entries and push them to SSE
   * subscribers as `file_update` events — event-driven live updates instead
   * of client polling. Debounced; only runs while subscribers exist.
   */
  private startFileWatch(): void {
    if (this.fileWatcher) return;
    try {
      // Watch the session directory, not the file — pi creates the JSONL
      // lazily on the first message of a fresh session.
      const dir = path.dirname(this.file);
      const base = path.basename(this.file);
      this.fileWatcher = fs.watch(dir, (_eventType: string, filename: string | null) => {
        if (filename && filename.toString() !== base) return;
        this.scheduleTailRead();
      });
      this.fileWatcher.on('error', () => this.stopFileWatch());
      try {
        this.fileOffset = fs.statSync(this.file).size;
      } catch {
        this.fileOffset = 0; // file not created yet
      }
    } catch {
      this.fileWatcher = null;
    }
  }

  private stopFileWatch(): void {
    if (this.watchTimer) {
      clearTimeout(this.watchTimer);
      this.watchTimer = null;
    }
    if (this.fileWatcher) {
      try { this.fileWatcher.close(); } catch { /* ignore */ }
      this.fileWatcher = null;
    }
  }

  private scheduleTailRead(): void {
    if (this.watchTimer) return;
    this.watchTimer = setTimeout(() => {
      this.watchTimer = null;
      this.tailRead();
    }, FILE_WATCH_DEBOUNCE_MS);
  }

  private tailRead(): void {
    try {
      const stat = fs.statSync(this.file);
      if (stat.size < this.fileOffset) {
        this.fileOffset = stat.size; // file rewritten (compaction) — skip burst
        return;
      }
      if (stat.size === this.fileOffset) return;
      const fd = fs.openSync(this.file, 'r');
      const buf = Buffer.alloc(stat.size - this.fileOffset);
      try {
        fs.readSync(fd, buf, 0, buf.length, this.fileOffset);
      } finally {
        fs.closeSync(fd);
      }
      this.fileOffset = stat.size;
      const text = buf.toString('utf8');
      for (const line of text.split('\n')) {
        if (!line.trim()) continue;
        try {
          const entry = JSON.parse(line) as { type?: string; id?: string; message?: AgentMessage };
          if (entry.type !== 'message' || !entry.message) continue;
          const msgWithId = { ...entry.message } as AgentMessage;
          if (entry.id) msgWithId.id = entry.id;
          const mapped = mapAgentMessage(msgWithId);
          if (!mapped) continue;
          const rec: RingRecord = {
            seq: ++this.seq,
            type: 'file_update',
            data: { type: 'file_update', message: mapped } as RpcEvent,
          };
          this.ring.push(rec);
          if (this.ring.length > EVENT_RING_CAPACITY) {
            this.ring.splice(0, this.ring.length - EVENT_RING_CAPACITY);
          }
          for (const sink of this.sinks) sink.send(rec);
        } catch { /* skip malformed line */ }
      }
    } catch {
      this.stopFileWatch();
    }
  }

  /* ---------------- summary ---------------- */

  toSummary(): SessionSummary {
    return {
      id: this.id,
      name: this.name,
      running: this.running,
      busy: this.busy,
      model: this.model,
      messageCount: this.messageCount,
      createdAt: this.createdAt,
      lastActivityAt: this.lastActivityAt,
      lastMessageAt: this.lastActivityAt,
      error: this.error,
      phase: this.phase,
      owner: this.proc ? 'bridge' : 'none',
      source: this.source,
      workdir: this.workdir,
      active: this.busy || this.lastActivityAt > Date.now() - 5 * 60_000 && this.running,
      live: false,
      livePid: null,
      writing: this.busy,
    };
  }
}
