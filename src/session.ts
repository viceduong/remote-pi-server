import { once } from 'node:events';
import net from 'node:net';
import path from 'node:path';
import fs from 'node:fs';
import type { ChildProcess } from 'node:child_process';
import type { Logger } from 'pino';
import { killPiProcess, spawnPiProcess } from './pi.js';
import { mapAgentMessage } from './history.js';
import type { Lease } from './lease.js';
import type { AgentMessage, AgentState, RpcEvent, RpcResponse, SessionPhase, SessionSummary } from './types.js';
import type { QueueItem } from './queue.js';

const EVENT_RING_CAPACITY = 500;
/** Ring is byte-capped too — 500 × 2MB payloads would OOM the server. */
const EVENT_RING_MAX_BYTES = 8 * 1024 * 1024;
const RPC_TIMEOUT_MS = 10_000;
const FILE_WATCH_DEBOUNCE_MS = 250;
const MAX_RPC_LINE_BYTES = 16 * 1024 * 1024;

/** One event record in the replay ring. */
export interface RingRecord {
  seq: number;
  type: string;
  data: RpcEvent;
}

/** Destination for streamed events (SSE connection). */
export interface EventSink {
  send(record: RingRecord): void;
  close?(): void;
  /** When true, tool output payloads are truncated (focus-mode clients). */
  skeleton?: boolean;
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
  id: string;
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
   *  vanish and never double-send). Durable: persisted to disk. */
  queue: QueueItem[] = [];
  /** Called when a turn completes and the agent is ready for the next prompt. */
  onIdle: (() => void) | null = null;
  /** Called whenever the child exits so durable queue state can recover. */
  onExit: (() => void) | null = null;

  /**
   * pi's own session id (JSONL header id, from get_state). The manager keys
   * sessions by this once known — the file basename is just a storage name
   * (the bridge placeholder id) and keying by it split one session into two
   * list entries (the iOS duplicate + 409-on-send bug).
   */
  piId: string | null = null;
  /** Runtime phase derived from the owner's event stream. */
  phase: SessionPhase = 'idle';
  /** Read-only mirror mode: file watcher only, no pi process (convertible). */
  readOnly = false;
  /** Queue hooks wired (idempotency flag for wireQueue). */
  queueWired = false;
  model: string | null = null;
  messageCount = 0;
  error: string | null = null;

  private proc: ChildProcess | null = null;
  /** Owner-proxy transport: live host TUI holding the session lease. */
  private ownerSocket: net.Socket | null = null;
  ownerLease: Lease | null = null;
  private ownerBuffer = '';
  private buffer = '';
  private readonly ring: RingRecord[] = [];
  private ringBytes = 0;
  private readonly stderrTail: string[] = [];
  private readonly sinks = new Set<EventSink>();

  sinkCount(): number {
    return this.sinks.size;
  }

  closeOldestSink(): void {
    const oldest = this.sinks.values().next().value as EventSink | undefined;
    if (oldest) {
      this.sinks.delete(oldest);
      oldest.close?.();
    }
  }
  private readonly pending = new Map<string, PendingRpc>();
  seq = 0;
  private rpcCounter = 0;
  private stopping = false;
  private restartTimer: NodeJS.Timeout | null = null;
  private promptReserved = false;
  /** True once the child process has terminated (spawn failure or exit). */
  closed = false;

  /* ----- file-tail live push (host/other-client activity) ----- */
  private fileWatcher: fs.FSWatcher | null = null;
  private fileOffset = 0;
  private fileRemainder = '';
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
    return this.proc !== null || this.ownerSocket !== null;
  }

  /** True while this session is proxied to a live host owner via IPC. */
  get isProxy(): boolean {
    return this.ownerSocket !== null;
  }

  /** Child pid of the live pi process (null when not running). */
  get pid(): number | null {
    return this.proc?.pid ?? null;
  }

  reservePrompt(): boolean {
    if (this.promptReserved || this.busy || this.phase === 'streaming') return false;
    this.promptReserved = true;
    return true;
  }

  releasePrompt(): void {
    this.promptReserved = false;
  }

  /* ---------------- RPC plumbing ---------------- */

  /** Send a fire-and-forget command (e.g. abort). */
  send(obj: Record<string, unknown>): boolean {
    if (this.ownerSocket) {
      try {
        this.ownerSocket.write(`${JSON.stringify(obj)}\n`);
        return true;
      } catch {
        return false;
      }
    }
    if (!this.proc?.stdin || this.proc.stdin.destroyed) return false;
    try {
      this.proc.stdin.write(`${JSON.stringify(obj)}\n`);
      return true;
    } catch {
      return false;
    }
  }

  /** Send a command and await its `response` record. */
  request<T = unknown>(obj: Record<string, unknown>, timeoutMs = RPC_TIMEOUT_MS): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const proc = this.proc;
      const viaOwner = this.ownerSocket !== null && !proc?.stdin;
      if (!proc?.stdin && !this.ownerSocket) {
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
      try {
        const wire = viaOwner ? { ...obj, op: obj.type, type: undefined } : { ...obj, id };
        if (viaOwner) (wire as Record<string, unknown>).id = id;
        const line = JSON.stringify(wire);
        if (viaOwner) this.ownerSocket!.write(line + '\n');
        else proc!.stdin!.write(`${line}\n`);
      } catch (err) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(err as Error);
      }
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

  /** Push a lightweight status frame to skeleton sinks. Status is derived
   *  server-side from the authoritative busy/phase/error fields. */
  private pushStatus(): void {
    const status = {
      working: this.busy || this.phase === 'streaming',
      phase: this.phase,
      error: this.error,
    };
    const record = { seq: ++this.seq, type: 'agent_status', data: { type: 'agent_status', ...status } as unknown as RpcEvent };
    for (const sink of this.sinks) {
      if ((sink as { skeleton?: boolean }).skeleton) sink.send(record);
    }
  }

  handleEvent(obj: RpcEvent): void {
    this.lastActivityAt = Date.now();
    if (obj.type === 'turn_start') {
      this.busy = true;
      this.lastTurnStartAt = Date.now();
      this.phase = 'streaming';
      this.error = null;
      this.pushStatus();
    }
    else if (obj.type === 'turn_end') {
      // Pi can still be internally processing between turn_end and agent_end.
      // Keep busy=true so a concurrent prompt cannot enter that gap.
      this.phase = 'awaitingInput';
    }
    else if (obj.type === 'agent_end') {
      this.busy = false;
      this.phase = 'awaitingInput';
      this.promptReserved = false;
      this.pushStatus();
      this.onIdle?.();
    }
    else if (obj.type === 'agent_settled') {
      this.pushStatus();
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
    if (obj.type === 'error') {
      this.error = String((obj.error as { message?: string } | string) ?? 'agent error');
      this.pushStatus();
    }

    // file_update payloads are live-only (replay would double-append in the
    // app) and can be huge — keep them OUT of the replay ring entirely.
    if (obj.type === 'file_update') {
      const record: RingRecord = { seq: ++this.seq, type: obj.type, data: obj };
      for (const sink of this.sinks) sink.send(record);
      return;
    }
    const wire = this.trimEventForWire(obj);
    const record: RingRecord = { seq: ++this.seq, type: wire.type, data: wire };
    this.ring.push(record);
    this.ringBytes += Buffer.byteLength(JSON.stringify(record.data));
    while (this.ring.length > EVENT_RING_CAPACITY || this.ringBytes > EVENT_RING_MAX_BYTES) {
      const oldest = this.ring.shift();
      if (!oldest) break;
      this.ringBytes -= Buffer.byteLength(JSON.stringify(oldest.data));
    }
    for (const sink of this.sinks) {
      if (sink.skeleton && this.isToolHeavyEvent(record)) {
        sink.send({ ...record, data: this.skeletonizeRecordData(record.data) } as RingRecord);
      } else {
        sink.send(record);
      }
    }
  }

  // Streaming decoders: pi pipes can carry invalid UTF-8; toString() on it
  // crashes Node (StringBytes assertion). TextDecoder replaces + carries
  // split multibyte sequences across chunks (stream:true).
  private rpcDecoder = new TextDecoder('utf8');
  private ownerDecoder = new TextDecoder('utf8');
  private watchDecoder = new TextDecoder('utf8');

  private onStdoutChunk(chunk: Buffer): void {
    this.buffer += this.rpcDecoder.decode(chunk, { stream: true });
    if (Buffer.byteLength(this.buffer, 'utf8') > MAX_RPC_LINE_BYTES) {
      const newline = this.buffer.indexOf('\n');
      this.buffer = newline >= 0 ? this.buffer.slice(newline + 1) : '';
      this.log.warn({ sessionId: this.id }, 'pi RPC line exceeded safety limit');
    }
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
    this.fileRemainder = '';
    this.startFileWatch();
  }

  /** Spawn the pi process and prime session metadata via get_state. */
  async start(): Promise<void> {
    if (this.readOnly) return this.startReadOnly();
    if (this.proc) return;
    if (this.ownerSocket) return; // proxy mode — already attached
    const args = [
      '--mode', 'rpc',
      '--session', this.file,
      '--session-dir', this.sessionDir,
      '--name', this.name,
      ...this.extraArgs,
    ];
    this.log.info({ sessionId: this.id }, `spawn pi ${this.bin} ${args.join(' ')}`);

    const child = spawnPiProcess(this.bin, args, this.workdir);
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }
    this.proc = child;
    this.buffer = '';
    this.stopping = false;
    this.promptReserved = false;
    this.closed = false;
    this.error = null;
    this.phase = 'idle';

    child.stdout?.on('data', (d: Buffer) => this.onStdoutChunk(d));
    child.stderr?.on('data', (d: Buffer) => {
      const line = new TextDecoder('utf8').decode(d).trim();
      this.stderrTail.push(line);
      if (this.stderrTail.length > 40) this.stderrTail.shift();
      this.log.debug({ sessionId: this.id, chunk: line }, 'pi stderr');
    });

    child.on('error', (err) => {
      this.error = err.message;
      this.log.error({ sessionId: this.id, err: err.message }, 'pi spawn failed');
      this.pushStatus();
    });

    child.on('close', (code, signal) => {
      // ALARM: any exit while the agent was mid-turn is logged as ERROR even
      // if our own stop() did it (the guards should prevent that — if this
      // fires, a caller is stopping a streaming agent and we need to know).
      if (this.phase === 'streaming' || this.busy) {
        this.log.error(
          { sessionId: this.id, code, signal, stopping: this.stopping, phase: this.phase, busy: this.busy },
          'agent stopped MID-TURN',
        );
        const crashRecord = {
          seq: ++this.seq,
          type: 'agent_crashed',
          data: { type: 'agent_crashed', code, midTurn: true } as RpcEvent,
        };
        for (const sink of this.sinks) sink.send(crashRecord);
      }
      // Signal exits report code=null. They are still unexpected unless our
      // own stop() initiated them, and must be recovered.
      const unexpected = !this.stopping && (code !== 0 || signal !== null);
      if (unexpected) {
        this.log.error(
          { sessionId: this.id, code, stderr: this.stderrTail.slice(-20) },
          'pi exited unexpectedly',
        );
        const crashRecord = {
          seq: ++this.seq,
          type: 'agent_crashed',
          data: { type: 'agent_crashed', code, stderr: this.stderrTail.slice(-20) } as RpcEvent,
        };
        for (const sink of this.sinks) sink.send(crashRecord);
        // Auto-recovery: respawn (resume) shortly after an unexpected exit.
        this.restartTimer = setTimeout(() => {
          this.restartTimer = null;
          if (!this.proc && !this.stopping) void this.start().catch(() => {});
        }, 1000);
      }
      this.log.info({ sessionId: this.id, code, signal }, 'pi exited');
      this.proc = null;
      this.closed = true;
      this.busy = false;
      this.promptReserved = false;
      this.phase = 'terminated';
      // Authoritative not-working frame for connected skeleton clients
      // (agent_exited alone was ignored by them, leaving stale status).
      this.pushStatus();
      for (const p of this.pending.values()) {
        clearTimeout(p.timer);
        p.reject(new Error('Agent exited'));
      }
      this.pending.clear();
      this.onExit?.();
      const exitRecord = {
        seq: ++this.seq,
        type: 'agent_exited',
        data: { type: 'agent_exited', code } as RpcEvent,
      };
      for (const sink of [...this.sinks]) {
        sink.send(exitRecord);
        sink.close?.();
      }
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
    // Extension-heavy Pi startups can take >10s before RPC is responsive.
    // State priming is advisory; give it a longer window without weakening
    // normal command timeouts.
    const st = await this.request<AgentState>({ type: 'get_state' }, 30_000);
    if (st.sessionId) {
      this.piId = st.sessionId;
      this.file = st.sessionFile ?? this.file;
    }
    if (st.sessionName) this.name = st.sessionName;
    if (st.model) this.model = `${st.model.provider}/${st.model.modelId ?? st.model.id ?? ''}`;
    if (typeof st.messageCount === 'number') this.messageCount = st.messageCount;
    if (st.isStreaming) {
      this.busy = true;
      this.phase = 'streaming';
    }
  }

  /** Terminate the child process. Safe to call multiple times. */
  stop(): void {
    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }
    this.stopping = true;
    this.promptReserved = false;
    const sock = this.ownerSocket;
    if (sock) {
      // Proxy mode: never kill the host owner — just drop our control pipe.
      try { sock.end(); } catch { /* ignore */ }
      this.ownerSocket = null;
      this.busy = false;
      this.phase = 'terminated';
      for (const p of this.pending.values()) {
        clearTimeout(p.timer);
        p.reject(new Error('Owner connection closed'));
      }
      this.pending.clear();
      return;
    }
    const proc = this.proc;
    if (!proc) return;
    try { proc.stdin?.end(); } catch { /* ignore */ }
    killPiProcess(proc);
  }

  /**
   * Attach to a live host TUI holding the session lease (owner-proxy mode).
   * The owner extension speaks pi-rpc-shaped NDJSON over a local pipe/UDS,
   * so responses/events flow through the exact same handleResponse /
   * handleEvent pipeline used for spawned children.
   */
  async attachOwner(lease: Lease): Promise<void> {
    if (this.proc) throw new Error('attachOwner: child already running');
    if (!lease.ipc) throw new Error('attachOwner: lease has no ipc endpoint');
    await new Promise<net.Socket>((resolve, reject) => {
      const socket = net.connect(lease.ipc); // named pipe or UDS path both work
      socket.once('connect', () => resolve(socket));
      socket.once('error', (err) => reject(err));
      setTimeout(() => reject(new Error('owner connect timeout')), 3000).unref();
    }).then(async (socket) => {
      this.ownerSocket = socket;
      this.ownerLease = lease;
      this.buffer = '';
      this.ownerBuffer = '';
      this.stopping = false;
      this.closed = false;
      this.error = null;
      this.phase = 'idle';
      this.log.info({ sessionId: this.id, ipc: lease.ipc }, 'attached to live owner');

      socket.on('data', (d: Buffer) => this.onOwnerChunk(d));
      socket.on('error', (err) => this.log.warn({ sessionId: this.id, err: err.message }, 'owner ipc error'));
      socket.on('close', () => {
        if (this.ownerSocket !== socket) return;
        this.ownerSocket = null;
        this.busy = false;
        this.phase = 'terminated';
        this.pushStatus();
        for (const p of this.pending.values()) {
          clearTimeout(p.timer);
          p.reject(new Error('Owner disconnected'));
        }
        this.pending.clear();
        const exitRecord = {
          seq: ++this.seq,
          type: 'agent_exited',
          data: { type: 'agent_exited', code: 0 } as RpcEvent,
        };
        for (const sink of [...this.sinks]) sink.send(exitRecord);
        this.onExit?.();
      });
      // Handshake FIRST: the owner's IPC server rejects every op from
      // un-helloed sockets with "not attached (hello required)". Send hello
      // as a bridge attachment, await its response, then prime metadata.
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('owner hello timeout')), 3000).unref();
        const onHello = (d: Buffer) => {
          try {
            const line = new TextDecoder('utf8').decode(d).split('\n')[0] ?? '';
            const obj = JSON.parse(line) as { type?: string; success?: boolean };
            if (obj.type === 'response') {
              socket.removeListener('data', onHello);
              clearTimeout(timer);
              if (obj.success === false) { reject(new Error('owner hello rejected')); return; }
              resolve();
            }
          } catch { /* partial line — wait for more */ }
        };
        socket.on('data', onHello);
        socket.write(JSON.stringify({ id: 'hello', op: 'hello', type: 'bridge', lastSeq: 0 }) + '\n');
      });
      // Prime metadata like spawn path does.
      return this.request<{ isStreaming?: boolean }>({ type: 'get_state' })
        .then((st) => {
          this.busy = st.isStreaming === true;
          if (st.isStreaming) this.phase = 'streaming';
        })
        .catch(() => {});
    });
  }

  private onOwnerChunk(chunk: Buffer): void {
    this.ownerBuffer += this.ownerDecoder.decode(chunk, { stream: true });
    let idx: number;
    while ((idx = this.ownerBuffer.indexOf('\n')) >= 0) {
      const line = this.ownerBuffer.slice(0, idx).replace(/\r$/, '');
      this.ownerBuffer = this.ownerBuffer.slice(idx + 1);
      if (!line.trim()) continue;
      let obj: unknown;
      try {
        obj = JSON.parse(line);
      } catch {
        continue;
      }
      const r = obj as { type?: string };
      if (r.type === 'response') {
        this.handleResponse(obj as RpcResponse);
      } else if (r.type === 'event') {
        // Attached-socket frame from remote-pi-owner: {type:'event', seq,
        // eventType, event}. Unwrap to the real event so busy tracking,
        // the ring, and SSE fan-out see canonical event types.
        const frame = obj as { event?: RpcEvent };
        if (frame.event) this.handleEvent(frame.event);
      } else {
        this.handleEvent(obj as RpcEvent);
      }
    }
  }

  /* ---------------- SSE fan-out ---------------- */

  /**
   * Bandwidth trim: pi's message_update carries the growing partial message
   * PLUS the delta. The app consumes only the delta for text/thinking streams,
   * so ship just the role (keeps tool-vs-assistant routing correct).
   */
  /** Skeleton mode: truncate tool output in streamed events. Full output is
   *  persisted in the JSONL and fetchable via /toolresult/:toolCallId. */
  skeleton = false;

  private static SKELETON_BYTES = 256;

  private skeletonizeToolMessage(msg: unknown): unknown {
    if (!msg || typeof msg !== 'object') return msg;
    const m = msg as Record<string, unknown>;
    const content = m.content;
    if (typeof content === 'string') {
      if (content.length <= Session.SKELETON_BYTES) return m;
      return { ...m, outputTruncated: true,
        content: content.slice(0, Session.SKELETON_BYTES) };
    }
    if (Array.isArray(content)) {
      let truncated = false;
      const blocks = (content as { type?: string; text?: string }[]).map((b) => {
        if (b.type !== 'text' || !b.text || b.text.length <= Session.SKELETON_BYTES) return b;
        truncated = true;
        return { ...b, text: b.text.slice(0, Session.SKELETON_BYTES), truncated: true };
      });
      return truncated ? { ...m, outputTruncated: true, content: blocks } : m;
    }
    return m;
  }


  /** Events whose payload is dominated by tool output text. */
  /** True for tool-call/output events (focus clients drop them entirely). */
  isToolEvent(record: { type: string; data: unknown }): boolean {
    if (record.type.startsWith('tool_execution')) return true;
    const msg = (record.data as { message?: { role?: string; toolName?: string } }).message;
    if (msg?.role === 'tool' || msg?.role === 'toolResult' || msg?.toolName) return true;
    return false;
  }

  isToolHeavyEvent(record: { type: string; data: unknown }): boolean {
    if (record.type === 'tool_execution_update' || record.type === 'tool_execution_end') return true;
    if (record.type === 'message_end' || record.type === 'message_start') {
      const msg = (record.data as { message?: { role?: string } }).message;
      return msg?.role === 'tool' || msg?.role === 'toolResult';
    }
    return false;
  }

  skeletonizeRecordData(data: unknown): unknown {
    const d = data as Record<string, unknown>;
    if (d.partialResult) return { ...d, partialResult: this.skeletonizeToolMessage(d.partialResult) };
    if (d.result) return { ...d, result: this.skeletonizeToolMessage(d.result) };
    if (d.message) return { ...d, message: this.skeletonizeToolMessage(d.message) };
    return d;
  }

  private trimEventForWire(obj: RpcEvent): RpcEvent {
    // Skeleton mode: shrink tool result payloads before fan-out.
    if (this.skeleton) {
      if (obj.type === 'message_end' || obj.type === 'message_start') {
        const msg = obj.message as { role?: string } | undefined;
        if (msg && (msg.role === 'tool' || msg.role === 'toolResult')) {
          return { ...obj, message: this.skeletonizeToolMessage(obj.message) };
        }
      }
      if (obj.type === 'tool_execution_update') {
        const partial = obj.partialResult as { content?: unknown } | undefined;
        if (partial?.content) {
          return { ...obj, partialResult: this.skeletonizeToolMessage(partial) };
        }
      }
      if (obj.type === 'tool_execution_end') {
        const result = obj.result as { content?: unknown } | undefined;
        if (result?.content) {
          return { ...obj, result: this.skeletonizeToolMessage(result) };
        }
      }
    }
    if (obj.type !== 'message_update') return obj;
    const ev = obj.assistantMessageEvent as { type?: string } | undefined;
    if (ev && (ev.type === 'text_delta' || ev.type === 'thinking_delta')) {
      const msg = obj.message as { role?: string } | undefined;
      return { ...obj, message: { role: msg?.role } };
    }
    return obj;
  }

  /** Fan a synthetic event out to current subscribers (queue_update etc.). */
  broadcast(type: string, data: Record<string, unknown>): void {
    for (const sink of this.sinks) {
      sink.send({ seq: ++this.seq, type, data: { type, ...data } as RpcEvent });
    }
  }

  subscribe(sink: EventSink): void {
    this.sinks.add(sink);
    // Bridge-owned sessions already stream canonical RPC events. Watching
    // their own JSONL duplicates every message; mirrors alone need the tailer.
    if (this.sinks.size === 1 && this.readOnly) this.startFileWatch();
  }

  unsubscribe(sink: EventSink): void {
    this.sinks.delete(sink);
    if (this.sinks.size === 0 && this.readOnly) this.stopFileWatch();
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
    this.fileRemainder = '';
    if (this.sinks.size > 0 && this.readOnly) this.startFileWatch();
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
        this.fileRemainder = '';
      } catch {
        this.fileOffset = 0; // file not created yet
        this.fileRemainder = '';
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
        // Compaction/rewrite: restart from a clean JSONL boundary.
        this.fileOffset = 0;
        this.fileRemainder = '';
      }
      if (stat.size === this.fileOffset) return;
      const fd = fs.openSync(this.file, 'r');
      const buf = Buffer.alloc(stat.size - this.fileOffset);
      try {
        fs.readSync(fd, buf, 0, buf.length, this.fileOffset);
      } finally {
        fs.closeSync(fd);
      }
      const text = this.fileRemainder + this.watchDecoder.decode(buf, { stream: true });
      const parts = text.split('\n');
      this.fileRemainder = parts.pop() ?? '';
      this.fileOffset = stat.size - Buffer.byteLength(this.fileRemainder, 'utf8');
      const mappedList: ReturnType<typeof mapAgentMessage>[] = [];
      let lastRole: string | undefined;
      for (const line of parts) {
        if (!line.trim()) continue;
        try {
          const entry = JSON.parse(line) as { type?: string; id?: string; message?: AgentMessage };
          if (entry.type !== 'message' || !entry.message) continue;
          const msgWithId = { ...entry.message } as AgentMessage;
          if (entry.id) msgWithId.id = entry.id;
          const mapped = mapAgentMessage(msgWithId);
          if (!mapped) continue;
          lastRole = mapped.role;
          mappedList.push(mapped);
        } catch { /* skip malformed line */ }
      }
      if (mappedList.length === 0) return;
      // Server-derived working flag: file just changed + last entry is a user
      // prompt awaiting a reply (or our own process is mid-turn). Survives
      // client reconnects mid-turn (file_update events aren't replayable).
      const fresh = Date.now() - stat.mtimeMs < 120_000;
      const working = this.busy || this.phase === 'streaming' || (fresh && lastRole === 'user');
      for (const mapped of mappedList) {
        for (const sink of this.sinks) {
          sink.send({
            seq: ++this.seq,
            type: 'file_update',
            data: { type: 'file_update', message: mapped, working } as RpcEvent,
          });
        }
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
