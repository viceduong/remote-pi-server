/**
 * Shared wire types: pi RPC protocol (JSONL) + Remote Pi HTTP API.
 */

/* ---------------- pi RPC protocol (subset of pi docs/rpc.md) ---------------- */

/** A command response record from pi (`type: "response"`). */
export interface RpcResponse {
  id?: string;
  type: 'response';
  command: string;
  success: boolean;
  data?: unknown;
  error?: string;
}

/** A streamed event from pi. Event type is discriminated by `type`. */
export interface RpcEvent {
  type: string;
  [key: string]: unknown;
}

/** Assistant streaming delta inside `message_update`. */
export interface AssistantMessageEvent {
  type:
    | 'start' | 'text_start' | 'text_delta' | 'text_end'
    | 'thinking_start' | 'thinking_delta' | 'thinking_end'
    | 'toolcall_start' | 'toolcall_delta' | 'toolcall_end'
    | 'done' | 'error';
  contentIndex?: number;
  delta?: string;
  content?: string;
  [key: string]: unknown;
}

/** AgentMessage as emitted by pi (role/content blocks). */
export interface AgentMessage {
  role: 'user' | 'assistant' | 'tool' | 'toolResult' | 'custom' | string;
  content?: string | ContentBlock[];
  id?: string;
  timestamp?: number;
  model?: string;
  api?: string;
  provider?: string;
  errorMessage?: string;
  toolName?: string;
  isError?: boolean;
  customType?: string;
  [key: string]: unknown;
}

export interface ContentBlock {
  type: 'text' | 'thinking' | 'toolCall' | 'toolResult' | 'image';
  text?: string;
  thinking?: string;
  id?: string;
  name?: string;
  arguments?: unknown;
  [key: string]: unknown;
}

/** get_state response payload. */
export interface AgentState {
  sessionId?: string;
  sessionFile?: string;
  sessionName?: string;
  model?: { provider?: string; modelId?: string; id?: string } | null;
  isStreaming?: boolean;
  isCompacting?: boolean;
  messageCount?: number;
  [key: string]: unknown;
}

/* ---------------- Remote Pi HTTP API ---------------- */

/** Chat message rendered for the iOS app. */
export interface ChatMessage {
  /** pi entry id ("m…") — used for forking from a message. */
  id?: string | null;
  role: 'user' | 'assistant' | 'tool';
  text: string;
  thinking: string | null;
  toolCalls: { id: string | null; name: string; arguments: Record<string, unknown> }[];
  toolName?: string | null;
  isError?: boolean;
  /** Prune summaries etc. — pi serves them as role "user" but they are notes. */
  system?: boolean;
  model: string | null;
  timestamp: number | null;
  errorMessage: string | null;
  /** CONTRACT §3: set on user rows the server can map to a delivered queue item. */
  clientMessageId?: string;
}

/** Runtime phase of a session (derived from the owner's event stream). */
export type SessionPhase = 'idle' | 'streaming' | 'awaitingInput' | 'terminated';

/** Who owns the session's agent runtime (the single writer, from leases). */
export type SessionOwner = 'none' | 'tui' | 'bridge';

/** CONTRACT §5 status phase carried by `agent_status` frames. */
export type StatusPhase = 'idle' | 'waiting' | 'streaming' | 'error' | 'exited';

export interface AgentStatusFrame {
  type: 'agent_status';
  phase: StatusPhase;
  busy: boolean;
  /** Legacy alias of `busy` for pre-v2 iOS builds. */
  working: boolean;
  error?: string;
  owner: SessionOwner;
  [key: string]: unknown;
}

export interface SessionSummary {
  id: string;
  phase: SessionPhase;
  owner: SessionOwner;
  name: string;
  running: boolean;
  busy: boolean;
  model: string | null;
  messageCount: number;
  createdAt: number;
  lastActivityAt: number;
  error: string | null;
  /** Timestamp of the actual last message (not file mtime — pi touches files on open). */
  lastMessageAt: number | null;
  /** Working directory of the agent (file cwd for discovered sessions). */
  workdir: string | null;
  /** 'app' = created through the bridge, 'pi' = pre-existing pi session. */
  source: 'app' | 'pi';
  /** True when the file was modified recently (likely live on the host). */
  active: boolean;
  /** True when a host TUI holds the ownership lease (owner === 'tui'). */
  live: boolean;
  /** Lease holder pid when owner === 'tui'. */
  livePid: number | null;
  /** True when the session file was written in the last 30s. */
  writing: boolean;
}

export interface ServerConfig {
  name: string;
  piVersion: string | null;
  api: string;
  workdir: string;
  features: string[];
}

/** /api/status `liveInstances[]` — now derived from held TUI leases. */
export interface LiveInstance {
  pid: number;
  cwd: string | null;
  startedAt: number | null;
  args: string;
  sessionId: string | null;
}
