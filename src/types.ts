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
  type: 'text' | 'thinking' | 'toolCall' | 'image';
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
}

export interface SessionSummary {
  id: string;
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
  /** True when an external (non-bridge) pi process owns this session. */
  live: boolean;
  livePid: number | null;
  /** True when the session file was written in the last 30s (agent working now). */
  writing: boolean;
}

export interface ServerConfig {
  name: string;
  piVersion: string | null;
  api: string;
  workdir: string;
  features: string[];
}

export interface LiveInstance {
  pid: number;
  cwd: string | null;
  startedAt: number | null;
  args: string;
  sessionId: string | null;
}
