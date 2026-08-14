import type { AgentMessage, ChatMessage, ContentBlock } from './types.js';

/**
 * Map pi AgentMessage objects to the flat ChatMessage shape the iOS app
 * renders. Tool-call blocks become structured entries; thinking is kept
 * separate from visible text so clients can collapse it.
 */
export function mapAgentMessage(msg: AgentMessage | null | undefined): ChatMessage | null {
  if (!msg) return null;

  const role =
    msg.role === 'tool' || msg.role === 'toolResult'
      ? 'tool'
      : msg.role === 'assistant'
        ? 'assistant'
        : 'user';

  // pi 0.79 stores tool results as messages with role "toolResult" (and
  // sometimes assistant/user) carrying a `toolName` field. A toolName means
  // tool output — classify it as a tool message so clients render a tool
  // block instead of a user/assistant bubble.
  const hasToolName = typeof msg.toolName === 'string' && msg.toolName.length > 0;
  // Legacy shapes (older pi / custom routers): a message whose content is
  // exclusively toolCall/toolResult blocks with no text is a tool message
  // even without toolName.
  const contentArr = Array.isArray(msg.content) ? (msg.content as ContentBlock[]) : [];
  // Assistant tool-call messages must remain assistant messages. Treating a
  // toolCall-only assistant message as a tool result orphaned later outputs
  // and caused upstream "role tool must follow tool_calls" 400s.
  const hasToolResultBlock = contentArr.some((b) => b.type === 'toolResult');
  const isToolResult = hasToolName || msg.role === 'tool' || msg.role === 'toolResult' || hasToolResultBlock;
  // pi serves extension custom messages (e.g. context-prune summaries) with
  // role "custom" + customType — system notes, never user bubbles.
  const isSystemNote =
    msg.role === 'custom' ||
    (typeof msg.customType === 'string' && msg.customType.length > 0);

  const base: ChatMessage = {
    id: typeof msg.id === 'string' ? msg.id : null,
    role: isToolResult ? 'tool' : role,
    text: '',
    thinking: null,
    toolCalls: [],
    toolName: typeof msg.toolName === 'string' ? msg.toolName : null,
    isError: Boolean(msg.isError),
    model: null,
    timestamp: typeof msg.timestamp === 'number' ? msg.timestamp : null,
    errorMessage: typeof msg.errorMessage === 'string' ? msg.errorMessage : null,
  };

  if (isToolResult) {
    base.text = typeof msg.content === 'string'
      ? msg.content
      : textBlocks(msg.content as ContentBlock[] | undefined);
    // Keep call args on tool messages so clients can still show them.
    for (const b of contentArr) {
      if (b.type === 'toolCall') {
        base.toolCalls.push({
          id: b.id ?? null,
          name: b.name ?? 'tool',
          arguments: parseArguments(b.arguments),
        });
      }
    }
    return base;
  }
  if (role === 'user') {
    base.text = typeof msg.content === 'string'
      ? msg.content
      : textBlocks(msg.content as ContentBlock[] | undefined);
    // pi serves context-prune summaries as role "user"/"custom"; system notes.
    base.system = isSystemNote || isPruneSummary(base.text);
  } else if (role === 'assistant') {
    const blocks = (msg.content as ContentBlock[] | undefined) ?? [];    for (const block of blocks) {
      if (block.type === 'text' && block.text) base.text += block.text;
      else if (block.type === 'thinking' && block.thinking) base.thinking = block.thinking;
      else if (block.type === 'toolCall') {
        base.toolCalls.push({
          id: block.id ?? null,
          name: block.name ?? 'tool',
          arguments: parseArguments(block.arguments),
        });
      }
    }
    base.model = typeof msg.model === 'string' && msg.model
      ? msg.model
      : msg.provider && msg.model
        ? `${msg.provider}/${msg.model}`
        : null;
  } else {
    base.text = textBlocks(msg.content as ContentBlock[] | undefined);
  }

  return base;
}
function parseArguments(raw: unknown): Record<string, unknown> {
  if (raw && typeof raw === 'object') return raw as Record<string, unknown>;
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === 'object') return parsed as Record<string, unknown>;
    } catch { /* not JSON */ }
    return { raw };
  }
  return {};
}

function textBlocks(blocks: ContentBlock[] | undefined): string {
  return (blocks ?? [])
    .filter((b): b is ContentBlock & { text: string } => b.type === 'text' && typeof b.text === 'string')
    .map((b) => b.text)
    .join('\n');
}

/** context-prune summary style: "### Tool Call N: …" / "## Tool Call Summary". */
function isPruneSummary(text: string): boolean {
  return /(^|\n)#{1,4}\s*Tool Call/i.test(text);
}
