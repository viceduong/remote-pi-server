import type { ChatMessage } from './types.js';

/**
 * CONTRACT §4 — the single visibility rule (focus mode). A row is visible iff:
 *  - role user and trimmed text non-empty (and not a system/extension note);
 *  - role assistant and trimmed text EXCLUDING thinking non-empty;
 *  - an error row (`isError`, role `error`, or an assistant turn that ended
 *    in an error — `errorMessage` set);
 * Thinking-only assistant rows, tool calls, tool results and custom /
 * extension messages are NOT visible. (agent_question cards are SSE-only and
 * never part of /messages.) iOS `isVisibleInFocus(_:)` mirrors this exactly.
 */
export function isVisibleMessage(m: ChatMessage): boolean {
  const role = m.role as string;
  if (role === 'tool' || (typeof m.toolName === 'string' && m.toolName.length > 0)) return false;
  if (role === 'error' || m.isError === true) return true;
  if (m.system === true) return false;
  const text = (m.text ?? '').trim();
  if (role === 'user') return text.length > 0;
  if (role === 'assistant') {
    if (text.length > 0) return true;
    return typeof m.errorMessage === 'string' && m.errorMessage.trim().length > 0;
  }
  return false;
}
