/**
 * CONTRACT §9 — Q&A transcript rows. The ONE builder that turns the
 * `toolResult` of pi's `ask_user_question` tool into the compact `qa` object
 * every surface carries (/messages rows, message_start/message_end wire
 * events, tailer file_update rows).
 *
 * toolResult shape (pi JSONL): {role:"toolResult", toolName:"ask_user_question",
 *   toolCallId, content:[{type:"text",…}], details:{answers:QuestionAnswer[], cancelled, error?}}
 * Validation failures carry `details: {}` (+ isError) — no qa (stays hidden).
 */

export const ASK_USER_QUESTION_TOOL = 'ask_user_question';

export interface QaItem {
  question: string;
  answer: string;
}

export interface QaInfo {
  cancelled: boolean;
  items: QaItem[];
}

/** pi's QuestionAnswer (rpiv-ask-user-question tool/types.ts). */
interface QuestionAnswerLike {
  questionIndex?: unknown;
  question?: unknown;
  kind?: unknown;
  answer?: unknown;
  selected?: unknown;
  notes?: unknown;
}

const CHAT_ANSWER = 'Chat about this';
const NO_INPUT = '(no input)';

/** option → label; multi → selected joined ", "; custom → typed text; chat → "Chat about this"; + " — notes". */
export function formatQaAnswer(a: QuestionAnswerLike): string {
  const str = (v: unknown): string => (typeof v === 'string' ? v : '');
  let out: string;
  switch (a.kind) {
    case 'chat':
      out = CHAT_ANSWER;
      break;
    case 'multi': {
      const sel = Array.isArray(a.selected) ? a.selected.filter((s): s is string => typeof s === 'string' && s.length > 0) : [];
      out = sel.length ? sel.join(', ') : str(a.answer);
      break;
    }
    default: // option / custom (and unknown kinds): the answer text
      out = str(a.answer);
  }
  if (!out.trim()) out = NO_INPUT;
  const notes = str(a.notes).trim();
  if (notes) out += ` — ${notes}`;
  return out;
}

/** `qa` for an ask_user_question tool result, else null. Accepts AgentMessage-ish objects. */
export function qaFromToolResult(msg: unknown): QaInfo | null {
  if (!msg || typeof msg !== 'object') return null;
  const m = msg as { role?: unknown; toolName?: unknown; details?: unknown };
  if (m.toolName !== ASK_USER_QUESTION_TOOL) return null;
  if (m.role !== 'toolResult' && m.role !== 'tool') return null;
  const d = m.details as { answers?: unknown; cancelled?: unknown } | null | undefined;
  if (!d || typeof d !== 'object') return null;
  const hasAnswers = Array.isArray(d.answers);
  if (!hasAnswers && typeof d.cancelled !== 'boolean') return null;
  if (d.cancelled === true) return { cancelled: true, items: [] };
  const answers = (hasAnswers ? (d.answers as unknown[]) : [])
    .filter((a): a is QuestionAnswerLike => !!a && typeof a === 'object')
    .map((a, i) => ({ a, order: typeof a.questionIndex === 'number' ? a.questionIndex : i }))
    .sort((x, y) => x.order - y.order);
  return {
    cancelled: false,
    items: answers.map(({ a }) => ({
      question: typeof a.question === 'string' ? a.question : '',
      answer: formatQaAnswer(a),
    })),
  };
}
