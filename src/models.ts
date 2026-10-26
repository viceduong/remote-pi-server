/**
 * CONTRACT-V2 §11 model picker: normalize pi `Model` objects (pi-ai
 * `Model<Api>`: {id, name, provider, reasoning, contextWindow, ...}) into the
 * wire `ModelEntry` shape, from either transport:
 *  - RPC child: `get_available_models` → `{models: Model[]}`, `get_state.model`;
 *  - TUI owner v6: `get_available_models` → `{models, current}` (already slim).
 */

export interface ModelEntry {
  provider: string;
  id: string;
  name: string;
  contextWindow?: number;
  reasoning?: boolean;
}

export interface ModelRef {
  provider: string;
  id: string;
}

export interface ModelListing {
  models: ModelEntry[];
  current: ModelRef | null;
}

/** One pi Model (or already-normalized entry) → ModelEntry; null when unusable. */
export function normalizeModel(raw: unknown): ModelEntry | null {
  if (!raw || typeof raw !== 'object') return null;
  const m = raw as Record<string, unknown>;
  const id = typeof m.id === 'string' && m.id ? m.id : typeof m.modelId === 'string' && m.modelId ? m.modelId : '';
  const provider = typeof m.provider === 'string' ? m.provider : '';
  if (!id || !provider) return null;
  const entry: ModelEntry = { provider, id, name: typeof m.name === 'string' && m.name ? m.name : id };
  if (typeof m.contextWindow === 'number' && Number.isFinite(m.contextWindow) && m.contextWindow > 0) {
    entry.contextWindow = Math.round(m.contextWindow);
  }
  if (typeof m.reasoning === 'boolean') entry.reasoning = m.reasoning;
  return entry;
}

/** `{provider, id}` from a pi Model / ModelRef; null when not identifiable. */
export function modelRefOf(raw: unknown): ModelRef | null {
  const e = normalizeModel(raw);
  return e ? { provider: e.provider, id: e.id } : null;
}

/** Accepts `Model[]` or `{models: Model[]}`; dedupes by provider+id; sorts provider, then name. */
export function normalizeModelList(raw: unknown): ModelEntry[] {
  const list = Array.isArray(raw)
    ? raw
    : raw && typeof raw === 'object' && Array.isArray((raw as { models?: unknown }).models)
      ? (raw as { models: unknown[] }).models
      : [];
  const seen = new Map<string, ModelEntry>();
  for (const item of list) {
    const e = normalizeModel(item);
    if (!e) continue;
    const key = `${e.provider}\u0000${e.id}`;
    if (!seen.has(key)) seen.set(key, e);
  }
  const cmp = (a: string, b: string): number => a.localeCompare(b, 'en', { sensitivity: 'base' }) || (a < b ? -1 : a > b ? 1 : 0);
  return [...seen.values()].sort((a, b) => cmp(a.provider, b.provider) || cmp(a.name, b.name) || cmp(a.id, b.id));
}

/** Session summary string form (unchanged from v1): `provider/id`. */
export function modelString(ref: ModelRef): string {
  return `${ref.provider}/${ref.id}`;
}

export type ResolveResult =
  | { ok: true; model: ModelEntry }
  | { ok: false; reason: 'not_found' | 'ambiguous'; candidates?: ModelEntry[] };

/**
 * Resolve a POST body against the available list. Provider given → exact
 * match. Legacy `{modelId}` only → unique id across providers; a
 * `provider/id` string is accepted when it names exactly one model.
 */
export function resolveModel(models: ModelEntry[], modelId: string, provider?: string): ResolveResult {
  if (provider) {
    const hit = models.find((m) => m.provider === provider && m.id === modelId);
    return hit ? { ok: true, model: hit } : { ok: false, reason: 'not_found' };
  }
  const byId = models.filter((m) => m.id === modelId);
  if (byId.length === 1) return { ok: true, model: byId[0]! };
  if (byId.length > 1) return { ok: false, reason: 'ambiguous', candidates: byId };
  const slash = modelId.indexOf('/');
  if (slash > 0) {
    const p = modelId.slice(0, slash);
    const rest = modelId.slice(slash + 1);
    const hit = models.find((m) => m.provider === p && m.id === rest);
    if (hit) return { ok: true, model: hit };
  }
  return { ok: false, reason: 'not_found' };
}

/** Owner protocol needed for `get_available_models` / `model_changed`. */
export const OWNER_MODELS_PROTOCOL = 6;
export const OWNER_MODELS_CAPABILITY = 'models';
export const OWNER_TOO_OLD_MESSAGE =
  'The host pi TUI runs an older Remote Pi owner extension that cannot list models — update the host pi owner extension (restart/reload the TUI)';
