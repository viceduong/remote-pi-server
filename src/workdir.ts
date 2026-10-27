import fs from 'node:fs';
import path from 'node:path';

/**
 * CONTRACT §12 — session working directories chosen (and optionally created)
 * by the client, confined to the configured roots (`REMOTE_PI_WORKDIR_ROOTS`).
 *
 * A requested path is allowed iff it is syntactically sane (absolute, no UNC /
 * device prefix, no reserved names, no trailing dot/space segments, ≤ 260
 * chars) AND both its lexical `path.resolve` form and its real form (realpath
 * of the nearest existing ancestor + the still-missing tail) lie inside a root.
 * The real form catches junction/symlink escapes; comparison is
 * case-insensitive on Windows.
 */

const IS_WIN = process.platform === 'win32';

export const MAX_WORKDIR_LENGTH = 260;
export const MAX_DIR_ENTRIES = 500;

/** A configured root: as configured (resolved) plus its realpath (when it exists). */
export interface WorkdirRoot {
  path: string;
  real: string;
}

/** Thrown for every §12 rejection; routes map it 1:1 to HTTP. */
export class WorkdirError extends Error {
  constructor(
    readonly status: 400 | 403 | 404,
    readonly apiCode: 'workdir_forbidden' | 'workdir_missing' | 'workdir_not_dir',
    message: string,
    readonly roots?: string[],
  ) {
    super(message);
  }
}

const RESERVED = /^(con|prn|aux|nul|conin\$|conout\$|com[0-9¹²³]|lpt[0-9¹²³])(\..*)?$/i;
const CONTROL = /[\x00-\x1f\x7f]/;
const WIN_INVALID = /[<>:"|?*]/;

function pathApi(win: boolean): path.PlatformPath {
  return win ? path.win32 : path.posix;
}

/** Lowercase on Windows (case-insensitive filesystem). */
function key(p: string, win: boolean): string {
  return win ? p.toLowerCase() : p;
}

/** `child` equals `root` or lies below it (both already resolved). */
export function isInside(child: string, root: string, win = IS_WIN): boolean {
  const sep = win ? '\\' : '/';
  const c = key(child, win);
  const r = key(root, win);
  if (c === r) return true;
  return c.startsWith(r.endsWith(sep) ? r : r + sep);
}

/**
 * Syntactic check of a requested path (no disk access). Returns a reason
 * string when the path must be rejected, else null.
 */
export function syntaxError(input: string, win = IS_WIN): string | null {
  if (typeof input !== 'string' || input.length === 0) return 'empty path';
  if (CONTROL.test(input)) return 'control characters are not allowed';
  if (input.length > MAX_WORKDIR_LENGTH) return `path longer than ${MAX_WORKDIR_LENGTH} characters`;
  let rest: string;
  if (win) {
    if (/^[\\/]{2}/.test(input)) return 'UNC and device paths are not allowed';
    const m = /^[A-Za-z]:[\\/]/.exec(input);
    if (!m) return 'path must be absolute (X:\\...)';
    rest = input.slice(m[0].length);
  } else {
    if (!input.startsWith('/')) return 'path must be absolute';
    if (input.startsWith('//')) return 'network paths are not allowed';
    rest = input.slice(1);
  }
  for (const seg of rest.split(win ? /[\\/]/ : /\//)) {
    // `..` is refused outright: even when it resolves inside a root (`D:\..\x`
    // → `D:\x`) the result is rarely what the user typed.
    if (seg === '..') return '".." segments are not allowed';
    if (seg === '' || seg === '.') continue;
    if (win) {
      if (WIN_INVALID.test(seg)) return `invalid character in "${seg}"`;
      if (RESERVED.test(seg)) return `reserved name "${seg}"`;
      if (/[. ]$/.test(seg)) return `segment "${seg}" ends with a dot or space`;
    }
  }
  const resolved = pathApi(win).resolve(input);
  if (resolved.length > MAX_WORKDIR_LENGTH) return `path longer than ${MAX_WORKDIR_LENGTH} characters`;
  return null;
}

function realOrSelf(p: string): string {
  try {
    return fs.realpathSync.native(p);
  } catch {
    return p;
  }
}

/** Parse `REMOTE_PI_WORKDIR_ROOTS` (`;`-separated); default = the server workdir. */
export function parseRoots(raw: string | undefined, fallback: string): WorkdirRoot[] {
  const items = (raw ?? '').split(';').map((s) => s.trim()).filter(Boolean);
  const list = items.length ? items : [fallback];
  const out: WorkdirRoot[] = [];
  const seen = new Set<string>();
  for (const item of list) {
    if (!path.isAbsolute(item)) throw new Error(`REMOTE_PI_WORKDIR_ROOTS entry is not absolute: ${item}`);
    const resolved = path.resolve(item);
    const k = key(resolved, IS_WIN);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push({ path: resolved, real: realOrSelf(resolved) });
  }
  return out;
}

export function rootPaths(roots: WorkdirRoot[]): string[] {
  return roots.map((r) => r.path);
}

function lexicallyAllowed(p: string, roots: WorkdirRoot[]): boolean {
  return roots.some((r) => isInside(p, r.path) || isInside(p, r.real));
}

function forbidden(roots: WorkdirRoot[], reason: string): WorkdirError {
  return new WorkdirError(403, 'workdir_forbidden', `Working directory not allowed: ${reason}`, rootPaths(roots));
}

export interface WorkdirCheck {
  /** Canonical path: realpath of the nearest existing ancestor + missing tail. */
  path: string;
  exists: boolean;
  isDir: boolean;
  /** The path as resolved lexically (before realpath). */
  resolved: string;
}

/**
 * Validate a requested directory against the roots. Throws a 403
 * WorkdirError when it is not allowed; never creates anything.
 */
export function checkWorkdir(input: string, roots: WorkdirRoot[]): WorkdirCheck {
  const bad = syntaxError(input);
  if (bad) throw forbidden(roots, bad);
  const resolved = path.resolve(input);
  if (!lexicallyAllowed(resolved, roots)) throw forbidden(roots, 'outside the allowed roots');

  // Nearest existing ancestor (lstat: a dangling link still "exists").
  let ancestor = resolved;
  let found = false;
  const tail: string[] = [];
  for (;;) {
    try {
      fs.lstatSync(ancestor);
      found = true;
      break;
    } catch {
      const parent = path.dirname(ancestor);
      if (parent === ancestor) break; // missing drive — nothing to realpath
      tail.unshift(path.basename(ancestor));
      ancestor = parent;
    }
  }
  let realAncestor = ancestor;
  if (found) {
    try {
      realAncestor = fs.realpathSync.native(ancestor);
    } catch {
      // Exists (lstat) but cannot be resolved: dangling link / no access.
      throw forbidden(roots, 'unresolvable link');
    }
  }
  const real = tail.length ? path.join(realAncestor, ...tail) : realAncestor;
  if (!roots.some((r) => isInside(real, r.real) || isInside(real, r.path))) {
    throw forbidden(roots, 'resolves outside the allowed roots (link/junction)');
  }
  if (real.length > MAX_WORKDIR_LENGTH) throw forbidden(roots, `path longer than ${MAX_WORKDIR_LENGTH} characters`);
  let exists = false;
  let isDir = false;
  if (tail.length === 0) {
    try {
      const st = fs.statSync(real);
      exists = true;
      isDir = st.isDirectory();
    } catch { /* raced away */ }
  }
  return { path: real, exists, isDir, resolved };
}

/**
 * POST /api/sessions `workdir`: validate, then require an existing directory
 * (404 / 400) or `mkdir -p` it when `createDir`. Returns the canonical path.
 */
export function prepareWorkdir(input: string, roots: WorkdirRoot[], createDir: boolean): string {
  const check = checkWorkdir(input, roots);
  if (check.exists) {
    if (!check.isDir) throw new WorkdirError(400, 'workdir_not_dir', `Not a directory: ${check.path}`);
    return check.path;
  }
  if (!createDir) throw new WorkdirError(404, 'workdir_missing', `Directory does not exist: ${check.path}`);
  try {
    fs.mkdirSync(check.path, { recursive: true });
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'EEXIST' || code === 'ENOTDIR') {
      throw new WorkdirError(400, 'workdir_not_dir', `A path component is not a directory: ${check.path}`);
    }
    throw err;
  }
  // Re-check after creation (a link swapped in meanwhile must not escape).
  const after = checkWorkdir(check.path, roots);
  if (!after.isDir) throw new WorkdirError(400, 'workdir_not_dir', `Not a directory: ${after.path}`);
  return after.path;
}

/* ---------------- directory listing (GET /api/dirs?path=) ---------------- */

const SKIP_NAMES = new Set([
  'node_modules', '.git', 'system volume information', 'recovery', 'config.msi',
  'msocache', 'documents and settings',
]);

/** Hidden/system/noise directories never offered by the picker. */
export function skipDirName(name: string): boolean {
  if (name.startsWith('.') || name.startsWith('$') || name.startsWith('~')) return true;
  return SKIP_NAMES.has(name.toLowerCase());
}

export interface DirEntry {
  name: string;
  path: string;
}

export interface DirListing {
  path: string;
  exists: boolean;
  isDir: boolean;
  allowed: boolean;
  parent: string | null;
  entries: DirEntry[];
  truncated: boolean;
}

export function listDir(input: string, roots: WorkdirRoot[], max = MAX_DIR_ENTRIES): DirListing {
  const check = checkWorkdir(input, roots);
  const atRoot = roots.some((r) => key(r.path, IS_WIN) === key(check.path, IS_WIN)
    || key(r.real, IS_WIN) === key(check.path, IS_WIN));
  const parentPath = path.dirname(check.path);
  const parent = atRoot || parentPath === check.path ? null : parentPath;
  const base: DirListing = {
    path: check.path, exists: check.exists, isDir: check.isDir, allowed: true, parent, entries: [], truncated: false,
  };
  if (!check.isDir) return base;
  let dirents: fs.Dirent[];
  try {
    dirents = fs.readdirSync(check.path, { withFileTypes: true });
  } catch {
    return base; // unreadable (EPERM) — empty listing
  }
  const entries: DirEntry[] = [];
  for (const d of dirents) {
    if (skipDirName(d.name)) continue;
    const full = path.join(check.path, d.name);
    if (full.length > MAX_WORKDIR_LENGTH || syntaxError(full)) continue;
    if (d.isDirectory()) {
      entries.push({ name: d.name, path: full });
    } else if (d.isSymbolicLink()) {
      // Junction/symlink: offer it only when its target is a directory inside the roots.
      try {
        const real = fs.realpathSync.native(full);
        if (!roots.some((r) => isInside(real, r.real) || isInside(real, r.path))) continue;
        if (!fs.statSync(real).isDirectory()) continue;
        entries.push({ name: d.name, path: full });
      } catch { /* dangling */ }
    }
  }
  entries.sort((a, b) => {
    const x = a.name.toLowerCase();
    const y = b.name.toLowerCase();
    return x < y ? -1 : x > y ? 1 : a.name < b.name ? -1 : a.name > b.name ? 1 : 0;
  });
  base.truncated = entries.length > max;
  base.entries = entries.slice(0, max);
  return base;
}
