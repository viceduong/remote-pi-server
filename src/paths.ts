/**
 * pi session-directory path mangling.
 *
 * pi stores sessions under `~/.pi/agent/sessions/<encoded-cwd>/<id>.jsonl`.
 * Encoding (pi 0.79, session-manager.js `getDefaultSessionDirPath`):
 * strip a leading separator, then every `[/\\:]` -> `-`, wrapped in `--...--`.
 * Verified against real on-disk dirs: `C:\Users\Admin` -> `--C--Users-Admin--`,
 * `D:\` -> `--D----`.
 *
 * The dir name is lossy for odd paths — ground truth for the working directory
 * is the session file's first line (`"cwd": "C:\\..."`); demangleDir is only a
 * best-effort fallback.
 */
export function manglePath(p: string): string {
  const normalized = p.replace(/^[/\\]/, '');
  return `--${normalized.replace(/[/\\:]/g, '-')}--`;
}

/** Lossy fallback decode of a mangled dir name (separators become `/`). */
export function demangleDir(dirName: string): string | null {
  const inner = dirName.replace(/^--/, '').replace(/--$/, '');
  if (!inner) return null;
  return inner.replace(/-/g, '/');
}
