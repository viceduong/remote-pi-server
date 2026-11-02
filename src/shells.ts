import { execFile } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/**
 * CONTRACT-V2 §16: background shells owned by the pi-bash extension.
 *
 * pi-bash persists each shell as `<OUTPUT_DIR>/<id>.json` next to `<id>.log`,
 * where OUTPUT_DIR = `join(os.tmpdir(), "pi-bg-bash")` **of the pi process**.
 * The bridge's RPC children inherit the server's environment, but host TUIs run
 * as the interactive user — when the server runs as a service (LocalSystem:
 * TEMP = C:\Windows\TEMP) those differ, so several dirs are scanned.
 *
 * Paths never come from the client: ids are validated and the log path is
 * derived from the dir the json was found in.
 */

export const SHELL_ID_RE = /^bg-[0-9a-f]{8}$/;
export const OUTPUT_TAIL_DEFAULT = 16_384;
export const OUTPUT_TAIL_MAX = 262_144;

export type ShellStatus = 'running' | 'completed' | 'failed' | 'killed';

/** The persisted record (pi-bash `<id>.json`). */
export interface ShellRecord {
  id: string;
  command: string;
  status: ShellStatus;
  pid: number | null;
  startTime: number;
  endTime?: number;
  exitCode?: number;
  outputFile?: string;
  sessionFile: string | null;
  cwd?: string;
}

/** Wire shape of one `GET /shells` entry. */
export interface ShellView {
  id: string;
  command: string;
  status: ShellStatus;
  pid: number | null;
  startTime: number;
  endTime?: number;
  exitCode?: number;
  durationMs?: number;
  outputBytes: number;
}

export interface ShellOutput {
  text: string;
  truncated: boolean;
  size: number;
}

export type KillResult =
  | { result: 'killed' }
  | { result: 'not_found' }
  | { result: 'not_running'; status: ShellStatus }
  | { result: 'failed'; error: string };

/** pi-bash's own OUTPUT_DIR resolution for a given environment. */
export function piBashOutputDir(tmp: string = os.tmpdir()): string {
  return path.join(tmp, 'pi-bg-bash');
}

/**
 * Default dirs: the server process's own (= bridge children's) pi-bash dir,
 * plus — on Windows — the interactive user's `%USERPROFILE%\AppData\Local\Temp`
 * one (host TUIs), when different.
 */
export function defaultShellDirs(env: NodeJS.ProcessEnv = process.env, platform: NodeJS.Platform = process.platform): string[] {
  const dirs = [piBashOutputDir()];
  const home = env.USERPROFILE ?? env.HOME;
  if (platform === 'win32' && home) dirs.push(piBashOutputDir(path.join(home, 'AppData', 'Local', 'Temp')));
  return uniqueDirs(dirs, platform);
}

/** Config value (`;`-separated) → dirs; empty → defaults. */
export function resolveShellDirs(value: string | undefined, env: NodeJS.ProcessEnv = process.env): string[] {
  const list = (value ?? '').split(';').map((s) => s.trim()).filter(Boolean);
  return list.length ? uniqueDirs(list.map((d) => path.resolve(d))) : defaultShellDirs(env);
}

function uniqueDirs(dirs: string[], platform: NodeJS.Platform = process.platform): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const d of dirs) {
    const k = normalizeFile(d, platform);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(d);
  }
  return out;
}

/** Path identity for comparisons (resolved; case-insensitive on Windows). */
export function normalizeFile(file: string, platform: NodeJS.Platform = process.platform): string {
  const r = path.resolve(file);
  return platform === 'win32' ? r.toLowerCase() : r;
}

export function isValidShellId(id: unknown): id is string {
  return typeof id === 'string' && SHELL_ID_RE.test(id);
}

/** pid liveness: signal 0 (EPERM = exists but not ours → alive). */
export function pidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/** Kill a process tree: Windows `taskkill /PID <pid> /T /F` (no shell); POSIX group SIGTERM. */
export function killTree(pid: number): Promise<void> {
  if (process.platform !== 'win32') {
    try {
      process.kill(-pid, 'SIGTERM');
    } catch {
      process.kill(pid, 'SIGTERM');
    }
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    execFile('taskkill', ['/PID', String(pid), '/T', '/F'], { windowsHide: true, timeout: 15_000 }, (err, _stdout, stderr) => {
      if (err) reject(new Error(String(stderr || err.message).trim()));
      else resolve();
    });
  });
}

/**
 * Trim a tail buffer to UTF-8 character boundaries: a leading partial sequence
 * is skipped when the read cut into the file (`cutAtStart`); a trailing
 * incomplete one (writer mid-character) is always held back.
 */
export function utf8SafeSlice(buf: Buffer, cutAtStart: boolean): Buffer {
  let start = 0;
  if (cutAtStart) {
    while (start < buf.length && start < 4 && (buf[start]! & 0xc0) === 0x80) start++;
  }
  let end = buf.length;
  // Find the lead byte of the last character (look back at most 3 continuation bytes).
  let i = end - 1;
  let cont = 0;
  while (i >= start && cont < 3 && (buf[i]! & 0xc0) === 0x80) {
    i--;
    cont++;
  }
  if (i >= start) {
    const b = buf[i]!;
    const need = b >= 0xf0 ? 4 : b >= 0xe0 ? 3 : b >= 0xc0 ? 2 : 1;
    if (need > 1 && end - i < need) end = i; // incomplete trailing sequence
  }
  return buf.subarray(start, end);
}

function readRecord(file: string): ShellRecord | null {
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch {
    return null;
  }
  let r: unknown;
  try {
    r = JSON.parse(raw);
  } catch {
    return null;
  }
  const o = r as Partial<ShellRecord>;
  if (!o || typeof o !== 'object' || !isValidShellId(o.id) || typeof o.command !== 'string') return null;
  if (!['running', 'completed', 'failed', 'killed'].includes(String(o.status))) return null;
  if (typeof o.startTime !== 'number') return null;
  return {
    id: o.id,
    command: o.command,
    status: o.status as ShellStatus,
    pid: typeof o.pid === 'number' && Number.isInteger(o.pid) && o.pid > 0 ? o.pid : null,
    startTime: o.startTime,
    ...(typeof o.endTime === 'number' ? { endTime: o.endTime } : {}),
    ...(typeof o.exitCode === 'number' ? { exitCode: o.exitCode } : {}),
    ...(typeof o.outputFile === 'string' ? { outputFile: o.outputFile } : {}),
    sessionFile: typeof o.sessionFile === 'string' && o.sessionFile ? o.sessionFile : null,
    ...(typeof o.cwd === 'string' ? { cwd: o.cwd } : {}),
  };
}

/** tmp + rename, retried (Windows: a reader may briefly hold the target). */
function writeAtomic(file: string, data: string): void {
  const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`;
  fs.writeFileSync(tmp, data);
  for (let i = 0; ; i++) {
    try {
      fs.renameSync(tmp, file);
      return;
    } catch (err) {
      if (i >= 5) {
        try { fs.unlinkSync(tmp); } catch { /* ignore */ }
        throw err;
      }
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
    }
  }
}

interface Located {
  dir: string;
  record: ShellRecord;
}

export interface ShellStoreOptions {
  isAlive?: (pid: number) => boolean;
  kill?: (pid: number) => Promise<void>;
  now?: () => number;
}

export class ShellStore {
  private readonly isAlive: (pid: number) => boolean;
  private readonly killFn: (pid: number) => Promise<void>;
  private readonly now: () => number;

  constructor(readonly dirs: string[], opts: ShellStoreOptions = {}) {
    this.isAlive = opts.isAlive ?? pidAlive;
    this.killFn = opts.kill ?? killTree;
    this.now = opts.now ?? Date.now;
  }

  private belongs(rec: ShellRecord, sessionFile: string): boolean {
    return rec.sessionFile !== null && normalizeFile(rec.sessionFile) === normalizeFile(sessionFile);
  }

  /** Effective status: a "running" record whose pid is gone reports "failed". */
  private effectiveStatus(rec: ShellRecord): ShellStatus {
    if (rec.status !== 'running') return rec.status;
    return rec.pid !== null && this.isAlive(rec.pid) ? 'running' : 'failed';
  }

  /** The shell `id` of this session (json found in one of the dirs), else null. */
  private locate(sessionFile: string, id: string): Located | null {
    if (!isValidShellId(id)) return null;
    for (const dir of this.dirs) {
      const record = readRecord(path.join(dir, `${id}.json`));
      if (record && record.id === id && this.belongs(record, sessionFile)) return { dir, record };
    }
    return null;
  }

  private outputBytes(dir: string, id: string): number {
    try {
      return fs.statSync(path.join(dir, `${id}.log`)).size;
    } catch {
      return 0;
    }
  }

  /** This session's shells, newest first. */
  list(sessionFile: string): ShellView[] {
    const out: ShellView[] = [];
    const seen = new Set<string>();
    for (const dir of this.dirs) {
      let names: string[];
      try {
        names = fs.readdirSync(dir);
      } catch {
        continue;
      }
      for (const name of names) {
        if (!name.endsWith('.json')) continue;
        const id = name.slice(0, -5);
        if (!isValidShellId(id) || seen.has(id)) continue;
        const rec = readRecord(path.join(dir, name));
        if (!rec || rec.id !== id || !this.belongs(rec, sessionFile)) continue;
        seen.add(id);
        const status = this.effectiveStatus(rec);
        const v: ShellView = {
          id: rec.id,
          command: rec.command,
          status,
          pid: rec.pid,
          startTime: rec.startTime,
          outputBytes: this.outputBytes(dir, id),
        };
        if (rec.endTime !== undefined) v.endTime = rec.endTime;
        if (rec.exitCode !== undefined) v.exitCode = rec.exitCode;
        if (rec.endTime !== undefined) v.durationMs = Math.max(0, rec.endTime - rec.startTime);
        else if (status === 'running') v.durationMs = Math.max(0, this.now() - rec.startTime);
        out.push(v);
      }
    }
    out.sort((a, b) => b.startTime - a.startTime || (a.id < b.id ? 1 : -1));
    return out;
  }

  /** UTF-8-safe tail of the shell's log (ANSI kept); null = not this session's shell. */
  output(sessionFile: string, id: string, tail: number = OUTPUT_TAIL_DEFAULT): ShellOutput | null {
    const loc = this.locate(sessionFile, id);
    if (!loc) return null;
    const max = Math.min(Math.max(1, Math.floor(tail)), OUTPUT_TAIL_MAX);
    const file = path.join(loc.dir, `${id}.log`);
    let fd: number;
    try {
      fd = fs.openSync(file, 'r');
    } catch {
      return { text: '', truncated: false, size: 0 };
    }
    try {
      const size = fs.fstatSync(fd).size;
      const start = Math.max(0, size - max);
      const buf = Buffer.alloc(size - start);
      let got = 0;
      while (got < buf.length) {
        const n = fs.readSync(fd, buf, got, buf.length - got, start + got);
        if (n <= 0) break;
        got += n;
      }
      const slice = utf8SafeSlice(buf.subarray(0, got), start > 0);
      return { text: slice.toString('utf8'), truncated: start > 0, size };
    } finally {
      fs.closeSync(fd);
    }
  }

  /**
   * Kill a running shell of this session. The json is marked "killed" BEFORE
   * the tree is killed so pi-bash's close handler keeps "killed" (it adopts an
   * on-disk "killed" instead of reporting "failed" and waking the agent).
   */
  async kill(sessionFile: string, id: string): Promise<KillResult> {
    const loc = this.locate(sessionFile, id);
    if (!loc) return { result: 'not_found' };
    const rec = loc.record;
    const status = this.effectiveStatus(rec);
    if (status !== 'running' || rec.pid === null) return { result: 'not_running', status };
    const file = path.join(loc.dir, `${id}.json`);
    const prevRaw = fs.readFileSync(file, 'utf8');
    let prev: Record<string, unknown>;
    try {
      prev = JSON.parse(prevRaw) as Record<string, unknown>;
    } catch {
      return { result: 'not_found' };
    }
    try {
      writeAtomic(file, JSON.stringify({ ...prev, status: 'killed', endTime: this.now(), exitCode: -1 }));
    } catch (err) {
      return { result: 'failed', error: `Could not update the shell state: ${(err as Error).message}` };
    }
    try {
      await this.killFn(rec.pid);
    } catch (err) {
      if (this.isAlive(rec.pid)) {
        // Still running: put the record back (unless pi-bash rewrote it meanwhile).
        try {
          const cur = readRecord(file);
          if (cur?.status === 'killed') writeAtomic(file, prevRaw);
        } catch { /* ignore */ }
        return { result: 'failed', error: `Could not kill the process: ${(err as Error).message}` };
      }
    }
    return { result: 'killed' };
  }
}
