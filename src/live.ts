import { execFile } from 'node:child_process';
import fs from 'node:fs';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const IS_WIN = process.platform === 'win32';

export interface LivePiInstance {
  pid: number;
  cwd: string | null;
  startedAt: number | null;
  args: string;
}

/**
 * Enumerate pi agent processes running on the host (externally started).
 * Never includes bridge children — callers subtract their own pids.
 *
 * POSIX: `ps` + /proc/<pid>/cwd (exact working directory).
 * Windows: Get-CimInstance command line filter + P/Invoke cwd probe with a
 * null fallback (callers then use mtime correlation).
 */
export async function probeLivePiInstances(): Promise<LivePiInstance[]> {
  try {
    return IS_WIN ? await probeWindows() : await probePosix();
  } catch {
    return [];
  }
}

/* ---------------- POSIX ---------------- */

async function probePosix(): Promise<LivePiInstance[]> {
  const { stdout } = await execFileAsync('ps', ['-eo', 'pid=,lstart=,args='], {
    maxBuffer: 4 * 1024 * 1024,
  });
  const out: LivePiInstance[] = [];
  for (const line of stdout.split('\n')) {
    const m = /^\s*(\d+)\s+(.+?)\s+(\S.*)$/.exec(line);
    if (!m) continue;
    const pid = Number(m[1]);
    const started = Date.parse(m[2] ?? '');
    const args = m[3] ?? '';
    if (!isPiAgent(args)) continue;
    out.push({ pid, cwd: readProcCwd(pid), startedAt: Number.isNaN(started) ? null : started, args });
  }
  return out;
}

function readProcCwd(pid: number): string | null {
  try {
    return fs.realpathSync(`/proc/${pid}/cwd`);
  } catch {
    return null;
  }
}

/* ---------------- Windows ---------------- */

async function probeWindows(): Promise<LivePiInstance[]> {
  // Keep this probe dependency-free. The old embedded C# PEB reader used
  // invalid array declarations, so Add-Type failed and live protection became
  // silently disabled. Recency mapping remains safe as a fallback when cwd is
  // unavailable; the command line is still retained for diagnostics.
  const script = `
$ErrorActionPreference = 'SilentlyContinue'
$items = Get-CimInstance Win32_Process |
  Where-Object { $_.Name -in @('node.exe','cmd.exe') -and $_.CommandLine -match 'pi-coding-agent[\\\\/]dist[\\\\/]cli\\.js' } |
  ForEach-Object {
    [PSCustomObject]@{
      pid = [int]$_.ProcessId
      started = $_.CreationDate
      args = [string]$_.CommandLine
      cwd = $null
    }
  }
$items | ConvertTo-Json -Compress
`;
  const { stdout } = await execFileAsync(
    'powershell',
    ['-NoProfile', '-NonInteractive', '-Command', script],
    { maxBuffer: 4 * 1024 * 1024, windowsHide: true },
  );
  try {
    const parsed = JSON.parse(stdout.trim());
    const items = Array.isArray(parsed) ? parsed : [parsed];
    return items
      .filter((i: { pid?: unknown }) => typeof i?.pid === 'number')
      .map((i: { pid: number; started?: string; args?: string; cwd?: string | null }) => ({
        pid: i.pid,
        cwd: i.cwd || null,
        startedAt: i.started ? new Date(i.started).getTime() : null,
        args: i.args ?? '',
      }));
  } catch {
    return [];
  }
}

/** A pi agent process (either the pi CLI itself or the agent harness). */
function isPiAgent(args: string): boolean {
  return /pi-coding-agent[\\/]dist[\\/]cli\.js/.test(args);
}
