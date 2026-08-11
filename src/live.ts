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
  const script = `
$ErrorActionPreference = 'SilentlyContinue'
$items = Get-CimInstance Win32_Process -Filter "Name='node.exe'" |
  Where-Object { $_.CommandLine -match 'pi-coding-agent' } |
  ForEach-Object {
    [PSCustomObject]@{
      pid = $_.ProcessId
      started = $_.CreationDate
      args = $_.CommandLine
      cwd = (Get-ProcessCwd $_.ProcessId)
    }
  }
$items | ConvertTo-Json -Compress
`;
  // P/Invoke helper for process cwd (NtQueryInformationProcess -> PEB -> RTL_USER_PROCESS_PARAMETERS).
  const helper = `
function Get-ProcessCwd([int]$ProcId) {
  $sig = @'
using System;
using System.Runtime.InteropServices;
public static class ProcCwd {
  [StructLayout(LayoutKind.Sequential)] public struct PROCESS_BASIC_INFORMATION {
    public IntPtr ExitStatus; public IntPtr PebBaseAddress; public IntPtr AffinityMask;
    public IntPtr BasePriority; public IntPtr UniqueProcessId; public IntPtr InheritedFromUniqueProcessId;
  }
  [StructLayout(LayoutKind.Sequential)] public struct UNICODE_STRING {
    public ushort Length; public ushort MaxLength; public IntPtr Buffer;
  }
  [StructLayout(LayoutKind.Sequential)] public struct RTL_USER_PROCESS_PARAMETERS {
    public byte Reserved1[16]; public IntPtr Reserved2[10];
    public UNICODE_STRING ImagePathName; public UNICODE_STRING CommandLine;
  }
  [StructLayout(LayoutKind.Sequential)] public struct PEB {
    public byte Reserved1[2]; public byte BeingDebugged; public byte Reserved2[1];
    public IntPtr Reserved3[2]; public IntPtr Ldr; public IntPtr ProcessParameters;
  }
  [DllImport("kernel32.dll", SetLastError=true)] public static extern IntPtr OpenProcess(uint access, bool inherit, int pid);
  [DllImport("kernel32.dll")] public static extern bool CloseHandle(IntPtr h);
  [DllImport("ntdll.dll")] public static extern int NtQueryInformationProcess(IntPtr h, int cls, ref PROCESS_BASIC_INFORMATION info, int len, out int ret);
  [DllImport("kernel32.dll")] public static extern bool ReadProcessMemory(IntPtr h, IntPtr addr, out IntPtr buf, IntPtr size, out IntPtr read);
  [DllImport("kernel32.dll")] public static extern bool ReadProcessMemory(IntPtr h, IntPtr addr, byte[] buf, IntPtr size, out IntPtr read);
  public static string Cwd(int pid) {
    IntPtr h = OpenProcess(0x0400, false, pid);
    if (h == IntPtr.Zero) return null;
    try {
      PROCESS_BASIC_INFORMATION pbi = new PROCESS_BASIC_INFORMATION();
      int ret;
      if (NtQueryInformationProcess(h, 0, ref pbi, Marshal.SizeOf(pbi), out ret) != 0) return null;
      IntPtr paramsAddr;
      IntPtr read;
      if (!ReadProcessMemory(h, pbi.PebBaseAddress, out paramsAddr, new IntPtr(IntPtr.Size), out read)) return null;
      if (IntPtr.Size == 8) paramsAddr = Marshal.ReadIntPtr(paramsAddr, 0x20);
      else paramsAddr = Marshal.ReadIntPtr(paramsAddr, 0x10);
      byte[] buf = new byte[1024];
      if (!ReadProcessMemory(h, paramsAddr, buf, new IntPtr(buf.Length), out read)) return null;
      return System.Text.Encoding.Unicode.GetString(buf).Split('\\0')[0];
    } finally { CloseHandle(h); }
  }
}
'@
  Add-Type -TypeDefinition $sig
  try { return [ProcCwd]::Cwd($ProcId) } catch { return $null }
}
`;
  const { stdout } = await execFileAsync(
    'powershell',
    ['-NoProfile', '-NonInteractive', '-Command', `${helper}\n${script}`],
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
