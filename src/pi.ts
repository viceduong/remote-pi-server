import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';

const IS_WIN = process.platform === 'win32';

/** Detect the pi version by capturing stdout+stderr (platform dependent). */
export async function detectPiVersion(bin: string): Promise<string | null> {
  try {
    const child = IS_WIN
      ? spawn('cmd.exe', ['/c', bin, '--version'], { windowsHide: true })
      : spawn(bin, ['--version'], { windowsHide: true });
    let out = '';
    child.stdout.on('data', (d: Buffer) => (out += d.toString()));
    child.stderr.on('data', (d: Buffer) => (out += d.toString()));
    child.on('error', () => undefined);
    const [code] = await once(child, 'close') as [number | null];
    if (code !== 0 && out.trim() === '') return null;
    return out.trim() || null;
  } catch {
    return null;
  }
}

/**
 * Spawn a `pi --mode rpc` process. On Windows, pi is a .cmd shim and must be
 * invoked through cmd.exe. JSONL flows on stdout/stdin; stderr is for logs.
 */
export function spawnPiProcess(
  bin: string,
  args: string[],
  workdir: string,
): ChildProcess {
  if (IS_WIN) {
    return spawn('cmd.exe', ['/c', bin, ...args], { cwd: workdir, windowsHide: true });
  }
  return spawn(bin, args, { cwd: workdir });
}
