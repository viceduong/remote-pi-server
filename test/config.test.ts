import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';

describe('loadConfig', () => {
  it('applies defaults', () => {
    const c = loadConfig({});
    expect(c.REMOTE_PI_PORT).toBe(8787);
    expect(c.REMOTE_PI_HOST).toBe('127.0.0.1');
    expect(c.REMOTE_PI_MAX_AGENTS).toBe(6);
    expect(c.PI_BIN).toBe('pi');
  });

  it('parses env values', () => {
    const c = loadConfig({
      REMOTE_PI_PORT: '9000',
      REMOTE_PI_TOKEN: 'secret',
      REMOTE_PI_MAX_AGENTS: '2',
      REMOTE_PI_EXTRA_ARGS: '--no-tools --plan',
    });
    expect(c.REMOTE_PI_PORT).toBe(9000);
    expect(c.REMOTE_PI_TOKEN).toBe('secret');
    expect(c.REMOTE_PI_MAX_AGENTS).toBe(2);
    expect(c.REMOTE_PI_EXTRA_ARGS).toBe('--no-tools --plan');
  });

  it('requires auth for non-loopback binding', () => {
    expect(() => loadConfig({ REMOTE_PI_HOST: '0.0.0.0' })).toThrow(/REMOTE_PI_TOKEN is required/);
    expect(() => loadConfig({ REMOTE_PI_HOST: '0.0.0.0', REMOTE_PI_ALLOW_ANONYMOUS: '1' })).not.toThrow();
  });

  it('REMOTE_PI_WORKDIR_ROOTS (§12): optional, entries must be absolute', () => {
    expect(loadConfig({}).REMOTE_PI_WORKDIR_ROOTS).toBeUndefined();
    const abs = process.platform === 'win32' ? 'D:\\;E:\\work' : '/srv;/home/u';
    expect(loadConfig({ REMOTE_PI_WORKDIR_ROOTS: abs }).REMOTE_PI_WORKDIR_ROOTS).toBe(abs);
    expect(() => loadConfig({ REMOTE_PI_WORKDIR_ROOTS: 'relative/dir' })).toThrow(/must be absolute/);
  });

  it('fails fast on invalid values', () => {
    expect(() => loadConfig({ REMOTE_PI_PORT: 'not-a-number' })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ REMOTE_PI_MAX_AGENTS: '99' })).toThrow(/Invalid configuration/);
  });
});
