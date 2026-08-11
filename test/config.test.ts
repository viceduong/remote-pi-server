import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';

describe('loadConfig', () => {
  it('applies defaults', () => {
    const c = loadConfig({});
    expect(c.REMOTE_PI_PORT).toBe(8787);
    expect(c.REMOTE_PI_HOST).toBe('0.0.0.0');
    expect(c.REMOTE_PI_MAX_AGENTS).toBe(4);
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

  it('fails fast on invalid values', () => {
    expect(() => loadConfig({ REMOTE_PI_PORT: 'not-a-number' })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ REMOTE_PI_MAX_AGENTS: '99' })).toThrow(/Invalid configuration/);
  });
});
