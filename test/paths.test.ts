import { describe, expect, it } from 'vitest';
import { demangleDir, manglePath } from '../src/paths.js';

describe('path mangling (pi session dir convention)', () => {
  it('matches pi 0.79 encoding — verified against real dirs', () => {
    // Real dirs on disk:
    expect(manglePath('C:\\Users\\Admin')).toBe('--C--Users-Admin--');
    expect(manglePath('D:\\')).toBe('--D----');
    expect(manglePath('D:\\ai-ching')).toBe('--D--ai-ching--');
    // POSIX:
    expect(manglePath('/home/user/projects')).toBe('--home-user-projects--');
    expect(manglePath('/work/proj')).toBe('--work-proj--');
  });

  it('keeps trailing separators as dashes (pi does not strip them)', () => {
    expect(manglePath('/work/proj/')).toBe('--work-proj---');
  });

  it('demangles to a best-effort fallback path', () => {
    expect(demangleDir('--home-user-projects--')).toBe('home/user/projects');
    expect(demangleDir('--C-Users-Admin--')).toBe('C/Users/Admin');
    expect(demangleDir('--C--Users-Admin--')).toBe('C//Users/Admin');
    expect(demangleDir('--D----')).toBe('D//');
  });

  it('returns null for degenerate dir names', () => {
    expect(demangleDir('--')).toBeNull();
    expect(demangleDir('')).toBeNull();
  });
});
