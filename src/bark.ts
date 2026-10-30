/**
 * CONTRACT-V2 §14 — Bark push client + Bark's encrypted push format.
 *
 * Sources (Finb/Bark + Finb/bark-server, verified 2026-10-30):
 *  - docs/en-us/tutorial.md: POST `/push` with every parameter in the body
 *    (JSON `device_key`), or `/:key` with form/JSON body.
 *  - docs/en-us/params.md: same parameter names for GET / form / JSON;
 *    `level` ∈ active|timeSensitive|passive|critical; encryption params
 *    `ciphertext` + `iv` ("the random IV used while encrypting needs to be
 *    sent to the server together with the ciphertext").
 *  - docs/en-us/encryption.md: the request parameters are serialized as a
 *    JSON string, encrypted, base64'd (standard alphabet) and sent as
 *    `ciphertext`. Known-answer vector (AES-128-CBC, PKCS7):
 *    key "1234567890123456", iv "1111111111111111",
 *    plaintext `{"body": "test", "sound": "birdsong"}` →
 *    "d3QhjQjP5majvNt5CjsvFWwqqj2gKl96RFj5OO+u6ynTt7lkyigDYNA3abnnCLpr".
 *  - Model/Algorithm.swift (the app's decryptor, CryptoSwift): algorithms
 *    AES128/AES192/AES256 (key = 16/24/32 characters used as raw bytes),
 *    modes CBC (iv 16 chars) / ECB (no iv) / GCM (iv 12 chars, `.combined`
 *    = tag appended to the ciphertext); padding pkcs7 (or noPadding).
 *  - NotificationServiceExtension/Processor/CiphertextProcessor.swift: an
 *    `iv` sent with the push overrides the IV configured in the app; the
 *    decrypted JSON's keys are applied as title/subtitle/body/group/sound/
 *    badge and the rest (url, …) land in userInfo.
 *
 * We always send a FRESH random IV per push (printable ASCII, the app uses the
 * IV string's bytes) — reusing a static IV leaks equal prefixes in CBC and is
 * catastrophic in GCM. The IV configured in settings is accepted for parity
 * with the Bark app UI but never needed by the server.
 */
import crypto from 'node:crypto';

export const BARK_ALGORITHMS = ['AES128', 'AES192', 'AES256'] as const;
export const BARK_MODES = ['CBC', 'ECB', 'GCM'] as const;
export const BARK_LEVELS = ['active', 'timeSensitive', 'passive', 'critical'] as const;
export type BarkAlgorithm = (typeof BARK_ALGORITHMS)[number];
export type BarkMode = (typeof BARK_MODES)[number];
export type BarkLevel = (typeof BARK_LEVELS)[number];

export const KEY_LENGTH: Record<BarkAlgorithm, number> = { AES128: 16, AES192: 24, AES256: 32 };
export const IV_LENGTH: Record<BarkMode, number> = { CBC: 16, ECB: 0, GCM: 12 };
const GCM_TAG_BYTES = 16;

/** Plaintext notification content (what goes INSIDE the ciphertext). */
export interface BarkMessage {
  title: string;
  body: string;
  group: string;
  url: string;
  level: BarkLevel;
}

export interface BarkCrypto {
  algorithm: BarkAlgorithm;
  mode: BarkMode;
  key: string;
}

function cipherName(algorithm: BarkAlgorithm, mode: BarkMode): crypto.CipherGCMTypes | string {
  const bits = KEY_LENGTH[algorithm] * 8;
  return `aes-${bits}-${mode.toLowerCase()}`;
}

/** Random IV string of `len` alphanumeric chars (the app uses the string's bytes). */
export function randomIv(len: number): string {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = crypto.randomBytes(len);
  let out = '';
  for (let i = 0; i < len; i++) out += alphabet[bytes[i]! % alphabet.length];
  return out;
}

/** Validation message for a key/iv pair, or null when usable. */
export function cryptoProblem(algorithm: BarkAlgorithm, mode: BarkMode, key: string | null, iv?: string | null): string | null {
  if (!key) return 'encryption key not set';
  if (Buffer.byteLength(key, 'utf8') !== KEY_LENGTH[algorithm]) {
    return `encryption key must be ${KEY_LENGTH[algorithm]} characters for ${algorithm}`;
  }
  if (iv && IV_LENGTH[mode] && Buffer.byteLength(iv, 'utf8') !== IV_LENGTH[mode]) {
    return `iv must be ${IV_LENGTH[mode]} characters for ${mode}`;
  }
  return null;
}

/**
 * Encrypt a UTF-8 plaintext exactly like Bark's documented openssl recipe:
 * key/iv bytes = the strings' UTF-8 bytes, PKCS7 for CBC/ECB, GCM tag (16 B)
 * appended, standard base64.
 */
export function barkEncrypt(plaintext: string, c: BarkCrypto, iv: string): string {
  const key = Buffer.from(c.key, 'utf8');
  const ivBytes = c.mode === 'ECB' ? null : Buffer.from(iv, 'utf8');
  const name = cipherName(c.algorithm, c.mode);
  if (c.mode === 'GCM') {
    const cipher = crypto.createCipheriv(name as crypto.CipherGCMTypes, key, ivBytes!, { authTagLength: GCM_TAG_BYTES });
    const body = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
    return Buffer.concat([body, cipher.getAuthTag()]).toString('base64');
  }
  const cipher = crypto.createCipheriv(name, key, ivBytes);
  return Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]).toString('base64');
}

/** Inverse of {@link barkEncrypt} (tests / dry-run self-check). */
export function barkDecrypt(ciphertext: string, c: BarkCrypto, iv: string): string {
  const key = Buffer.from(c.key, 'utf8');
  const ivBytes = c.mode === 'ECB' ? null : Buffer.from(iv, 'utf8');
  const data = Buffer.from(ciphertext, 'base64');
  const name = cipherName(c.algorithm, c.mode);
  if (c.mode === 'GCM') {
    const decipher = crypto.createDecipheriv(name as crypto.CipherGCMTypes, key, ivBytes!, { authTagLength: GCM_TAG_BYTES });
    decipher.setAuthTag(data.subarray(data.length - GCM_TAG_BYTES));
    return Buffer.concat([decipher.update(data.subarray(0, data.length - GCM_TAG_BYTES)), decipher.final()]).toString('utf8');
  }
  const decipher = crypto.createDecipheriv(name, key, ivBytes);
  return Buffer.concat([decipher.update(data), decipher.final()]).toString('utf8');
}

/** JSON body POSTed to `<server>/push`. */
export type BarkRequestBody = Record<string, string>;

/**
 * Build the `/push` body. Encrypted: only `device_key`, `ciphertext`, `iv`
 * (non-ECB) and `level` (not secret; lets bark-server set the APNs
 * interruption level) are in clear — title/body/group/url are inside the
 * ciphertext. Plain: the Bark parameters as-is.
 */
export function buildPushBody(deviceKey: string, msg: BarkMessage, enc: BarkCrypto | null): BarkRequestBody {
  if (!enc) {
    return { device_key: deviceKey, title: msg.title, body: msg.body, group: msg.group, url: msg.url, level: msg.level };
  }
  const plaintext = JSON.stringify({ title: msg.title, body: msg.body, group: msg.group, url: msg.url, level: msg.level });
  const iv = IV_LENGTH[enc.mode] ? randomIv(IV_LENGTH[enc.mode]) : '';
  const out: BarkRequestBody = { device_key: deviceKey, ciphertext: barkEncrypt(plaintext, enc, iv) };
  if (iv) out.iv = iv;
  out.level = msg.level;
  return out;
}

export interface BarkResult {
  ok: boolean;
  /** HTTP status of the last attempt (0 = network error / timeout). */
  status: number;
  error?: string;
}

export interface BarkClientOptions {
  timeoutMs?: number;
  retryDelayMs?: number;
  fetchImpl?: typeof fetch;
}

/** Strip trailing slashes; `/push` is appended by the client. */
export function normalizeServer(url: string): string {
  return url.trim().replace(/\/+$/, '');
}

/**
 * POST one push. One retry on a network error / timeout / 5xx (after a short
 * delay); never more. Error strings never contain the request body or the
 * device key (only status + a short reason).
 */
export async function sendBark(server: string, body: BarkRequestBody, opts: BarkClientOptions = {}): Promise<BarkResult> {
  const doFetch = opts.fetchImpl ?? fetch;
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const url = `${normalizeServer(server)}/push`;
  let last: BarkResult = { ok: false, status: 0, error: 'not sent' };
  for (let attempt = 0; attempt < 2; attempt++) {
    if (attempt > 0) await new Promise((r) => setTimeout(r, opts.retryDelayMs ?? 1_000));
    let retryable = false;
    try {
      const res = await doFetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json; charset=utf-8' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const text = await res.text().catch(() => '');
      let code: unknown;
      let message: unknown;
      try {
        const j = JSON.parse(text) as { code?: unknown; message?: unknown };
        code = j.code;
        message = j.message;
      } catch { /* non-JSON */ }
      if (res.ok && (code === undefined || code === 200)) return { ok: true, status: res.status };
      const reason = typeof message === 'string' && message ? message.slice(0, 200) : `HTTP ${res.status}`;
      last = { ok: false, status: res.status, error: reason };
      retryable = res.status >= 500;
    } catch (err) {
      const e = err as Error;
      const reason = e.name === 'TimeoutError' || e.name === 'AbortError' ? 'timeout' : (e.message || 'network error');
      last = { ok: false, status: 0, error: scrub(reason, body.device_key) };
      retryable = true;
    }
    if (!retryable) break;
  }
  return last;
}

function scrub(s: string, secret: string | undefined): string {
  return secret ? s.split(secret).join('***') : s;
}
