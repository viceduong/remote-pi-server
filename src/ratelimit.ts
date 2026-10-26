import crypto from 'node:crypto';
import type { FastifyRequest } from 'fastify';

/**
 * Rate-limit key: token fingerprint + real client IP. Behind cloudflared every
 * request arrives from 127.0.0.1, so keying by the socket address made one
 * global bucket. CF-Connecting-IP (set by Cloudflare's edge) or the trusted
 * X-Forwarded-For hop (Fastify `trustProxy`, loopback only) identifies the
 * client; the token fingerprint keeps distinct users apart behind one NAT.
 */
export function rateLimitKey(req: Pick<FastifyRequest, 'headers' | 'ip'>): string {
  const cf = req.headers['cf-connecting-ip'];
  const ip = (Array.isArray(cf) ? cf[0] : cf) || req.ip;
  const auth = req.headers.authorization ?? '';
  const tokenTag = auth ? crypto.createHash('sha256').update(auth).digest('hex').slice(0, 12) : 'anon';
  return `${tokenTag}:${ip}`;
}
