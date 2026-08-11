import { describe, expect, it } from 'vitest';

// Pure ring/SSE replay semantics extracted for testing without a pi binary:
// replayAfter must return only records with seq > lastSeq, in order.
function makeRing(capacity: number) {
  const ring: { seq: number; type: string }[] = [];
  return {
    push(r: { seq: number; type: string }) {
      ring.push(r);
      if (ring.length > capacity) ring.splice(0, ring.length - capacity);
    },
    replayAfter(lastSeq: number) {
      return ring.filter((r) => r.seq > lastSeq);
    },
    size: () => ring.length,
  };
}

describe('event ring replay', () => {
  it('replays only events after lastSeq', () => {
    const ring = makeRing(10);
    for (let i = 1; i <= 5; i++) ring.push({ seq: i, type: 'x' });
    const replayed = ring.replayAfter(2);
    expect(replayed.map((r) => r.seq)).toEqual([3, 4, 5]);
  });

  it('replays everything when lastSeq is 0', () => {
    const ring = makeRing(10);
    for (let i = 1; i <= 3; i++) ring.push({ seq: i, type: 'x' });
    expect(ring.replayAfter(0)).toHaveLength(3);
  });

  it('drops nothing when lastSeq is current', () => {
    const ring = makeRing(10);
    ring.push({ seq: 1, type: 'x' });
    expect(ring.replayAfter(1)).toHaveLength(0);
  });

  it('caps capacity (oldest evicted)', () => {
    const ring = makeRing(3);
    for (let i = 1; i <= 6; i++) ring.push({ seq: i, type: 'x' });
    expect(ring.size()).toBe(3);
    expect(ring.replayAfter(0).map((r) => r.seq)).toEqual([4, 5, 6]);
  });
});
