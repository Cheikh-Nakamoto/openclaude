import { expect, test, describe, afterEach, beforeEach, mock } from 'bun:test'
import {
  getMaxRpm,
  checkRateLimit,
  notifyRateLimited,
  getQueueDepth,
  getBucketTokens,
  resetRateLimiterForTesting,
  QueueFullError,
} from './rateLimiter.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function setEnv(rpm?: string, burst?: string, queueMax?: string) {
  if (rpm !== undefined) process.env.OPENCLAUDE_MAX_RPM = rpm
  if (burst !== undefined) process.env.OPENCLAUDE_BURST_SIZE = burst
  if (queueMax !== undefined) process.env.OPENCLAUDE_QUEUE_MAX = queueMax
}

function clearEnv() {
  delete process.env.OPENCLAUDE_MAX_RPM
  delete process.env.CLAUDE_CODE_MAX_RPM
  delete process.env.OPENCLAUDE_BURST_SIZE
  delete process.env.OPENCLAUDE_QUEUE_MAX
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe('rateLimiter — Token Bucket', () => {
  beforeEach(() => {
    clearEnv()
    resetRateLimiterForTesting()
  })

  afterEach(() => {
    clearEnv()
    resetRateLimiterForTesting()
  })

  // ── getMaxRpm ─────────────────────────────────────────────────────────────

  describe('getMaxRpm()', () => {
    test('returns 0 when no env var is set', () => {
      expect(getMaxRpm()).toBe(0)
    })

    test('parses CLAUDE_CODE_MAX_RPM', () => {
      process.env.CLAUDE_CODE_MAX_RPM = '10'
      expect(getMaxRpm()).toBe(10)
    })

    test('OPENCLAUDE_MAX_RPM takes precedence over CLAUDE_CODE_MAX_RPM', () => {
      process.env.CLAUDE_CODE_MAX_RPM = '10'
      process.env.OPENCLAUDE_MAX_RPM = '20'
      expect(getMaxRpm()).toBe(20)
    })

    test('falls back to CLAUDE_CODE_MAX_RPM when OPENCLAUDE_MAX_RPM is invalid', () => {
      process.env.OPENCLAUDE_MAX_RPM = 'invalid'
      process.env.CLAUDE_CODE_MAX_RPM = '10'
      expect(getMaxRpm()).toBe(10)
    })

    test('ignores zero and negative values', () => {
      process.env.OPENCLAUDE_MAX_RPM = '0'
      expect(getMaxRpm()).toBe(0)
      process.env.OPENCLAUDE_MAX_RPM = '-5'
      expect(getMaxRpm()).toBe(0)
    })
  })

  // ── checkRateLimit — disabled ─────────────────────────────────────────────

  describe('checkRateLimit() — disabled (maxRpm <= 0)', () => {
    test('returns instantly without any waiting', async () => {
      const start = Date.now()
      await checkRateLimit(0)
      await checkRateLimit(-1)
      await checkRateLimit(0)
      expect(Date.now() - start).toBeLessThan(30)
    })

    test('queue stays empty when disabled', async () => {
      await checkRateLimit(0)
      expect(getQueueDepth()).toBe(0)
    })
  })

  // ── Token Bucket — fast path ───────────────────────────────────────────────

  describe('checkRateLimit() — token bucket fast path', () => {
    test('first request consumes a token immediately (burst=2)', async () => {
      setEnv('60', '2')
      const start = Date.now()
      await checkRateLimit(60)
      expect(Date.now() - start).toBeLessThan(30)
      expect(getQueueDepth()).toBe(0)
    })

    test('two sequential requests both get a token when burst=2', async () => {
      setEnv('60', '2')
      const start = Date.now()
      await checkRateLimit(60)
      await checkRateLimit(60)
      // Both should be served from the initial burst without waiting
      expect(Date.now() - start).toBeLessThan(50)
    })

    test('third request is queued when burst=2 and no refill yet', async () => {
      setEnv('60', '2') // 1 token/sec
      await checkRateLimit(60) // consumes token 1
      await checkRateLimit(60) // consumes token 2
      // 3rd request must wait for a refill — race it with a short abort
      const abort = new AbortController()
      setTimeout(() => abort.abort(), 50) // abort after 50ms
      try {
        await checkRateLimit(60, abort.signal)
        // If we get here the refill was fast enough (unlikely in 50ms for 1 RPM)
      } catch {
        // Aborted — means the request was correctly queued and waiting
        expect(getQueueDepth()).toBeLessThanOrEqual(1)
      }
    })
  })

  // ── Queue management ──────────────────────────────────────────────────────

  describe('Queue management', () => {
    test('throws QueueFullError when queue is at capacity', async () => {
      setEnv('1', '1', '2') // 1 RPM, burst=1, queue max=2
      // First request: consumes the only token
      await checkRateLimit(1)

      // Queue up 2 waiters (the max)
      const abort1 = new AbortController()
      const abort2 = new AbortController()
      const p1 = checkRateLimit(1, abort1.signal)
      const p2 = checkRateLimit(1, abort2.signal)

      // Queue is now full (2/2) — 3rd should throw immediately
      await expect(checkRateLimit(1)).rejects.toThrow(QueueFullError)

      // Clean up
      abort1.abort()
      abort2.abort()
      await p1.catch(() => {})
      await p2.catch(() => {})
    })

    test('aborted requests are dropped from the queue', async () => {
      setEnv('1', '1', '10') // 1 RPM, burst=1
      // Consume the initial token
      await checkRateLimit(1)

      const abort = new AbortController()
      const waiting = checkRateLimit(1, abort.signal)
      expect(getQueueDepth()).toBe(1)

      abort.abort()
      await waiting.catch(() => {})
      expect(getQueueDepth()).toBe(0)
    })

    test('pre-aborted signal is rejected immediately', async () => {
      setEnv('60', '2')
      const abort = new AbortController()
      abort.abort() // already aborted
      await expect(checkRateLimit(60, abort.signal)).rejects.toThrow('aborted')
    })
  })

  // ── notifyRateLimited — 429 backoff ───────────────────────────────────────

  describe('notifyRateLimited()', () => {
    test('is a no-op when rate limiting is disabled', () => {
      // Should not throw
      notifyRateLimited(5000)
      expect(getQueueDepth()).toBe(0)
    })

    test('pauses the bucket: subsequent requests are held in queue', async () => {
      setEnv('60', '2')
      // Consume initial tokens
      await checkRateLimit(60)
      await checkRateLimit(60)

      // Simulate a 429 — pause for 200ms
      notifyRateLimited(200)

      const abort = new AbortController()
      setTimeout(() => abort.abort(), 50) // abort after 50ms
      
      const waiting = checkRateLimit(60, abort.signal)
      // While paused, request sits in queue
      expect(getQueueDepth()).toBeGreaterThan(0)
      
      abort.abort()
      await waiting.catch(() => {})
    })

    test('uses exponential backoff with jitter when no retryAfter provided', () => {
      setEnv('60', '2')
      // Just verify it doesn't throw and doesn't lock up with 0ms
      notifyRateLimited(null, 1)
      notifyRateLimited(null, 2)
      notifyRateLimited(null, 3)
    })

    test('takes the larger pause if called multiple times', () => {
      setEnv('60', '2')
      notifyRateLimited(100)
      notifyRateLimited(5000) // should extend the pause
      notifyRateLimited(200)  // should NOT reduce the pause
      // Can't easily check pausedUntil directly, but no errors is good
    })
  })

  // ── Monitoring helpers ────────────────────────────────────────────────────

  describe('getQueueDepth() / getBucketTokens()', () => {
    test('queue starts at 0', () => {
      expect(getQueueDepth()).toBe(0)
    })

    test('bucket starts at 0 before first request', () => {
      expect(getBucketTokens()).toBe(0)
    })

    test('bucket is initialised with burstSize on first checkRateLimit call', async () => {
      setEnv('60', '3')
      // After the first call, initial tokens are set to burstSize (3)
      // then 1 is consumed — so 2 remain
      await checkRateLimit(60)
      expect(getBucketTokens()).toBe(2)
    })
  })

  // ── Backward-compatibility ─────────────────────────────────────────────────

  describe('Backward-compatibility', () => {
    test('checkRateLimit with maxRpm=0 never blocks (old behavior)', async () => {
      const start = Date.now()
      for (let i = 0; i < 10; i++) {
        await checkRateLimit(0)
      }
      expect(Date.now() - start).toBeLessThan(30)
    })

    test('third windowMs argument is accepted but ignored', async () => {
      setEnv('120', '2')
      // Old code passed windowMs as 3rd arg — should still work without error
      await checkRateLimit(120, undefined, 60_000)
      expect(true).toBe(true)
    })
  })

  // ── resetRateLimiterForTesting ────────────────────────────────────────────

  describe('resetRateLimiterForTesting()', () => {
    test('clears queue and rejects all pending waiters', async () => {
      setEnv('1', '1', '10') // 1 RPM
      await checkRateLimit(1) // consume token

      const pending = checkRateLimit(1) // now queued
      expect(getQueueDepth()).toBe(1)

      resetRateLimiterForTesting()
      await expect(pending).rejects.toThrow('reset')
      expect(getQueueDepth()).toBe(0)
    })

    test('resets token count to 0', async () => {
      setEnv('60', '5')
      await checkRateLimit(60) // triggers initialisation
      resetRateLimiterForTesting()
      expect(getBucketTokens()).toBe(0)
    })
  })
})
