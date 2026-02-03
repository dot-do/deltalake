/**
 * Retry with Exponential Backoff Tests (RED Phase)
 *
 * Tests for retry logic that handles ConcurrencyError on write conflicts.
 * Uses exponential backoff with jitter to prevent thundering herd problems.
 *
 * Coverage:
 * - Exponential backoff timing (e.g., 100ms, 200ms, 400ms, 800ms)
 * - Jitter to randomize delay slightly
 * - Configurable maxRetries (default 3)
 * - Configurable baseDelay (default 100ms)
 * - Success after N retries when conflict resolves
 * - Failure after exhausting all retries
 * - Original error preserved when retries exhausted
 * - Works with DeltaTable.write() on ConcurrencyError
 * - Non-retryable errors pass through immediately
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { MemoryStorage } from '../../../src/storage/index'
import { DeltaTable } from '../../../src/delta/index'

// Import retry utilities (will fail until implemented)
// @ts-expect-error - Module does not exist yet (RED phase)
import {
  withRetry,
  ConcurrencyError,
  RetryConfig,
  RetryMetrics,
  isRetryableError,
  DEFAULT_RETRY_CONFIG,
} from '../../../src/delta/retry'

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a function that fails N times then succeeds
 */
function createFailingThenSucceedingFn<T>(
  failCount: number,
  successValue: T,
  errorFactory: () => Error = () => new ConcurrencyError('Concurrent modification detected')
): jest.Mock<Promise<T>> {
  let callCount = 0
  return vi.fn(async () => {
    callCount++
    if (callCount <= failCount) {
      throw errorFactory()
    }
    return successValue
  })
}

/**
 * Create a function that always fails with the given error
 */
function createAlwaysFailingFn(error: Error): jest.Mock<Promise<never>> {
  return vi.fn(async () => {
    throw error
  })
}

/**
 * Helper to measure elapsed time of an async operation
 */
async function measureTime<T>(fn: () => Promise<T>): Promise<{ result: T; elapsedMs: number }> {
  const start = Date.now()
  const result = await fn()
  const elapsedMs = Date.now() - start
  return { result, elapsedMs }
}

/**
 * Helper to capture all delay times during retries
 */
function createDelayCapture(): { delays: number[]; mockDelay: (ms: number) => Promise<void> } {
  const delays: number[] = []
  const mockDelay = vi.fn(async (ms: number) => {
    delays.push(ms)
    // Don't actually wait in tests
  })
  return { delays, mockDelay }
}

// =============================================================================
// CONCURRENCY ERROR TYPE TESTS
// =============================================================================

describe('ConcurrencyError', () => {
  it('should be an instance of Error', () => {
    const error = new ConcurrencyError('test message')
    expect(error).toBeInstanceOf(Error)
  })

  it('should have correct name property', () => {
    const error = new ConcurrencyError('test message')
    expect(error.name).toBe('ConcurrencyError')
  })

  it('should preserve the error message', () => {
    const message = 'Concurrent modification detected'
    const error = new ConcurrencyError(message)
    expect(error.message).toBe(message)
  })

  it('should have a stack trace', () => {
    const error = new ConcurrencyError('test')
    expect(error.stack).toBeDefined()
  })

  it('should support optional version information', () => {
    const error = new ConcurrencyError('Conflict at version 5', { expectedVersion: 4, actualVersion: 5 })
    expect(error.expectedVersion).toBe(4)
    expect(error.actualVersion).toBe(5)
  })

  it('should be identifiable via isRetryableError', () => {
    const error = new ConcurrencyError('test')
    expect(isRetryableError(error)).toBe(true)
  })
})

// =============================================================================
// isRetryableError TESTS
// =============================================================================

describe('isRetryableError', () => {
  it('should return true for ConcurrencyError', () => {
    const error = new ConcurrencyError('test')
    expect(isRetryableError(error)).toBe(true)
  })

  it('should return false for generic Error', () => {
    const error = new Error('generic error')
    expect(isRetryableError(error)).toBe(false)
  })

  it('should return false for TypeError', () => {
    const error = new TypeError('type error')
    expect(isRetryableError(error)).toBe(false)
  })

  it('should return false for SyntaxError', () => {
    const error = new SyntaxError('syntax error')
    expect(isRetryableError(error)).toBe(false)
  })

  it('should return true for error with retryable: true property', () => {
    const error: Error & { retryable?: boolean } = new Error('custom retryable')
    error.retryable = true
    expect(isRetryableError(error)).toBe(true)
  })

  it('should return false for null', () => {
    expect(isRetryableError(null as any)).toBe(false)
  })

  it('should return false for undefined', () => {
    expect(isRetryableError(undefined as any)).toBe(false)
  })

  it('should return false for non-error objects', () => {
    expect(isRetryableError({ message: 'not an error' } as any)).toBe(false)
  })
})

// =============================================================================
// DEFAULT CONFIG TESTS
// =============================================================================

describe('DEFAULT_RETRY_CONFIG', () => {
  it('should have maxRetries of 3', () => {
    expect(DEFAULT_RETRY_CONFIG.maxRetries).toBe(3)
  })

  it('should have baseDelay of 100ms', () => {
    expect(DEFAULT_RETRY_CONFIG.baseDelay).toBe(100)
  })

  it('should have maxDelay of 10000ms', () => {
    expect(DEFAULT_RETRY_CONFIG.maxDelay).toBe(10000)
  })

  it('should have jitter enabled by default', () => {
    expect(DEFAULT_RETRY_CONFIG.jitter).toBe(true)
  })

  it('should have exponential multiplier of 2', () => {
    expect(DEFAULT_RETRY_CONFIG.multiplier).toBe(2)
  })
})

// =============================================================================
// EXPONENTIAL BACKOFF TIMING TESTS
// =============================================================================

describe('Exponential Backoff Timing', () => {
  let delayCapture: ReturnType<typeof createDelayCapture>

  beforeEach(() => {
    delayCapture = createDelayCapture()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should use exponential delays: 100ms, 200ms, 400ms', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')

    await withRetry(fn, {
      maxRetries: 4,
      baseDelay: 100,
      jitter: false, // Disable jitter for deterministic testing
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toEqual([100, 200, 400])
  })

  it('should double delay on each retry', async () => {
    const fn = createFailingThenSucceedingFn(4, 'success')

    await withRetry(fn, {
      maxRetries: 5,
      baseDelay: 50,
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toEqual([50, 100, 200, 400])
  })

  it('should support custom multiplier', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')

    await withRetry(fn, {
      maxRetries: 4,
      baseDelay: 100,
      multiplier: 3,
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toEqual([100, 300, 900])
  })

  it('should cap delay at maxDelay', async () => {
    const fn = createFailingThenSucceedingFn(5, 'success')

    await withRetry(fn, {
      maxRetries: 6,
      baseDelay: 100,
      maxDelay: 500,
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    // 100, 200, 400, 500 (capped), 500 (capped)
    expect(delayCapture.delays).toEqual([100, 200, 400, 500, 500])
  })

  it('should not delay before first attempt', async () => {
    const fn = vi.fn().mockResolvedValue('immediate')

    await withRetry(fn, {
      maxRetries: 3,
      baseDelay: 100,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toHaveLength(0)
    expect(fn).toHaveBeenCalledTimes(1)
  })
})

// =============================================================================
// JITTER TESTS
// =============================================================================

describe('Jitter', () => {
  let delayCapture: ReturnType<typeof createDelayCapture>

  beforeEach(() => {
    delayCapture = createDelayCapture()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('should add random jitter to delays when enabled', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')

    await withRetry(fn, {
      maxRetries: 4,
      baseDelay: 100,
      jitter: true,
      _delayFn: delayCapture.mockDelay,
    })

    // With jitter, delays should vary around the base values
    expect(delayCapture.delays).toHaveLength(3)

    // First delay should be around 100ms (with jitter typically +/- 50%)
    expect(delayCapture.delays[0]).toBeGreaterThanOrEqual(50)
    expect(delayCapture.delays[0]).toBeLessThanOrEqual(150)

    // Second delay should be around 200ms
    expect(delayCapture.delays[1]).toBeGreaterThanOrEqual(100)
    expect(delayCapture.delays[1]).toBeLessThanOrEqual(300)
  })

  it('should produce different delays on multiple runs', async () => {
    const results: number[][] = []

    for (let i = 0; i < 5; i++) {
      const capture = createDelayCapture()
      const fn = createFailingThenSucceedingFn(2, 'success')

      await withRetry(fn, {
        maxRetries: 3,
        baseDelay: 100,
        jitter: true,
        _delayFn: capture.mockDelay,
      })

      results.push([...capture.delays])
    }

    // Not all results should be identical (with jitter)
    const allSame = results.every(
      r => r[0] === results[0][0] && r[1] === results[0][1]
    )
    expect(allSame).toBe(false)
  })

  it('should not exceed baseDelay * (1 + jitterFactor)', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success')

    await withRetry(fn, {
      maxRetries: 2,
      baseDelay: 100,
      jitter: true,
      jitterFactor: 0.5, // +/- 50%
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays[0]).toBeGreaterThanOrEqual(50)
    expect(delayCapture.delays[0]).toBeLessThanOrEqual(150)
  })

  it('should support custom jitter factor', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success')

    await withRetry(fn, {
      maxRetries: 2,
      baseDelay: 100,
      jitter: true,
      jitterFactor: 0.25, // +/- 25%
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays[0]).toBeGreaterThanOrEqual(75)
    expect(delayCapture.delays[0]).toBeLessThanOrEqual(125)
  })

  it('should apply jitter before maxDelay cap', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')

    await withRetry(fn, {
      maxRetries: 4,
      baseDelay: 400,
      maxDelay: 500,
      jitter: true,
      _delayFn: delayCapture.mockDelay,
    })

    // All delays should be <= maxDelay even with jitter
    for (const delay of delayCapture.delays) {
      expect(delay).toBeLessThanOrEqual(500)
    }
  })
})

// =============================================================================
// CONFIGURABLE maxRetries TESTS
// =============================================================================

describe('Configurable maxRetries', () => {
  it('should default to 3 retries', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('always fails'))

    await expect(withRetry(fn)).rejects.toThrow()

    // 1 initial + 3 retries = 4 total calls
    expect(fn).toHaveBeenCalledTimes(4)
  })

  it('should respect custom maxRetries of 1', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('always fails'))

    await expect(withRetry(fn, { maxRetries: 1 })).rejects.toThrow()

    // 1 initial + 1 retry = 2 total calls
    expect(fn).toHaveBeenCalledTimes(2)
  })

  it('should respect custom maxRetries of 5', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('always fails'))

    await expect(withRetry(fn, { maxRetries: 5 })).rejects.toThrow()

    // 1 initial + 5 retries = 6 total calls
    expect(fn).toHaveBeenCalledTimes(6)
  })

  it('should not retry when maxRetries is 0', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('always fails'))

    await expect(withRetry(fn, { maxRetries: 0 })).rejects.toThrow()

    // Only initial attempt
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('should succeed without retries if first attempt succeeds', async () => {
    const fn = vi.fn().mockResolvedValue('success')

    const result = await withRetry(fn, { maxRetries: 5 })

    expect(result).toBe('success')
    expect(fn).toHaveBeenCalledTimes(1)
  })
})

// =============================================================================
// CONFIGURABLE baseDelay TESTS
// =============================================================================

describe('Configurable baseDelay', () => {
  let delayCapture: ReturnType<typeof createDelayCapture>

  beforeEach(() => {
    delayCapture = createDelayCapture()
  })

  it('should default to 100ms baseDelay', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success')

    await withRetry(fn, {
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays[0]).toBe(100)
  })

  it('should respect custom baseDelay of 50ms', async () => {
    const fn = createFailingThenSucceedingFn(2, 'success')

    await withRetry(fn, {
      baseDelay: 50,
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toEqual([50, 100])
  })

  it('should respect custom baseDelay of 500ms', async () => {
    const fn = createFailingThenSucceedingFn(2, 'success')

    await withRetry(fn, {
      baseDelay: 500,
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toEqual([500, 1000])
  })

  it('should use baseDelay of 1ms for fast retries', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')

    await withRetry(fn, {
      baseDelay: 1,
      jitter: false,
      _delayFn: delayCapture.mockDelay,
    })

    expect(delayCapture.delays).toEqual([1, 2, 4])
  })
})

// =============================================================================
// SUCCESS AFTER N RETRIES TESTS
// =============================================================================

describe('Success after N retries', () => {
  it('should succeed on second attempt (1 retry)', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success after 1 retry')

    const result = await withRetry(fn, { maxRetries: 3 })

    expect(result).toBe('success after 1 retry')
    expect(fn).toHaveBeenCalledTimes(2)
  })

  it('should succeed on third attempt (2 retries)', async () => {
    const fn = createFailingThenSucceedingFn(2, 'success after 2 retries')

    const result = await withRetry(fn, { maxRetries: 3 })

    expect(result).toBe('success after 2 retries')
    expect(fn).toHaveBeenCalledTimes(3)
  })

  it('should succeed on final retry attempt', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success on final retry')

    const result = await withRetry(fn, { maxRetries: 3 })

    expect(result).toBe('success on final retry')
    expect(fn).toHaveBeenCalledTimes(4)
  })

  it('should return the successful value', async () => {
    const successValue = { data: [1, 2, 3], version: 5 }
    const fn = createFailingThenSucceedingFn(2, successValue)

    const result = await withRetry(fn, { maxRetries: 3 })

    expect(result).toEqual(successValue)
  })

  it('should pass through to function with context preserved', async () => {
    let callCount = 0
    const fn = vi.fn(async function(this: { value: number }) {
      callCount++
      if (callCount < 2) {
        throw new ConcurrencyError('retry')
      }
      return this?.value ?? 'no context'
    })

    const context = { value: 42 }
    const result = await withRetry(fn.bind(context), { maxRetries: 3 })

    expect(result).toBe(42)
  })
})

// =============================================================================
// FAILURE AFTER EXHAUSTING RETRIES TESTS
// =============================================================================

describe('Failure after exhausting retries', () => {
  it('should throw after exhausting all retries', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('persistent conflict'))

    await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow('persistent conflict')
    expect(fn).toHaveBeenCalledTimes(4)
  })

  it('should throw after exhausting 5 retries', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('conflict'))

    await expect(withRetry(fn, { maxRetries: 5 })).rejects.toThrow()
    expect(fn).toHaveBeenCalledTimes(6)
  })

  it('should fail when function fails more times than maxRetries', async () => {
    const fn = createFailingThenSucceedingFn(5, 'success')

    await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow()
    expect(fn).toHaveBeenCalledTimes(4) // 1 initial + 3 retries
  })
})

// =============================================================================
// ORIGINAL ERROR PRESERVED TESTS
// =============================================================================

describe('Original error preserved', () => {
  it('should preserve original error message', async () => {
    const originalMessage = 'Version conflict: expected 5, got 6'
    const fn = createAlwaysFailingFn(new ConcurrencyError(originalMessage))

    await expect(withRetry(fn, { maxRetries: 2 })).rejects.toThrow(originalMessage)
  })

  it('should preserve error type', async () => {
    const error = new ConcurrencyError('conflict')
    const fn = createAlwaysFailingFn(error)

    try {
      await withRetry(fn, { maxRetries: 2 })
      expect.fail('Should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(ConcurrencyError)
    }
  })

  it('should preserve custom error properties', async () => {
    const error = new ConcurrencyError('conflict', {
      expectedVersion: 10,
      actualVersion: 11,
    })
    const fn = createAlwaysFailingFn(error)

    try {
      await withRetry(fn, { maxRetries: 1 })
      expect.fail('Should have thrown')
    } catch (e: any) {
      expect(e.expectedVersion).toBe(10)
      expect(e.actualVersion).toBe(11)
    }
  })

  it('should preserve error stack trace', async () => {
    const error = new ConcurrencyError('conflict')
    const fn = createAlwaysFailingFn(error)

    try {
      await withRetry(fn, { maxRetries: 1 })
      expect.fail('Should have thrown')
    } catch (e: any) {
      expect(e.stack).toBeDefined()
      expect(e.stack).toContain('ConcurrencyError')
    }
  })

  it('should throw the last error when all retries fail', async () => {
    let callCount = 0
    const fn = vi.fn(async () => {
      callCount++
      throw new ConcurrencyError(`Conflict attempt ${callCount}`)
    })

    try {
      await withRetry(fn, { maxRetries: 3 })
      expect.fail('Should have thrown')
    } catch (e: any) {
      expect(e.message).toBe('Conflict attempt 4')
    }
  })
})

// =============================================================================
// NON-RETRYABLE ERRORS TESTS
// =============================================================================

describe('Non-retryable errors pass through immediately', () => {
  it('should not retry on generic Error', async () => {
    const fn = createAlwaysFailingFn(new Error('generic error'))

    await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow('generic error')
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('should not retry on TypeError', async () => {
    const fn = createAlwaysFailingFn(new TypeError('type error'))

    await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow('type error')
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('should not retry on RangeError', async () => {
    const fn = createAlwaysFailingFn(new RangeError('range error'))

    await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow('range error')
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('should not retry on SyntaxError', async () => {
    const fn = createAlwaysFailingFn(new SyntaxError('syntax error'))

    await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow('syntax error')
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('should preserve non-retryable error properties', async () => {
    const error = new TypeError('invalid type')
    ;(error as any).code = 'ERR_INVALID_TYPE'
    const fn = createAlwaysFailingFn(error)

    try {
      await withRetry(fn, { maxRetries: 3 })
      expect.fail('Should have thrown')
    } catch (e: any) {
      expect(e).toBeInstanceOf(TypeError)
      expect(e.code).toBe('ERR_INVALID_TYPE')
    }
  })

  it('should support custom isRetryable predicate', async () => {
    const customError = new Error('custom retryable')
    ;(customError as any).code = 'ECONNRESET'

    const fn = createFailingThenSucceedingFn(
      2,
      'success',
      () => {
        const e = new Error('connection reset')
        ;(e as any).code = 'ECONNRESET'
        return e
      }
    )

    const result = await withRetry(fn, {
      maxRetries: 3,
      isRetryable: (err) => (err as any).code === 'ECONNRESET',
    })

    expect(result).toBe('success')
    expect(fn).toHaveBeenCalledTimes(3)
  })
})

// =============================================================================
// DELTA TABLE INTEGRATION TESTS
// =============================================================================

describe('DeltaTable.write() with retry', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should retry write on ConcurrencyError', async () => {
    const table = new DeltaTable(storage, 'test-table')

    // First write succeeds
    await table.write([{ id: 1, value: 'initial' }])

    // Simulate concurrent modification
    let attempt = 0
    const writeWithConflict = vi.fn(async (rows: any[]) => {
      attempt++
      if (attempt < 3) {
        throw new ConcurrencyError('Concurrent modification')
      }
      return table.write(rows)
    })

    const result = await withRetry(() => writeWithConflict([{ id: 2, value: 'new' }]), {
      maxRetries: 3,
    })

    expect(result).toBeDefined()
    expect(writeWithConflict).toHaveBeenCalledTimes(3)
  })

  it('should succeed after conflict resolves', async () => {
    const table1 = new DeltaTable(storage, 'concurrent-table')
    const table2 = new DeltaTable(storage, 'concurrent-table')

    // Initialize table
    await table1.write([{ id: 1 }])

    let attempt = 0
    const conflictingWrite = async () => {
      attempt++
      if (attempt === 1) {
        // Simulate another writer winning the race
        await table1.write([{ id: 2 }])
        throw new ConcurrencyError('Lost race condition')
      }
      // Refresh and retry
      return table2.write([{ id: 3 }])
    }

    const result = await withRetry(conflictingWrite, { maxRetries: 3 })

    expect(result).toBeDefined()
  })

  it('should fail after max retries on persistent conflict', async () => {
    const table = new DeltaTable(storage, 'persistent-conflict-table')
    await table.write([{ id: 1 }])

    const alwaysConflicting = vi.fn(async () => {
      throw new ConcurrencyError('Persistent conflict')
    })

    await expect(
      withRetry(alwaysConflicting, { maxRetries: 3 })
    ).rejects.toThrow('Persistent conflict')

    expect(alwaysConflicting).toHaveBeenCalledTimes(4)
  })

  it('should pass through validation errors without retry', async () => {
    const table = new DeltaTable(storage, 'validation-table')

    const writeEmpty = async () => {
      return table.write([]) // Should throw validation error
    }

    await expect(
      withRetry(writeEmpty, { maxRetries: 3 })
    ).rejects.toThrow('Cannot write empty data')

    // Should only attempt once (no retries for validation errors)
  })
})

// =============================================================================
// RETRY METRICS TESTS
// =============================================================================

describe('Retry Metrics', () => {
  it('should return metrics when requested', async () => {
    const fn = createFailingThenSucceedingFn(2, 'success')

    const { result, metrics } = await withRetry(fn, {
      maxRetries: 3,
      returnMetrics: true,
    })

    expect(result).toBe('success')
    expect(metrics).toBeDefined()
    expect(metrics.attempts).toBe(3)
    expect(metrics.retries).toBe(2)
    expect(metrics.succeeded).toBe(true)
  })

  it('should track total elapsed time', async () => {
    const fn = createFailingThenSucceedingFn(2, 'success')
    const delayCapture = createDelayCapture()

    const { metrics } = await withRetry(fn, {
      maxRetries: 3,
      baseDelay: 100,
      jitter: false,
      returnMetrics: true,
      _delayFn: delayCapture.mockDelay,
    })

    expect(metrics.totalDelayMs).toBe(300) // 100 + 200
    expect(metrics.elapsedMs).toBeGreaterThanOrEqual(0)
  })

  it('should track individual delay times', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')
    const delayCapture = createDelayCapture()

    const { metrics } = await withRetry(fn, {
      maxRetries: 4,
      baseDelay: 100,
      jitter: false,
      returnMetrics: true,
      _delayFn: delayCapture.mockDelay,
    })

    expect(metrics.delays).toEqual([100, 200, 400])
  })

  it('should include error history', async () => {
    let count = 0
    const fn = vi.fn(async () => {
      count++
      if (count <= 2) {
        throw new ConcurrencyError(`Error ${count}`)
      }
      return 'success'
    })

    const { metrics } = await withRetry(fn, {
      maxRetries: 3,
      returnMetrics: true,
    })

    expect(metrics.errors).toHaveLength(2)
    expect(metrics.errors[0].message).toBe('Error 1')
    expect(metrics.errors[1].message).toBe('Error 2')
  })

  it('should indicate failure in metrics', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('always fails'))

    try {
      await withRetry(fn, {
        maxRetries: 2,
        returnMetrics: true,
      })
      expect.fail('Should have thrown')
    } catch (e: any) {
      expect(e.metrics).toBeDefined()
      expect(e.metrics.succeeded).toBe(false)
      expect(e.metrics.attempts).toBe(3)
      expect(e.metrics.retries).toBe(2)
    }
  })
})

// =============================================================================
// CALLBACK/HOOK TESTS
// =============================================================================

describe('Retry callbacks and hooks', () => {
  it('should call onRetry before each retry', async () => {
    const fn = createFailingThenSucceedingFn(3, 'success')
    const onRetry = vi.fn()

    await withRetry(fn, {
      maxRetries: 4,
      baseDelay: 1,
      onRetry,
    })

    expect(onRetry).toHaveBeenCalledTimes(3)
  })

  it('should pass attempt info to onRetry', async () => {
    const fn = createFailingThenSucceedingFn(2, 'success')
    const onRetry = vi.fn()

    await withRetry(fn, {
      maxRetries: 3,
      baseDelay: 100,
      jitter: false,
      onRetry,
    })

    expect(onRetry).toHaveBeenNthCalledWith(1, expect.objectContaining({
      attempt: 1,
      error: expect.any(ConcurrencyError),
      delay: 100,
    }))

    expect(onRetry).toHaveBeenNthCalledWith(2, expect.objectContaining({
      attempt: 2,
      error: expect.any(ConcurrencyError),
      delay: 200,
    }))
  })

  it('should call onSuccess when successful', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success')
    const onSuccess = vi.fn()

    await withRetry(fn, {
      maxRetries: 3,
      onSuccess,
    })

    expect(onSuccess).toHaveBeenCalledTimes(1)
    expect(onSuccess).toHaveBeenCalledWith(expect.objectContaining({
      result: 'success',
      attempts: 2,
    }))
  })

  it('should call onFailure when all retries exhausted', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('fail'))
    const onFailure = vi.fn()

    await expect(withRetry(fn, {
      maxRetries: 2,
      onFailure,
    })).rejects.toThrow()

    expect(onFailure).toHaveBeenCalledTimes(1)
    expect(onFailure).toHaveBeenCalledWith(expect.objectContaining({
      error: expect.any(ConcurrencyError),
      attempts: 3,
    }))
  })

  it('should allow onRetry to abort retries', async () => {
    const fn = createAlwaysFailingFn(new ConcurrencyError('fail'))
    const onRetry = vi.fn().mockImplementation(({ attempt }) => {
      if (attempt >= 2) {
        return false // Abort
      }
      return true
    })

    await expect(withRetry(fn, {
      maxRetries: 5,
      onRetry,
    })).rejects.toThrow()

    expect(fn).toHaveBeenCalledTimes(3) // 1 initial + 2 retries (aborted before 3rd retry)
  })
})

// =============================================================================
// ABORT CONTROLLER SUPPORT TESTS
// =============================================================================

describe('AbortController support', () => {
  it('should respect AbortController signal', async () => {
    const controller = new AbortController()
    const fn = createAlwaysFailingFn(new ConcurrencyError('retry'))

    setTimeout(() => controller.abort(), 50)

    await expect(withRetry(fn, {
      maxRetries: 10,
      baseDelay: 100,
      signal: controller.signal,
    })).rejects.toThrow(/abort/i)
  })

  it('should stop retrying when aborted', async () => {
    const controller = new AbortController()
    const fn = vi.fn(async () => {
      throw new ConcurrencyError('retry')
    })

    const promise = withRetry(fn, {
      maxRetries: 10,
      baseDelay: 50,
      signal: controller.signal,
    })

    // Abort after a short delay
    await new Promise(resolve => setTimeout(resolve, 75))
    controller.abort()

    await expect(promise).rejects.toThrow()
    expect(fn.mock.calls.length).toBeLessThan(11) // Should have been stopped
  })

  it('should throw AbortError with correct name', async () => {
    const controller = new AbortController()
    controller.abort()

    const fn = vi.fn().mockRejectedValue(new ConcurrencyError('test'))

    try {
      await withRetry(fn, {
        maxRetries: 3,
        signal: controller.signal,
      })
      expect.fail('Should have thrown')
    } catch (e: any) {
      expect(e.name).toBe('AbortError')
    }
  })

  it('should not start if already aborted', async () => {
    const controller = new AbortController()
    controller.abort()

    const fn = vi.fn().mockResolvedValue('success')

    await expect(withRetry(fn, {
      maxRetries: 3,
      signal: controller.signal,
    })).rejects.toThrow(/abort/i)

    expect(fn).not.toHaveBeenCalled()
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING TESTS
// =============================================================================

describe('Edge cases and error handling', () => {
  it('should handle function that throws synchronously', async () => {
    const fn = vi.fn(() => {
      throw new ConcurrencyError('sync throw')
    })

    await expect(withRetry(fn as any, { maxRetries: 2 })).rejects.toThrow('sync throw')
    expect(fn).toHaveBeenCalledTimes(3)
  })

  it('should handle function that returns rejected promise', async () => {
    const fn = vi.fn(() => Promise.reject(new ConcurrencyError('rejected')))

    await expect(withRetry(fn, { maxRetries: 2 })).rejects.toThrow('rejected')
    expect(fn).toHaveBeenCalledTimes(3)
  })

  it('should handle function that returns non-promise', async () => {
    const fn = vi.fn(() => 'sync value')

    const result = await withRetry(fn as any, { maxRetries: 3 })

    expect(result).toBe('sync value')
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it('should handle null return value', async () => {
    const fn = vi.fn().mockResolvedValue(null)

    const result = await withRetry(fn, { maxRetries: 3 })

    expect(result).toBeNull()
  })

  it('should handle undefined return value', async () => {
    const fn = vi.fn().mockResolvedValue(undefined)

    const result = await withRetry(fn, { maxRetries: 3 })

    expect(result).toBeUndefined()
  })

  it('should handle function with arguments', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success')

    const wrappedFn = (arg1: string, arg2: number) => fn()
    const result = await withRetry(() => wrappedFn('test', 42), { maxRetries: 3 })

    expect(result).toBe('success')
  })

  it('should handle very fast retries', async () => {
    const fn = createFailingThenSucceedingFn(5, 'success')

    const result = await withRetry(fn, {
      maxRetries: 10,
      baseDelay: 0,
    })

    expect(result).toBe('success')
  })

  it('should handle error with circular reference', async () => {
    const error: any = new ConcurrencyError('circular')
    error.circular = error // Create circular reference

    const fn = createAlwaysFailingFn(error)

    await expect(withRetry(fn, { maxRetries: 1 })).rejects.toThrow('circular')
  })

  it('should handle error thrown during delay', async () => {
    const fn = createFailingThenSucceedingFn(1, 'success')
    const badDelay = vi.fn(async () => {
      throw new Error('delay error')
    })

    await expect(withRetry(fn, {
      maxRetries: 3,
      _delayFn: badDelay,
    })).rejects.toThrow('delay error')
  })
})

// =============================================================================
// CONCURRENT RETRY OPERATIONS TESTS
// =============================================================================

describe('Concurrent retry operations', () => {
  it('should handle multiple independent retries', async () => {
    const fn1 = createFailingThenSucceedingFn(1, 'result1')
    const fn2 = createFailingThenSucceedingFn(2, 'result2')
    const fn3 = createFailingThenSucceedingFn(0, 'result3')

    const [r1, r2, r3] = await Promise.all([
      withRetry(fn1, { maxRetries: 3 }),
      withRetry(fn2, { maxRetries: 3 }),
      withRetry(fn3, { maxRetries: 3 }),
    ])

    expect(r1).toBe('result1')
    expect(r2).toBe('result2')
    expect(r3).toBe('result3')
  })

  it('should isolate retry state between calls', async () => {
    let call1Count = 0
    let call2Count = 0

    const fn1 = vi.fn(async () => {
      call1Count++
      if (call1Count < 3) throw new ConcurrencyError('retry1')
      return 'success1'
    })

    const fn2 = vi.fn(async () => {
      call2Count++
      if (call2Count < 2) throw new ConcurrencyError('retry2')
      return 'success2'
    })

    const [r1, r2] = await Promise.all([
      withRetry(fn1, { maxRetries: 5 }),
      withRetry(fn2, { maxRetries: 5 }),
    ])

    expect(r1).toBe('success1')
    expect(r2).toBe('success2')
    expect(fn1).toHaveBeenCalledTimes(3)
    expect(fn2).toHaveBeenCalledTimes(2)
  })
})

// =============================================================================
// TYPE DEFINITIONS FOR TESTS (Expected API)
// =============================================================================

/**
 * Configuration options for retry behavior
 */
interface RetryConfig {
  /** Maximum number of retry attempts (default: 3) */
  maxRetries?: number
  /** Base delay in milliseconds (default: 100) */
  baseDelay?: number
  /** Maximum delay in milliseconds (default: 10000) */
  maxDelay?: number
  /** Multiplier for exponential backoff (default: 2) */
  multiplier?: number
  /** Whether to add random jitter to delays (default: true) */
  jitter?: boolean
  /** Jitter factor (default: 0.5, meaning +/- 50%) */
  jitterFactor?: number
  /** Custom predicate to determine if error is retryable */
  isRetryable?: (error: Error) => boolean
  /** Called before each retry attempt */
  onRetry?: (info: RetryInfo) => boolean | void
  /** Called on successful completion */
  onSuccess?: (info: SuccessInfo) => void
  /** Called when all retries are exhausted */
  onFailure?: (info: FailureInfo) => void
  /** Whether to return metrics with result */
  returnMetrics?: boolean
  /** AbortController signal to cancel retries */
  signal?: AbortSignal
  /** Internal: custom delay function for testing */
  _delayFn?: (ms: number) => Promise<void>
}

interface RetryInfo {
  attempt: number
  error: Error
  delay: number
}

interface SuccessInfo {
  result: unknown
  attempts: number
}

interface FailureInfo {
  error: Error
  attempts: number
}

interface RetryMetrics {
  attempts: number
  retries: number
  succeeded: boolean
  totalDelayMs: number
  elapsedMs: number
  delays: number[]
  errors: Error[]
}
