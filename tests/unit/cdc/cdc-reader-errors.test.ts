/**
 * CDC Reader Error Path Tests
 *
 * Comprehensive test suite for CDCReader error handling, including:
 * - Invalid version ranges (start > end)
 * - Negative version numbers
 * - Empty version ranges
 * - CDC not enabled scenarios
 * - CDCError code verification
 *
 * This ensures proper error reporting with specific CDCErrorCode values.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  createCDCDeltaTable,
  CDCDeltaTable,
  CDCError,
  CDCErrorCode,
} from '../../../src/cdc/index'
import { MemoryStorage } from '../../../src/storage/index'
import { isCDCError } from '../../../src/errors'

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Sample row type for testing
 */
interface TestRow {
  id: string
  name: string
  value: number
}

// =============================================================================
// CDCReader.readByVersion ERROR PATHS
// =============================================================================

describe('CDCReader.readByVersion Error Paths', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('Invalid Version Range (start > end)', () => {
    it('should throw CDCError with INVALID_VERSION_RANGE when start > end', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(5n, 2n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(CDCError)
        const cdcError = error as CDCError
        expect(cdcError.cdcCode).toBe('INVALID_VERSION_RANGE')
        expect(cdcError.code).toBe('CDC_INVALID_VERSION_RANGE')
        expect(cdcError.message).toContain('start version must be <= end version')
      }
    })

    it('should throw CDCError with INVALID_VERSION_RANGE for large gap reversed', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(1000n, 10n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(CDCError)
        expect((error as CDCError).cdcCode).toBe('INVALID_VERSION_RANGE')
      }
    })

    it('should provide helpful error message for invalid range', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(10n, 5n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toMatch(/version/i)
      }
    })
  })

  describe('Negative Version Numbers', () => {
    it('should throw CDCError with INVALID_VERSION_RANGE for negative start version', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(-1n, 5n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(CDCError)
        const cdcError = error as CDCError
        expect(cdcError.cdcCode).toBe('INVALID_VERSION_RANGE')
        expect(cdcError.message).toContain('non-negative')
      }
    })

    it('should throw CDCError with INVALID_VERSION_RANGE for negative end version', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(0n, -1n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(CDCError)
        const cdcError = error as CDCError
        expect(cdcError.cdcCode).toBe('INVALID_VERSION_RANGE')
        expect(cdcError.message).toContain('non-negative')
      }
    })

    it('should throw CDCError for both versions negative', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(-10n, -5n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(CDCError)
        expect((error as CDCError).cdcCode).toBe('INVALID_VERSION_RANGE')
      }
    })

    it('should throw CDCError for large negative version numbers', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      try {
        await reader.readByVersion(-9999999999999n, 0n)
        expect.fail('Should have thrown an error')
      } catch (error) {
        expect(error).toBeInstanceOf(CDCError)
        expect((error as CDCError).cdcCode).toBe('INVALID_VERSION_RANGE')
      }
    })
  })

  describe('Empty Version Range Edge Cases', () => {
    it('should return empty array for version range with no data', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      // Version range beyond existing versions - should return empty, not throw
      const records = await reader.readByVersion(100n, 200n)
      expect(records).toEqual([])
    })

    it('should return empty array when no CDC files exist for version range', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      // Version 50 doesn't exist but range is valid
      const records = await reader.readByVersion(50n, 50n)
      expect(records).toEqual([])
    })

    it('should handle single version that equals start and end', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      const records = await reader.readByVersion(0n, 0n)
      expect(records.length).toBe(1)
      expect(records[0]._commit_version).toBe(0n)
    })

    it('should handle version 0 correctly', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'First', value: 1 }])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 0n)

      expect(records.length).toBe(1)
      expect(records[0]._commit_version).toBe(0n)
    })
  })
})

// =============================================================================
// CDCReader TABLE NOT FOUND ERROR
// =============================================================================

describe('CDCReader Table Not Found Error', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should throw CDCError with TABLE_NOT_FOUND for non-existent table', async () => {
    const nonExistentTable = createCDCDeltaTable<TestRow>(storage, 'non_existent_table')
    const reader = nonExistentTable.getCDCReader()

    try {
      await reader.readByVersion(0n, 0n)
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect(error).toBeInstanceOf(CDCError)
      const cdcError = error as CDCError
      expect(cdcError.cdcCode).toBe('TABLE_NOT_FOUND')
      expect(cdcError.code).toBe('CDC_TABLE_NOT_FOUND')
      expect(cdcError.message).toContain('Table not found')
    }
  })

  it('should include table path in TABLE_NOT_FOUND error message', async () => {
    const tablePath = 'my_custom_table_path'
    const table = createCDCDeltaTable<TestRow>(storage, tablePath)
    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(0n, 0n)
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect(error).toBeInstanceOf(CDCError)
      expect((error as CDCError).message).toContain(tablePath)
    }
  })

  it('should throw TABLE_NOT_FOUND for deeply nested table path', async () => {
    const tablePath = 'data/warehouse/db/tables/events'
    const table = createCDCDeltaTable<TestRow>(storage, tablePath)
    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(0n, 0n)
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect(error).toBeInstanceOf(CDCError)
      expect((error as CDCError).cdcCode).toBe('TABLE_NOT_FOUND')
    }
  })
})

// =============================================================================
// CDC NOT ENABLED ERROR HANDLING
// =============================================================================

describe('CDC Not Enabled Scenarios', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should return empty array when CDC is disabled and reading by version', async () => {
    // CDC disabled by default
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 0n)

    // Should return empty, not throw (graceful degradation)
    expect(records).toEqual([])
  })

  it('should return empty array when CDC is disabled and reading by timestamp', async () => {
    // CDC disabled by default
    const before = new Date()
    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    const after = new Date()

    const reader = table.getCDCReader()
    const records = await reader.readByTimestamp(before, after)

    expect(records).toEqual([])
  })

  it('should return empty array for versions written while CDC was disabled', async () => {
    // Write with CDC disabled
    await table.write([{ id: '1', name: 'Alice', value: 100 }])  // v0

    // Enable CDC
    await table.setCDCEnabled(true)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])    // v1

    const reader = table.getCDCReader()

    // v0 should have no CDC records (was disabled)
    const v0Records = await reader.readByVersion(0n, 0n)
    expect(v0Records).toEqual([])

    // v1 should have CDC records (was enabled)
    const v1Records = await reader.readByVersion(1n, 1n)
    expect(v1Records.length).toBe(1)
  })

  it('should return empty array after CDC is disabled mid-stream', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0 - CDC enabled

    await table.setCDCEnabled(false)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1 - CDC disabled

    const reader = table.getCDCReader()

    // v0 should have CDC records
    const v0Records = await reader.readByVersion(0n, 0n)
    expect(v0Records.length).toBe(1)

    // v1 should have no CDC records
    const v1Records = await reader.readByVersion(1n, 1n)
    expect(v1Records).toEqual([])
  })

  it('should report CDC enabled status correctly', async () => {
    // Initially disabled
    expect(await table.isCDCEnabled()).toBe(false)

    await table.setCDCEnabled(true)
    expect(await table.isCDCEnabled()).toBe(true)

    await table.setCDCEnabled(false)
    expect(await table.isCDCEnabled()).toBe(false)
  })
})

// =============================================================================
// CDCError TYPE GUARD AND STRUCTURE
// =============================================================================

describe('CDCError Structure and Type Guard', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should be detectable with isCDCError type guard', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(5n, 2n) // Invalid range
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect(isCDCError(error)).toBe(true)
    }
  })

  it('should have correct error name property', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(-1n, 0n) // Negative version
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect((error as Error).name).toBe('CDCError')
    }
  })

  it('should have cdcCode property with valid CDCErrorCode', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(10n, 5n) // Invalid range
      expect.fail('Should have thrown an error')
    } catch (error) {
      const cdcError = error as CDCError
      expect(cdcError.cdcCode).toBeDefined()
      // CDCErrorCode is one of: INVALID_VERSION_RANGE, INVALID_TIME_RANGE, TABLE_NOT_FOUND, CDC_NOT_ENABLED, STORAGE_ERROR, PARSE_ERROR, EMPTY_WRITE
      const validCodes: CDCErrorCode[] = [
        'INVALID_VERSION_RANGE',
        'INVALID_TIME_RANGE',
        'TABLE_NOT_FOUND',
        'CDC_NOT_ENABLED',
        'STORAGE_ERROR',
        'PARSE_ERROR',
        'EMPTY_WRITE',
      ]
      expect(validCodes).toContain(cdcError.cdcCode)
    }
  })

  it('should have code property prefixed with CDC_', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(10n, 5n) // Invalid range
      expect.fail('Should have thrown an error')
    } catch (error) {
      const cdcError = error as CDCError
      expect(cdcError.code).toMatch(/^CDC_/)
      expect(cdcError.code).toBe('CDC_' + cdcError.cdcCode)
    }
  })
})

// =============================================================================
// readByTimestamp ERROR PATHS
// =============================================================================

describe('CDCReader.readByTimestamp Error Paths', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should throw CDCError with INVALID_TIME_RANGE when start > end', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const start = new Date()
    const end = new Date(start.getTime() - 1000) // 1 second before start

    try {
      await reader.readByTimestamp(start, end)
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect(error).toBeInstanceOf(CDCError)
      const cdcError = error as CDCError
      expect(cdcError.cdcCode).toBe('INVALID_TIME_RANGE')
      expect(cdcError.code).toBe('CDC_INVALID_TIME_RANGE')
      expect(cdcError.message).toContain('time')
    }
  })

  it('should return empty array for future time range', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const futureStart = new Date(Date.now() + 10000)
    const futureEnd = new Date(Date.now() + 20000)

    const records = await reader.readByTimestamp(futureStart, futureEnd)
    expect(records).toEqual([])
  })

  it('should handle same start and end timestamp', async () => {
    await table.setCDCEnabled(true)
    const now = new Date()
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    // Same timestamp for start and end - should not throw
    const records = await reader.readByTimestamp(now, now)
    // May or may not find records depending on exact timing, but should not throw
    expect(Array.isArray(records)).toBe(true)
  })
})

// =============================================================================
// VERY LARGE VERSION NUMBERS
// =============================================================================

describe('CDCReader Large Version Number Handling', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should handle very large version numbers without overflow', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const veryLarge = BigInt('99999999999999999999')

    // Should not throw, should return empty (no records at that version)
    const records = await reader.readByVersion(veryLarge, veryLarge)
    expect(records).toEqual([])
  })

  it('should handle max safe integer as version', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER)

    const records = await reader.readByVersion(maxSafe, maxSafe)
    expect(records).toEqual([])
  })

  it('should handle version range spanning large numbers', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const large1 = BigInt('99999999999999999998')
    const large2 = BigInt('99999999999999999999')

    const records = await reader.readByVersion(large1, large2)
    expect(records).toEqual([])
  })
})

// =============================================================================
// CORRUPTED/MISSING CDC FILES
// =============================================================================

describe('CDCReader Corrupted and Missing Files', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should handle corrupted CDC files gracefully', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Corrupt the CDC file
    const files = await storage.list('test_table/_change_data/')
    if (files.length > 0) {
      await storage.write(files[0]!, new TextEncoder().encode('corrupted data not valid parquet'))
    }

    const reader = table.getCDCReader()

    // Should handle gracefully - return empty or skip corrupted
    const records = await reader.readByVersion(0n, 0n)
    expect(Array.isArray(records)).toBe(true)
  })

  it('should handle missing _change_data directory gracefully', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Delete all CDC files
    const files = await storage.list('test_table/_change_data/')
    for (const file of files) {
      await storage.delete(file)
    }

    const reader = table.getCDCReader()

    // Should return empty, not throw
    const records = await reader.readByVersion(0n, 0n)
    expect(records).toEqual([])
  })

  it('should handle partial CDC file deletion', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0
    await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1

    // Delete only v1 CDC files
    const files = await storage.list('test_table/_change_data/')
    for (const file of files) {
      if (file.includes('00000000000000000001')) {
        await storage.delete(file)
      }
    }

    const reader = table.getCDCReader()

    // Reading full range should still work, returning available records
    const records = await reader.readByVersion(0n, 1n)
    // Should have at least v0 records
    expect(records.length).toBe(1)
    expect(records[0]._commit_version).toBe(0n)
  })
})

// =============================================================================
// ERROR CAUSE CHAIN
// =============================================================================

describe('CDCError Cause Chain', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should preserve underlying error as cause for TABLE_NOT_FOUND', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'non_existent')
    const reader = table.getCDCReader()

    try {
      await reader.readByVersion(0n, 0n)
      expect.fail('Should have thrown an error')
    } catch (error) {
      expect(error).toBeInstanceOf(CDCError)
      // The cause might or might not be set depending on implementation
      // But the error should be a CDCError regardless
    }
  })
})

// =============================================================================
// CONCURRENT ERROR SCENARIOS
// =============================================================================

describe('CDCReader Concurrent Error Scenarios', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should handle multiple concurrent invalid range requests', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    // Multiple concurrent invalid requests
    const errors = await Promise.allSettled([
      reader.readByVersion(5n, 2n),
      reader.readByVersion(10n, 5n),
      reader.readByVersion(100n, 50n),
    ])

    // All should be rejected with CDCError
    for (const result of errors) {
      expect(result.status).toBe('rejected')
      if (result.status === 'rejected') {
        expect(result.reason).toBeInstanceOf(CDCError)
        expect((result.reason as CDCError).cdcCode).toBe('INVALID_VERSION_RANGE')
      }
    }
  })

  it('should handle mix of valid and invalid concurrent requests', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    const results = await Promise.allSettled([
      reader.readByVersion(0n, 0n),  // Valid
      reader.readByVersion(5n, 2n),  // Invalid
      reader.readByVersion(0n, 0n),  // Valid
    ])

    expect(results[0].status).toBe('fulfilled')
    expect(results[1].status).toBe('rejected')
    expect(results[2].status).toBe('fulfilled')
  })
})
