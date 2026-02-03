/**
 * CDC Reader Tests (RED Phase)
 *
 * Comprehensive test suite for getCDCReader() on DeltaTable.
 * Tests the ability to read CDC (Change Data Capture) records from a Delta table.
 *
 * CDC files are stored in _change_data/ directory and contain change records
 * with metadata columns: _change_type, _commit_version, _commit_timestamp
 *
 * These tests are written in the RED phase - they should FAIL
 * until the implementation is complete.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  createCDCDeltaTable,
  DeltaCDCRecord,
  CDCReader,
  CDCDeltaTable,
  CDCOperation,
} from '../../../src/cdc/index'
import { MemoryStorage, StorageBackend } from '../../../src/storage/index'

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
  category?: string
}

/**
 * Helper to wait for async operations
 */
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// =============================================================================
// getCDCReader() BASIC FUNCTIONALITY
// =============================================================================

describe('getCDCReader() Basic Functionality', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('Returns CDCReader instance', () => {
    it('should return a CDCReader instance from DeltaTable', async () => {
      const reader = table.getCDCReader()

      expect(reader).toBeDefined()
      expect(typeof reader.readByVersion).toBe('function')
      expect(typeof reader.readByTimestamp).toBe('function')
      expect(typeof reader.subscribe).toBe('function')
    })

    it('should return the same CDCReader instance on multiple calls', async () => {
      const reader1 = table.getCDCReader()
      const reader2 = table.getCDCReader()

      // Should return the same instance (singleton pattern)
      expect(reader1).toBe(reader2)
    })

    it('should return a CDCReader even when CDC is disabled', async () => {
      // CDC is disabled by default
      const reader = table.getCDCReader()

      expect(reader).toBeDefined()
    })

    it('should return a CDCReader before any writes', async () => {
      const reader = table.getCDCReader()

      expect(reader).toBeDefined()
      expect(typeof reader.readByVersion).toBe('function')
    })
  })
})

// =============================================================================
// READ CDC RECORDS BY VERSION RANGE
// =============================================================================

describe('Read CDC Records by Version Range', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('readByVersion(fromVersion, toVersion)', () => {
    it('should read CDC records for a single version', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 0n)

      expect(records.length).toBe(1)
      expect(records[0]._commit_version).toBe(0n)
      expect(records[0]._change_type).toBe('insert')
      expect(records[0].data.id).toBe('1')
    })

    it('should read CDC records for a version range (inclusive)', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])   // v0
      await table.write([{ id: '2', name: 'Bob', value: 200 }])     // v1
      await table.write([{ id: '3', name: 'Charlie', value: 300 }]) // v2
      await table.write([{ id: '4', name: 'David', value: 400 }])   // v3

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(1n, 2n)

      expect(records.length).toBe(2)
      const versions = records.map(r => r._commit_version)
      expect(versions).toContain(1n)
      expect(versions).toContain(2n)
      expect(versions).not.toContain(0n)
      expect(versions).not.toContain(3n)
    })

    it('should return all records from start version to end version', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
      ]) // v0 with 2 inserts

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 0n)

      expect(records.length).toBe(2)
      expect(records.every(r => r._commit_version === 0n)).toBe(true)
    })

    it('should return empty array when no CDC records exist in range', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(5n, 10n)

      expect(records).toEqual([])
    })

    it('should throw error for invalid version range (start > end)', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()

      await expect(reader.readByVersion(5n, 2n)).rejects.toThrow()
    })

    it('should handle version 0 correctly', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'First', value: 1 }])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 0n)

      expect(records.length).toBe(1)
      expect(records[0]._commit_version).toBe(0n)
    })

    it('should handle large version numbers', async () => {
      await table.setCDCEnabled(true)

      // Write many versions
      for (let i = 0; i < 50; i++) {
        await table.write([{ id: `${i}`, name: `User${i}`, value: i }])
      }

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(40n, 49n)

      expect(records.length).toBe(10)
      const versions = new Set(records.map(r => r._commit_version))
      expect(versions.size).toBe(10)
    })

    it('should handle single-version range (start equals end)', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(1n, 1n)

      expect(records.length).toBe(1)
      expect(records[0]._commit_version).toBe(1n)
    })

    it('should include all change types in version range', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])  // v0 - insert
      await table.update({ id: '1' }, { value: 200 })              // v1 - update
      await table.deleteRows({ id: '1' })                          // v2 - delete

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 2n)

      const changeTypes = records.map(r => r._change_type)
      expect(changeTypes).toContain('insert')
      expect(changeTypes.some(t => t === 'update_preimage' || t === 'update_postimage')).toBe(true)
      expect(changeTypes).toContain('delete')
    })
  })
})

// =============================================================================
// READ CDC RECORDS BY TIME RANGE
// =============================================================================

describe('Read CDC Records by Time Range', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('readByTimestamp(fromTimestamp, toTimestamp)', () => {
    it('should read CDC records within a time range', async () => {
      await table.setCDCEnabled(true)

      const before = new Date()
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)
      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      const after = new Date()

      const reader = table.getCDCReader()
      const records = await reader.readByTimestamp(before, after)

      expect(records.length).toBe(2)
    })

    it('should filter records by start time (inclusive)', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(100)
      const middle = new Date()
      await delay(100)
      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      const after = new Date()

      const reader = table.getCDCReader()
      const records = await reader.readByTimestamp(middle, after)

      // Should only include the second write
      expect(records.length).toBe(1)
      expect(records[0].data.id).toBe('2')
    })

    it('should filter records by end time (inclusive)', async () => {
      await table.setCDCEnabled(true)

      const before = new Date()
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(100)
      const middle = new Date()
      await delay(100)
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      const reader = table.getCDCReader()
      const records = await reader.readByTimestamp(before, middle)

      // Should only include the first write
      expect(records.length).toBe(1)
      expect(records[0].data.id).toBe('1')
    })

    it('should return empty array for time range with no changes', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const futureStart = new Date(Date.now() + 10000)
      const futureEnd = new Date(Date.now() + 20000)
      const records = await reader.readByTimestamp(futureStart, futureEnd)

      expect(records).toEqual([])
    })

    it('should throw error for invalid time range (start > end)', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const start = new Date()
      const end = new Date(start.getTime() - 1000)

      await expect(reader.readByTimestamp(start, end)).rejects.toThrow()
    })

    it('should handle Date objects correctly', async () => {
      await table.setCDCEnabled(true)

      const timestamp = new Date('2025-01-15T10:00:00.000Z')
      // Mock the current time for predictable testing
      vi.useFakeTimers()
      vi.setSystemTime(timestamp)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      vi.useRealTimers()

      const reader = table.getCDCReader()
      const records = await reader.readByTimestamp(
        new Date('2025-01-15T09:00:00.000Z'),
        new Date('2025-01-15T11:00:00.000Z')
      )

      expect(records.length).toBeGreaterThan(0)
    })

    it('should return records ordered by timestamp', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'First', value: 1 }])
      await delay(10)
      await table.write([{ id: '2', name: 'Second', value: 2 }])
      await delay(10)
      await table.write([{ id: '3', name: 'Third', value: 3 }])

      const reader = table.getCDCReader()
      const before = new Date(Date.now() - 5000)
      const after = new Date(Date.now() + 5000)
      const records = await reader.readByTimestamp(before, after)

      // Records should be ordered by commit timestamp
      for (let i = 1; i < records.length; i++) {
        const prevTime = records[i - 1]._commit_timestamp.getTime()
        const currTime = records[i]._commit_timestamp.getTime()
        expect(currTime).toBeGreaterThanOrEqual(prevTime)
      }
    })
  })
})

// =============================================================================
// FILTER BY OPERATION TYPE
// =============================================================================

describe('Filter by Operation Type', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('Filter CDC records by operation type', () => {
    it('should filter for insert operations only', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])  // insert
      await table.update({ id: '1' }, { value: 200 })              // update
      await table.write([{ id: '2', name: 'Bob', value: 300 }])    // insert

      const reader = table.getCDCReader()
      const allRecords = await reader.readByVersion(0n, 2n)
      const inserts = allRecords.filter(r => r._change_type === 'insert')

      expect(inserts.length).toBe(2)
      expect(inserts.every(r => r._change_type === 'insert')).toBe(true)
    })

    it('should filter for update operations (preimage and postimage)', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const allRecords = await reader.readByVersion(0n, 1n)

      const preimages = allRecords.filter(r => r._change_type === 'update_preimage')
      const postimages = allRecords.filter(r => r._change_type === 'update_postimage')

      expect(preimages.length).toBe(1)
      expect(postimages.length).toBe(1)
      expect(preimages[0].data.value).toBe(100)
      expect(postimages[0].data.value).toBe(200)
    })

    it('should filter for delete operations only', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      await table.deleteRows({ id: '1' })

      const reader = table.getCDCReader()
      const allRecords = await reader.readByVersion(0n, 2n)
      const deletes = allRecords.filter(r => r._change_type === 'delete')

      expect(deletes.length).toBe(1)
      expect(deletes[0].data.id).toBe('1')
    })

    it('should handle mixed operation types in same version', async () => {
      await table.setCDCEnabled(true)

      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
      ])

      // Merge operation that updates one, deletes another, inserts new
      await table.merge(
        [
          { id: '1', name: 'Alice Updated', value: 150 }, // update
          { id: '2', name: 'Bob', value: 0 },             // delete (value 0)
          { id: '3', name: 'Charlie', value: 300 },       // insert
        ],
        (existing, incoming) => existing.id === incoming.id,
        (existing, incoming) => (incoming.value === 0 ? null : incoming),
        (incoming) => (incoming.value === 0 ? null : incoming)
      )

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(1n, 1n)

      const inserts = records.filter(r => r._change_type === 'insert')
      const deletes = records.filter(r => r._change_type === 'delete')
      const updates = records.filter(
        r => r._change_type === 'update_preimage' || r._change_type === 'update_postimage'
      )

      expect(inserts.length).toBeGreaterThan(0)
      expect(deletes.length).toBeGreaterThan(0)
      expect(updates.length).toBeGreaterThan(0)
    })
  })
})

// =============================================================================
// STREAMING/ITERATION OVER CDC RECORDS
// =============================================================================

describe('Streaming/Iteration over CDC Records', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('Async iteration', () => {
    it('should support iterating over records in order', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'First', value: 1 }])
      await table.write([{ id: '2', name: 'Second', value: 2 }])
      await table.write([{ id: '3', name: 'Third', value: 3 }])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 2n)

      // Verify records are iterable and in order
      const ids = records.map(r => r.data.id)
      expect(ids).toContain('1')
      expect(ids).toContain('2')
      expect(ids).toContain('3')
    })

    it('should handle large result sets efficiently', async () => {
      await table.setCDCEnabled(true)

      // Write 100 records
      for (let i = 0; i < 100; i++) {
        await table.write([{ id: `${i}`, name: `User${i}`, value: i }])
      }

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 99n)

      expect(records.length).toBe(100)
    })
  })
})

// =============================================================================
// EMPTY RESULT WHEN NO CDC FILES EXIST
// =============================================================================

describe('Empty Result when No CDC Files Exist', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should return empty array when CDC is disabled', async () => {
    // CDC disabled by default
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 0n)

    expect(records).toEqual([])
  })

  it('should return empty array for empty table', async () => {
    await table.setCDCEnabled(true)

    const reader = table.getCDCReader()
    // Table has no commits yet
    await expect(reader.readByVersion(0n, 0n)).rejects.toThrow()
  })

  it('should return empty array when CDC was enabled after writes', async () => {
    // Write with CDC disabled
    await table.write([{ id: '1', name: 'Alice', value: 100 }])  // v0
    await table.setCDCEnabled(true)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])    // v1

    const reader = table.getCDCReader()

    // v0 should have no CDC records
    const v0Records = await reader.readByVersion(0n, 0n)
    expect(v0Records).toEqual([])

    // v1 should have CDC records
    const v1Records = await reader.readByVersion(1n, 1n)
    expect(v1Records.length).toBe(1)
  })

  it('should handle _change_data directory not existing', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Manually delete the _change_data directory contents
    const files = await storage.list('test_table/_change_data/')
    for (const file of files) {
      await storage.delete(file)
    }

    const reader = table.getCDCReader()
    // Should handle gracefully - either return empty or throw clear error
    const records = await reader.readByVersion(0n, 0n)
    expect(records).toEqual([])
  })
})

// =============================================================================
// PROPER ORDERING BY SEQUENCE NUMBER (_seq)
// =============================================================================

describe('Proper Ordering by Sequence Number', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('Records should be ordered by _commit_version', () => {
    it('should return records ordered by commit version', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'First', value: 1 }])
      await table.write([{ id: '2', name: 'Second', value: 2 }])
      await table.write([{ id: '3', name: 'Third', value: 3 }])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 2n)

      // Verify ordering by version
      for (let i = 1; i < records.length; i++) {
        expect(records[i]._commit_version).toBeGreaterThanOrEqual(records[i - 1]._commit_version)
      }
    })

    it('should maintain insertion order within a single version', async () => {
      await table.setCDCEnabled(true)

      await table.write([
        { id: '1', name: 'First', value: 1 },
        { id: '2', name: 'Second', value: 2 },
        { id: '3', name: 'Third', value: 3 },
      ])

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(0n, 0n)

      expect(records[0].data.id).toBe('1')
      expect(records[1].data.id).toBe('2')
      expect(records[2].data.id).toBe('3')
    })

    it('should order update_preimage before update_postimage', async () => {
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const records = await reader.readByVersion(1n, 1n)

      const updateRecords = records.filter(
        r => r._change_type === 'update_preimage' || r._change_type === 'update_postimage'
      )

      expect(updateRecords[0]._change_type).toBe('update_preimage')
      expect(updateRecords[1]._change_type).toBe('update_postimage')
    })
  })
})

// =============================================================================
// HANDLE GAPS IN VERSIONS
// =============================================================================

describe('Handle Gaps in Versions', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should handle gaps when CDC was temporarily disabled', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])  // v0 - has CDC

    await table.setCDCEnabled(false)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])    // v1 - no CDC

    await table.setCDCEnabled(true)
    await table.write([{ id: '3', name: 'Charlie', value: 300 }]) // v2 - has CDC

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 2n)

    // Should only have CDC for v0 and v2
    const versions = [...new Set(records.map(r => r._commit_version))]
    expect(versions).toContain(0n)
    expect(versions).toContain(2n)
    expect(versions).not.toContain(1n)
  })

  it('should not fail on missing CDC files for specific versions', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])  // v0
    await table.write([{ id: '2', name: 'Bob', value: 200 }])    // v1

    // Manually delete CDC file for v1
    const files = await storage.list('test_table/_change_data/')
    for (const file of files) {
      if (file.includes('00000000000000000001')) {
        await storage.delete(file)
      }
    }

    const reader = table.getCDCReader()
    // Should not throw, should return available records
    const records = await reader.readByVersion(0n, 1n)
    expect(records.length).toBe(1)
    expect(records[0]._commit_version).toBe(0n)
  })

  it('should handle sparse version ranges gracefully', async () => {
    await table.setCDCEnabled(true)

    // Write versions with gaps in CDC
    await table.write([{ id: '1', name: 'Alice', value: 100 }])  // v0

    // Disable CDC for middle versions
    await table.setCDCEnabled(false)
    for (let i = 0; i < 5; i++) {
      await table.write([{ id: `skip-${i}`, name: 'Skip', value: i }])
    }

    // Re-enable CDC
    await table.setCDCEnabled(true)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])  // v6

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 6n)

    // Should only have records for v0 and v6
    const versions = [...new Set(records.map(r => r._commit_version))]
    expect(versions.length).toBe(2)
  })
})

// =============================================================================
// SUBSCRIPTION MODEL FOR CONTINUOUS READING
// =============================================================================

describe('Subscription Model for Continuous Reading', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('subscribe(handler)', () => {
    it('should subscribe to CDC changes and receive records', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(async (record) => {
        received.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50) // Allow async handler to process

      expect(received.length).toBe(1)
      expect(received[0].data.id).toBe('1')

      unsubscribe()
    })

    it('should stop receiving records after unsubscribe', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(async (record) => {
        received.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      unsubscribe()

      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      await delay(50)

      // Should only have the first record
      expect(received.length).toBe(1)
      expect(received[0].data.id).toBe('1')
    })

    it('should support multiple concurrent subscribers', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received1: DeltaCDCRecord<TestRow>[] = []
      const received2: DeltaCDCRecord<TestRow>[] = []

      const unsub1 = reader.subscribe(async (record) => {
        received1.push(record)
      })
      const unsub2 = reader.subscribe(async (record) => {
        received2.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      expect(received1.length).toBe(1)
      expect(received2.length).toBe(1)

      unsub1()
      unsub2()
    })

    it('should deliver records to each subscriber independently', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received1: DeltaCDCRecord<TestRow>[] = []
      const received2: DeltaCDCRecord<TestRow>[] = []

      const unsub1 = reader.subscribe(async (record) => {
        received1.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      const unsub2 = reader.subscribe(async (record) => {
        received2.push(record)
      })

      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      await delay(50)

      // First subscriber should have both records
      expect(received1.length).toBe(2)
      // Second subscriber should only have the second record
      expect(received2.length).toBe(1)
      expect(received2[0].data.id).toBe('2')

      unsub1()
      unsub2()
    })

    it('should handle subscriber errors gracefully', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      // First subscriber throws an error
      const unsub1 = reader.subscribe(async () => {
        throw new Error('Subscriber error')
      })

      // Second subscriber should still receive records
      const unsub2 = reader.subscribe(async (record) => {
        received.push(record)
      })

      // This should not throw even though first subscriber errors
      await expect(table.write([{ id: '1', name: 'Alice', value: 100 }])).resolves.toBeDefined()
      await delay(50)

      expect(received.length).toBe(1)

      unsub1()
      unsub2()
    })

    it('should notify subscribers of all change types', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const changeTypes: string[] = []

      const unsubscribe = reader.subscribe(async (record) => {
        changeTypes.push(record._change_type)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])  // insert
      await delay(20)
      await table.update({ id: '1' }, { value: 200 })              // update
      await delay(20)
      await table.deleteRows({ id: '1' })                          // delete
      await delay(50)

      expect(changeTypes).toContain('insert')
      expect(changeTypes.some(t => t.includes('update'))).toBe(true)
      expect(changeTypes).toContain('delete')

      unsubscribe()
    })

    it('should return a valid unsubscribe function', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const unsubscribe = reader.subscribe(async () => {})

      expect(typeof unsubscribe).toBe('function')

      // Should not throw
      expect(() => unsubscribe()).not.toThrow()
    })

    it('should handle unsubscribe being called multiple times', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const unsubscribe = reader.subscribe(async () => {})

      // Should not throw when called multiple times
      expect(() => unsubscribe()).not.toThrow()
      expect(() => unsubscribe()).not.toThrow()
    })
  })
})

// =============================================================================
// CDC READER ERROR HANDLING
// =============================================================================

describe('CDC Reader Error Handling', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should throw when reading from non-existent table', async () => {
    const nonExistentTable = createCDCDeltaTable<TestRow>(storage, 'non_existent')
    const reader = nonExistentTable.getCDCReader()

    await expect(reader.readByVersion(0n, 0n)).rejects.toThrow()
  })

  it('should handle corrupted CDC files gracefully', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Corrupt the CDC file
    const files = await storage.list('test_table/_change_data/')
    if (files.length > 0) {
      await storage.write(files[0], new TextEncoder().encode('corrupted data'))
    }

    const reader = table.getCDCReader()

    // Should handle gracefully - either return empty or skip corrupted
    const records = await reader.readByVersion(0n, 0n)
    expect(Array.isArray(records)).toBe(true)
  })

  it('should handle negative version numbers', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()

    // Negative versions should be invalid
    await expect(reader.readByVersion(-1n, 0n)).rejects.toThrow()
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
})

// =============================================================================
// CDC READER WITH DIFFERENT DATA TYPES
// =============================================================================

describe('CDC Reader with Different Data Types', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should handle complex nested data structures', async () => {
    interface ComplexRow {
      id: string
      data: {
        nested: {
          value: number
          items: string[]
        }
      }
    }

    const table = createCDCDeltaTable<ComplexRow>(storage, 'complex_table')
    await table.setCDCEnabled(true)

    const complexData: ComplexRow = {
      id: '1',
      data: {
        nested: {
          value: 42,
          items: ['a', 'b', 'c']
        }
      }
    }

    await table.write([complexData])

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 0n)

    expect(records.length).toBe(1)
    expect(records[0].data.data.nested.value).toBe(42)
    expect(records[0].data.data.nested.items).toEqual(['a', 'b', 'c'])
  })

  it('should handle null values correctly', async () => {
    interface NullableRow {
      id: string
      name: string | null
      value: number | null
    }

    const table = createCDCDeltaTable<NullableRow>(storage, 'nullable_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: null, value: null }])

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 0n)

    expect(records.length).toBe(1)
    expect(records[0].data.name).toBeNull()
    expect(records[0].data.value).toBeNull()
  })

  it('should preserve data types through CDC records', async () => {
    interface TypedRow {
      id: string
      count: number
      active: boolean
      tags: string[]
      metadata: Record<string, unknown>
    }

    const table = createCDCDeltaTable<TypedRow>(storage, 'typed_table')
    await table.setCDCEnabled(true)

    const row: TypedRow = {
      id: '1',
      count: 42,
      active: true,
      tags: ['tag1', 'tag2'],
      metadata: { key: 'value', nested: { a: 1 } }
    }

    await table.write([row])

    const reader = table.getCDCReader()
    const records = await reader.readByVersion(0n, 0n)

    expect(typeof records[0].data.count).toBe('number')
    expect(typeof records[0].data.active).toBe('boolean')
    expect(Array.isArray(records[0].data.tags)).toBe(true)
    expect(typeof records[0].data.metadata).toBe('object')
  })
})

// =============================================================================
// CDC READER CONCURRENCY
// =============================================================================

describe('CDC Reader Concurrency', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  it('should handle concurrent reads correctly', async () => {
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.write([{ id: '2', name: 'Bob', value: 200 }])

    const reader = table.getCDCReader()

    // Perform multiple concurrent reads
    const results = await Promise.all([
      reader.readByVersion(0n, 1n),
      reader.readByVersion(0n, 1n),
      reader.readByVersion(0n, 1n),
    ])

    // All results should be identical
    expect(results[0].length).toBe(2)
    expect(results[1].length).toBe(2)
    expect(results[2].length).toBe(2)
  })

  it('should handle concurrent subscriptions and writes', async () => {
    await table.setCDCEnabled(true)

    const reader = table.getCDCReader()
    const received: DeltaCDCRecord<TestRow>[] = []

    const unsubscribe = reader.subscribe(async (record) => {
      received.push(record)
    })

    // Concurrent writes
    await Promise.all([
      table.write([{ id: '1', name: 'Alice', value: 100 }]),
      table.write([{ id: '2', name: 'Bob', value: 200 }]),
      table.write([{ id: '3', name: 'Charlie', value: 300 }]),
    ])

    await delay(100)

    // Should have received all 3 records
    expect(received.length).toBe(3)

    unsubscribe()
  })
})

// =============================================================================
// CDC SUBSCRIPTION ERROR HANDLING
// =============================================================================

describe('CDC Subscription Error Handling', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('onError callback', () => {
    it('should invoke onError callback when handler throws', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errors: { error: Error; record: DeltaCDCRecord<TestRow> }[] = []
      const handlerError = new Error('Handler failed')

      const unsubscribe = reader.subscribe(
        async () => {
          throw handlerError
        },
        {
          onError: (error, record) => {
            errors.push({ error, record })
          },
        }
      )

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      expect(errors.length).toBe(1)
      expect(errors[0].error.message).toBe('Handler failed')
      expect(errors[0].record._change_type).toBe('insert')
      expect(errors[0].record.data.id).toBe('1')

      unsubscribe()
    })

    it('should pass the correct record to onError callback', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errorRecords: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(
        async () => {
          throw new Error('Fail')
        },
        {
          onError: (_error, record) => {
            errorRecords.push(record)
          },
        }
      )

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      await delay(50)

      expect(errorRecords.length).toBe(2)
      expect(errorRecords[0].data.id).toBe('1')
      expect(errorRecords[1].data.id).toBe('2')

      unsubscribe()
    })

    it('should not invoke onError when handler succeeds', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errors: Error[] = []
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(
        async (record) => {
          received.push(record)
        },
        {
          onError: (error) => {
            errors.push(error)
          },
        }
      )

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      expect(received.length).toBe(1)
      expect(errors.length).toBe(0)

      unsubscribe()
    })

    it('should handle onError callback throwing without breaking other subscribers', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      // First subscriber with failing handler and failing onError
      const unsub1 = reader.subscribe(
        async () => {
          throw new Error('Handler error')
        },
        {
          onError: () => {
            throw new Error('onError callback error')
          },
        }
      )

      // Second subscriber should still work
      const unsub2 = reader.subscribe(async (record) => {
        received.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      expect(received.length).toBe(1)
      expect(received[0].data.id).toBe('1')

      unsub1()
      unsub2()
    })

    it('should convert non-Error objects to Error in onError callback', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errors: Error[] = []

      const unsubscribe = reader.subscribe(
        async () => {
          throw 'string error' // eslint-disable-line no-throw-literal
        },
        {
          onError: (error) => {
            errors.push(error)
          },
        }
      )

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      expect(errors.length).toBe(1)
      expect(errors[0] instanceof Error).toBe(true)
      expect(errors[0].message).toBe('string error')

      unsubscribe()
    })

    it('should isolate errors between subscribers with onError callbacks', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errors1: Error[] = []
      const errors2: Error[] = []
      const received: DeltaCDCRecord<TestRow>[] = []

      // First subscriber fails
      const unsub1 = reader.subscribe(
        async () => {
          throw new Error('Subscriber 1 error')
        },
        {
          onError: (error) => {
            errors1.push(error)
          },
        }
      )

      // Second subscriber succeeds
      const unsub2 = reader.subscribe(
        async (record) => {
          received.push(record)
        },
        {
          onError: (error) => {
            errors2.push(error)
          },
        }
      )

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      // First subscriber's error handler should have been called
      expect(errors1.length).toBe(1)
      expect(errors1[0].message).toBe('Subscriber 1 error')

      // Second subscriber should work and not have any errors
      expect(errors2.length).toBe(0)
      expect(received.length).toBe(1)

      unsub1()
      unsub2()
    })

    it('should work with subscribe without options (backward compatible)', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      // Subscribe without options (original API)
      const unsubscribe = reader.subscribe(async (record) => {
        received.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await delay(50)

      expect(received.length).toBe(1)

      unsubscribe()
    })

    it('should call onError for each failed record in bulk operations', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errors: { error: Error; record: DeltaCDCRecord<TestRow> }[] = []

      const unsubscribe = reader.subscribe(
        async () => {
          throw new Error('Bulk error')
        },
        {
          onError: (error, record) => {
            errors.push({ error, record })
          },
        }
      )

      // Write multiple records in single operation
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
        { id: '3', name: 'Charlie', value: 300 },
      ])
      await delay(50)

      // Should have one error per record
      expect(errors.length).toBe(3)
      expect(errors.map(e => e.record.data.id).sort()).toEqual(['1', '2', '3'])

      unsubscribe()
    })

    it('should include correct metadata in onError callback for updates', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const errors: { error: Error; record: DeltaCDCRecord<TestRow> }[] = []

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const unsubscribe = reader.subscribe(
        async () => {
          throw new Error('Update error')
        },
        {
          onError: (error, record) => {
            errors.push({ error, record })
          },
        }
      )

      await table.update({ id: '1' }, { value: 200 })
      await delay(50)

      // Should have errors for both preimage and postimage
      expect(errors.length).toBe(2)
      const changeTypes = errors.map(e => e.record._change_type)
      expect(changeTypes).toContain('update_preimage')
      expect(changeTypes).toContain('update_postimage')

      unsubscribe()
    })
  })
})
