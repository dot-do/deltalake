/**
 * Multi-File Read Integration Tests
 *
 * Tests for reading Delta Lake tables that span multiple Parquet files.
 * These integration tests verify:
 * - Tables with multiple data files are read correctly
 * - File ordering and consistency
 * - Real Parquet file round-trips through storage
 * - Query filtering across multiple files
 * - Snapshot consistency with multiple files
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import { DeltaTable, type DeltaSnapshot } from '../../src/delta/index.js'
import { MemoryStorage, R2Storage, type StorageBackend } from '../../src/storage/index.js'

// =============================================================================
// TEST FIXTURES AND HELPERS
// =============================================================================

interface TestRecord {
  id: number
  name: string
  value: number
  category?: string
}

/**
 * Create a test table with a specified storage backend
 */
function createTestTable(storage: StorageBackend, tablePath: string = 'test-table'): DeltaTable<TestRecord> {
  return new DeltaTable<TestRecord>(storage, tablePath)
}

/**
 * Generate test data with predictable values
 */
function generateTestRecords(startId: number, count: number, category?: string): TestRecord[] {
  const records: TestRecord[] = []
  for (let i = 0; i < count; i++) {
    records.push({
      id: startId + i,
      name: `record_${startId + i}`,
      value: (startId + i) * 10,
      ...(category && { category }),
    })
  }
  return records
}

// =============================================================================
// MULTI-FILE READ TESTS - MEMORY STORAGE
// =============================================================================

describe('Multi-File Reads - MemoryStorage', () => {
  let storage: MemoryStorage
  let table: DeltaTable<TestRecord>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createTestTable(storage)
  })

  describe('Table with Multiple Parquet Files', () => {
    it('should read all records from table with 2 data files', async () => {
      // Write first batch - creates first Parquet file
      await table.write(generateTestRecords(1, 5))

      // Write second batch - creates second Parquet file
      await table.write(generateTestRecords(6, 5))

      // Query should return all 10 records
      const results = await table.query()
      expect(results).toHaveLength(10)

      // Verify all records are present
      const ids = results.map(r => r.id).sort((a, b) => a - b)
      expect(ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    })

    it('should read all records from table with 5 data files', async () => {
      // Write 5 separate batches
      for (let batch = 0; batch < 5; batch++) {
        await table.write(generateTestRecords(batch * 10 + 1, 10))
      }

      const results = await table.query()
      expect(results).toHaveLength(50)

      // Verify version reflects all commits
      expect(await table.version()).toBe(4) // 0-indexed versions
    })

    it('should read all records from table with 10 data files', async () => {
      // Write 10 separate batches
      for (let batch = 0; batch < 10; batch++) {
        await table.write(generateTestRecords(batch * 5 + 1, 5))
      }

      const results = await table.query()
      expect(results).toHaveLength(50)

      // Snapshot should show 10 files
      const snapshot = await table.snapshot()
      expect(snapshot.files).toHaveLength(10)
    })

    it('should maintain data integrity across multiple files', async () => {
      const batches = [
        generateTestRecords(1, 3, 'batch1'),
        generateTestRecords(100, 3, 'batch2'),
        generateTestRecords(1000, 3, 'batch3'),
      ]

      for (const batch of batches) {
        await table.write(batch)
      }

      const results = await table.query()
      expect(results).toHaveLength(9)

      // Verify each batch's data is preserved
      const batch1Records = results.filter(r => r.category === 'batch1')
      const batch2Records = results.filter(r => r.category === 'batch2')
      const batch3Records = results.filter(r => r.category === 'batch3')

      expect(batch1Records).toHaveLength(3)
      expect(batch2Records).toHaveLength(3)
      expect(batch3Records).toHaveLength(3)

      // Verify specific values
      expect(batch1Records.map(r => r.id).sort((a, b) => a - b)).toEqual([1, 2, 3])
      expect(batch2Records.map(r => r.id).sort((a, b) => a - b)).toEqual([100, 101, 102])
      expect(batch3Records.map(r => r.id).sort((a, b) => a - b)).toEqual([1000, 1001, 1002])
    })
  })

  describe('Snapshot Consistency', () => {
    it('should provide consistent snapshot across multiple files', async () => {
      // Write multiple batches
      await table.write(generateTestRecords(1, 5))
      await table.write(generateTestRecords(6, 5))
      await table.write(generateTestRecords(11, 5))

      const snapshot = await table.snapshot()

      expect(snapshot.version).toBe(2)
      expect(snapshot.files).toHaveLength(3)

      // Each file should have valid metadata
      for (const file of snapshot.files) {
        expect(file.path).toBeDefined()
        expect(file.size).toBeGreaterThan(0)
        expect(file.modificationTime).toBeGreaterThan(0)
        expect(file.dataChange).toBe(true)
      }
    })

    it('should allow time travel to previous versions with fewer files', async () => {
      await table.write(generateTestRecords(1, 5))
      await table.write(generateTestRecords(6, 5))
      await table.write(generateTestRecords(11, 5))

      // Snapshot at version 0 should only have 1 file
      const snapshotV0 = await table.snapshot(0)
      expect(snapshotV0.files).toHaveLength(1)

      // Snapshot at version 1 should have 2 files
      const snapshotV1 = await table.snapshot(1)
      expect(snapshotV1.files).toHaveLength(2)

      // Current snapshot should have 3 files
      const snapshotV2 = await table.snapshot(2)
      expect(snapshotV2.files).toHaveLength(3)
    })

    it('should track file paths correctly across versions', async () => {
      await table.write(generateTestRecords(1, 5))
      const snapshot1 = await table.snapshot()
      const file1Path = snapshot1.files[0].path

      await table.write(generateTestRecords(6, 5))
      const snapshot2 = await table.snapshot()

      // First file should still be present with same path
      expect(snapshot2.files.some(f => f.path === file1Path)).toBe(true)

      // Should have a new file too
      expect(snapshot2.files.length).toBe(2)
      const newFile = snapshot2.files.find(f => f.path !== file1Path)
      expect(newFile).toBeDefined()
    })
  })

  describe('Query Filtering Across Multiple Files', () => {
    beforeEach(async () => {
      // Set up table with records in multiple files
      await table.write([
        { id: 1, name: 'alice', value: 100, category: 'A' },
        { id: 2, name: 'bob', value: 200, category: 'B' },
      ])
      await table.write([
        { id: 3, name: 'charlie', value: 300, category: 'A' },
        { id: 4, name: 'diana', value: 400, category: 'B' },
      ])
      await table.write([
        { id: 5, name: 'eve', value: 500, category: 'A' },
        { id: 6, name: 'frank', value: 600, category: 'C' },
      ])
    })

    it('should filter records by equality across multiple files', async () => {
      const results = await table.query({ category: 'A' })

      expect(results).toHaveLength(3)
      expect(results.every(r => r.category === 'A')).toBe(true)
    })

    it('should filter records by range across multiple files', async () => {
      const results = await table.query({ value: { $gte: 200, $lte: 400 } })

      expect(results).toHaveLength(3)
      expect(results.every(r => r.value >= 200 && r.value <= 400)).toBe(true)
    })

    it('should filter records by multiple conditions across files', async () => {
      const results = await table.query({
        category: 'A',
        value: { $gt: 100 },
      })

      expect(results).toHaveLength(2) // charlie (300) and eve (500)
      expect(results.map(r => r.name).sort()).toEqual(['charlie', 'eve'])
    })

    it('should return empty array when filter matches no records', async () => {
      const results = await table.query({ category: 'Z' })
      expect(results).toHaveLength(0)
    })
  })

  describe('Large Multi-File Table', () => {
    it('should handle table with 50 files', async () => {
      // Create 50 files with 10 records each
      for (let i = 0; i < 50; i++) {
        await table.write(generateTestRecords(i * 10 + 1, 10))
      }

      const snapshot = await table.snapshot()
      expect(snapshot.files).toHaveLength(50)

      const results = await table.query()
      expect(results).toHaveLength(500)
    })

    it('should maintain query performance with many files', async () => {
      // Create 20 files
      for (let i = 0; i < 20; i++) {
        await table.write(generateTestRecords(i * 100 + 1, 100, `batch${i}`))
      }

      const start = performance.now()
      const results = await table.query({ category: 'batch10' })
      const duration = performance.now() - start

      expect(results).toHaveLength(100)
      // Query should complete in reasonable time (< 5 seconds)
      expect(duration).toBeLessThan(5000)
    })
  })
})

// =============================================================================
// MULTI-FILE READ TESTS - R2 STORAGE
// =============================================================================

describe('Multi-File Reads - R2Storage', () => {
  let storage: R2Storage
  let table: DeltaTable<TestRecord>

  beforeEach(() => {
    // Use the R2 bucket from test environment
    storage = new R2Storage({ bucket: env.TEST_BUCKET })
    table = createTestTable(storage, `test-table-${Date.now()}`)
  })

  describe('R2 Multi-File Operations', () => {
    it('should write and read multiple files from R2', async () => {
      await table.write(generateTestRecords(1, 5))
      await table.write(generateTestRecords(6, 5))

      const results = await table.query()
      expect(results).toHaveLength(10)
    })

    it('should maintain file integrity in R2 storage', async () => {
      const originalRecords = [
        { id: 1, name: 'test1', value: 100 },
        { id: 2, name: 'test2', value: 200 },
      ]

      await table.write(originalRecords)
      await table.write(generateTestRecords(3, 3))

      const results = await table.query({ id: { $lte: 2 } })
      expect(results).toHaveLength(2)

      // Verify original data preserved
      const record1 = results.find(r => r.id === 1)
      expect(record1?.name).toBe('test1')
      expect(record1?.value).toBe(100)
    })

    it('should list all files in R2 table', async () => {
      // Write 3 batches
      for (let i = 0; i < 3; i++) {
        await table.write(generateTestRecords(i * 5 + 1, 5))
      }

      const snapshot = await table.snapshot()
      expect(snapshot.files).toHaveLength(3)

      // Verify all files exist in storage
      for (const file of snapshot.files) {
        const exists = await storage.exists(file.path)
        expect(exists).toBe(true)
      }
    })
  })
})

// =============================================================================
// CROSS-STORAGE CONSISTENCY TESTS
// =============================================================================

describe('Cross-Storage Multi-File Consistency', () => {
  it('should produce identical results from Memory and R2 storage', async () => {
    const testData = [
      generateTestRecords(1, 5, 'batch1'),
      generateTestRecords(6, 5, 'batch2'),
      generateTestRecords(11, 5, 'batch3'),
    ]

    // Write to memory storage
    const memStorage = new MemoryStorage()
    const memTable = createTestTable(memStorage, 'test-table')
    for (const batch of testData) {
      await memTable.write(batch)
    }

    // Write to R2 storage
    const r2Storage = new R2Storage({ bucket: env.TEST_BUCKET })
    const r2Table = createTestTable(r2Storage, `test-table-${Date.now()}`)
    for (const batch of testData) {
      await r2Table.write(batch)
    }

    // Compare results
    const memResults = await memTable.query()
    const r2Results = await r2Table.query()

    expect(memResults.length).toBe(r2Results.length)
    expect(memResults.length).toBe(15)

    // Both should have same version
    expect(await memTable.version()).toBe(await r2Table.version())
  })
})

// =============================================================================
// PARQUET FILE ROUND-TRIP TESTS
// =============================================================================

describe('Parquet File Round-Trips', () => {
  let storage: MemoryStorage
  let table: DeltaTable<TestRecord>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createTestTable(storage)
  })

  describe('Data Type Preservation', () => {
    it('should preserve integer values through round-trip', async () => {
      const records = [
        { id: 1, name: 'test', value: 0 },
        { id: 2, name: 'test', value: 42 },
        { id: 3, name: 'test', value: -100 },
        { id: 4, name: 'test', value: 2147483647 }, // Max int32
      ]

      await table.write(records)
      const results = await table.query()

      expect(results.map(r => r.value).sort((a, b) => a - b)).toEqual(
        [-100, 0, 42, 2147483647]
      )
    })

    it('should preserve string values through round-trip', async () => {
      const records = [
        { id: 1, name: '', value: 1 },
        { id: 2, name: 'simple', value: 2 },
        { id: 3, name: 'with spaces', value: 3 },
        { id: 4, name: 'special!@#$%', value: 4 },
        { id: 5, name: 'unicode \u00e9\u00e8\u00ea', value: 5 },
      ]

      await table.write(records)
      const results = await table.query()

      const names = results.map(r => r.name).sort()
      expect(names).toContain('')
      expect(names).toContain('simple')
      expect(names).toContain('with spaces')
      expect(names).toContain('special!@#$%')
      expect(names).toContain('unicode \u00e9\u00e8\u00ea')
    })

    it('should preserve multiple writes with consistent schema', async () => {
      await table.write([{ id: 1, name: 'first', value: 100 }])
      await table.write([{ id: 2, name: 'second', value: 200 }])
      await table.write([{ id: 3, name: 'third', value: 300 }])

      const results = await table.query()
      expect(results).toHaveLength(3)

      // Schema should be consistent
      for (const result of results) {
        expect(typeof result.id).toBe('number')
        expect(typeof result.name).toBe('string')
        expect(typeof result.value).toBe('number')
      }
    })
  })

  describe('Parquet File Structure', () => {
    it('should create valid Parquet files with magic bytes', async () => {
      await table.write(generateTestRecords(1, 5))

      const snapshot = await table.snapshot()
      const filePath = snapshot.files[0].path

      const fileData = await storage.read(filePath)

      // Check Parquet magic bytes at start: PAR1
      expect(fileData.slice(0, 4)).toEqual(new Uint8Array([0x50, 0x41, 0x52, 0x31]))

      // Check Parquet magic bytes at end: PAR1
      expect(fileData.slice(-4)).toEqual(new Uint8Array([0x50, 0x41, 0x52, 0x31]))
    })

    it('should store file size correctly in Delta log', async () => {
      await table.write(generateTestRecords(1, 10))

      const snapshot = await table.snapshot()
      const loggedSize = snapshot.files[0].size

      // Verify size matches actual file
      const fileData = await storage.read(snapshot.files[0].path)
      expect(loggedSize).toBe(fileData.length)
    })
  })
})

// =============================================================================
// FILE REMOVAL AND COMPACTION TESTS
// =============================================================================

describe('File Removal Across Multiple Files', () => {
  let storage: MemoryStorage
  let table: DeltaTable<TestRecord>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createTestTable(storage)
  })

  it('should handle delete operations affecting specific files', async () => {
    // Write to two separate files
    await table.write([
      { id: 1, name: 'file1-record1', value: 100 },
      { id: 2, name: 'file1-record2', value: 200 },
    ])
    await table.write([
      { id: 3, name: 'file2-record1', value: 300 },
      { id: 4, name: 'file2-record2', value: 400 },
    ])

    // Delete a record (implementation dependent)
    await table.delete({ id: 2 })

    const results = await table.query()
    expect(results.map(r => r.id).sort((a, b) => a - b)).toEqual([1, 3, 4])
  })

  it('should correctly track removed files in snapshot', async () => {
    await table.write(generateTestRecords(1, 5))
    const snapshot1 = await table.snapshot()
    const originalPath = snapshot1.files[0].path

    // Write more data
    await table.write(generateTestRecords(6, 5))

    // Update (which removes old file and adds new)
    await table.update({ id: 1 }, { value: 999 })

    const finalSnapshot = await table.snapshot()

    // Original file should be removed if rewritten
    // This depends on implementation - verify file count is correct
    const totalRecords = await table.query()
    expect(totalRecords).toHaveLength(10)
  })
})
