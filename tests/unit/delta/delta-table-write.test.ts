/**
 * DeltaTable.write() Tests with Actual Parquet Output
 *
 * TDD RED Phase: These tests define the expected behavior for DeltaTable.write()
 * with actual Parquet file creation using StreamingParquetWriter.
 *
 * Currently, DeltaTable.write() is a stub that stores data in memory.
 * These tests expect actual Parquet files to be written to storage.
 *
 * Tests cover:
 * - Writing rows creates actual Parquet file in storage
 * - AddAction is added to Delta log with correct path, size, stats
 * - Schema inference from row types
 * - Variant encoding for object/array fields
 * - File statistics generation (numRecords, minValues, maxValues)
 * - Multiple writes create multiple Parquet files
 * - Error handling for invalid rows
 * - Integration with MemoryStorage for testing
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { DeltaTable, type AddAction, type DeltaCommit, parseStats } from '../../../src/delta/index.js'
import { MemoryStorage } from '../../../src/storage/index.js'
import { parquetReadObjects } from '@dotdo/hyparquet'

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Helper to read a Parquet file from storage and return its rows
 * @param preserveBinary - if true, byte arrays are returned as Uint8Array instead of utf8 strings
 */
async function readParquetFile(
  storage: MemoryStorage,
  path: string,
  preserveBinary: boolean = false
): Promise<Record<string, unknown>[]> {
  const data = await storage.read(path)
  const arrayBuffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
  return await parquetReadObjects({
    file: arrayBuffer,
    utf8: !preserveBinary, // When preserveBinary is true, don't decode as utf8
  }) as Record<string, unknown>[]
}

/**
 * Helper to check if data is a valid Parquet file (has PAR1 magic bytes)
 */
function isValidParquetFile(data: Uint8Array): boolean {
  if (data.length < 8) return false
  // Check header magic
  const header = String.fromCharCode(data[0], data[1], data[2], data[3])
  // Check footer magic
  const footer = String.fromCharCode(
    data[data.length - 4],
    data[data.length - 3],
    data[data.length - 2],
    data[data.length - 1]
  )
  return header === 'PAR1' && footer === 'PAR1'
}

/**
 * Helper to get the AddAction from a commit
 */
function getAddAction(commit: DeltaCommit): AddAction | undefined {
  return commit.actions.find((a): a is AddAction => 'add' in a)
}

// =============================================================================
// PARQUET FILE CREATION TESTS
// =============================================================================

describe('DeltaTable.write() - Parquet File Creation', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('writes actual Parquet files to storage', () => {
    it('should write a valid Parquet file when writing rows', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      expect(addAction).toBeDefined()

      // Read the file from storage
      const parquetData = await storage.read(addAction!.add.path)

      // Verify it's a valid Parquet file
      expect(isValidParquetFile(parquetData)).toBe(true)
    })

    it('should store actual row data in the Parquet file', async () => {
      const rows = [
        { id: 1, name: 'Alice', active: true },
        { id: 2, name: 'Bob', active: false },
        { id: 3, name: 'Charlie', active: true },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Read back the Parquet file
      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows).toHaveLength(3)
      expect(readRows[0]).toEqual({ id: 1, name: 'Alice', active: true })
      expect(readRows[1]).toEqual({ id: 2, name: 'Bob', active: false })
      expect(readRows[2]).toEqual({ id: 3, name: 'Charlie', active: true })
    })

    it('should write Parquet file to correct path in table directory', async () => {
      const rows = [{ id: 1 }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Path should be within the table directory
      expect(addAction!.add.path).toMatch(/^test-table\//)
      // Should have .parquet extension
      expect(addAction!.add.path).toMatch(/\.parquet$/)
    })

    it('should report accurate file size in AddAction', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Read actual file size from storage
      const parquetData = await storage.read(addAction!.add.path)
      const actualSize = parquetData.length

      // Size in AddAction should match actual file size
      expect(addAction!.add.size).toBe(actualSize)
    })

    it('should set dataChange to true for write operations', async () => {
      const rows = [{ id: 1 }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      expect(addAction!.add.dataChange).toBe(true)
    })

    it('should set modificationTime to current timestamp', async () => {
      const before = Date.now()
      const rows = [{ id: 1 }]

      const commit = await table.write(rows)
      const after = Date.now()

      const addAction = getAddAction(commit)

      expect(addAction!.add.modificationTime).toBeGreaterThanOrEqual(before)
      expect(addAction!.add.modificationTime).toBeLessThanOrEqual(after)
    })
  })
})

// =============================================================================
// STATISTICS GENERATION TESTS
// =============================================================================

describe('DeltaTable.write() - File Statistics', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('generates correct file statistics', () => {
    it('should include numRecords in stats', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      expect(addAction!.add.stats).toBeDefined()
      const stats = parseStats(addAction!.add.stats!)

      expect(stats.numRecords).toBe(3)
    })

    it('should include minValues for numeric columns', async () => {
      const rows = [
        { id: 5, value: 100 },
        { id: 2, value: 50 },
        { id: 8, value: 200 },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      expect(stats.minValues.id).toBe(2)
      expect(stats.minValues.value).toBe(50)
    })

    it('should include maxValues for numeric columns', async () => {
      const rows = [
        { id: 5, value: 100 },
        { id: 2, value: 50 },
        { id: 8, value: 200 },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      expect(stats.maxValues.id).toBe(8)
      expect(stats.maxValues.value).toBe(200)
    })

    it('should include minValues for string columns', async () => {
      const rows = [
        { name: 'Charlie' },
        { name: 'Alice' },
        { name: 'Bob' },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      expect(stats.minValues.name).toBe('Alice')
    })

    it('should include maxValues for string columns', async () => {
      const rows = [
        { name: 'Charlie' },
        { name: 'Alice' },
        { name: 'Bob' },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      expect(stats.maxValues.name).toBe('Charlie')
    })

    it('should include nullCount for columns with null values', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
        { id: 3, name: 'Charlie' },
        { id: 4, name: null },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      expect(stats.nullCount.id).toBe(0)
      expect(stats.nullCount.name).toBe(2)
    })

    it('should handle all non-null values with nullCount of 0', async () => {
      const rows = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      expect(stats.nullCount.id).toBe(0)
      expect(stats.nullCount.name).toBe(0)
    })
  })
})

// =============================================================================
// SCHEMA INFERENCE TESTS
// =============================================================================

describe('DeltaTable.write() - Schema Inference', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('infers schema from row types', () => {
    it('should infer int32 type for integer values', async () => {
      const rows = [{ count: 42 }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Read back and verify the value is stored correctly
      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].count).toBe(42)
    })

    it('should infer double type for floating point values', async () => {
      const rows = [{ price: 99.99 }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].price).toBeCloseTo(99.99)
    })

    it('should infer string type for string values', async () => {
      const rows = [{ name: 'Test String' }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].name).toBe('Test String')
    })

    it('should infer boolean type for boolean values', async () => {
      const rows = [{ active: true }, { active: false }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].active).toBe(true)
      expect(readRows[1].active).toBe(false)
    })

    it('should infer int64 type for bigint values', async () => {
      const rows = [{ bigId: BigInt('9223372036854775807') }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].bigId).toBe(BigInt('9223372036854775807'))
    })

    it('should handle mixed types across multiple columns', async () => {
      const rows = [{
        id: 1,
        name: 'Alice',
        score: 95.5,
        active: true,
      }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0]).toEqual({
        id: 1,
        name: 'Alice',
        score: 95.5,
        active: true,
      })
    })

    it('should handle nullable fields', async () => {
      const rows = [
        { id: 1, optional: 'value' },
        { id: 2, optional: null },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].optional).toBe('value')
      expect(readRows[1].optional).toBeNull()
    })

    it('should infer integer type from subsequent rows when first row has null', async () => {
      // First row has null for 'value' field, subsequent rows have integers
      const rows = [
        { id: 1, value: null },
        { id: 2, value: 42 },
        { id: 3, value: 100 },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Read back and verify the values are stored correctly as integers
      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].value).toBeNull()
      expect(readRows[1].value).toBe(42)
      expect(readRows[2].value).toBe(100)
    })

    it('should infer type from non-null values when first row has undefined', async () => {
      const rows = [
        { id: 1, score: undefined },
        { id: 2, score: 85.5 },
        { id: 3, score: 92.0 },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].score).toBeNull()
      expect(readRows[1].score).toBeCloseTo(85.5)
      expect(readRows[2].score).toBeCloseTo(92.0)
    })

    it('should infer boolean type when first row has null', async () => {
      const rows = [
        { id: 1, active: null },
        { id: 2, active: true },
        { id: 3, active: false },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].active).toBeNull()
      expect(readRows[1].active).toBe(true)
      expect(readRows[2].active).toBe(false)
    })

    it('should infer object type (variant) when first row has null', async () => {
      const rows = [
        { id: 1, metadata: null },
        { id: 2, metadata: { key: 'value' } },
        { id: 3, metadata: { nested: { deep: true } } },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].metadata).toBeNull()
      expect(readRows[1].metadata).toEqual({ key: 'value' })
      expect(readRows[2].metadata).toEqual({ nested: { deep: true } })
    })

    it('should default to string when all values are null', async () => {
      const rows = [
        { id: 1, alwaysNull: null },
        { id: 2, alwaysNull: null },
        { id: 3, alwaysNull: null },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].alwaysNull).toBeNull()
      expect(readRows[1].alwaysNull).toBeNull()
      expect(readRows[2].alwaysNull).toBeNull()
    })

    it('should correctly infer types with multiple null fields in first row', async () => {
      const rows = [
        { id: 1, intField: null, strField: null, boolField: null },
        { id: 2, intField: 42, strField: 'hello', boolField: true },
        { id: 3, intField: 100, strField: 'world', boolField: false },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      // First row should have nulls
      expect(readRows[0].intField).toBeNull()
      expect(readRows[0].strField).toBeNull()
      expect(readRows[0].boolField).toBeNull()

      // Second row should have the correct types
      expect(readRows[1].intField).toBe(42)
      expect(readRows[1].strField).toBe('hello')
      expect(readRows[1].boolField).toBe(true)

      // Third row should also have correct types
      expect(readRows[2].intField).toBe(100)
      expect(readRows[2].strField).toBe('world')
      expect(readRows[2].boolField).toBe(false)
    })
  })
})

// =============================================================================
// VARIANT ENCODING TESTS
// =============================================================================

describe('DeltaTable.write() - Variant Encoding', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('encodes object and array fields as variant type', () => {
    it('should encode nested objects as variant', async () => {
      const rows = [{
        id: 1,
        metadata: {
          key: 'value',
          nested: { deep: true },
        },
      }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].id).toBe(1)
      expect(readRows[0].metadata).toEqual({
        key: 'value',
        nested: { deep: true },
      })
    })

    it('should encode arrays as variant', async () => {
      const rows = [{
        id: 1,
        tags: ['a', 'b', 'c'],
      }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].tags).toEqual(['a', 'b', 'c'])
    })

    it('should encode mixed arrays as variant', async () => {
      const rows = [{
        id: 1,
        mixed: [1, 'two', { three: 3 }, [4, 5]],
      }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].mixed).toEqual([1, 'two', { three: 3 }, [4, 5]])
    })

    it('should preserve variant data through write/read cycle', async () => {
      const complexData = {
        string: 'hello',
        number: 42,
        float: 3.14,
        boolean: true,
        null: null,
        array: [1, 'two', { three: 3 }],
        object: { nested: { deep: 'value' } },
      }

      const rows = [{ id: 1, data: complexData }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].data).toEqual(complexData)
    })

    it('should handle empty objects', async () => {
      const rows = [{ id: 1, empty: {} }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].empty).toEqual({})
    })

    it('should handle empty arrays', async () => {
      const rows = [{ id: 1, empty: [] }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].empty).toEqual([])
    })
  })
})

// =============================================================================
// MULTIPLE WRITES TESTS
// =============================================================================

describe('DeltaTable.write() - Multiple Writes', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('creates separate Parquet files for each write', () => {
    it('should create multiple Parquet files for multiple writes', async () => {
      const commit1 = await table.write([{ id: 1 }])
      const commit2 = await table.write([{ id: 2 }])
      const commit3 = await table.write([{ id: 3 }])

      const addAction1 = getAddAction(commit1)
      const addAction2 = getAddAction(commit2)
      const addAction3 = getAddAction(commit3)

      // All paths should be unique
      expect(addAction1!.add.path).not.toBe(addAction2!.add.path)
      expect(addAction2!.add.path).not.toBe(addAction3!.add.path)
      expect(addAction1!.add.path).not.toBe(addAction3!.add.path)

      // All files should exist in storage
      expect(await storage.exists(addAction1!.add.path)).toBe(true)
      expect(await storage.exists(addAction2!.add.path)).toBe(true)
      expect(await storage.exists(addAction3!.add.path)).toBe(true)

      // All files should be valid Parquet
      const data1 = await storage.read(addAction1!.add.path)
      const data2 = await storage.read(addAction2!.add.path)
      const data3 = await storage.read(addAction3!.add.path)

      expect(isValidParquetFile(data1)).toBe(true)
      expect(isValidParquetFile(data2)).toBe(true)
      expect(isValidParquetFile(data3)).toBe(true)
    })

    it('should maintain data isolation between writes', async () => {
      await table.write([{ id: 1, name: 'First' }])
      await table.write([{ id: 2, name: 'Second' }])

      const snapshot = await table.snapshot()

      // Should have 2 files
      expect(snapshot.files).toHaveLength(2)

      // Read each file and verify content
      const rows1 = await readParquetFile(storage, snapshot.files[0].path)
      const rows2 = await readParquetFile(storage, snapshot.files[1].path)

      // Each file should contain only its own data
      expect(rows1).toHaveLength(1)
      expect(rows2).toHaveLength(1)

      // Verify the data is correct (order may vary)
      const allRows = [...rows1, ...rows2]
      expect(allRows).toContainEqual({ id: 1, name: 'First' })
      expect(allRows).toContainEqual({ id: 2, name: 'Second' })
    })

    it('should increment version numbers correctly', async () => {
      const commit1 = await table.write([{ id: 1 }])
      const commit2 = await table.write([{ id: 2 }])
      const commit3 = await table.write([{ id: 3 }])

      expect(commit1.version).toBe(0)
      expect(commit2.version).toBe(1)
      expect(commit3.version).toBe(2)
    })

    it('should query all data across multiple writes', async () => {
      await table.write([{ id: 1 }])
      await table.write([{ id: 2 }])
      await table.write([{ id: 3 }])

      const allRows = await table.query()

      expect(allRows).toHaveLength(3)
      expect(allRows.map(r => r.id).sort()).toEqual([1, 2, 3])
    })
  })
})

// =============================================================================
// DELTA LOG INTEGRATION TESTS
// =============================================================================

describe('DeltaTable.write() - Delta Log Integration', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('creates correct Delta log entries', () => {
    it('should create protocol action on first write', async () => {
      await table.write([{ id: 1 }])

      const logContent = await storage.read('test-table/_delta_log/00000000000000000000.json')
      const lines = new TextDecoder().decode(logContent).trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const protocolAction = actions.find(a => 'protocol' in a)
      expect(protocolAction).toBeDefined()
      expect(protocolAction.protocol.minReaderVersion).toBeGreaterThanOrEqual(1)
      expect(protocolAction.protocol.minWriterVersion).toBeGreaterThanOrEqual(1)
    })

    it('should create metadata action on first write', async () => {
      await table.write([{ id: 1 }])

      const logContent = await storage.read('test-table/_delta_log/00000000000000000000.json')
      const lines = new TextDecoder().decode(logContent).trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const metadataAction = actions.find(a => 'metaData' in a)
      expect(metadataAction).toBeDefined()
      expect(metadataAction.metaData.id).toBeDefined()
      expect(metadataAction.metaData.format.provider).toBe('parquet')
    })

    it('should create add action with correct file reference', async () => {
      const commit = await table.write([{ id: 1 }])

      const logContent = await storage.read('test-table/_delta_log/00000000000000000000.json')
      const lines = new TextDecoder().decode(logContent).trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const addAction = actions.find(a => 'add' in a)
      expect(addAction).toBeDefined()

      // The file referenced in the add action should exist
      expect(await storage.exists(addAction.add.path)).toBe(true)
    })

    it('should create commitInfo action', async () => {
      await table.write([{ id: 1 }])

      const logContent = await storage.read('test-table/_delta_log/00000000000000000000.json')
      const lines = new TextDecoder().decode(logContent).trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      const commitInfoAction = actions.find(a => 'commitInfo' in a)
      expect(commitInfoAction).toBeDefined()
      expect(commitInfoAction.commitInfo.operation).toBe('WRITE')
      expect(commitInfoAction.commitInfo.timestamp).toBeGreaterThan(0)
    })

    it('should not include protocol/metadata in subsequent writes', async () => {
      await table.write([{ id: 1 }])
      await table.write([{ id: 2 }])

      const logContent = await storage.read('test-table/_delta_log/00000000000000000001.json')
      const lines = new TextDecoder().decode(logContent).trim().split('\n')
      const actions = lines.map(line => JSON.parse(line))

      // Second commit should not have protocol or metadata
      const protocolAction = actions.find(a => 'protocol' in a)
      const metadataAction = actions.find(a => 'metaData' in a)

      expect(protocolAction).toBeUndefined()
      expect(metadataAction).toBeUndefined()

      // But should still have add and commitInfo
      const addAction = actions.find(a => 'add' in a)
      const commitInfoAction = actions.find(a => 'commitInfo' in a)

      expect(addAction).toBeDefined()
      expect(commitInfoAction).toBeDefined()
    })
  })
})

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

describe('DeltaTable.write() - Error Handling', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('handles error cases correctly', () => {
    it('should throw on empty rows array', async () => {
      await expect(table.write([])).rejects.toThrow('Cannot write empty data')
    })

    it('should allow schema evolution with new fields in subsequent writes', async () => {
      // First write establishes schema
      await table.write([{ id: 1, name: 'Alice' }])

      // Second write with additional fields is allowed (schema evolution)
      const commit = await table.write([{ id: 2, name: 'Bob', differentField: 'value' }])
      expect(commit.version).toBe(1)

      // Query should return all rows (missing fields will be null)
      const rows = await table.query()
      expect(rows).toHaveLength(2)
    })

    it('should throw on type mismatch within same write', async () => {
      // Rows with inconsistent types for same field
      const rows = [
        { id: 1, value: 42 },
        { id: 2, value: 'not a number' },
      ]

      await expect(table.write(rows)).rejects.toThrow(/type/i)
    })

    it('should handle undefined values as null', async () => {
      const rows = [
        { id: 1, optional: 'value' },
        { id: 2, optional: undefined },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      expect(readRows[0].optional).toBe('value')
      expect(readRows[1].optional).toBeNull()
    })
  })
})

// =============================================================================
// LARGE DATA TESTS
// =============================================================================

describe('DeltaTable.write() - Large Data Handling', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('handles large datasets', () => {
    it('should write 1000 rows successfully', async () => {
      const rows = Array.from({ length: 1000 }, (_, i) => ({
        id: i,
        name: `user_${i}`,
        score: Math.random() * 100,
      }))

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Verify stats
      const stats = parseStats(addAction!.add.stats!)
      expect(stats.numRecords).toBe(1000)

      // Verify min/max for id
      expect(stats.minValues.id).toBe(0)
      expect(stats.maxValues.id).toBe(999)
    })

    it('should write rows with large string values', async () => {
      const largeString = 'x'.repeat(10000)
      const rows = [
        { id: 1, data: largeString },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].data).toBe(largeString)
    })

    it('should write rows with deeply nested objects', async () => {
      const deepObject = {
        level1: {
          level2: {
            level3: {
              level4: {
                level5: {
                  value: 'deep',
                },
              },
            },
          },
        },
      }

      const rows = [{ id: 1, nested: deepObject }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)
      expect(readRows[0].nested).toEqual(deepObject)
    })
  })
})

// =============================================================================
// TIMESTAMP HANDLING TESTS
// =============================================================================

describe('DeltaTable.write() - Timestamp Handling', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('handles date and timestamp values', () => {
    it('should write and read Date objects', async () => {
      const date = new Date('2024-06-15T10:30:00Z')
      const rows = [{ id: 1, timestamp: date }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const readRows = await readParquetFile(storage, addAction!.add.path)

      // Date should be preserved (may be stored as milliseconds)
      const readDate = new Date(readRows[0].timestamp as number | Date)
      expect(readDate.getTime()).toBe(date.getTime())
    })

    it('should include Date values in statistics', async () => {
      const date1 = new Date('2024-01-15T00:00:00Z')
      const date2 = new Date('2024-06-15T00:00:00Z')
      const date3 = new Date('2024-03-15T00:00:00Z')

      const rows = [
        { id: 1, timestamp: date1 },
        { id: 2, timestamp: date2 },
        { id: 3, timestamp: date3 },
      ]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      const stats = parseStats(addAction!.add.stats!)

      // Min should be January, max should be June
      const minDate = new Date(stats.minValues.timestamp as number)
      const maxDate = new Date(stats.maxValues.timestamp as number)

      expect(minDate.getTime()).toBe(date1.getTime())
      expect(maxDate.getTime()).toBe(date2.getTime())
    })
  })
})

// =============================================================================
// BINARY DATA TESTS
// =============================================================================

describe('DeltaTable.write() - Binary Data Handling', () => {
  let storage: MemoryStorage
  let table: DeltaTable

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('handles binary data', () => {
    it('should write and read Uint8Array values', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      const rows = [{ id: 1, data: binaryData }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Use preserveBinary=true to get raw bytes instead of utf8 strings
      const readRows = await readParquetFile(storage, addAction!.add.path, true)

      // Binary data should be preserved
      expect(new Uint8Array(readRows[0].data as ArrayBuffer)).toEqual(binaryData)
    })

    it('should handle empty binary data', async () => {
      const emptyBinary = new Uint8Array(0)
      const rows = [{ id: 1, data: emptyBinary }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Use preserveBinary=true to get raw bytes instead of utf8 strings
      const readRows = await readParquetFile(storage, addAction!.add.path, true)

      expect(new Uint8Array(readRows[0].data as ArrayBuffer).length).toBe(0)
    })

    it('should handle large binary data', async () => {
      const largeBinary = new Uint8Array(10000)
      for (let i = 0; i < largeBinary.length; i++) {
        largeBinary[i] = i % 256
      }
      const rows = [{ id: 1, data: largeBinary }]

      const commit = await table.write(rows)
      const addAction = getAddAction(commit)

      // Use preserveBinary=true to get raw bytes instead of utf8 strings
      const readRows = await readParquetFile(storage, addAction!.add.path, true)

      expect(new Uint8Array(readRows[0].data as ArrayBuffer)).toEqual(largeBinary)
    })
  })
})

// =============================================================================
// CONCURRENT WRITE TESTS
// =============================================================================

describe('DeltaTable.write() - Concurrent Operations', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('handles concurrent writes', () => {
    it('should handle sequential writes from same table instance', async () => {
      const table = new DeltaTable(storage, 'test-table')

      // Write sequentially
      await table.write([{ id: 1 }])
      await table.write([{ id: 2 }])
      await table.write([{ id: 3 }])

      const version = await table.version()
      expect(version).toBe(2)

      const allRows = await table.query()
      expect(allRows).toHaveLength(3)
    })

    it('should handle writes with Promise.all (same table)', async () => {
      const table = new DeltaTable(storage, 'test-table')

      // Note: This tests concurrent invocation but DeltaTable may serialize internally
      // The important thing is no data corruption occurs
      const results = await Promise.allSettled([
        table.write([{ id: 1 }]),
        table.write([{ id: 2 }]),
        table.write([{ id: 3 }]),
      ])

      // At least some should succeed (may have version conflicts)
      const succeeded = results.filter(r => r.status === 'fulfilled')
      expect(succeeded.length).toBeGreaterThan(0)
    })
  })
})
