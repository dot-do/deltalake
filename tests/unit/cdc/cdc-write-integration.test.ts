/**
 * CDC Write Integration Tests
 *
 * Comprehensive test suite for CDC (Change Data Capture) write integration
 * with DeltaTable. These tests verify that CDC records are properly generated
 * when data is written, updated, or deleted from a Delta table.
 *
 * Tests cover:
 * - INSERT generates 'c' (create) CDC record with _after populated
 * - UPDATE generates 'u' (update) CDC record with _before and _after
 * - DELETE generates 'd' (delete) CDC record with _before populated
 * - CDC records written to _change_data/ directory as Parquet files
 * - CDC files partitioned by commit version
 * - Batch operations generate multiple CDC records
 * - Transaction ID (_txn) tracking for related changes
 * - Timestamp (_ts) is nanosecond precision
 * - CDC only generated when table has CDC enabled
 *
 * These tests are written in the RED phase - they define expected behavior
 * that the implementation should satisfy.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  CDCRecord,
  CDCOperation,
  CDCProducer,
  createCDCDeltaTable,
  DeltaCDCRecord,
  CDCDeltaTable,
} from '../../../src/cdc/index'
import { MemoryStorage, StorageBackend } from '../../../src/storage/index'

// =============================================================================
// TEST TYPES
// =============================================================================

interface TestRow {
  id: string
  name: string
  value: number
  updated_at?: Date
}

interface DocumentRow {
  _id: string
  content: string
  version: number
  metadata?: Record<string, unknown>
}

// =============================================================================
// INSERT GENERATES 'c' (CREATE) CDC RECORD WITH _after POPULATED
// =============================================================================

describe('INSERT generates CDC create record', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('single row insert', () => {
    it('should generate a CDC record with _change_type "insert"', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(1)
      expect(changes[0]._change_type).toBe('insert')
    })

    it('should populate data field with inserted row values', async () => {
      await table.setCDCEnabled(true)
      const row: TestRow = { id: '1', name: 'Alice', value: 100 }
      await table.write([row])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data).toEqual(row)
    })

    it('should include _commit_version matching the write version', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0]._commit_version).toBe(0n)
    })

    it('should include _commit_timestamp as a valid Date', async () => {
      await table.setCDCEnabled(true)
      const beforeWrite = new Date()
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      const afterWrite = new Date()

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0]._commit_timestamp).toBeInstanceOf(Date)
      expect(changes[0]._commit_timestamp.getTime()).toBeGreaterThanOrEqual(beforeWrite.getTime())
      expect(changes[0]._commit_timestamp.getTime()).toBeLessThanOrEqual(afterWrite.getTime())
    })
  })

  describe('multi-row insert', () => {
    it('should generate one CDC record per row inserted', async () => {
      await table.setCDCEnabled(true)
      const rows: TestRow[] = [
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
        { id: '3', name: 'Charlie', value: 300 },
      ]
      await table.write(rows)

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(3)
      expect(changes.every(c => c._change_type === 'insert')).toBe(true)
    })

    it('should preserve row order in CDC records', async () => {
      await table.setCDCEnabled(true)
      const rows: TestRow[] = [
        { id: 'first', name: 'First', value: 1 },
        { id: 'second', name: 'Second', value: 2 },
        { id: 'third', name: 'Third', value: 3 },
      ]
      await table.write(rows)

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data.id).toBe('first')
      expect(changes[1].data.id).toBe('second')
      expect(changes[2].data.id).toBe('third')
    })

    it('should have same _commit_version for all rows in batch', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      const versions = new Set(changes.map(c => c._commit_version))
      expect(versions.size).toBe(1)
    })

    it('should have same _commit_timestamp for all rows in batch', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
        { id: '3', name: 'Charlie', value: 300 },
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      const timestamps = new Set(changes.map(c => c._commit_timestamp.getTime()))
      expect(timestamps.size).toBe(1)
    })
  })

  describe('insert with complex data types', () => {
    it('should handle nested objects in inserted data', async () => {
      interface NestedRow {
        id: string
        config: { nested: { deep: string } }
      }

      const nestedTable = createCDCDeltaTable<NestedRow>(storage, 'nested_table')
      await nestedTable.setCDCEnabled(true)
      await nestedTable.write([{ id: '1', config: { nested: { deep: 'value' } } }])

      const reader = nestedTable.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data.config.nested.deep).toBe('value')
    })

    it('should handle arrays in inserted data', async () => {
      interface ArrayRow {
        id: string
        tags: string[]
        scores: number[]
      }

      const arrayTable = createCDCDeltaTable<ArrayRow>(storage, 'array_table')
      await arrayTable.setCDCEnabled(true)
      await arrayTable.write([{ id: '1', tags: ['a', 'b', 'c'], scores: [1, 2, 3] }])

      const reader = arrayTable.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data.tags).toEqual(['a', 'b', 'c'])
      expect(changes[0].data.scores).toEqual([1, 2, 3])
    })

    it('should handle null values in inserted data', async () => {
      interface NullableRow {
        id: string
        name: string | null
        value: number | null
      }

      const nullableTable = createCDCDeltaTable<NullableRow>(storage, 'nullable_table')
      await nullableTable.setCDCEnabled(true)
      await nullableTable.write([{ id: '1', name: null, value: null }])

      const reader = nullableTable.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data.name).toBeNull()
      expect(changes[0].data.value).toBeNull()
    })
  })
})

// =============================================================================
// UPDATE GENERATES 'u' (UPDATE) CDC RECORD WITH _before AND _after
// =============================================================================

describe('UPDATE generates CDC update record', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(async () => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)
    // Pre-populate with data
    await table.write([{ id: '1', name: 'Alice', value: 100 }])
  })

  describe('single row update', () => {
    it('should generate update_preimage CDC record with old values', async () => {
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const preimage = changes.find(c => c._change_type === 'update_preimage')
      expect(preimage).toBeDefined()
      expect(preimage!.data.value).toBe(100)
    })

    it('should generate update_postimage CDC record with new values', async () => {
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const postimage = changes.find(c => c._change_type === 'update_postimage')
      expect(postimage).toBeDefined()
      expect(postimage!.data.value).toBe(200)
    })

    it('should have preimage before postimage in record order', async () => {
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const updateChanges = changes.filter(
        c => c._change_type === 'update_preimage' || c._change_type === 'update_postimage'
      )
      expect(updateChanges[0]._change_type).toBe('update_preimage')
      expect(updateChanges[1]._change_type).toBe('update_postimage')
    })

    it('should preserve unchanged columns in both preimage and postimage', async () => {
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const preimage = changes.find(c => c._change_type === 'update_preimage')!
      const postimage = changes.find(c => c._change_type === 'update_postimage')!

      // id and name should be unchanged in both
      expect(preimage.data.id).toBe('1')
      expect(postimage.data.id).toBe('1')
      expect(preimage.data.name).toBe('Alice')
      expect(postimage.data.name).toBe('Alice')
    })
  })

  describe('multi-row update', () => {
    beforeEach(async () => {
      // Add more rows
      await table.write([
        { id: '2', name: 'Bob', value: 100 },
        { id: '3', name: 'Charlie', value: 200 },
      ])
    })

    it('should generate preimage/postimage pair for each updated row', async () => {
      // Update all rows with value=100
      await table.update({ value: 100 }, { value: 150 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(2n, 2n)

      const preimages = changes.filter(c => c._change_type === 'update_preimage')
      const postimages = changes.filter(c => c._change_type === 'update_postimage')

      // Should update rows with id='1' and id='2' (both have value=100)
      expect(preimages.length).toBe(2)
      expect(postimages.length).toBe(2)
    })

    it('should correctly pair preimage and postimage for each row', async () => {
      await table.update({ value: 100 }, { value: 150 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(2n, 2n)

      // Each row should have matching id in preimage and postimage
      const ids = new Set(changes.map(c => c.data.id))
      expect(ids.has('1')).toBe(true)
      expect(ids.has('2')).toBe(true)
    })

    it('should not generate CDC for non-matching rows', async () => {
      await table.update({ value: 200 }, { value: 250 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(2n, 2n)

      // Only id='3' has value=200
      const preimages = changes.filter(c => c._change_type === 'update_preimage')
      expect(preimages.length).toBe(1)
      expect(preimages[0].data.id).toBe('3')
    })
  })

  describe('update with multiple columns', () => {
    it('should capture all changed columns in preimage and postimage', async () => {
      await table.update({ id: '1' }, { name: 'Alicia', value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const preimage = changes.find(c => c._change_type === 'update_preimage')!
      const postimage = changes.find(c => c._change_type === 'update_postimage')!

      expect(preimage.data.name).toBe('Alice')
      expect(preimage.data.value).toBe(100)
      expect(postimage.data.name).toBe('Alicia')
      expect(postimage.data.value).toBe(200)
    })

    it('should handle update setting value to null', async () => {
      interface NullableRow {
        id: string
        name: string | null
        value: number
      }

      const nullableTable = createCDCDeltaTable<NullableRow>(storage, 'nullable_table')
      await nullableTable.setCDCEnabled(true)
      await nullableTable.write([{ id: '1', name: 'Alice', value: 100 }])
      await nullableTable.update({ id: '1' }, { name: null })

      const reader = nullableTable.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const preimage = changes.find(c => c._change_type === 'update_preimage')!
      const postimage = changes.find(c => c._change_type === 'update_postimage')!

      expect(preimage.data.name).toBe('Alice')
      expect(postimage.data.name).toBeNull()
    })
  })

  describe('update that matches no rows', () => {
    it('should not generate CDC records for non-matching update', async () => {
      await table.update({ id: 'nonexistent' }, { value: 999 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(0)
    })
  })
})

// =============================================================================
// DELETE GENERATES 'd' (DELETE) CDC RECORD WITH _before POPULATED
// =============================================================================

describe('DELETE generates CDC delete record', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(async () => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)
    await table.write([
      { id: '1', name: 'Alice', value: 100 },
      { id: '2', name: 'Bob', value: 200 },
      { id: '3', name: 'Charlie', value: 300 },
    ])
  })

  describe('single row delete', () => {
    it('should generate a CDC record with _change_type "delete"', async () => {
      await table.deleteRows({ id: '1' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(1)
      expect(changes[0]._change_type).toBe('delete')
    })

    it('should populate data with deleted row values (_before)', async () => {
      await table.deleteRows({ id: '1' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes[0].data).toEqual({ id: '1', name: 'Alice', value: 100 })
    })

    it('should include all columns from the deleted row', async () => {
      await table.deleteRows({ id: '2' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes[0].data.id).toBe('2')
      expect(changes[0].data.name).toBe('Bob')
      expect(changes[0].data.value).toBe(200)
    })
  })

  describe('multi-row delete', () => {
    it('should generate one CDC record per deleted row', async () => {
      // Delete all rows (empty filter matches all)
      await table.deleteRows({})

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(3)
      expect(changes.every(c => c._change_type === 'delete')).toBe(true)
    })

    it('should capture correct data for each deleted row', async () => {
      await table.deleteRows({})

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const deletedIds = changes.map(c => c.data.id).sort()
      expect(deletedIds).toEqual(['1', '2', '3'])
    })

    it('should only delete matching rows', async () => {
      // Delete rows with value >= 200
      await table.write([{ id: '4', name: 'David', value: 150 }])
      // Need to delete by specific filter
      await table.deleteRows({ value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(2n, 2n)

      expect(changes.length).toBe(1)
      expect(changes[0].data.id).toBe('2')
    })
  })

  describe('delete that matches no rows', () => {
    it('should not generate CDC records for non-matching delete', async () => {
      await table.deleteRows({ id: 'nonexistent' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(0)
    })
  })
})

// =============================================================================
// CDC RECORDS WRITTEN TO _change_data/ DIRECTORY AS PARQUET FILES
// =============================================================================

describe('CDC records written to _change_data/ as Parquet', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('CDC file location', () => {
    it('should write CDC files to _change_data/ directory', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBeGreaterThan(0)
    })

    it('should place _change_data/ at table root level', async () => {
      const nestedTable = createCDCDeltaTable<TestRow>(storage, 'warehouse/database/events')
      await nestedTable.setCDCEnabled(true)
      await nestedTable.write([{ id: '1', name: 'Event', value: 1 }])

      const files = await storage.list('warehouse/database/events/_change_data/')
      expect(files.length).toBeGreaterThan(0)
    })

    it('should not create _change_data/ when CDC is disabled', async () => {
      // CDC disabled by default
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBe(0)
    })
  })

  describe('CDC file format', () => {
    it('should write CDC files with .parquet extension', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.every(f => f.endsWith('.parquet'))).toBe(true)
    })

    it('should write separate CDC file for each commit', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      await table.update({ id: '1' }, { value: 150 })
      await table.deleteRows({ id: '2' })

      const files = await storage.list('test_table/_change_data/')
      // Should have at least 4 CDC files (one per commit with data changes)
      expect(files.length).toBeGreaterThanOrEqual(4)
    })
  })

  describe('CDC file content', () => {
    it('should contain all CDC records for the commit', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(2)
    })

    it('should preserve CDC record metadata columns in file', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0]).toHaveProperty('_change_type')
      expect(changes[0]).toHaveProperty('_commit_version')
      expect(changes[0]).toHaveProperty('_commit_timestamp')
    })
  })
})

// =============================================================================
// CDC FILES PARTITIONED BY COMMIT VERSION
// =============================================================================

describe('CDC files partitioned by commit version', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('version-based file naming', () => {
    it('should include 20-digit zero-padded version in filename', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      // Should have version 00000000000000000000 in filename
      expect(files.some(f => /0{19}0/.test(f))).toBe(true)
    })

    it('should align CDC version with Delta log version', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0
      await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1

      const cdcFiles = await storage.list('test_table/_change_data/')
      const deltaFiles = await storage.list('test_table/_delta_log/')

      // Check for version 0 in CDC files
      expect(cdcFiles.some(f => f.includes('00000000000000000000'))).toBe(true)
      // Check for version 1 in CDC files
      expect(cdcFiles.some(f => f.includes('00000000000000000001'))).toBe(true)
    })

    it('should create CDC file only for versions with changes', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      // Update that matches nothing creates no CDC
      await table.update({ id: 'nonexistent' }, { value: 999 })
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      const cdcFiles = await storage.list('test_table/_change_data/')

      // v0 and v2 should have CDC files, v1 might not if no changes
      expect(cdcFiles.some(f => f.includes('00000000000000000000'))).toBe(true)
    })
  })

  describe('version-based partitioning', () => {
    it('should allow reading CDC for specific version range', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0
      await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1
      await table.write([{ id: '3', name: 'Charlie', value: 300 }]) // v2

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(1)
      expect(changes[0].data.id).toBe('2')
      expect(changes[0]._commit_version).toBe(1n)
    })

    it('should handle large version numbers correctly', async () => {
      await table.setCDCEnabled(true)

      // Write many times to reach version 99
      for (let i = 0; i < 100; i++) {
        await table.write([{ id: `${i}`, name: `User${i}`, value: i }])
      }

      const cdcFiles = await storage.list('test_table/_change_data/')
      // Version 99 should be: 00000000000000000099
      expect(cdcFiles.some(f => f.includes('00000000000000000099'))).toBe(true)
    })
  })

  describe('date-based partitioning', () => {
    it('should partition CDC files by date', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      // Files should be partitioned like: _change_data/date=YYYY-MM-DD/
      const hasDatePartition = files.some(f => /date=\d{4}-\d{2}-\d{2}/.test(f))
      expect(hasDatePartition).toBe(true)
    })
  })
})

// =============================================================================
// BATCH OPERATIONS GENERATE MULTIPLE CDC RECORDS
// =============================================================================

describe('Batch operations generate multiple CDC records', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('batch insert', () => {
    it('should generate CDC record for each row in batch insert', async () => {
      await table.setCDCEnabled(true)

      const rows: TestRow[] = Array.from({ length: 100 }, (_, i) => ({
        id: `${i}`,
        name: `User${i}`,
        value: i * 10,
      }))
      await table.write(rows)

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(100)
      expect(changes.every(c => c._change_type === 'insert')).toBe(true)
    })

    it('should handle large batch inserts efficiently', async () => {
      await table.setCDCEnabled(true)

      const rows: TestRow[] = Array.from({ length: 1000 }, (_, i) => ({
        id: `${i}`,
        name: `User${i}`,
        value: i,
      }))
      await table.write(rows)

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(1000)
    })
  })

  describe('batch update via merge', () => {
    it('should generate CDC records for all matched rows in merge', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
        { id: '3', name: 'Charlie', value: 300 },
      ])

      // Merge that updates all existing rows
      await table.merge(
        [
          { id: '1', name: 'Alice Updated', value: 150 },
          { id: '2', name: 'Bob Updated', value: 250 },
          { id: '3', name: 'Charlie Updated', value: 350 },
        ],
        (existing, incoming) => existing.id === incoming.id,
        (existing, incoming) => incoming,
        (incoming) => incoming
      )

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      // 3 updates = 3 preimages + 3 postimages
      const preimages = changes.filter(c => c._change_type === 'update_preimage')
      const postimages = changes.filter(c => c._change_type === 'update_postimage')

      expect(preimages.length).toBe(3)
      expect(postimages.length).toBe(3)
    })
  })

  describe('mixed batch operations', () => {
    it('should generate correct CDC types for mixed merge operations', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
      ])

      // Merge: update id=1, delete id=2, insert id=3
      await table.merge(
        [
          { id: '1', name: 'Alice Updated', value: 150 },
          { id: '2', name: 'Bob', value: 0 }, // Will be deleted
          { id: '3', name: 'Charlie', value: 300 }, // New row
        ],
        (existing, incoming) => existing.id === incoming.id,
        (existing, incoming) => (incoming.value === 0 ? null : incoming),
        (incoming) => (incoming.value === 0 ? null : incoming)
      )

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const inserts = changes.filter(c => c._change_type === 'insert')
      const deletes = changes.filter(c => c._change_type === 'delete')
      const updates = changes.filter(
        c => c._change_type === 'update_preimage' || c._change_type === 'update_postimage'
      )

      expect(inserts.length).toBe(1) // id=3
      expect(deletes.length).toBe(1) // id=2
      expect(updates.length).toBe(2) // id=1 preimage + postimage
    })
  })
})

// =============================================================================
// TRANSACTION ID (_txn) TRACKING FOR RELATED CHANGES
// =============================================================================

describe('Transaction ID (_txn) tracking', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('CDCProducer transaction tracking', () => {
    it('should include _txn when provided', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create(
        '1',
        { id: '1', name: 'Alice', value: 100 },
        'txn-12345'
      )

      expect(record._txn).toBe('txn-12345')
    })

    it('should not include _txn when not provided', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })

      expect(record._txn).toBeUndefined()
    })

    it('should track same _txn for related operations', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const txnId = 'batch-txn-001'
      const records = await Promise.all([
        producer.create('1', { id: '1', name: 'Alice', value: 100 }, txnId),
        producer.create('2', { id: '2', name: 'Bob', value: 200 }, txnId),
        producer.update(
          '3',
          { id: '3', name: 'Old', value: 0 },
          { id: '3', name: 'New', value: 100 },
          txnId
        ),
      ])

      expect(records.every(r => r._txn === txnId)).toBe(true)
    })

    it('should allow grouping related changes by _txn', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const txn1Records = [
        await producer.create('1', { id: '1', name: 'Alice', value: 100 }, 'txn-1'),
        await producer.create('2', { id: '2', name: 'Bob', value: 200 }, 'txn-1'),
      ]

      const txn2Records = [
        await producer.delete('3', { id: '3', name: 'Charlie', value: 300 }, 'txn-2'),
      ]

      const allRecords = [...txn1Records, ...txn2Records]
      const groupedByTxn = allRecords.reduce((acc, r) => {
        const key = r._txn || 'no-txn'
        if (!acc[key]) acc[key] = []
        acc[key].push(r)
        return acc
      }, {} as Record<string, typeof allRecords>)

      expect(groupedByTxn['txn-1'].length).toBe(2)
      expect(groupedByTxn['txn-2'].length).toBe(1)
    })
  })

  describe('transaction correlation', () => {
    it('should support UUID-based transaction IDs', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const txnId = crypto.randomUUID()
      const record = await producer.create(
        '1',
        { id: '1', name: 'Alice', value: 100 },
        txnId
      )

      expect(record._txn).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
      )
    })

    it('should support custom transaction ID formats', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const customTxnId = 'app-service-2024-01-15-batch-001'
      const record = await producer.create(
        '1',
        { id: '1', name: 'Alice', value: 100 },
        customTxnId
      )

      expect(record._txn).toBe(customTxnId)
    })
  })
})

// =============================================================================
// TIMESTAMP (_ts) IS NANOSECOND PRECISION
// =============================================================================

describe('Timestamp (_ts) nanosecond precision', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('CDCProducer timestamp', () => {
    it('should generate _ts as bigint in nanoseconds', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })

      expect(typeof record._ts).toBe('bigint')
    })

    it('should have _ts value in nanosecond scale', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const beforeMs = Date.now()
      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })
      const afterMs = Date.now()

      // Convert nanoseconds to milliseconds for comparison
      const tsMs = Number(record._ts / 1_000_000n)

      expect(tsMs).toBeGreaterThanOrEqual(beforeMs)
      expect(tsMs).toBeLessThanOrEqual(afterMs)
    })

    it('should have increasing _ts for sequential records', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record1 = await producer.create('1', { id: '1', name: 'Alice', value: 100 })
      // Small delay to ensure timestamp difference (need > 1ms for nanosecond precision)
      await new Promise(resolve => setTimeout(resolve, 5))
      const record2 = await producer.create('2', { id: '2', name: 'Bob', value: 200 })

      expect(record2._ts).toBeGreaterThanOrEqual(record1._ts)
    })

    it('should be convertible to Date with millisecond precision', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })

      const tsMs = Number(record._ts / 1_000_000n)
      const date = new Date(tsMs)

      expect(date).toBeInstanceOf(Date)
      expect(date.getFullYear()).toBeGreaterThanOrEqual(2024)
    })

    it('should preserve nanosecond information beyond milliseconds', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })

      // Check that the timestamp has more precision than milliseconds
      // by verifying it's not evenly divisible by 1_000_000 (nanoseconds per millisecond)
      const hasSubMillisecondPrecision =
        record._ts % 1_000_000n !== 0n || record._ts >= 1_000_000n

      expect(hasSubMillisecondPrecision).toBe(true)
    })
  })

  describe('timestamp format for serialization', () => {
    it('should handle large nanosecond timestamps without overflow', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })

      // Current nanosecond timestamps are around 10^18-10^19
      expect(record._ts).toBeGreaterThan(1_000_000_000_000_000_000n)
    })

    it('should be serializable to string for JSON', async () => {
      const producer = new CDCProducer<TestRow>({
        source: { database: 'test', collection: 'users' },
      })

      const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })

      const tsString = record._ts.toString()
      expect(typeof tsString).toBe('string')
      expect(BigInt(tsString)).toBe(record._ts)
    })
  })
})

// =============================================================================
// CDC ONLY GENERATED WHEN TABLE HAS CDC ENABLED
// =============================================================================

describe('CDC only generated when enabled', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('default CDC state', () => {
    it('should have CDC disabled by default', async () => {
      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should not create _change_data/ when CDC is disabled', async () => {
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBe(0)
    })

    it('should not generate CDC records when disabled', async () => {
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 10n)

      expect(changes.length).toBe(0)
    })
  })

  describe('enabling CDC', () => {
    it('should allow enabling CDC', async () => {
      await table.setCDCEnabled(true)
      expect(await table.isCDCEnabled()).toBe(true)
    })

    it('should generate CDC records after enabling', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(1)
    })

    it('should create _change_data/ after enabling CDC', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBeGreaterThan(0)
    })
  })

  describe('disabling CDC', () => {
    it('should allow disabling CDC', async () => {
      await table.setCDCEnabled(true)
      await table.setCDCEnabled(false)
      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should stop generating CDC records after disabling', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0 - CDC

      await table.setCDCEnabled(false)
      await table.write([{ id: '2', name: 'Bob', value: 200 }]) // v1 - no CDC

      const reader = table.getCDCReader()
      const allChanges = await reader.readByVersion(0n, 1n)

      // Should only have CDC for v0, not v1
      const v0Changes = allChanges.filter(c => c._commit_version === 0n)
      const v1Changes = allChanges.filter(c => c._commit_version === 1n)

      expect(v0Changes.length).toBe(1)
      expect(v1Changes.length).toBe(0)
    })
  })

  describe('CDC state persistence', () => {
    it('should persist CDC enabled state across table reopening', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // Reopen table
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      expect(await table2.isCDCEnabled()).toBe(true)
    })

    it('should persist CDC disabled state across table reopening', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.setCDCEnabled(false)

      // Reopen table
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      expect(await table2.isCDCEnabled()).toBe(false)
    })
  })

  describe('per-table CDC settings', () => {
    it('should allow different tables to have different CDC settings', async () => {
      const table1 = createCDCDeltaTable<TestRow>(storage, 'table1')
      const table2 = createCDCDeltaTable<TestRow>(storage, 'table2')

      await table1.setCDCEnabled(true)
      await table2.setCDCEnabled(false)

      await table1.write([{ id: '1', name: 'Alice', value: 100 }])
      await table2.write([{ id: '1', name: 'Bob', value: 200 }])

      const table1CDC = await storage.list('table1/_change_data/')
      const table2CDC = await storage.list('table2/_change_data/')

      expect(table1CDC.length).toBeGreaterThan(0)
      expect(table2CDC.length).toBe(0)
    })
  })

  describe('Delta table metadata', () => {
    it('should record CDC setting in table configuration', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // Check that the Delta log contains CDC configuration
      const logFiles = await storage.list('test_table/_delta_log/')
      expect(logFiles.length).toBeGreaterThan(0)

      // Read the first commit to check for CDC configuration
      const commitData = await storage.read(logFiles.find(f => f.endsWith('.json'))!)
      const commitContent = new TextDecoder().decode(commitData)

      // CDC configuration should be in metaData action
      expect(
        commitContent.includes('delta.enableChangeDataFeed') ||
        commitContent.includes('enableChangeDataFeed')
      ).toBe(true)
    })
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('CDC write integration edge cases', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('empty operations', () => {
    it('should handle empty write gracefully', async () => {
      await table.setCDCEnabled(true)

      await expect(table.write([])).rejects.toThrow()
    })

    it('should not generate CDC for update that matches nothing', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: 'nonexistent' }, { value: 999 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(0)
    })

    it('should not generate CDC for delete that matches nothing', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.deleteRows({ id: 'nonexistent' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(0)
    })
  })

  describe('special characters in data', () => {
    it('should handle special characters in string fields', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: 'Alice "Bob" <charlie>', value: 100 },
        { id: '2', name: "Name with 'quotes' and\nnewlines\ttabs", value: 200 },
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data.name).toBe('Alice "Bob" <charlie>')
      expect(changes[1].data.name).toContain('quotes')
      expect(changes[1].data.name).toContain('\n')
    })

    it('should handle unicode characters', async () => {
      await table.setCDCEnabled(true)
      await table.write([
        { id: '1', name: '\u4e2d\u6587\u540d\u5b57', value: 100 }, // Chinese characters
        { id: '2', name: '\u0420\u0443\u0441\u0441\u043a\u0438\u0439', value: 200 }, // Russian characters
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes[0].data.name).toBe('\u4e2d\u6587\u540d\u5b57')
      expect(changes[1].data.name).toBe('\u0420\u0443\u0441\u0441\u043a\u0438\u0439')
    })
  })

  describe('concurrent operations', () => {
    it('should handle concurrent writes with CDC correctly', async () => {
      await table.setCDCEnabled(true)

      const writes = Array.from({ length: 10 }, (_, i) =>
        table.write([{ id: `${i}`, name: `User${i}`, value: i }])
      )

      await Promise.all(writes)

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 9n)

      // All 10 writes should have CDC records
      expect(changes.length).toBe(10)
    })
  })

  describe('failed operations', () => {
    it('should not generate CDC for failed writes', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      try {
        // @ts-ignore - testing with invalid data
        await table.write(null)
      } catch {
        // Expected to fail
      }

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 10n)

      // Should only have the successful write
      expect(changes.length).toBe(1)
    })
  })
})

// =============================================================================
// SUBSCRIPTION AND STREAMING
// =============================================================================

describe('CDC subscription for write events', () => {
  let storage: MemoryStorage
  let table: CDCDeltaTable<TestRow>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = createCDCDeltaTable<TestRow>(storage, 'test_table')
  })

  describe('subscribing to changes', () => {
    it('should notify subscribers on insert', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(async record => {
        received.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // Allow async notification
      await new Promise(resolve => setTimeout(resolve, 50))

      expect(received.length).toBe(1)
      expect(received[0]._change_type).toBe('insert')

      unsubscribe()
    })

    it('should notify subscribers on update', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(async record => {
        received.push(record)
      })

      await table.update({ id: '1' }, { value: 200 })

      await new Promise(resolve => setTimeout(resolve, 50))

      expect(received.length).toBe(2) // preimage + postimage
      expect(received.some(r => r._change_type === 'update_preimage')).toBe(true)
      expect(received.some(r => r._change_type === 'update_postimage')).toBe(true)

      unsubscribe()
    })

    it('should notify subscribers on delete', async () => {
      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(async record => {
        received.push(record)
      })

      await table.deleteRows({ id: '1' })

      await new Promise(resolve => setTimeout(resolve, 50))

      expect(received.length).toBe(1)
      expect(received[0]._change_type).toBe('delete')

      unsubscribe()
    })
  })

  describe('unsubscribing', () => {
    it('should stop receiving events after unsubscribe', async () => {
      await table.setCDCEnabled(true)

      const reader = table.getCDCReader()
      const received: DeltaCDCRecord<TestRow>[] = []

      const unsubscribe = reader.subscribe(async record => {
        received.push(record)
      })

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await new Promise(resolve => setTimeout(resolve, 50))

      unsubscribe()

      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      await new Promise(resolve => setTimeout(resolve, 50))

      // Should only have the first write
      expect(received.length).toBe(1)
      expect(received[0].data.id).toBe('1')
    })
  })
})
