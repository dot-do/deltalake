/**
 * CDC (Change Data Capture) Format Tests
 *
 * Comprehensive test suite for the Unified CDC Format feature.
 * This tests the Delta Lake CDC functionality including:
 * - CDC record structure (_change_type, _commit_version, _commit_timestamp)
 * - INSERT, UPDATE, DELETE change type generation
 * - MERGE operations with mixed changes
 * - CDC file storage in _change_data directory
 * - CDC reader by version and time range
 * - CDC enabling/disabling per table
 *
 * These tests are written in the RED phase - they should FAIL
 * because the implementations are not complete.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  CDCRecord,
  CDCSource,
  CDCOperation,
  CDCProducer,
  CDCConsumer,
  createCDCDeltaTable as createCDCDeltaTableImpl,
  DeltaCDCRecord,
  CDCDeltaTable,
} from '../../../src/cdc/index'
import { DeltaTable, DeltaCommit, AddAction } from '../../../src/delta/index'
import { MemoryStorage, StorageBackend } from '../../../src/storage/index'

// =============================================================================
// EXTENDED CDC TYPES (not yet implemented)
// =============================================================================

// These types are now imported from the implementation

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
  updated_at?: Date
}

/**
 * Create a CDC-enabled Delta table (using the implementation)
 */
function createCDCDeltaTable<T extends Record<string, unknown>>(
  storage: StorageBackend,
  tablePath: string
): CDCDeltaTable<T> {
  return createCDCDeltaTableImpl<T>(storage, tablePath)
}

/**
 * Helper to decode CDC file content
 */
function decodeCDCFile(data: Uint8Array): DeltaCDCRecord[] {
  throw new Error('CDC file decoding not implemented')
}

// =============================================================================
// CDC RECORD STRUCTURE TESTS
// =============================================================================

describe('CDC Record Structure', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('_change_type column', () => {
    it('should have _change_type as "insert" for new rows', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(1)
      expect(changes[0]._change_type).toBe('insert')
      expect(changes[0].data.id).toBe('1')
    })

    it('should have _change_type as "delete" for deleted rows', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.deleteRows({ id: '1' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(1)
      expect(changes[0]._change_type).toBe('delete')
      expect(changes[0].data.id).toBe('1')
    })

    it('should have _change_type as "update_preimage" for row before update', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const preimage = changes.find(c => c._change_type === 'update_preimage')
      expect(preimage).toBeDefined()
      expect(preimage!.data.value).toBe(100)
    })

    it('should have _change_type as "update_postimage" for row after update', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const postimage = changes.find(c => c._change_type === 'update_postimage')
      expect(postimage).toBeDefined()
      expect(postimage!.data.value).toBe(200)
    })
  })

  describe('_commit_version column', () => {
    it('should include _commit_version matching the transaction version', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 1n)

      const v0Changes = changes.filter(c => c._commit_version === 0n)
      const v1Changes = changes.filter(c => c._commit_version === 1n)

      expect(v0Changes.length).toBe(1)
      expect(v0Changes[0].data.id).toBe('1')
      expect(v1Changes.length).toBe(1)
      expect(v1Changes[0].data.id).toBe('2')
    })

    it('should have sequential _commit_version values', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      for (let i = 0; i < 5; i++) {
        await table.write([{ id: `${i}`, name: `User${i}`, value: i * 100 }])
      }

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 4n)

      const versions = [...new Set(changes.map(c => c._commit_version))].sort()
      expect(versions).toEqual([0n, 1n, 2n, 3n, 4n])
    })
  })

  describe('_commit_timestamp column', () => {
    it('should include _commit_timestamp as a Date', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
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

    it('should have consistent _commit_timestamp within a transaction', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([
        { id: '1', name: 'Alice', value: 100 },
        { id: '2', name: 'Bob', value: 200 },
        { id: '3', name: 'Charlie', value: 300 },
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      const timestamps = changes.map(c => c._commit_timestamp.getTime())
      expect(new Set(timestamps).size).toBe(1) // All timestamps should be the same
    })

    it('should have increasing _commit_timestamp across transactions', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await new Promise(resolve => setTimeout(resolve, 10))
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 1n)

      const v0Timestamp = changes.find(c => c._commit_version === 0n)!._commit_timestamp
      const v1Timestamp = changes.find(c => c._commit_version === 1n)!._commit_timestamp

      expect(v1Timestamp.getTime()).toBeGreaterThan(v0Timestamp.getTime())
    })
  })

  describe('row ordering within transactions', () => {
    it('should preserve row order within a single transaction', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([
        { id: '1', name: 'First', value: 1 },
        { id: '2', name: 'Second', value: 2 },
        { id: '3', name: 'Third', value: 3 },
      ])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      // Changes should be in the same order as written
      expect(changes[0].data.id).toBe('1')
      expect(changes[1].data.id).toBe('2')
      expect(changes[2].data.id).toBe('3')
    })

    it('should order update_preimage before update_postimage', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      // Filter to just the update changes
      const updateChanges = changes.filter(
        c => c._change_type === 'update_preimage' || c._change_type === 'update_postimage'
      )

      expect(updateChanges[0]._change_type).toBe('update_preimage')
      expect(updateChanges[1]._change_type).toBe('update_postimage')
    })
  })
})

// =============================================================================
// INSERT CHANGE TYPE GENERATION TESTS
// =============================================================================

describe('INSERT Change Type Generation', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should generate insert CDC record for single row insert', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes.length).toBe(1)
    expect(changes[0]._change_type).toBe('insert')
    expect(changes[0].data).toEqual({ id: '1', name: 'Alice', value: 100 })
  })

  it('should generate insert CDC records for batch insert', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
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
    expect(changes.map(c => c.data.id)).toEqual(['1', '2', '3'])
  })

  it('should generate insert CDC records for large batch', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
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
    expect(changes.every(c => c._change_type === 'insert')).toBe(true)
  })

  it('should include all columns in insert CDC record', async () => {
    interface FullRow {
      id: string
      name: string
      value: number
      metadata: Record<string, string>
      tags: string[]
      created_at: Date
    }

    const table = createCDCDeltaTable<FullRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const row: FullRow = {
      id: '1',
      name: 'Complex Row',
      value: 42,
      metadata: { key: 'value' },
      tags: ['tag1', 'tag2'],
      created_at: new Date('2024-01-01'),
    }
    await table.write([row])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes[0].data).toEqual(row)
  })

  it('should handle null values in insert CDC record', async () => {
    interface NullableRow {
      id: string
      name: string | null
      value: number | null
    }

    const table = createCDCDeltaTable<NullableRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: null, value: null }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes[0].data.name).toBeNull()
    expect(changes[0].data.value).toBeNull()
  })
})

// =============================================================================
// UPDATE WITH BEFORE/AFTER IMAGES TESTS
// =============================================================================

describe('UPDATE with Before/After Images', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should generate update_preimage and update_postimage for single row update', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.update({ id: '1' }, { value: 200 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimage = changes.find(c => c._change_type === 'update_preimage')
    const postimage = changes.find(c => c._change_type === 'update_postimage')

    expect(preimage).toBeDefined()
    expect(postimage).toBeDefined()
    expect(preimage!.data.value).toBe(100)
    expect(postimage!.data.value).toBe(200)
  })

  it('should capture all changed columns in update', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
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

  it('should generate pairs for multi-row update', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([
      { id: '1', name: 'Alice', value: 100 },
      { id: '2', name: 'Bob', value: 100 },
      { id: '3', name: 'Charlie', value: 200 },
    ])
    // Update all rows with value = 100
    await table.update({ value: 100 }, { value: 150 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimages = changes.filter(c => c._change_type === 'update_preimage')
    const postimages = changes.filter(c => c._change_type === 'update_postimage')

    expect(preimages.length).toBe(2)
    expect(postimages.length).toBe(2)
    expect(preimages.every(c => c.data.value === 100)).toBe(true)
    expect(postimages.every(c => c.data.value === 150)).toBe(true)
  })

  it('should preserve unchanged columns in before/after images', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.update({ id: '1' }, { value: 200 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimage = changes.find(c => c._change_type === 'update_preimage')!
    const postimage = changes.find(c => c._change_type === 'update_postimage')!

    // id and name should be unchanged
    expect(preimage.data.id).toBe('1')
    expect(postimage.data.id).toBe('1')
    expect(preimage.data.name).toBe('Alice')
    expect(postimage.data.name).toBe('Alice')
  })

  it('should handle update that affects no rows', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.update({ id: 'nonexistent' }, { value: 200 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    // No changes should be generated for update that matched no rows
    expect(changes.length).toBe(0)
  })

  it('should handle update with null values', async () => {
    interface NullableRow {
      id: string
      name: string | null
      value: number
    }

    const table = createCDCDeltaTable<NullableRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.update({ id: '1' }, { name: null })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimage = changes.find(c => c._change_type === 'update_preimage')!
    const postimage = changes.find(c => c._change_type === 'update_postimage')!

    expect(preimage.data.name).toBe('Alice')
    expect(postimage.data.name).toBeNull()
  })
})

// =============================================================================
// DELETE CHANGE TYPE GENERATION TESTS
// =============================================================================

describe('DELETE Change Type Generation', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should generate delete CDC record for single row delete', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.deleteRows({ id: '1' })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(1)
    expect(changes[0]._change_type).toBe('delete')
    expect(changes[0].data.id).toBe('1')
  })

  it('should include full row data in delete CDC record', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.deleteRows({ id: '1' })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes[0].data).toEqual({ id: '1', name: 'Alice', value: 100 })
  })

  it('should generate delete CDC records for batch delete', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([
      { id: '1', name: 'Alice', value: 100 },
      { id: '2', name: 'Bob', value: 100 },
      { id: '3', name: 'Charlie', value: 200 },
    ])
    // Delete all rows with value = 100
    await table.deleteRows({ value: 100 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(2)
    expect(changes.every(c => c._change_type === 'delete')).toBe(true)
    expect(changes.map(c => c.data.id).sort()).toEqual(['1', '2'])
  })

  it('should handle delete that affects no rows', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.deleteRows({ id: 'nonexistent' })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(0)
  })

  it('should handle delete all rows', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([
      { id: '1', name: 'Alice', value: 100 },
      { id: '2', name: 'Bob', value: 200 },
    ])
    // Delete all rows (empty filter matches all)
    await table.deleteRows({})

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(2)
    expect(changes.every(c => c._change_type === 'delete')).toBe(true)
  })
})

// =============================================================================
// MERGE WITH MIXED OPERATIONS TESTS
// =============================================================================

describe('MERGE with Mixed Operations', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should generate insert for unmatched rows in merge', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Merge with a new row that doesn't exist
    await table.merge(
      [{ id: '2', name: 'Bob', value: 200 }],
      (existing, incoming) => existing.id === incoming.id,
      (existing, incoming) => incoming, // When matched, replace
      (incoming) => incoming // When not matched, insert
    )

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(1)
    expect(changes[0]._change_type).toBe('insert')
    expect(changes[0].data.id).toBe('2')
  })

  it('should generate update for matched rows in merge', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Merge with an existing row
    await table.merge(
      [{ id: '1', name: 'Alice Updated', value: 150 }],
      (existing, incoming) => existing.id === incoming.id,
      (existing, incoming) => incoming,
      (incoming) => incoming
    )

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimage = changes.find(c => c._change_type === 'update_preimage')
    const postimage = changes.find(c => c._change_type === 'update_postimage')

    expect(preimage).toBeDefined()
    expect(postimage).toBeDefined()
    expect(preimage!.data.value).toBe(100)
    expect(postimage!.data.value).toBe(150)
  })

  it('should generate delete for matched rows with null result', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Merge that deletes matched rows
    await table.merge(
      [{ id: '1', name: 'Alice', value: 0 }],
      (existing, incoming) => existing.id === incoming.id,
      () => null, // When matched, return null to delete
      (incoming) => incoming
    )

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(1)
    expect(changes[0]._change_type).toBe('delete')
    expect(changes[0].data.id).toBe('1')
  })

  it('should generate mixed CDC records for complex merge', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    // Initial data
    await table.write([
      { id: '1', name: 'Alice', value: 100 },
      { id: '2', name: 'Bob', value: 200 },
      { id: '3', name: 'Charlie', value: 300 },
    ])

    // Merge: update id=1, delete id=2, insert id=4
    await table.merge(
      [
        { id: '1', name: 'Alice Updated', value: 150 },
        { id: '2', name: 'Bob', value: 0 }, // Will be deleted
        { id: '4', name: 'David', value: 400 }, // Will be inserted
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

    expect(inserts.length).toBe(1)
    expect(inserts[0].data.id).toBe('4')

    expect(deletes.length).toBe(1)
    expect(deletes[0].data.id).toBe('2')

    expect(updates.length).toBe(2) // preimage + postimage
  })

  it('should handle merge with no matches and no inserts', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Merge with unmatched row but whenNotMatched returns null
    await table.merge(
      [{ id: '2', name: 'Bob', value: 200 }],
      (existing, incoming) => existing.id === incoming.id,
      (existing, incoming) => incoming,
      () => null // Don't insert unmatched
    )

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(0)
  })
})

// =============================================================================
// CDC FILE STORAGE IN _change_data DIRECTORY TESTS
// =============================================================================

describe('CDC File Storage in _change_data Directory', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should store CDC files in _change_data directory', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    expect(files.length).toBeGreaterThan(0)
  })

  it('should create _change_data directory at table root level', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'data/tables/events')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Event', value: 1 }])

    const files = await storage.list('data/tables/events/_change_data/')
    expect(files.length).toBeGreaterThan(0)
  })

  it('should store CDC files in Parquet format', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    expect(files.every(f => f.endsWith('.parquet'))).toBe(true)
  })

  it('should not create CDC files when CDC is disabled', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    // CDC is disabled by default

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    expect(files.length).toBe(0)
  })

  it('should create separate CDC files for each commit', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.write([{ id: '2', name: 'Bob', value: 200 }])
    await table.update({ id: '1' }, { value: 150 })

    const files = await storage.list('test_table/_change_data/')
    // Each commit creates 2 CDC files: one at direct path and one at date-partitioned path
    // 3 commits * 2 files = 6 files total
    expect(files.length).toBe(6)
  })

  it('should partition CDC files by date when configured', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    // Files should be partitioned by date like _change_data/date=2024-01-15/
    const hasDatePartition = files.some(f => /date=\d{4}-\d{2}-\d{2}/.test(f))
    expect(hasDatePartition).toBe(true)
  })
})

// =============================================================================
// CDC READER BY VERSION RANGE TESTS
// =============================================================================

describe('CDC Reader by Version Range', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should read changes for a single version', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.write([{ id: '2', name: 'Bob', value: 200 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(1)
    expect(changes[0].data.id).toBe('2')
    expect(changes[0]._commit_version).toBe(1n)
  })

  it('should read changes for a version range (inclusive)', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0
    await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1
    await table.write([{ id: '3', name: 'Charlie', value: 300 }]) // v2
    await table.write([{ id: '4', name: 'David', value: 400 }])   // v3

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 2n)

    expect(changes.length).toBe(2)
    expect(changes.map(c => c.data.id).sort()).toEqual(['2', '3'])
  })

  it('should return empty array for version range with no CDC', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(5n, 10n)

    expect(changes).toEqual([])
  })

  it('should throw error for invalid version range (start > end)', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    await expect(reader.readByVersion(5n, 2n)).rejects.toThrow()
  })

  it('should handle version 0', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes.length).toBe(1)
    expect(changes[0]._commit_version).toBe(0n)
  })

  it('should handle gaps in CDC when CDC was temporarily disabled', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

    // Enable CDC, write, disable, write, enable, write
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0 - has CDC
    await table.setCDCEnabled(false)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1 - no CDC
    await table.setCDCEnabled(true)
    await table.write([{ id: '3', name: 'Charlie', value: 300 }]) // v2 - has CDC

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 2n)

    // Should only have changes for v0 and v2, not v1
    const versions = [...new Set(changes.map(c => c._commit_version))]
    expect(versions).toContain(0n)
    expect(versions).toContain(2n)
    expect(versions).not.toContain(1n)
  })
})

// =============================================================================
// CDC READER BY TIME RANGE TESTS
// =============================================================================

describe('CDC Reader by Time Range', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should read changes within a time range', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const before = new Date()
    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await new Promise(resolve => setTimeout(resolve, 50))
    const middle = new Date()
    await new Promise(resolve => setTimeout(resolve, 50))
    await table.write([{ id: '2', name: 'Bob', value: 200 }])
    const after = new Date()

    const reader = table.getCDCReader()
    const changes = await reader.readByTimestamp(before, after)

    expect(changes.length).toBe(2)
  })

  it('should filter changes by start time (inclusive)', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await new Promise(resolve => setTimeout(resolve, 50))
    const middle = new Date()
    await new Promise(resolve => setTimeout(resolve, 50))
    await table.write([{ id: '2', name: 'Bob', value: 200 }])
    const after = new Date()

    const reader = table.getCDCReader()
    const changes = await reader.readByTimestamp(middle, after)

    // Should only include the second write
    expect(changes.length).toBe(1)
    expect(changes[0].data.id).toBe('2')
  })

  it('should filter changes by end time (inclusive)', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const before = new Date()
    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await new Promise(resolve => setTimeout(resolve, 50))
    const middle = new Date()
    await new Promise(resolve => setTimeout(resolve, 50))
    await table.write([{ id: '2', name: 'Bob', value: 200 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByTimestamp(before, middle)

    // Should only include the first write
    expect(changes.length).toBe(1)
    expect(changes[0].data.id).toBe('1')
  })

  it('should return empty array for time range with no changes', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const futureStart = new Date(Date.now() + 10000)
    const futureEnd = new Date(Date.now() + 20000)
    const changes = await reader.readByTimestamp(futureStart, futureEnd)

    expect(changes).toEqual([])
  })

  it('should throw error for invalid time range (start > end)', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const reader = table.getCDCReader()
    const start = new Date()
    const end = new Date(start.getTime() - 1000)

    await expect(reader.readByTimestamp(start, end)).rejects.toThrow()
  })
})

// =============================================================================
// CDC FILE NAMING WITH VERSION ALIGNMENT TESTS
// =============================================================================

describe('CDC File Naming with Version Alignment', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should name CDC files with zero-padded version numbers', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    // File should be named like: cdc-00000000000000000000.parquet
    expect(files.some(f => /cdc-0{19}0\.parquet/.test(f))).toBe(true)
  })

  it('should include version number matching Delta log version', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }]) // v0
    await table.write([{ id: '2', name: 'Bob', value: 200 }])   // v1

    const cdcFiles = await storage.list('test_table/_change_data/')
    const deltaFiles = await storage.list('test_table/_delta_log/')

    // There should be alignment between CDC and Delta log versions
    expect(cdcFiles.some(f => f.includes('00000000000000000000'))).toBe(true)
    expect(cdcFiles.some(f => f.includes('00000000000000000001'))).toBe(true)
  })

  it('should use 20-digit zero-padding for version numbers', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    // All files should have 20-digit version numbers
    files.forEach(f => {
      const match = f.match(/(\d{20})/)
      expect(match).not.toBeNull()
      expect(match![1].length).toBe(20)
    })
  })

  it('should allow lookup of CDC file by version', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Should be able to construct CDC file path from version
    const version = 0
    const paddedVersion = version.toString().padStart(20, '0')
    const cdcPath = `test_table/_change_data/cdc-${paddedVersion}.parquet`

    const exists = await storage.exists(cdcPath)
    expect(exists).toBe(true)
  })

  it('should handle high version numbers', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    // Simulate many writes
    for (let i = 0; i < 100; i++) {
      await table.write([{ id: `${i}`, name: `User${i}`, value: i }])
    }

    const files = await storage.list('test_table/_change_data/')
    // Should have CDC file for version 99
    expect(files.some(f => f.includes('00000000000000000099'))).toBe(true)
  })
})

// =============================================================================
// CDC ENABLING/DISABLING PER TABLE TESTS
// =============================================================================

describe('CDC Enabling/Disabling per Table', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should not generate CDC by default', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    // Don't call setCDCEnabled

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    expect(files.length).toBe(0)
  })

  it('should generate CDC when enabled', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    expect(files.length).toBeGreaterThan(0)
  })

  it('should stop generating CDC when disabled', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }]) // CDC generated
    await table.setCDCEnabled(false)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])   // No CDC

    const files = await storage.list('test_table/_change_data/')
    // Should only have CDC files for the first write
    // Each commit creates 2 CDC files: one at direct path and one at date-partitioned path
    expect(files.length).toBe(2)
  })

  it('should report CDC enabled status correctly', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

    expect(await table.isCDCEnabled()).toBe(false)

    await table.setCDCEnabled(true)
    expect(await table.isCDCEnabled()).toBe(true)

    await table.setCDCEnabled(false)
    expect(await table.isCDCEnabled()).toBe(false)
  })

  it('should persist CDC enabled status in table metadata', async () => {
    const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table1.setCDCEnabled(true)
    await table1.write([{ id: '1', name: 'Alice', value: 100 }])

    // Recreate table instance (simulating restart)
    const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
    expect(await table2.isCDCEnabled()).toBe(true)
  })

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

  it('should record CDC setting change in Delta log', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

    await table.setCDCEnabled(true)

    const logFiles = await storage.list('test_table/_delta_log/')
    // Should have a commit that records the CDC configuration change
    expect(logFiles.length).toBeGreaterThan(0)
  })
})

// =============================================================================
// SCHEMA HANDLING TESTS
// =============================================================================

describe('CDC Schema Handling', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should include CDC metadata columns in CDC file schema', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    // Verify CDC metadata columns exist
    expect(changes[0]).toHaveProperty('_change_type')
    expect(changes[0]).toHaveProperty('_commit_version')
    expect(changes[0]).toHaveProperty('_commit_timestamp')
  })

  it('should handle schema evolution in CDC records', async () => {
    interface RowV1 {
      id: string
      name: string
    }

    interface RowV2 {
      id: string
      name: string
      email?: string
    }

    const table = createCDCDeltaTable<RowV1 | RowV2>(storage, 'test_table')
    await table.setCDCEnabled(true)

    // Write with v1 schema
    await table.write([{ id: '1', name: 'Alice' }])

    // Write with v2 schema (new column)
    await table.write([{ id: '2', name: 'Bob', email: 'bob@example.com' }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 1n)

    // First record shouldn't have email
    expect(changes[0].data).not.toHaveProperty('email')
    // Second record should have email
    expect(changes[1].data).toHaveProperty('email')
  })

  it('should maintain data types across CDC operations', async () => {
    interface TypedRow {
      id: string
      count: number
      active: boolean
      tags: string[]
      metadata: Record<string, unknown>
    }

    const table = createCDCDeltaTable<TypedRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const row: TypedRow = {
      id: '1',
      count: 42,
      active: true,
      tags: ['a', 'b'],
      metadata: { key: 'value' },
    }
    await table.write([row])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(typeof changes[0].data.count).toBe('number')
    expect(typeof changes[0].data.active).toBe('boolean')
    expect(Array.isArray(changes[0].data.tags)).toBe(true)
    expect(typeof changes[0].data.metadata).toBe('object')
  })

  it('should handle nullable columns in before/after images', async () => {
    interface NullableRow {
      id: string
      name: string | null
      value: number | null
    }

    const table = createCDCDeltaTable<NullableRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: null, value: 100 }])
    await table.update({ id: '1' }, { name: 'Alice', value: null })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimage = changes.find(c => c._change_type === 'update_preimage')!
    const postimage = changes.find(c => c._change_type === 'update_postimage')!

    expect(preimage.data.name).toBeNull()
    expect(preimage.data.value).toBe(100)
    expect(postimage.data.name).toBe('Alice')
    expect(postimage.data.value).toBeNull()
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING TESTS
// =============================================================================

describe('CDC Edge Cases and Error Handling', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should handle empty change sets', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    // Update that matches nothing
    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.update({ id: 'nonexistent' }, { value: 200 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes.length).toBe(0)
  })

  it('should handle large batch changes', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const rows: TestRow[] = Array.from({ length: 10000 }, (_, i) => ({
      id: `${i}`,
      name: `User${i}`,
      value: i,
    }))
    await table.write(rows)

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes.length).toBe(10000)
  })

  it('should handle concurrent writes with CDC', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    // Simulate concurrent writes
    const writes = Array.from({ length: 10 }, (_, i) =>
      table.write([{ id: `${i}`, name: `User${i}`, value: i }])
    )

    await Promise.all(writes)

    const reader = table.getCDCReader()
    // Read all versions
    const changes = await reader.readByVersion(0n, 9n)

    // All 10 inserts should be captured
    expect(changes.length).toBe(10)
  })

  it('should handle failed transaction (no CDC for uncommitted changes)', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // Simulate a failed write (implementation should throw)
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
    expect(changes[0].data.id).toBe('1')
  })

  it('should handle special characters in data', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([
      { id: '1', name: 'Alice "Bob" <charlie>', value: 100 },
      { id: '2', name: "Name with 'quotes' and\nnewlines\ttabs", value: 200 },
      { id: '3', name: '\u0000\u0001\u0002', value: 300 }, // Control characters
    ])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes.length).toBe(3)
    expect(changes[0].data.name).toBe('Alice "Bob" <charlie>')
    expect(changes[1].data.name).toContain('quotes')
    expect(changes[1].data.name).toContain('\n')
  })

  it('should throw when reading CDC from non-existent table', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'nonexistent_table')

    const reader = table.getCDCReader()
    await expect(reader.readByVersion(0n, 0n)).rejects.toThrow()
  })
})

// =============================================================================
// CDC STREAMING/SUBSCRIPTION TESTS
// =============================================================================

describe('CDC Streaming and Subscription', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should subscribe to CDC changes', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const reader = table.getCDCReader()
    const received: DeltaCDCRecord<TestRow>[] = []

    const unsubscribe = reader.subscribe(async record => {
      received.push(record)
    })

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await table.write([{ id: '2', name: 'Bob', value: 200 }])

    // Give time for async subscription
    await new Promise(resolve => setTimeout(resolve, 100))

    expect(received.length).toBe(2)

    unsubscribe()
  })

  it('should unsubscribe from CDC changes', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
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

    // Should only have received the first change
    expect(received.length).toBe(1)
    expect(received[0].data.id).toBe('1')
  })

  it('should support multiple subscribers', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    const reader = table.getCDCReader()
    const received1: DeltaCDCRecord<TestRow>[] = []
    const received2: DeltaCDCRecord<TestRow>[] = []

    const unsub1 = reader.subscribe(async record => {
      received1.push(record)
    })
    const unsub2 = reader.subscribe(async record => {
      received2.push(record)
    })

    await table.write([{ id: '1', name: 'Alice', value: 100 }])
    await new Promise(resolve => setTimeout(resolve, 100))

    expect(received1.length).toBe(1)
    expect(received2.length).toBe(1)

    unsub1()
    unsub2()
  })
})

// =============================================================================
// CDC RETENTION AND CLEANUP TESTS
// =============================================================================

describe('CDC Retention and Cleanup', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should retain CDC files according to retention policy', async () => {
    // This test would require mocking time or using a test-friendly implementation
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const files = await storage.list('test_table/_change_data/')
    expect(files.length).toBeGreaterThan(0)
  })

  it('should not delete CDC files before retention period', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // CDC files should still exist immediately after write
    const files = await storage.list('test_table/_change_data/')
    expect(files.length).toBeGreaterThan(0)
  })
})

// =============================================================================
// INTEGRATION WITH EXISTING CDC PRIMITIVES TESTS
// =============================================================================

describe('Integration with Existing CDC Primitives', () => {
  it('should produce CDCRecord compatible with CDCConsumer', async () => {
    const producer = new CDCProducer<TestRow>({
      source: { database: 'test', collection: 'events' },
    })

    const consumer = new CDCConsumer<TestRow>()
    const received: CDCRecord<TestRow>[] = []

    consumer.subscribe(async record => {
      received.push(record)
    })

    const record = await producer.create('1', { id: '1', name: 'Alice', value: 100 })
    await consumer.process(record)

    expect(received.length).toBe(1)
    expect(received[0]._op).toBe('c')
    expect(received[0]._after?.id).toBe('1')
  })

  it('should convert Delta CDC to standard CDCRecord format', async () => {
    const storage = new MemoryStorage()
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
    await table.setCDCEnabled(true)

    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    const reader = table.getCDCReader()
    const deltaCDC = await reader.readByVersion(0n, 0n)

    // Delta CDC record should be convertible to standard CDC record
    // This would require a conversion function
    expect(deltaCDC[0]._change_type).toBe('insert')
    // Conversion: 'insert' -> 'c', 'delete' -> 'd', 'update_postimage' -> 'u'
  })
})
