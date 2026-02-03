/**
 * Partition Support Tests
 *
 * Tests for partitioned table writes and partition pruning on reads.
 *
 * Key scenarios:
 * - Partitioned writes create files in correct paths
 * - Partition values are stored in AddAction
 * - Partition pruning skips non-matching files
 * - Multiple partition columns work correctly
 * - Mixed partitioned and non-partitioned queries
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { DeltaTable, type AddAction, type DeltaCommit, parseStats, type WriteOptions } from '../../../src/delta/index.js'
import { MemoryStorage } from '../../../src/storage/index.js'

// =============================================================================
// TEST TYPES
// =============================================================================

interface PartitionedRecord {
  id: number
  name: string
  year: number
  month: number
  category: string
  value: number
}

interface SimplePartitionedRecord {
  id: number
  category: string
  value: number
}

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Helper to get the AddActions from a commit
 */
function getAddActions(commit: DeltaCommit): AddAction[] {
  return commit.actions.filter((a): a is AddAction => 'add' in a)
}

/**
 * Create sample partitioned data
 */
function createPartitionedData(): PartitionedRecord[] {
  return [
    { id: 1, name: 'Alice', year: 2024, month: 1, category: 'A', value: 100 },
    { id: 2, name: 'Bob', year: 2024, month: 1, category: 'B', value: 200 },
    { id: 3, name: 'Charlie', year: 2024, month: 2, category: 'A', value: 150 },
    { id: 4, name: 'Diana', year: 2024, month: 2, category: 'B', value: 250 },
    { id: 5, name: 'Eve', year: 2025, month: 1, category: 'A', value: 175 },
    { id: 6, name: 'Frank', year: 2025, month: 1, category: 'B', value: 225 },
  ]
}

// =============================================================================
// PARTITIONED WRITE TESTS
// =============================================================================

describe('DeltaTable - Partition Support', () => {
  let storage: MemoryStorage
  let table: DeltaTable<PartitionedRecord>

  beforeEach(() => {
    storage = new MemoryStorage()
    table = new DeltaTable(storage, 'test-table')
  })

  describe('Partitioned Writes', () => {
    it('should write data to partitioned paths', async () => {
      const data = createPartitionedData()

      const commit = await table.write(data, { partitionColumns: ['year'] })
      const addActions = getAddActions(commit)

      // Should create separate files for each year partition
      expect(addActions.length).toBeGreaterThanOrEqual(2)

      // Check that paths include partition values
      const paths = addActions.map(a => a.add.path)
      expect(paths.some(p => p.includes('year=2024'))).toBe(true)
      expect(paths.some(p => p.includes('year=2025'))).toBe(true)
    })

    it('should store partition values in AddAction', async () => {
      const data = createPartitionedData()

      const commit = await table.write(data, { partitionColumns: ['year', 'month'] })
      const addActions = getAddActions(commit)

      // Each add action should have partition values
      for (const action of addActions) {
        expect(action.add.partitionValues).toBeDefined()
        expect(action.add.partitionValues!.year).toBeDefined()
        expect(action.add.partitionValues!.month).toBeDefined()
      }
    })

    it('should create correct partition directory structure', async () => {
      const data = createPartitionedData()

      const commit = await table.write(data, { partitionColumns: ['year', 'month'] })
      const addActions = getAddActions(commit)

      // Check that paths have nested partition structure
      const paths = addActions.map(a => a.add.path)

      // Should have paths like: table/year=2024/month=1/part-*.parquet
      expect(paths.some(p => p.includes('year=2024/month=1'))).toBe(true)
      expect(paths.some(p => p.includes('year=2024/month=2'))).toBe(true)
      expect(paths.some(p => p.includes('year=2025/month=1'))).toBe(true)
    })

    it('should store partitionColumns in metadata', async () => {
      const data = createPartitionedData()

      await table.write(data, { partitionColumns: ['year', 'month'] })
      const snapshot = await table.snapshot()

      expect(snapshot.metadata).toBeDefined()
      expect(snapshot.metadata!.partitionColumns).toEqual(['year', 'month'])
    })

    it('should group rows correctly by partition values', async () => {
      const data = createPartitionedData()

      const commit = await table.write(data, { partitionColumns: ['year'] })
      const addActions = getAddActions(commit)

      // Count rows in each partition by checking stats
      let year2024Count = 0
      let year2025Count = 0

      for (const action of addActions) {
        const stats = parseStats(action.add.stats!)
        if (action.add.partitionValues?.year === '2024') {
          year2024Count += stats.numRecords
        } else if (action.add.partitionValues?.year === '2025') {
          year2025Count += stats.numRecords
        }
      }

      // 4 records for 2024, 2 records for 2025
      expect(year2024Count).toBe(4)
      expect(year2025Count).toBe(2)
    })

    it('should handle null/undefined partition values', async () => {
      const data: Array<{ id: number; category: string | null }> = [
        { id: 1, category: 'A' },
        { id: 2, category: null },
        { id: 3, category: 'B' },
      ]

      const nullableTable = new DeltaTable<{ id: number; category: string | null }>(storage, 'nullable-table')
      const commit = await nullableTable.write(data, { partitionColumns: ['category'] })
      const addActions = getAddActions(commit)

      // Should have partition for null values
      const paths = addActions.map(a => a.add.path)
      expect(paths.some(p => p.includes('__HIVE_DEFAULT_PARTITION__'))).toBe(true)
    })

    it('should encode special characters in partition values', async () => {
      const data: Array<{ id: number; category: string }> = [
        { id: 1, category: 'with space' },
        { id: 2, category: 'with/slash' },
        { id: 3, category: 'normal' },
      ]

      const specialTable = new DeltaTable<{ id: number; category: string }>(storage, 'special-table')
      const commit = await specialTable.write(data, { partitionColumns: ['category'] })
      const addActions = getAddActions(commit)

      // Paths should be URL-encoded
      const paths = addActions.map(a => a.add.path)
      expect(paths.some(p => p.includes('%20') || p.includes('with%20space'))).toBe(true)
      expect(paths.some(p => p.includes('%2F') || p.includes('with%2Fslash'))).toBe(true)
    })
  })

  describe('Partition Pruning on Reads', () => {
    beforeEach(async () => {
      // Write partitioned data
      const data = createPartitionedData()
      await table.write(data, { partitionColumns: ['year', 'month'] })
    })

    it('should prune partitions on equality filter', async () => {
      // Query only year=2024
      const results = await table.query({ year: 2024 })

      expect(results.length).toBe(4) // 4 records for 2024
      expect(results.every(r => r.year === 2024)).toBe(true)

      // Should have skipped year=2025 partition
      expect(table.lastQuerySkippedFiles).toBeGreaterThan(0)
    })

    it('should prune partitions on $eq operator', async () => {
      const results = await table.query({ year: { $eq: 2025 } })

      expect(results.length).toBe(2) // 2 records for 2025
      expect(results.every(r => r.year === 2025)).toBe(true)
    })

    it('should prune partitions on $in operator', async () => {
      const results = await table.query({ month: { $in: [1] } })

      // Records with month=1: id 1, 2, 5, 6
      expect(results.length).toBe(4)
      expect(results.every(r => r.month === 1)).toBe(true)
    })

    it('should prune with multiple partition columns', async () => {
      const results = await table.query({ year: 2024, month: 1 })

      // Records with year=2024 AND month=1: id 1, 2
      expect(results.length).toBe(2)
      expect(results.every(r => r.year === 2024 && r.month === 1)).toBe(true)
    })

    it('should not prune on non-partition columns', async () => {
      // Filter on category which is not a partition column
      const results = await table.query({ category: 'A' })

      // Records with category=A: id 1, 3, 5
      expect(results.length).toBe(3)
      expect(results.every(r => r.category === 'A')).toBe(true)

      // No partition pruning should occur
      expect(table.lastQuerySkippedFiles).toBe(0)
    })

    it('should handle mixed partition and non-partition filters', async () => {
      // year is partition column, category is not
      const results = await table.query({ year: 2024, category: 'A' })

      // Records with year=2024 AND category=A: id 1, 3
      expect(results.length).toBe(2)
      expect(results.every(r => r.year === 2024 && r.category === 'A')).toBe(true)

      // Should have pruned year=2025 partition
      expect(table.lastQuerySkippedFiles).toBeGreaterThan(0)
    })

    it('should return all rows when no filter provided', async () => {
      const results = await table.query()

      expect(results.length).toBe(6) // All records
      expect(table.lastQuerySkippedFiles).toBe(0)
    })

    it('should return all rows when empty filter provided', async () => {
      const results = await table.query({})

      expect(results.length).toBe(6) // All records
    })
  })

  describe('Multiple Writes with Partitions', () => {
    it('should handle multiple writes to same partition', async () => {
      // First write
      const data1: PartitionedRecord[] = [
        { id: 1, name: 'Alice', year: 2024, month: 1, category: 'A', value: 100 },
      ]
      await table.write(data1, { partitionColumns: ['year'] })

      // Second write to same partition
      const data2: PartitionedRecord[] = [
        { id: 2, name: 'Bob', year: 2024, month: 2, category: 'B', value: 200 },
      ]
      await table.write(data2)

      // Query all year=2024
      const results = await table.query({ year: 2024 })

      expect(results.length).toBe(2)
    })

    it('should handle writes to different partitions', async () => {
      // First write
      const data1: PartitionedRecord[] = [
        { id: 1, name: 'Alice', year: 2024, month: 1, category: 'A', value: 100 },
      ]
      await table.write(data1, { partitionColumns: ['year'] })

      // Second write to different partition
      const data2: PartitionedRecord[] = [
        { id: 2, name: 'Bob', year: 2025, month: 1, category: 'B', value: 200 },
      ]
      await table.write(data2)

      // Query all data
      const allResults = await table.query()
      expect(allResults.length).toBe(2)

      // Query specific partition
      const results2024 = await table.query({ year: 2024 })
      expect(results2024.length).toBe(1)

      const results2025 = await table.query({ year: 2025 })
      expect(results2025.length).toBe(1)
    })
  })

  describe('Edge Cases', () => {
    it('should handle single partition value', async () => {
      const data: SimplePartitionedRecord[] = [
        { id: 1, category: 'A', value: 100 },
        { id: 2, category: 'A', value: 200 },
        { id: 3, category: 'A', value: 300 },
      ]

      const simpleTable = new DeltaTable<SimplePartitionedRecord>(storage, 'simple-table')
      const commit = await simpleTable.write(data, { partitionColumns: ['category'] })
      const addActions = getAddActions(commit)

      // Should have only one partition file
      expect(addActions.length).toBe(1)
      expect(addActions[0].add.partitionValues?.category).toBe('A')
    })

    it('should handle empty partition columns array', async () => {
      const data = createPartitionedData()

      const commit = await table.write(data, { partitionColumns: [] })
      const addActions = getAddActions(commit)

      // Should write to single non-partitioned file
      expect(addActions.length).toBe(1)
      expect(addActions[0].add.partitionValues).toBeUndefined()
    })

    it('should handle numeric partition values', async () => {
      const data = createPartitionedData()

      const commit = await table.write(data, { partitionColumns: ['year'] })
      const addActions = getAddActions(commit)

      // Partition values should be strings
      for (const action of addActions) {
        expect(typeof action.add.partitionValues?.year).toBe('string')
      }
    })

    it('should prune correctly with string partition values queried as numbers', async () => {
      const data = createPartitionedData()
      await table.write(data, { partitionColumns: ['year'] })

      // Query with number (partition values are stored as strings)
      const results = await table.query({ year: 2024 })

      // Should still work because we convert filter values to strings
      expect(results.length).toBe(4)
    })
  })

  describe('Partition Columns Persistence', () => {
    it('should load partitionColumns from metadata when opening existing table', async () => {
      // First, write partitioned data to create the table
      const data = createPartitionedData()
      await table.write(data, { partitionColumns: ['year', 'month'] })

      // Verify metadata was written correctly
      const snapshot = await table.snapshot()
      expect(snapshot.metadata?.partitionColumns).toEqual(['year', 'month'])

      // Create a new DeltaTable instance pointing to the same storage/path
      // This simulates "reopening" an existing table
      const table2 = new DeltaTable<PartitionedRecord>(storage, 'test-table')

      // Write new data without specifying partitionColumns
      // The table should automatically use the persisted partition columns
      const newData: PartitionedRecord[] = [
        { id: 7, name: 'Grace', year: 2025, month: 2, category: 'A', value: 300 },
        { id: 8, name: 'Henry', year: 2025, month: 2, category: 'B', value: 350 },
      ]

      const commit2 = await table2.write(newData)
      const addActions = commit2.actions.filter((a): a is AddAction => 'add' in a)

      // The new files should be written to partitioned paths
      const paths = addActions.map(a => a.add.path)
      expect(paths.some(p => p.includes('year=2025/month=2'))).toBe(true)

      // Verify partition values are stored in the add actions
      for (const action of addActions) {
        expect(action.add.partitionValues).toBeDefined()
        expect(action.add.partitionValues!.year).toBe('2025')
        expect(action.add.partitionValues!.month).toBe('2')
      }
    })

    it('should support partition pruning after reopening table', async () => {
      // Write initial partitioned data
      const data = createPartitionedData()
      await table.write(data, { partitionColumns: ['year'] })

      // Create a new DeltaTable instance (simulating reopening)
      const table2 = new DeltaTable<PartitionedRecord>(storage, 'test-table')

      // Query with partition filter - should use partition pruning
      const results = await table2.query({ year: 2024 })

      // Should return only 2024 data
      expect(results.length).toBe(4)
      expect(results.every(r => r.year === 2024)).toBe(true)

      // Verify partition pruning happened by checking that files were skipped
      // (2025 partition files should have been skipped)
      expect(table2.lastQuerySkippedFiles).toBeGreaterThan(0)
    })
  })
})
