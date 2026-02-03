/**
 * Delta Lake Add/Remove Action Tests
 *
 * [RED] TDD Phase - These tests define the expected behavior for Add and Remove actions.
 * Tests should fail initially until implementation is complete.
 */

import { describe, it, expect } from 'vitest'
import type { AddAction, RemoveAction, DeltaAction } from '../../../src/delta/index.js'
import {
  createAddAction,
  createRemoveAction,
  isAddAction,
  isRemoveAction,
  serializeAction,
  deserializeAction,
  validateAddAction,
  validateRemoveAction,
  parseStats,
  encodeStats,
  type FileStats,
} from '../../../src/delta/index.js'

// =============================================================================
// TYPE DEFINITIONS FOR EXTENDED ACTIONS (to be implemented)
// =============================================================================

/**
 * Extended Add action with full Delta Lake spec support
 */
interface ExtendedAddAction {
  add: {
    path: string
    size: number
    modificationTime: number
    dataChange: boolean
    partitionValues?: Record<string, string>
    stats?: string // JSON encoded FileStats
    tags?: Record<string, string>
  }
}

/**
 * Extended Remove action with full Delta Lake spec support
 */
interface ExtendedRemoveAction {
  remove: {
    path: string
    deletionTimestamp: number
    dataChange: boolean
    partitionValues?: Record<string, string>
    extendedFileMetadata?: boolean
    size?: number
  }
}

// =============================================================================
// ADD ACTION TESTS
// =============================================================================

describe('AddAction', () => {
  describe('creation with required fields', () => {
    it('should create AddAction with path, size, modificationTime, and dataChange', () => {
      const action = createAddAction({
        path: 'part-00000-abc123.parquet',
        size: 12345,
        modificationTime: 1706745600000,
        dataChange: true,
      })

      expect(action.add.path).toBe('part-00000-abc123.parquet')
      expect(action.add.size).toBe(12345)
      expect(action.add.modificationTime).toBe(1706745600000)
      expect(action.add.dataChange).toBe(true)
    })

    it('should create AddAction with dataChange=false for metadata-only changes', () => {
      const action = createAddAction({
        path: 'part-00000-def456.parquet',
        size: 5678,
        modificationTime: 1706745700000,
        dataChange: false,
      })

      expect(action.add.dataChange).toBe(false)
    })

    it('should handle zero-size files', () => {
      const action = createAddAction({
        path: 'part-00000-empty.parquet',
        size: 0,
        modificationTime: 1706745800000,
        dataChange: true,
      })

      expect(action.add.size).toBe(0)
    })
  })

  describe('path to parquet file', () => {
    it('should accept relative paths within table directory', () => {
      const action = createAddAction({
        path: 'part-00000-abc123.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.path).toBe('part-00000-abc123.parquet')
    })

    it('should accept nested partition paths', () => {
      const action = createAddAction({
        path: 'year=2024/month=01/part-00000-abc123.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.path).toBe('year=2024/month=01/part-00000-abc123.parquet')
    })

    it('should handle URL-encoded paths with special characters', () => {
      const action = createAddAction({
        path: 'category=A%26B/part-00000-abc123.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.path).toBe('category=A%26B/part-00000-abc123.parquet')
    })

    it('should handle paths with unicode characters', () => {
      const action = createAddAction({
        path: 'region=%E6%97%A5%E6%9C%AC/part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.path).toContain('region=')
    })
  })

  describe('partitionValues map', () => {
    it('should create AddAction with single partition value', () => {
      const action = createAddAction({
        path: 'year=2024/part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        partitionValues: { year: '2024' },
      })

      expect(action.add.partitionValues).toEqual({ year: '2024' })
    })

    it('should create AddAction with multiple partition values', () => {
      const action = createAddAction({
        path: 'year=2024/month=01/day=15/part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        partitionValues: { year: '2024', month: '01', day: '15' },
      })

      expect(action.add.partitionValues).toEqual({
        year: '2024',
        month: '01',
        day: '15',
      })
    })

    it('should handle null partition values as empty string', () => {
      const action = createAddAction({
        path: 'status=__HIVE_DEFAULT_PARTITION__/part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        partitionValues: { status: '' },
      })

      expect(action.add.partitionValues).toEqual({ status: '' })
    })

    it('should handle partition values with special characters', () => {
      const action = createAddAction({
        path: 'category=A%26B/part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        partitionValues: { category: 'A&B' },
      })

      expect(action.add.partitionValues?.category).toBe('A&B')
    })

    it('should support empty partitionValues for non-partitioned tables', () => {
      const action = createAddAction({
        path: 'part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        partitionValues: {},
      })

      expect(action.add.partitionValues).toEqual({})
    })
  })

  describe('size in bytes', () => {
    it('should accept small file sizes', () => {
      const action = createAddAction({
        path: 'small.parquet',
        size: 100,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.size).toBe(100)
    })

    it('should accept large file sizes (multi-GB)', () => {
      const action = createAddAction({
        path: 'large.parquet',
        size: 5_000_000_000, // 5 GB
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.size).toBe(5_000_000_000)
    })

    it('should handle maximum safe integer sizes', () => {
      const action = createAddAction({
        path: 'huge.parquet',
        size: Number.MAX_SAFE_INTEGER,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.size).toBe(Number.MAX_SAFE_INTEGER)
    })
  })

  describe('modificationTime', () => {
    it('should store modification time as Unix timestamp in milliseconds', () => {
      const timestamp = 1706745600000 // 2024-02-01 00:00:00 UTC
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: timestamp,
        dataChange: true,
      })

      expect(action.add.modificationTime).toBe(timestamp)
    })

    it('should handle timestamps at epoch', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: 0,
        dataChange: true,
      })

      expect(action.add.modificationTime).toBe(0)
    })

    it('should handle future timestamps', () => {
      const futureTimestamp = Date.now() + 365 * 24 * 60 * 60 * 1000 // 1 year from now
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: futureTimestamp,
        dataChange: true,
      })

      expect(action.add.modificationTime).toBe(futureTimestamp)
    })
  })

  describe('dataChange flag', () => {
    it('should set dataChange=true for data-modifying operations', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(action.add.dataChange).toBe(true)
    })

    it('should set dataChange=false for compaction operations', () => {
      const action = createAddAction({
        path: 'compacted.parquet',
        size: 5000,
        modificationTime: Date.now(),
        dataChange: false,
      })

      expect(action.add.dataChange).toBe(false)
    })

    it('should set dataChange=false for file reorganization', () => {
      const action = createAddAction({
        path: 'reorganized.parquet',
        size: 3000,
        modificationTime: Date.now(),
        dataChange: false,
      })

      expect(action.add.dataChange).toBe(false)
    })
  })

  describe('stats (file statistics)', () => {
    it('should create AddAction with numRecords stat', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        stats: {
          numRecords: 1000,
          minValues: {},
          maxValues: {},
          nullCount: {},
        },
      })

      const stats = parseStats(action.add.stats!)
      expect(stats.numRecords).toBe(1000)
    })

    it('should include minValues for columns', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        stats: {
          numRecords: 100,
          minValues: { id: 1, name: 'Alice', timestamp: 1706745600000 },
          maxValues: {},
          nullCount: {},
        },
      })

      const stats = parseStats(action.add.stats!)
      expect(stats.minValues).toEqual({ id: 1, name: 'Alice', timestamp: 1706745600000 })
    })

    it('should include maxValues for columns', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        stats: {
          numRecords: 100,
          minValues: {},
          maxValues: { id: 1000, name: 'Zoe', timestamp: 1706832000000 },
          nullCount: {},
        },
      })

      const stats = parseStats(action.add.stats!)
      expect(stats.maxValues).toEqual({ id: 1000, name: 'Zoe', timestamp: 1706832000000 })
    })

    it('should include nullCount for columns', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        stats: {
          numRecords: 100,
          minValues: {},
          maxValues: {},
          nullCount: { id: 0, name: 5, optional_field: 50 },
        },
      })

      const stats = parseStats(action.add.stats!)
      expect(stats.nullCount).toEqual({ id: 0, name: 5, optional_field: 50 })
    })

    it('should store stats as JSON string', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        stats: {
          numRecords: 50,
          minValues: { value: 10 },
          maxValues: { value: 100 },
          nullCount: { value: 2 },
        },
      })

      expect(typeof action.add.stats).toBe('string')
      const parsed = JSON.parse(action.add.stats!)
      expect(parsed.numRecords).toBe(50)
    })

    it('should handle stats with nested column values', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        stats: {
          numRecords: 100,
          minValues: { 'nested.field': 1 },
          maxValues: { 'nested.field': 100 },
          nullCount: { 'nested.field': 5 },
        },
      })

      const stats = parseStats(action.add.stats!)
      expect(stats.minValues['nested.field']).toBe(1)
    })
  })

  describe('tags for custom metadata', () => {
    it('should create AddAction with custom tags', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        tags: { source: 'kafka', topic: 'events' },
      })

      expect((action as ExtendedAddAction).add.tags).toEqual({
        source: 'kafka',
        topic: 'events',
      })
    })

    it('should support empty tags', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        tags: {},
      })

      expect((action as ExtendedAddAction).add.tags).toEqual({})
    })

    it('should handle tags with special characters in values', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        tags: { query: 'SELECT * FROM table WHERE id > 10' },
      })

      expect((action as ExtendedAddAction).add.tags?.query).toBe(
        'SELECT * FROM table WHERE id > 10'
      )
    })

    it('should preserve tag insertion order', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        tags: { a: '1', b: '2', c: '3' },
      })

      const keys = Object.keys((action as ExtendedAddAction).add.tags || {})
      expect(keys).toEqual(['a', 'b', 'c'])
    })
  })
})

// =============================================================================
// REMOVE ACTION TESTS
// =============================================================================

describe('RemoveAction', () => {
  describe('creation with required fields', () => {
    it('should create RemoveAction with path, deletionTimestamp, and dataChange', () => {
      const action = createRemoveAction({
        path: 'part-00000-abc123.parquet',
        deletionTimestamp: 1706745600000,
        dataChange: true,
      })

      expect(action.remove.path).toBe('part-00000-abc123.parquet')
      expect(action.remove.deletionTimestamp).toBe(1706745600000)
      expect(action.remove.dataChange).toBe(true)
    })
  })

  describe('path to removed file', () => {
    it('should reference the same path as the original AddAction', () => {
      const addAction = createAddAction({
        path: 'year=2024/part-00000.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      const removeAction = createRemoveAction({
        path: addAction.add.path,
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect(removeAction.remove.path).toBe(addAction.add.path)
    })

    it('should handle URL-encoded paths', () => {
      const action = createRemoveAction({
        path: 'category=A%26B/part-00000.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect(action.remove.path).toBe('category=A%26B/part-00000.parquet')
    })

    it('should handle nested partition paths', () => {
      const action = createRemoveAction({
        path: 'year=2024/month=01/day=15/part-00000.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect(action.remove.path).toContain('year=2024/month=01/day=15/')
    })
  })

  describe('deletionTimestamp', () => {
    it('should store deletion time as Unix timestamp in milliseconds', () => {
      const timestamp = 1706745600000
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: timestamp,
        dataChange: true,
      })

      expect(action.remove.deletionTimestamp).toBe(timestamp)
    })

    it('should handle current timestamp', () => {
      const now = Date.now()
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: now,
        dataChange: true,
      })

      expect(action.remove.deletionTimestamp).toBeGreaterThanOrEqual(now)
    })
  })

  describe('dataChange flag', () => {
    it('should set dataChange=true for DELETE operations', () => {
      const action = createRemoveAction({
        path: 'deleted.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect(action.remove.dataChange).toBe(true)
    })

    it('should set dataChange=false for compaction (data preserved)', () => {
      const action = createRemoveAction({
        path: 'old-file.parquet',
        deletionTimestamp: Date.now(),
        dataChange: false,
      })

      expect(action.remove.dataChange).toBe(false)
    })

    it('should set dataChange=false for VACUUM operations', () => {
      const action = createRemoveAction({
        path: 'expired.parquet',
        deletionTimestamp: Date.now(),
        dataChange: false,
      })

      expect(action.remove.dataChange).toBe(false)
    })
  })

  describe('extendedFileMetadata (optional)', () => {
    it('should support extendedFileMetadata flag', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
        extendedFileMetadata: true,
      })

      expect((action as ExtendedRemoveAction).remove.extendedFileMetadata).toBe(true)
    })

    it('should include size when extendedFileMetadata is true', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
        extendedFileMetadata: true,
        size: 12345,
      })

      expect((action as ExtendedRemoveAction).remove.size).toBe(12345)
    })

    it('should default extendedFileMetadata to undefined when not provided', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect((action as ExtendedRemoveAction).remove.extendedFileMetadata).toBeUndefined()
    })
  })

  describe('partitionValues', () => {
    it('should store partition values for removed file', () => {
      const action = createRemoveAction({
        path: 'year=2024/part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
        partitionValues: { year: '2024' },
      })

      expect((action as ExtendedRemoveAction).remove.partitionValues).toEqual({ year: '2024' })
    })

    it('should support multiple partition values', () => {
      const action = createRemoveAction({
        path: 'year=2024/month=01/part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
        partitionValues: { year: '2024', month: '01' },
      })

      expect((action as ExtendedRemoveAction).remove.partitionValues).toEqual({
        year: '2024',
        month: '01',
      })
    })

    it('should handle null partition values', () => {
      const action = createRemoveAction({
        path: 'status=__HIVE_DEFAULT_PARTITION__/part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
        partitionValues: { status: '' },
      })

      expect((action as ExtendedRemoveAction).remove.partitionValues).toEqual({ status: '' })
    })
  })
})

// =============================================================================
// JSON SERIALIZATION/DESERIALIZATION TESTS
// =============================================================================

describe('JSON Serialization', () => {
  describe('AddAction serialization', () => {
    it('should serialize AddAction to single-line JSON', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: 1706745600000,
        dataChange: true,
      })

      const json = serializeAction(action)
      expect(json).not.toContain('\n')
      expect(json).toContain('"add"')
      expect(json).toContain('"path":"part.parquet"')
    })

    it('should serialize AddAction with stats as nested JSON string', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: 1706745600000,
        dataChange: true,
        stats: { numRecords: 100, minValues: {}, maxValues: {}, nullCount: {} },
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)
      expect(typeof parsed.add.stats).toBe('string')
    })

    it('should serialize AddAction with partition values', () => {
      const action = createAddAction({
        path: 'year=2024/part.parquet',
        size: 1000,
        modificationTime: 1706745600000,
        dataChange: true,
        partitionValues: { year: '2024' },
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)
      expect(parsed.add.partitionValues).toEqual({ year: '2024' })
    })

    it('should serialize AddAction with tags', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: 1706745600000,
        dataChange: true,
        tags: { source: 'test' },
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)
      expect(parsed.add.tags).toEqual({ source: 'test' })
    })
  })

  describe('RemoveAction serialization', () => {
    it('should serialize RemoveAction to single-line JSON', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: 1706745600000,
        dataChange: true,
      })

      const json = serializeAction(action)
      expect(json).not.toContain('\n')
      expect(json).toContain('"remove"')
      expect(json).toContain('"path":"part.parquet"')
    })

    it('should serialize RemoveAction with extendedFileMetadata', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: 1706745600000,
        dataChange: true,
        extendedFileMetadata: true,
        size: 5000,
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)
      expect(parsed.remove.extendedFileMetadata).toBe(true)
      expect(parsed.remove.size).toBe(5000)
    })

    it('should serialize RemoveAction with partition values', () => {
      const action = createRemoveAction({
        path: 'year=2024/part.parquet',
        deletionTimestamp: 1706745600000,
        dataChange: true,
        partitionValues: { year: '2024' },
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)
      expect(parsed.remove.partitionValues).toEqual({ year: '2024' })
    })
  })

  describe('AddAction deserialization', () => {
    it('should deserialize JSON string to AddAction', () => {
      const json = '{"add":{"path":"part.parquet","size":1000,"modificationTime":1706745600000,"dataChange":true}}'

      const action = deserializeAction(json) as AddAction
      expect(action.add.path).toBe('part.parquet')
      expect(action.add.size).toBe(1000)
      expect(action.add.modificationTime).toBe(1706745600000)
      expect(action.add.dataChange).toBe(true)
    })

    it('should deserialize AddAction with stats', () => {
      const statsJson = JSON.stringify({ numRecords: 100, minValues: {}, maxValues: {}, nullCount: {} })
      const json = `{"add":{"path":"part.parquet","size":1000,"modificationTime":1706745600000,"dataChange":true,"stats":${JSON.stringify(statsJson)}}}`

      const action = deserializeAction(json) as AddAction
      const stats = parseStats(action.add.stats!)
      expect(stats.numRecords).toBe(100)
    })

    it('should deserialize AddAction with partition values', () => {
      const json = '{"add":{"path":"year=2024/part.parquet","size":1000,"modificationTime":1706745600000,"dataChange":true,"partitionValues":{"year":"2024"}}}'

      const action = deserializeAction(json) as AddAction
      expect(action.add.partitionValues).toEqual({ year: '2024' })
    })
  })

  describe('RemoveAction deserialization', () => {
    it('should deserialize JSON string to RemoveAction', () => {
      const json = '{"remove":{"path":"part.parquet","deletionTimestamp":1706745600000,"dataChange":true}}'

      const action = deserializeAction(json) as RemoveAction
      expect(action.remove.path).toBe('part.parquet')
      expect(action.remove.deletionTimestamp).toBe(1706745600000)
      expect(action.remove.dataChange).toBe(true)
    })

    it('should deserialize RemoveAction with extended metadata', () => {
      const json = '{"remove":{"path":"part.parquet","deletionTimestamp":1706745600000,"dataChange":true,"extendedFileMetadata":true,"size":5000}}'

      const action = deserializeAction(json) as ExtendedRemoveAction
      expect(action.remove.extendedFileMetadata).toBe(true)
      expect(action.remove.size).toBe(5000)
    })
  })

  describe('round-trip serialization', () => {
    it('should preserve AddAction through serialization round-trip', () => {
      const original = createAddAction({
        path: 'year=2024/month=01/part.parquet',
        size: 12345,
        modificationTime: 1706745600000,
        dataChange: true,
        partitionValues: { year: '2024', month: '01' },
        stats: { numRecords: 500, minValues: { id: 1 }, maxValues: { id: 500 }, nullCount: { id: 0 } },
        tags: { source: 'test' },
      })

      const json = serializeAction(original)
      const restored = deserializeAction(json) as AddAction

      expect(restored.add.path).toBe(original.add.path)
      expect(restored.add.size).toBe(original.add.size)
      expect(restored.add.modificationTime).toBe(original.add.modificationTime)
      expect(restored.add.dataChange).toBe(original.add.dataChange)
      expect(restored.add.partitionValues).toEqual(original.add.partitionValues)
    })

    it('should preserve RemoveAction through serialization round-trip', () => {
      const original = createRemoveAction({
        path: 'year=2024/part.parquet',
        deletionTimestamp: 1706745600000,
        dataChange: true,
        partitionValues: { year: '2024' },
        extendedFileMetadata: true,
        size: 5000,
      })

      const json = serializeAction(original)
      const restored = deserializeAction(json) as ExtendedRemoveAction

      expect(restored.remove.path).toBe(original.remove.path)
      expect(restored.remove.deletionTimestamp).toBe((original as ExtendedRemoveAction).remove.deletionTimestamp)
      expect(restored.remove.dataChange).toBe(original.remove.dataChange)
    })
  })
})

// =============================================================================
// ACTION VALIDATION TESTS
// =============================================================================

describe('Action Validation', () => {
  describe('AddAction validation', () => {
    it('should fail validation when path is missing', () => {
      const action = { add: { size: 1000, modificationTime: Date.now(), dataChange: true } } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('path is required')
    })

    it('should fail validation when path is empty string', () => {
      const action = {
        add: { path: '', size: 1000, modificationTime: Date.now(), dataChange: true },
      } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('path cannot be empty')
    })

    it('should fail validation when size is negative', () => {
      const action = {
        add: { path: 'part.parquet', size: -100, modificationTime: Date.now(), dataChange: true },
      } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('size must be non-negative')
    })

    it('should fail validation when size is not a number', () => {
      const action = {
        add: { path: 'part.parquet', size: 'large' as unknown as number, modificationTime: Date.now(), dataChange: true },
      } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('size must be a number')
    })

    it('should fail validation when modificationTime is missing', () => {
      const action = { add: { path: 'part.parquet', size: 1000, dataChange: true } } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('modificationTime is required')
    })

    it('should fail validation when modificationTime is negative', () => {
      const action = {
        add: { path: 'part.parquet', size: 1000, modificationTime: -1, dataChange: true },
      } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('modificationTime must be non-negative')
    })

    it('should fail validation when dataChange is not a boolean', () => {
      const action = {
        add: { path: 'part.parquet', size: 1000, modificationTime: Date.now(), dataChange: 'yes' as unknown as boolean },
      } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('dataChange must be a boolean')
    })

    it('should fail validation when stats is not valid JSON', () => {
      const action = {
        add: { path: 'part.parquet', size: 1000, modificationTime: Date.now(), dataChange: true, stats: 'invalid{json' },
      } as AddAction

      const result = validateAddAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('stats must be valid JSON')
    })

    it('should pass validation for valid AddAction', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      const result = validateAddAction(action)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('should pass validation for valid AddAction with all optional fields', () => {
      const action = createAddAction({
        path: 'year=2024/part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
        partitionValues: { year: '2024' },
        stats: { numRecords: 100, minValues: {}, maxValues: {}, nullCount: {} },
        tags: { source: 'test' },
      })

      const result = validateAddAction(action)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })
  })

  describe('RemoveAction validation', () => {
    it('should fail validation when path is missing', () => {
      const action = { remove: { deletionTimestamp: Date.now(), dataChange: true } } as RemoveAction

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('path is required')
    })

    it('should fail validation when path is empty string', () => {
      const action = {
        remove: { path: '', deletionTimestamp: Date.now(), dataChange: true },
      } as RemoveAction

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('path cannot be empty')
    })

    it('should fail validation when deletionTimestamp is missing', () => {
      const action = { remove: { path: 'part.parquet', dataChange: true } } as RemoveAction

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('deletionTimestamp is required')
    })

    it('should fail validation when deletionTimestamp is negative', () => {
      const action = {
        remove: { path: 'part.parquet', deletionTimestamp: -1, dataChange: true },
      } as RemoveAction

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('deletionTimestamp must be non-negative')
    })

    it('should fail validation when dataChange is not a boolean', () => {
      const action = {
        remove: { path: 'part.parquet', deletionTimestamp: Date.now(), dataChange: 1 as unknown as boolean },
      } as RemoveAction

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(false)
      expect(result.errors).toContain('dataChange must be a boolean')
    })

    it('should pass validation for valid RemoveAction', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('should pass validation for valid RemoveAction with optional fields', () => {
      const action = createRemoveAction({
        path: 'year=2024/part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
        partitionValues: { year: '2024' },
        extendedFileMetadata: true,
        size: 5000,
      })

      const result = validateRemoveAction(action)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })
  })
})

// =============================================================================
// TYPE GUARD TESTS
// =============================================================================

describe('Type Guards', () => {
  describe('isAddAction', () => {
    it('should return true for AddAction', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(isAddAction(action)).toBe(true)
    })

    it('should return false for RemoveAction', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect(isAddAction(action)).toBe(false)
    })

    it('should return false for MetadataAction', () => {
      const action: DeltaAction = {
        metaData: {
          id: 'test-id',
          format: { provider: 'parquet' },
          schemaString: '{}',
          partitionColumns: [],
        },
      }

      expect(isAddAction(action)).toBe(false)
    })

    it('should return false for ProtocolAction', () => {
      const action: DeltaAction = {
        protocol: {
          minReaderVersion: 1,
          minWriterVersion: 1,
        },
      }

      expect(isAddAction(action)).toBe(false)
    })

    it('should return false for CommitInfoAction', () => {
      const action: DeltaAction = {
        commitInfo: {
          timestamp: Date.now(),
          operation: 'WRITE',
        },
      }

      expect(isAddAction(action)).toBe(false)
    })

    it('should handle null and undefined safely', () => {
      expect(isAddAction(null as unknown as DeltaAction)).toBe(false)
      expect(isAddAction(undefined as unknown as DeltaAction)).toBe(false)
    })

    it('should handle malformed objects', () => {
      expect(isAddAction({} as DeltaAction)).toBe(false)
      expect(isAddAction({ add: null } as unknown as DeltaAction)).toBe(false)
      expect(isAddAction({ add: 'not an object' } as unknown as DeltaAction)).toBe(false)
    })
  })

  describe('isRemoveAction', () => {
    it('should return true for RemoveAction', () => {
      const action = createRemoveAction({
        path: 'part.parquet',
        deletionTimestamp: Date.now(),
        dataChange: true,
      })

      expect(isRemoveAction(action)).toBe(true)
    })

    it('should return false for AddAction', () => {
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: Date.now(),
        dataChange: true,
      })

      expect(isRemoveAction(action)).toBe(false)
    })

    it('should return false for MetadataAction', () => {
      const action: DeltaAction = {
        metaData: {
          id: 'test-id',
          format: { provider: 'parquet' },
          schemaString: '{}',
          partitionColumns: [],
        },
      }

      expect(isRemoveAction(action)).toBe(false)
    })

    it('should return false for ProtocolAction', () => {
      const action: DeltaAction = {
        protocol: {
          minReaderVersion: 1,
          minWriterVersion: 1,
        },
      }

      expect(isRemoveAction(action)).toBe(false)
    })

    it('should return false for CommitInfoAction', () => {
      const action: DeltaAction = {
        commitInfo: {
          timestamp: Date.now(),
          operation: 'WRITE',
        },
      }

      expect(isRemoveAction(action)).toBe(false)
    })

    it('should handle null and undefined safely', () => {
      expect(isRemoveAction(null as unknown as DeltaAction)).toBe(false)
      expect(isRemoveAction(undefined as unknown as DeltaAction)).toBe(false)
    })

    it('should handle malformed objects', () => {
      expect(isRemoveAction({} as DeltaAction)).toBe(false)
      expect(isRemoveAction({ remove: null } as unknown as DeltaAction)).toBe(false)
      expect(isRemoveAction({ remove: 'not an object' } as unknown as DeltaAction)).toBe(false)
    })
  })

  describe('type narrowing', () => {
    it('should allow TypeScript type narrowing with isAddAction', () => {
      const actions: DeltaAction[] = [
        {
          add: {
            path: 'part.parquet',
            size: 1000,
            modificationTime: Date.now(),
            dataChange: true,
          },
        },
        {
          remove: {
            path: 'old.parquet',
            deletionTimestamp: Date.now(),
            dataChange: true,
          },
        },
      ]

      const addActions = actions.filter(isAddAction)
      expect(addActions).toHaveLength(1)
      // TypeScript should now know these are AddAction type
      expect(addActions[0].add.path).toBe('part.parquet')
    })

    it('should allow TypeScript type narrowing with isRemoveAction', () => {
      const actions: DeltaAction[] = [
        {
          add: {
            path: 'part.parquet',
            size: 1000,
            modificationTime: Date.now(),
            dataChange: true,
          },
        },
        {
          remove: {
            path: 'old.parquet',
            deletionTimestamp: Date.now(),
            dataChange: true,
          },
        },
      ]

      const removeActions = actions.filter(isRemoveAction)
      expect(removeActions).toHaveLength(1)
      // TypeScript should now know these are RemoveAction type
      expect(removeActions[0].remove.path).toBe('old.parquet')
    })
  })
})

// =============================================================================
// STATS PARSING TESTS
// =============================================================================

describe('Stats Parsing', () => {
  describe('parseStats', () => {
    it('should parse valid stats JSON', () => {
      const statsJson = JSON.stringify({
        numRecords: 100,
        minValues: { id: 1, name: 'Alice' },
        maxValues: { id: 100, name: 'Zoe' },
        nullCount: { id: 0, name: 5 },
      })

      const stats = parseStats(statsJson)
      expect(stats.numRecords).toBe(100)
      expect(stats.minValues).toEqual({ id: 1, name: 'Alice' })
      expect(stats.maxValues).toEqual({ id: 100, name: 'Zoe' })
      expect(stats.nullCount).toEqual({ id: 0, name: 5 })
    })

    it('should handle stats with numeric min/max values', () => {
      const statsJson = JSON.stringify({
        numRecords: 50,
        minValues: { price: 9.99, quantity: 0 },
        maxValues: { price: 999.99, quantity: 1000 },
        nullCount: { price: 0, quantity: 10 },
      })

      const stats = parseStats(statsJson)
      expect(stats.minValues.price).toBe(9.99)
      expect(stats.maxValues.quantity).toBe(1000)
    })

    it('should handle stats with timestamp values', () => {
      const statsJson = JSON.stringify({
        numRecords: 25,
        minValues: { created_at: 1706745600000 },
        maxValues: { created_at: 1706832000000 },
        nullCount: { created_at: 0 },
      })

      const stats = parseStats(statsJson)
      expect(stats.minValues.created_at).toBe(1706745600000)
    })

    it('should handle stats with boolean min/max values', () => {
      const statsJson = JSON.stringify({
        numRecords: 100,
        minValues: { active: false },
        maxValues: { active: true },
        nullCount: { active: 5 },
      })

      const stats = parseStats(statsJson)
      expect(stats.minValues.active).toBe(false)
      expect(stats.maxValues.active).toBe(true)
    })

    it('should handle empty stats', () => {
      const statsJson = JSON.stringify({
        numRecords: 0,
        minValues: {},
        maxValues: {},
        nullCount: {},
      })

      const stats = parseStats(statsJson)
      expect(stats.numRecords).toBe(0)
      expect(Object.keys(stats.minValues)).toHaveLength(0)
    })

    it('should throw error for invalid JSON', () => {
      expect(() => parseStats('invalid{json')).toThrow()
    })

    it('should throw error for missing numRecords', () => {
      const statsJson = JSON.stringify({
        minValues: {},
        maxValues: {},
        nullCount: {},
      })

      expect(() => parseStats(statsJson)).toThrow('Invalid file stats format')
    })
  })

  describe('encodeStats', () => {
    it('should encode stats to JSON string', () => {
      const stats: FileStats = {
        numRecords: 100,
        minValues: { id: 1 },
        maxValues: { id: 100 },
        nullCount: { id: 0 },
      }

      const json = encodeStats(stats)
      expect(typeof json).toBe('string')
      const parsed = JSON.parse(json)
      expect(parsed.numRecords).toBe(100)
    })

    it('should produce compact JSON without whitespace', () => {
      const stats: FileStats = {
        numRecords: 50,
        minValues: { a: 1, b: 2 },
        maxValues: { a: 10, b: 20 },
        nullCount: { a: 0, b: 1 },
      }

      const json = encodeStats(stats)
      expect(json).not.toContain('\n')
      expect(json).not.toContain('  ')
    })

    it('should handle special characters in string values', () => {
      const stats: FileStats = {
        numRecords: 10,
        minValues: { name: 'O\'Brien' },
        maxValues: { name: 'Zeta "Z"' },
        nullCount: { name: 0 },
      }

      const json = encodeStats(stats)
      const parsed = parseStats(json)
      expect(parsed.minValues.name).toBe('O\'Brien')
      expect(parsed.maxValues.name).toBe('Zeta "Z"')
    })
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING TESTS
// =============================================================================

describe('Edge Cases and Error Handling', () => {
  describe('path format validation', () => {
    it('should reject absolute paths', () => {
      expect(() =>
        createAddAction({
          path: '/absolute/path/part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
        })
      ).toThrow('path must be relative')
    })

    it('should reject paths with parent directory traversal', () => {
      expect(() =>
        createAddAction({
          path: '../escape/part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
        })
      ).toThrow('path cannot contain parent directory traversal')
    })

    it('should reject paths starting with ./', () => {
      expect(() =>
        createAddAction({
          path: './current/part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
        })
      ).toThrow('path should not start with ./')
    })
  })

  describe('partition value consistency', () => {
    it('should warn when partitionValues do not match path', () => {
      const result = validateAddAction({
        add: {
          path: 'year=2024/part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
          partitionValues: { year: '2025' }, // Mismatch!
        },
      })

      expect(result.valid).toBe(false)
      expect(result.errors.some((e) => e.includes('partition value mismatch'))).toBe(true)
    })
  })

  describe('stats consistency', () => {
    it('should validate numRecords is non-negative', () => {
      expect(() =>
        createAddAction({
          path: 'part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
          stats: {
            numRecords: -1,
            minValues: {},
            maxValues: {},
            nullCount: {},
          },
        })
      ).toThrow('numRecords must be non-negative')
    })

    it('should validate nullCount values are non-negative', () => {
      expect(() =>
        createAddAction({
          path: 'part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
          stats: {
            numRecords: 100,
            minValues: {},
            maxValues: {},
            nullCount: { id: -5 },
          },
        })
      ).toThrow('nullCount values must be non-negative')
    })

    it('should validate nullCount does not exceed numRecords', () => {
      expect(() =>
        createAddAction({
          path: 'part.parquet',
          size: 1000,
          modificationTime: Date.now(),
          dataChange: true,
          stats: {
            numRecords: 100,
            minValues: {},
            maxValues: {},
            nullCount: { id: 150 }, // Exceeds numRecords
          },
        })
      ).toThrow('nullCount cannot exceed numRecords')
    })
  })

  describe('timestamp validation', () => {
    it('should handle very large timestamps (year 3000+)', () => {
      const futureTimestamp = 32503680000000 // Year 3000
      const action = createAddAction({
        path: 'part.parquet',
        size: 1000,
        modificationTime: futureTimestamp,
        dataChange: true,
      })

      expect(action.add.modificationTime).toBe(futureTimestamp)
    })

    it('should reject non-integer timestamps', () => {
      expect(() =>
        createAddAction({
          path: 'part.parquet',
          size: 1000,
          modificationTime: 1706745600000.5,
          dataChange: true,
        })
      ).toThrow('modificationTime must be an integer')
    })
  })

  describe('size validation', () => {
    it('should reject non-integer sizes', () => {
      expect(() =>
        createAddAction({
          path: 'part.parquet',
          size: 1000.5,
          modificationTime: Date.now(),
          dataChange: true,
        })
      ).toThrow('size must be an integer')
    })

    it('should reject sizes exceeding MAX_SAFE_INTEGER', () => {
      expect(() =>
        createAddAction({
          path: 'part.parquet',
          size: Number.MAX_SAFE_INTEGER + 1,
          modificationTime: Date.now(),
          dataChange: true,
        })
      ).toThrow('size exceeds maximum safe integer')
    })
  })
})

// =============================================================================
// COMPATIBILITY TESTS
// =============================================================================

describe('Delta Lake Compatibility', () => {
  describe('Delta Lake spec compliance', () => {
    it('should produce JSON compatible with Delta Lake readers', () => {
      const action = createAddAction({
        path: 'part-00000-abc123.snappy.parquet',
        size: 12345,
        modificationTime: 1706745600000,
        dataChange: true,
        partitionValues: { date: '2024-02-01' },
        stats: {
          numRecords: 1000,
          minValues: { id: 1 },
          maxValues: { id: 1000 },
          nullCount: { id: 0 },
        },
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      // Delta Lake expects these exact field names
      expect(parsed).toHaveProperty('add')
      expect(parsed.add).toHaveProperty('path')
      expect(parsed.add).toHaveProperty('size')
      expect(parsed.add).toHaveProperty('modificationTime')
      expect(parsed.add).toHaveProperty('dataChange')
      expect(parsed.add).toHaveProperty('partitionValues')
      expect(parsed.add).toHaveProperty('stats')
    })

    it('should handle Delta Lake format for remove actions', () => {
      const action = createRemoveAction({
        path: 'part-00000-abc123.snappy.parquet',
        deletionTimestamp: 1706745600000,
        dataChange: true,
      })

      const json = serializeAction(action)
      const parsed = JSON.parse(json)

      expect(parsed).toHaveProperty('remove')
      expect(parsed.remove).toHaveProperty('path')
      expect(parsed.remove).toHaveProperty('deletionTimestamp')
      expect(parsed.remove).toHaveProperty('dataChange')
    })
  })

  describe('interoperability with other Delta Lake implementations', () => {
    it('should parse AddAction from delta-rs format', () => {
      // Example from delta-rs
      const deltaRsJson = '{"add":{"path":"part-00000-abc.parquet","partitionValues":{},"size":1234,"modificationTime":1706745600000,"dataChange":true,"stats":"{\\"numRecords\\":100}"}}'

      const action = deserializeAction(deltaRsJson) as AddAction
      expect(action.add.path).toBe('part-00000-abc.parquet')
      expect(action.add.size).toBe(1234)
    })

    it('should parse AddAction from Spark Delta format', () => {
      // Example from Spark Delta
      const sparkDeltaJson = '{"add":{"path":"part-00000-def.snappy.parquet","partitionValues":{"year":"2024"},"size":5678,"modificationTime":1706832000000,"dataChange":true,"stats":"{\\"numRecords\\":500,\\"minValues\\":{\\"id\\":1},\\"maxValues\\":{\\"id\\":500},\\"nullCount\\":{\\"id\\":0}}","tags":{"OPTIMIZE_TARGET_SIZE":"268435456"}}}'

      const action = deserializeAction(sparkDeltaJson) as ExtendedAddAction
      expect(action.add.path).toBe('part-00000-def.snappy.parquet')
      expect(action.add.partitionValues).toEqual({ year: '2024' })
      expect(action.add.tags?.OPTIMIZE_TARGET_SIZE).toBe('268435456')
    })

    it('should parse RemoveAction from delta-rs format', () => {
      const deltaRsJson = '{"remove":{"path":"part-00000-old.parquet","deletionTimestamp":1706745600000,"dataChange":true,"extendedFileMetadata":false}}'

      const action = deserializeAction(deltaRsJson) as ExtendedRemoveAction
      expect(action.remove.path).toBe('part-00000-old.parquet')
      expect(action.remove.extendedFileMetadata).toBe(false)
    })
  })
})
