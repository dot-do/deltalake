/**
 * Variant Shredding Integration Tests
 *
 * TDD RED Phase: Tests for VARIANT column shredding functionality.
 * Shredding extracts specific fields from VARIANT columns into typed Parquet
 * columns, enabling predicate pushdown and statistics-based row group skipping.
 *
 * See: https://parquet.apache.org/docs/file-format/types/variantshredding/
 *
 * These tests verify:
 * - createShreddedVariantColumn() for various field types
 * - Statistics generation on shredded fields (min/max)
 * - mapFilterPathToStats() for query optimization
 * - Schema structure with typed_value columns
 * - Integration with zone maps for row group skipping
 */

import { describe, it, expect } from 'vitest'
import {
  getStatisticsPaths,
  mapFilterPathToStats,
  canSkipZoneMap,
  type ZoneMap,
  type ZoneMapFilter,
} from '../../../src/parquet/index.js'

// =============================================================================
// MOCK TYPES FOR SHREDDING (to be imported from hyparquet-writer)
// =============================================================================

/**
 * Schema element from hyparquet
 */
interface SchemaElement {
  name: string
  type?: string
  repetition_type?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED'
  num_children?: number
  converted_type?: string
  logical_type?: { type: string }
}

/**
 * Result from createShreddedVariantColumn
 */
interface ShreddedColumnResult {
  schema: SchemaElement[]
  columnData: Map<string, unknown[]>
  shredPaths: string[]
}

// =============================================================================
// createShreddedVariantColumn BASIC USAGE
// =============================================================================

describe('createShreddedVariantColumn - Basic Usage', () => {
  describe('schema_generation', () => {
    it('should generate correct VARIANT schema structure with shredded fields', async () => {
      // Import from hyparquet-writer
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { name: 'Alice', age: 30, active: true },
        { name: 'Bob', age: 25, active: false },
        { name: 'Charlie', age: 35, active: true },
      ]

      const result = createShreddedVariantColumn('$index', values, ['name', 'age'])

      // Root should be VARIANT group with 3 children
      expect(result.schema[0]).toMatchObject({
        name: '$index',
        repetition_type: 'OPTIONAL',
        num_children: 3,
        logical_type: { type: 'VARIANT' },
      })

      // Children: metadata, value, typed_value
      expect(result.schema[1].name).toBe('metadata')
      expect(result.schema[1].type).toBe('BYTE_ARRAY')
      expect(result.schema[1].repetition_type).toBe('REQUIRED')

      expect(result.schema[2].name).toBe('value')
      expect(result.schema[2].type).toBe('BYTE_ARRAY')
      expect(result.schema[2].repetition_type).toBe('OPTIONAL')

      expect(result.schema[3].name).toBe('typed_value')
      expect(result.schema[3].repetition_type).toBe('OPTIONAL')
      expect(result.schema[3].num_children).toBe(2) // name, age
    })

    it('should create typed_value group for each shredded field', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { category: 'A', price: 100, quantity: 5 },
        { category: 'B', price: 200, quantity: 10 },
      ]

      const result = createShreddedVariantColumn('data', values, ['category', 'price', 'quantity'])

      // typed_value should have 3 children
      const typedValueGroup = result.schema.find(s => s.name === 'typed_value')
      expect(typedValueGroup).toBeDefined()
      expect(typedValueGroup!.num_children).toBe(3)

      // Each field should have value + typed_value subcolumns
      const fieldNames = result.schema
        .filter(s => ['category', 'price', 'quantity'].includes(s.name))
        .map(s => s.name)
      expect(fieldNames).toContain('category')
      expect(fieldNames).toContain('price')
      expect(fieldNames).toContain('quantity')
    })

    it('should include shred paths in result', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ field1: 'a', field2: 'b' }]
      const result = createShreddedVariantColumn('col', values, ['field1', 'field2'])

      expect(result.shredPaths).toEqual([
        'col.typed_value.field1.typed_value',
        'col.typed_value.field2.typed_value',
      ])
    })
  })

  describe('nullable_handling', () => {
    it('should default to nullable VARIANT column', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ x: 1 }]
      const result = createShreddedVariantColumn('col', values, ['x'])

      expect(result.schema[0].repetition_type).toBe('OPTIONAL')
    })

    it('should support non-nullable VARIANT column', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ x: 1 }]
      const result = createShreddedVariantColumn('col', values, ['x'], { nullable: false })

      expect(result.schema[0].repetition_type).toBe('REQUIRED')
    })
  })
})

// =============================================================================
// TYPE DETECTION AND INFERENCE
// =============================================================================

describe('createShreddedVariantColumn - Type Detection', () => {
  describe('integer_types', () => {
    it('should detect INT32 for small integers', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { count: 0 },
        { count: 100 },
        { count: -50 },
        { count: 2147483647 }, // max INT32
      ]

      const result = createShreddedVariantColumn('data', values, ['count'])

      // Find the typed_value column for 'count'
      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'INT32'
      )
      expect(typedValueSchema).toBeDefined()
    })

    it('should detect INT64 for large integers', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { bigNum: 2147483648 }, // exceeds INT32
        { bigNum: 9007199254740991 }, // MAX_SAFE_INTEGER
      ]

      const result = createShreddedVariantColumn('data', values, ['bigNum'])

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValueSchema).toBeDefined()
    })

    it('should detect INT64 for bigint values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { id: BigInt('9223372036854775807') },
        { id: BigInt('-9223372036854775808') },
      ]

      const result = createShreddedVariantColumn('data', values, ['id'])

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValueSchema).toBeDefined()
    })
  })

  describe('floating_point_types', () => {
    it('should detect DOUBLE for floating point numbers', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { price: 19.99 },
        { price: 0.001 },
        { price: -123.456 },
      ]

      const result = createShreddedVariantColumn('data', values, ['price'])

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'DOUBLE'
      )
      expect(typedValueSchema).toBeDefined()
    })
  })

  describe('string_types', () => {
    it('should detect UTF8 STRING for string values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { name: 'Alice' },
        { name: 'Bob' },
        { name: '' }, // empty string
      ]

      const result = createShreddedVariantColumn('data', values, ['name'])

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValueSchema).toBeDefined()
      expect(typedValueSchema!.converted_type).toBe('UTF8')
    })
  })

  describe('boolean_types', () => {
    it('should detect BOOLEAN for boolean values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { active: true },
        { active: false },
      ]

      const result = createShreddedVariantColumn('data', values, ['active'])

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'BOOLEAN'
      )
      expect(typedValueSchema).toBeDefined()
    })
  })

  describe('timestamp_types', () => {
    it('should detect TIMESTAMP_MILLIS for Date values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { created: new Date('2024-01-15T10:00:00Z') },
        { created: new Date('2024-06-20T15:30:00Z') },
      ]

      const result = createShreddedVariantColumn('data', values, ['created'])

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'INT64'
      )
      expect(typedValueSchema).toBeDefined()
      expect(typedValueSchema!.converted_type).toBe('TIMESTAMP_MILLIS')
    })
  })

  describe('mixed_types', () => {
    it('should fall back to STRING for mixed types in same field', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { value: 42 },      // number
        { value: 'hello' }, // string
        { value: true },    // boolean
      ]

      const result = createShreddedVariantColumn('data', values, ['value'])

      // Mixed types should fall back to UTF8 string
      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValueSchema).toBeDefined()
      expect(typedValueSchema!.converted_type).toBe('UTF8')
    })
  })

  describe('type_overrides', () => {
    it('should respect fieldTypes override for explicit typing', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { value: 42 },
        { value: 100 },
      ]

      const result = createShreddedVariantColumn('data', values, ['value'], {
        fieldTypes: { value: 'STRING' },
      })

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.type === 'BYTE_ARRAY'
      )
      expect(typedValueSchema).toBeDefined()
      expect(typedValueSchema!.converted_type).toBe('UTF8')
    })

    it('should support INT32, INT64, FLOAT, DOUBLE type overrides', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ a: 1, b: 2, c: 3.0, d: 4.0 }]

      const result = createShreddedVariantColumn('data', values, ['a', 'b', 'c', 'd'], {
        fieldTypes: {
          a: 'INT32',
          b: 'INT64',
          c: 'FLOAT',
          d: 'DOUBLE',
        },
      })

      const findType = (name: string) =>
        result.schema.filter(s => s.name === 'typed_value' && result.schema.indexOf(s) > 0)

      expect(result.schema.some(s => s.type === 'INT32')).toBe(true)
      expect(result.schema.some(s => s.type === 'INT64')).toBe(true)
      expect(result.schema.some(s => s.type === 'FLOAT')).toBe(true)
      expect(result.schema.some(s => s.type === 'DOUBLE')).toBe(true)
    })

    it('should support TIMESTAMP type override', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ ts: 1704067200000 }] // epoch millis

      const result = createShreddedVariantColumn('data', values, ['ts'], {
        fieldTypes: { ts: 'TIMESTAMP' },
      })

      const typedValueSchema = result.schema.find(
        s => s.name === 'typed_value' && s.converted_type === 'TIMESTAMP_MILLIS'
      )
      expect(typedValueSchema).toBeDefined()
    })
  })
})

// =============================================================================
// COLUMN DATA EXTRACTION
// =============================================================================

describe('createShreddedVariantColumn - Column Data Extraction', () => {
  describe('metadata_column', () => {
    it('should populate metadata column with dictionary for all values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]

      const result = createShreddedVariantColumn('data', values, ['name', 'age'])

      const metadata = result.columnData.get('data.metadata')
      expect(metadata).toBeDefined()
      expect(metadata).toHaveLength(2)

      // Each metadata should be a Uint8Array
      expect(metadata![0]).toBeInstanceOf(Uint8Array)
      expect(metadata![1]).toBeInstanceOf(Uint8Array)
    })
  })

  describe('value_column', () => {
    it('should be null when ALL fields are shredded', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { x: 1, y: 2 },
        { x: 3, y: 4 },
      ]

      // Shred ALL fields
      const result = createShreddedVariantColumn('data', values, ['x', 'y'])

      const valueColumn = result.columnData.get('data.value')
      expect(valueColumn).toBeDefined()
      // All values should be null since everything is shredded
      expect(valueColumn!.every(v => v === null)).toBe(true)
    })

    it('should contain non-shredded fields when partial shredding', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { x: 1, y: 2, extra: 'keep' },
        { x: 3, y: 4, extra: 'this' },
      ]

      // Only shred x and y, keep 'extra'
      const result = createShreddedVariantColumn('data', values, ['x', 'y'])

      const valueColumn = result.columnData.get('data.value')
      expect(valueColumn).toBeDefined()
      // Value column should contain encoded VARIANT for 'extra' field
      expect(valueColumn![0]).not.toBeNull()
      expect(valueColumn![0]).toBeInstanceOf(Uint8Array)
    })
  })

  describe('typed_value_columns', () => {
    it('should extract typed values for each shredded field', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { count: 10, label: 'first' },
        { count: 20, label: 'second' },
        { count: 30, label: 'third' },
      ]

      const result = createShreddedVariantColumn('data', values, ['count', 'label'])

      const countColumn = result.columnData.get('data.typed_value.count.typed_value')
      expect(countColumn).toEqual([10, 20, 30])

      const labelColumn = result.columnData.get('data.typed_value.label.typed_value')
      expect(labelColumn).toEqual(['first', 'second', 'third'])
    })

    it('should handle null values in typed columns', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { score: 100 },
        { score: null },
        { score: undefined },
        { score: 200 },
      ]

      const result = createShreddedVariantColumn('data', values, ['score'])

      const scoreColumn = result.columnData.get('data.typed_value.score.typed_value')
      expect(scoreColumn).toEqual([100, null, null, 200])
    })

    it('should handle missing fields as null', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { a: 1, b: 2 },
        { a: 3 },       // missing 'b'
        { b: 4 },       // missing 'a'
      ]

      const result = createShreddedVariantColumn('data', values, ['a', 'b'])

      const aColumn = result.columnData.get('data.typed_value.a.typed_value')
      expect(aColumn).toEqual([1, 3, null])

      const bColumn = result.columnData.get('data.typed_value.b.typed_value')
      expect(bColumn).toEqual([2, null, 4])
    })

    it('should convert Date to BigInt millis in typed column', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const date = new Date('2024-06-15T12:00:00Z')
      const values = [{ ts: date }]

      const result = createShreddedVariantColumn('data', values, ['ts'])

      const tsColumn = result.columnData.get('data.typed_value.ts.typed_value')
      expect(tsColumn![0]).toBe(BigInt(date.getTime()))
    })
  })

  describe('null_row_handling', () => {
    it('should handle null/undefined row values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { x: 1 },
        null,
        undefined,
        { x: 2 },
      ]

      const result = createShreddedVariantColumn('data', values as any[], ['x'])

      const xColumn = result.columnData.get('data.typed_value.x.typed_value')
      expect(xColumn).toEqual([1, null, null, 2])
    })
  })
})

// =============================================================================
// STATISTICS GENERATION
// =============================================================================

describe('Variant Shredding - Statistics Generation', () => {
  describe('getStatisticsPaths', () => {
    it('should return correct paths for statistics collection', () => {
      const paths = getStatisticsPaths('$index', ['titleType', 'startYear', 'isAdult'])

      expect(paths).toEqual([
        '$index.typed_value.titleType.typed_value',
        '$index.typed_value.startYear.typed_value',
        '$index.typed_value.isAdult.typed_value',
      ])
    })

    it('should handle single field', () => {
      const paths = getStatisticsPaths('data', ['id'])

      expect(paths).toEqual(['data.typed_value.id.typed_value'])
    })

    it('should handle empty shred fields', () => {
      const paths = getStatisticsPaths('data', [])

      expect(paths).toEqual([])
    })
  })

  describe('min_max_for_int32', () => {
    it('should enable min/max statistics on INT32 shredded columns', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { year: 1990 },
        { year: 2020 },
        { year: 1985 },
        { year: 2024 },
      ]

      const result = createShreddedVariantColumn('data', values, ['year'])

      const yearColumn = result.columnData.get('data.typed_value.year.typed_value')
      expect(yearColumn).toBeDefined()

      // Statistics can be computed from column values
      const min = Math.min(...(yearColumn as number[]))
      const max = Math.max(...(yearColumn as number[]))

      expect(min).toBe(1985)
      expect(max).toBe(2024)
    })
  })

  describe('min_max_for_string', () => {
    it('should enable min/max statistics on STRING shredded columns', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { category: 'Drama' },
        { category: 'Action' },
        { category: 'Comedy' },
        { category: 'Thriller' },
      ]

      const result = createShreddedVariantColumn('data', values, ['category'])

      const categoryColumn = result.columnData.get('data.typed_value.category.typed_value')
      const sorted = [...(categoryColumn as string[])].sort()

      expect(sorted[0]).toBe('Action')  // min
      expect(sorted[sorted.length - 1]).toBe('Thriller')  // max
    })
  })

  describe('min_max_for_timestamp', () => {
    it('should enable min/max statistics on TIMESTAMP shredded columns', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { created: new Date('2024-03-15') },
        { created: new Date('2024-01-01') },
        { created: new Date('2024-06-30') },
      ]

      const result = createShreddedVariantColumn('data', values, ['created'])

      const createdColumn = result.columnData.get('data.typed_value.created.typed_value')
      expect(createdColumn).toBeDefined()

      // Values should be BigInt timestamps
      const timestamps = createdColumn as bigint[]
      const min = timestamps.reduce((a, b) => (a < b ? a : b))
      const max = timestamps.reduce((a, b) => (a > b ? a : b))

      expect(min).toBe(BigInt(new Date('2024-01-01').getTime()))
      expect(max).toBe(BigInt(new Date('2024-06-30').getTime()))
    })
  })

  describe('statistics_with_nulls', () => {
    it('should compute min/max excluding null values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { score: 100 },
        { score: null },
        { score: 50 },
        { score: undefined },
        { score: 200 },
      ]

      const result = createShreddedVariantColumn('data', values, ['score'])

      const scoreColumn = result.columnData.get('data.typed_value.score.typed_value')
      const nonNullValues = (scoreColumn as (number | null)[]).filter(v => v !== null) as number[]

      expect(Math.min(...nonNullValues)).toBe(50)
      expect(Math.max(...nonNullValues)).toBe(200)
    })
  })
})

// =============================================================================
// mapFilterPathToStats - QUERY OPTIMIZATION
// =============================================================================

describe('mapFilterPathToStats - Query Optimization', () => {
  describe('basic_mapping', () => {
    it('should map filter path to statistics path for shredded field', () => {
      const statsPath = mapFilterPathToStats('$index.titleType', '$index', ['titleType', 'startYear'])

      expect(statsPath).toBe('$index.typed_value.titleType.typed_value')
    })

    it('should return null for non-shredded fields', () => {
      const statsPath = mapFilterPathToStats('$index.genres', '$index', ['titleType', 'startYear'])

      expect(statsPath).toBeNull()
    })

    it('should return null for paths not starting with column name', () => {
      const statsPath = mapFilterPathToStats('other.field', '$index', ['titleType'])

      expect(statsPath).toBeNull()
    })
  })

  describe('nested_path_handling', () => {
    it('should handle simple field paths', () => {
      const statsPath = mapFilterPathToStats('data.id', 'data', ['id', 'name'])

      expect(statsPath).toBe('data.typed_value.id.typed_value')
    })

    it('should extract first field from nested paths', () => {
      // Filter on 'data.metadata.source' where only 'metadata' is shredded
      const statsPath = mapFilterPathToStats('data.metadata', 'data', ['metadata'])

      expect(statsPath).toBe('data.typed_value.metadata.typed_value')
    })
  })

  describe('edge_cases', () => {
    it('should handle empty shred fields', () => {
      const statsPath = mapFilterPathToStats('data.field', 'data', [])

      expect(statsPath).toBeNull()
    })

    it('should handle exact column name match (no field)', () => {
      const statsPath = mapFilterPathToStats('data', 'data', ['field'])

      expect(statsPath).toBeNull()
    })

    it('should be case-sensitive', () => {
      const statsPath = mapFilterPathToStats('data.Field', 'data', ['field'])

      expect(statsPath).toBeNull()
    })
  })
})

// =============================================================================
// ZONE MAP INTEGRATION
// =============================================================================

describe('Variant Shredding - Zone Map Integration', () => {
  describe('zone_map_skipping_with_shredded_int', () => {
    it('should skip row group when filter value outside shredded INT range', () => {
      // Shredded column has min=1990, max=2020
      const zoneMap: ZoneMap = {
        column: '$index.typed_value.startYear.typed_value',
        min: 1990,
        max: 2020,
        nullCount: 0,
      }

      // Looking for year = 2025 (outside range)
      const filter: ZoneMapFilter = {
        column: '$index.typed_value.startYear.typed_value',
        operator: 'eq',
        value: 2025,
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should not skip when filter value within shredded INT range', () => {
      const zoneMap: ZoneMap = {
        column: '$index.typed_value.startYear.typed_value',
        min: 1990,
        max: 2020,
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: '$index.typed_value.startYear.typed_value',
        operator: 'eq',
        value: 2000,
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
    })
  })

  describe('zone_map_skipping_with_shredded_string', () => {
    it('should skip row group when filter string outside shredded range', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.category.typed_value',
        min: 'Action',
        max: 'Drama',
        nullCount: 0,
      }

      // Looking for 'Thriller' (after 'Drama' alphabetically)
      const filter: ZoneMapFilter = {
        column: 'data.typed_value.category.typed_value',
        operator: 'eq',
        value: 'Thriller',
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should not skip when filter string within shredded range', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.category.typed_value',
        min: 'Action',
        max: 'Drama',
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: 'data.typed_value.category.typed_value',
        operator: 'eq',
        value: 'Comedy',
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
    })
  })

  describe('zone_map_range_queries', () => {
    it('should skip for gt when all values below threshold', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.score.typed_value',
        min: 10,
        max: 50,
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: 'data.typed_value.score.typed_value',
        operator: 'gt',
        value: 100,  // Looking for score > 100, but max is 50
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should skip for lt when all values above threshold', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.score.typed_value',
        min: 100,
        max: 200,
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: 'data.typed_value.score.typed_value',
        operator: 'lt',
        value: 50,  // Looking for score < 50, but min is 100
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should skip for between when range does not overlap', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.year.typed_value',
        min: 2000,
        max: 2010,
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: 'data.typed_value.year.typed_value',
        operator: 'between',
        value: 2020,
        value2: 2025,
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })
  })

  describe('zone_map_in_queries', () => {
    it('should skip when all IN values outside range', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.category.typed_value',
        min: 'Action',
        max: 'Drama',
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: 'data.typed_value.category.typed_value',
        operator: 'in',
        value: ['Thriller', 'Western', 'Zombie'],  // All after 'Drama'
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should not skip when any IN value within range', () => {
      const zoneMap: ZoneMap = {
        column: 'data.typed_value.category.typed_value',
        min: 'Action',
        max: 'Drama',
        nullCount: 0,
      }

      const filter: ZoneMapFilter = {
        column: 'data.typed_value.category.typed_value',
        operator: 'in',
        value: ['Thriller', 'Comedy', 'Western'],  // 'Comedy' is in range
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
    })
  })
})

// =============================================================================
// FILTER PATH TO STATS PATH INTEGRATION
// =============================================================================

describe('Variant Shredding - End-to-End Filter Optimization', () => {
  describe('user_filter_to_zone_map_skip', () => {
    it('should transform user filter to zone map check for row group skipping', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      // Create shredded column
      const values = [
        { titleType: 'movie', startYear: 2015 },
        { titleType: 'series', startYear: 2018 },
        { titleType: 'movie', startYear: 2020 },
      ]

      const result = createShreddedVariantColumn('$index', values, ['titleType', 'startYear'])

      // User wants to filter: $index.startYear > 2022
      const userFilterPath = '$index.startYear'
      const statsPath = mapFilterPathToStats(userFilterPath, '$index', ['titleType', 'startYear'])

      expect(statsPath).toBe('$index.typed_value.startYear.typed_value')

      // Build zone map from shredded column data
      const yearValues = result.columnData.get('$index.typed_value.startYear.typed_value') as number[]
      const zoneMap: ZoneMap = {
        column: statsPath!,
        min: Math.min(...yearValues),
        max: Math.max(...yearValues),
        nullCount: 0,
      }

      // Check if we can skip this row group
      const filter: ZoneMapFilter = {
        column: statsPath!,
        operator: 'gt',
        value: 2022,
      }

      // Should skip because max(2020) <= 2022
      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should not skip when filter matches shredded data range', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [
        { titleType: 'movie', startYear: 2015 },
        { titleType: 'series', startYear: 2018 },
        { titleType: 'movie', startYear: 2020 },
      ]

      const result = createShreddedVariantColumn('$index', values, ['titleType', 'startYear'])

      const statsPath = mapFilterPathToStats('$index.startYear', '$index', ['titleType', 'startYear'])

      const yearValues = result.columnData.get('$index.typed_value.startYear.typed_value') as number[]
      const zoneMap: ZoneMap = {
        column: statsPath!,
        min: Math.min(...yearValues),
        max: Math.max(...yearValues),
        nullCount: 0,
      }

      // Looking for year = 2018 (in range)
      const filter: ZoneMapFilter = {
        column: statsPath!,
        operator: 'eq',
        value: 2018,
      }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
    })
  })

  describe('non_shredded_field_fallback', () => {
    it('should return null for non-shredded field, requiring full scan', () => {
      // 'genres' is not shredded
      const statsPath = mapFilterPathToStats('$index.genres', '$index', ['titleType', 'startYear'])

      expect(statsPath).toBeNull()
      // Null means we cannot use zone maps for this filter - must scan
    })
  })
})

// =============================================================================
// SCHEMA STRUCTURE VALIDATION
// =============================================================================

describe('Variant Shredding - Schema Structure', () => {
  describe('schema_hierarchy', () => {
    it('should produce correct nested schema hierarchy', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ field1: 'a', field2: 123 }]
      const result = createShreddedVariantColumn('col', values, ['field1', 'field2'])

      // Expected structure:
      // col (VARIANT, 3 children)
      //   metadata (BYTE_ARRAY, REQUIRED)
      //   value (BYTE_ARRAY, OPTIONAL)
      //   typed_value (group, 2 children)
      //     field1 (group, 2 children)
      //       value (BYTE_ARRAY, OPTIONAL)
      //       typed_value (BYTE_ARRAY/UTF8, OPTIONAL)
      //     field2 (group, 2 children)
      //       value (BYTE_ARRAY, OPTIONAL)
      //       typed_value (INT32, OPTIONAL)

      const schema = result.schema

      // Root
      expect(schema[0]).toMatchObject({ name: 'col', num_children: 3 })

      // metadata
      expect(schema[1]).toMatchObject({ name: 'metadata', type: 'BYTE_ARRAY' })

      // value
      expect(schema[2]).toMatchObject({ name: 'value', type: 'BYTE_ARRAY' })

      // typed_value group
      expect(schema[3]).toMatchObject({ name: 'typed_value', num_children: 2 })

      // field1 group
      expect(schema[4]).toMatchObject({ name: 'field1', num_children: 2 })

      // field1.value
      expect(schema[5]).toMatchObject({ name: 'value', type: 'BYTE_ARRAY' })

      // field1.typed_value (STRING)
      expect(schema[6]).toMatchObject({
        name: 'typed_value',
        type: 'BYTE_ARRAY',
        converted_type: 'UTF8',
      })

      // field2 group
      expect(schema[7]).toMatchObject({ name: 'field2', num_children: 2 })

      // field2.value
      expect(schema[8]).toMatchObject({ name: 'value', type: 'BYTE_ARRAY' })

      // field2.typed_value (INT32)
      expect(schema[9]).toMatchObject({ name: 'typed_value', type: 'INT32' })
    })
  })

  describe('column_paths', () => {
    it('should produce all required column data paths', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ a: 1, b: 'x' }]
      const result = createShreddedVariantColumn('data', values, ['a', 'b'])

      const paths = Array.from(result.columnData.keys())

      expect(paths).toContain('data.metadata')
      expect(paths).toContain('data.value')
      expect(paths).toContain('data.typed_value.a.value')
      expect(paths).toContain('data.typed_value.a.typed_value')
      expect(paths).toContain('data.typed_value.b.value')
      expect(paths).toContain('data.typed_value.b.typed_value')
    })

    it('should set field.value columns to all nulls (typed values used)', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ x: 1 }, { x: 2 }, { x: 3 }]
      const result = createShreddedVariantColumn('data', values, ['x'])

      const fieldValueColumn = result.columnData.get('data.typed_value.x.value')
      expect(fieldValueColumn).toBeDefined()
      expect(fieldValueColumn!.every(v => v === null)).toBe(true)
    })
  })
})

// =============================================================================
// MULTIPLE SHREDDED COLUMNS
// =============================================================================

describe('Variant Shredding - Multiple Columns', () => {
  describe('multiple_variant_columns_same_writer', () => {
    it('should support shredding multiple VARIANT columns independently', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      // Column 1: document metadata
      const metaValues = [
        { source: 'api', version: 1 },
        { source: 'web', version: 2 },
      ]
      const metaResult = createShreddedVariantColumn('_meta', metaValues, ['source', 'version'])

      // Column 2: index data
      const indexValues = [
        { titleType: 'movie', year: 2020 },
        { titleType: 'series', year: 2021 },
      ]
      const indexResult = createShreddedVariantColumn('$index', indexValues, ['titleType', 'year'])

      // Each should have independent schemas and paths
      expect(metaResult.shredPaths).toEqual([
        '_meta.typed_value.source.typed_value',
        '_meta.typed_value.version.typed_value',
      ])

      expect(indexResult.shredPaths).toEqual([
        '$index.typed_value.titleType.typed_value',
        '$index.typed_value.year.typed_value',
      ])
    })
  })
})

// =============================================================================
// ERROR HANDLING
// =============================================================================

describe('Variant Shredding - Error Handling', () => {
  describe('empty_values', () => {
    it('should handle empty values array', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const result = createShreddedVariantColumn('data', [], ['field'])

      expect(result.schema).toBeDefined()
      expect(result.columnData.get('data.typed_value.field.typed_value')).toEqual([])
    })
  })

  describe('empty_shred_fields', () => {
    it('should handle empty shred fields array', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ a: 1, b: 2 }]
      const result = createShreddedVariantColumn('data', values, [])

      // typed_value group should have 0 children
      const typedValueGroup = result.schema.find(s => s.name === 'typed_value')
      expect(typedValueGroup?.num_children).toBe(0)
    })
  })

  describe('non_existent_shred_fields', () => {
    it('should handle shred fields that do not exist in values', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = [{ a: 1 }, { a: 2 }]
      // 'nonexistent' field doesn't exist in any value
      const result = createShreddedVariantColumn('data', values, ['a', 'nonexistent'])

      const nonExistentColumn = result.columnData.get('data.typed_value.nonexistent.typed_value')
      expect(nonExistentColumn).toBeDefined()
      // All values should be null since field doesn't exist
      expect(nonExistentColumn!.every(v => v === null)).toBe(true)
    })
  })
})

// =============================================================================
// LARGE DATASET HANDLING
// =============================================================================

describe('Variant Shredding - Large Datasets', () => {
  describe('many_rows', () => {
    it('should efficiently shred 10,000 rows', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const values = Array.from({ length: 10000 }, (_, i) => ({
        id: i,
        category: `cat_${i % 100}`,
        score: Math.random() * 100,
      }))

      const startTime = Date.now()
      const result = createShreddedVariantColumn('data', values, ['id', 'category', 'score'])
      const elapsed = Date.now() - startTime

      expect(result.columnData.get('data.typed_value.id.typed_value')).toHaveLength(10000)
      expect(result.columnData.get('data.typed_value.category.typed_value')).toHaveLength(10000)
      expect(result.columnData.get('data.typed_value.score.typed_value')).toHaveLength(10000)

      // Should complete in reasonable time (< 1 second)
      expect(elapsed).toBeLessThan(1000)
    })
  })

  describe('many_shredded_fields', () => {
    it('should handle shredding many fields from each row', async () => {
      const { createShreddedVariantColumn } = await import('@dotdo/hyparquet-writer')

      const fields = Array.from({ length: 50 }, (_, i) => `field${i}`)
      const values = [
        Object.fromEntries(fields.map((f, i) => [f, i])),
        Object.fromEntries(fields.map((f, i) => [f, i + 100])),
      ]

      const result = createShreddedVariantColumn('data', values, fields)

      // Should have typed_value group with 50 children
      const typedValueGroup = result.schema.find(s => s.name === 'typed_value')
      expect(typedValueGroup?.num_children).toBe(50)

      // Each field should have its column data
      for (const field of fields) {
        const columnPath = `data.typed_value.${field}.typed_value`
        expect(result.columnData.has(columnPath)).toBe(true)
      }
    })
  })
})
