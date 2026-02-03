/**
 * DeltaTable.query() Tests
 *
 * TDD RED Phase: Comprehensive tests for DeltaTable query functionality.
 *
 * These tests verify that query() reads Parquet files from the table snapshot
 * and applies MongoDB-style filters. The current implementation uses an in-memory
 * dataStore for testing; these tests define the expected behavior when reading
 * actual Parquet files.
 *
 * Key scenarios:
 * - Reading rows from Parquet files in snapshot
 * - Filter application using MongoDB-style operators ($eq, $gt, $in, etc.)
 * - Predicate pushdown via shredded columns (zone map filtering)
 * - Row group skipping when filter excludes entire group
 * - Variant column decoding
 * - Empty result when no matches
 * - Query across multiple Parquet files
 * - Projection support (select specific fields)
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  DeltaTable,
  type DeltaCommit,
  type DeltaSnapshot,
  type AddAction,
  type QueryOptions,
} from '../../../src/delta/index.js'
import { MemoryStorage, type StorageBackend } from '../../../src/storage/index.js'
import {
  type Filter,
  matchesFilter,
  filterToParquetPredicate,
} from '../../../src/query/index.js'
import {
  type ZoneMap,
  type ZoneMapFilter,
  canSkipZoneMap,
  mapFilterPathToStats,
  getStatisticsPaths,
} from '../../../src/parquet/index.js'

// =============================================================================
// TEST DATA TYPES
// =============================================================================

interface TestDocument {
  _id: string
  name: string
  age: number
  score: number
  active: boolean
  tags?: string[]
  metadata?: {
    source: string
    tier: string
  }
}

interface Product {
  _id: string
  name: string
  price: number
  category: string
  inStock: boolean
  quantity: number
  rating?: number
}

interface Event {
  _id: string
  type: string
  timestamp: number
  payload: Record<string, unknown>
}

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a test DeltaTable with memory storage
 */
function createTestTable<T extends Record<string, unknown> = TestDocument>(): {
  table: DeltaTable<T>
  storage: MemoryStorage
} {
  const storage = new MemoryStorage()
  const table = new DeltaTable<T>(storage, 'test-table')
  return { table, storage }
}

/**
 * Sample test documents
 */
const sampleDocuments: TestDocument[] = [
  { _id: '1', name: 'Alice', age: 30, score: 85.5, active: true, tags: ['premium', 'verified'], metadata: { source: 'web', tier: 'gold' } },
  { _id: '2', name: 'Bob', age: 25, score: 72.0, active: true, tags: ['basic'], metadata: { source: 'mobile', tier: 'silver' } },
  { _id: '3', name: 'Charlie', age: 35, score: 91.2, active: false, tags: ['premium', 'admin'], metadata: { source: 'api', tier: 'platinum' } },
  { _id: '4', name: 'Diana', age: 28, score: 65.8, active: true, tags: [] },
  { _id: '5', name: 'Eve', age: 42, score: 88.0, active: false, tags: ['premium', 'verified', 'vip'], metadata: { source: 'web', tier: 'gold' } },
]

const sampleProducts: Product[] = [
  { _id: 'p1', name: 'Laptop', price: 999.99, category: 'electronics', inStock: true, quantity: 50, rating: 4.5 },
  { _id: 'p2', name: 'Headphones', price: 149.99, category: 'electronics', inStock: true, quantity: 200, rating: 4.2 },
  { _id: 'p3', name: 'Desk Chair', price: 299.99, category: 'furniture', inStock: false, quantity: 0, rating: 4.8 },
  { _id: 'p4', name: 'Monitor', price: 449.99, category: 'electronics', inStock: true, quantity: 30 },
  { _id: 'p5', name: 'Keyboard', price: 79.99, category: 'electronics', inStock: true, quantity: 150, rating: 4.0 },
]

// =============================================================================
// BASIC QUERY FUNCTIONALITY
// =============================================================================

describe('DeltaTable.query() - Basic Functionality', () => {
  describe('Query without filters', () => {
    it('should return all rows when no filter is provided', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query()

      expect(results).toHaveLength(sampleDocuments.length)
    })

    it('should return empty array for empty table', async () => {
      const { table } = createTestTable<TestDocument>()

      const results = await table.query()

      expect(results).toHaveLength(0)
    })

    it('should return all rows when filter is empty object', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query({})

      expect(results).toHaveLength(sampleDocuments.length)
    })

    it('should return rows from multiple writes (commits)', async () => {
      const { table } = createTestTable<TestDocument>()

      // Write in batches
      await table.write(sampleDocuments.slice(0, 2))
      await table.write(sampleDocuments.slice(2, 4))
      await table.write(sampleDocuments.slice(4))

      const results = await table.query()

      expect(results).toHaveLength(sampleDocuments.length)
    })
  })

  describe('Query with equality filters', () => {
    it('should filter by exact string value', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query({ name: 'Alice' })

      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Alice')
    })

    it('should filter by exact numeric value', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query({ age: 30 })

      expect(results).toHaveLength(1)
      expect(results[0].age).toBe(30)
    })

    it('should filter by boolean value', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query({ active: true })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.active === true)).toBe(true)
    })

    it('should filter by $eq operator', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query({ name: { $eq: 'Bob' } })

      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Bob')
    })

    it('should filter by multiple equality conditions (implicit AND)', async () => {
      const { table } = createTestTable<TestDocument>()
      await table.write(sampleDocuments)

      const results = await table.query({ active: true, age: 25 })

      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Bob')
    })
  })
})

// =============================================================================
// COMPARISON OPERATOR FILTERS
// =============================================================================

describe('DeltaTable.query() - Comparison Operators', () => {
  let table: DeltaTable<TestDocument>

  beforeEach(async () => {
    const setup = createTestTable<TestDocument>()
    table = setup.table
    await table.write(sampleDocuments)
  })

  describe('$ne - Not Equal', () => {
    it('should filter out matching values', async () => {
      const results = await table.query({ name: { $ne: 'Alice' } })

      expect(results.length).toBe(sampleDocuments.length - 1)
      expect(results.every(r => r.name !== 'Alice')).toBe(true)
    })

    it('should return all when value does not exist', async () => {
      const results = await table.query({ name: { $ne: 'NonExistent' } })

      expect(results).toHaveLength(sampleDocuments.length)
    })
  })

  describe('$gt - Greater Than', () => {
    it('should filter numeric values greater than threshold', async () => {
      const results = await table.query({ age: { $gt: 30 } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.age > 30)).toBe(true)
    })

    it('should not include equal values', async () => {
      const results = await table.query({ age: { $gt: 30 } })

      expect(results.some(r => r.age === 30)).toBe(false)
    })

    it('should filter floating point values', async () => {
      const results = await table.query({ score: { $gt: 85.0 } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.score > 85.0)).toBe(true)
    })
  })

  describe('$gte - Greater Than or Equal', () => {
    it('should include equal values', async () => {
      const results = await table.query({ age: { $gte: 30 } })

      expect(results.some(r => r.age === 30)).toBe(true)
      expect(results.every(r => r.age >= 30)).toBe(true)
    })

    it('should filter floating point values', async () => {
      const results = await table.query({ score: { $gte: 85.5 } })

      expect(results.some(r => r.score === 85.5)).toBe(true)
      expect(results.every(r => r.score >= 85.5)).toBe(true)
    })
  })

  describe('$lt - Less Than', () => {
    it('should filter numeric values less than threshold', async () => {
      const results = await table.query({ age: { $lt: 30 } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.age < 30)).toBe(true)
    })

    it('should not include equal values', async () => {
      const results = await table.query({ age: { $lt: 30 } })

      expect(results.some(r => r.age === 30)).toBe(false)
    })
  })

  describe('$lte - Less Than or Equal', () => {
    it('should include equal values', async () => {
      const results = await table.query({ age: { $lte: 30 } })

      expect(results.some(r => r.age === 30)).toBe(true)
      expect(results.every(r => r.age <= 30)).toBe(true)
    })
  })

  describe('$in - Set Membership', () => {
    it('should match values in array', async () => {
      const results = await table.query({ name: { $in: ['Alice', 'Bob', 'Charlie'] } })

      expect(results).toHaveLength(3)
      expect(results.every(r => ['Alice', 'Bob', 'Charlie'].includes(r.name))).toBe(true)
    })

    it('should return empty for empty $in array', async () => {
      const results = await table.query({ name: { $in: [] } })

      expect(results).toHaveLength(0)
    })

    it('should match numeric values in array', async () => {
      const results = await table.query({ age: { $in: [25, 30, 35] } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => [25, 30, 35].includes(r.age))).toBe(true)
    })
  })

  describe('$nin - Not In Set', () => {
    it('should exclude values in array', async () => {
      const results = await table.query({ name: { $nin: ['Alice', 'Bob'] } })

      expect(results.every(r => !['Alice', 'Bob'].includes(r.name))).toBe(true)
    })

    it('should return all for empty $nin array', async () => {
      const results = await table.query({ name: { $nin: [] } })

      expect(results).toHaveLength(sampleDocuments.length)
    })
  })

  describe('Range queries (combined operators)', () => {
    it('should support $gte and $lte on same field', async () => {
      const results = await table.query({ age: { $gte: 25, $lte: 35 } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.age >= 25 && r.age <= 35)).toBe(true)
    })

    it('should support $gt and $lt on same field', async () => {
      const results = await table.query({ score: { $gt: 70, $lt: 90 } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.score > 70 && r.score < 90)).toBe(true)
    })
  })
})

// =============================================================================
// LOGICAL OPERATOR FILTERS
// =============================================================================

describe('DeltaTable.query() - Logical Operators', () => {
  let table: DeltaTable<TestDocument>

  beforeEach(async () => {
    const setup = createTestTable<TestDocument>()
    table = setup.table
    await table.write(sampleDocuments)
  })

  describe('$and - Logical AND', () => {
    it('should match when all conditions are true', async () => {
      const results = await table.query({
        $and: [
          { active: true },
          { age: { $gte: 25 } },
        ],
      })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.active === true && r.age >= 25)).toBe(true)
    })

    it('should return empty when any condition fails', async () => {
      const results = await table.query({
        $and: [
          { active: true },
          { age: { $gt: 100 } }, // No one is older than 100
        ],
      })

      expect(results).toHaveLength(0)
    })

    it('should handle empty $and array (match all)', async () => {
      const results = await table.query({ $and: [] })

      expect(results).toHaveLength(sampleDocuments.length)
    })
  })

  describe('$or - Logical OR', () => {
    it('should match when any condition is true', async () => {
      const results = await table.query({
        $or: [
          { name: 'Alice' },
          { name: 'Bob' },
        ],
      })

      expect(results).toHaveLength(2)
      expect(results.every(r => r.name === 'Alice' || r.name === 'Bob')).toBe(true)
    })

    it('should return empty for empty $or array', async () => {
      const results = await table.query({ $or: [] })

      expect(results).toHaveLength(0)
    })

    it('should handle complex $or conditions', async () => {
      const results = await table.query({
        $or: [
          { age: { $lt: 26 } },
          { score: { $gt: 90 } },
        ],
      })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.age < 26 || r.score > 90)).toBe(true)
    })
  })

  describe('$not - Logical NOT', () => {
    it('should negate a simple condition', async () => {
      const results = await table.query({
        $not: { active: true },
      })

      expect(results.every(r => r.active !== true)).toBe(true)
    })

    it('should negate complex conditions', async () => {
      const results = await table.query({
        $not: { age: { $gte: 30 } },
      })

      expect(results.every(r => r.age < 30)).toBe(true)
    })
  })

  describe('$nor - Logical NOR', () => {
    it('should match when none of the conditions are true', async () => {
      const results = await table.query({
        $nor: [
          { name: 'Alice' },
          { name: 'Bob' },
        ],
      })

      expect(results.every(r => r.name !== 'Alice' && r.name !== 'Bob')).toBe(true)
    })

    it('should handle empty $nor array (match all)', async () => {
      const results = await table.query({ $nor: [] })

      expect(results).toHaveLength(sampleDocuments.length)
    })
  })

  describe('Combined logical operators', () => {
    it('should handle $and with nested $or', async () => {
      const results = await table.query({
        $and: [
          { active: true },
          {
            $or: [
              { age: { $lt: 26 } },
              { score: { $gt: 80 } },
            ],
          },
        ],
      })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.active === true && (r.age < 26 || r.score > 80))).toBe(true)
    })
  })
})

// =============================================================================
// EXISTENCE AND REGEX FILTERS
// =============================================================================

describe('DeltaTable.query() - Existence and Pattern Filters', () => {
  let table: DeltaTable<TestDocument>

  beforeEach(async () => {
    const setup = createTestTable<TestDocument>()
    table = setup.table
    await table.write(sampleDocuments)
  })

  describe('$exists - Field Existence', () => {
    it('should match documents where field exists', async () => {
      const results = await table.query({ metadata: { $exists: true } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.metadata !== undefined)).toBe(true)
    })

    it('should match documents where field does not exist', async () => {
      const results = await table.query({ metadata: { $exists: false } })

      expect(results.every(r => r.metadata === undefined)).toBe(true)
    })
  })

  describe('$regex - Pattern Matching', () => {
    it('should match string patterns', async () => {
      const results = await table.query({ name: { $regex: '^A' } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.name.startsWith('A'))).toBe(true)
    })

    it('should support RegExp object', async () => {
      const results = await table.query({ name: { $regex: /^[A-C]/ } })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => /^[A-C]/.test(r.name))).toBe(true)
    })
  })
})

// =============================================================================
// NESTED FIELD ACCESS
// =============================================================================

describe('DeltaTable.query() - Nested Field Access', () => {
  let table: DeltaTable<TestDocument>

  beforeEach(async () => {
    const setup = createTestTable<TestDocument>()
    table = setup.table
    await table.write(sampleDocuments)
  })

  it('should access nested object fields with dot notation', async () => {
    const results = await table.query({ 'metadata.source': 'web' } as Filter<TestDocument>)

    expect(results.length).toBeGreaterThan(0)
    expect(results.every(r => r.metadata?.source === 'web')).toBe(true)
  })

  it('should apply comparison operators on nested fields', async () => {
    const results = await table.query({
      'metadata.tier': { $in: ['gold', 'platinum'] },
    } as Filter<TestDocument>)

    expect(results.length).toBeGreaterThan(0)
    expect(results.every(r => ['gold', 'platinum'].includes(r.metadata?.tier || ''))).toBe(true)
  })

  it('should handle missing intermediate objects', async () => {
    const results = await table.query({ 'metadata.tier': 'gold' } as Filter<TestDocument>)

    expect(results.length).toBeGreaterThan(0)
    // Documents without metadata should not match
    expect(results.every(r => r.metadata?.tier === 'gold')).toBe(true)
  })
})

// =============================================================================
// EMPTY RESULTS AND EDGE CASES
// =============================================================================

describe('DeltaTable.query() - Empty Results and Edge Cases', () => {
  let table: DeltaTable<TestDocument>

  beforeEach(async () => {
    const setup = createTestTable<TestDocument>()
    table = setup.table
    await table.write(sampleDocuments)
  })

  it('should return empty array when no documents match', async () => {
    const results = await table.query({ age: { $gt: 100 } })

    expect(results).toHaveLength(0)
  })

  it('should return empty array for non-existent field value', async () => {
    const results = await table.query({ name: 'NonExistent' })

    expect(results).toHaveLength(0)
  })

  it('should handle single row result', async () => {
    const results = await table.query({ _id: '1' })

    expect(results).toHaveLength(1)
  })

  it('should handle query after delete operation', async () => {
    // Delete Alice
    await table.delete({ name: 'Alice' })

    const results = await table.query()

    expect(results).toHaveLength(sampleDocuments.length - 1)
    expect(results.some(r => r.name === 'Alice')).toBe(false)
  })

  it('should handle query after update operation', async () => {
    // Update Bob's age
    await table.update({ name: 'Bob' }, { age: 99 })

    const results = await table.query({ name: 'Bob' })

    expect(results).toHaveLength(1)
    expect(results[0].age).toBe(99)
  })
})

// =============================================================================
// MULTI-FILE QUERIES
// =============================================================================

describe('DeltaTable.query() - Multiple Parquet Files', () => {
  it('should query across multiple Parquet files from separate writes', async () => {
    const { table } = createTestTable<TestDocument>()

    // Each write creates a new Parquet file
    await table.write([sampleDocuments[0]])
    await table.write([sampleDocuments[1]])
    await table.write([sampleDocuments[2]])

    const results = await table.query()

    expect(results).toHaveLength(3)
  })

  it('should apply filters across all Parquet files', async () => {
    const { table } = createTestTable<TestDocument>()

    await table.write([sampleDocuments[0], sampleDocuments[1]]) // active: true
    await table.write([sampleDocuments[2]]) // active: false
    await table.write([sampleDocuments[3], sampleDocuments[4]]) // mixed

    const results = await table.query({ active: true })

    const expectedCount = sampleDocuments.filter(d => d.active).length
    expect(results).toHaveLength(expectedCount)
  })

  it('should handle removed files in snapshot', async () => {
    const { table, storage } = createTestTable<TestDocument>()

    // Write initial data
    await table.write(sampleDocuments.slice(0, 2))

    // Delete all data (creates remove actions)
    await table.delete({})

    // Write new data
    await table.write(sampleDocuments.slice(2, 4))

    const results = await table.query()

    // Should only see the new data, not the deleted data
    expect(results).toHaveLength(2)
  })
})

// =============================================================================
// PROJECTION SUPPORT
// =============================================================================

describe('DeltaTable.query() - Projection Support', () => {
  it('should select only specified fields', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Test array projection format
    const results = await table.query({}, { projection: ['name', 'age'] })

    expect(results.length).toBe(5)
    expect(Object.keys(results[0]).sort()).toEqual(['age', 'name'])
    expect(results[0].name).toBe('Alice')
    expect(results[0].age).toBe(30)
    // Other fields should not be present
    expect(results[0]).not.toHaveProperty('_id')
    expect(results[0]).not.toHaveProperty('score')
    expect(results[0]).not.toHaveProperty('active')
  })

  it('should handle nested field projection', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Test nested field projection
    const results = await table.query({}, { projection: ['name', 'metadata.tier'] })

    expect(results.length).toBe(5)

    // First result should have name and nested metadata.tier
    expect(results[0].name).toBe('Alice')
    expect(results[0].metadata).toEqual({ tier: 'gold' })

    // Fourth document has no metadata
    expect(results[3].name).toBe('Diana')
    expect(results[3].metadata).toBeUndefined()
  })

  it('should optimize Parquet column reads with projection', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Query with projection to track columns
    await table.query({}, { projection: ['name', 'age', 'metadata.tier'] })

    // Check that the projection columns were identified
    expect(table.lastQueryProjectionColumns).toBeDefined()
    expect(table.lastQueryProjectionColumns).toContain('name')
    expect(table.lastQueryProjectionColumns).toContain('age')
    expect(table.lastQueryProjectionColumns).toContain('metadata')
    // Note: 'metadata' is the root column for 'metadata.tier'
  })

  it('should support object projection format with include', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Test object projection format with 1 (include)
    const results = await table.query({}, { projection: { name: 1, score: 1 } })

    expect(results.length).toBe(5)
    expect(Object.keys(results[0]).sort()).toEqual(['name', 'score'])
    expect(results[0].name).toBe('Alice')
    expect(results[0].score).toBe(85.5)
  })

  it('should support object projection format with exclude', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Test object projection format with 0 (exclude)
    const results = await table.query({}, { projection: { tags: 0, metadata: 0 } })

    expect(results.length).toBe(5)
    expect(results[0]).toHaveProperty('_id')
    expect(results[0]).toHaveProperty('name')
    expect(results[0]).toHaveProperty('age')
    expect(results[0]).toHaveProperty('score')
    expect(results[0]).toHaveProperty('active')
    expect(results[0]).not.toHaveProperty('tags')
    expect(results[0]).not.toHaveProperty('metadata')
  })

  it('should combine filter and projection', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Query with both filter and projection
    const results = await table.query(
      { active: true },
      { projection: ['name', 'age'] }
    )

    // Should only return active users (Alice, Bob, Diana)
    expect(results.length).toBe(3)

    // Should only have projected fields
    expect(Object.keys(results[0]).sort()).toEqual(['age', 'name'])

    // Verify the filtered results
    const names = results.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Bob')
    expect(names).toContain('Diana')
    expect(names).not.toContain('Charlie')
    expect(names).not.toContain('Eve')
  })
})

// =============================================================================
// PREDICATE PUSHDOWN (Zone Map Filtering)
// =============================================================================

describe('DeltaTable.query() - Predicate Pushdown', () => {
  describe('Zone map filter generation', () => {
    it('should generate zone map filters for simple equality', () => {
      const filter: Filter<TestDocument> = { age: { $eq: 30 } }
      const predicates = filterToParquetPredicate(filter)

      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'eq',
        value: 30,
      })
    })

    it('should generate zone map filters for range queries', () => {
      const filter: Filter<TestDocument> = { age: { $gte: 25, $lte: 35 } }
      const predicates = filterToParquetPredicate(filter)

      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'gte',
        value: 25,
      })
      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'lte',
        value: 35,
      })
    })

    it('should generate between predicate for range queries', () => {
      const filter: Filter<TestDocument> = { age: { $gte: 25, $lte: 35 } }
      const predicates = filterToParquetPredicate(filter)

      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'between',
        value: 25,
        value2: 35,
      })
    })

    it('should handle $and predicates', () => {
      const filter: Filter<TestDocument> = {
        $and: [
          { age: { $gte: 25 } },
          { score: { $gt: 80 } },
        ],
      }
      const predicates = filterToParquetPredicate(filter)

      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'gte',
        value: 25,
      })
      expect(predicates).toContainEqual({
        column: 'score',
        operator: 'gt',
        value: 80,
      })
    })
  })

  describe('Row group skipping', () => {
    it('should skip row group when eq value is outside range', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 50 }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should not skip row group when eq value is within range', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 30 }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
    })

    it('should skip row group when gt filter exceeds max', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'gt', value: 35 }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should skip row group when lt filter is below min', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'lt', value: 25 }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should skip row group when $in values are all outside range', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'in', value: [10, 15, 50, 60] }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })

    it('should not skip when any $in value is within range', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'in', value: [10, 30, 50] }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
    })

    it('should skip row group when between ranges do not overlap', () => {
      const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
      const filter: ZoneMapFilter = { column: 'age', operator: 'between', value: 40, value2: 50 }

      expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
    })
  })
})

// =============================================================================
// SHREDDED COLUMNS AND STATISTICS
// =============================================================================

describe('DeltaTable.query() - Shredded Column Statistics', () => {
  it('should use shredded column min/max for zone map filtering', async () => {
    // When variant columns are shredded, typed values have statistics
    // that can be used for predicate pushdown

    // Simulate a shredded column's zone map (e.g., 'doc.price' shredded from a variant column)
    // The shredded path would be 'doc.typed_value.price.typed_value' internally
    const shreddedZoneMap: ZoneMap = {
      column: 'doc.price',
      min: 10,
      max: 100,
      nullCount: 2,
    }

    // Filter that should allow skipping (value outside range)
    const skipFilter: ZoneMapFilter = {
      column: 'doc.price',
      operator: 'eq',
      value: 200, // Outside [10, 100]
    }
    expect(canSkipZoneMap(shreddedZoneMap, skipFilter)).toBe(true)

    // Filter that should NOT skip (value within range)
    const noSkipFilter: ZoneMapFilter = {
      column: 'doc.price',
      operator: 'eq',
      value: 50, // Within [10, 100]
    }
    expect(canSkipZoneMap(shreddedZoneMap, noSkipFilter)).toBe(false)

    // Range filter that should skip (completely outside)
    const rangeSkipFilter: ZoneMapFilter = {
      column: 'doc.price',
      operator: 'gt',
      value: 100, // All values in zone are <= 100
    }
    expect(canSkipZoneMap(shreddedZoneMap, rangeSkipFilter)).toBe(true)

    // Range filter that should NOT skip (overlaps)
    const rangeNoSkipFilter: ZoneMapFilter = {
      column: 'doc.price',
      operator: 'gte',
      value: 50, // Some values in [50, 100] exist
    }
    expect(canSkipZoneMap(shreddedZoneMap, rangeNoSkipFilter)).toBe(false)
  })

  it('should map filter paths to shredded column statistics', async () => {
    // Filter on doc.field should map to variant.typed_value.field.typed_value
    const columnName = 'doc'
    const shredFields = ['price', 'category', 'name']

    // Test mapping for shredded fields
    const pricePath = mapFilterPathToStats('doc.price', columnName, shredFields)
    expect(pricePath).toBe('doc.typed_value.price.typed_value')

    const categoryPath = mapFilterPathToStats('doc.category', columnName, shredFields)
    expect(categoryPath).toBe('doc.typed_value.category.typed_value')

    const namePath = mapFilterPathToStats('doc.name', columnName, shredFields)
    expect(namePath).toBe('doc.typed_value.name.typed_value')

    // Test getStatisticsPaths helper
    const allPaths = getStatisticsPaths(columnName, shredFields)
    expect(allPaths).toHaveLength(3)
    expect(allPaths).toContain('doc.typed_value.price.typed_value')
    expect(allPaths).toContain('doc.typed_value.category.typed_value')
    expect(allPaths).toContain('doc.typed_value.name.typed_value')
  })

  it('should fall back to full scan when column is not shredded', async () => {
    // If a field is not in shredFields, cannot use zone map filtering
    const columnName = 'doc'
    const shredFields = ['price', 'category'] // Only these are shredded

    // Non-shredded field should return null
    const descriptionPath = mapFilterPathToStats('doc.description', columnName, shredFields)
    expect(descriptionPath).toBeNull()

    // Field from different column should return null
    const otherColumnPath = mapFilterPathToStats('other.price', columnName, shredFields)
    expect(otherColumnPath).toBeNull()

    // Top-level field (not nested) should return null
    const topLevelPath = mapFilterPathToStats('price', columnName, shredFields)
    expect(topLevelPath).toBeNull()

    // Nested field beyond shredded depth should return null
    const deepNestedPath = mapFilterPathToStats('doc.nested.price', columnName, shredFields)
    expect(deepNestedPath).toBeNull()
  })
})

// =============================================================================
// VARIANT COLUMN DECODING
// =============================================================================

describe('DeltaTable.query() - Variant Column Decoding', () => {
  it('should decode variant-encoded documents', async () => {
    // Documents stored as variant type need to be decoded when reading
    // This tests that objects with nested structure are preserved through write/read cycle
    interface VariantDoc {
      _id: string
      data: {
        name: string
        count: number
        active: boolean
      }
    }

    const { table } = createTestTable<VariantDoc>()

    const documents: VariantDoc[] = [
      { _id: '1', data: { name: 'first', count: 10, active: true } },
      { _id: '2', data: { name: 'second', count: 20, active: false } },
      { _id: '3', data: { name: 'third', count: 30, active: true } },
    ]

    await table.write(documents)
    const results = await table.query()

    expect(results).toHaveLength(3)
    // Verify variant objects are properly decoded
    expect(results[0].data).toEqual({ name: 'first', count: 10, active: true })
    expect(results[1].data).toEqual({ name: 'second', count: 20, active: false })
    expect(results[2].data).toEqual({ name: 'third', count: 30, active: true })

    // Verify all fields are accessible and have correct types
    for (const doc of results) {
      expect(typeof doc.data.name).toBe('string')
      expect(typeof doc.data.count).toBe('number')
      expect(typeof doc.data.active).toBe('boolean')
    }
  })

  it('should handle nested variant values', async () => {
    // Nested objects within variant columns - deeply nested structures
    interface DeeplyNestedDoc {
      _id: string
      level1: {
        level2: {
          level3: {
            value: string
            numbers: number[]
          }
        }
        sibling: string
      }
    }

    const { table } = createTestTable<DeeplyNestedDoc>()

    const documents: DeeplyNestedDoc[] = [
      {
        _id: '1',
        level1: {
          level2: {
            level3: {
              value: 'deep-value-1',
              numbers: [1, 2, 3],
            },
          },
          sibling: 'sibling-1',
        },
      },
      {
        _id: '2',
        level1: {
          level2: {
            level3: {
              value: 'deep-value-2',
              numbers: [4, 5, 6],
            },
          },
          sibling: 'sibling-2',
        },
      },
    ]

    await table.write(documents)
    const results = await table.query()

    expect(results).toHaveLength(2)

    // Verify deeply nested values are preserved
    expect(results[0].level1.level2.level3.value).toBe('deep-value-1')
    expect(results[0].level1.level2.level3.numbers).toEqual([1, 2, 3])
    expect(results[0].level1.sibling).toBe('sibling-1')

    expect(results[1].level1.level2.level3.value).toBe('deep-value-2')
    expect(results[1].level1.level2.level3.numbers).toEqual([4, 5, 6])
    expect(results[1].level1.sibling).toBe('sibling-2')
  })

  it('should apply filters on decoded variant values', async () => {
    // Filters should work on decoded values, not raw bytes
    interface FilterableVariantDoc {
      _id: string
      profile: {
        role: string
        level: number
        permissions: string[]
      }
    }

    const { table } = createTestTable<FilterableVariantDoc>()

    const documents: FilterableVariantDoc[] = [
      { _id: '1', profile: { role: 'admin', level: 10, permissions: ['read', 'write', 'delete'] } },
      { _id: '2', profile: { role: 'user', level: 5, permissions: ['read'] } },
      { _id: '3', profile: { role: 'admin', level: 8, permissions: ['read', 'write'] } },
      { _id: '4', profile: { role: 'guest', level: 1, permissions: [] } },
    ]

    await table.write(documents)

    // Filter on nested variant field using dot notation
    const admins = await table.query({ 'profile.role': 'admin' } as Filter<FilterableVariantDoc>)
    expect(admins).toHaveLength(2)
    expect(admins.every(d => d.profile.role === 'admin')).toBe(true)

    // Filter with comparison operator on nested numeric field
    const highLevel = await table.query({ 'profile.level': { $gte: 8 } } as Filter<FilterableVariantDoc>)
    expect(highLevel).toHaveLength(2)
    expect(highLevel.every(d => d.profile.level >= 8)).toBe(true)

    // Filter with $in operator on nested field
    const roleFilter = await table.query({ 'profile.role': { $in: ['admin', 'guest'] } } as Filter<FilterableVariantDoc>)
    expect(roleFilter).toHaveLength(3)
    expect(roleFilter.every(d => ['admin', 'guest'].includes(d.profile.role))).toBe(true)
  })

  it('should decode array values from variant', async () => {
    // Array fields stored in variant encoding
    interface ArrayVariantDoc {
      _id: string
      tags: string[]
      scores: number[]
      matrix: number[][]
      items: Array<{ name: string; value: number }>
    }

    const { table } = createTestTable<ArrayVariantDoc>()

    const documents: ArrayVariantDoc[] = [
      {
        _id: '1',
        tags: ['tag1', 'tag2', 'tag3'],
        scores: [85, 90, 95],
        matrix: [[1, 2], [3, 4]],
        items: [{ name: 'item1', value: 100 }, { name: 'item2', value: 200 }],
      },
      {
        _id: '2',
        tags: [],
        scores: [70],
        matrix: [],
        items: [],
      },
      {
        _id: '3',
        tags: ['single'],
        scores: [88, 92],
        matrix: [[5, 6, 7]],
        items: [{ name: 'only', value: 50 }],
      },
    ]

    await table.write(documents)
    const results = await table.query()

    expect(results).toHaveLength(3)

    // Verify string arrays are decoded
    expect(results[0].tags).toEqual(['tag1', 'tag2', 'tag3'])
    expect(Array.isArray(results[0].tags)).toBe(true)

    // Verify number arrays are decoded
    expect(results[0].scores).toEqual([85, 90, 95])
    expect(results[0].scores.every(s => typeof s === 'number')).toBe(true)

    // Verify nested arrays (matrix) are decoded
    expect(results[0].matrix).toEqual([[1, 2], [3, 4]])

    // Verify arrays of objects are decoded
    expect(results[0].items).toEqual([{ name: 'item1', value: 100 }, { name: 'item2', value: 200 }])
    expect(results[0].items[0].name).toBe('item1')
    expect(results[0].items[0].value).toBe(100)

    // Verify empty arrays are preserved
    expect(results[1].tags).toEqual([])
    expect(results[1].matrix).toEqual([])
    expect(results[1].items).toEqual([])

    // Verify single-element arrays work
    expect(results[2].tags).toEqual(['single'])
  })
})

// =============================================================================
// INTEGRATION WITH MEMORY STORAGE
// =============================================================================

describe('DeltaTable.query() - MemoryStorage Integration', () => {
  it('should work with MemoryStorage backend', async () => {
    const storage = new MemoryStorage()
    const table = new DeltaTable<TestDocument>(storage, 'test-table')

    await table.write(sampleDocuments)
    const results = await table.query({ active: true })

    expect(results.length).toBeGreaterThan(0)
  })

  it('should maintain consistency across operations', async () => {
    const storage = new MemoryStorage()
    const table = new DeltaTable<TestDocument>(storage, 'test-table')

    // Write, query, update, query sequence
    await table.write(sampleDocuments)
    const initial = await table.query()
    expect(initial).toHaveLength(sampleDocuments.length)

    await table.update({ name: 'Alice' }, { score: 100 })
    const afterUpdate = await table.query({ name: 'Alice' })
    expect(afterUpdate[0].score).toBe(100)

    await table.delete({ name: 'Bob' })
    const afterDelete = await table.query()
    expect(afterDelete).toHaveLength(sampleDocuments.length - 1)
  })

  it('should handle concurrent read operations', async () => {
    const storage = new MemoryStorage()
    const table = new DeltaTable<TestDocument>(storage, 'test-table')

    await table.write(sampleDocuments)

    // Multiple concurrent queries
    const results = await Promise.all([
      table.query({ active: true }),
      table.query({ age: { $gt: 30 } }),
      table.query({ name: { $in: ['Alice', 'Bob'] } }),
    ])

    expect(results[0].length).toBeGreaterThan(0)
    expect(results[1].length).toBeGreaterThan(0)
    expect(results[2]).toHaveLength(2)
  })
})

// =============================================================================
// QUERY PERFORMANCE SCENARIOS
// =============================================================================

describe('DeltaTable.query() - Performance Scenarios', () => {
  it('should handle large number of rows efficiently', async () => {
    const { table } = createTestTable<{ _id: string; value: number }>()

    // Generate 1000 rows
    const rows = Array.from({ length: 1000 }, (_, i) => ({
      _id: `id-${i}`,
      value: i,
    }))

    await table.write(rows)
    const results = await table.query({ value: { $gt: 500 } })

    expect(results).toHaveLength(499) // 501-999
  })

  it('should handle many small writes (multiple files)', async () => {
    const { table } = createTestTable<{ _id: string; value: number }>()

    // 100 separate writes
    for (let i = 0; i < 100; i++) {
      await table.write([{ _id: `id-${i}`, value: i }])
    }

    const results = await table.query()

    expect(results).toHaveLength(100)
  })

  it('should efficiently filter with selective predicate', async () => {
    const { table } = createTestTable<{ _id: string; value: number; category: string }>()

    // Create data with rare category
    const rows = Array.from({ length: 1000 }, (_, i) => ({
      _id: `id-${i}`,
      value: i,
      category: i < 10 ? 'rare' : 'common',
    }))

    await table.write(rows)
    const results = await table.query({ category: 'rare' })

    expect(results).toHaveLength(10)
  })
})

// =============================================================================
// TYPE-SPECIFIC QUERIES
// =============================================================================

describe('DeltaTable.query() - Type-Specific Queries', () => {
  describe('Product queries', () => {
    let table: DeltaTable<Product>

    beforeEach(async () => {
      const setup = createTestTable<Product>()
      table = setup.table
      await table.write(sampleProducts)
    })

    it('should query by category', async () => {
      const results = await table.query({ category: 'electronics' })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.category === 'electronics')).toBe(true)
    })

    it('should query by price range', async () => {
      const results = await table.query({
        price: { $gte: 100, $lte: 500 },
      })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.price >= 100 && r.price <= 500)).toBe(true)
    })

    it('should query in-stock products', async () => {
      const results = await table.query({
        inStock: true,
        quantity: { $gt: 0 },
      })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => r.inStock === true && r.quantity > 0)).toBe(true)
    })

    it('should query products with high ratings', async () => {
      const results = await table.query({
        rating: { $gte: 4.5 },
      })

      expect(results.length).toBeGreaterThan(0)
      expect(results.every(r => (r.rating ?? 0) >= 4.5)).toBe(true)
    })
  })

  describe('Event queries with timestamps', () => {
    let table: DeltaTable<Event>

    beforeEach(async () => {
      const setup = createTestTable<Event>()
      table = setup.table

      const events: Event[] = [
        { _id: 'e1', type: 'click', timestamp: 1706800000000, payload: { x: 100 } },
        { _id: 'e2', type: 'view', timestamp: 1706800001000, payload: { page: '/home' } },
        { _id: 'e3', type: 'click', timestamp: 1706800002000, payload: { x: 200 } },
        { _id: 'e4', type: 'purchase', timestamp: 1706800003000, payload: { amount: 99.99 } },
        { _id: 'e5', type: 'view', timestamp: 1706800004000, payload: { page: '/product' } },
      ]

      await table.write(events)
    })

    it('should query events by type', async () => {
      const results = await table.query({ type: 'click' })

      expect(results).toHaveLength(2)
      expect(results.every(r => r.type === 'click')).toBe(true)
    })

    it('should query events by timestamp range', async () => {
      const results = await table.query({
        timestamp: { $gte: 1706800001000, $lte: 1706800003000 },
      })

      expect(results).toHaveLength(3)
    })

    it('should query events after specific timestamp', async () => {
      const results = await table.query({
        timestamp: { $gt: 1706800002000 },
      })

      expect(results).toHaveLength(2)
    })
  })
})

// =============================================================================
// SNAPSHOT VERSION QUERIES
// =============================================================================

describe('DeltaTable.query() - Snapshot Version Queries', () => {
  it('should query at specific snapshot version', async () => {
    // Query table state at a previous version
    const { table } = createTestTable<TestDocument>()

    // Write initial data (version 0)
    await table.write([
      { _id: '1', name: 'Alice', age: 30, score: 85.5, active: true },
      { _id: '2', name: 'Bob', age: 25, score: 72.0, active: true },
    ])

    // Write more data (version 1)
    await table.write([
      { _id: '3', name: 'Charlie', age: 35, score: 91.2, active: false },
    ])

    // Query at version 0 should only return the first 2 records
    const resultsV0 = await table.query({}, { version: 0 })
    expect(resultsV0).toHaveLength(2)
    expect(resultsV0.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])

    // Query at version 1 should return all 3 records
    const resultsV1 = await table.query({}, { version: 1 })
    expect(resultsV1).toHaveLength(3)

    // Query without version should return latest (3 records)
    const resultsLatest = await table.query()
    expect(resultsLatest).toHaveLength(3)
  })

  it('should return consistent results within a snapshot', async () => {
    // Multiple queries at same snapshot should return same results
    const { table } = createTestTable<TestDocument>()

    // Write initial data
    await table.write([
      { _id: '1', name: 'Alice', age: 30, score: 85.5, active: true },
      { _id: '2', name: 'Bob', age: 25, score: 72.0, active: true },
    ])

    // Get a snapshot at version 0
    const snapshot = await table.snapshot(0)

    // Write more data (this creates version 1)
    await table.write([
      { _id: '3', name: 'Charlie', age: 35, score: 91.2, active: false },
    ])

    // Both queries using the same snapshot should return consistent results
    const results1 = await table.query({ active: true }, { snapshot })
    const results2 = await table.query({}, { snapshot })

    // Should only see data from version 0 (2 records), not the new write
    expect(results2).toHaveLength(2)
    expect(results1).toHaveLength(2)

    // Latest query should see all 3 records
    const resultsLatest = await table.query()
    expect(resultsLatest).toHaveLength(3)
  })

  it('should see new data after snapshot refresh', async () => {
    // After writes, new snapshot should include new data
    const { table } = createTestTable<TestDocument>()

    // Write initial data
    await table.write([
      { _id: '1', name: 'Alice', age: 30, score: 85.5, active: true },
    ])

    // Get initial snapshot
    const snapshot1 = await table.snapshot()
    expect(snapshot1.version).toBe(0)

    // Query with initial snapshot
    const results1 = await table.query({}, { snapshot: snapshot1 })
    expect(results1).toHaveLength(1)

    // Write more data
    await table.write([
      { _id: '2', name: 'Bob', age: 25, score: 72.0, active: true },
    ])

    // Old snapshot still only sees 1 record
    const resultsOld = await table.query({}, { snapshot: snapshot1 })
    expect(resultsOld).toHaveLength(1)

    // Get refreshed snapshot
    const snapshot2 = await table.snapshot()
    expect(snapshot2.version).toBe(1)

    // New snapshot sees both records
    const resultsNew = await table.query({}, { snapshot: snapshot2 })
    expect(resultsNew).toHaveLength(2)

    // Using version option should also see both records
    const resultsVersion = await table.query({}, { version: 1 })
    expect(resultsVersion).toHaveLength(2)
  })
})

// =============================================================================
// ITERATOR-BASED QUERY METHODS (BATCH/STREAMING)
// =============================================================================

describe('DeltaTable.queryIterator() - Row-by-Row Streaming', () => {
  it('should yield rows one at a time', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const rows: TestDocument[] = []
    for await (const row of table.queryIterator()) {
      rows.push(row as TestDocument)
    }

    expect(rows).toHaveLength(sampleDocuments.length)
  })

  it('should return empty iterator for empty table', async () => {
    const { table } = createTestTable<TestDocument>()

    const rows: TestDocument[] = []
    for await (const row of table.queryIterator()) {
      rows.push(row as TestDocument)
    }

    expect(rows).toHaveLength(0)
  })

  it('should apply filter during iteration', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const rows: TestDocument[] = []
    for await (const row of table.queryIterator({ active: true })) {
      rows.push(row as TestDocument)
    }

    // Only active documents should be returned
    expect(rows.every(r => r.active === true)).toBe(true)
    expect(rows.length).toBe(sampleDocuments.filter(d => d.active).length)
  })

  it('should support early termination', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const rows: TestDocument[] = []
    for await (const row of table.queryIterator()) {
      rows.push(row as TestDocument)
      if (rows.length >= 2) break
    }

    expect(rows).toHaveLength(2)
  })

  it('should apply projection during iteration', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const rows: Partial<TestDocument>[] = []
    for await (const row of table.queryIterator({}, { projection: ['name', 'age'] })) {
      rows.push(row)
    }

    expect(rows).toHaveLength(sampleDocuments.length)
    expect(Object.keys(rows[0]).sort()).toEqual(['age', 'name'])
  })

  it('should support time travel with version option', async () => {
    const { table } = createTestTable<TestDocument>()

    // Write initial data (version 0)
    await table.write([sampleDocuments[0], sampleDocuments[1]])

    // Write more data (version 1)
    await table.write([sampleDocuments[2]])

    // Query at version 0
    const rowsV0: TestDocument[] = []
    for await (const row of table.queryIterator({}, { version: 0 })) {
      rowsV0.push(row as TestDocument)
    }
    expect(rowsV0).toHaveLength(2)

    // Query at version 1
    const rowsV1: TestDocument[] = []
    for await (const row of table.queryIterator({}, { version: 1 })) {
      rowsV1.push(row as TestDocument)
    }
    expect(rowsV1).toHaveLength(3)
  })

  it('should iterate across multiple Parquet files', async () => {
    const { table } = createTestTable<TestDocument>()

    // Write data in separate commits (creates multiple files)
    await table.write([sampleDocuments[0]])
    await table.write([sampleDocuments[1]])
    await table.write([sampleDocuments[2]])

    const rows: TestDocument[] = []
    for await (const row of table.queryIterator()) {
      rows.push(row as TestDocument)
    }

    expect(rows).toHaveLength(3)
  })

  it('should validate filter parameter', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Test invalid filter (array instead of object)
    await expect(async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      for await (const _row of table.queryIterator(['invalid'] as any)) {
        // Should not reach here
      }
    }).rejects.toThrow('filter must be an object')
  })

  it('should validate batchSize parameter in queryBatch', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    // Test invalid batchSize (zero)
    await expect(async () => {
      for await (const _batch of table.queryBatch({}, 0)) {
        // Should not reach here
      }
    }).rejects.toThrow('batchSize must be a positive integer')

    // Test invalid batchSize (negative)
    await expect(async () => {
      for await (const _batch of table.queryBatch({}, -1)) {
        // Should not reach here
      }
    }).rejects.toThrow('batchSize must be a positive integer')

    // Test invalid batchSize (non-integer)
    await expect(async () => {
      for await (const _batch of table.queryBatch({}, 1.5)) {
        // Should not reach here
      }
    }).rejects.toThrow('batchSize must be a positive integer')
  })
})

describe('DeltaTable.queryBatch() - Batch Streaming', () => {
  it('should yield rows in batches', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const batches: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 2)) {
      batches.push(batch as TestDocument[])
    }

    // With 5 documents and batch size 2, should get 3 batches (2, 2, 1)
    expect(batches).toHaveLength(3)
    expect(batches[0]).toHaveLength(2)
    expect(batches[1]).toHaveLength(2)
    expect(batches[2]).toHaveLength(1)

    // Total rows should match
    const totalRows = batches.reduce((sum, batch) => sum + batch.length, 0)
    expect(totalRows).toBe(sampleDocuments.length)
  })

  it('should use default batch size of 1000', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const batches: TestDocument[][] = []
    for await (const batch of table.queryBatch()) {
      batches.push(batch as TestDocument[])
    }

    // With 5 documents and default batch size 1000, should get 1 batch
    expect(batches).toHaveLength(1)
    expect(batches[0]).toHaveLength(sampleDocuments.length)
  })

  it('should return empty iterator for empty table', async () => {
    const { table } = createTestTable<TestDocument>()

    const batches: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 10)) {
      batches.push(batch as TestDocument[])
    }

    expect(batches).toHaveLength(0)
  })

  it('should apply filter during batch iteration', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const allRows: TestDocument[] = []
    for await (const batch of table.queryBatch({ active: true }, 10)) {
      allRows.push(...batch as TestDocument[])
    }

    // Only active documents should be returned
    expect(allRows.every(r => r.active === true)).toBe(true)
    expect(allRows.length).toBe(sampleDocuments.filter(d => d.active).length)
  })

  it('should handle batch size larger than result set', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const batches: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 1000)) {
      batches.push(batch as TestDocument[])
    }

    // Should get 1 batch with all documents
    expect(batches).toHaveLength(1)
    expect(batches[0]).toHaveLength(sampleDocuments.length)
  })

  it('should handle batch size of 1', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const batches: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 1)) {
      batches.push(batch as TestDocument[])
    }

    // Should get one batch per document
    expect(batches).toHaveLength(sampleDocuments.length)
    expect(batches.every(b => b.length === 1)).toBe(true)
  })

  it('should support early termination', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const batches: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 2)) {
      batches.push(batch as TestDocument[])
      if (batches.length >= 2) break
    }

    expect(batches).toHaveLength(2)
    expect(batches[0]).toHaveLength(2)
    expect(batches[1]).toHaveLength(2)
  })

  it('should apply projection to batches', async () => {
    const { table } = createTestTable<TestDocument>()
    await table.write(sampleDocuments)

    const batches: Partial<TestDocument>[][] = []
    for await (const batch of table.queryBatch({}, 10, { projection: ['name'] })) {
      batches.push(batch)
    }

    expect(batches).toHaveLength(1)
    expect(batches[0]).toHaveLength(sampleDocuments.length)
    expect(Object.keys(batches[0][0])).toEqual(['name'])
  })

  it('should work with time travel', async () => {
    const { table } = createTestTable<TestDocument>()

    // Write initial data (version 0)
    await table.write([sampleDocuments[0], sampleDocuments[1]])

    // Write more data (version 1)
    await table.write([sampleDocuments[2], sampleDocuments[3], sampleDocuments[4]])

    // Query at version 0
    const batchesV0: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 10, { version: 0 })) {
      batchesV0.push(batch as TestDocument[])
    }
    expect(batchesV0).toHaveLength(1)
    expect(batchesV0[0]).toHaveLength(2)

    // Query at version 1
    const batchesV1: TestDocument[][] = []
    for await (const batch of table.queryBatch({}, 10, { version: 1 })) {
      batchesV1.push(batch as TestDocument[])
    }
    expect(batchesV1).toHaveLength(1)
    expect(batchesV1[0]).toHaveLength(5)
  })
})

describe('DeltaTable Iterator Performance', () => {
  it('should handle large result sets efficiently', async () => {
    const { table } = createTestTable<{ _id: string; value: number }>()

    // Generate 500 rows (reduced from 1000 for faster test execution)
    const rows = Array.from({ length: 500 }, (_, i) => ({
      _id: `id-${i}`,
      value: i,
    }))
    await table.write(rows)

    // Use queryBatch for efficient processing
    let processedCount = 0
    for await (const batch of table.queryBatch({}, 100)) {
      processedCount += batch.length
    }

    expect(processedCount).toBe(500)
  })

  it('should handle filtered iteration efficiently', async () => {
    const { table } = createTestTable<{ _id: string; value: number; category: string }>()

    // Create data with rare category
    const rows = Array.from({ length: 500 }, (_, i) => ({
      _id: `id-${i}`,
      value: i,
      category: i < 10 ? 'rare' : 'common',
    }))
    await table.write(rows)

    // Iterate only over rare items
    let count = 0
    for await (const _row of table.queryIterator({ category: 'rare' })) {
      count++
    }

    expect(count).toBe(10)
  })
})
