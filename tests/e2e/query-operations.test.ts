/**
 * E2E Tests: Query Operations
 *
 * Tests query functionality with real storage:
 * - Filter operations (equality, comparison, logical)
 * - Projections (field selection)
 * - Nested field queries
 * - Edge cases and complex queries
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DeltaTable } from '../../src/delta/index.js'
import { FileSystemStorage } from '../../src/storage/index.js'
import type { Filter } from '../../src/query/index.js'
import * as fs from 'node:fs/promises'
import * as path from 'node:path'
import * as os from 'node:os'

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a unique test directory for each test
 */
async function createTestDir(): Promise<string> {
  const testDir = path.join(os.tmpdir(), `deltalake-e2e-query-${Date.now()}-${Math.random().toString(36).slice(2)}`)
  await fs.mkdir(testDir, { recursive: true })
  return testDir
}

/**
 * Clean up test directory after test
 */
async function cleanupTestDir(testDir: string): Promise<void> {
  try {
    await fs.rm(testDir, { recursive: true, force: true })
  } catch {
    // Ignore cleanup errors
  }
}

// =============================================================================
// TEST DATA TYPES
// =============================================================================

interface User {
  [key: string]: unknown
  _id: string
  name: string
  email: string
  age: number
  active: boolean
  role: string
  score: number
  department?: string
  metadata?: {
    source: string
    tier: string
    tags: string[]
  }
}

interface Product {
  [key: string]: unknown
  _id: string
  name: string
  price: number
  category: string
  inStock: boolean
  quantity: number
  rating?: number
  specs?: {
    brand: string
    model: string
    features: string[]
  }
}

// =============================================================================
// SAMPLE DATA
// =============================================================================

const sampleUsers: User[] = [
  { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, role: 'admin', score: 95.5, department: 'engineering', metadata: { source: 'web', tier: 'gold', tags: ['premium', 'verified'] } },
  { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, role: 'user', score: 72.0, department: 'marketing', metadata: { source: 'mobile', tier: 'silver', tags: ['basic'] } },
  { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, role: 'admin', score: 88.0, department: 'engineering', metadata: { source: 'api', tier: 'platinum', tags: ['premium', 'vip'] } },
  { _id: '4', name: 'Diana', email: 'diana@example.com', age: 28, active: true, role: 'user', score: 65.0, department: 'sales' },
  { _id: '5', name: 'Eve', email: 'eve@example.com', age: 42, active: false, role: 'moderator', score: 91.2, department: 'support', metadata: { source: 'web', tier: 'gold', tags: ['premium', 'verified', 'admin'] } },
  { _id: '6', name: 'Frank', email: 'frank@example.com', age: 33, active: true, role: 'user', score: 78.5, department: 'engineering' },
  { _id: '7', name: 'Grace', email: 'grace@example.com', age: 29, active: true, role: 'admin', score: 99.0, department: 'engineering', metadata: { source: 'desktop', tier: 'platinum', tags: ['premium', 'verified', 'vip', 'early-adopter'] } },
]

const sampleProducts: Product[] = [
  { _id: 'p1', name: 'Laptop Pro', price: 1299.99, category: 'electronics', inStock: true, quantity: 50, rating: 4.5, specs: { brand: 'TechCorp', model: 'LP-2024', features: ['16GB RAM', 'SSD', 'Retina Display'] } },
  { _id: 'p2', name: 'Wireless Mouse', price: 49.99, category: 'electronics', inStock: true, quantity: 200, rating: 4.2, specs: { brand: 'ClickMaster', model: 'WM-100', features: ['Ergonomic', 'Bluetooth'] } },
  { _id: 'p3', name: 'Office Chair', price: 299.99, category: 'furniture', inStock: false, quantity: 0, rating: 4.8, specs: { brand: 'ComfortPlus', model: 'OC-500', features: ['Lumbar Support', 'Adjustable'] } },
  { _id: 'p4', name: 'Monitor 27"', price: 449.99, category: 'electronics', inStock: true, quantity: 30, specs: { brand: 'ViewMax', model: 'VM-27K', features: ['4K', 'HDR'] } },
  { _id: 'p5', name: 'Keyboard RGB', price: 129.99, category: 'electronics', inStock: true, quantity: 100, rating: 4.0, specs: { brand: 'TypeFast', model: 'KB-RGB', features: ['Mechanical', 'RGB', 'Wireless'] } },
  { _id: 'p6', name: 'Standing Desk', price: 599.99, category: 'furniture', inStock: true, quantity: 15, rating: 4.7, specs: { brand: 'WorkWell', model: 'SD-100', features: ['Electric', 'Memory Presets'] } },
  { _id: 'p7', name: 'Webcam HD', price: 79.99, category: 'electronics', inStock: false, quantity: 0, rating: 3.9, specs: { brand: 'StreamPro', model: 'WC-HD', features: ['1080p', 'Auto-focus'] } },
]

// =============================================================================
// EQUALITY FILTER TESTS
// =============================================================================

describe('E2E: Query Equality Filters', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
    await userTable.write(sampleUsers)
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should filter by exact string value', async () => {
    const results = await userTable.query({ name: 'Alice' })

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')
    expect(results[0]._id).toBe('1')
  })

  it('should filter by exact numeric value', async () => {
    const results = await userTable.query({ age: 30 })

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')
  })

  it('should filter by boolean value', async () => {
    const results = await userTable.query({ active: true })

    expect(results.length).toBe(5)
    expect(results.every(r => r.active === true)).toBe(true)
  })

  it('should filter by $eq operator', async () => {
    const results = await userTable.query({ role: { $eq: 'admin' } })

    expect(results.length).toBe(3)
    expect(results.every(r => r.role === 'admin')).toBe(true)
  })

  it('should filter by multiple equality conditions (implicit AND)', async () => {
    const results = await userTable.query({ active: true, role: 'admin' })

    expect(results.length).toBe(2)
    expect(results.every(r => r.active === true && r.role === 'admin')).toBe(true)
  })

  it('should return empty array when no match', async () => {
    const results = await userTable.query({ name: 'NonExistent' })

    expect(results).toHaveLength(0)
  })

  it('should return all rows with empty filter', async () => {
    const results = await userTable.query({})

    expect(results).toHaveLength(sampleUsers.length)
  })

  it('should return all rows when no filter provided', async () => {
    const results = await userTable.query()

    expect(results).toHaveLength(sampleUsers.length)
  })
})

// =============================================================================
// COMPARISON FILTER TESTS
// =============================================================================

describe('E2E: Query Comparison Filters', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
    await userTable.write(sampleUsers)
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  describe('$ne - Not Equal', () => {
    it('should filter out matching values', async () => {
      const results = await userTable.query({ role: { $ne: 'admin' } })

      expect(results.length).toBe(4)
      expect(results.every(r => r.role !== 'admin')).toBe(true)
    })
  })

  describe('$gt - Greater Than', () => {
    it('should filter numeric values greater than threshold', async () => {
      const results = await userTable.query({ age: { $gt: 30 } })

      expect(results.every(r => r.age > 30)).toBe(true)
      expect(results.some(r => r.age === 30)).toBe(false)
    })

    it('should filter floating point values', async () => {
      const results = await userTable.query({ score: { $gt: 90 } })

      expect(results.every(r => r.score > 90)).toBe(true)
    })
  })

  describe('$gte - Greater Than or Equal', () => {
    it('should include equal values', async () => {
      const results = await userTable.query({ age: { $gte: 35 } })

      expect(results.some(r => r.age === 35)).toBe(true)
      expect(results.every(r => r.age >= 35)).toBe(true)
    })
  })

  describe('$lt - Less Than', () => {
    it('should filter numeric values less than threshold', async () => {
      const results = await userTable.query({ age: { $lt: 30 } })

      expect(results.every(r => r.age < 30)).toBe(true)
      expect(results.some(r => r.age === 30)).toBe(false)
    })
  })

  describe('$lte - Less Than or Equal', () => {
    it('should include equal values', async () => {
      const results = await userTable.query({ age: { $lte: 25 } })

      expect(results.some(r => r.age === 25)).toBe(true)
      expect(results.every(r => r.age <= 25)).toBe(true)
    })
  })

  describe('$in - Set Membership', () => {
    it('should match values in array', async () => {
      const results = await userTable.query({ role: { $in: ['admin', 'moderator'] } })

      expect(results.length).toBe(4)
      expect(results.every(r => ['admin', 'moderator'].includes(r.role))).toBe(true)
    })

    it('should match numeric values in array', async () => {
      const results = await userTable.query({ age: { $in: [25, 30, 35] } })

      expect(results.every(r => [25, 30, 35].includes(r.age))).toBe(true)
    })

    it('should return empty for empty $in array', async () => {
      const results = await userTable.query({ role: { $in: [] } })

      expect(results).toHaveLength(0)
    })
  })

  describe('$nin - Not In Set', () => {
    it('should exclude values in array', async () => {
      const results = await userTable.query({ role: { $nin: ['admin', 'moderator'] } })

      expect(results.every(r => !['admin', 'moderator'].includes(r.role))).toBe(true)
    })
  })

  describe('Range queries', () => {
    it('should support $gte and $lte on same field', async () => {
      const results = await userTable.query({ age: { $gte: 28, $lte: 33 } })

      expect(results.every(r => r.age >= 28 && r.age <= 33)).toBe(true)
    })

    it('should support $gt and $lt on same field', async () => {
      const results = await userTable.query({ score: { $gt: 70, $lt: 90 } })

      expect(results.every(r => r.score > 70 && r.score < 90)).toBe(true)
    })
  })
})

// =============================================================================
// LOGICAL OPERATOR TESTS
// =============================================================================

describe('E2E: Query Logical Operators', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
    await userTable.write(sampleUsers)
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  describe('$and - Logical AND', () => {
    it('should match when all conditions are true', async () => {
      const results = await userTable.query({
        $and: [
          { active: true },
          { role: 'admin' },
        ],
      })

      expect(results.length).toBe(2)
      expect(results.every(r => r.active === true && r.role === 'admin')).toBe(true)
    })

    it('should return empty when any condition fails', async () => {
      const results = await userTable.query({
        $and: [
          { active: true },
          { age: { $gt: 100 } },
        ],
      })

      expect(results).toHaveLength(0)
    })
  })

  describe('$or - Logical OR', () => {
    it('should match when any condition is true', async () => {
      const results = await userTable.query({
        $or: [
          { name: 'Alice' },
          { name: 'Bob' },
        ],
      })

      expect(results).toHaveLength(2)
      expect(results.every(r => r.name === 'Alice' || r.name === 'Bob')).toBe(true)
    })

    it('should handle complex $or conditions', async () => {
      const results = await userTable.query({
        $or: [
          { age: { $lt: 26 } },
          { score: { $gt: 95 } },
        ],
      })

      expect(results.every(r => r.age < 26 || r.score > 95)).toBe(true)
    })
  })

  describe('$not - Logical NOT', () => {
    it('should negate a simple condition', async () => {
      const results = await userTable.query({
        $not: { active: true },
      })

      expect(results.every(r => r.active !== true)).toBe(true)
    })

    it('should negate complex conditions', async () => {
      const results = await userTable.query({
        $not: { age: { $gte: 30 } },
      })

      expect(results.every(r => r.age < 30)).toBe(true)
    })
  })

  describe('$nor - Logical NOR', () => {
    it('should match when none of the conditions are true', async () => {
      const results = await userTable.query({
        $nor: [
          { role: 'admin' },
          { role: 'moderator' },
        ],
      })

      expect(results.every(r => r.role !== 'admin' && r.role !== 'moderator')).toBe(true)
    })
  })

  describe('Combined logical operators', () => {
    it('should handle $and with nested $or', async () => {
      const results = await userTable.query({
        $and: [
          { active: true },
          {
            $or: [
              { role: 'admin' },
              { score: { $gt: 90 } },
            ],
          },
        ],
      })

      expect(results.every(r => r.active === true && (r.role === 'admin' || r.score > 90))).toBe(true)
    })

    it('should handle complex nested logical operators', async () => {
      const results = await userTable.query({
        $or: [
          {
            $and: [
              { role: 'admin' },
              { department: 'engineering' },
            ],
          },
          {
            $and: [
              { active: false },
              { score: { $gt: 85 } },
            ],
          },
        ],
      })

      expect(results.every(r =>
        (r.role === 'admin' && r.department === 'engineering') ||
        (r.active === false && r.score > 85)
      )).toBe(true)
    })
  })
})

// =============================================================================
// EXISTENCE AND PATTERN FILTER TESTS
// =============================================================================

describe('E2E: Query Existence and Pattern Filters', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
    await userTable.write(sampleUsers)
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  describe('$exists - Field Existence', () => {
    it('should match documents where field exists', async () => {
      const results = await userTable.query({ metadata: { $exists: true } })

      expect(results.every(r => r.metadata !== undefined)).toBe(true)
    })

    it('should match documents where field does not exist', async () => {
      const results = await userTable.query({ metadata: { $exists: false } })

      expect(results.every(r => r.metadata === undefined)).toBe(true)
    })
  })

  describe('$regex - Pattern Matching', () => {
    it('should match string patterns', async () => {
      const results = await userTable.query({ name: { $regex: '^A' } })

      expect(results.every(r => r.name.startsWith('A'))).toBe(true)
    })

    it('should match email patterns', async () => {
      const results = await userTable.query({ email: { $regex: 'example\\.com$' } })

      expect(results.every(r => r.email.endsWith('example.com'))).toBe(true)
    })

    it('should support RegExp object', async () => {
      const results = await userTable.query({ name: { $regex: /^[A-E]/ } })

      expect(results.every(r => /^[A-E]/.test(r.name))).toBe(true)
    })
  })
})

// =============================================================================
// NESTED FIELD QUERY TESTS
// =============================================================================

describe('E2E: Query Nested Fields', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>
  let productTable: DeltaTable<Product>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
    productTable = new DeltaTable<Product>(storage, 'products')
    await userTable.write(sampleUsers)
    await productTable.write(sampleProducts)
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should access nested object fields with dot notation', async () => {
    const results = await userTable.query({ 'metadata.source': 'web' } as Filter<User>)

    expect(results.every(r => r.metadata?.source === 'web')).toBe(true)
  })

  it('should apply comparison operators on nested fields', async () => {
    const results = await userTable.query({
      'metadata.tier': { $in: ['gold', 'platinum'] },
    } as Filter<User>)

    expect(results.every(r => ['gold', 'platinum'].includes(r.metadata?.tier || ''))).toBe(true)
  })

  it('should handle missing intermediate objects', async () => {
    const results = await userTable.query({ 'metadata.tier': 'gold' } as Filter<User>)

    expect(results.every(r => r.metadata?.tier === 'gold')).toBe(true)
  })

  it('should query nested object in product specs', async () => {
    const results = await productTable.query({ 'specs.brand': 'TechCorp' } as Filter<Product>)

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Laptop Pro')
  })

  it('should combine nested field queries with other filters', async () => {
    const results = await productTable.query({
      category: 'electronics',
      'specs.brand': 'TechCorp',
    } as Filter<Product>)

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Laptop Pro')
  })
})

// =============================================================================
// PROJECTION TESTS
// =============================================================================

describe('E2E: Query Projections', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
    await userTable.write(sampleUsers)
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should select only specified fields (array format)', async () => {
    const results = await userTable.query({}, { projection: ['name', 'age'] })

    expect(results.length).toBe(sampleUsers.length)
    expect(Object.keys(results[0]).sort()).toEqual(['age', 'name'])
    expect(results[0].name).toBeDefined()
    expect(results[0].age).toBeDefined()
    expect((results[0] as any)._id).toBeUndefined()
    expect((results[0] as any).email).toBeUndefined()
  })

  it('should select only specified fields (object format - include)', async () => {
    const results = await userTable.query({}, { projection: { name: 1, email: 1 } })

    expect(results.length).toBe(sampleUsers.length)
    expect(Object.keys(results[0]).sort()).toEqual(['email', 'name'])
  })

  it('should exclude specified fields (object format - exclude)', async () => {
    const results = await userTable.query({}, { projection: { metadata: 0, department: 0 } })

    expect(results.length).toBe(sampleUsers.length)
    expect(results[0]).toHaveProperty('name')
    expect(results[0]).toHaveProperty('_id')
    expect(results[0]).not.toHaveProperty('metadata')
    expect(results[0]).not.toHaveProperty('department')
  })

  it('should handle nested field projection', async () => {
    const results = await userTable.query({}, { projection: ['name', 'metadata.tier'] })

    expect(results.length).toBe(sampleUsers.length)

    // For users with metadata, should have nested tier
    const aliceResult = results.find(r => r.name === 'Alice')
    expect(aliceResult?.metadata).toEqual({ tier: 'gold' })

    // For users without metadata, should not have it
    const dianaResult = results.find(r => r.name === 'Diana')
    expect(dianaResult?.metadata).toBeUndefined()
  })

  it('should combine filter and projection', async () => {
    const results = await userTable.query(
      { active: true },
      { projection: ['name', 'role'] }
    )

    expect(results.length).toBe(5)
    expect(Object.keys(results[0]).sort()).toEqual(['name', 'role'])
    expect(results.every(r => r.name !== undefined && r.role !== undefined)).toBe(true)
  })
})

// =============================================================================
// MULTI-FILE QUERY TESTS
// =============================================================================

describe('E2E: Query Across Multiple Files', () => {
  let testDir: string
  let storage: FileSystemStorage
  let userTable: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    userTable = new DeltaTable<User>(storage, 'users')
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should query across multiple writes (multiple Parquet files)', async () => {
    // Each write creates a new Parquet file
    await userTable.write([sampleUsers[0]])
    await userTable.write([sampleUsers[1], sampleUsers[2]])
    await userTable.write([sampleUsers[3], sampleUsers[4], sampleUsers[5]])
    await userTable.write([sampleUsers[6]])

    const results = await userTable.query()

    expect(results).toHaveLength(sampleUsers.length)
  })

  it('should apply filters across all Parquet files', async () => {
    // Write with different active statuses in different files
    await userTable.write(sampleUsers.filter(u => u.active))
    await userTable.write(sampleUsers.filter(u => !u.active))

    const activeResults = await userTable.query({ active: true })
    const inactiveResults = await userTable.query({ active: false })

    const expectedActive = sampleUsers.filter(u => u.active).length
    const expectedInactive = sampleUsers.filter(u => !u.active).length

    expect(activeResults).toHaveLength(expectedActive)
    expect(inactiveResults).toHaveLength(expectedInactive)
  })

  it('should handle removed files after delete', async () => {
    await userTable.write(sampleUsers.slice(0, 3))
    await userTable.delete({ _id: '1' })
    await userTable.write(sampleUsers.slice(3))

    const results = await userTable.query()

    // Should not include deleted user
    expect(results.find(r => r._id === '1')).toBeUndefined()
    expect(results.length).toBe(sampleUsers.length - 1)
  })
})

// =============================================================================
// EDGE CASES AND COMPLEX QUERIES
// =============================================================================

describe('E2E: Query Edge Cases', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should handle empty table query', async () => {
    const table = new DeltaTable<User>(storage, 'empty_users')

    const results = await table.query()

    expect(results).toHaveLength(0)
  })

  it('should handle single row table', async () => {
    const table = new DeltaTable<User>(storage, 'single_user')
    await table.write([sampleUsers[0]])

    const results = await table.query({ name: 'Alice' })

    expect(results).toHaveLength(1)
  })

  it('should handle special characters in string values', async () => {
    interface SpecialDoc {
      _id: string
      text: string
    }

    const table = new DeltaTable<SpecialDoc>(storage, 'special')
    await table.write([
      { _id: '1', text: "Hello 'World'" },
      { _id: '2', text: 'Line1\nLine2' },
      { _id: '3', text: 'Tab\tSeparated' },
      { _id: '4', text: 'Unicode: \u00e9\u00e8\u00ea' },
    ])

    const results = await table.query()

    expect(results).toHaveLength(4)
    expect(results.find(r => r._id === '1')?.text).toBe("Hello 'World'")
    expect(results.find(r => r._id === '2')?.text).toBe('Line1\nLine2')
  })

  it('should handle very long string values', async () => {
    interface LongDoc {
      _id: string
      content: string
    }

    const table = new DeltaTable<LongDoc>(storage, 'long_strings')
    const longString = 'x'.repeat(100000)

    await table.write([{ _id: '1', content: longString }])

    const results = await table.query()

    expect(results[0].content).toBe(longString)
  })

  it('should handle numeric precision', async () => {
    interface PrecisionDoc {
      _id: string
      floatValue: number
      intValue: number
    }

    const table = new DeltaTable<PrecisionDoc>(storage, 'precision')
    await table.write([
      { _id: '1', floatValue: 0.1 + 0.2, intValue: 42 },
      { _id: '2', floatValue: 3.14159265358979, intValue: 2147483647 },
    ])

    const results = await table.query()

    expect(results[0].floatValue).toBeCloseTo(0.3, 10)
    expect(results[1].floatValue).toBeCloseTo(3.14159265358979, 10)
  })

  it('should handle mixed type arrays in variant columns', async () => {
    interface MixedArrayDoc {
      _id: string
      mixed: (string | number | boolean | null)[]
    }

    const table = new DeltaTable<MixedArrayDoc>(storage, 'mixed_arrays')
    await table.write([
      { _id: '1', mixed: ['hello', 42, true, null, 3.14] },
    ])

    const results = await table.query()

    expect(results[0].mixed).toEqual(['hello', 42, true, null, 3.14])
  })

  it('should query after multiple CRUD operations', async () => {
    const table = new DeltaTable<User>(storage, 'crud_users')

    // Create
    await table.write(sampleUsers.slice(0, 3))

    // Update
    await table.update({ name: 'Alice' }, { score: 100 })

    // Create more
    await table.write(sampleUsers.slice(3, 5))

    // Delete
    await table.delete({ name: 'Bob' })

    // Complex query
    const results = await table.query({
      $and: [
        { active: true },
        { score: { $gte: 65 } },
      ],
    })

    // Alice (updated score=100) and Diana (score=65)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Diana'])
  })
})

// =============================================================================
// PERFORMANCE SCENARIOS
// =============================================================================

describe('E2E: Query Performance Scenarios', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should handle large number of rows', async () => {
    const table = new DeltaTable<{ _id: string; value: number; category: string }>(storage, 'large')

    // Generate 5000 rows
    const rows = Array.from({ length: 5000 }, (_, i) => ({
      _id: `id-${i}`,
      value: i,
      category: i % 10 === 0 ? 'special' : 'normal',
    }))

    await table.write(rows)

    // Filter query
    const results = await table.query({ category: 'special' })

    expect(results).toHaveLength(500)
  })

  it('should handle selective filter efficiently', async () => {
    const table = new DeltaTable<{ _id: string; value: number; rare: boolean }>(storage, 'selective')

    // Generate data with rare category
    const rows = Array.from({ length: 1000 }, (_, i) => ({
      _id: `id-${i}`,
      value: i,
      rare: i < 5, // Only 5 rare items
    }))

    await table.write(rows)

    const results = await table.query({ rare: true })

    expect(results).toHaveLength(5)
  })
})
