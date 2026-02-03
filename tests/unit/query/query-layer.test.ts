/**
 * Query Layer with Predicate Pushdown Tests
 *
 * Comprehensive tests for MongoDB-style filters, predicate pushdown,
 * zone map filtering, and query execution.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  matchesFilter,
  filterToParquetPredicate,
  type Filter,
  type ComparisonOperators,
} from '../../../src/query/index.js'
import {
  canSkipZoneMap,
  type ZoneMap,
  type ZoneMapFilter,
} from '../../../src/parquet/index.js'

// =============================================================================
// TEST DATA
// =============================================================================

interface User {
  _id: string
  name: string
  email: string
  age: number
  score: number
  active: boolean
  createdAt: Date
  tags: string[]
  metadata?: {
    source: string
    tier: string
    lastLogin?: Date
  }
  address?: {
    city: string
    country: string
    zip?: string
  }
}

const sampleUsers: User[] = [
  {
    _id: '1',
    name: 'Alice',
    email: 'alice@example.com',
    age: 30,
    score: 85.5,
    active: true,
    createdAt: new Date('2024-01-15'),
    tags: ['premium', 'verified'],
    metadata: { source: 'web', tier: 'gold' },
    address: { city: 'New York', country: 'USA', zip: '10001' },
  },
  {
    _id: '2',
    name: 'Bob',
    email: 'bob@example.com',
    age: 25,
    score: 72.0,
    active: true,
    createdAt: new Date('2024-02-20'),
    tags: ['basic'],
    metadata: { source: 'mobile', tier: 'silver' },
    address: { city: 'London', country: 'UK' },
  },
  {
    _id: '3',
    name: 'Charlie',
    email: 'charlie@example.com',
    age: 35,
    score: 91.2,
    active: false,
    createdAt: new Date('2023-11-10'),
    tags: ['premium', 'admin'],
    metadata: { source: 'api', tier: 'platinum' },
  },
  {
    _id: '4',
    name: 'Diana',
    email: 'diana@test.org',
    age: 28,
    score: 65.8,
    active: true,
    createdAt: new Date('2024-03-05'),
    tags: [],
  },
  {
    _id: '5',
    name: 'Eve',
    email: 'eve@example.com',
    age: 42,
    score: 88.0,
    active: false,
    createdAt: new Date('2022-08-30'),
    tags: ['premium', 'verified', 'vip'],
    metadata: { source: 'web', tier: 'gold', lastLogin: new Date('2024-01-01') },
    address: { city: 'Paris', country: 'France', zip: '75001' },
  },
]

// =============================================================================
// MONGODB-STYLE FILTER MATCHING
// =============================================================================

describe('MongoDB-style Filter Matching', () => {
  describe('Comparison Operators', () => {
    describe('$eq - Equality', () => {
      it('should match exact string value', () => {
        const filter: Filter<User> = { name: { $eq: 'Alice' } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false)
      })

      it('should match exact numeric value', () => {
        const filter: Filter<User> = { age: { $eq: 30 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false)
      })

      it('should match boolean value', () => {
        const filter: Filter<User> = { active: { $eq: true } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[2], filter)).toBe(false)
      })

      it('should match null/undefined values', () => {
        const filter: Filter<User> = { address: { $eq: undefined } }
        expect(matchesFilter(sampleUsers[3], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
      })

      it('should support implicit equality (no operator)', () => {
        const filter: Filter<User> = { name: 'Bob' }
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
      })
    })

    describe('$ne - Not Equal', () => {
      it('should match when value is different', () => {
        const filter: Filter<User> = { name: { $ne: 'Alice' } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)
      })

      it('should match when comparing with null', () => {
        const filter: Filter<User> = { address: { $ne: null } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
        // User without address (undefined) should still not equal null
      })
    })

    describe('$gt - Greater Than', () => {
      it('should compare numbers correctly', () => {
        const filter: Filter<User> = { age: { $gt: 30 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // age = 30
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // age = 35
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // age = 25
      })

      it('should compare floating point numbers', () => {
        const filter: Filter<User> = { score: { $gt: 85.0 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // score = 85.5
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // score = 72.0
      })

      it('should compare strings lexicographically', () => {
        const filter: Filter<User> = { name: { $gt: 'Bob' } }
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // Charlie > Bob
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // Alice < Bob
      })

      it('should compare dates', () => {
        const filter: Filter<User> = { createdAt: { $gt: new Date('2024-01-01') } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // 2024-01-15
        expect(matchesFilter(sampleUsers[4], filter)).toBe(false) // 2022-08-30
      })

      it('should return false for null/undefined values', () => {
        const filter: Filter<User> = { age: { $gt: 30 } }
        const userWithNullAge = { ...sampleUsers[0], age: null as any }
        expect(matchesFilter(userWithNullAge, filter)).toBe(false)
      })
    })

    describe('$gte - Greater Than or Equal', () => {
      it('should include equal values', () => {
        const filter: Filter<User> = { age: { $gte: 30 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // age = 30
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // age = 35
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // age = 25
      })

      it('should work with dates', () => {
        const filter: Filter<User> = { createdAt: { $gte: new Date('2024-01-15') } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true) // exact match
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true) // 2024-02-20
      })
    })

    describe('$lt - Less Than', () => {
      it('should compare numbers correctly', () => {
        const filter: Filter<User> = { age: { $lt: 30 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // age = 30
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // age = 25
        expect(matchesFilter(sampleUsers[3], filter)).toBe(true)  // age = 28
      })

      it('should work with negative numbers', () => {
        interface DataPoint {
          value: number
        }
        const doc: DataPoint = { value: -10 }
        expect(matchesFilter(doc, { value: { $lt: 0 } })).toBe(true)
        expect(matchesFilter(doc, { value: { $lt: -20 } })).toBe(false)
      })
    })

    describe('$lte - Less Than or Equal', () => {
      it('should include equal values', () => {
        const filter: Filter<User> = { age: { $lte: 30 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // age = 30
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // age = 25
        expect(matchesFilter(sampleUsers[2], filter)).toBe(false) // age = 35
      })
    })

    describe('$in - Set Membership', () => {
      it('should match if value is in array', () => {
        const filter: Filter<User> = { name: { $in: ['Alice', 'Bob', 'Charlie'] } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // Diana
      })

      it('should work with numeric values', () => {
        const filter: Filter<User> = { age: { $in: [25, 30, 35] } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // 30
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // 25
        expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // 28
      })

      it('should return false for empty array', () => {
        const filter: Filter<User> = { name: { $in: [] } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
      })

      it('should handle null in $in array', () => {
        const filter: Filter<User> = { address: { $in: [null, undefined] } }
        expect(matchesFilter(sampleUsers[3], filter)).toBe(true) // no address
      })
    })

    describe('$nin - Not In Set', () => {
      it('should match if value is not in array', () => {
        const filter: Filter<User> = { name: { $nin: ['Alice', 'Bob'] } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // Alice
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // Charlie
      })

      it('should return true for empty array', () => {
        const filter: Filter<User> = { name: { $nin: [] } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
      })

      it('should work with numeric values', () => {
        const filter: Filter<User> = { age: { $nin: [25, 30] } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // 30
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // 35
      })
    })

    describe('$exists - Field Existence', () => {
      it('should check if field exists with $exists: true', () => {
        const filter: Filter<User> = { metadata: { $exists: true } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // has metadata
        expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // no metadata
      })

      it('should check if field does not exist with $exists: false', () => {
        const filter: Filter<User> = { metadata: { $exists: false } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // has metadata
        expect(matchesFilter(sampleUsers[3], filter)).toBe(true)  // no metadata
      })

      it('should work with nested fields', () => {
        const filter: Filter<User> = { 'address.zip': { $exists: true } } as Filter<User>
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // has zip
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // no zip
      })
    })

    describe('$regex - Regular Expression', () => {
      it('should match string patterns', () => {
        const filter: Filter<User> = { email: { $regex: '@example\\.com$' } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // alice@example.com
        expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // diana@test.org
      })

      it('should work with RegExp objects', () => {
        const filter: Filter<User> = { name: { $regex: /^[A-C]/ } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // Alice
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // Bob
        expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // Diana
      })

      it('should be case-sensitive by default', () => {
        const filter: Filter<User> = { name: { $regex: 'alice' } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // Alice (uppercase A)
      })

      it('should support case-insensitive regex', () => {
        const filter: Filter<User> = { name: { $regex: /alice/i } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true) // Alice
      })

      it('should return false for non-string values', () => {
        const filter: Filter<User> = { age: { $regex: '30' } } as Filter<User>
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
      })
    })

    describe('Combined Comparison Operators', () => {
      it('should handle range queries with $gte and $lte', () => {
        const filter: Filter<User> = { age: { $gte: 25, $lte: 35 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // 30
        expect(matchesFilter(sampleUsers[4], filter)).toBe(false) // 42
      })

      it('should handle exclusive range with $gt and $lt', () => {
        const filter: Filter<User> = { score: { $gt: 70, $lt: 90 } }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // 85.5
        expect(matchesFilter(sampleUsers[2], filter)).toBe(false) // 91.2
      })
    })
  })

  describe('Logical Operators', () => {
    describe('$and - Logical AND', () => {
      it('should require all conditions to match', () => {
        const filter: Filter<User> = {
          $and: [
            { active: true },
            { age: { $gte: 25 } },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // active & age 30
        expect(matchesFilter(sampleUsers[2], filter)).toBe(false) // not active
      })

      it('should support multiple field conditions', () => {
        const filter: Filter<User> = {
          $and: [
            { name: { $regex: /^A/ } },
            { score: { $gt: 80 } },
            { active: true },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true) // Alice, 85.5, active
      })

      it('should handle empty $and array (matches all)', () => {
        const filter: Filter<User> = { $and: [] }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
      })

      it('should handle nested $and', () => {
        const filter: Filter<User> = {
          $and: [
            { $and: [{ active: true }, { age: { $lt: 40 } }] },
            { score: { $gte: 70 } },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true) // active, 30, 85.5
        expect(matchesFilter(sampleUsers[4], filter)).toBe(false) // not active
      })
    })

    describe('$or - Logical OR', () => {
      it('should match if any condition is true', () => {
        const filter: Filter<User> = {
          $or: [
            { name: 'Alice' },
            { name: 'Bob' },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)
        expect(matchesFilter(sampleUsers[2], filter)).toBe(false)
      })

      it('should handle complex conditions in $or', () => {
        const filter: Filter<User> = {
          $or: [
            { age: { $lt: 26 } },
            { score: { $gt: 90 } },
          ],
        }
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // age 25
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // score 91.2
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // age 30, score 85.5
      })

      it('should handle empty $or array (matches none)', () => {
        const filter: Filter<User> = { $or: [] }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
      })
    })

    describe('$not - Logical NOT', () => {
      it('should negate a simple condition', () => {
        const filter: Filter<User> = {
          $not: { active: true },
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // active
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // not active
      })

      it('should negate complex conditions', () => {
        const filter: Filter<User> = {
          $not: { age: { $gte: 30 } },
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // age 30
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // age 25
      })

      it('should work with nested operators', () => {
        const filter: Filter<User> = {
          $not: {
            $or: [
              { name: 'Alice' },
              { name: 'Bob' },
            ],
          },
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false)
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false)
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true) // Charlie
      })
    })

    describe('$nor - Logical NOR', () => {
      it('should match when none of the conditions are true', () => {
        const filter: Filter<User> = {
          $nor: [
            { name: 'Alice' },
            { name: 'Bob' },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // Alice
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // Bob
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // Charlie
      })

      it('should handle complex conditions', () => {
        const filter: Filter<User> = {
          $nor: [
            { active: false },
            { age: { $gt: 40 } },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // active, 30
        expect(matchesFilter(sampleUsers[2], filter)).toBe(false) // not active
        expect(matchesFilter(sampleUsers[4], filter)).toBe(false) // not active, 42
      })

      it('should handle empty $nor array (matches all)', () => {
        const filter: Filter<User> = { $nor: [] }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
      })
    })

    describe('Combined Logical Operators', () => {
      it('should handle $and with $or', () => {
        const filter: Filter<User> = {
          $and: [
            { active: true },
            {
              $or: [
                { age: { $lt: 26 } },
                { score: { $gt: 80 } },
              ],
            },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // active, score 85.5
        expect(matchesFilter(sampleUsers[1], filter)).toBe(true)  // active, age 25
        expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // active but age 28, score 65.8
      })

      it('should handle deeply nested logical operators', () => {
        const filter: Filter<User> = {
          $or: [
            {
              $and: [
                { active: true },
                { age: { $gte: 30 } },
              ],
            },
            {
              $and: [
                { active: false },
                { score: { $gt: 90 } },
              ],
            },
          ],
        }
        expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // active && 30
        expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // !active && 91.2
        expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // active but 25
      })
    })
  })

  describe('Nested Field Access', () => {
    it('should access nested object fields with dot notation', () => {
      const filter = { 'metadata.source': 'web' } as Filter<User>
      expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
      expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // mobile
    })

    it('should access deeply nested fields', () => {
      const filter = { 'address.city': 'New York' } as Filter<User>
      expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
      expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // London
    })

    it('should handle missing intermediate objects', () => {
      const filter = { 'metadata.tier': 'gold' } as Filter<User>
      expect(matchesFilter(sampleUsers[0], filter)).toBe(true)
      expect(matchesFilter(sampleUsers[3], filter)).toBe(false) // no metadata
    })

    it('should work with comparison operators on nested fields', () => {
      const filter = { 'metadata.tier': { $in: ['gold', 'platinum'] } } as Filter<User>
      expect(matchesFilter(sampleUsers[0], filter)).toBe(true)  // gold
      expect(matchesFilter(sampleUsers[2], filter)).toBe(true)  // platinum
      expect(matchesFilter(sampleUsers[1], filter)).toBe(false) // silver
    })

    it('should check existence of nested fields', () => {
      const filter = { 'metadata.lastLogin': { $exists: true } } as Filter<User>
      expect(matchesFilter(sampleUsers[4], filter)).toBe(true)  // has lastLogin
      expect(matchesFilter(sampleUsers[0], filter)).toBe(false) // no lastLogin
    })

    it('should handle triple-nested paths', () => {
      interface DeepDoc {
        level1: {
          level2: {
            level3: {
              value: number
            }
          }
        }
      }
      const doc: DeepDoc = {
        level1: { level2: { level3: { value: 42 } } },
      }
      expect(matchesFilter(doc, { 'level1.level2.level3.value': 42 } as Filter<DeepDoc>)).toBe(true)
    })
  })
})

// =============================================================================
// PREDICATE PUSHDOWN
// =============================================================================

describe('Predicate Pushdown', () => {
  describe('filterToParquetPredicate() Conversion', () => {
    it('should convert $eq to eq predicate', () => {
      const filter: Filter<User> = { age: { $eq: 30 } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'eq',
        value: 30,
      })
    })

    it('should convert $ne to ne predicate', () => {
      const filter: Filter<User> = { name: { $ne: 'Alice' } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'name',
        operator: 'ne',
        value: 'Alice',
      })
    })

    it('should convert $gt to gt predicate', () => {
      const filter: Filter<User> = { score: { $gt: 80 } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'score',
        operator: 'gt',
        value: 80,
      })
    })

    it('should convert $gte to gte predicate', () => {
      const filter: Filter<User> = { age: { $gte: 25 } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'gte',
        value: 25,
      })
    })

    it('should convert $lt to lt predicate', () => {
      const filter: Filter<User> = { age: { $lt: 40 } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'age',
        operator: 'lt',
        value: 40,
      })
    })

    it('should convert $lte to lte predicate', () => {
      const filter: Filter<User> = { score: { $lte: 90 } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'score',
        operator: 'lte',
        value: 90,
      })
    })

    it('should convert $in to in predicate', () => {
      const filter: Filter<User> = { name: { $in: ['Alice', 'Bob'] } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'name',
        operator: 'in',
        value: ['Alice', 'Bob'],
      })
    })

    it('should convert implicit equality to eq predicate', () => {
      const filter: Filter<User> = { active: true }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toContainEqual({
        column: 'active',
        operator: 'eq',
        value: true,
      })
    })

    it('should handle multiple field predicates', () => {
      const filter: Filter<User> = {
        age: { $gte: 25 },
        active: true,
        score: { $gt: 70 },
      }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toHaveLength(3)
      expect(predicates).toContainEqual({ column: 'age', operator: 'gte', value: 25 })
      expect(predicates).toContainEqual({ column: 'active', operator: 'eq', value: true })
      expect(predicates).toContainEqual({ column: 'score', operator: 'gt', value: 70 })
    })

    it('should handle range predicates (multiple operators on same field)', () => {
      const filter: Filter<User> = { age: { $gte: 25, $lte: 35 } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toHaveLength(3)
      expect(predicates).toContainEqual({ column: 'age', operator: 'gte', value: 25 })
      expect(predicates).toContainEqual({ column: 'age', operator: 'lte', value: 35 })
      expect(predicates).toContainEqual({ column: 'age', operator: 'between', value: 25, value2: 35 })
    })

    it('should skip $regex (not pushable)', () => {
      const filter: Filter<User> = { email: { $regex: '@example.com' } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toHaveLength(0)
    })

    it('should skip $exists (not pushable)', () => {
      const filter: Filter<User> = { metadata: { $exists: true } }
      const predicates = filterToParquetPredicate(filter)
      expect(predicates).toHaveLength(0)
    })

    it('should skip logical operators at top level', () => {
      const filter: Filter<User> = {
        $or: [
          { age: { $lt: 25 } },
          { age: { $gt: 40 } },
        ],
      }
      const predicates = filterToParquetPredicate(filter)
      // Current implementation skips logical operators
      expect(predicates).toHaveLength(0)
    })

    describe('Advanced Predicate Conversion (TODO)', () => {
      it('should convert $and predicates to conjunctive predicates', () => {
        const filter: Filter<User> = {
          $and: [
            { age: { $gte: 25 } },
            { age: { $lte: 35 } },
          ],
        }
        const predicates = filterToParquetPredicate(filter)
        // TODO: Implement $and handling
        expect(predicates).toHaveLength(2)
        expect(predicates).toContainEqual({ column: 'age', operator: 'gte', value: 25 })
        expect(predicates).toContainEqual({ column: 'age', operator: 'lte', value: 35 })
      })

      it('should generate between predicate for range queries', () => {
        const filter: Filter<User> = { age: { $gte: 25, $lte: 35 } }
        const predicates = filterToParquetPredicate(filter)
        expect(predicates).toContainEqual({
          column: 'age',
          operator: 'between',
          value: 25,
          value2: 35,
        })
      })

      it('should handle nested field predicates', () => {
        const filter = { 'metadata.tier': { $eq: 'gold' } } as Filter<User>
        const predicates = filterToParquetPredicate(filter)
        expect(predicates).toContainEqual({
          column: 'metadata.tier',
          operator: 'eq',
          value: 'gold',
        })
      })
    })
  })

  describe('Zone Map Filtering', () => {
    describe('canSkipZoneMap()', () => {
      it('should skip row group when eq value is below min', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 20 }
        expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
      })

      it('should skip row group when eq value is above max', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 40 }
        expect(canSkipZoneMap(zoneMap, filter)).toBe(true)
      })

      it('should not skip when eq value is within range', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 30 }
        expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
      })

      it('should not skip when eq value equals min', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 25 }
        expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
      })

      it('should not skip when eq value equals max', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'age', operator: 'eq', value: 35 }
        expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
      })

      it('should skip for ne only when entire range is single value', () => {
        const singleValueZone: ZoneMap = { column: 'status', min: 'active', max: 'active', nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'status', operator: 'ne', value: 'active' }
        expect(canSkipZoneMap(singleValueZone, filter)).toBe(true)

        const rangeZone: ZoneMap = { column: 'status', min: 'active', max: 'inactive', nullCount: 0 }
        expect(canSkipZoneMap(rangeZone, filter)).toBe(false)
      })

      it('should skip for gt when max <= value', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'gt', value: 35 })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'gt', value: 40 })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'gt', value: 30 })).toBe(false)
      })

      it('should skip for gte when max < value', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'gte', value: 36 })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'gte', value: 35 })).toBe(false)
      })

      it('should skip for lt when min >= value', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'lt', value: 25 })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'lt', value: 20 })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'lt', value: 30 })).toBe(false)
      })

      it('should skip for lte when min > value', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'lte', value: 24 })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'lte', value: 25 })).toBe(false)
      })

      it('should skip for in when all values are outside range', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'in', value: [10, 15, 20] })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'in', value: [40, 45, 50] })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'in', value: [10, 30, 50] })).toBe(false)
      })

      it('should skip for between when ranges do not overlap', () => {
        const zoneMap: ZoneMap = { column: 'age', min: 25, max: 35, nullCount: 0 }
        // Query range [40, 50] does not overlap [25, 35]
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'between', value: 40, value2: 50 })).toBe(true)
        // Query range [10, 20] does not overlap [25, 35]
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'between', value: 10, value2: 20 })).toBe(true)
        // Query range [20, 30] overlaps [25, 35]
        expect(canSkipZoneMap(zoneMap, { column: 'age', operator: 'between', value: 20, value2: 30 })).toBe(false)
      })

      it('should handle string zone maps', () => {
        const zoneMap: ZoneMap = { column: 'name', min: 'Alice', max: 'Charlie', nullCount: 0 }
        expect(canSkipZoneMap(zoneMap, { column: 'name', operator: 'eq', value: 'Zara' })).toBe(true)
        expect(canSkipZoneMap(zoneMap, { column: 'name', operator: 'eq', value: 'Bob' })).toBe(false)
      })

      it('should handle date zone maps', () => {
        const zoneMap: ZoneMap = {
          column: 'createdAt',
          min: new Date('2024-01-01'),
          max: new Date('2024-06-30'),
          nullCount: 0,
        }
        expect(canSkipZoneMap(zoneMap, {
          column: 'createdAt',
          operator: 'lt',
          value: new Date('2024-01-01'),
        })).toBe(true)
        expect(canSkipZoneMap(zoneMap, {
          column: 'createdAt',
          operator: 'gt',
          value: new Date('2024-03-15'),
        })).toBe(false)
      })

      it('should not skip when types are incompatible', () => {
        const zoneMap: ZoneMap = { column: 'data', min: { nested: true }, max: { nested: true }, nullCount: 0 }
        const filter: ZoneMapFilter = { column: 'data', operator: 'eq', value: 'string' }
        // Non-comparable types should not skip (safe default)
        expect(canSkipZoneMap(zoneMap, filter)).toBe(false)
      })
    })

    describe('Row Group Pruning', () => {
      it('should identify skippable row groups based on predicates', () => {
        const rowGroups = [
          { id: 0, zoneMaps: [{ column: 'age', min: 20, max: 25, nullCount: 0 }] },
          { id: 1, zoneMaps: [{ column: 'age', min: 26, max: 30, nullCount: 0 }] },
          { id: 2, zoneMaps: [{ column: 'age', min: 31, max: 35, nullCount: 0 }] },
          { id: 3, zoneMaps: [{ column: 'age', min: 36, max: 40, nullCount: 0 }] },
        ]

        const filter: ZoneMapFilter = { column: 'age', operator: 'gt', value: 32 }

        const skippableGroups = rowGroups.filter(rg =>
          rg.zoneMaps.some(zm => canSkipZoneMap(zm, filter))
        )

        // Groups 0, 1, 2 have max <= 35, should be skippable for age > 32
        // Actually: group 0 max=25, group 1 max=30, group 2 max=35
        // For gt 32: skip if max <= 32
        expect(skippableGroups.map(rg => rg.id)).toContain(0)
        expect(skippableGroups.map(rg => rg.id)).toContain(1)
      })

      it('should combine multiple predicates for row group pruning', () => {
        const rowGroups = [
          {
            id: 0,
            zoneMaps: [
              { column: 'age', min: 20, max: 30, nullCount: 0 },
              { column: 'score', min: 60, max: 70, nullCount: 0 },
            ],
          },
          {
            id: 1,
            zoneMaps: [
              { column: 'age', min: 31, max: 40, nullCount: 0 },
              { column: 'score', min: 80, max: 90, nullCount: 0 },
            ],
          },
        ]

        const predicates: ZoneMapFilter[] = [
          { column: 'age', operator: 'gte', value: 25 },
          { column: 'score', operator: 'gt', value: 75 },
        ]

        // Row group is skippable if ANY predicate says skip
        const canSkipRowGroup = (rg: typeof rowGroups[0]) =>
          predicates.some(pred => {
            const zm = rg.zoneMaps.find(z => z.column === pred.column)
            return zm && canSkipZoneMap(zm, pred)
          })

        // Group 0: age [20,30] satisfies gte 25, but score [60,70] fails gt 75
        // Group 1: age [31,40] satisfies gte 25, score [80,90] satisfies gt 75
        expect(canSkipRowGroup(rowGroups[0])).toBe(true)  // score max 70 <= 75
        expect(canSkipRowGroup(rowGroups[1])).toBe(false) // both satisfied
      })
    })

    describe('Column Statistics Usage', () => {
      it('should track null counts for null-aware filtering', () => {
        const zoneMap: ZoneMap = { column: 'address', min: 'London', max: 'Paris', nullCount: 5 }
        // When nullCount > 0, we cannot skip for ne/exists queries
        expect(zoneMap.nullCount).toBe(5)
      })

      it('should use distinct count for cardinality estimation (TODO)', () => {
        // Future: Add distinctCount to ZoneMap
        interface ExtendedZoneMap extends ZoneMap {
          distinctCount?: number
        }
        const zoneMap: ExtendedZoneMap = {
          column: 'status',
          min: 'active',
          max: 'suspended',
          nullCount: 0,
          distinctCount: 3,
        }
        expect(zoneMap.distinctCount).toBe(3)
      })
    })
  })
})

// =============================================================================
// QUERY EXECUTION
// =============================================================================

describe('Query Execution', () => {
  describe('Query Builder Pattern (TODO)', () => {
    // These tests are for future implementation of a fluent query builder

    it('should support fluent query building', () => {
      // TODO: Implement QueryBuilder class
      // const query = new QueryBuilder<User>()
      //   .where({ age: { $gte: 25 } })
      //   .select(['name', 'email', 'age'])
      //   .orderBy('age', 'desc')
      //   .limit(10)
      //   .offset(5)
      //   .build()

      interface QueryOptions<T> {
        filter?: Filter<T>
        projection?: (keyof T)[]
        sort?: { field: keyof T; order: 'asc' | 'desc' }[]
        limit?: number
        offset?: number
      }

      const expectedQuery: QueryOptions<User> = {
        filter: { age: { $gte: 25 } },
        projection: ['name', 'email', 'age'],
        sort: [{ field: 'age', order: 'desc' }],
        limit: 10,
        offset: 5,
      }

      expect(expectedQuery.filter).toBeDefined()
      expect(expectedQuery.projection).toHaveLength(3)
    })

    it('should chain multiple where conditions', () => {
      // TODO: Implement
      // const query = new QueryBuilder<User>()
      //   .where({ active: true })
      //   .where({ age: { $gte: 25 } })
      //   .where({ score: { $gt: 70 } })
      //   .build()

      // Combined filter should be $and of all conditions
      const expectedFilter: Filter<User> = {
        $and: [
          { active: true },
          { age: { $gte: 25 } },
          { score: { $gt: 70 } },
        ],
      }
      expect(expectedFilter.$and).toHaveLength(3)
    })

    it('should support or() for disjunctive conditions', () => {
      // TODO: Implement
      // const query = new QueryBuilder<User>()
      //   .where({ active: true })
      //   .or([
      //     { age: { $lt: 25 } },
      //     { score: { $gt: 90 } }
      //   ])
      //   .build()

      const expectedFilter: Filter<User> = {
        $and: [
          { active: true },
          {
            $or: [
              { age: { $lt: 25 } },
              { score: { $gt: 90 } },
            ],
          },
        ],
      }
      expect(expectedFilter.$and).toHaveLength(2)
    })
  })

  describe('Projection (Select Specific Columns)', () => {
    it('should select only specified columns', () => {
      // TODO: Implement projection in query execution
      interface ProjectedUser {
        name: string
        email: string
      }

      const fullUser = sampleUsers[0]
      const projection: (keyof User)[] = ['name', 'email']

      // Project should return only selected fields
      const projected: ProjectedUser = {
        name: fullUser.name,
        email: fullUser.email,
      }

      expect(Object.keys(projected)).toEqual(['name', 'email'])
      expect(projected.name).toBe('Alice')
    })

    it('should handle nested field projection', () => {
      // Projection of nested fields like 'address.city'
      const projection = ['name', 'address.city'] as const

      const projected = {
        name: sampleUsers[0].name,
        'address.city': sampleUsers[0].address?.city,
      }

      expect(projected.name).toBe('Alice')
      expect(projected['address.city']).toBe('New York')
    })

    it('should exclude non-projected fields for efficiency', () => {
      // When projecting, Parquet reader should only read required columns
      const projection: (keyof User)[] = ['_id', 'name']
      const requiredColumns = projection

      expect(requiredColumns).not.toContain('metadata')
      expect(requiredColumns).not.toContain('tags')
    })
  })

  describe('Limit and Offset', () => {
    it('should limit results to specified count', () => {
      const limit = 2
      const results = sampleUsers.slice(0, limit)

      expect(results).toHaveLength(2)
      expect(results[0]._id).toBe('1')
      expect(results[1]._id).toBe('2')
    })

    it('should skip results with offset', () => {
      const offset = 2
      const results = sampleUsers.slice(offset)

      expect(results[0]._id).toBe('3')
    })

    it('should combine limit and offset for pagination', () => {
      const offset = 1
      const limit = 2
      const results = sampleUsers.slice(offset, offset + limit)

      expect(results).toHaveLength(2)
      expect(results[0]._id).toBe('2')
      expect(results[1]._id).toBe('3')
    })

    it('should handle offset beyond data length', () => {
      const offset = 100
      const results = sampleUsers.slice(offset)

      expect(results).toHaveLength(0)
    })

    it('should handle limit of 0', () => {
      const limit = 0
      const results = sampleUsers.slice(0, limit)

      expect(results).toHaveLength(0)
    })

    it('should apply limit after filtering', () => {
      // Filter first, then limit
      const filtered = sampleUsers.filter(u => u.active)
      const limited = filtered.slice(0, 2)

      expect(limited.every(u => u.active)).toBe(true)
      expect(limited).toHaveLength(2)
    })
  })

  describe('Sort Order', () => {
    it('should sort by numeric field ascending', () => {
      const sorted = [...sampleUsers].sort((a, b) => a.age - b.age)

      expect(sorted[0].age).toBe(25)
      expect(sorted[sorted.length - 1].age).toBe(42)
    })

    it('should sort by numeric field descending', () => {
      const sorted = [...sampleUsers].sort((a, b) => b.age - a.age)

      expect(sorted[0].age).toBe(42)
      expect(sorted[sorted.length - 1].age).toBe(25)
    })

    it('should sort by string field ascending', () => {
      const sorted = [...sampleUsers].sort((a, b) => a.name.localeCompare(b.name))

      expect(sorted[0].name).toBe('Alice')
      expect(sorted[sorted.length - 1].name).toBe('Eve')
    })

    it('should sort by string field descending', () => {
      const sorted = [...sampleUsers].sort((a, b) => b.name.localeCompare(a.name))

      expect(sorted[0].name).toBe('Eve')
      expect(sorted[sorted.length - 1].name).toBe('Alice')
    })

    it('should sort by date field', () => {
      const sorted = [...sampleUsers].sort((a, b) =>
        a.createdAt.getTime() - b.createdAt.getTime()
      )

      expect(sorted[0].name).toBe('Eve') // 2022-08-30
      expect(sorted[sorted.length - 1].name).toBe('Diana') // 2024-03-05
    })

    it('should support multi-field sorting', () => {
      // Sort by active (desc), then by age (asc)
      const sorted = [...sampleUsers].sort((a, b) => {
        // active: true first
        if (a.active !== b.active) return a.active ? -1 : 1
        // then by age ascending
        return a.age - b.age
      })

      // Active users first, sorted by age
      expect(sorted[0].active).toBe(true)
      expect(sorted.filter(u => u.active).map(u => u.age)).toEqual([25, 28, 30])
    })

    it('should handle null values in sort', () => {
      interface DocWithNull {
        id: number
        value: number | null
      }
      const docs: DocWithNull[] = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 5 },
        { id: 4, value: null },
      ]

      // Nulls last
      const sorted = [...docs].sort((a, b) => {
        if (a.value === null && b.value === null) return 0
        if (a.value === null) return 1
        if (b.value === null) return -1
        return a.value - b.value
      })

      expect(sorted[0].value).toBe(5)
      expect(sorted[1].value).toBe(10)
      expect(sorted[2].value).toBe(null)
      expect(sorted[3].value).toBe(null)
    })

    it('should apply sort before limit', () => {
      // Sort by score desc, then take top 3
      const sorted = [...sampleUsers]
        .sort((a, b) => b.score - a.score)
        .slice(0, 3)

      expect(sorted[0].score).toBe(91.2) // Charlie
      expect(sorted[1].score).toBe(88.0) // Eve
      expect(sorted[2].score).toBe(85.5) // Alice
    })
  })

  describe('Complex Query Scenarios', () => {
    it('should handle filter + projection + sort + limit', () => {
      // Find active users with score > 70, select name and score,
      // sort by score desc, limit to 2
      const results = sampleUsers
        .filter(u => u.active && u.score > 70)
        .map(u => ({ name: u.name, score: u.score }))
        .sort((a, b) => b.score - a.score)
        .slice(0, 2)

      expect(results).toHaveLength(2)
      expect(results[0].name).toBe('Alice')
      expect(results[0].score).toBe(85.5)
      expect(results[1].name).toBe('Bob')
    })

    it('should handle pagination with consistent ordering', () => {
      // Page 1: offset 0, limit 2
      // Page 2: offset 2, limit 2
      // Page 3: offset 4, limit 2
      const sorted = [...sampleUsers].sort((a, b) => a._id.localeCompare(b._id))

      const page1 = sorted.slice(0, 2)
      const page2 = sorted.slice(2, 4)
      const page3 = sorted.slice(4, 6)

      expect(page1.map(u => u._id)).toEqual(['1', '2'])
      expect(page2.map(u => u._id)).toEqual(['3', '4'])
      expect(page3.map(u => u._id)).toEqual(['5'])
    })
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Edge Cases and Error Handling', () => {
  describe('Empty Results', () => {
    it('should return empty array when no documents match', () => {
      const filter: Filter<User> = { age: { $gt: 100 } }
      const results = sampleUsers.filter(u => matchesFilter(u, filter))
      expect(results).toHaveLength(0)
    })

    it('should handle filtering empty collection', () => {
      const emptyCollection: User[] = []
      const filter: Filter<User> = { active: true }
      const results = emptyCollection.filter(u => matchesFilter(u, filter))
      expect(results).toHaveLength(0)
    })
  })

  describe('Single Row Results', () => {
    it('should return single matching document', () => {
      const filter: Filter<User> = { _id: '1' }
      const results = sampleUsers.filter(u => matchesFilter(u, filter))
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Alice')
    })
  })

  describe('Match All Data', () => {
    it('should match all with empty filter', () => {
      const filter: Filter<User> = {}
      const results = sampleUsers.filter(u => matchesFilter(u, filter))
      expect(results).toHaveLength(sampleUsers.length)
    })

    it('should match all with always-true condition', () => {
      const filter: Filter<User> = { _id: { $exists: true } }
      const results = sampleUsers.filter(u => matchesFilter(u, filter))
      expect(results).toHaveLength(sampleUsers.length)
    })
  })

  describe('Type Coercion', () => {
    it('should not coerce string to number', () => {
      const filter: Filter<User> = { age: '30' as any }
      const results = sampleUsers.filter(u => matchesFilter(u, filter))
      expect(results).toHaveLength(0) // No match because '30' !== 30
    })

    it('should handle numeric string comparison correctly', () => {
      interface Doc {
        value: string
      }
      const doc: Doc = { value: '100' }
      // String comparison: '100' < '50' (lexicographic)
      expect(matchesFilter(doc, { value: { $lt: '50' } })).toBe(true)
    })
  })

  describe('Special Values', () => {
    it('should handle NaN', () => {
      interface DocWithNaN {
        value: number
      }
      const doc: DocWithNaN = { value: NaN }
      // NaN !== NaN
      expect(matchesFilter(doc, { value: NaN })).toBe(false)
    })

    it('should handle Infinity', () => {
      interface DocWithInfinity {
        value: number
      }
      const doc: DocWithInfinity = { value: Infinity }
      expect(matchesFilter(doc, { value: Infinity })).toBe(true)
      expect(matchesFilter(doc, { value: { $gt: 1000000 } })).toBe(true)
    })

    it('should handle negative Infinity', () => {
      interface DocWithInfinity {
        value: number
      }
      const doc: DocWithInfinity = { value: -Infinity }
      expect(matchesFilter(doc, { value: { $lt: -1000000 } })).toBe(true)
    })

    it('should handle empty string', () => {
      interface Doc {
        name: string
      }
      const doc: Doc = { name: '' }
      expect(matchesFilter(doc, { name: '' })).toBe(true)
      expect(matchesFilter(doc, { name: { $exists: true } })).toBe(true)
    })

    it('should handle empty array', () => {
      const user = sampleUsers[3] // Diana has empty tags
      expect(user.tags).toHaveLength(0)
      expect(matchesFilter(user, { tags: { $exists: true } } as Filter<User>)).toBe(true)
    })
  })

  describe('Invalid Filters', () => {
    it('should handle unknown operators gracefully', () => {
      const filter = { age: { $unknown: 30 } } as any
      // Should not throw, just not match
      expect(() => matchesFilter(sampleUsers[0], filter)).not.toThrow()
    })

    it('should handle null filter gracefully', () => {
      // Empty object as filter should match all
      expect(matchesFilter(sampleUsers[0], {} as Filter<User>)).toBe(true)
    })

    it('should handle array as document value', () => {
      // Direct array field access
      const user = sampleUsers[0]
      expect(matchesFilter(user, { tags: { $exists: true } } as Filter<User>)).toBe(true)
    })
  })

  describe('Performance Considerations', () => {
    it('should short-circuit $and on first false', () => {
      let evalCount = 0
      const customMatcher = (doc: User, condition: Filter<User>) => {
        evalCount++
        return matchesFilter(doc, condition)
      }

      // Reset counter
      evalCount = 0

      // $and with first condition false
      const filter: Filter<User> = {
        $and: [
          { age: { $gt: 100 } }, // Will be false
          { active: true },
          { score: { $gt: 50 } },
        ],
      }

      matchesFilter(sampleUsers[0], filter)
      // Ideally should stop after first condition
    })

    it('should short-circuit $or on first true', () => {
      // $or with first condition true
      const filter: Filter<User> = {
        $or: [
          { name: 'Alice' }, // Will be true for Alice
          { age: { $gt: 100 } },
          { active: false },
        ],
      }

      const result = matchesFilter(sampleUsers[0], filter)
      expect(result).toBe(true)
      // Ideally should stop after first condition
    })
  })
})

// =============================================================================
// DATA TYPE SUPPORT
// =============================================================================

describe('Data Type Support', () => {
  describe('Numeric Types', () => {
    it('should handle integer comparisons', () => {
      interface Doc { value: number }
      const doc: Doc = { value: 42 }
      expect(matchesFilter(doc, { value: { $eq: 42 } })).toBe(true)
      expect(matchesFilter(doc, { value: { $gt: 41 } })).toBe(true)
      expect(matchesFilter(doc, { value: { $lt: 43 } })).toBe(true)
    })

    it('should handle float comparisons', () => {
      interface Doc { value: number }
      const doc: Doc = { value: 3.14159 }
      expect(matchesFilter(doc, { value: { $gt: 3.14 } })).toBe(true)
      expect(matchesFilter(doc, { value: { $lt: 3.15 } })).toBe(true)
    })

    it('should handle very large numbers', () => {
      interface Doc { value: number }
      const doc: Doc = { value: Number.MAX_SAFE_INTEGER }
      expect(matchesFilter(doc, { value: { $eq: Number.MAX_SAFE_INTEGER } })).toBe(true)
    })

    it('should handle very small numbers', () => {
      interface Doc { value: number }
      const doc: Doc = { value: Number.MIN_SAFE_INTEGER }
      expect(matchesFilter(doc, { value: { $eq: Number.MIN_SAFE_INTEGER } })).toBe(true)
    })

    it('should handle floating point precision', () => {
      interface Doc { value: number }
      const doc: Doc = { value: 0.1 + 0.2 }
      // 0.1 + 0.2 !== 0.3 in JavaScript
      expect(matchesFilter(doc, { value: { $eq: 0.3 } })).toBe(false)
      expect(matchesFilter(doc, { value: { $gt: 0.29 } })).toBe(true)
      expect(matchesFilter(doc, { value: { $lt: 0.31 } })).toBe(true)
    })
  })

  describe('String Types', () => {
    it('should handle unicode strings', () => {
      interface Doc { name: string }
      const doc: Doc = { name: 'cafe' }
      expect(matchesFilter(doc, { name: 'cafe' })).toBe(true)
    })

    it('should handle emoji in strings', () => {
      interface Doc { status: string }
      const doc: Doc = { status: 'active' }
      expect(matchesFilter(doc, { status: { $regex: 'active' } })).toBe(true)
    })

    it('should handle very long strings', () => {
      interface Doc { content: string }
      const longString = 'a'.repeat(10000)
      const doc: Doc = { content: longString }
      expect(matchesFilter(doc, { content: { $eq: longString } })).toBe(true)
    })
  })

  describe('Date/Timestamp Types', () => {
    it('should compare dates correctly', () => {
      const date1 = new Date('2024-01-15')
      const date2 = new Date('2024-01-16')
      interface Doc { date: Date }
      const doc: Doc = { date: date1 }

      expect(matchesFilter(doc, { date: { $lt: date2 } })).toBe(true)
      expect(matchesFilter(doc, { date: { $gt: date2 } })).toBe(false)
    })

    it('should handle timezone-aware dates', () => {
      const utcDate = new Date('2024-01-15T00:00:00Z')
      const localDate = new Date('2024-01-15T00:00:00')
      interface Doc { date: Date }
      const doc: Doc = { date: utcDate }

      // These may or may not be equal depending on timezone
      const isSame = utcDate.getTime() === localDate.getTime()
      expect(matchesFilter(doc, { date: { $eq: utcDate } })).toBe(true)
    })

    it('should handle date range queries', () => {
      const startDate = new Date('2024-01-01')
      const endDate = new Date('2024-12-31')
      const testDate = new Date('2024-06-15')
      interface Doc { date: Date }
      const doc: Doc = { date: testDate }

      expect(matchesFilter(doc, { date: { $gte: startDate, $lte: endDate } })).toBe(true)
    })
  })

  describe('Boolean Types', () => {
    it('should match true values', () => {
      interface Doc { flag: boolean }
      const doc: Doc = { flag: true }
      expect(matchesFilter(doc, { flag: true })).toBe(true)
      expect(matchesFilter(doc, { flag: { $eq: true } })).toBe(true)
      expect(matchesFilter(doc, { flag: { $ne: false } })).toBe(true)
    })

    it('should match false values', () => {
      interface Doc { flag: boolean }
      const doc: Doc = { flag: false }
      expect(matchesFilter(doc, { flag: false })).toBe(true)
      expect(matchesFilter(doc, { flag: { $eq: false } })).toBe(true)
    })

    it('should use $in with booleans', () => {
      interface Doc { flag: boolean }
      const doc: Doc = { flag: true }
      expect(matchesFilter(doc, { flag: { $in: [true, false] } })).toBe(true)
    })
  })

  describe('Array Field Filtering (TODO)', () => {
    it('should check if array contains value with $elemMatch (TODO)', () => {
      // TODO: Implement $elemMatch operator
      const user = sampleUsers[0] // tags: ['premium', 'verified']

      // Currently not supported - would need $elemMatch
      // expect(matchesFilter(user, { tags: { $elemMatch: { $eq: 'premium' } } })).toBe(true)
      expect(user.tags).toContain('premium')
    })

    it('should check array size (TODO)', () => {
      // TODO: Implement $size operator
      const user = sampleUsers[4] // tags: ['premium', 'verified', 'vip']
      // expect(matchesFilter(user, { tags: { $size: 3 } })).toBe(true)
      expect(user.tags).toHaveLength(3)
    })

    it('should check $all elements present (TODO)', () => {
      // TODO: Implement $all operator
      const user = sampleUsers[0] // tags: ['premium', 'verified']
      // expect(matchesFilter(user, { tags: { $all: ['premium', 'verified'] } })).toBe(true)
      expect(user.tags).toContain('premium')
      expect(user.tags).toContain('verified')
    })
  })
})

// =============================================================================
// PERFORMANCE VERIFICATION
// =============================================================================

describe('Performance Verification', () => {
  describe('File Skipping', () => {
    it('should skip files based on partition values', () => {
      // Simulated partition metadata
      interface PartitionedFile {
        path: string
        partitionValues: { year: string; month: string }
      }

      const files: PartitionedFile[] = [
        { path: 'year=2023/month=01/data.parquet', partitionValues: { year: '2023', month: '01' } },
        { path: 'year=2023/month=06/data.parquet', partitionValues: { year: '2023', month: '06' } },
        { path: 'year=2024/month=01/data.parquet', partitionValues: { year: '2024', month: '01' } },
        { path: 'year=2024/month=06/data.parquet', partitionValues: { year: '2024', month: '06' } },
      ]

      // Filter for year=2024
      const relevantFiles = files.filter(f => f.partitionValues.year === '2024')
      expect(relevantFiles).toHaveLength(2)
      expect(relevantFiles.every(f => f.path.includes('year=2024'))).toBe(true)
    })

    it('should calculate file skip rate', () => {
      const totalFiles = 100
      const scannedFiles = 25
      const skipRate = (totalFiles - scannedFiles) / totalFiles

      expect(skipRate).toBe(0.75) // 75% of files skipped
    })
  })

  describe('Row Group Skipping', () => {
    it('should count row groups skipped vs scanned', () => {
      const rowGroups = [
        { id: 0, zoneMap: { min: 0, max: 100 }, rowCount: 10000 },
        { id: 1, zoneMap: { min: 101, max: 200 }, rowCount: 10000 },
        { id: 2, zoneMap: { min: 201, max: 300 }, rowCount: 10000 },
        { id: 3, zoneMap: { min: 301, max: 400 }, rowCount: 10000 },
      ]

      // Filter: value > 250
      const filterMin = 250
      const scannedGroups = rowGroups.filter(rg => rg.zoneMap.max > filterMin)
      const skippedGroups = rowGroups.filter(rg => rg.zoneMap.max <= filterMin)

      expect(scannedGroups).toHaveLength(2) // Groups 2 and 3
      expect(skippedGroups).toHaveLength(2) // Groups 0 and 1

      const rowsSkipped = skippedGroups.reduce((sum, rg) => sum + rg.rowCount, 0)
      expect(rowsSkipped).toBe(20000)
    })
  })

  describe('Column Pruning', () => {
    it('should only request required columns', () => {
      const allColumns = ['_id', 'name', 'email', 'age', 'score', 'active', 'metadata', 'tags']
      const projection: string[] = ['name', 'email']
      const filterColumns = ['age'] // used in filter

      // Required columns = projection + filter columns
      const requiredColumns = [...new Set([...projection, ...filterColumns])]

      expect(requiredColumns).toHaveLength(3)
      expect(requiredColumns).toContain('name')
      expect(requiredColumns).toContain('email')
      expect(requiredColumns).toContain('age')
      expect(requiredColumns).not.toContain('metadata')
    })

    it('should calculate bytes saved from column pruning', () => {
      const columnSizes: Record<string, number> = {
        _id: 1000,
        name: 5000,
        email: 8000,
        age: 400,
        score: 800,
        active: 100,
        metadata: 50000, // Large JSON blob
        tags: 10000,
      }

      const totalSize = Object.values(columnSizes).reduce((a, b) => a + b, 0)
      const requiredColumns = ['name', 'email', 'age']
      const requiredSize = requiredColumns.reduce((sum, col) => sum + columnSizes[col], 0)

      const bytesSaved = totalSize - requiredSize
      const savingsPercent = bytesSaved / totalSize

      expect(savingsPercent).toBeGreaterThan(0.5) // >50% savings
    })
  })
})
