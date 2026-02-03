/**
 * Query Latency Benchmarks
 *
 * Measures the performance of query operations in Delta Lake tables.
 * Tests various filter types, projections, and data sizes.
 *
 * Run with: npm run bench -- benchmarks/query.bench.ts
 */

import { bench, describe, beforeAll, afterAll, beforeEach } from 'vitest'
import { MemoryStorage } from '../src/storage/index'
import { DeltaTable } from '../src/delta/index'
import {
  generateDocuments,
  generateNestedDocuments,
  createPopulatedTable,
} from './utils'

// =============================================================================
// CONFIGURATION
// =============================================================================

/** Number of warmup iterations before measuring */
const WARMUP_ITERATIONS = 3

/** Number of measurement iterations */
const ITERATIONS = 20

/** Timeout for each benchmark (ms) */
const TIMEOUT = 60000

// =============================================================================
// TEST DATA SETUP
// =============================================================================

// Pre-populated tables for query benchmarks
let smallTable: { storage: MemoryStorage; table: DeltaTable }
let mediumTable: { storage: MemoryStorage; table: DeltaTable }
let largeTable: { storage: MemoryStorage; table: DeltaTable }
let nestedTable: { storage: MemoryStorage; table: DeltaTable }
let multiFileTable: { storage: MemoryStorage; table: DeltaTable }

beforeAll(async () => {
  // Small table: 100 documents in 1 file
  smallTable = await createPopulatedTable(100, 100)

  // Medium table: 1000 documents in 1 file
  mediumTable = await createPopulatedTable(1000, 1000)

  // Large table: 10000 documents in 10 files
  largeTable = await createPopulatedTable(10000, 1000)

  // Nested documents table: 500 documents with nested structures
  const nestedStorage = new MemoryStorage()
  const nestedTableInstance = new DeltaTable(nestedStorage, 'nested-table')
  await nestedTableInstance.write(generateNestedDocuments(500))
  nestedTable = { storage: nestedStorage, table: nestedTableInstance }

  // Multi-file table: 50 files with 100 documents each
  const multiFileStorage = new MemoryStorage()
  const multiFileTableInstance = new DeltaTable(multiFileStorage, 'multi-file-table')
  for (let i = 0; i < 50; i++) {
    await multiFileTableInstance.write(generateDocuments(100, i * 100))
  }
  multiFileTable = { storage: multiFileStorage, table: multiFileTableInstance }
})

afterAll(() => {
  smallTable?.storage.clear()
  mediumTable?.storage.clear()
  largeTable?.storage.clear()
  nestedTable?.storage.clear()
  multiFileTable?.storage.clear()
})

// =============================================================================
// FULL TABLE SCAN BENCHMARKS
// =============================================================================

describe('Query Latency - Full Table Scan', () => {
  describe('Small table (100 docs)', () => {
    bench(
      'query all documents',
      async () => {
        const results = await smallTable.table.query({})
        // Consume results to ensure iteration completes
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Medium table (1000 docs)', () => {
    bench(
      'query all documents',
      async () => {
        const results = await mediumTable.table.query({})
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Large table (10000 docs)', () => {
    bench(
      'query all documents',
      async () => {
        const results = await largeTable.table.query({})
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS / 2, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// EQUALITY FILTER BENCHMARKS
// =============================================================================

describe('Query Latency - Equality Filters', () => {
  describe('Single field equality', () => {
    bench(
      'filter by _id (unique, 1000 docs)',
      async () => {
        const results = await mediumTable.table.query({ _id: 'doc-500' })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'filter by category (20% selectivity, 1000 docs)',
      async () => {
        const results = await mediumTable.table.query({ category: 'electronics' })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'filter by active flag (50% selectivity, 1000 docs)',
      async () => {
        const results = await mediumTable.table.query({ active: true })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Multi-field equality', () => {
    bench(
      'filter by two fields (category AND active)',
      async () => {
        const results = await mediumTable.table.query({
          category: 'electronics',
          active: true,
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// COMPARISON FILTER BENCHMARKS
// =============================================================================

describe('Query Latency - Comparison Filters', () => {
  describe('Range queries', () => {
    bench(
      'filter by price > 50 (1000 docs)',
      async () => {
        const results = await mediumTable.table.query({
          priceCents: { $gt: 5000 },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'filter by price range (20-80)',
      async () => {
        const results = await mediumTable.table.query({
          priceCents: { $gte: 2000, $lte: 8000 },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('$in operator', () => {
    bench(
      'filter by category $in (3 values)',
      async () => {
        const results = await mediumTable.table.query({
          category: { $in: ['electronics', 'clothing', 'food'] },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('$exists operator', () => {
    bench(
      'filter by field exists',
      async () => {
        const results = await mediumTable.table.query({
          metadata: { $exists: true },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// LOGICAL OPERATOR BENCHMARKS
// =============================================================================

describe('Query Latency - Logical Operators', () => {
  describe('$and operator', () => {
    bench(
      '$and with two conditions',
      async () => {
        const results = await mediumTable.table.query({
          $and: [{ category: 'electronics' }, { priceCents: { $gt: 5000 } }],
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('$or operator', () => {
    bench(
      '$or with two conditions',
      async () => {
        const results = await mediumTable.table.query({
          $or: [{ category: 'electronics' }, { category: 'clothing' }],
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Complex nested query', () => {
    bench(
      'complex query with $and and $or',
      async () => {
        const results = await mediumTable.table.query({
          $and: [
            { $or: [{ category: 'electronics' }, { category: 'clothing' }] },
            { priceCents: { $gte: 1000, $lte: 9000 } },
            { active: true },
          ],
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// FIELD FILTER BENCHMARKS
// =============================================================================

describe('Query Latency - Field Filters', () => {
  describe('Integer range queries', () => {
    bench(
      'filter by userAge >= 30',
      async () => {
        const results = await nestedTable.table.query({
          userAge: { $gte: 30 },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'filter by theme = dark',
      async () => {
        const results = await nestedTable.table.query({
          theme: 'dark',
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// PROJECTION BENCHMARKS
// =============================================================================

describe('Query Latency - Projections', () => {
  describe('Include projection', () => {
    bench(
      'project 2 fields from 1000 docs',
      async () => {
        const results = await mediumTable.table.query({}, {
          projection: ['_id', 'name'],
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'project 5 fields from 1000 docs',
      async () => {
        const results = await mediumTable.table.query({}, {
          projection: ['_id', 'name', 'category', 'price', 'active'],
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Exclude projection', () => {
    bench(
      'exclude metadata field from 1000 docs',
      async () => {
        const results = await mediumTable.table.query({}, {
          projection: { metadata: 0, tags: 0 },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// MULTI-FILE QUERY BENCHMARKS
// =============================================================================

describe('Query Latency - Multi-File Tables', () => {
  describe('Query across many files', () => {
    bench(
      'full scan across 50 files (5000 docs)',
      async () => {
        const results = await multiFileTable.table.query({})
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS / 2, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'filtered query across 50 files',
      async () => {
        const results = await multiFileTable.table.query({
          category: 'electronics',
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS / 2, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// REGEX QUERY BENCHMARKS
// =============================================================================

describe('Query Latency - Regex Filters', () => {
  describe('Regex matching', () => {
    bench(
      'simple regex pattern on 1000 docs',
      async () => {
        const results = await mediumTable.table.query({
          name: { $regex: '^Document 5' },
        })
        const docs = []
        for await (const doc of results) {
          docs.push(doc)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})
