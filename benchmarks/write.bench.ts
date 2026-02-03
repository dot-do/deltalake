/**
 * Write Throughput Benchmarks
 *
 * Measures the performance of write operations in Delta Lake tables.
 * Tests various batch sizes and document complexities.
 *
 * Run with: npm run bench -- benchmarks/write.bench.ts
 */

import { bench, describe, beforeAll, afterAll } from 'vitest'
import { MemoryStorage } from '../src/storage/index'
import { DeltaTable } from '../src/delta/index'
import {
  generateDocuments,
  generateNestedDocuments,
  generateLargeDocument,
  createBenchmarkTable,
} from './utils'

// =============================================================================
// CONFIGURATION
// =============================================================================

/** Number of warmup iterations before measuring */
const WARMUP_ITERATIONS = 3

/** Number of measurement iterations */
const ITERATIONS = 10

/** Timeout for each benchmark (ms) */
const TIMEOUT = 60000

// =============================================================================
// BATCH WRITE BENCHMARKS
// =============================================================================

describe('Write Throughput - Batch Sizes', () => {
  describe('Small batches (10 docs)', () => {
    bench(
      'write 10 documents',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = generateDocuments(10)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Medium batches (100 docs)', () => {
    bench(
      'write 100 documents',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = generateDocuments(100)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Large batches (1000 docs)', () => {
    bench(
      'write 1000 documents',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = generateDocuments(1000)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Very large batches (10000 docs)', () => {
    bench(
      'write 10000 documents',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = generateDocuments(10000)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// SEQUENTIAL WRITE BENCHMARKS
// =============================================================================

describe('Write Throughput - Sequential Writes', () => {
  describe('Multiple sequential writes', () => {
    bench(
      '10 sequential writes of 100 docs each',
      async () => {
        const { table } = createBenchmarkTable()
        for (let i = 0; i < 10; i++) {
          const docs = generateDocuments(100, i * 100)
          await table.write(docs)
        }
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      '100 sequential writes of 10 docs each',
      async () => {
        const { table } = createBenchmarkTable()
        for (let i = 0; i < 100; i++) {
          const docs = generateDocuments(10, i * 10)
          await table.write(docs)
        }
      },
      { iterations: ITERATIONS / 2, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// DOCUMENT COMPLEXITY BENCHMARKS
// =============================================================================

describe('Write Throughput - Document Complexity', () => {
  describe('Simple flat documents', () => {
    bench(
      'write 500 flat documents',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = Array.from({ length: 500 }, (_, i) => ({
          _id: `flat-${i}`,
          name: `Item ${i}`,
          value: i * 10,
          activeFlag: i % 2 === 0,
        }))
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Nested documents', () => {
    bench(
      'write 500 nested documents',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = generateNestedDocuments(500)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Large documents', () => {
    bench(
      'write 10 large documents (10KB each)',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = Array.from({ length: 10 }, () => generateLargeDocument(10))
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'write 5 large documents (50KB each)',
      async () => {
        const { table } = createBenchmarkTable()
        const docs = Array.from({ length: 5 }, () => generateLargeDocument(50))
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// INCREMENTAL WRITE BENCHMARKS
// =============================================================================

describe('Write Throughput - Incremental (append to existing table)', () => {
  let prePopulatedStorage: MemoryStorage
  let existingDocsTable: DeltaTable

  beforeAll(async () => {
    // Pre-populate a table with existing data
    prePopulatedStorage = new MemoryStorage()
    existingDocsTable = new DeltaTable(prePopulatedStorage, 'existing-table')

    // Add initial data - 5000 documents across multiple files
    for (let i = 0; i < 5; i++) {
      const docs = generateDocuments(1000, i * 1000)
      await existingDocsTable.write(docs)
    }
  })

  afterAll(() => {
    prePopulatedStorage.clear()
  })

  describe('Append to existing table', () => {
    bench(
      'append 100 docs to table with 5000 existing docs',
      async () => {
        // Create a fresh copy for each iteration to avoid state pollution
        const storage = new MemoryStorage()
        const table = new DeltaTable(storage, 'test-table')

        // Copy existing state
        const snapshot = prePopulatedStorage.snapshot()
        storage.restore(snapshot)

        // Append new documents
        const docs = generateDocuments(100, 10000)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// PARTITIONED WRITE BENCHMARKS
// =============================================================================

describe('Write Throughput - Partitioned Tables', () => {
  describe('Write with partition columns', () => {
    bench(
      'write 500 docs to partitioned table (5 partitions)',
      async () => {
        const storage = new MemoryStorage()
        const table = new DeltaTable(storage, 'partitioned-table')

        // Set partition columns
        table.setTableConfiguration({ 'delta.partitionColumns': 'category' })

        const docs = generateDocuments(500)
        await table.write(docs)
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})
