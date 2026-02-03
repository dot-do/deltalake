/**
 * Compaction Speed Benchmarks
 *
 * Measures the performance of compaction and deduplication operations.
 * Tests file merging, Z-order clustering, and deduplication strategies.
 *
 * Run with: npm run bench -- benchmarks/compaction.bench.ts
 */

import { bench, describe, beforeAll, afterAll } from 'vitest'
import { MemoryStorage } from '../src/storage/index'
import { DeltaTable } from '../src/delta/index'
import { compact, deduplicate, zOrderCluster } from '../src/compaction/index'
import {
  generateDocuments,
  createFragmentedTable,
  createBenchmarkTable,
} from './utils'

// =============================================================================
// CONFIGURATION
// =============================================================================

/** Number of warmup iterations before measuring */
const WARMUP_ITERATIONS = 2

/** Number of measurement iterations */
const ITERATIONS = 5

/** Timeout for each benchmark (ms) */
const TIMEOUT = 120000

// =============================================================================
// FILE COMPACTION BENCHMARKS
// =============================================================================

describe('Compaction Speed - File Merging', () => {
  describe('Small fragmented tables', () => {
    bench(
      'compact 10 files x 100 rows each',
      async () => {
        const { storage, table } = await createFragmentedTable(10, 100)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024, // 10MB
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Medium fragmented tables', () => {
    bench(
      'compact 20 files x 500 rows each',
      async () => {
        const { storage, table } = await createFragmentedTable(20, 500)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Large fragmented tables', () => {
    bench(
      'compact 50 files x 200 rows each',
      async () => {
        const { storage, table } = await createFragmentedTable(50, 200)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Very fragmented tables (many small files)', () => {
    bench(
      'compact 100 files x 50 rows each',
      async () => {
        const { storage, table } = await createFragmentedTable(100, 50)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// COMPACTION STRATEGY BENCHMARKS
// =============================================================================

describe('Compaction Speed - Strategies', () => {
  describe('Bin-packing strategy', () => {
    bench(
      'compact with bin-packing (30 files)',
      async () => {
        const { storage, table } = await createFragmentedTable(30, 300)
        await compact(table, {
          targetFileSize: 5 * 1024 * 1024,
          strategy: 'bin-packing',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Greedy strategy', () => {
    bench(
      'compact with greedy (30 files)',
      async () => {
        const { storage, table } = await createFragmentedTable(30, 300)
        await compact(table, {
          targetFileSize: 5 * 1024 * 1024,
          strategy: 'greedy',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Sort-by-size strategy', () => {
    bench(
      'compact with sort-by-size (30 files)',
      async () => {
        const { storage, table } = await createFragmentedTable(30, 300)
        await compact(table, {
          targetFileSize: 5 * 1024 * 1024,
          strategy: 'sort-by-size',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// DRY RUN BENCHMARKS
// =============================================================================

describe('Compaction Speed - Dry Run (Planning Only)', () => {
  describe('Dry run overhead', () => {
    bench(
      'dry run on 50 files',
      async () => {
        const { storage, table } = await createFragmentedTable(50, 200)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
          dryRun: true,
        })
        storage.clear()
      },
      { iterations: ITERATIONS * 2, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// DEDUPLICATION BENCHMARKS
// =============================================================================

describe('Compaction Speed - Deduplication', () => {
  /**
   * Create a table with duplicate data for deduplication benchmarks.
   */
  async function createTableWithDuplicates(
    uniqueCount: number,
    duplicateFactor: number
  ): Promise<{ storage: MemoryStorage; table: DeltaTable }> {
    const { storage, table } = createBenchmarkTable()

    // Generate unique rows
    const uniqueDocs = generateDocuments(uniqueCount)

    // Generate duplicates by repeating unique rows with modified timestamps
    const allDocs: Record<string, unknown>[] = []
    for (let d = 0; d < duplicateFactor; d++) {
      for (const doc of uniqueDocs) {
        allDocs.push({
          ...doc,
          _version: d + 1,
          _timestamp: Date.now() + d * 1000,
        })
      }
    }

    // Write in batches to create multiple files
    const batchSize = Math.ceil(allDocs.length / 5)
    for (let i = 0; i < allDocs.length; i += batchSize) {
      await table.write(allDocs.slice(i, i + batchSize))
    }

    return { storage, table }
  }

  describe('Deduplication by primary key', () => {
    bench(
      'deduplicate 1000 unique rows (3x duplicates) - keep latest',
      async () => {
        const { storage, table } = await createTableWithDuplicates(1000, 3)
        await deduplicate(table, {
          primaryKey: ['_id'],
          keepStrategy: 'latest',
          orderByColumn: '_timestamp',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'deduplicate 1000 unique rows (3x duplicates) - keep first',
      async () => {
        const { storage, table } = await createTableWithDuplicates(1000, 3)
        await deduplicate(table, {
          primaryKey: ['_id'],
          keepStrategy: 'first',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Deduplication with composite key', () => {
    bench(
      'deduplicate with composite key (_id, category)',
      async () => {
        const { storage, table } = await createTableWithDuplicates(500, 4)
        await deduplicate(table, {
          primaryKey: ['_id', 'category'],
          keepStrategy: 'latest',
          orderByColumn: '_timestamp',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Exact duplicate removal', () => {
    bench(
      'remove exact duplicates (500 rows, 5x duplicates)',
      async () => {
        const { storage, table } = createBenchmarkTable()

        // Create exact duplicates (same content)
        const docs = generateDocuments(500)
        for (let i = 0; i < 5; i++) {
          await table.write(docs)
        }

        await deduplicate(table, {
          exactDuplicates: true,
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// Z-ORDER CLUSTERING BENCHMARKS
// =============================================================================

describe('Compaction Speed - Z-Order Clustering', () => {
  describe('Single column clustering', () => {
    bench(
      'cluster by category (5000 rows)',
      async () => {
        const { storage, table } = await createFragmentedTable(10, 500)
        await zOrderCluster(table, {
          columns: ['category'],
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Multi-column clustering', () => {
    bench(
      'cluster by (category, price) - 2 columns',
      async () => {
        const { storage, table } = await createFragmentedTable(10, 500)
        await zOrderCluster(table, {
          columns: ['category', 'priceCents'],
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )

    bench(
      'cluster by (category, price, quantity) - 3 columns',
      async () => {
        const { storage, table } = await createFragmentedTable(10, 500)
        await zOrderCluster(table, {
          columns: ['category', 'priceCents', 'quantity'],
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Hilbert curve clustering', () => {
    bench(
      'hilbert clustering (category, price) - 2 columns',
      async () => {
        const { storage, table } = await createFragmentedTable(10, 500)
        await zOrderCluster(table, {
          columns: ['category', 'priceCents'],
          curveType: 'hilbert',
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Clustering dry run', () => {
    bench(
      'z-order dry run (10 files)',
      async () => {
        const { storage, table } = await createFragmentedTable(10, 500)
        await zOrderCluster(table, {
          columns: ['category', 'priceCents'],
          dryRun: true,
        })
        storage.clear()
      },
      { iterations: ITERATIONS * 2, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// COMBINED OPERATION BENCHMARKS
// =============================================================================

describe('Compaction Speed - Combined Operations', () => {
  describe('Compact then deduplicate', () => {
    bench(
      'compact + deduplicate pipeline',
      async () => {
        const { storage, table } = createBenchmarkTable()

        // Create fragmented table with duplicates
        for (let i = 0; i < 20; i++) {
          const docs = generateDocuments(100, (i % 10) * 100)
          await table.write(docs)
        }

        // Compact first
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
        })

        // Then deduplicate
        await deduplicate(table, {
          primaryKey: ['_id'],
          keepStrategy: 'latest',
          orderByColumn: 'timestamp',
        })

        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})

// =============================================================================
// INTEGRITY VERIFICATION BENCHMARKS
// =============================================================================

describe('Compaction Speed - Integrity Verification', () => {
  describe('With integrity check', () => {
    bench(
      'compact with integrity verification',
      async () => {
        const { storage, table } = await createFragmentedTable(20, 250)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
          verifyIntegrity: true,
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })

  describe('Without integrity check', () => {
    bench(
      'compact without integrity verification',
      async () => {
        const { storage, table } = await createFragmentedTable(20, 250)
        await compact(table, {
          targetFileSize: 10 * 1024 * 1024,
          verifyIntegrity: false,
        })
        storage.clear()
      },
      { iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS, time: TIMEOUT }
    )
  })
})
