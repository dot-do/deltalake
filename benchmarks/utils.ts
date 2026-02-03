/**
 * Benchmark Utilities
 *
 * Helper functions for creating test data and measuring performance
 * in Delta Lake benchmarks.
 */

import { MemoryStorage } from '../src/storage/index'
import { DeltaTable } from '../src/delta/index'

// =============================================================================
// DATA GENERATION
// =============================================================================

/**
 * Generate a batch of test documents with various field types.
 *
 * @param count - Number of documents to generate
 * @param startId - Starting ID for documents (for batch generation)
 * @returns Array of test documents
 */
export function generateDocuments(count: number, startId = 0): Record<string, unknown>[] {
  const docs: Record<string, unknown>[] = []

  for (let i = 0; i < count; i++) {
    const id = startId + i
    docs.push({
      _id: `doc-${id}`,
      name: `Document ${id}`,
      category: ['electronics', 'clothing', 'food', 'books', 'toys'][id % 5],
      // Use integer cents to avoid float precision issues with Parquet INT32
      priceCents: Math.floor(Math.random() * 10000),
      quantity: Math.floor(Math.random() * 1000),
      active: id % 2 === 0,
      timestamp: Date.now() - Math.floor(Math.random() * 86400000),
      // Store tags as a JSON string to avoid array serialization issues
      tags: JSON.stringify([`tag-${id % 10}`, `tag-${(id + 1) % 10}`]),
      // Store metadata as JSON string to avoid nested object issues
      metadata: JSON.stringify({
        source: 'benchmark',
        version: 1,
        region: ['us-east', 'us-west', 'eu-west', 'ap-east'][id % 4],
      }),
    })
  }

  return docs
}

/**
 * Generate documents with nested structures for complex query testing.
 */
export function generateNestedDocuments(count: number): Record<string, unknown>[] {
  const docs: Record<string, unknown>[] = []

  for (let i = 0; i < count; i++) {
    // Store nested structures as JSON strings for Parquet compatibility
    docs.push({
      _id: `nested-${i}`,
      userId: i,
      userName: `User ${i}`,
      userAge: 18 + (i % 50),
      theme: i % 2 === 0 ? 'dark' : 'light',
      notifications: i % 3 !== 0,
      city: ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'][i % 5],
      country: 'USA',
      zip: 10000 + i,
      // Store complex nested data as JSON
      orders: JSON.stringify(Array.from({ length: i % 5 }, (_, j) => ({
        id: `order-${i}-${j}`,
        totalCents: Math.floor(Math.random() * 100000),
      }))),
    })
  }

  return docs
}

/**
 * Generate a large document for throughput testing.
 */
export function generateLargeDocument(sizeKb: number): Record<string, unknown> {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let data = ''
  const targetLength = sizeKb * 1024

  while (data.length < targetLength) {
    data += chars[Math.floor(Math.random() * chars.length)]
  }

  return {
    _id: `large-doc-${Date.now()}`,
    payload: data,
    size: sizeKb,
    timestamp: Date.now(),
  }
}

// =============================================================================
// TABLE SETUP
// =============================================================================

/**
 * Create a fresh DeltaTable with MemoryStorage for benchmarking.
 */
export function createBenchmarkTable<T extends Record<string, unknown> = Record<string, unknown>>(
  tablePath = 'benchmark-table'
): { storage: MemoryStorage; table: DeltaTable<T> } {
  const storage = new MemoryStorage()
  const table = new DeltaTable<T>(storage, tablePath)
  return { storage, table }
}

/**
 * Create a table pre-populated with data for read benchmarks.
 */
export async function createPopulatedTable(
  docCount: number,
  batchSize = 1000
): Promise<{ storage: MemoryStorage; table: DeltaTable }> {
  const { storage, table } = createBenchmarkTable()

  // Write data in batches
  for (let i = 0; i < docCount; i += batchSize) {
    const count = Math.min(batchSize, docCount - i)
    const docs = generateDocuments(count, i)
    await table.write(docs)
  }

  return { storage, table }
}

/**
 * Create a table with multiple small files for compaction benchmarks.
 */
export async function createFragmentedTable(
  fileCount: number,
  rowsPerFile: number
): Promise<{ storage: MemoryStorage; table: DeltaTable }> {
  const { storage, table } = createBenchmarkTable()

  for (let i = 0; i < fileCount; i++) {
    const docs = generateDocuments(rowsPerFile, i * rowsPerFile)
    await table.write(docs)
  }

  return { storage, table }
}

// =============================================================================
// METRICS
// =============================================================================

/**
 * Calculate throughput metrics from benchmark results.
 */
export function calculateThroughput(
  operations: number,
  durationMs: number
): { opsPerSecond: number; msPerOp: number } {
  const opsPerSecond = (operations / durationMs) * 1000
  const msPerOp = durationMs / operations
  return { opsPerSecond, msPerOp }
}

/**
 * Format bytes for human-readable output.
 */
export function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB']
  let value = bytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex++
  }

  return `${value.toFixed(2)} ${units[unitIndex]}`
}

/**
 * Calculate data size of documents (approximate).
 */
export function estimateDocSize(docs: Record<string, unknown>[]): number {
  return new TextEncoder().encode(JSON.stringify(docs)).length
}
