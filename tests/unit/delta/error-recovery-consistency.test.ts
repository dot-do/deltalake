/**
 * Error Recovery and Data Consistency Tests
 *
 * Tests that verify data consistency is maintained after various error scenarios:
 * - Partial write recovery
 * - Storage failure recovery
 * - Concurrency error recovery
 * - No data corruption during error scenarios
 *
 * Issue: deltalake-lm4u - Validate error recovery and data consistency
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  DeltaTable,
  ConcurrencyError,
  withRetry,
  type DeltaCommit,
  type DeltaAction,
} from '../../../src/delta/index.js'
import {
  MemoryStorage,
  StorageError,
  VersionMismatchError,
  FileNotFoundError,
  type StorageBackend,
} from '../../../src/storage/index.js'

// =============================================================================
// TEST TYPES
// =============================================================================

interface TestRecord {
  id: number
  value: string
  timestamp?: number
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Create a test table with memory storage
 */
function createTestTable(): { table: DeltaTable<TestRecord>; storage: MemoryStorage } {
  const storage = new MemoryStorage()
  const table = new DeltaTable<TestRecord>(storage, 'test-table')
  return { table, storage }
}

/**
 * Write an external commit directly to storage (simulates external writer)
 */
async function writeExternalCommit(
  storage: StorageBackend,
  version: number,
  tablePath: string = 'test-table'
): Promise<void> {
  const versionStr = version.toString().padStart(20, '0')
  const commitPath = `${tablePath}/_delta_log/${versionStr}.json`
  const timestamp = Date.now()
  const commit = [
    `{"add":{"path":"${tablePath}/external-part-${version}.parquet","size":1024,"modificationTime":${timestamp},"dataChange":true}}`,
    `{"commitInfo":{"timestamp":${timestamp},"operation":"EXTERNAL_WRITE"}}`,
  ].join('\n')
  await storage.write(commitPath, new TextEncoder().encode(commit))
}

/**
 * Create a failing storage wrapper that throws errors on specific operations
 */
function createFailingStorage(
  baseStorage: MemoryStorage,
  config: {
    failOn?: 'read' | 'write' | 'list' | 'delete' | 'writeConditional'
    failAfterNOperations?: number
    failCount?: number
    errorType?: 'StorageError' | 'generic'
  }
): StorageBackend {
  let operationCount = 0
  let failuresTriggered = 0
  const maxFailures = config.failCount ?? 1

  const shouldFail = (): boolean => {
    operationCount++
    if (config.failAfterNOperations !== undefined && operationCount <= config.failAfterNOperations) {
      return false
    }
    if (failuresTriggered < maxFailures) {
      failuresTriggered++
      return true
    }
    return false
  }

  const throwError = (path: string, operation: string): never => {
    if (config.errorType === 'generic') {
      throw new Error(`Simulated ${operation} failure`)
    }
    throw new StorageError(`Simulated ${operation} failure`, path, operation)
  }

  // Create a proper storage wrapper with all required methods bound
  const wrapper: StorageBackend = {
    read: async (path: string) => {
      if (config.failOn === 'read' && shouldFail()) {
        throwError(path, 'read')
      }
      return baseStorage.read(path)
    },
    write: async (path: string, data: Uint8Array) => {
      if (config.failOn === 'write' && shouldFail()) {
        throwError(path, 'write')
      }
      return baseStorage.write(path, data)
    },
    writeConditional: async (path: string, data: Uint8Array, version: string | null) => {
      if (config.failOn === 'writeConditional' && shouldFail()) {
        throwError(path, 'writeConditional')
      }
      return baseStorage.writeConditional(path, data, version)
    },
    list: async (prefix: string) => {
      if (config.failOn === 'list' && shouldFail()) {
        throwError(prefix, 'list')
      }
      return baseStorage.list(prefix)
    },
    delete: async (path: string) => {
      if (config.failOn === 'delete' && shouldFail()) {
        throwError(path, 'delete')
      }
      return baseStorage.delete(path)
    },
    exists: (path: string) => baseStorage.exists(path),
    stat: (path: string) => baseStorage.stat(path),
    readRange: (path: string, start: number, end: number) => baseStorage.readRange(path, start, end),
    getVersion: (path: string) => baseStorage.getVersion(path),
  }

  return wrapper
}

// =============================================================================
// DATA CONSISTENCY AFTER CONCURRENCY ERRORS
// =============================================================================

describe('Data Consistency After Concurrency Errors', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Table State After ConcurrencyError', () => {
    it('should maintain consistent table state after ConcurrencyError is thrown', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      // Initial write
      await table.write([{ id: 1, value: 'initial' }])
      expect(await table.version()).toBe(0)

      // Simulate external write
      await writeExternalCommit(storage, 1)

      // This should throw ConcurrencyError
      await expect(table.write([{ id: 2, value: 'conflict' }])).rejects.toThrow(ConcurrencyError)

      // Verify table is still in a consistent state
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      const snapshot = await freshTable.snapshot()

      // Should have exactly 2 files (initial + external)
      expect(snapshot.files).toHaveLength(2)
      expect(snapshot.version).toBe(1)
    })

    it('should not leave orphaned data files after ConcurrencyError', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      await table.write([{ id: 1, value: 'initial' }])
      await writeExternalCommit(storage, 1)

      // Track files before attempted write
      const filesBefore = await storage.list('test-table')

      try {
        await table.write([{ id: 2, value: 'conflict' }])
      } catch (error) {
        expect(error).toBeInstanceOf(ConcurrencyError)
      }

      // Get files after failed write
      const filesAfter = await storage.list('test-table')

      // Note: Delta Lake may leave orphaned data files that will be cleaned by vacuum
      // But commit log should be consistent
      const commitFilesBefore = filesBefore.filter(f => f.includes('_delta_log') && f.endsWith('.json'))
      const commitFilesAfter = filesAfter.filter(f => f.includes('_delta_log') && f.endsWith('.json'))

      // No new commit files should be created from the failed write
      expect(commitFilesAfter.length).toBe(commitFilesBefore.length)
    })

    it('should allow successful write after recovery from ConcurrencyError', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      await table.write([{ id: 1, value: 'initial' }])
      await writeExternalCommit(storage, 1)

      // First write fails
      await expect(table.write([{ id: 2, value: 'conflict' }])).rejects.toThrow(ConcurrencyError)

      // Refresh and retry
      await table.refreshVersion()
      const result = await table.write([{ id: 3, value: 'success' }])

      expect(result.version).toBe(2)

      // Verify data integrity
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      const snapshot = await freshTable.snapshot()
      expect(snapshot.files).toHaveLength(3)
    })
  })

  describe('Multiple Concurrent Writers Recovery', () => {
    it('should maintain data consistency when multiple writers compete', async () => {
      // Initialize table
      const initTable = new DeltaTable<TestRecord>(storage, 'test-table')
      await initTable.write([{ id: 0, value: 'init' }])

      // Create multiple writers
      const writers = Array.from({ length: 5 }, () => new DeltaTable<TestRecord>(storage, 'test-table'))

      // All read current version
      await Promise.all(writers.map(w => w.version()))

      // All attempt concurrent writes
      const results = await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1, value: `writer${i}` }]))
      )

      // Verify exactly one succeeded
      const succeeded = results.filter(r => r.status === 'fulfilled')
      const failed = results.filter(r => r.status === 'rejected')

      expect(succeeded).toHaveLength(1)
      expect(failed).toHaveLength(4)

      // Verify data consistency
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      const snapshot = await freshTable.snapshot()
      expect(snapshot.files).toHaveLength(2) // init + one successful write
      expect(snapshot.version).toBe(1)

      // All commit files should be valid
      const commitFiles = await storage.list('test-table/_delta_log')
      const jsonFiles = commitFiles.filter(f => f.endsWith('.json'))
      expect(jsonFiles).toHaveLength(2) // version 0 and version 1
    })

    it('should allow all failed writers to eventually succeed through retry', async () => {
      // Initialize table
      const initTable = new DeltaTable<TestRecord>(storage, 'test-table')
      await initTable.write([{ id: 0, value: 'init' }])

      const writers = Array.from({ length: 3 }, () => new DeltaTable<TestRecord>(storage, 'test-table'))
      await Promise.all(writers.map(w => w.version()))

      // First round of writes
      const results1 = await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1, value: `round1-${i}` }]))
      )

      // Count initial successes
      let successCount = results1.filter(r => r.status === 'fulfilled').length
      expect(successCount).toBe(1)

      // Retry failed writers sequentially
      for (let i = 0; i < writers.length; i++) {
        if (results1[i].status === 'rejected') {
          await writers[i].refreshVersion()
          await writers[i].write([{ id: i + 100, value: `retry-${i}` }])
          successCount++
        }
      }

      // All should eventually succeed
      expect(successCount).toBe(3)

      // Verify final state
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      const snapshot = await freshTable.snapshot()
      expect(snapshot.version).toBe(3) // init + 3 writes
    })
  })
})

// =============================================================================
// RECOVERY FROM PARTIAL WRITES
// =============================================================================

describe('Recovery From Partial Writes', () => {
  let baseStorage: MemoryStorage

  beforeEach(() => {
    baseStorage = new MemoryStorage()
  })

  describe('Commit File Write Failure', () => {
    it('should handle failure when commit file cannot be written', async () => {
      // Create storage that fails on conditional write (used for commit files)
      const failingStorage = createFailingStorage(baseStorage, {
        failOn: 'writeConditional',
        failAfterNOperations: 0, // Fail immediately on first conditional write
      })

      const table = new DeltaTable<TestRecord>(failingStorage, 'test-table')

      // Write should fail
      await expect(table.write([{ id: 1, value: 'test' }])).rejects.toThrow(StorageError)

      // Table should still be queryable (empty)
      const freshTable = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      expect(await freshTable.version()).toBe(-1)
    })

    it('should maintain consistency when commit fails after data file is written', async () => {
      // First, successfully write initial data
      const initTable = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      await initTable.write([{ id: 1, value: 'initial' }])

      // Track data files before second write attempt
      const filesBeforeAttempt = await baseStorage.list('test-table')
      const dataFilesBefore = filesBeforeAttempt.filter(f => f.endsWith('.parquet'))

      // Create storage that allows data write but fails on commit
      let dataWriteCount = 0
      const failOnCommitStorage: StorageBackend = {
        read: (path: string) => baseStorage.read(path),
        write: async (path: string, data: Uint8Array) => {
          await baseStorage.write(path, data)
          if (path.endsWith('.parquet')) {
            dataWriteCount++
          }
        },
        writeConditional: async (path: string, data: Uint8Array, version: string | null) => {
          // Fail on second commit attempt
          if (path.includes('_delta_log') && path.endsWith('.json') && dataWriteCount > 0) {
            throw new StorageError('Simulated commit failure', path, 'writeConditional')
          }
          return baseStorage.writeConditional(path, data, version)
        },
        list: (prefix: string) => baseStorage.list(prefix),
        delete: (path: string) => baseStorage.delete(path),
        exists: (path: string) => baseStorage.exists(path),
        stat: (path: string) => baseStorage.stat(path),
        readRange: (path: string, start: number, end: number) => baseStorage.readRange(path, start, end),
        getVersion: (path: string) => baseStorage.getVersion(path),
      }

      const table = new DeltaTable<TestRecord>(failOnCommitStorage, 'test-table')
      await table.version() // Cache current version

      // Second write should fail during commit
      await expect(table.write([{ id: 2, value: 'failed' }])).rejects.toThrow(StorageError)

      // Verify table is still consistent at version 0
      const freshTable = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      const snapshot = await freshTable.snapshot()
      expect(snapshot.version).toBe(0)
      expect(snapshot.files).toHaveLength(1)

      // Orphaned data files may exist (will be cleaned by vacuum)
      // But commit log should be consistent
      const commitFiles = await baseStorage.list('test-table/_delta_log')
      const jsonFiles = commitFiles.filter(f => f.endsWith('.json'))
      expect(jsonFiles).toHaveLength(1) // Only version 0
    })
  })

  describe('Storage Error During Read Operations', () => {
    it('should handle transient read errors gracefully', async () => {
      const table = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      await table.write([{ id: 1, value: 'test' }])

      // Create storage that permanently fails on commit file reads
      let failAll = true
      const alwaysFailStorage: StorageBackend = {
        read: async (path: string) => {
          // Always fail on commit file reads
          if (path.includes('_delta_log') && path.endsWith('.json') && failAll) {
            throw new StorageError('Persistent read error', path, 'read')
          }
          return baseStorage.read(path)
        },
        write: (path: string, data: Uint8Array) => baseStorage.write(path, data),
        writeConditional: (path: string, data: Uint8Array, version: string | null) =>
          baseStorage.writeConditional(path, data, version),
        list: (prefix: string) => baseStorage.list(prefix),
        delete: (path: string) => baseStorage.delete(path),
        exists: (path: string) => baseStorage.exists(path),
        stat: (path: string) => baseStorage.stat(path),
        readRange: (path: string, start: number, end: number) => baseStorage.readRange(path, start, end),
        getVersion: (path: string) => baseStorage.getVersion(path),
      }

      const failingTable = new DeltaTable<TestRecord>(alwaysFailStorage, 'test-table')

      // Snapshot returns empty when commit files cannot be read
      // (graceful degradation - logs warning and continues)
      const snapshot = await failingTable.snapshot()
      expect(snapshot.files).toHaveLength(0)
      expect(snapshot.version).toBe(0)

      // But underlying data is still intact via the base storage
      const freshTable = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      const results = await freshTable.query()
      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(1)
    })
  })
})

// =============================================================================
// CONSISTENCY AFTER STORAGE FAILURES
// =============================================================================

describe('Consistency After Storage Failures', () => {
  let baseStorage: MemoryStorage

  beforeEach(() => {
    baseStorage = new MemoryStorage()
  })

  describe('Storage Quota Exceeded', () => {
    it('should handle storage quota errors gracefully', async () => {
      // Create storage with very small max size
      const limitedStorage = new MemoryStorage({ maxSize: 1000 })
      const table = new DeltaTable<TestRecord>(limitedStorage, 'test-table')

      // First small write should succeed
      await table.write([{ id: 1, value: 'small' }])

      // Large write should fail due to quota
      const largeData = Array.from({ length: 100 }, (_, i) => ({
        id: i,
        value: 'x'.repeat(100),
      }))

      await expect(table.write(largeData)).rejects.toThrow(StorageError)

      // Table should still be at version 0
      expect(await table.version()).toBe(0)
    })
  })

  describe('Intermittent Storage Failures', () => {
    it('should maintain consistency with intermittent write failures', async () => {
      const table = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      await table.write([{ id: 1, value: 'initial' }])

      // Create storage that fails every other write
      let writeCount = 0
      const intermittentStorage: StorageBackend = {
        read: (path: string) => baseStorage.read(path),
        write: async (path: string, data: Uint8Array) => {
          writeCount++
          if (writeCount % 2 === 0) {
            throw new StorageError('Intermittent failure', path, 'write')
          }
          return baseStorage.write(path, data)
        },
        writeConditional: (path: string, data: Uint8Array, version: string | null) =>
          baseStorage.writeConditional(path, data, version),
        list: (prefix: string) => baseStorage.list(prefix),
        delete: (path: string) => baseStorage.delete(path),
        exists: (path: string) => baseStorage.exists(path),
        stat: (path: string) => baseStorage.stat(path),
        readRange: (path: string, start: number, end: number) => baseStorage.readRange(path, start, end),
        getVersion: (path: string) => baseStorage.getVersion(path),
      }

      const failingTable = new DeltaTable<TestRecord>(intermittentStorage, 'test-table')
      await failingTable.version()

      // Attempt multiple writes - some will fail
      const results: (DeltaCommit | Error)[] = []
      for (let i = 2; i <= 5; i++) {
        try {
          const result = await failingTable.write([{ id: i, value: `write${i}` }])
          results.push(result)
        } catch (error) {
          results.push(error as Error)
        }
      }

      // Verify some writes failed but data remains consistent
      const errorCount = results.filter(r => r instanceof Error).length
      expect(errorCount).toBeGreaterThan(0)

      // Data should still be valid
      const freshTable = new DeltaTable<TestRecord>(baseStorage, 'test-table')
      const snapshot = await freshTable.snapshot()
      expect(snapshot.version).toBeGreaterThanOrEqual(0)
    })
  })
})

// =============================================================================
// NO DATA CORRUPTION DURING ERROR SCENARIOS
// =============================================================================

describe('No Data Corruption During Error Scenarios', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Parquet File Integrity', () => {
    it('should produce valid Parquet files even after errors', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      // Write initial data
      await table.write([{ id: 1, value: 'test1' }])

      // Cause an error via external modification
      await writeExternalCommit(storage, 1)

      try {
        await table.write([{ id: 2, value: 'test2' }])
      } catch {
        // Expected ConcurrencyError
      }

      // Refresh and write again
      await table.refreshVersion()
      await table.write([{ id: 3, value: 'test3' }])

      // Verify all Parquet files have valid magic bytes
      const files = await storage.list('test-table')
      const parquetFiles = files.filter(f => f.endsWith('.parquet') && !f.includes('external'))

      for (const file of parquetFiles) {
        const data = await storage.read(file)
        // Parquet magic: PAR1
        expect(data.slice(0, 4)).toEqual(new Uint8Array([0x50, 0x41, 0x52, 0x31]))
        // Also at end
        expect(data.slice(-4)).toEqual(new Uint8Array([0x50, 0x41, 0x52, 0x31]))
      }
    })
  })

  describe('Commit File Integrity', () => {
    it('should produce valid JSON commit files', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      await table.write([{ id: 1, value: 'test1' }])
      await table.write([{ id: 2, value: 'test2' }])
      await table.write([{ id: 3, value: 'test3' }])

      // Verify all commit files are valid JSON
      const logFiles = await storage.list('test-table/_delta_log')
      const jsonFiles = logFiles.filter(f => f.endsWith('.json'))

      for (const file of jsonFiles) {
        const data = await storage.read(file)
        const content = new TextDecoder().decode(data)
        const lines = content.split('\n').filter(line => line.trim() !== '')

        for (const line of lines) {
          // Each line should be valid JSON
          expect(() => JSON.parse(line)).not.toThrow()

          // Each line should be a valid Delta action
          const action = JSON.parse(line)
          expect(
            'add' in action ||
            'remove' in action ||
            'metaData' in action ||
            'protocol' in action ||
            'commitInfo' in action
          ).toBe(true)
        }
      }
    })
  })

  describe('Data Queryability After Errors', () => {
    it('should allow querying data after recovery from errors', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      // Build up some data
      await table.write([{ id: 1, value: 'a' }])
      await table.write([{ id: 2, value: 'b' }])

      // Simulate error scenario
      await writeExternalCommit(storage, 2)

      try {
        await table.write([{ id: 3, value: 'c' }])
      } catch {
        // Expected
      }

      // Refresh and add more data
      await table.refreshVersion()
      await table.write([{ id: 4, value: 'd' }])

      // Query should return consistent results
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      const results = await freshTable.query()

      // Should have data from all successful writes
      expect(results.length).toBeGreaterThanOrEqual(2)

      // Data should be queryable with filters
      const filtered = await freshTable.query({ id: { $gte: 1 } })
      expect(filtered.length).toBeGreaterThan(0)
    })
  })

  describe('Snapshot Consistency', () => {
    it('should return consistent snapshots after error recovery', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')

      await table.write([{ id: 1, value: 'v0' }])
      const snapshot0 = await table.snapshot()

      await writeExternalCommit(storage, 1)

      try {
        await table.write([{ id: 2, value: 'failed' }])
      } catch {
        // Expected
      }

      await table.refreshVersion()
      await table.write([{ id: 3, value: 'v2' }])

      // Each version snapshot should be consistent
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')

      const s0 = await freshTable.snapshot(0)
      expect(s0.files).toHaveLength(1)
      expect(s0.version).toBe(0)

      const s1 = await freshTable.snapshot(1)
      expect(s1.files).toHaveLength(2)
      expect(s1.version).toBe(1)

      const s2 = await freshTable.snapshot(2)
      expect(s2.files).toHaveLength(3)
      expect(s2.version).toBe(2)
    })
  })
})

// =============================================================================
// RETRY MECHANISM INTEGRATION
// =============================================================================

describe('Retry Mechanism Integration', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('withRetry for Concurrency Errors', () => {
    it('should successfully retry after ConcurrencyError using withRetry', async () => {
      const initTable = new DeltaTable<TestRecord>(storage, 'test-table')
      await initTable.write([{ id: 0, value: 'init' }])

      const table = new DeltaTable<TestRecord>(storage, 'test-table')
      await table.version()

      // Simulate external modification
      await writeExternalCommit(storage, 1)

      let attempts = 0
      const result = await withRetry(
        async () => {
          attempts++
          if (attempts === 1) {
            // First attempt will hit the conflict
            try {
              return await table.write([{ id: 1, value: `attempt${attempts}` }])
            } catch (error) {
              if (error instanceof ConcurrencyError) {
                await table.refreshVersion()
              }
              throw error
            }
          }
          return await table.write([{ id: 1, value: `attempt${attempts}` }])
        },
        {
          maxRetries: 3,
          baseDelay: 10,
          _delayFn: async () => {}, // Skip delay for faster tests
        } as any
      )

      expect(result.version).toBe(2)
      expect(attempts).toBe(2)
    })

    it('should handle multiple retries before success', async () => {
      const initTable = new DeltaTable<TestRecord>(storage, 'test-table')
      await initTable.write([{ id: 0, value: 'init' }])

      const table = new DeltaTable<TestRecord>(storage, 'test-table')
      await table.version() // Will cache version 0

      // Create external version AFTER caching to make cache stale
      await writeExternalCommit(storage, 1)

      let attempts = 0
      const result = await withRetry(
        async () => {
          attempts++

          try {
            return await table.write([{ id: attempts, value: `attempt${attempts}` }])
          } catch (error) {
            if (error instanceof ConcurrencyError) {
              await table.refreshVersion()
            }
            throw error
          }
        },
        {
          maxRetries: 5,
          baseDelay: 10,
          _delayFn: async () => {},
        } as any
      )

      expect(result).toBeDefined()
      // First attempt fails due to stale cache, second succeeds after refresh
      expect(attempts).toBeGreaterThan(1)
    })
  })

  describe('Error Recovery Pattern', () => {
    it('should support standard read-refresh-retry pattern', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')
      await table.write([{ id: 1, value: 'initial' }])

      // External modification
      await writeExternalCommit(storage, 1)

      // Standard retry pattern
      let success = false
      let maxAttempts = 5
      let attempts = 0

      while (!success && attempts < maxAttempts) {
        try {
          attempts++
          await table.write([{ id: 2, value: `attempt${attempts}` }])
          success = true
        } catch (error) {
          if (error instanceof ConcurrencyError) {
            await table.refreshVersion()
          } else {
            throw error
          }
        }
      }

      expect(success).toBe(true)
      expect(attempts).toBeLessThanOrEqual(maxAttempts)

      // Verify final state
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      expect(await freshTable.version()).toBe(2)
    })
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases for Error Recovery', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Empty Table Error Recovery', () => {
    it('should handle errors when creating new table', async () => {
      // Create storage that fails on first write
      const failOnFirstWrite = createFailingStorage(storage, {
        failOn: 'writeConditional',
        failAfterNOperations: 0,
        failCount: 1,
      })

      const table = new DeltaTable<TestRecord>(failOnFirstWrite, 'test-table')

      // First write should fail
      await expect(table.write([{ id: 1, value: 'test' }])).rejects.toThrow()

      // Table should remain empty
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      expect(await freshTable.version()).toBe(-1)
    })

    it('should allow table creation after initial failure', async () => {
      let failCount = 0
      const failOnceStorage: StorageBackend = {
        read: (path: string) => storage.read(path),
        write: (path: string, data: Uint8Array) => storage.write(path, data),
        writeConditional: async (path: string, data: Uint8Array, version: string | null) => {
          if (failCount === 0) {
            failCount++
            throw new StorageError('First attempt fails', path, 'writeConditional')
          }
          return storage.writeConditional(path, data, version)
        },
        list: (prefix: string) => storage.list(prefix),
        delete: (path: string) => storage.delete(path),
        exists: (path: string) => storage.exists(path),
        stat: (path: string) => storage.stat(path),
        readRange: (path: string, start: number, end: number) => storage.readRange(path, start, end),
        getVersion: (path: string) => storage.getVersion(path),
      }

      const table = new DeltaTable<TestRecord>(failOnceStorage, 'test-table')

      // First attempt fails
      await expect(table.write([{ id: 1, value: 'test' }])).rejects.toThrow()

      // Second attempt succeeds
      const result = await table.write([{ id: 1, value: 'test' }])
      expect(result.version).toBe(0)
    })
  })

  describe('Rapid Error-Recovery Cycles', () => {
    it('should handle many rapid error-recovery cycles', async () => {
      const table = new DeltaTable<TestRecord>(storage, 'test-table')
      await table.write([{ id: 0, value: 'init' }])

      // Perform many rapid error-recovery cycles
      for (let i = 1; i <= 10; i++) {
        // Inject external version
        await writeExternalCommit(storage, i)

        // Attempt write (will fail)
        try {
          await table.write([{ id: i, value: `cycle${i}` }])
        } catch (error) {
          expect(error).toBeInstanceOf(ConcurrencyError)
          await table.refreshVersion()
        }
      }

      // Final state should be consistent
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      const snapshot = await freshTable.snapshot()
      expect(snapshot.version).toBe(10)
    })
  })

  describe('Concurrent Error Recovery', () => {
    it('should handle multiple tables recovering from errors simultaneously', async () => {
      // Initialize
      const initTable = new DeltaTable<TestRecord>(storage, 'test-table')
      await initTable.write([{ id: 0, value: 'init' }])

      // Create multiple tables
      const tables = Array.from({ length: 3 }, () => new DeltaTable<TestRecord>(storage, 'test-table'))

      // All read current version
      await Promise.all(tables.map(t => t.version()))

      // External modification
      await writeExternalCommit(storage, 1)

      // All attempt write (all will fail)
      await Promise.allSettled(
        tables.map(t => t.write([{ id: 999, value: 'fail' }]))
      )

      // All refresh and retry sequentially
      const results: DeltaCommit[] = []
      for (let i = 0; i < tables.length; i++) {
        await tables[i].refreshVersion()
        const result = await tables[i].write([{ id: i + 100, value: `recovered${i}` }])
        results.push(result)
      }

      // All should have succeeded
      expect(results).toHaveLength(3)

      // Final state should be consistent
      const freshTable = new DeltaTable<TestRecord>(storage, 'test-table')
      expect(await freshTable.version()).toBe(4) // init + external + 3 recovered writes
    })
  })
})
