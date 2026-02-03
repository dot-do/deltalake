/**
 * Concurrent Write Integration Tests
 *
 * Tests for concurrent write scenarios in Delta Lake tables.
 * These integration tests verify:
 * - Multiple writers attempting concurrent commits
 * - Optimistic concurrency control behavior
 * - Version conflict detection and handling
 * - Retry mechanisms after conflicts
 * - Data integrity under concurrent access
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import {
  DeltaTable,
  ConcurrencyError,
  type DeltaCommit,
} from '../../src/delta/index.js'
import {
  MemoryStorage,
  R2Storage,
  VersionMismatchError,
  type StorageBackend,
} from '../../src/storage/index.js'

// =============================================================================
// TEST FIXTURES AND HELPERS
// =============================================================================

interface TestRecord {
  id: number
  value: string
  writer?: string
  timestamp?: number
}

/**
 * Create a test table with the given storage backend
 */
function createTable(storage: StorageBackend, path: string = 'test-table'): DeltaTable<TestRecord> {
  return new DeltaTable<TestRecord>(storage, path)
}

/**
 * Helper to write a commit directly to storage (simulates external writer)
 */
async function writeExternalCommit(
  storage: StorageBackend,
  tablePath: string,
  version: number
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
 * Delay utility for timing tests
 */
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// =============================================================================
// CONCURRENT WRITE DETECTION TESTS
// =============================================================================

describe('Concurrent Write Detection', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('External Modification Detection', () => {
    it('should detect when another writer modified the table', async () => {
      const table = createTable(storage)

      // Initial write
      await table.write([{ id: 1, value: 'initial' }])
      expect(await table.version()).toBe(0)

      // Simulate external writer creating version 1
      await writeExternalCommit(storage, 'test-table', 1)

      // Our table still has cached version 0
      // Write should detect the conflict
      await expect(table.write([{ id: 2, value: 'conflict' }])).rejects.toThrow(ConcurrencyError)
    })

    it('should include version information in ConcurrencyError', async () => {
      const table = createTable(storage)

      await table.write([{ id: 1, value: 'first' }])
      await writeExternalCommit(storage, 'test-table', 1)
      await writeExternalCommit(storage, 'test-table', 2)

      try {
        await table.write([{ id: 2, value: 'conflict' }])
        expect.fail('Should have thrown ConcurrencyError')
      } catch (error) {
        expect(error).toBeInstanceOf(ConcurrencyError)
        const concurrencyError = error as ConcurrencyError
        expect(concurrencyError.expectedVersion).toBe(0)
        expect(concurrencyError.actualVersion).toBe(2)
      }
    })

    it('should succeed when no external modifications occurred', async () => {
      const table = createTable(storage)

      await table.write([{ id: 1, value: 'first' }])

      // No external modifications - should succeed
      const result = await table.write([{ id: 2, value: 'second' }])
      expect(result.version).toBe(1)
    })
  })

  describe('Stale Cache Detection', () => {
    it('should detect stale version cache after external write', async () => {
      const table = createTable(storage)

      await table.write([{ id: 1, value: 'initial' }])

      // Cache the version
      const cachedVersion = await table.version()
      expect(cachedVersion).toBe(0)

      // External modification
      await writeExternalCommit(storage, 'test-table', 1)

      // Write should fail due to stale cache
      await expect(table.write([{ id: 2, value: 'new' }])).rejects.toThrow(ConcurrencyError)
    })

    it('should allow write after refreshing version', async () => {
      const table = createTable(storage)

      await table.write([{ id: 1, value: 'initial' }])
      await writeExternalCommit(storage, 'test-table', 1)

      // First write should fail
      await expect(table.write([{ id: 2, value: 'fail' }])).rejects.toThrow(ConcurrencyError)

      // Refresh the version cache
      await table.refreshVersion()
      expect(await table.version()).toBe(1)

      // Now write should succeed
      const result = await table.write([{ id: 3, value: 'success' }])
      expect(result.version).toBe(2)
    })
  })
})

// =============================================================================
// TWO WRITER SCENARIO TESTS
// =============================================================================

describe('Two Writer Scenarios', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Sequential Writes from Different Instances', () => {
    it('should allow sequential writes when each refreshes first', async () => {
      const writer1 = createTable(storage)
      const writer2 = createTable(storage)

      // Writer1 creates the table
      await writer1.write([{ id: 1, value: 'from-writer1', writer: 'writer1' }])

      // Writer2 refreshes and writes
      await writer2.refreshVersion()
      await writer2.write([{ id: 2, value: 'from-writer2', writer: 'writer2' }])

      // Writer1 refreshes and writes again
      await writer1.refreshVersion()
      await writer1.write([{ id: 3, value: 'from-writer1-again', writer: 'writer1' }])

      // Verify all writes succeeded
      const freshTable = createTable(storage)
      const results = await freshTable.query()
      expect(results).toHaveLength(3)
      expect(await freshTable.version()).toBe(2)
    })

    it('should fail second write when first has not synced', async () => {
      const writer1 = createTable(storage)
      const writer2 = createTable(storage)

      // Both read initial state (empty table)
      await writer1.version()
      await writer2.version()

      // Writer1 creates the table
      await writer1.write([{ id: 1, value: 'first' }])

      // Writer2 tries to write without refresh - should fail
      await expect(
        writer2.write([{ id: 2, value: 'conflict' }])
      ).rejects.toThrow(ConcurrencyError)
    })
  })

  describe('Concurrent Write Race', () => {
    it('should allow only one of two concurrent writers to succeed', async () => {
      // Initialize the table first
      const initTable = createTable(storage)
      await initTable.write([{ id: 0, value: 'init' }])

      // Create two independent writers
      const writer1 = createTable(storage)
      const writer2 = createTable(storage)

      // Both read current version (0)
      await writer1.version()
      await writer2.version()

      // Both attempt concurrent writes
      const results = await Promise.allSettled([
        writer1.write([{ id: 1, value: 'from-writer1', writer: 'writer1' }]),
        writer2.write([{ id: 2, value: 'from-writer2', writer: 'writer2' }]),
      ])

      // Exactly one should succeed, one should fail
      const succeeded = results.filter(r => r.status === 'fulfilled')
      const failed = results.filter(r => r.status === 'rejected')

      expect(succeeded).toHaveLength(1)
      expect(failed).toHaveLength(1)

      // Failed one should be ConcurrencyError
      const failedResult = failed[0] as PromiseRejectedResult
      expect(failedResult.reason).toBeInstanceOf(ConcurrencyError)
    })

    it('should maintain data integrity after concurrent attempts', async () => {
      // Initialize
      const initTable = createTable(storage)
      await initTable.write([{ id: 0, value: 'init' }])

      const writer1 = createTable(storage)
      const writer2 = createTable(storage)

      await writer1.version()
      await writer2.version()

      await Promise.allSettled([
        writer1.write([{ id: 1, value: 'w1' }]),
        writer2.write([{ id: 2, value: 'w2' }]),
      ])

      // Verify table integrity
      const freshTable = createTable(storage)
      const snapshot = await freshTable.snapshot()

      // Should have exactly 2 files (init + one successful write)
      expect(snapshot.files).toHaveLength(2)
      expect(snapshot.version).toBe(1)

      const results = await freshTable.query()
      expect(results).toHaveLength(2)
    })
  })
})

// =============================================================================
// MULTIPLE CONCURRENT WRITERS TESTS
// =============================================================================

describe('Multiple Concurrent Writers', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Many Writers Race', () => {
    it('should handle 5 concurrent writers correctly', async () => {
      // Initialize table
      const initTable = createTable(storage)
      await initTable.write([{ id: 0, value: 'init' }])

      // Create 5 writers
      const writers = Array.from({ length: 5 }, () => createTable(storage))

      // All read current version
      await Promise.all(writers.map(w => w.version()))

      // All attempt to write concurrently
      const results = await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1, value: `writer${i}`, writer: `w${i}` }]))
      )

      // Exactly one should succeed
      const succeeded = results.filter(r => r.status === 'fulfilled')
      expect(succeeded).toHaveLength(1)

      // Others should fail with ConcurrencyError
      const failed = results.filter(r => r.status === 'rejected')
      expect(failed).toHaveLength(4)

      failed.forEach(result => {
        const rejected = result as PromiseRejectedResult
        expect(rejected.reason).toBeInstanceOf(ConcurrencyError)
      })
    })

    it('should handle 10 concurrent writers without data corruption', async () => {
      // Initialize
      const initTable = createTable(storage)
      await initTable.write([{ id: 0, value: 'init' }])

      const writers = Array.from({ length: 10 }, () => createTable(storage))
      await Promise.all(writers.map(w => w.version()))

      await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1, value: `data${i}` }]))
      )

      // Verify no corruption
      const freshTable = createTable(storage)
      const snapshot = await freshTable.snapshot()

      // Should have exactly 2 files
      expect(snapshot.files).toHaveLength(2)

      // All files should be valid Parquet
      for (const file of snapshot.files) {
        const data = await storage.read(file.path)
        // Check Parquet magic bytes
        expect(data.slice(0, 4)).toEqual(new Uint8Array([0x50, 0x41, 0x52, 0x31]))
      }
    })
  })

  describe('Retry Mechanism', () => {
    it('should allow failed writers to succeed after refresh and retry', async () => {
      // Initialize
      const initTable = createTable(storage)
      await initTable.write([{ id: 0, value: 'init' }])

      const writers = Array.from({ length: 3 }, () => createTable(storage))
      await Promise.all(writers.map(w => w.version()))

      // First round - one succeeds
      const results1 = await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1, value: `round1-${i}` }]))
      )

      const firstRoundSucceeded = results1.filter(r => r.status === 'fulfilled').length
      expect(firstRoundSucceeded).toBe(1)

      // Failed writers refresh and retry
      for (let i = 0; i < writers.length; i++) {
        if (results1[i].status === 'rejected') {
          await writers[i].refreshVersion()
          await writers[i].write([{ id: i + 100, value: `retry-${i}` }])
        }
      }

      // All should have eventually succeeded
      const finalSnapshot = await createTable(storage).snapshot()
      expect(finalSnapshot.version).toBe(3) // init + 3 successful writes
    })
  })
})

// =============================================================================
// CONDITIONAL WRITE TESTS
// =============================================================================

describe('Conditional Write Behavior', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Storage-Level Conditional Writes', () => {
    it('should use conditional writes for commit files', async () => {
      const table = createTable(storage)

      // Track writeConditional calls
      const conditionalWrites: string[] = []
      const originalWriteConditional = storage.writeConditional.bind(storage)
      storage.writeConditional = async (path, data, version) => {
        conditionalWrites.push(path)
        return originalWriteConditional(path, data, version)
      }

      await table.write([{ id: 1, value: 'test' }])

      // Commit file should be written with conditional write
      expect(conditionalWrites.some(p => p.includes('_delta_log') && p.endsWith('.json'))).toBe(true)
    })

    it('should prevent duplicate version files', async () => {
      const table1 = createTable(storage)
      const table2 = createTable(storage)

      // Table 1 writes version 0
      await table1.write([{ id: 1, value: 'first' }])

      // Table 2 also tries to write version 0 (without knowing about table1)
      // This should fail at the storage level
      await expect(
        table2.write([{ id: 2, value: 'duplicate' }])
      ).rejects.toThrow()
    })
  })

  describe('Version File Atomicity', () => {
    it('should not leave partial commit files on failure', async () => {
      const table = createTable(storage)

      await table.write([{ id: 1, value: 'first' }])

      // Simulate external write
      await writeExternalCommit(storage, 'test-table', 1)

      // Try to write (should fail)
      try {
        await table.write([{ id: 2, value: 'conflict' }])
      } catch {
        // Expected
      }

      // Check that no partial version 1 file exists from our attempt
      // (external write already created version 1, we would try version 1)
      const files = await storage.list('test-table/_delta_log')
      const jsonFiles = files.filter(f => f.endsWith('.json'))

      // Should only have version 0 and version 1 (from external)
      expect(jsonFiles.length).toBe(2)
    })
  })
})

// =============================================================================
// R2 STORAGE CONCURRENT WRITE TESTS
// =============================================================================

describe('Concurrent Writes - R2Storage', () => {
  let storage: R2Storage
  const tablePath = `test-table-${Date.now()}`

  beforeEach(() => {
    storage = new R2Storage({ bucket: env.TEST_BUCKET })
  })

  it('should handle concurrent writes in R2', async () => {
    // Initialize
    const initTable = createTable(storage, tablePath)
    await initTable.write([{ id: 0, value: 'init' }])

    const writer1 = createTable(storage, tablePath)
    const writer2 = createTable(storage, tablePath)

    await writer1.version()
    await writer2.version()

    const results = await Promise.allSettled([
      writer1.write([{ id: 1, value: 'w1' }]),
      writer2.write([{ id: 2, value: 'w2' }]),
    ])

    const succeeded = results.filter(r => r.status === 'fulfilled')
    expect(succeeded).toHaveLength(1)
  })

  it('should maintain consistency after concurrent R2 writes', async () => {
    const initTable = createTable(storage, tablePath)
    await initTable.write([{ id: 0, value: 'init' }])

    const writers = Array.from({ length: 3 }, () => createTable(storage, tablePath))
    await Promise.all(writers.map(w => w.version()))

    await Promise.allSettled(
      writers.map((w, i) => w.write([{ id: i + 1, value: `data${i}` }]))
    )

    // Verify consistency
    const freshTable = createTable(storage, tablePath)
    const snapshot = await freshTable.snapshot()
    expect(snapshot.files).toHaveLength(2)
  })
})

// =============================================================================
// ERROR HANDLING AND RECOVERY TESTS
// =============================================================================

describe('Error Handling and Recovery', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('ConcurrencyError Properties', () => {
    it('should have correct error name', async () => {
      const table = createTable(storage)
      await table.write([{ id: 1, value: 'first' }])
      await writeExternalCommit(storage, 'test-table', 1)

      try {
        await table.write([{ id: 2, value: 'conflict' }])
      } catch (error) {
        expect(error).toBeInstanceOf(ConcurrencyError)
        expect((error as ConcurrencyError).name).toBe('ConcurrencyError')
      }
    })

    it('should include helpful message', async () => {
      const table = createTable(storage)
      await table.write([{ id: 1, value: 'first' }])
      await writeExternalCommit(storage, 'test-table', 1)

      try {
        await table.write([{ id: 2, value: 'conflict' }])
      } catch (error) {
        const message = (error as Error).message.toLowerCase()
        expect(message).toMatch(/version|conflict|concurrent|modified/i)
      }
    })
  })

  describe('Recovery Patterns', () => {
    it('should support read-refresh-retry pattern', async () => {
      const table = createTable(storage)
      await table.write([{ id: 1, value: 'initial' }])

      // Simulate external modifications
      await writeExternalCommit(storage, 'test-table', 1)

      // Implement retry pattern
      let attempts = 0
      const maxAttempts = 3

      while (attempts < maxAttempts) {
        try {
          await table.write([{ id: 2, value: `attempt${attempts}` }])
          break
        } catch (error) {
          if (error instanceof ConcurrencyError) {
            attempts++
            await table.refreshVersion()
          } else {
            throw error
          }
        }
      }

      expect(attempts).toBeLessThan(maxAttempts)
      expect(await table.version()).toBe(2)
    })

    it('should provide updated version info after conflict', async () => {
      const table = createTable(storage)
      await table.write([{ id: 1, value: 'first' }])

      // Multiple external writes
      await writeExternalCommit(storage, 'test-table', 1)
      await writeExternalCommit(storage, 'test-table', 2)
      await writeExternalCommit(storage, 'test-table', 3)

      try {
        await table.write([{ id: 2, value: 'conflict' }])
      } catch (error) {
        const concurrencyError = error as ConcurrencyError
        // actualVersion should show the current true version
        expect(concurrencyError.actualVersion).toBe(3)
      }
    })
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Empty Table Conflicts', () => {
    it('should handle concurrent creation of new table', async () => {
      const writer1 = createTable(storage)
      const writer2 = createTable(storage)

      // Both see empty table (version -1)
      expect(await writer1.version()).toBe(-1)
      expect(await writer2.version()).toBe(-1)

      // Both try to create
      const results = await Promise.allSettled([
        writer1.write([{ id: 1, value: 'creator1' }]),
        writer2.write([{ id: 2, value: 'creator2' }]),
      ])

      const succeeded = results.filter(r => r.status === 'fulfilled')
      expect(succeeded).toHaveLength(1)

      // Table should exist with one record
      const freshTable = createTable(storage)
      expect(await freshTable.version()).toBe(0)
    })
  })

  describe('Rapid Sequential Writes', () => {
    it('should handle rapid writes from same instance', async () => {
      const table = createTable(storage)

      // Rapid sequential writes should all succeed
      await table.write([{ id: 1, value: 'v1' }])
      await table.write([{ id: 2, value: 'v2' }])
      await table.write([{ id: 3, value: 'v3' }])
      await table.write([{ id: 4, value: 'v4' }])
      await table.write([{ id: 5, value: 'v5' }])

      expect(await table.version()).toBe(4)

      const results = await table.query()
      expect(results).toHaveLength(5)
    })
  })

  describe('Version Gap Detection', () => {
    it('should detect large version gaps from external writes', async () => {
      const table = createTable(storage)
      await table.write([{ id: 1, value: 'first' }])

      // Multiple external writes creating a gap
      for (let v = 1; v <= 10; v++) {
        await writeExternalCommit(storage, 'test-table', v)
      }

      try {
        await table.write([{ id: 2, value: 'conflict' }])
        expect.fail('Should have thrown')
      } catch (error) {
        const concurrencyError = error as ConcurrencyError
        expect(concurrencyError.expectedVersion).toBe(0)
        expect(concurrencyError.actualVersion).toBe(10)
      }
    })
  })
})
