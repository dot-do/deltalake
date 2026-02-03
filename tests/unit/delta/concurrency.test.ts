/**
 * Version Conflict Detection and Optimistic Concurrency Control Tests (RED Phase)
 *
 * Delta Lake requires optimistic concurrency control to prevent data corruption
 * when multiple writers attempt to modify the same table concurrently.
 *
 * The protocol:
 * 1. Read current version (cached in DeltaTable.currentVersion)
 * 2. Prepare write operations based on that version
 * 3. Before writing commit file, verify version hasn't changed
 * 4. If version changed, throw ConcurrencyError
 * 5. Client can refresh version and retry
 *
 * These tests define the expected behavior for optimistic concurrency control.
 * All tests will fail until the ConcurrencyError class and version conflict
 * detection are implemented.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  DeltaTable,
  type DeltaCommit,
  type AddAction,
} from '../../../src/delta/index.js'
import { MemoryStorage, type StorageBackend } from '../../../src/storage/index.js'

// =============================================================================
// CONCURRENCY ERROR CLASS
// =============================================================================

/**
 * ConcurrencyError should be thrown when a version conflict is detected.
 * This error class needs to be implemented and exported from src/delta/index.ts
 */

// Import and check if ConcurrencyError is exported
import * as deltaModule from '../../../src/delta/index.js'

// Get ConcurrencyError from module - will be undefined if not exported
const ConcurrencyError: typeof Error | undefined = (deltaModule as any).ConcurrencyError

/**
 * Helper to assert ConcurrencyError is defined before using it.
 */
function requireConcurrencyError(): typeof Error {
  if (!ConcurrencyError) {
    throw new Error(
      'ConcurrencyError is not exported from src/delta/index.ts. ' +
      'This test requires the ConcurrencyError class to be implemented and exported.'
    )
  }
  return ConcurrencyError
}

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a test DeltaTable with memory storage
 */
function createTestTable(): { table: DeltaTable; storage: MemoryStorage } {
  const storage = new MemoryStorage()
  const table = new DeltaTable(storage, 'test-table')
  return { table, storage }
}

/**
 * Create a second DeltaTable instance pointing to the same storage
 * (simulates a second writer/process)
 */
function createSecondWriter(storage: StorageBackend): DeltaTable {
  return new DeltaTable(storage, 'test-table')
}

/**
 * Helper to write a commit directly to storage (bypassing DeltaTable)
 * Simulates external writer modifying the table
 */
async function writeExternalCommit(
  storage: StorageBackend,
  version: number
): Promise<void> {
  const versionStr = version.toString().padStart(20, '0')
  const commitPath = `test-table/_delta_log/${versionStr}.json`
  const commit = [
    `{"add":{"path":"external-part-${version}.parquet","size":1024,"modificationTime":${Date.now()},"dataChange":true}}`,
    `{"commitInfo":{"timestamp":${Date.now()},"operation":"EXTERNAL_WRITE"}}`,
  ].join('\n')
  await storage.write(commitPath, new TextEncoder().encode(commit))
}

// =============================================================================
// CONCURRENCY ERROR CLASS TESTS
// =============================================================================

describe('ConcurrencyError Class', () => {
  describe('Error Structure', () => {
    it('should be a subclass of Error', () => {
      const ErrorClass = requireConcurrencyError()
      const error = new ErrorClass('Version conflict')

      expect(error).toBeInstanceOf(Error)
    })

    it('should have name property set to ConcurrencyError', () => {
      const ErrorClass = requireConcurrencyError()
      const error = new ErrorClass('Version conflict')

      expect(error.name).toBe('ConcurrencyError')
    })

    it('should include expected and actual versions in message', () => {
      const ErrorClass = requireConcurrencyError()
      // The error should accept expectedVersion and actualVersion
      const error = new (ErrorClass as any)({
        expectedVersion: 5,
        actualVersion: 7,
      })

      expect(error.message).toContain('5')
      expect(error.message).toContain('7')
    })

    it('should expose expectedVersion and actualVersion properties', () => {
      const ErrorClass = requireConcurrencyError()
      const error = new (ErrorClass as any)({
        expectedVersion: 5,
        actualVersion: 7,
      })

      expect(error.expectedVersion).toBe(5)
      expect(error.actualVersion).toBe(7)
    })

    it('should have proper stack trace', () => {
      const ErrorClass = requireConcurrencyError()
      const error = new ErrorClass('Version conflict')

      expect(error.stack).toBeDefined()
      expect(error.stack).toContain('ConcurrencyError')
    })
  })
})

// =============================================================================
// VERSION CONFLICT DETECTION TESTS
// =============================================================================

describe('Version Conflict Detection', () => {
  describe('Detecting External Modifications', () => {
    it('should detect when another writer modified the table between read and write', async () => {
      const { table, storage } = createTestTable()

      // Initial write to create the table
      await table.write([{ id: 1, value: 'initial' }])
      expect(await table.version()).toBe(0)

      // Simulate external writer creating version 1
      await writeExternalCommit(storage, 1)

      // Our table still thinks version is 0 (cached)
      // When we try to write, it should detect the conflict
      await expect(table.write([{ id: 2, value: 'conflict' }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })

    it('should throw ConcurrencyError with correct version information', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // External writer creates versions 1, 2, 3
      await writeExternalCommit(storage, 1)
      await writeExternalCommit(storage, 2)
      await writeExternalCommit(storage, 3)

      try {
        await table.write([{ id: 2 }])
        expect.fail('Should have thrown ConcurrencyError')
      } catch (error: unknown) {
        expect(error).toBeInstanceOf(requireConcurrencyError())
        const concurrencyError = error as { expectedVersion: number; actualVersion: number }
        expect(concurrencyError.expectedVersion).toBe(0) // What table thought version was
        expect(concurrencyError.actualVersion).toBe(3)   // What version actually is
      }
    })

    it('should detect conflict even with single version difference', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      await expect(table.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })

    it('should not throw if no external modifications occurred', async () => {
      const { table } = createTestTable()

      await table.write([{ id: 1 }])

      // No external modifications - this should succeed
      await expect(table.write([{ id: 2 }])).resolves.toBeDefined()
    })
  })

  describe('Stale Version Cache Detection', () => {
    it('should detect stale cache when version() was called before external write', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // Cache the version
      const cachedVersion = await table.version()
      expect(cachedVersion).toBe(0)

      // External modification
      await writeExternalCommit(storage, 1)

      // Write should detect the stale cache
      await expect(table.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })

    it('should detect stale cache when snapshot() was called before external write', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // Cache version via snapshot
      await table.snapshot()

      // External modification
      await writeExternalCommit(storage, 1)

      // Write should detect the stale cache
      await expect(table.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })

    it('should work correctly after refreshing version', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      // First write should fail
      await expect(table.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )

      // Refresh the version cache
      await (table as any).refreshVersion()

      // Now write should succeed (version is 1, we write version 2)
      await expect(table.write([{ id: 3 }])).resolves.toBeDefined()
    })
  })
})

// =============================================================================
// VERSION REFRESH MECHANISM TESTS
// =============================================================================

describe('Version Refresh Mechanism', () => {
  describe('Manual Version Refresh', () => {
    it('should expose refreshVersion() method on DeltaTable', async () => {
      const { table } = createTestTable()

      expect(typeof (table as any).refreshVersion).toBe('function')
    })

    it('should update cached version after refresh', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      expect(await table.version()).toBe(0)

      // External modification
      await writeExternalCommit(storage, 1)
      await writeExternalCommit(storage, 2)

      // Cached version is still 0
      // After refresh, should return 2
      await (table as any).refreshVersion()
      expect(await table.version()).toBe(2)
    })

    it('should return the new version from refreshVersion()', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      const newVersion = await (table as any).refreshVersion()
      expect(newVersion).toBe(1)
    })

    it('should clear internal caches on refresh', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // Call snapshot to populate any internal caches
      await table.snapshot()

      // External modification
      await writeExternalCommit(storage, 1)

      // Refresh should clear caches
      await (table as any).refreshVersion()

      // Snapshot should now include the external file
      const snapshot = await table.snapshot()
      expect(snapshot.files.some(f => f.path.includes('external'))).toBe(true)
    })
  })

  describe('Automatic Version Refresh on Error', () => {
    it('should include refreshed version in ConcurrencyError', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)
      await writeExternalCommit(storage, 2)

      try {
        await table.write([{ id: 2 }])
        expect.fail('Should have thrown')
      } catch (error: unknown) {
        // Error should include the actual (refreshed) version
        const concurrencyError = error as { actualVersion: number }
        expect(concurrencyError.actualVersion).toBe(2)
      }
    })
  })
})

// =============================================================================
// OPTIMISTIC CONCURRENCY CONTROL FLOW TESTS
// =============================================================================

describe('Optimistic Concurrency Control Flow', () => {
  describe('Read-Check-Write Cycle', () => {
    it('should read version before preparing write', async () => {
      const { table, storage } = createTestTable()

      // Spy on storage.list to track when version is checked
      const listSpy = vi.spyOn(storage, 'list')

      await table.write([{ id: 1 }])

      // Should have checked _delta_log at least once
      expect(listSpy).toHaveBeenCalledWith(expect.stringContaining('_delta_log'))
    })

    it('should check version again before writing commit file', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // Track read operations during second write
      const readCalls: string[] = []
      const originalList = storage.list.bind(storage)
      storage.list = async (prefix: string) => {
        readCalls.push(prefix)
        return originalList(prefix)
      }

      await table.write([{ id: 2 }])

      // Should check _delta_log to verify version before commit
      const deltaLogChecks = readCalls.filter(p => p.includes('_delta_log'))
      expect(deltaLogChecks.length).toBeGreaterThanOrEqual(1)
    })

    it('should atomically check version and write in same operation', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // Intercept write to inject external modification AFTER version check but BEFORE write
      // This tests that version check and write are atomic
      let writeCount = 0
      const originalWrite = storage.write.bind(storage)
      storage.write = async (path: string, data: Uint8Array) => {
        writeCount++
        if (writeCount === 2 && path.includes('.json')) {
          // Inject external write just before our commit
          await writeExternalCommit({ ...storage, write: originalWrite } as any, 1)
        }
        return originalWrite(path, data)
      }

      // This should fail because external write happened during our write
      // Note: This may succeed if implementation uses conditional writes
      // The key is that the write should either:
      // 1. Fail with ConcurrencyError, or
      // 2. Succeed atomically (if using conditional writes like R2's onlyIf)
      try {
        await table.write([{ id: 2 }])
      } catch (error) {
        expect(error).toBeInstanceOf(requireConcurrencyError())
      }
    })
  })

  describe('Retry After Conflict', () => {
    it('should succeed after refreshing version and retrying', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      // First attempt fails
      await expect(table.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )

      // Refresh and retry
      await (table as any).refreshVersion()
      const result = await table.write([{ id: 3 }])

      expect(result.version).toBe(2)
    })

    it('should fail again if another conflict occurs during retry', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      // First attempt fails
      await expect(table.write([{ id: 2 }])).rejects.toThrow()

      // Refresh
      await (table as any).refreshVersion()

      // Another external write before our retry
      await writeExternalCommit(storage, 2)

      // Retry should also fail
      await expect(table.write([{ id: 3 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })
  })
})

// =============================================================================
// MULTIPLE CONCURRENT WRITERS SCENARIO TESTS
// =============================================================================

describe('Multiple Concurrent Writers', () => {
  describe('Two Writers Scenario', () => {
    it('should allow only one of two concurrent writers to succeed', async () => {
      const storage = new MemoryStorage()

      // Initialize table
      const initTable = new DeltaTable(storage, 'test-table')
      await initTable.write([{ id: 0 }])

      // Two independent writers
      const writer1 = createSecondWriter(storage)
      const writer2 = createSecondWriter(storage)

      // Both read version 0
      await writer1.version()
      await writer2.version()

      // Both try to write concurrently
      const results = await Promise.allSettled([
        writer1.write([{ id: 1, writer: 'writer1' }]),
        writer2.write([{ id: 2, writer: 'writer2' }]),
      ])

      // Exactly one should succeed, one should fail
      const succeeded = results.filter(r => r.status === 'fulfilled')
      const failed = results.filter(r => r.status === 'rejected')

      expect(succeeded.length).toBe(1)
      expect(failed.length).toBe(1)

      // Failed one should be ConcurrencyError
      const failedResult = failed[0] as PromiseRejectedResult
      expect(failedResult.reason).toBeInstanceOf(requireConcurrencyError())
    })

    it('should handle sequential writes from different table instances', async () => {
      const storage = new MemoryStorage()

      const table1 = new DeltaTable(storage, 'test-table')
      await table1.write([{ id: 1 }])

      const table2 = createSecondWriter(storage)

      // table2 reads current version (0)
      await table2.version()

      // table1 writes again (version 1)
      await table1.write([{ id: 2 }])

      // table2 tries to write but has stale version
      await expect(table2.write([{ id: 3 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })

    it('should allow second writer to succeed after first completes and refresh', async () => {
      const storage = new MemoryStorage()

      const table1 = new DeltaTable(storage, 'test-table')
      const table2 = createSecondWriter(storage)

      // table1 writes first
      await table1.write([{ id: 1 }])

      // table2 refreshes to get current version
      await (table2 as any).refreshVersion()

      // table2 can now write successfully
      const result = await table2.write([{ id: 2 }])
      expect(result.version).toBe(1)
    })
  })

  describe('Multiple Writers Race Condition', () => {
    it('should handle many concurrent writers correctly', async () => {
      const storage = new MemoryStorage()

      // Initialize
      const initTable = new DeltaTable(storage, 'test-table')
      await initTable.write([{ id: 0 }])

      // Create 5 writers
      const writers = Array.from({ length: 5 }, () => createSecondWriter(storage))

      // All read version 0
      await Promise.all(writers.map(w => w.version()))

      // All try to write concurrently
      const results = await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1, writer: `writer${i}` }]))
      )

      // Exactly one should succeed
      const succeeded = results.filter(r => r.status === 'fulfilled')
      expect(succeeded.length).toBe(1)

      // Others should fail with ConcurrencyError
      const failed = results.filter(r => r.status === 'rejected')
      expect(failed.length).toBe(4)

      failed.forEach(result => {
        const rejected = result as PromiseRejectedResult
        expect(rejected.reason).toBeInstanceOf(requireConcurrencyError())
      })
    })

    it('should maintain table integrity after concurrent write attempts', async () => {
      const storage = new MemoryStorage()

      const initTable = new DeltaTable(storage, 'test-table')
      await initTable.write([{ id: 0 }])

      const writers = Array.from({ length: 3 }, () => createSecondWriter(storage))
      await Promise.all(writers.map(w => w.version()))

      await Promise.allSettled(
        writers.map((w, i) => w.write([{ id: i + 1 }]))
      )

      // Verify table integrity
      const freshTable = new DeltaTable(storage, 'test-table')
      const snapshot = await freshTable.snapshot()

      // Should have exactly 2 files (initial + one successful write)
      expect(snapshot.files.length).toBe(2)

      // Version should be 1
      expect(snapshot.version).toBe(1)
    })
  })
})

// =============================================================================
// CONFLICT DETECTION FOR DIFFERENT OPERATIONS
// =============================================================================

describe('Conflict Detection for Different Operations', () => {
  describe('Delete Operation Conflicts', () => {
    it('should detect conflict during delete operation', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }, { id: 2 }])
      await writeExternalCommit(storage, 1)

      await expect(table.delete({ id: 1 })).rejects.toThrow(
        requireConcurrencyError()
      )
    })
  })

  describe('Update Operation Conflicts', () => {
    it('should detect conflict during update operation', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1, value: 'initial' }])
      await writeExternalCommit(storage, 1)

      await expect(table.update({ id: 1 }, { value: 'updated' })).rejects.toThrow(
        requireConcurrencyError()
      )
    })
  })

  describe('Commit Operation Conflicts', () => {
    it('should detect conflict during custom commit', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      await expect(table.commit([
        {
          add: {
            path: 'custom-part.parquet',
            size: 100,
            modificationTime: Date.now(),
            dataChange: true,
          },
        },
      ])).rejects.toThrow(requireConcurrencyError())
    })
  })

  describe('Metadata Update Conflicts', () => {
    it('should detect conflict during metadata update', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      await expect(table.updateMetadata({ name: 'new-name' })).rejects.toThrow(
        requireConcurrencyError()
      )
    })
  })
})

// =============================================================================
// VERSION CHECK BEHAVIOR TESTS
// =============================================================================

describe('Version Check Behavior', () => {
  describe('Version Check During Read Operations', () => {
    it('should not throw conflict error for read-only operations', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      // Read operations should not throw, just return potentially stale data
      await expect(table.version()).resolves.toBe(0) // Returns cached version
      await expect(table.query()).resolves.toBeDefined() // Uses cached snapshot
    })

    it('should return fresh data after refresh for read operations', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      // Refresh version cache
      await (table as any).refreshVersion()

      // Now version() should return current version
      expect(await table.version()).toBe(1)
    })
  })

  describe('Checking Specific Version Exists', () => {
    it('should detect if expected version file already exists', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])
      await writeExternalCommit(storage, 1)

      // Table thinks version is 0, will try to write version 1
      // But version 1 already exists
      await expect(table.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })
  })
})

// =============================================================================
// CONDITIONAL WRITE SUPPORT TESTS
// =============================================================================

describe('Conditional Write Support (Future Enhancement)', () => {
  describe('R2 onlyIf Support', () => {
    it.skip('should use conditional write when storage supports it', async () => {
      // This test is for future enhancement
      // R2 supports onlyIf condition to atomically check before write
      // For now, we use version check + write which has a small race window
    })
  })
})

// =============================================================================
// ERROR MESSAGE QUALITY TESTS
// =============================================================================

describe('Error Message Quality', () => {
  it('should provide clear error message for version conflicts', async () => {
    const { table, storage } = createTestTable()

    await table.write([{ id: 1 }])
    await writeExternalCommit(storage, 1)

    try {
      await table.write([{ id: 2 }])
      expect.fail('Should have thrown')
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error)
      expect(message).toMatch(/version|conflict|concurrent|modified/i)
      expect(message).toContain('0') // Expected version
      expect(message).toContain('1') // Actual version
    }
  })

  it('should suggest refresh/retry in error message', async () => {
    const { table, storage } = createTestTable()

    await table.write([{ id: 1 }])
    await writeExternalCommit(storage, 1)

    try {
      await table.write([{ id: 2 }])
      expect.fail('Should have thrown')
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error)
      expect(message).toMatch(/refresh|retry/i)
    }
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  describe('Empty Table Conflicts', () => {
    it('should detect conflict when writing to newly created table concurrently', async () => {
      const storage = new MemoryStorage()

      const writer1 = new DeltaTable(storage, 'test-table')
      const writer2 = new DeltaTable(storage, 'test-table')

      // Both see empty table (version -1)
      expect(await writer1.version()).toBe(-1)
      expect(await writer2.version()).toBe(-1)

      // Writer1 creates the table
      await writer1.write([{ id: 1 }])

      // Writer2 also tries to create (should conflict)
      await expect(writer2.write([{ id: 2 }])).rejects.toThrow(
        requireConcurrencyError()
      )
    })
  })

  describe('Rapid Successive Writes', () => {
    it('should handle rapid writes from same instance correctly', async () => {
      const { table } = createTestTable()

      // Rapid sequential writes should all succeed
      await table.write([{ id: 1 }])
      await table.write([{ id: 2 }])
      await table.write([{ id: 3 }])

      expect(await table.version()).toBe(2)
    })
  })

  describe('Version Gap Detection', () => {
    it('should detect when multiple versions were created externally', async () => {
      const { table, storage } = createTestTable()

      await table.write([{ id: 1 }])

      // Multiple external writes
      await writeExternalCommit(storage, 1)
      await writeExternalCommit(storage, 2)
      await writeExternalCommit(storage, 3)

      try {
        await table.write([{ id: 2 }])
        expect.fail('Should have thrown')
      } catch (error: unknown) {
        const concurrencyError = error as { expectedVersion: number; actualVersion: number }
        expect(concurrencyError.expectedVersion).toBe(0)
        expect(concurrencyError.actualVersion).toBe(3)
      }
    })
  })
})
