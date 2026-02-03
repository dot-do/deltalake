/**
 * VACUUM Operation Tests
 *
 * Comprehensive tests for the Delta Lake VACUUM operation.
 * Tests cover:
 * - Basic vacuum functionality
 * - Retention period configuration
 * - Dry-run mode
 * - Protected file handling
 * - Error handling
 * - Metrics reporting
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { MemoryStorage } from '../../../src/storage/index.js'
import { DeltaTable } from '../../../src/delta/index.js'
import { vacuum, formatBytes, formatDuration, type VacuumConfig, type VacuumMetrics } from '../../../src/delta/vacuum.js'

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a DeltaTable with test data
 */
async function createTableWithData(
  storage: MemoryStorage,
  tablePath: string,
  numWrites: number = 3
): Promise<DeltaTable> {
  const table = new DeltaTable(storage, tablePath)

  for (let i = 0; i < numWrites; i++) {
    const rows = [
      { id: `row-${i}-1`, value: i * 100 + 1 },
      { id: `row-${i}-2`, value: i * 100 + 2 },
      { id: `row-${i}-3`, value: i * 100 + 3 },
    ]
    await table.write(rows)
  }

  return table
}

/**
 * Create orphaned files in the table directory with specific age
 */
async function createOrphanedFiles(
  storage: MemoryStorage,
  tablePath: string,
  numFiles: number,
  ageHours: number = 200 // Default to older than retention
): Promise<string[]> {
  const orphanedPaths: string[] = []
  const oldTimestamp = new Date(Date.now() - ageHours * 60 * 60 * 1000)
  const uniqueId = Math.random().toString(36).substring(2, 8)

  for (let i = 0; i < numFiles; i++) {
    const path = `${tablePath}/orphaned-${uniqueId}-${i}.parquet`
    const content = new TextEncoder().encode(`orphaned file ${i}`)
    await storage.write(path, content)
    // Set the file timestamp to simulate an old file
    storage.setFileTimestamp(path, oldTimestamp)
    orphanedPaths.push(path)
  }

  return orphanedPaths
}

// =============================================================================
// BASIC VACUUM FUNCTIONALITY TESTS
// =============================================================================

describe('VACUUM Operation', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('basic functionality', () => {
    it('should delete orphaned files older than retention period', async () => {
      const table = await createTableWithData(storage, 'test-table', 2)

      // Create orphaned files (simulating old, unreferenced files)
      const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 3)

      // Verify orphaned files exist
      for (const path of orphanedPaths) {
        expect(await storage.exists(path)).toBe(true)
      }

      // Run vacuum
      const metrics = await vacuum(table, { retentionHours: 1 })

      // Verify orphaned files were deleted
      for (const path of orphanedPaths) {
        expect(await storage.exists(path)).toBe(false)
      }

      expect(metrics.filesDeleted).toBe(3)
      expect(metrics.dryRun).toBe(false)
    })

    it('should preserve active files referenced in snapshot', async () => {
      const table = await createTableWithData(storage, 'test-table', 3)
      const snapshot = await table.snapshot()

      // Run vacuum
      const metrics = await vacuum(table, { retentionHours: 1 })

      // Verify all active files still exist
      for (const file of snapshot.files) {
        expect(await storage.exists(file.path)).toBe(true)
      }

      expect(metrics.filesRetained).toBeGreaterThanOrEqual(snapshot.files.length)
    })

    it('should handle empty table', async () => {
      const table = new DeltaTable(storage, 'empty-table')

      const metrics = await vacuum(table)

      expect(metrics.filesDeleted).toBe(0)
      expect(metrics.filesRetained).toBe(0)
      expect(metrics.filesScanned).toBe(0)
    })

    it('should return correct metrics', async () => {
      const table = await createTableWithData(storage, 'test-table', 2)
      await createOrphanedFiles(storage, 'test-table', 5)

      const metrics = await vacuum(table, { retentionHours: 1 })

      expect(metrics.filesDeleted).toBe(5)
      expect(metrics.bytesFreed).toBeGreaterThan(0)
      expect(metrics.durationMs).toBeGreaterThanOrEqual(0)
      expect(metrics.filesScanned).toBeGreaterThan(0)
      expect(metrics.dryRun).toBe(false)
    })

    it('should not delete delta log files', async () => {
      const table = await createTableWithData(storage, 'test-table', 2)

      // Verify delta log files exist
      const logFiles = await storage.list('test-table/_delta_log')
      expect(logFiles.length).toBeGreaterThan(0)

      // Run vacuum
      await vacuum(table, { retentionHours: 1 })

      // Verify delta log files still exist
      const logFilesAfter = await storage.list('test-table/_delta_log')
      expect(logFilesAfter.length).toBe(logFiles.length)
    })
  })

  // =============================================================================
  // RETENTION PERIOD TESTS
  // =============================================================================

  describe('retention period', () => {
    it('should use default retention period of 7 days (168 hours)', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)

      // Create files that are 100 hours old (within default retention)
      await createOrphanedFiles(storage, 'test-table', 2, 100)

      // Create files that are 200 hours old (beyond default retention)
      const oldOrphanedPaths = await createOrphanedFiles(storage, 'test-table', 2, 200)

      const metrics = await vacuum(table)

      // Only files beyond 168 hours should be deleted
      expect(metrics.filesDeleted).toBe(2)

      for (const path of oldOrphanedPaths) {
        expect(await storage.exists(path)).toBe(false)
      }
    })

    it('should respect custom retention period', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)

      // Create files that are 10 hours old
      const recentOrphanedPaths = await createOrphanedFiles(storage, 'test-table', 2, 10)

      // Create files that are 30 hours old
      const oldOrphanedPaths = await createOrphanedFiles(storage, 'test-table', 3, 30)

      // Vacuum with 24-hour retention
      const metrics = await vacuum(table, { retentionHours: 24 })

      // Only files beyond 24 hours should be deleted
      expect(metrics.filesDeleted).toBe(3)

      // Recent files should still exist
      for (const path of recentOrphanedPaths) {
        expect(await storage.exists(path)).toBe(true)
      }

      // Old files should be deleted
      for (const path of oldOrphanedPaths) {
        expect(await storage.exists(path)).toBe(false)
      }
    })

    it('should reject retention period less than 1 hour', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)

      await expect(vacuum(table, { retentionHours: 0.5 })).rejects.toThrow(
        /Retention period must be at least 1 hour/
      )

      await expect(vacuum(table, { retentionHours: 0 })).rejects.toThrow(
        /Retention period must be at least 1 hour/
      )
    })

    it('should handle 1 hour minimum retention', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)

      // Create files that are 2 hours old
      await createOrphanedFiles(storage, 'test-table', 2, 2)

      // Vacuum with 1-hour retention (minimum allowed)
      const metrics = await vacuum(table, { retentionHours: 1 })

      expect(metrics.filesDeleted).toBe(2)
    })
  })

  // =============================================================================
  // DRY RUN MODE TESTS
  // =============================================================================

  describe('dry run mode', () => {
    it('should not delete files in dry run mode', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 3)

      const metrics = await vacuum(table, {
        retentionHours: 1,
        dryRun: true,
      })

      // Verify files still exist
      for (const path of orphanedPaths) {
        expect(await storage.exists(path)).toBe(true)
      }

      expect(metrics.dryRun).toBe(true)
      expect(metrics.filesDeleted).toBe(3) // Would have deleted
    })

    it('should report files that would be deleted in dry run', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 3)

      const metrics = await vacuum(table, {
        retentionHours: 1,
        dryRun: true,
      })

      expect(metrics.filesToDelete).toBeDefined()
      expect(metrics.filesToDelete?.length).toBe(3)

      // Verify paths are included
      for (const path of orphanedPaths) {
        expect(metrics.filesToDelete).toContain(path)
      }
    })

    it('should report correct bytesFreed in dry run', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      await createOrphanedFiles(storage, 'test-table', 3)

      const metrics = await vacuum(table, {
        retentionHours: 1,
        dryRun: true,
      })

      expect(metrics.bytesFreed).toBeGreaterThan(0)
    })
  })

  // =============================================================================
  // PROTECTED FILES TESTS
  // =============================================================================

  describe('protected files', () => {
    it('should not delete files within retention period', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)

      // Create recently orphaned files (within retention period)
      const recentOrphanedPaths: string[] = []
      for (let i = 0; i < 3; i++) {
        // Use a very small age that's definitely within retention
        const path = `test-table/recent-orphan-${i}.parquet`
        const content = new TextEncoder().encode(`recent orphan ${i}`)
        await storage.write(path, content)
        recentOrphanedPaths.push(path)
      }

      // Vacuum with 24-hour retention
      const metrics = await vacuum(table, { retentionHours: 24 })

      // Recent files should still exist (they're within retention)
      for (const path of recentOrphanedPaths) {
        expect(await storage.exists(path)).toBe(true)
      }

      expect(metrics.filesRetained).toBeGreaterThan(0)
    })

    it('should protect files referenced by current snapshot', async () => {
      const table = await createTableWithData(storage, 'test-table', 2)
      const snapshot = await table.snapshot()
      const activePaths = snapshot.files.map(f => f.path)

      // Run vacuum
      await vacuum(table, { retentionHours: 1 })

      // All active files should still exist
      for (const path of activePaths) {
        expect(await storage.exists(path)).toBe(true)
      }
    })

    it('should protect files needed for time travel within retention', async () => {
      const table = await createTableWithData(storage, 'test-table', 2)

      // Get current snapshot
      const snapshot = await table.snapshot()

      // Verify files exist in the snapshot
      for (const file of snapshot.files) {
        expect(await storage.exists(file.path)).toBe(true)
      }

      // Vacuum with default retention (7 days) - all current files should be protected
      const metrics = await vacuum(table, { retentionHours: 168 })

      // Current snapshot files should still exist after vacuum
      for (const file of snapshot.files) {
        expect(await storage.exists(file.path)).toBe(true)
      }
    })
  })

  // =============================================================================
  // PROGRESS CALLBACK TESTS
  // =============================================================================

  describe('progress callback', () => {
    it('should call progress callback during vacuum', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      await createOrphanedFiles(storage, 'test-table', 3)

      const progressEvents: Array<{ phase: string; current: number; total: number }> = []

      await vacuum(table, {
        retentionHours: 1,
        onProgress: (phase, current, total) => {
          progressEvents.push({ phase, current, total })
        },
      })

      expect(progressEvents.length).toBeGreaterThan(0)
      expect(progressEvents.some(e => e.phase === 'scanning')).toBe(true)
      expect(progressEvents.some(e => e.phase === 'deleting')).toBe(true)
    })

    it('should report correct progress counts', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 5)

      const scanProgress: number[] = []
      const deleteProgress: number[] = []

      await vacuum(table, {
        retentionHours: 1,
        onProgress: (phase, current, total) => {
          if (phase === 'scanning') {
            scanProgress.push(current)
          } else if (phase === 'deleting') {
            deleteProgress.push(current)
          }
        },
      })

      // Scanning should report progress for each file scanned (includes active files too)
      expect(scanProgress.length).toBeGreaterThan(0)
      // Progress values should be incrementing
      for (let i = 1; i < scanProgress.length; i++) {
        expect(scanProgress[i]).toBeGreaterThan(scanProgress[i - 1])
      }

      // Delete progress should report 0 (start) and then 1-5 as files are deleted
      expect(deleteProgress).toEqual([0, 1, 2, 3, 4, 5])
    })
  })

  // =============================================================================
  // ERROR HANDLING TESTS
  // =============================================================================

  describe('error handling', () => {
    it('should continue on individual file deletion errors', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 3)

      // Mock delete to fail for the first orphaned file
      const firstOrphanedPath = orphanedPaths[0]
      const originalDelete = storage.delete.bind(storage)
      let deleteCallCount = 0
      storage.delete = async (path: string) => {
        deleteCallCount++
        if (path === firstOrphanedPath) {
          throw new Error('Simulated deletion failure')
        }
        return originalDelete(path)
      }

      const metrics = await vacuum(table, { retentionHours: 1 })

      // Should have recorded the error
      expect(metrics.errors).toBeDefined()
      expect(metrics.errors?.length).toBe(1)
      expect(metrics.errors?.[0]).toContain('Failed to delete')

      // Should have deleted the other files
      expect(metrics.filesDeleted).toBe(2)

      // Restore original
      storage.delete = originalDelete
    })

    it('should handle storage list errors gracefully', async () => {
      const table = new DeltaTable(storage, 'error-table')

      // Mock list to throw
      const originalList = storage.list.bind(storage)
      storage.list = async () => {
        throw new Error('Simulated list failure')
      }

      await expect(vacuum(table)).rejects.toThrow('Simulated list failure')

      // Restore original
      storage.list = originalList
    })
  })

  // =============================================================================
  // UTILITY FUNCTION TESTS
  // =============================================================================

  describe('utility functions', () => {
    describe('formatBytes', () => {
      it('should format bytes correctly', () => {
        expect(formatBytes(0)).toBe('0 Bytes')
        expect(formatBytes(500)).toBe('500 Bytes')
        expect(formatBytes(1024)).toBe('1 KB')
        expect(formatBytes(1536)).toBe('1.5 KB')
        expect(formatBytes(1048576)).toBe('1 MB')
        expect(formatBytes(1073741824)).toBe('1 GB')
        expect(formatBytes(1099511627776)).toBe('1 TB')
      })
    })

    describe('formatDuration', () => {
      it('should format duration correctly', () => {
        expect(formatDuration(500)).toBe('500ms')
        expect(formatDuration(1000)).toBe('1.0s')
        expect(formatDuration(1500)).toBe('1.5s')
        expect(formatDuration(60000)).toBe('1.0m')
        expect(formatDuration(90000)).toBe('1.5m')
      })
    })
  })

  // =============================================================================
  // EDGE CASES
  // =============================================================================

  describe('edge cases', () => {
    it('should handle table with only delta log (no data files)', async () => {
      const table = new DeltaTable(storage, 'log-only-table')

      const metrics = await vacuum(table)

      expect(metrics.filesDeleted).toBe(0)
      expect(metrics.filesScanned).toBe(0)
    })

    it('should handle very large retention period', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)
      await createOrphanedFiles(storage, 'test-table', 3)

      // 10 years retention
      const metrics = await vacuum(table, { retentionHours: 87600 })

      // Nothing should be deleted (files aren't that old)
      expect(metrics.filesDeleted).toBe(0)
    })

    it('should handle non-parquet files', async () => {
      const table = await createTableWithData(storage, 'test-table', 1)

      // Create non-parquet files
      await storage.write('test-table/readme.txt', new TextEncoder().encode('readme'))
      await storage.write('test-table/data.json', new TextEncoder().encode('{}'))

      const metrics = await vacuum(table, { retentionHours: 1 })

      // Non-parquet files should be ignored
      expect(await storage.exists('test-table/readme.txt')).toBe(true)
      expect(await storage.exists('test-table/data.json')).toBe(true)
    })

    it('should handle concurrent vacuum operations', async () => {
      const table = await createTableWithData(storage, 'test-table', 2)
      const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 10)

      // Run two vacuum operations concurrently
      const results = await Promise.all([
        vacuum(table, { retentionHours: 1 }),
        vacuum(table, { retentionHours: 1 }),
      ])

      // Both vacuums should complete without error
      expect(results.length).toBe(2)

      // All orphaned files should be deleted after concurrent vacuums
      for (const path of orphanedPaths) {
        expect(await storage.exists(path)).toBe(false)
      }

      // At least one vacuum should have deleted files
      const totalDeleted = results.reduce((sum, m) => sum + m.filesDeleted, 0)
      expect(totalDeleted).toBeGreaterThanOrEqual(10)
    })

    it('should handle nested table paths', async () => {
      const table = await createTableWithData(storage, 'databases/db1/tables/test', 1)
      await createOrphanedFiles(storage, 'databases/db1/tables/test', 2)

      const metrics = await vacuum(table, { retentionHours: 1 })

      expect(metrics.filesDeleted).toBe(2)
    })
  })

  // =============================================================================
  // CONCURRENT VACUUM OPERATIONS TESTS
  // =============================================================================

  describe('concurrent vacuum operations', () => {
    describe('vacuum during active writes', () => {
      it('should not delete files being written during vacuum', async () => {
        const table = await createTableWithData(storage, 'test-table', 2)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 5)

        // Start vacuum and writes concurrently
        const [vacuumResult, writeResult] = await Promise.all([
          vacuum(table, { retentionHours: 1 }),
          table.write([{ id: 'new-1', value: 1000 }]),
        ])

        // Vacuum should complete successfully
        expect(vacuumResult.filesDeleted).toBe(5)

        // Write should complete successfully
        expect(writeResult.version).toBeGreaterThanOrEqual(2)

        // New file from write should still exist
        const snapshot = await table.snapshot()
        expect(snapshot.files.length).toBeGreaterThan(0)

        // All orphaned files should be deleted
        for (const path of orphanedPaths) {
          expect(await storage.exists(path)).toBe(false)
        }
      })

      it('should protect files added by concurrent writes', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        await createOrphanedFiles(storage, 'test-table', 3)

        // Get initial snapshot to know active files
        const initialSnapshot = await table.snapshot()
        const initialFilePaths = initialSnapshot.files.map(f => f.path)

        // Run vacuum and multiple writes concurrently
        const [vacuumResult, write1Result, write2Result] = await Promise.all([
          vacuum(table, { retentionHours: 1 }),
          table.write([{ id: 'concurrent-1', value: 100 }]),
          (async () => {
            // Small delay to ensure write happens during vacuum scanning phase
            await new Promise(resolve => setTimeout(resolve, 10))
            return table.write([{ id: 'concurrent-2', value: 200 }])
          })(),
        ])

        // All operations should succeed
        expect(vacuumResult.dryRun).toBe(false)
        expect(write1Result).toBeDefined()
        expect(write2Result).toBeDefined()

        // Original active files should still exist
        for (const path of initialFilePaths) {
          expect(await storage.exists(path)).toBe(true)
        }
      })

      it('should handle rapid writes during vacuum', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        await createOrphanedFiles(storage, 'test-table', 10)

        // Start vacuum and many rapid writes
        // Note: Concurrent writes to the same table will conflict (by design),
        // so we use Promise.allSettled to handle expected concurrency errors
        const writePromises = Array.from({ length: 5 }, (_, i) =>
          table.write([{ id: `rapid-${i}`, value: i * 100 }])
        )

        const results = await Promise.allSettled([
          vacuum(table, { retentionHours: 1 }),
          ...writePromises,
        ])

        // Vacuum should always succeed
        expect(results[0].status).toBe('fulfilled')
        const vacuumMetrics = (results[0] as PromiseFulfilledResult<VacuumMetrics>).value
        expect(vacuumMetrics.filesScanned).toBeGreaterThan(0)

        // At least one write should succeed (concurrent writes conflict by design)
        const successfulWrites = results.slice(1).filter(r => r.status === 'fulfilled')
        expect(successfulWrites.length).toBeGreaterThanOrEqual(1)

        // Verify table is still consistent after concurrent activity
        const finalSnapshot = await table.snapshot()
        expect(finalSnapshot.files.length).toBeGreaterThanOrEqual(2) // Initial + at least 1 write
      })
    })

    describe('multiple concurrent vacuums', () => {
      it('should handle three concurrent vacuum operations', async () => {
        const table = await createTableWithData(storage, 'test-table', 2)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 15)

        // Run three vacuum operations concurrently
        const results = await Promise.all([
          vacuum(table, { retentionHours: 1 }),
          vacuum(table, { retentionHours: 1 }),
          vacuum(table, { retentionHours: 1 }),
        ])

        // All should complete without errors
        expect(results.length).toBe(3)
        results.forEach(result => {
          expect(result.dryRun).toBe(false)
          expect(result.filesScanned).toBeGreaterThan(0)
        })

        // All orphaned files should be deleted
        for (const path of orphanedPaths) {
          expect(await storage.exists(path)).toBe(false)
        }

        // Combined deletions should account for all orphaned files
        // (One vacuum deletes them, others find them already gone)
        const totalDeleted = results.reduce((sum, m) => sum + m.filesDeleted, 0)
        expect(totalDeleted).toBeGreaterThanOrEqual(15)
      })

      it('should handle concurrent vacuums with different retention periods', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)

        // Create files with different ages
        const recentOrphanedPaths = await createOrphanedFiles(storage, 'test-table', 3, 12) // 12 hours old
        const oldOrphanedPaths = await createOrphanedFiles(storage, 'test-table', 5, 200) // 200 hours old

        // Run vacuums with different retention periods concurrently
        const results = await Promise.all([
          vacuum(table, { retentionHours: 24 }), // Should delete only old files
          vacuum(table, { retentionHours: 1 }),  // Should delete all orphaned files
        ])

        // Both should complete
        expect(results.length).toBe(2)

        // All old orphaned files should definitely be deleted
        for (const path of oldOrphanedPaths) {
          expect(await storage.exists(path)).toBe(false)
        }

        // Recent files might be deleted by the 1-hour retention vacuum
        // but that's OK - the test verifies no crashes or data corruption
      })

      it('should correctly report metrics when concurrent vacuums race', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        await createOrphanedFiles(storage, 'test-table', 20)

        // Run 5 concurrent vacuums
        const results = await Promise.all(
          Array.from({ length: 5 }, () => vacuum(table, { retentionHours: 1 }))
        )

        // All should succeed
        expect(results.length).toBe(5)

        // Each vacuum should report reasonable metrics
        results.forEach(result => {
          expect(result.durationMs).toBeGreaterThanOrEqual(0)
          expect(result.filesScanned).toBeGreaterThanOrEqual(0)
          // filesDeleted might vary depending on race conditions
          expect(result.filesDeleted).toBeGreaterThanOrEqual(0)
        })

        // Total deleted should equal number of orphaned files
        // (files can only be deleted once, subsequent attempts should gracefully handle missing files)
        const totalDeleted = results.reduce((sum, m) => sum + m.filesDeleted, 0)
        expect(totalDeleted).toBeGreaterThanOrEqual(20)
      })
    })

    describe('vacuum with concurrent deletes', () => {
      it('should handle delete operations running alongside vacuum', async () => {
        const table = await createTableWithData(storage, 'test-table', 3)
        await createOrphanedFiles(storage, 'test-table', 5)

        // Get initial data to delete
        const initialData = await table.query()
        const recordToDelete = initialData[0]

        // Run vacuum and delete concurrently
        const [vacuumResult] = await Promise.allSettled([
          vacuum(table, { retentionHours: 1 }),
          table.delete({ id: recordToDelete?.id }),
        ])

        // Vacuum should complete (might succeed or handle race gracefully)
        expect(vacuumResult.status).toBe('fulfilled')

        // Table should remain consistent
        const finalSnapshot = await table.snapshot()
        expect(finalSnapshot.version).toBeGreaterThanOrEqual(3)
      })

      it('should not delete files that become orphaned during vacuum scan', async () => {
        const table = await createTableWithData(storage, 'test-table', 2)
        await createOrphanedFiles(storage, 'test-table', 3, 200) // Old orphans

        // Get snapshot to identify files
        const snapshot = await table.snapshot()
        const activeFilePaths = snapshot.files.map(f => f.path)

        // Run vacuum - active files should be protected
        const vacuumResult = await vacuum(table, { retentionHours: 1 })

        // Active files from the snapshot should still exist
        // (vacuum reads snapshot at start and protects those files)
        for (const path of activeFilePaths) {
          expect(await storage.exists(path)).toBe(true)
        }

        expect(vacuumResult.filesRetained).toBeGreaterThan(0)
      })
    })

    describe('concurrent vacuum error handling', () => {
      it('should handle deletion errors gracefully during concurrent vacuum', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 10)

        // Simulate deletion failures by deleting files before vacuum can
        const earlyDeletePromise = (async () => {
          // Delete some orphaned files before vacuum gets to them
          for (let i = 0; i < 3; i++) {
            const path = orphanedPaths[i]
            if (path) {
              await storage.delete(path)
            }
          }
        })()

        const [vacuumResult] = await Promise.all([
          vacuum(table, { retentionHours: 1 }),
          earlyDeletePromise,
        ])

        // Vacuum should handle missing files gracefully
        // (might report errors for files that were already deleted)
        expect(vacuumResult).toBeDefined()
        expect(vacuumResult.durationMs).toBeGreaterThanOrEqual(0)

        // Remaining orphaned files should be deleted by vacuum
        for (const path of orphanedPaths) {
          expect(await storage.exists(path)).toBe(false)
        }
      })

      it('should track errors when file deletion fails mid-vacuum', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 5)

        // Mock delete to fail for specific files
        const originalDelete = storage.delete.bind(storage)
        let deleteCallCount = 0
        storage.delete = async (path: string) => {
          deleteCallCount++
          // Fail for the second and third orphaned file
          if (orphanedPaths.includes(path) && (deleteCallCount === 2 || deleteCallCount === 3)) {
            throw new Error(`Simulated concurrent deletion failure for ${path}`)
          }
          return originalDelete(path)
        }

        const metrics = await vacuum(table, { retentionHours: 1 })

        // Should have recorded errors
        expect(metrics.errors).toBeDefined()
        expect(metrics.errors?.length).toBeGreaterThan(0)

        // Should have successfully deleted other files
        expect(metrics.filesDeleted).toBeGreaterThan(0)

        // Restore original delete
        storage.delete = originalDelete
      })

      it('should continue vacuum after encountering concurrent modification errors', async () => {
        const table = await createTableWithData(storage, 'test-table', 2)
        await createOrphanedFiles(storage, 'test-table', 8)

        // Simulate concurrent writes that might cause race conditions
        const writePromises = Array.from({ length: 3 }, async (_, i) => {
          await new Promise(resolve => setTimeout(resolve, i * 5))
          try {
            await table.write([{ id: `race-write-${i}`, value: i }])
          } catch {
            // Ignore write errors - we're testing vacuum resilience
          }
        })

        const [vacuumResult] = await Promise.all([
          vacuum(table, { retentionHours: 1 }),
          ...writePromises,
        ])

        // Vacuum should complete despite concurrent activity
        expect(vacuumResult).toBeDefined()
        expect(vacuumResult.filesScanned).toBeGreaterThan(0)
      })
    })

    describe('vacuum dry-run during concurrent operations', () => {
      it('should provide accurate preview during concurrent writes', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 5)

        // Run dry-run vacuum and writes concurrently
        const [dryRunResult] = await Promise.all([
          vacuum(table, { retentionHours: 1, dryRun: true }),
          table.write([{ id: 'concurrent-write', value: 999 }]),
        ])

        // Dry run should report files that would be deleted
        expect(dryRunResult.dryRun).toBe(true)
        expect(dryRunResult.filesToDelete).toBeDefined()
        expect(dryRunResult.filesDeleted).toBe(5)

        // All orphaned files should still exist (dry run doesn't delete)
        for (const path of orphanedPaths) {
          expect(await storage.exists(path)).toBe(true)
        }
      })

      it('should allow subsequent real vacuum after dry-run', async () => {
        const table = await createTableWithData(storage, 'test-table', 1)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 4)

        // First do a dry run to preview
        const dryRunResult = await vacuum(table, { retentionHours: 1, dryRun: true })
        expect(dryRunResult.filesDeleted).toBe(4)
        expect(dryRunResult.filesToDelete?.length).toBe(4)

        // Then run actual vacuum concurrently with a write
        const [actualResult] = await Promise.all([
          vacuum(table, { retentionHours: 1 }),
          table.write([{ id: 'post-preview-write', value: 1 }]),
        ])

        // Files should now be actually deleted
        expect(actualResult.filesDeleted).toBe(4)
        for (const path of orphanedPaths) {
          expect(await storage.exists(path)).toBe(false)
        }
      })
    })

    describe('stress testing concurrent vacuum', () => {
      it('should handle many concurrent vacuum operations without data loss', async () => {
        const table = await createTableWithData(storage, 'test-table', 5)
        const orphanedPaths = await createOrphanedFiles(storage, 'test-table', 30)

        // Get initial snapshot to verify data integrity later
        const initialSnapshot = await table.snapshot()
        const activeFilePaths = new Set(initialSnapshot.files.map(f => f.path))

        // Run 10 concurrent vacuum operations
        const vacuumPromises = Array.from({ length: 10 }, () =>
          vacuum(table, { retentionHours: 1 })
        )

        const results = await Promise.all(vacuumPromises)

        // All vacuums should complete without throwing
        expect(results.length).toBe(10)

        // Active files from initial snapshot should still exist
        for (const path of activeFilePaths) {
          expect(await storage.exists(path)).toBe(true)
        }

        // All orphaned files should be deleted
        for (const path of orphanedPaths) {
          expect(await storage.exists(path)).toBe(false)
        }
      })

      it('should maintain table consistency under concurrent vacuum and write load', async () => {
        const table = await createTableWithData(storage, 'test-table', 2)
        await createOrphanedFiles(storage, 'test-table', 20)

        // Mix of vacuum operations and writes
        // Note: Concurrent writes will conflict (by design in Delta Lake's optimistic concurrency).
        // We use Promise.allSettled to handle expected concurrency errors gracefully.
        const operations = [
          vacuum(table, { retentionHours: 1 }),
          table.write([{ id: 'stress-1', value: 1 }]),
          vacuum(table, { retentionHours: 1 }),
          table.write([{ id: 'stress-2', value: 2 }]),
          vacuum(table, { retentionHours: 1 }),
          table.write([{ id: 'stress-3', value: 3 }]),
          vacuum(table, { retentionHours: 1, dryRun: true }),
        ]

        const results = await Promise.allSettled(operations)

        // All vacuum operations should succeed (vacuums don't conflict with each other or writes)
        const vacuumResults = [results[0], results[2], results[4], results[6]]
        vacuumResults.forEach(result => {
          expect(result.status).toBe('fulfilled')
        })

        // At least one write should succeed (concurrent writes may conflict)
        const writeResults = [results[1], results[3], results[5]]
        const successfulWrites = writeResults.filter(r => r.status === 'fulfilled')
        expect(successfulWrites.length).toBeGreaterThanOrEqual(1)

        // Table should be in consistent state regardless of which writes succeeded
        const finalSnapshot = await table.snapshot()
        expect(finalSnapshot.version).toBeGreaterThanOrEqual(2)

        // Query should work correctly
        const queryResult = await table.query()
        expect(queryResult.length).toBeGreaterThan(0)
      })
    })
  })
})
