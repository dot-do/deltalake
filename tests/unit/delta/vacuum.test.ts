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
})
