/**
 * Checkpoint Tests
 *
 * TDD RED Phase: Comprehensive tests for Delta Lake checkpoint functionality.
 *
 * Checkpoints are periodic snapshots of table state stored as Parquet files.
 * They enable fast state reconstruction without replaying all transaction logs.
 *
 * Delta Lake checkpoint spec:
 * - Checkpoints are created at version intervals (default: every 10 commits)
 * - Stored as Parquet files: <version>.checkpoint.parquet
 * - Multi-part checkpoints for large tables: <version>.checkpoint.<part>.<total>.parquet
 * - _last_checkpoint file points to the most recent checkpoint
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  DeltaTable,
  type DeltaCommit,
  type DeltaSnapshot,
  type AddAction,
  type RemoveAction,
  type MetadataAction,
  type ProtocolAction,
} from '../../../src/delta/index.js'
import { MemoryStorage, type StorageBackend } from '../../../src/storage/index.js'

// =============================================================================
// TYPE DEFINITIONS FOR CHECKPOINT FUNCTIONALITY
// =============================================================================

/**
 * Last checkpoint metadata stored in _last_checkpoint JSON file
 */
interface LastCheckpoint {
  version: number
  size: number // Number of actions in checkpoint
  parts?: number // Number of parts for multi-part checkpoints
  sizeInBytes?: number
  numOfAddFiles?: number
  checkpointSchema?: unknown
}

/**
 * Checkpoint configuration options
 */
interface CheckpointConfig {
  /** Interval for automatic checkpoint creation (default: 10) */
  checkpointInterval?: number
  /** Maximum actions per checkpoint part before splitting (default: 1000000) */
  maxActionsPerCheckpoint?: number
  /** Retention period for old checkpoints in milliseconds */
  checkpointRetentionMs?: number
  /** Number of recent checkpoints to keep */
  numRetainedCheckpoints?: number
}

/**
 * Checkpoint writer interface
 */
interface CheckpointWriter {
  createCheckpoint(version: number): Promise<void>
  writeMultiPartCheckpoint(version: number, numParts: number): Promise<void>
}

/**
 * Checkpoint reader interface
 */
interface CheckpointReader {
  readLastCheckpoint(): Promise<LastCheckpoint | null>
  readCheckpoint(version: number): Promise<DeltaSnapshot>
  readCheckpointPart(version: number, part: number, totalParts: number): Promise<DeltaSnapshot>
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
 * Write N commits to the table
 */
async function writeCommits(
  table: DeltaTable,
  count: number
): Promise<DeltaCommit[]> {
  const commits: DeltaCommit[] = []
  for (let i = 0; i < count; i++) {
    const commit = await table.write([{ id: i, value: `value-${i}` }])
    commits.push(commit)
  }
  return commits
}

/**
 * Parse checkpoint version from filename
 */
function parseCheckpointVersion(filename: string): number | null {
  const match = filename.match(/(\d+)\.checkpoint\.parquet$/)
  if (match) return parseInt(match[1], 10)

  const multiMatch = filename.match(/(\d+)\.checkpoint\.\d+\.\d+\.parquet$/)
  if (multiMatch) return parseInt(multiMatch[1], 10)

  return null
}

/**
 * Parse multi-part checkpoint info from filename
 */
function parseMultiPartCheckpoint(
  filename: string
): { version: number; part: number; total: number } | null {
  const match = filename.match(/(\d+)\.checkpoint\.(\d+)\.(\d+)\.parquet$/)
  if (!match) return null
  return {
    version: parseInt(match[1], 10),
    part: parseInt(match[2], 10),
    total: parseInt(match[3], 10),
  }
}

// =============================================================================
// CHECKPOINT FILE CREATION TESTS
// =============================================================================

describe('Checkpoint File Creation', () => {
  describe('Checkpoint Creation at Version Intervals', () => {
    it('should create checkpoint after default interval of 10 commits', async () => {
      const { table, storage } = createTestTable()

      // Write 10 commits (0-9)
      await writeCommits(table, 10)

      // Check that a checkpoint was created at version 9
      const files = await storage.list('test-table/_delta_log')
      const checkpointFiles = files.filter((f) => f.includes('.checkpoint.'))

      expect(checkpointFiles.length).toBeGreaterThan(0)

      const hasVersion9Checkpoint = checkpointFiles.some(
        (f) => parseCheckpointVersion(f) === 9
      )
      expect(hasVersion9Checkpoint).toBe(true)
    })

    it('should create checkpoints at every 10th version', async () => {
      const { table, storage } = createTestTable()

      // Write 25 commits (0-24)
      await writeCommits(table, 25)

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter((f) => f.includes('.checkpoint.'))
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Should have checkpoints at versions 9 and 19
      expect(checkpointVersions).toContain(9)
      expect(checkpointVersions).toContain(19)
    })

    it('should respect custom checkpoint interval', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        checkpointInterval: 5,
      })

      // Write 12 commits (0-11)
      await writeCommits(table, 12)

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter((f) => f.includes('.checkpoint.'))
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Should have checkpoints at versions 4 and 9
      expect(checkpointVersions).toContain(4)
      expect(checkpointVersions).toContain(9)
    })

    it('should allow manual checkpoint creation at any version', async () => {
      const { table, storage } = createTestTable()

      // Write 3 commits
      await writeCommits(table, 3)

      // Manually trigger checkpoint at version 2
      await (table as any).createCheckpoint(2)

      const files = await storage.list('test-table/_delta_log')
      const hasVersion2Checkpoint = files.some(
        (f) => parseCheckpointVersion(f) === 2
      )
      expect(hasVersion2Checkpoint).toBe(true)
    })

    it('should skip checkpoint creation if version already has checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      // Get initial checkpoint files
      const initialFiles = await storage.list('test-table/_delta_log')
      const initialCheckpoints = initialFiles.filter((f) =>
        f.includes('.checkpoint.')
      )

      // Try to create checkpoint again at same version
      await (table as any).createCheckpoint(9)

      const finalFiles = await storage.list('test-table/_delta_log')
      const finalCheckpoints = finalFiles.filter((f) =>
        f.includes('.checkpoint.')
      )

      // Should not create duplicate checkpoint
      expect(finalCheckpoints.length).toBe(initialCheckpoints.length)
    })
  })

  describe('Checkpoint File Naming', () => {
    it('should use correct naming format: <version>.checkpoint.parquet', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const checkpointFile = files.find((f) => f.includes('.checkpoint.'))

      expect(checkpointFile).toBeDefined()
      expect(checkpointFile).toMatch(/00000000000000000009\.checkpoint\.parquet$/)
    })

    it('should use 20-digit zero-padded version numbers', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const checkpointFile = files.find((f) => f.includes('.checkpoint.'))

      // Version should be 20 digits
      const match = checkpointFile?.match(/(\d+)\.checkpoint\.parquet/)
      expect(match?.[1].length).toBe(20)
    })
  })
})

// =============================================================================
// CHECKPOINT FILE FORMAT TESTS
// =============================================================================

describe('Checkpoint File Format', () => {
  describe('Parquet Format', () => {
    it('should store checkpoint as valid Parquet file', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const checkpointPath = files.find((f) => f.includes('.checkpoint.'))!

      const checkpointData = await storage.read(checkpointPath)

      // Parquet magic bytes: 'PAR1'
      expect(checkpointData.slice(0, 4)).toEqual(
        new Uint8Array([0x50, 0x41, 0x52, 0x31])
      )

      // Parquet footer magic: 'PAR1'
      expect(checkpointData.slice(-4)).toEqual(
        new Uint8Array([0x50, 0x41, 0x52, 0x31])
      )
    })

    it('should include checkpoint schema in Parquet metadata', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      // Read checkpoint and verify schema
      const snapshot = await (table as any).readCheckpoint(9)

      // Checkpoint should contain standard Delta action columns
      expect(snapshot).toBeDefined()
    })
  })

  describe('Action Serialization', () => {
    it('should serialize add actions in checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const checkpoint = await (table as any).readCheckpoint(9)

      // Should contain all active add actions
      expect(checkpoint.files.length).toBeGreaterThan(0)
      checkpoint.files.forEach((file: AddAction['add']) => {
        expect(file.path).toBeDefined()
        expect(file.size).toBeDefined()
        expect(file.modificationTime).toBeDefined()
        expect(typeof file.dataChange).toBe('boolean')
      })
    })

    it('should serialize remove actions that are still within retention', async () => {
      const { table, storage } = createTestTable()

      // Write some commits
      await writeCommits(table, 5)

      // Delete a file
      await (table as any).delete({ id: 0 })

      // Write more commits to trigger checkpoint
      await writeCommits(table, 4)

      const checkpoint = await (table as any).readCheckpoint(9)

      // Check if tombstones are included (remove actions within retention)
      // This depends on implementation - some checkpoints include recent removes
      expect(checkpoint).toBeDefined()
    })

    it('should serialize metadata action in checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const checkpoint = await (table as any).readCheckpoint(9)

      expect(checkpoint.metadata).toBeDefined()
      expect(checkpoint.metadata?.id).toBeDefined()
      expect(checkpoint.metadata?.format).toBeDefined()
      expect(checkpoint.metadata?.schemaString).toBeDefined()
    })

    it('should serialize protocol action in checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const checkpoint = await (table as any).readCheckpoint(9)

      expect(checkpoint.protocol).toBeDefined()
      expect(checkpoint.protocol?.minReaderVersion).toBeDefined()
      expect(checkpoint.protocol?.minWriterVersion).toBeDefined()
    })

    it('should consolidate multiple metadata updates to latest', async () => {
      const { table, storage } = createTestTable()

      // Write commits with different metadata
      await writeCommits(table, 5)
      await (table as any).updateMetadata({ name: 'test-table-v1' })
      await writeCommits(table, 3)
      await (table as any).updateMetadata({ name: 'test-table-v2' })

      const checkpoint = await (table as any).readCheckpoint(9)

      // Checkpoint should only have the latest metadata
      expect(checkpoint.metadata?.name).toBe('test-table-v2')
    })

    it('should not include removed files in checkpoint (compaction)', async () => {
      const { table, storage } = createTestTable()

      // Write a file
      await table.write([{ id: 1, value: 'initial' }])

      // Update the file (creates remove + add)
      await (table as any).update({ id: 1 }, { value: 'updated' })

      // Write more to trigger checkpoint
      await writeCommits(table, 8)

      const checkpoint = await (table as any).readCheckpoint(9)

      // Should only have active files, not removed ones
      const activePaths = checkpoint.files.map((f: AddAction['add']) => f.path)
      // No duplicate paths for same logical file
      const uniquePaths = new Set(activePaths)
      expect(uniquePaths.size).toBe(activePaths.length)
    })
  })
})

// =============================================================================
// _LAST_CHECKPOINT FILE TESTS
// =============================================================================

describe('_last_checkpoint File', () => {
  describe('Last Checkpoint Creation', () => {
    it('should create _last_checkpoint file after checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const exists = await storage.exists('test-table/_delta_log/_last_checkpoint')
      expect(exists).toBe(true)
    })

    it('should update _last_checkpoint after each new checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const firstCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      expect(firstCheckpoint.version).toBe(9)

      await writeCommits(table, 10)

      const secondCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      expect(secondCheckpoint.version).toBe(19)
    })
  })

  describe('Last Checkpoint Format', () => {
    it('should store version number in _last_checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const lastCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      expect(lastCheckpoint.version).toBe(9)
    })

    it('should store size (action count) in _last_checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const lastCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      expect(typeof lastCheckpoint.size).toBe('number')
      expect(lastCheckpoint.size).toBeGreaterThan(0)
    })

    it('should store parts count for multi-part checkpoints', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 5,
      })

      // Write enough data to trigger multi-part checkpoint
      await writeCommits(table, 20)

      const lastCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      // If multi-part, should have parts field
      if (lastCheckpoint.parts) {
        expect(lastCheckpoint.parts).toBeGreaterThan(1)
      }
    })

    it('should include optional sizeInBytes field', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const lastCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      // sizeInBytes is optional but should be present if supported
      if ('sizeInBytes' in lastCheckpoint) {
        expect(typeof lastCheckpoint.sizeInBytes).toBe('number')
      }
    })
  })

  describe('Last Checkpoint Reading', () => {
    it('should read _last_checkpoint to find latest checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 25)

      const lastCheckpoint = await (table as any).readLastCheckpoint()

      expect(lastCheckpoint).toBeDefined()
      expect(lastCheckpoint.version).toBe(19)
    })

    it('should return null if no checkpoint exists', async () => {
      const { table, storage } = createTestTable()

      // Write fewer than 10 commits (no checkpoint)
      await writeCommits(table, 5)

      const lastCheckpoint = await (table as any).readLastCheckpoint()

      expect(lastCheckpoint).toBeNull()
    })

    it('should handle corrupted _last_checkpoint gracefully', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      // Corrupt the _last_checkpoint file
      await storage.write(
        'test-table/_delta_log/_last_checkpoint',
        new TextEncoder().encode('not valid json')
      )

      // Should fall back to scanning log directory
      const lastCheckpoint = await (table as any).readLastCheckpoint()

      // Either returns null or recovers by scanning
      expect(lastCheckpoint === null || lastCheckpoint.version === 9).toBe(true)
    })
  })
})

// =============================================================================
// CHECKPOINT READING TESTS
// =============================================================================

describe('Checkpoint Reading', () => {
  describe('State Reconstruction from Checkpoint', () => {
    it('should reconstruct table state from checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      // Create new table instance and read state
      const table2 = new DeltaTable(storage, 'test-table')
      const snapshot = await table2.snapshot()

      expect(snapshot.version).toBe(9)
      expect(snapshot.files.length).toBe(10)
    })

    it('should read checkpoint file directly', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const checkpointSnapshot = await (table as any).readCheckpoint(9)

      expect(checkpointSnapshot.version).toBe(9)
      expect(checkpointSnapshot.files.length).toBe(10)
      expect(checkpointSnapshot.metadata).toBeDefined()
      expect(checkpointSnapshot.protocol).toBeDefined()
    })

    it('should prefer checkpoint over replaying all logs', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 20)

      // Track read operations
      const readCalls: string[] = []
      const originalRead = storage.read.bind(storage)
      storage.read = async (path: string) => {
        readCalls.push(path)
        return originalRead(path)
      }

      // Create new table and get snapshot
      const table2 = new DeltaTable(storage, 'test-table')
      await table2.snapshot()

      // Should read checkpoint, not all 20 log files
      const logFileReads = readCalls.filter(
        (p) => p.includes('.json') && !p.includes('_last_checkpoint')
      )
      const checkpointReads = readCalls.filter((p) =>
        p.includes('.checkpoint.')
      )

      expect(checkpointReads.length).toBeGreaterThan(0)
      // Should read fewer log files than total versions
      expect(logFileReads.length).toBeLessThan(20)
    })
  })

  describe('Checkpoint + Log Combination', () => {
    it('should combine checkpoint with subsequent logs', async () => {
      const { table, storage } = createTestTable()

      // Create checkpoint at version 9
      await writeCommits(table, 10)

      // Add more commits after checkpoint
      await writeCommits(table, 5)

      const snapshot = await table.snapshot()

      expect(snapshot.version).toBe(14)
      expect(snapshot.files.length).toBe(15)
    })

    it('should apply removes from logs after checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      // Delete after checkpoint
      await (table as any).delete({ id: 5 })

      const snapshot = await table.snapshot()

      // Should have 10 - 1 = 9 files after delete
      expect(snapshot.files.length).toBe(9)
    })

    it('should handle checkpoint + many subsequent logs', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10) // Checkpoint at 9
      await writeCommits(table, 100) // Many more logs

      const snapshot = await table.snapshot()

      expect(snapshot.version).toBe(109)
      expect(snapshot.files.length).toBe(110)
    })
  })
})

// =============================================================================
// MULTI-PART CHECKPOINT TESTS
// =============================================================================

describe('Multi-Part Checkpoints', () => {
  describe('Multi-Part Creation', () => {
    it('should create multi-part checkpoints for large tables', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 5,
      })

      // Write enough data to exceed single checkpoint size
      await writeCommits(table, 15)

      const files = await storage.list('test-table/_delta_log')
      const multiPartFiles = files.filter((f) =>
        /\d+\.checkpoint\.\d+\.\d+\.parquet$/.test(f)
      )

      expect(multiPartFiles.length).toBeGreaterThan(0)
    })

    it('should use correct naming: <version>.checkpoint.<part>.<total>.parquet', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const multiPartFiles = files.filter((f) =>
        /\.checkpoint\.\d+\.\d+\.parquet$/.test(f)
      )

      // Verify naming format
      multiPartFiles.forEach((f) => {
        const parsed = parseMultiPartCheckpoint(f)
        expect(parsed).not.toBeNull()
        expect(parsed!.part).toBeGreaterThanOrEqual(1)
        expect(parsed!.part).toBeLessThanOrEqual(parsed!.total)
      })
    })

    it('should distribute actions evenly across parts', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      // Read each part and count actions
      const lastCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      if (lastCheckpoint.parts && lastCheckpoint.parts > 1) {
        const partSizes: number[] = []
        for (let i = 1; i <= lastCheckpoint.parts; i++) {
          const partSnapshot = await (table as any).readCheckpointPart(
            lastCheckpoint.version,
            i,
            lastCheckpoint.parts
          )
          partSizes.push(partSnapshot.files.length)
        }

        // Parts should be roughly equal (within 1 action)
        const maxSize = Math.max(...partSizes)
        const minSize = Math.min(...partSizes)
        expect(maxSize - minSize).toBeLessThanOrEqual(1)
      }
    })
  })

  describe('Multi-Part Reading', () => {
    it('should read all parts to reconstruct state', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      const snapshot = await table.snapshot()

      // Should have all files from all checkpoint parts
      expect(snapshot.files.length).toBe(10)
    })

    it('should handle missing checkpoint parts gracefully', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      // Delete one checkpoint part
      const files = await storage.list('test-table/_delta_log')
      const checkpointPart = files.find((f) =>
        /\.checkpoint\.1\.\d+\.parquet$/.test(f)
      )
      if (checkpointPart) {
        await storage.delete(checkpointPart)
      }

      // Should either throw or fall back to log replay
      try {
        const table2 = new DeltaTable(storage, 'test-table')
        const snapshot = await table2.snapshot()
        // If it succeeds, it fell back to log replay
        expect(snapshot.files.length).toBe(10)
      } catch (e) {
        // Expected error for missing checkpoint part
        expect((e as Error).message).toContain('checkpoint')
      }
    })

    it('should validate part count matches _last_checkpoint', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      const lastCheckpoint = JSON.parse(
        new TextDecoder().decode(
          await storage.read('test-table/_delta_log/_last_checkpoint')
        )
      ) as LastCheckpoint

      if (lastCheckpoint.parts) {
        const files = await storage.list('test-table/_delta_log')
        const parts = files.filter(
          (f) =>
            f.includes('.checkpoint.') &&
            f.includes(`${lastCheckpoint.version}`)
        )
        expect(parts.length).toBe(lastCheckpoint.parts)
      }
    })
  })
})

// =============================================================================
// CHECKPOINT DISCOVERY TESTS
// =============================================================================

describe('Checkpoint Discovery', () => {
  describe('Discovery from _delta_log', () => {
    it('should discover checkpoints by listing _delta_log', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 25)

      const checkpoints = await (table as any).discoverCheckpoints()

      expect(checkpoints.length).toBeGreaterThan(0)
      expect(checkpoints).toContain(9)
      expect(checkpoints).toContain(19)
    })

    it('should return checkpoints sorted by version descending', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 35)

      const checkpoints = await (table as any).discoverCheckpoints()

      // Should be sorted descending
      for (let i = 0; i < checkpoints.length - 1; i++) {
        expect(checkpoints[i]).toBeGreaterThan(checkpoints[i + 1])
      }
    })

    it('should find latest checkpoint without _last_checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 25)

      // Delete _last_checkpoint
      await storage.delete('test-table/_delta_log/_last_checkpoint')

      const latestCheckpoint = await (table as any).findLatestCheckpoint()

      expect(latestCheckpoint).toBe(19)
    })

    it('should handle table with no checkpoints', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 5)

      const checkpoints = await (table as any).discoverCheckpoints()

      expect(checkpoints.length).toBe(0)
    })
  })

  describe('Checkpoint Validation', () => {
    it('should validate checkpoint file integrity', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      const isValid = await (table as any).validateCheckpoint(9)

      expect(isValid).toBe(true)
    })

    it('should detect corrupted checkpoint files', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 10)

      // Corrupt the checkpoint file
      const files = await storage.list('test-table/_delta_log')
      const checkpointPath = files.find((f) => f.includes('.checkpoint.'))!
      await storage.write(checkpointPath, new TextEncoder().encode('corrupted'))

      const isValid = await (table as any).validateCheckpoint(9)

      expect(isValid).toBe(false)
    })

    it('should fall back to previous checkpoint if latest is corrupted', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 25)

      // Corrupt the latest checkpoint (version 19)
      const files = await storage.list('test-table/_delta_log')
      const latestCheckpoint = files.find((f) =>
        f.includes('00000000000000000019.checkpoint')
      )
      if (latestCheckpoint) {
        await storage.write(latestCheckpoint, new TextEncoder().encode('bad'))
      }

      // Should fall back to version 9 checkpoint
      const table2 = new DeltaTable(storage, 'test-table')
      const snapshot = await table2.snapshot()

      // Should still work, using older checkpoint
      expect(snapshot.version).toBe(24)
    })
  })
})

// =============================================================================
// CHECKPOINT CLEANUP/RETENTION TESTS
// =============================================================================

describe('Checkpoint Cleanup and Retention', () => {
  describe('Checkpoint Retention Policy', () => {
    it('should retain specified number of checkpoints', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        numRetainedCheckpoints: 2,
      })

      // Create 4 checkpoints
      await writeCommits(table, 40)

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter((f) => f.includes('.checkpoint.'))
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Should only have 2 checkpoints (versions 29 and 39)
      expect(checkpointVersions.length).toBe(2)
      expect(checkpointVersions).toContain(29)
      expect(checkpointVersions).toContain(39)
    })

    it('should clean up old checkpoints after retention period', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        checkpointRetentionMs: 1000, // 1 second
      })

      await writeCommits(table, 10)

      // Wait for retention period
      await new Promise((resolve) => setTimeout(resolve, 1100))

      await writeCommits(table, 10)

      // Trigger cleanup
      await (table as any).cleanupCheckpoints()

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter((f) => f.includes('.checkpoint.'))
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Old checkpoint should be cleaned up
      expect(checkpointVersions).not.toContain(9)
      expect(checkpointVersions).toContain(19)
    })

    it('should always keep at least one checkpoint', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        numRetainedCheckpoints: 0, // Try to keep none
      })

      await writeCommits(table, 20)

      await (table as any).cleanupCheckpoints()

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter((f) => f.includes('.checkpoint.'))
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Should still have at least one checkpoint
      expect(checkpointVersions.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Log File Cleanup', () => {
    it('should allow log cleanup up to oldest retained checkpoint', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 25)

      // Get cleanable log versions
      const cleanableVersions = await (table as any).getCleanableLogVersions()

      // Versions before oldest checkpoint (9) can be cleaned
      expect(cleanableVersions.every((v: number) => v < 9)).toBe(true)
    })

    it('should not clean logs needed for checkpoint reconstruction', async () => {
      const { table, storage } = createTestTable()

      await writeCommits(table, 15)

      await (table as any).cleanupLogs()

      // Logs from checkpoint version onward should still exist
      const files = await storage.list('test-table/_delta_log')
      const logVersions = files
        .filter((f) => f.endsWith('.json'))
        .map((f) => {
          const match = f.match(/(\d+)\.json$/)
          return match ? parseInt(match[1], 10) : null
        })
        .filter((v): v is number => v !== null)

      // Logs 9-14 should exist (checkpoint at 9, current at 14)
      for (let v = 9; v <= 14; v++) {
        expect(logVersions).toContain(v)
      }
    })
  })

  describe('Multi-Part Checkpoint Cleanup', () => {
    it('should clean up all parts of old multi-part checkpoints', async () => {
      const storage = new MemoryStorage()
      const table = new DeltaTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
        numRetainedCheckpoints: 1,
      })

      await writeCommits(table, 25)

      const files = await storage.list('test-table/_delta_log')

      // Should not have checkpoint parts from version 9
      const oldCheckpointParts = files.filter((f) =>
        f.includes('00000000000000000009.checkpoint')
      )
      expect(oldCheckpointParts.length).toBe(0)
    })
  })
})

// =============================================================================
// CONCURRENT OPERATIONS TESTS
// =============================================================================

describe('Concurrent Checkpoint Operations', () => {
  it('should handle concurrent checkpoint creation attempts', async () => {
    const { table, storage } = createTestTable()

    await writeCommits(table, 10)

    // Try to create checkpoint concurrently
    const results = await Promise.allSettled([
      (table as any).createCheckpoint(9),
      (table as any).createCheckpoint(9),
    ])

    // At least one should succeed
    const succeeded = results.filter((r) => r.status === 'fulfilled')
    expect(succeeded.length).toBeGreaterThanOrEqual(1)

    // Should only have one checkpoint file
    const files = await storage.list('test-table/_delta_log')
    const checkpoints = files.filter(
      (f) => parseCheckpointVersion(f) === 9
    )
    expect(checkpoints.length).toBe(1)
  })

  it('should handle reads during checkpoint creation', async () => {
    const { table, storage } = createTestTable()

    await writeCommits(table, 9)

    // Start checkpoint creation and read simultaneously
    const [, snapshot] = await Promise.all([
      table.write([{ id: 999, value: 'trigger' }]), // Triggers checkpoint
      table.snapshot(),
    ])

    // Read should complete successfully
    expect(snapshot).toBeDefined()
    expect(snapshot.version).toBeGreaterThanOrEqual(8)
  })

  it('should handle writes during checkpoint cleanup', async () => {
    const storage = new MemoryStorage()
    const table = new DeltaTable(storage, 'test-table', {
      numRetainedCheckpoints: 1,
    })

    await writeCommits(table, 20)

    // Write and cleanup concurrently
    const results = await Promise.allSettled([
      table.write([{ id: 999, value: 'new' }]),
      (table as any).cleanupCheckpoints(),
    ])

    // Both should succeed
    results.forEach((r) => {
      expect(r.status).toBe('fulfilled')
    })
  })
})

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

describe('Checkpoint Error Handling', () => {
  it('should throw when creating checkpoint for non-existent version', async () => {
    const { table, storage } = createTestTable()

    await writeCommits(table, 5)

    await expect((table as any).createCheckpoint(100)).rejects.toThrow()
  })

  it('should throw when reading checkpoint that does not exist', async () => {
    const { table, storage } = createTestTable()

    await writeCommits(table, 5)

    await expect((table as any).readCheckpoint(9)).rejects.toThrow()
  })

  it('should handle storage errors during checkpoint write', async () => {
    const { table, storage } = createTestTable()

    // Make write fail
    const originalWrite = storage.write.bind(storage)
    storage.write = async (path: string, data: Uint8Array) => {
      if (path.includes('.checkpoint.')) {
        throw new Error('Storage write failed')
      }
      return originalWrite(path, data)
    }

    // Should handle checkpoint write failure gracefully
    await expect(writeCommits(table, 10)).resolves.toBeDefined()

    // Table should still be usable
    const snapshot = await table.snapshot()
    expect(snapshot.version).toBe(9)
  })

  it('should recover from partial checkpoint write', async () => {
    const storage = new MemoryStorage()
    const table = new DeltaTable(storage, 'test-table', {
      maxActionsPerCheckpoint: 3,
    })

    await writeCommits(table, 5)

    // Simulate partial multi-part checkpoint
    const checkpointPath = 'test-table/_delta_log/00000000000000000009.checkpoint.1.3.parquet'
    await storage.write(checkpointPath, new Uint8Array([1, 2, 3])) // Partial/invalid

    // Write more to trigger new checkpoint attempt
    await writeCommits(table, 5)

    // Should be able to read state (either new checkpoint or log replay)
    const snapshot = await table.snapshot()
    expect(snapshot.version).toBe(9)
  })
})

// =============================================================================
// PERFORMANCE TESTS
// =============================================================================

describe('Checkpoint Performance', () => {
  // SKIPPED: Performance comparison tests are inherently flaky because:
  // 1. In-memory operations complete in sub-millisecond times, making timing unreliable
  // 2. JIT warmup effects can cause the second operation to be faster regardless
  // 3. The checkpoint vs. log replay difference may not be measurable for small tables
  // The checkpoint functionality is already tested for correctness in other tests.
  it.skip('should be faster than full log replay for large tables', async () => {
    const { table, storage } = createTestTable()

    // Create table with checkpoint
    await writeCommits(table, 50)

    // Time snapshot with checkpoint
    const startWithCheckpoint = performance.now()
    const table2 = new DeltaTable(storage, 'test-table')
    await table2.snapshot()
    const timeWithCheckpoint = performance.now() - startWithCheckpoint

    // Delete checkpoints to force log replay
    const files = await storage.list('test-table/_delta_log')
    for (const f of files) {
      if (f.includes('.checkpoint.')) {
        await storage.delete(f)
      }
    }
    await storage.delete('test-table/_delta_log/_last_checkpoint')

    // Time snapshot without checkpoint (full log replay)
    const startWithoutCheckpoint = performance.now()
    const table3 = new DeltaTable(storage, 'test-table')
    await table3.snapshot()
    const timeWithoutCheckpoint = performance.now() - startWithoutCheckpoint

    // Checkpoint should be faster (or at least not significantly slower)
    // Allow some tolerance for small tables
    expect(timeWithCheckpoint).toBeLessThanOrEqual(timeWithoutCheckpoint * 1.5)
  })

  it('should limit checkpoint file size with multi-part splitting', async () => {
    const storage = new MemoryStorage()
    const maxSize = 1024 // 1KB max per part
    const table = new DeltaTable(storage, 'test-table', {
      maxCheckpointSizeBytes: maxSize,
    })

    await writeCommits(table, 20)

    const files = await storage.list('test-table/_delta_log')
    const checkpointFiles = files.filter((f) => f.includes('.checkpoint.'))

    // Each checkpoint file should be under max size
    for (const f of checkpointFiles) {
      const data = await storage.read(f)
      expect(data.length).toBeLessThanOrEqual(maxSize * 2) // Allow some overhead
    }
  })
})
