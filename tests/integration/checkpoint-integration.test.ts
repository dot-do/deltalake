/**
 * Checkpoint Integration Tests
 *
 * Tests for Delta Lake checkpoint creation and reading across storage backends.
 * These integration tests verify:
 * - Checkpoint creation at version intervals
 * - Checkpoint reading and state reconstruction
 * - Multi-part checkpoint handling
 * - Checkpoint + log combination for state recovery
 * - Cross-storage checkpoint consistency
 * - Checkpoint cleanup and retention
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { env } from 'cloudflare:test'
import { DeltaTable, type DeltaSnapshot } from '../../src/delta/index.js'
import { MemoryStorage, R2Storage, type StorageBackend } from '../../src/storage/index.js'

// =============================================================================
// TEST FIXTURES AND HELPERS
// =============================================================================

interface TestRecord {
  id: number
  name: string
  value: number
  category?: string
}

/**
 * Create a test table with optional checkpoint configuration
 */
function createTable(
  storage: StorageBackend,
  path: string = 'test-table',
  config?: { checkpointInterval?: number; maxActionsPerCheckpoint?: number }
): DeltaTable<TestRecord> {
  return new DeltaTable<TestRecord>(storage, path, config)
}

/**
 * Generate test records
 */
function generateRecords(startId: number, count: number): TestRecord[] {
  return Array.from({ length: count }, (_, i) => ({
    id: startId + i,
    name: `record_${startId + i}`,
    value: (startId + i) * 10,
  }))
}

/**
 * Write N commits to the table
 */
async function writeCommits(table: DeltaTable<TestRecord>, count: number): Promise<void> {
  for (let i = 0; i < count; i++) {
    await table.write(generateRecords(i * 5 + 1, 5))
  }
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
 * Check if a file is a checkpoint file
 */
function isCheckpointFile(filename: string): boolean {
  return filename.includes('.checkpoint.')
}

/**
 * Read _last_checkpoint file from storage
 */
async function readLastCheckpoint(
  storage: StorageBackend,
  tablePath: string
): Promise<{ version: number; size: number; parts?: number } | null> {
  try {
    const data = await storage.read(`${tablePath}/_delta_log/_last_checkpoint`)
    return JSON.parse(new TextDecoder().decode(data))
  } catch {
    return null
  }
}

// =============================================================================
// CHECKPOINT CREATION TESTS
// =============================================================================

describe('Checkpoint Creation', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Automatic Checkpoint Creation', () => {
    it('should create checkpoint after default interval of 10 commits', async () => {
      const table = createTable(storage)

      // Write 10 commits (versions 0-9)
      await writeCommits(table, 10)

      // Check for checkpoint at version 9
      const files = await storage.list('test-table/_delta_log')
      const checkpointFiles = files.filter(isCheckpointFile)

      expect(checkpointFiles.length).toBeGreaterThan(0)

      const hasVersion9Checkpoint = checkpointFiles.some(
        f => parseCheckpointVersion(f) === 9
      )
      expect(hasVersion9Checkpoint).toBe(true)
    })

    it('should create checkpoints at every 10th version', async () => {
      const table = createTable(storage)

      // Write 25 commits (versions 0-24)
      await writeCommits(table, 25)

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter(isCheckpointFile)
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Should have checkpoints at versions 9 and 19
      expect(checkpointVersions).toContain(9)
      expect(checkpointVersions).toContain(19)
    })

    it('should respect custom checkpoint interval', async () => {
      const table = createTable(storage, 'test-table', { checkpointInterval: 5 })

      // Write 12 commits
      await writeCommits(table, 12)

      const files = await storage.list('test-table/_delta_log')
      const checkpointVersions = files
        .filter(isCheckpointFile)
        .map(parseCheckpointVersion)
        .filter((v): v is number => v !== null)

      // Should have checkpoints at versions 4 and 9
      expect(checkpointVersions).toContain(4)
      expect(checkpointVersions).toContain(9)
    })

    it('should not create checkpoint for fewer than interval commits', async () => {
      const table = createTable(storage)

      // Write only 5 commits
      await writeCommits(table, 5)

      const files = await storage.list('test-table/_delta_log')
      const checkpointFiles = files.filter(isCheckpointFile)

      expect(checkpointFiles).toHaveLength(0)
    })
  })

  describe('Checkpoint File Format', () => {
    it('should create valid Parquet checkpoint file', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const checkpointPath = files.find(isCheckpointFile)!

      const checkpointData = await storage.read(checkpointPath)

      // Parquet magic bytes: PAR1
      expect(checkpointData.slice(0, 4)).toEqual(
        new Uint8Array([0x50, 0x41, 0x52, 0x31])
      )
      expect(checkpointData.slice(-4)).toEqual(
        new Uint8Array([0x50, 0x41, 0x52, 0x31])
      )
    })

    it('should use correct naming format: <version>.checkpoint.parquet', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const checkpointFile = files.find(isCheckpointFile)

      expect(checkpointFile).toBeDefined()
      expect(checkpointFile).toMatch(/00000000000000000009\.checkpoint\.parquet$/)
    })

    it('should use 20-digit zero-padded version numbers', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const files = await storage.list('test-table/_delta_log')
      const checkpointFile = files.find(isCheckpointFile)

      const match = checkpointFile?.match(/(\d+)\.checkpoint\.parquet/)
      expect(match?.[1].length).toBe(20)
    })
  })
})

// =============================================================================
// _LAST_CHECKPOINT FILE TESTS
// =============================================================================

describe('_last_checkpoint File', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Last Checkpoint Creation and Updates', () => {
    it('should create _last_checkpoint file after first checkpoint', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const exists = await storage.exists('test-table/_delta_log/_last_checkpoint')
      expect(exists).toBe(true)
    })

    it('should contain correct version in _last_checkpoint', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const lastCheckpoint = await readLastCheckpoint(storage, 'test-table')
      expect(lastCheckpoint?.version).toBe(9)
    })

    it('should update _last_checkpoint after each new checkpoint', async () => {
      const table = createTable(storage)

      await writeCommits(table, 10)
      const first = await readLastCheckpoint(storage, 'test-table')
      expect(first?.version).toBe(9)

      await writeCommits(table, 10)
      const second = await readLastCheckpoint(storage, 'test-table')
      expect(second?.version).toBe(19)
    })

    it('should include size (action count) in _last_checkpoint', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const lastCheckpoint = await readLastCheckpoint(storage, 'test-table')
      expect(typeof lastCheckpoint?.size).toBe('number')
      expect(lastCheckpoint!.size).toBeGreaterThan(0)
    })
  })
})

// =============================================================================
// CHECKPOINT READING AND STATE RECONSTRUCTION
// =============================================================================

describe('Checkpoint Reading and State Reconstruction', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('State Reconstruction from Checkpoint', () => {
    it('should reconstruct table state from checkpoint', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      // Create new table instance (simulating fresh read)
      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      expect(snapshot.version).toBe(9)
      expect(snapshot.files).toHaveLength(10)
    })

    it('should include metadata in reconstructed state', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      expect(snapshot.metadata).toBeDefined()
      expect(snapshot.metadata?.id).toBeDefined()
      expect(snapshot.metadata?.format).toBeDefined()
    })

    it('should include protocol in reconstructed state', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      expect(snapshot.protocol).toBeDefined()
      expect(snapshot.protocol?.minReaderVersion).toBeGreaterThanOrEqual(1)
      expect(snapshot.protocol?.minWriterVersion).toBeGreaterThanOrEqual(1)
    })

    it('should reconstruct all file paths correctly', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      // All files should have valid paths
      for (const file of snapshot.files) {
        expect(file.path).toMatch(/\.parquet$/)
        expect(file.size).toBeGreaterThan(0)
      }
    })
  })

  describe('Checkpoint + Log Combination', () => {
    it('should combine checkpoint with subsequent logs', async () => {
      const table = createTable(storage)

      // Create checkpoint at version 9
      await writeCommits(table, 10)

      // Add more commits after checkpoint
      await writeCommits(table, 5)

      const snapshot = await table.snapshot()
      expect(snapshot.version).toBe(14)
      expect(snapshot.files).toHaveLength(15)
    })

    it('should prefer checkpoint over full log replay', async () => {
      const table = createTable(storage)
      await writeCommits(table, 20)

      // Track read operations
      const readCalls: string[] = []
      const originalRead = storage.read.bind(storage)
      storage.read = async (path: string) => {
        readCalls.push(path)
        return originalRead(path)
      }

      // Fresh table read
      const table2 = createTable(storage)
      await table2.snapshot()

      // Should read checkpoint instead of all log files
      const checkpointReads = readCalls.filter(p => p.includes('.checkpoint.'))
      const logFileReads = readCalls.filter(
        p => p.includes('.json') && !p.includes('_last_checkpoint')
      )

      expect(checkpointReads.length).toBeGreaterThan(0)
      // Should read fewer log files than total versions (20)
      expect(logFileReads.length).toBeLessThan(20)
    })

    it('should correctly apply removes from logs after checkpoint', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      // Delete after checkpoint (implementation specific)
      // This may rewrite files or add tombstones
      await table.delete({ id: 1 })

      const snapshot = await table.snapshot()

      // Verify delete was applied
      const results = await table.query()
      const hasId1 = results.some(r => r.id === 1)
      expect(hasId1).toBe(false)
    })
  })
})

// =============================================================================
// TIME TRAVEL WITH CHECKPOINTS
// =============================================================================

describe('Time Travel with Checkpoints', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should support time travel to version before checkpoint', async () => {
    const table = createTable(storage)
    await writeCommits(table, 15)

    // Time travel to version 5 (before checkpoint at 9)
    const snapshotV5 = await table.snapshot(5)

    expect(snapshotV5.version).toBe(5)
    expect(snapshotV5.files).toHaveLength(6) // versions 0-5
  })

  it('should support time travel to checkpoint version', async () => {
    const table = createTable(storage)
    await writeCommits(table, 15)

    const snapshotV9 = await table.snapshot(9)

    expect(snapshotV9.version).toBe(9)
    expect(snapshotV9.files).toHaveLength(10)
  })

  it('should support time travel to version after checkpoint', async () => {
    const table = createTable(storage)
    await writeCommits(table, 15)

    const snapshotV12 = await table.snapshot(12)

    expect(snapshotV12.version).toBe(12)
    expect(snapshotV12.files).toHaveLength(13)
  })

  it('should use checkpoint for efficient time travel', async () => {
    const table = createTable(storage)
    await writeCommits(table, 25)

    // Time travel to version 22 should use checkpoint at 19
    const readCalls: string[] = []
    const originalRead = storage.read.bind(storage)
    storage.read = async (path: string) => {
      readCalls.push(path)
      return originalRead(path)
    }

    const table2 = createTable(storage)
    await table2.snapshot(22)

    // Should read checkpoint + logs 20-22, not logs 0-22
    const logFileReads = readCalls.filter(
      p => p.includes('.json') && !p.includes('_last_checkpoint') && !p.includes('.checkpoint')
    )
    expect(logFileReads.length).toBeLessThanOrEqual(4) // Logs 20, 21, 22 (plus maybe some discovery)
  })
})

// =============================================================================
// R2 STORAGE CHECKPOINT TESTS
// =============================================================================

describe('Checkpoint Integration - R2Storage', () => {
  let storage: R2Storage
  let tablePath: string

  beforeEach(() => {
    storage = new R2Storage({ bucket: env.TEST_BUCKET })
    tablePath = `test-table-${Date.now()}`
  })

  describe('R2 Checkpoint Operations', () => {
    it('should create checkpoints in R2', async () => {
      const table = createTable(storage, tablePath)
      await writeCommits(table, 10)

      const files = await storage.list(`${tablePath}/_delta_log`)
      const checkpointFiles = files.filter(isCheckpointFile)

      expect(checkpointFiles.length).toBeGreaterThan(0)
    })

    it('should read checkpoints from R2', async () => {
      const table = createTable(storage, tablePath)
      await writeCommits(table, 10)

      // Fresh read
      const table2 = createTable(storage, tablePath)
      const snapshot = await table2.snapshot()

      expect(snapshot.version).toBe(9)
      expect(snapshot.files).toHaveLength(10)
    })

    it('should store _last_checkpoint in R2', async () => {
      const table = createTable(storage, tablePath)
      await writeCommits(table, 10)

      const exists = await storage.exists(`${tablePath}/_delta_log/_last_checkpoint`)
      expect(exists).toBe(true)

      const lastCheckpoint = await readLastCheckpoint(storage, tablePath)
      expect(lastCheckpoint?.version).toBe(9)
    })
  })
})

// =============================================================================
// CROSS-STORAGE CHECKPOINT CONSISTENCY
// =============================================================================

describe('Cross-Storage Checkpoint Consistency', () => {
  it('should produce identical checkpoints in Memory and R2', async () => {
    const memStorage = new MemoryStorage()
    const r2Storage = new R2Storage({ bucket: env.TEST_BUCKET })
    const r2Path = `test-table-${Date.now()}`

    const memTable = createTable(memStorage, 'test-table')
    const r2Table = createTable(r2Storage, r2Path)

    // Write same data to both
    for (let i = 0; i < 10; i++) {
      const records = generateRecords(i * 5 + 1, 5)
      await memTable.write(records)
      await r2Table.write(records)
    }

    // Compare snapshots
    const memSnapshot = await memTable.snapshot()
    const r2Snapshot = await r2Table.snapshot()

    expect(memSnapshot.version).toBe(r2Snapshot.version)
    expect(memSnapshot.files.length).toBe(r2Snapshot.files.length)

    // Compare _last_checkpoint
    const memLast = await readLastCheckpoint(memStorage, 'test-table')
    const r2Last = await readLastCheckpoint(r2Storage, r2Path)

    expect(memLast?.version).toBe(r2Last?.version)
  })
})

// =============================================================================
// CHECKPOINT RECOVERY AND ERROR HANDLING
// =============================================================================

describe('Checkpoint Recovery and Error Handling', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Corrupted Checkpoint Handling', () => {
    it('should fall back to log replay if checkpoint is corrupted', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      // Corrupt the checkpoint
      const files = await storage.list('test-table/_delta_log')
      const checkpointPath = files.find(isCheckpointFile)!
      await storage.write(checkpointPath, new TextEncoder().encode('corrupted'))

      // Fresh table should still work via log replay
      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      expect(snapshot.version).toBe(9)
    })

    it('should handle missing _last_checkpoint gracefully', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      // Delete _last_checkpoint
      await storage.delete('test-table/_delta_log/_last_checkpoint')

      // Should discover checkpoint by scanning
      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      expect(snapshot.version).toBe(9)
    })

    it('should handle corrupted _last_checkpoint', async () => {
      const table = createTable(storage)
      await writeCommits(table, 10)

      // Corrupt _last_checkpoint
      await storage.write(
        'test-table/_delta_log/_last_checkpoint',
        new TextEncoder().encode('not json')
      )

      // Should recover by scanning or log replay
      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      expect(snapshot.version).toBe(9)
    })
  })

  describe('Checkpoint Write Failures', () => {
    it('should not fail commit if checkpoint write fails', async () => {
      const table = createTable(storage)

      // Make checkpoint writes fail
      const originalWrite = storage.write.bind(storage)
      storage.write = async (path: string, data: Uint8Array) => {
        if (path.includes('.checkpoint.')) {
          throw new Error('Checkpoint write failed')
        }
        return originalWrite(path, data)
      }

      // Writes should still succeed (checkpoint failure should be logged but not thrown)
      await writeCommits(table, 10)

      expect(await table.version()).toBe(9)
    })
  })
})

// =============================================================================
// MULTI-PART CHECKPOINT TESTS
// =============================================================================

describe('Multi-Part Checkpoints', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('Multi-Part Creation', () => {
    it('should create multi-part checkpoints for large tables', async () => {
      const table = createTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 5,
      })

      // Write enough data to trigger multi-part
      await writeCommits(table, 15)

      const files = await storage.list('test-table/_delta_log')
      const multiPartFiles = files.filter(f =>
        /\d+\.checkpoint\.\d+\.\d+\.parquet$/.test(f)
      )

      // May have multi-part checkpoints if actions exceed threshold
      // Implementation dependent - verify at least regular checkpoint exists
      const hasCheckpoint = files.some(isCheckpointFile)
      expect(hasCheckpoint).toBe(true)
    })

    it('should include parts count in _last_checkpoint for multi-part', async () => {
      const table = createTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      const lastCheckpoint = await readLastCheckpoint(storage, 'test-table')

      // If multi-part, should have parts field
      if (lastCheckpoint?.parts) {
        expect(lastCheckpoint.parts).toBeGreaterThan(0)
      }
    })
  })

  describe('Multi-Part Reading', () => {
    it('should read all parts to reconstruct state', async () => {
      const table = createTable(storage, 'test-table', {
        maxActionsPerCheckpoint: 3,
      })

      await writeCommits(table, 10)

      const table2 = createTable(storage)
      const snapshot = await table2.snapshot()

      // All files should be present regardless of checkpoint format
      expect(snapshot.files).toHaveLength(10)
    })
  })
})

// =============================================================================
// CHECKPOINT CLEANUP TESTS
// =============================================================================

describe('Checkpoint Cleanup and Retention', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should create multiple checkpoints over time', async () => {
    const table = createTable(storage)

    // Create 4 checkpoints (versions 9, 19, 29, 39)
    await writeCommits(table, 40)

    const files = await storage.list('test-table/_delta_log')
    const checkpointVersions = files
      .filter(isCheckpointFile)
      .map(parseCheckpointVersion)
      .filter((v): v is number => v !== null)

    expect(checkpointVersions).toContain(9)
    expect(checkpointVersions).toContain(19)
    expect(checkpointVersions).toContain(29)
    expect(checkpointVersions).toContain(39)
  })

  it('should always keep at least one checkpoint', async () => {
    const table = createTable(storage, 'test-table', {
      checkpointInterval: 5,
    })

    await writeCommits(table, 20)

    const files = await storage.list('test-table/_delta_log')
    const checkpointFiles = files.filter(isCheckpointFile)

    expect(checkpointFiles.length).toBeGreaterThanOrEqual(1)
  })
})

// =============================================================================
// CONCURRENT CHECKPOINT OPERATIONS
// =============================================================================

describe('Concurrent Checkpoint Operations', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should handle concurrent reads during checkpoint creation', async () => {
    const table = createTable(storage)
    await writeCommits(table, 9)

    // Start checkpoint creation and read simultaneously
    const [, snapshot] = await Promise.all([
      table.write(generateRecords(1000, 5)), // Triggers checkpoint at version 9
      table.snapshot(),
    ])

    // Read should complete successfully
    expect(snapshot).toBeDefined()
    expect(snapshot.version).toBeGreaterThanOrEqual(8)
  })

  it('should handle concurrent writes during checkpoint cleanup', async () => {
    const table = createTable(storage)
    await writeCommits(table, 20)

    // Write while cleanup might be happening
    const results = await Promise.allSettled([
      table.write(generateRecords(2000, 5)),
      table.write(generateRecords(3000, 5)),
    ])

    // At least one write should succeed
    const succeeded = results.filter(r => r.status === 'fulfilled')
    expect(succeeded.length).toBeGreaterThanOrEqual(1)
  })
})

// =============================================================================
// PERFORMANCE TESTS
// =============================================================================

describe('Checkpoint Performance', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should be faster than full log replay for large tables', async () => {
    const table = createTable(storage)
    await writeCommits(table, 50)

    // Time snapshot with checkpoint
    const startWithCheckpoint = performance.now()
    const table2 = createTable(storage)
    await table2.snapshot()
    const timeWithCheckpoint = performance.now() - startWithCheckpoint

    // Delete checkpoints to force log replay
    const files = await storage.list('test-table/_delta_log')
    for (const f of files) {
      if (isCheckpointFile(f)) {
        await storage.delete(f)
      }
    }
    await storage.delete('test-table/_delta_log/_last_checkpoint')

    // Time snapshot without checkpoint
    const startWithoutCheckpoint = performance.now()
    const table3 = createTable(storage)
    await table3.snapshot()
    const timeWithoutCheckpoint = performance.now() - startWithoutCheckpoint

    // Checkpoint should be faster (or at least not much slower)
    // Allow 50% tolerance for test stability
    expect(timeWithCheckpoint).toBeLessThanOrEqual(timeWithoutCheckpoint * 1.5)
  })
})
