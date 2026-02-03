/**
 * Delta Lake VACUUM Operation
 *
 * Cleans up orphaned files from a Delta table that are no longer
 * referenced by any snapshot. This is essential for production use
 * to prevent unbounded storage growth.
 *
 * Features:
 * - Configurable retention period (default 7 days)
 * - Dry-run mode to preview deletions without modifying data
 * - Returns metrics about files deleted and bytes freed
 * - Safe deletion - only removes files older than retention period
 *
 * @module delta/vacuum
 */

import type { StorageBackend } from '../storage/index.js'
import { getLogger } from '../utils/index.js'
import { ValidationError } from '../errors.js'
import { DELTA_LOG_DIR, isValidDeltaAction } from './index.js'
import type { DeltaAction, DeltaTable, DeltaSnapshot, RemoveAction } from './index.js'

// =============================================================================
// CONSTANTS
// =============================================================================

/** Default retention period in hours (7 days) */
const DEFAULT_RETENTION_HOURS = 168

/** Minimum allowed retention period in hours (1 hour for safety) */
const MINIMUM_RETENTION_HOURS = 1

// =============================================================================
// TYPES
// =============================================================================

/**
 * Configuration options for the vacuum operation.
 */
export interface VacuumConfig {
  /**
   * Retention period in hours.
   * Files older than this will be deleted.
   * Default: 168 hours (7 days)
   */
  readonly retentionHours?: number

  /**
   * If true, only preview what would be deleted without actually deleting.
   * Default: false
   */
  readonly dryRun?: boolean

  /**
   * Optional progress callback for monitoring vacuum progress.
   */
  readonly onProgress?: (phase: string, current: number, total: number) => void
}

/**
 * Metrics returned after a vacuum operation.
 */
export interface VacuumMetrics {
  /** Number of files deleted */
  filesDeleted: number

  /** Total bytes freed (sum of deleted file sizes) */
  bytesFreed: number

  /** Number of files retained (still referenced or within retention period) */
  filesRetained: number

  /** Whether this was a dry run */
  dryRun: boolean

  /** Files that would be deleted (populated in dry run mode) */
  filesToDelete?: string[]

  /** Duration of the vacuum operation in milliseconds */
  durationMs: number

  /** Number of files scanned */
  filesScanned: number

  /** Any errors encountered during deletion (non-fatal) */
  errors?: string[]
}

/**
 * Internal structure for tracking file metadata during vacuum.
 */
interface FileInfo {
  path: string
  size: number
  lastModified: Date
}

// =============================================================================
// VACUUM IMPLEMENTATION
// =============================================================================

/**
 * Perform a VACUUM operation on a Delta table.
 *
 * Removes orphaned files that are:
 * 1. Not referenced by any active snapshot
 * 2. Older than the retention period
 *
 * @param table - The DeltaTable to vacuum
 * @param config - Optional configuration for the vacuum operation
 * @returns Metrics about the vacuum operation
 *
 * @example
 * ```typescript
 * // Basic vacuum with defaults (7 day retention)
 * const metrics = await vacuum(table)
 * console.log(`Deleted ${metrics.filesDeleted} files, freed ${metrics.bytesFreed} bytes`)
 *
 * // Dry run to preview deletions
 * const preview = await vacuum(table, { dryRun: true })
 * console.log(`Would delete: ${preview.filesToDelete}`)
 *
 * // Custom retention period (24 hours)
 * const metrics = await vacuum(table, { retentionHours: 24 })
 * ```
 */
export async function vacuum(
  table: DeltaTable,
  config?: VacuumConfig
): Promise<VacuumMetrics> {
  const startTime = Date.now()
  const retentionHours = config?.retentionHours ?? DEFAULT_RETENTION_HOURS
  const dryRun = config?.dryRun ?? false
  const onProgress = config?.onProgress

  // Validate retention period
  if (retentionHours < MINIMUM_RETENTION_HOURS) {
    throw new ValidationError(
      `Retention period must be at least ${MINIMUM_RETENTION_HOURS} hour(s). ` +
        `Got: ${retentionHours} hours. This is a safety measure to prevent accidental data loss.`,
      'retentionHours',
      retentionHours
    )
  }

  // Calculate the retention threshold
  const retentionThreshold = new Date(Date.now() - retentionHours * 60 * 60 * 1000)

  // Access storage and table path via public accessors
  const storage = table.getStorage()
  const tablePath = table.getTablePath()

  // Get the current snapshot to find all active files
  const snapshot = await table.snapshot()
  const activeFiles = new Set(snapshot.files.map(f => f.path))

  // Also get files from historical snapshots that are within retention
  // by reading the remove actions from the transaction log
  const historicallyActiveFiles = await collectHistoricallyActiveFiles(
    storage,
    tablePath,
    retentionThreshold
  )

  // Combine active files with historically active files
  const protectedFiles = new Set([...activeFiles, ...historicallyActiveFiles])

  // List all parquet files in the table directory
  const allFiles = await listDataFiles(storage, tablePath)

  onProgress?.('scanning', 0, allFiles.length)

  const filesToDelete: FileInfo[] = []
  const filesToRetain: FileInfo[] = []
  const errors: string[] = []

  // Analyze each file
  for (let i = 0; i < allFiles.length; i++) {
    const file = allFiles[i]
    if (file === undefined) continue

    onProgress?.('scanning', i + 1, allFiles.length)

    // Skip files that are actively referenced
    if (protectedFiles.has(file.path)) {
      filesToRetain.push(file)
      continue
    }

    // Skip files within the retention period
    if (file.lastModified > retentionThreshold) {
      filesToRetain.push(file)
      continue
    }

    // This file is orphaned and older than retention - mark for deletion
    filesToDelete.push(file)
  }

  // Perform deletions (unless dry run)
  let filesDeleted = 0
  let bytesFreed = 0

  if (!dryRun && filesToDelete.length > 0) {
    onProgress?.('deleting', 0, filesToDelete.length)

    for (let i = 0; i < filesToDelete.length; i++) {
      const file = filesToDelete[i]
      if (file === undefined) continue

      try {
        await storage.delete(file.path)
        filesDeleted++
        bytesFreed += file.size
      } catch (error) {
        // Non-fatal: log the error but continue with other files
        errors.push(`Failed to delete ${file.path}: ${error}`)
      }

      onProgress?.('deleting', i + 1, filesToDelete.length)
    }
  } else if (dryRun) {
    // In dry run mode, calculate what would be deleted
    filesDeleted = filesToDelete.length
    bytesFreed = filesToDelete.reduce((sum, f) => sum + f.size, 0)
  }

  const durationMs = Date.now() - startTime

  const metrics: VacuumMetrics = {
    filesDeleted,
    bytesFreed,
    filesRetained: filesToRetain.length,
    dryRun,
    durationMs,
    filesScanned: allFiles.length,
  }

  if (dryRun) {
    metrics.filesToDelete = filesToDelete.map(f => f.path)
  }

  if (errors.length > 0) {
    metrics.errors = errors
  }

  return metrics
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * List all data files (parquet files) in the table directory.
 * Excludes the _delta_log directory.
 */
async function listDataFiles(
  storage: StorageBackend,
  tablePath: string
): Promise<FileInfo[]> {
  const allPaths = await storage.list(tablePath)
  const dataFiles: FileInfo[] = []

  for (const path of allPaths) {
    // Skip delta log files
    if (path.includes(`/${DELTA_LOG_DIR}/`) || path.includes(`${DELTA_LOG_DIR}/`)) {
      continue
    }

    // Only consider parquet files
    if (!path.endsWith('.parquet')) {
      continue
    }

    // Get file metadata
    const stat = await storage.stat(path)
    if (stat) {
      dataFiles.push({
        path,
        size: stat.size,
        lastModified: stat.lastModified,
      })
    }
  }

  return dataFiles
}

/**
 * Collect files that were removed but are still within the retention period.
 * These files should not be deleted during vacuum.
 *
 * This parses the transaction log to find remove actions and checks
 * if the deletion timestamp is within the retention threshold.
 */
async function collectHistoricallyActiveFiles(
  storage: StorageBackend,
  tablePath: string,
  retentionThreshold: Date
): Promise<Set<string>> {
  const historicallyActive = new Set<string>()
  const logPath = tablePath ? `${tablePath}/${DELTA_LOG_DIR}` : DELTA_LOG_DIR

  try {
    const logFiles = await storage.list(logPath)

    // Filter to JSON commit files only
    const commitFiles = logFiles
      .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
      .sort() // Sort by version (filename order)

    for (const commitFile of commitFiles) {
      try {
        const data = await storage.read(commitFile)
        const content = new TextDecoder().decode(data)
        const lines = content.split(/\r?\n/).filter(line => line.trim() !== '')

        for (const line of lines) {
          try {
            const parsed: unknown = JSON.parse(line)

            // Validate action structure
            if (!isValidDeltaAction(parsed)) {
              getLogger().warn(`[Vacuum] Skipping invalid action structure in ${commitFile}`)
              continue
            }

            // Check for remove actions
            if ('remove' in parsed) {
              const deletionTime = new Date(parsed.remove.deletionTimestamp)

              // If the file was removed within the retention period,
              // we need to keep it for time-travel queries
              if (deletionTime > retentionThreshold) {
                historicallyActive.add(parsed.remove.path)
              }
            }
          } catch (e) {
            // Skip malformed lines - log at debug level since this can happen with partial writes
            getLogger().warn(`[Vacuum] Skipping malformed action line in ${commitFile}:`, e)
          }
        }
      } catch (e) {
        // Skip unreadable commit files - log but continue processing other files
        getLogger().warn(`[Vacuum] Failed to read commit file ${commitFile}, skipping:`, e)
      }
    }
  } catch (e) {
    // If we can't read the log, return empty set (safe default)
    getLogger().warn(`[Vacuum] Failed to read transaction log at ${logPath}, returning empty historically active set:`, e)
  }

  return historicallyActive
}

/**
 * Utility function to format bytes as human-readable string.
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes'

  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

/**
 * Utility function to format duration as human-readable string.
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}
