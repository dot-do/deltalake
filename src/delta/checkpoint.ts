/**
 * Delta Lake Checkpoint Management
 *
 * This module provides functions for reading and writing Delta Lake checkpoints.
 * Checkpoints are Parquet files that contain a snapshot of the table state,
 * allowing faster snapshot reconstruction.
 *
 * @module delta/checkpoint
 */

import type { StorageBackend } from '../storage/index.js'
import type {
  DeltaSnapshot,
  AddAction,
  MetadataAction,
  ProtocolAction,
  LastCheckpoint,
  CheckpointConfig,
} from './types.js'
import {
  isValidLastCheckpoint,
  isValidAddAction,
  isValidMetadataAction,
  isValidProtocolAction,
} from './validators.js'
import { ValidationError, FileNotFoundError } from '../errors.js'
import { formatVersion, readParquetFromBuffer, getLogger } from '../utils/index.js'
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'

// =============================================================================
// CONSTANTS
// =============================================================================

/** Last checkpoint file name */
export const LAST_CHECKPOINT_FILE = '_last_checkpoint'

/** Parquet magic bytes for validation: "PAR1" */
export const PARQUET_MAGIC = [0x50, 0x41, 0x52, 0x31] as const

/** Default checkpoint configuration values */
export const DEFAULT_CHECKPOINT_CONFIG: Required<Omit<CheckpointConfig, 'checkpointRetentionMs' | 'maxCheckpointSizeBytes'>> & Pick<CheckpointConfig, 'checkpointRetentionMs' | 'maxCheckpointSizeBytes'> = {
  checkpointInterval: 10,
  maxActionsPerCheckpoint: 1000000,
  numRetainedCheckpoints: 10,
  checkpointRetentionMs: undefined,
  maxCheckpointSizeBytes: undefined,
} as const

// =============================================================================
// CHECKPOINT READING
// =============================================================================

/**
 * Read the _last_checkpoint file
 */
export async function readLastCheckpoint(
  storage: StorageBackend,
  logPath: string
): Promise<LastCheckpoint | null> {
  const lastCheckpointPath = `${logPath}/${LAST_CHECKPOINT_FILE}`

  try {
    const data = await storage.read(lastCheckpointPath)
    const json = new TextDecoder().decode(data)
    const parsed: unknown = JSON.parse(json)
    if (!isValidLastCheckpoint(parsed)) {
      throw new ValidationError('Invalid _last_checkpoint format', 'LastCheckpoint', parsed)
    }
    return parsed
  } catch (e) {
    // Fall back to scanning directory for checkpoint files
    // Note: _last_checkpoint file may not exist for new tables
    getLogger().warn(`[checkpoint] Failed to read _last_checkpoint file, falling back to directory scan:`, e)
    try {
      const files = await storage.list(logPath)
      const checkpoints = files
        .filter(f => f.includes('.checkpoint.'))
        .map(f => {
          const match = f.match(/(\d+)\.checkpoint\.parquet$/)
          return match && match[1] !== undefined ? parseInt(match[1], 10) : null
        })
        .filter((v): v is number => v !== null)
        .sort((a, b) => b - a)

      const latestVersion = checkpoints[0]
      if (latestVersion !== undefined) {
        return { version: latestVersion, size: 0 }
      }
    } catch (scanError) {
      // Directory scan also failed - log but return null (no checkpoint)
      getLogger().warn(`[checkpoint] Directory scan for checkpoints also failed:`, scanError)
    }
    return null
  }
}

/**
 * Write the _last_checkpoint file
 */
export async function writeLastCheckpoint(
  storage: StorageBackend,
  logPath: string,
  checkpoint: LastCheckpoint
): Promise<void> {
  const lastCheckpointPath = `${logPath}/${LAST_CHECKPOINT_FILE}`
  const data = JSON.stringify(checkpoint)
  await storage.write(lastCheckpointPath, new TextEncoder().encode(data))
}

/**
 * Read a checkpoint at the specified version
 */
export async function readCheckpoint(
  storage: StorageBackend,
  logPath: string,
  version: number,
  lastCheckpoint?: LastCheckpoint | null
): Promise<DeltaSnapshot> {
  // Check for multi-part checkpoint
  if (lastCheckpoint && lastCheckpoint.version === version && lastCheckpoint.parts) {
    return readMultiPartCheckpoint(storage, logPath, version, lastCheckpoint.parts)
  }

  // Single-part checkpoint
  const checkpointPath = `${logPath}/${formatVersion(version)}.checkpoint.parquet`

  if (!(await storage.exists(checkpointPath))) {
    throw new FileNotFoundError(checkpointPath, 'readCheckpoint')
  }

  // Read the entire file into memory using shared utility
  const data = await storage.read(checkpointPath)
  const rows = await readParquetFromBuffer<Record<string, unknown>>(data)

  return rowsToSnapshot(rows, version)
}

/**
 * Read a specific part of a multi-part checkpoint
 */
export async function readCheckpointPart(
  storage: StorageBackend,
  logPath: string,
  version: number,
  part: number,
  totalParts: number
): Promise<DeltaSnapshot> {
  const checkpointPath = `${logPath}/${formatVersion(version)}.checkpoint.${part}.${totalParts}.parquet`

  if (!(await storage.exists(checkpointPath))) {
    throw new FileNotFoundError(checkpointPath, 'readCheckpointPart')
  }

  // Read the entire file into memory using shared utility
  const data = await storage.read(checkpointPath)
  const rows = await readParquetFromBuffer<Record<string, unknown>>(data)

  return rowsToSnapshot(rows, version)
}

/**
 * Read all parts of a multi-part checkpoint
 */
export async function readMultiPartCheckpoint(
  storage: StorageBackend,
  logPath: string,
  version: number,
  totalParts: number
): Promise<DeltaSnapshot> {
  const allFiles: AddAction['add'][] = []
  let metadata: MetadataAction['metaData'] | undefined
  let protocol: ProtocolAction['protocol'] | undefined

  for (let part = 1; part <= totalParts; part++) {
    const partSnapshot = await readCheckpointPart(storage, logPath, version, part, totalParts)
    allFiles.push(...partSnapshot.files)
    if (partSnapshot.metadata) metadata = partSnapshot.metadata
    if (partSnapshot.protocol) protocol = partSnapshot.protocol
  }

  return { version, files: allFiles, metadata, protocol }
}

/**
 * Convert checkpoint rows to DeltaSnapshot
 */
export function rowsToSnapshot(rows: Record<string, unknown>[], version: number): DeltaSnapshot {
  const files: AddAction['add'][] = []
  let metadata: MetadataAction['metaData'] | undefined
  let protocol: ProtocolAction['protocol'] | undefined

  for (const row of rows) {
    // Parse JSON strings back to objects with validation
    if (row.add && typeof row.add === 'string' && row.add !== '') {
      const parsed: unknown = JSON.parse(row.add)
      if (!isValidAddAction(parsed)) {
        throw new ValidationError('Invalid add action in checkpoint', 'add', parsed)
      }
      files.push(parsed)
    }
    if (row.metaData && typeof row.metaData === 'string' && row.metaData !== '') {
      const parsed: unknown = JSON.parse(row.metaData)
      if (!isValidMetadataAction(parsed)) {
        throw new ValidationError('Invalid metadata action in checkpoint', 'metaData', parsed)
      }
      metadata = parsed
    }
    if (row.protocol && typeof row.protocol === 'string' && row.protocol !== '') {
      const parsed: unknown = JSON.parse(row.protocol)
      if (!isValidProtocolAction(parsed)) {
        throw new ValidationError('Invalid protocol action in checkpoint', 'protocol', parsed)
      }
      protocol = parsed
    }
  }

  return { version, files, metadata, protocol }
}

// =============================================================================
// CHECKPOINT WRITING
// =============================================================================

/**
 * Write a single-part checkpoint
 */
export async function writeSingleCheckpoint(
  storage: StorageBackend,
  logPath: string,
  version: number,
  actions: Record<string, unknown>[]
): Promise<void> {
  const checkpointPath = `${logPath}/${formatVersion(version)}.checkpoint.parquet`

  // Convert actions to column format for hyparquet-writer
  const columnData = actionsToColumns(actions)

  // Write parquet file
  const buffer = parquetWriteBuffer({
    columnData,
    codec: 'UNCOMPRESSED',
  })

  await storage.write(checkpointPath, new Uint8Array(buffer))
}

/**
 * Create a multi-part checkpoint
 */
export async function createMultiPartCheckpoint(
  storage: StorageBackend,
  logPath: string,
  version: number,
  actions: Record<string, unknown>[],
  config: CheckpointConfig
): Promise<number> {
  const maxActions = config.maxActionsPerCheckpoint ?? DEFAULT_CHECKPOINT_CONFIG.maxActionsPerCheckpoint!
  const maxSize = config.maxCheckpointSizeBytes
  const parts: Record<string, unknown>[][] = []

  if (maxSize) {
    // Split by size
    let currentPart: Record<string, unknown>[] = []
    let currentSize = 0

    for (const action of actions) {
      const actionSize = JSON.stringify(action).length

      // Start new part if adding this action would exceed size
      if (currentPart.length > 0 && currentSize + actionSize > maxSize) {
        parts.push(currentPart)
        currentPart = [action]
        currentSize = actionSize
      } else {
        currentPart.push(action)
        currentSize += actionSize
      }
    }

    if (currentPart.length > 0) {
      parts.push(currentPart)
    }
  } else {
    // Split by action count
    for (let i = 0; i < actions.length; i += maxActions) {
      parts.push(actions.slice(i, i + maxActions))
    }
  }

  const totalParts = parts.length

  // Write each part
  for (let i = 0; i < parts.length; i++) {
    const partNum = i + 1
    const checkpointPath = `${logPath}/${formatVersion(version)}.checkpoint.${partNum}.${totalParts}.parquet`

    const partActions = parts[i]
    if (partActions === undefined) continue
    const columnData = actionsToColumns(partActions)
    const buffer = parquetWriteBuffer({
      columnData,
      codec: 'UNCOMPRESSED',
    })

    await storage.write(checkpointPath, new Uint8Array(buffer))
  }

  return totalParts
}

/**
 * Convert actions to column format for parquet writing
 */
export function actionsToColumns(actions: Record<string, unknown>[]): Array<{ name: string; data: unknown[] }> {
  // Serialize each action type as JSON column
  const addData: string[] = []
  const removeData: string[] = []
  const metaDataData: string[] = []
  const protocolData: string[] = []

  for (const action of actions) {
    if (action.add) {
      addData.push(JSON.stringify(action.add))
    } else {
      addData.push('')
    }

    if (action.remove) {
      removeData.push(JSON.stringify(action.remove))
    } else {
      removeData.push('')
    }

    if (action.metaData) {
      metaDataData.push(JSON.stringify(action.metaData))
    } else {
      metaDataData.push('')
    }

    if (action.protocol) {
      protocolData.push(JSON.stringify(action.protocol))
    } else {
      protocolData.push('')
    }
  }

  return [
    { name: 'add', data: addData },
    { name: 'remove', data: removeData },
    { name: 'metaData', data: metaDataData },
    { name: 'protocol', data: protocolData },
  ]
}

/**
 * Estimate checkpoint size in bytes
 */
export function estimateCheckpointSize(actions: Record<string, unknown>[]): number {
  // Rough estimate: JSON size * 0.5 (parquet compression)
  const jsonSize = JSON.stringify(actions).length
  return Math.floor(jsonSize * 0.5)
}

// =============================================================================
// CHECKPOINT DISCOVERY AND VALIDATION
// =============================================================================

/**
 * Discover all checkpoint versions
 */
export async function discoverCheckpoints(
  storage: StorageBackend,
  logPath: string
): Promise<number[]> {
  const files = await storage.list(logPath)
  const checkpoints = files
    .filter(f => f.includes('.checkpoint.'))
    .map(f => {
      const match = f.match(/(\d+)\.checkpoint/)
      return match && match[1] !== undefined ? parseInt(match[1], 10) : null
    })
    .filter((v): v is number => v !== null)

  // Remove duplicates and sort descending
  return [...new Set(checkpoints)].sort((a, b) => b - a)
}

/**
 * Find the latest checkpoint version
 */
export async function findLatestCheckpoint(
  storage: StorageBackend,
  logPath: string
): Promise<number | null> {
  const checkpoints = await discoverCheckpoints(storage, logPath)
  return checkpoints[0] ?? null
}

/**
 * Validate a checkpoint file
 */
export async function validateCheckpoint(
  storage: StorageBackend,
  logPath: string,
  version: number
): Promise<boolean> {
  try {
    const checkpointPath = `${logPath}/${formatVersion(version)}.checkpoint.parquet`

    if (!(await storage.exists(checkpointPath))) {
      return false
    }

    // Try to read the checkpoint
    const data = await storage.read(checkpointPath)

    // Check for Parquet magic bytes at header
    if (data.length < PARQUET_MAGIC.length) return false
    for (let i = 0; i < PARQUET_MAGIC.length; i++) {
      if (data[i] !== PARQUET_MAGIC[i]) return false
    }

    // Check footer magic (same magic bytes at end of file)
    if (data.length < PARQUET_MAGIC.length * 2) return false
    const footerStart = data.length - PARQUET_MAGIC.length
    for (let i = 0; i < PARQUET_MAGIC.length; i++) {
      if (data[footerStart + i] !== PARQUET_MAGIC[i]) return false
    }

    return true
  } catch (e) {
    // File read failed or is not a valid Parquet file
    getLogger().warn(`[checkpoint] Failed to validate checkpoint at version ${version}:`, e)
    return false
  }
}

// =============================================================================
// CHECKPOINT CLEANUP
// =============================================================================

/**
 * Clean up old checkpoints based on retention policy
 */
export async function cleanupCheckpoints(
  storage: StorageBackend,
  logPath: string,
  config: CheckpointConfig,
  checkpointTimestamps: Map<number, number>
): Promise<void> {
  const checkpoints = await discoverCheckpoints(storage, logPath)
  const numRetained = config.numRetainedCheckpoints ?? DEFAULT_CHECKPOINT_CONFIG.numRetainedCheckpoints!
  const retentionMs = config.checkpointRetentionMs

  // Determine which checkpoints to delete
  const toDelete: number[] = []

  for (let i = 0; i < checkpoints.length; i++) {
    const version = checkpoints[i]
    if (version === undefined) continue
    let shouldDelete = false

    // Delete if beyond retention count (but always keep at least one)
    if (i >= numRetained && checkpoints.length > 1) {
      shouldDelete = true
    }

    // Delete if beyond retention time
    if (retentionMs) {
      const timestamp = checkpointTimestamps.get(version)
      if (timestamp && Date.now() - timestamp >= retentionMs) {
        // Only delete if we'll still have at least one checkpoint
        if (checkpoints.length - toDelete.length > 1 || i > 0) {
          shouldDelete = true
        }
      }
    }

    if (shouldDelete) {
      toDelete.push(version)
    }
  }

  // Always keep at least one checkpoint
  if (toDelete.length >= checkpoints.length) {
    toDelete.pop()
  }

  // Delete the checkpoints
  for (const version of toDelete) {
    const files = await storage.list(logPath)
    const checkpointFiles = files.filter(f =>
      f.includes(`${formatVersion(version)}.checkpoint`)
    )

    for (const file of checkpointFiles) {
      try {
        await storage.delete(file)
      } catch (e) {
        // Non-fatal: checkpoint deletion failed, but operation can continue
        getLogger().warn(`[checkpoint] Failed to delete checkpoint file ${file}:`, e)
      }
    }

    checkpointTimestamps.delete(version)
  }
}

/**
 * Get log versions that can be cleaned up (older than oldest checkpoint)
 */
export async function getCleanableLogVersions(
  storage: StorageBackend,
  logPath: string
): Promise<number[]> {
  const checkpoints = await discoverCheckpoints(storage, logPath)
  if (checkpoints.length === 0) return []

  const oldestCheckpoint = checkpoints[checkpoints.length - 1]
  if (oldestCheckpoint === undefined) return []
  const cleanable: number[] = []

  for (let v = 0; v < oldestCheckpoint; v++) {
    cleanable.push(v)
  }

  return cleanable
}

/**
 * Clean up old log files (older than oldest checkpoint)
 */
export async function cleanupLogs(
  storage: StorageBackend,
  logPath: string
): Promise<void> {
  const cleanable = await getCleanableLogVersions(storage, logPath)

  for (const version of cleanable) {
    const logFilePath = `${logPath}/${formatVersion(version)}.json`
    try {
      if (await storage.exists(logFilePath)) {
        await storage.delete(logFilePath)
      }
    } catch (e) {
      // Non-fatal: log deletion failed, but operation can continue
      getLogger().warn(`[checkpoint] Failed to delete log file ${logFilePath}:`, e)
    }
  }
}

/**
 * Determine if a checkpoint should be created at this version
 */
export function shouldCheckpoint(version: number, config: CheckpointConfig): boolean {
  const interval = config.checkpointInterval ?? DEFAULT_CHECKPOINT_CONFIG.checkpointInterval!
  return (version + 1) % interval === 0
}
