/**
 * Delta Lake Transaction Log
 *
 * JSON-based transaction log format for ACID operations.
 *
 * This module provides:
 * - Action types (Add, Remove, Metadata, Protocol, CommitInfo)
 * - Transaction log utilities for parsing and serialization
 * - DeltaTable class for table operations
 * - Checkpoint management
 * - Concurrency control with optimistic locking
 * - Deletion vector support (soft-delete without rewriting Parquet files)
 *
 * @module delta
 */

import type { StorageBackend } from '../storage/index.js'
import { ConcurrencyError, type ConcurrencyErrorOptions, ValidationError } from '../errors.js'

// Re-export ConcurrencyError and ValidationError for backwards compatibility
export { ConcurrencyError, type ConcurrencyErrorOptions, ValidationError }

// Re-export query types and functions for convenience
export { matchesFilter } from '../query/index.js'

// Import shared utilities
import {
  formatVersion as formatVersionUtil,
  parseVersionFromFilename as parseVersionFromFilenameUtil,
  getLogFilePath as getLogFilePathUtil,
  VERSION_DIGITS as SHARED_VERSION_DIGITS,
  DELTA_LOG_DIR as SHARED_DELTA_LOG_DIR,
} from '../utils/index.js'

// =============================================================================
// RE-EXPORTS FROM SUBMODULES
// =============================================================================

// Types
export type {
  DeletionVectorDescriptor,
  AddAction,
  RemoveAction,
  MetadataAction,
  ProtocolAction,
  CommitInfoAction,
  DeltaAction,
  DeltaCommit,
  DeltaSnapshot,
  DeltaSchemaField,
  DeltaSchema,
  QueryOptions,
  WriteOptions,
  CheckpointConfig,
  CompactionContext,
  LastCheckpoint,
  FileStats,
  InferredFieldType,
  InferredField,
  InferredSchema,
  Filter,
  Projection,
} from './types.js'

// Validators
export {
  isValidDeltaSchemaField,
  isValidDeltaSchema,
  isValidLastCheckpoint,
  isValidPartitionValues,
  isValidFileStats,
  isAddAction,
  isRemoveAction,
  isMetadataAction,
  isProtocolAction,
  isCommitInfoAction,
  isValidAddAction,
  isValidRemoveAction,
  isValidMetadataAction,
  isValidProtocolAction,
  isValidCommitInfoAction,
  isValidDeltaAction,
} from './validators.js'

// Snapshot utilities
export {
  buildColumnMapping,
  applyColumnMapping,
} from './snapshot.js'

// Checkpoint utilities
export {
  LAST_CHECKPOINT_FILE,
  PARQUET_MAGIC,
  DEFAULT_CHECKPOINT_CONFIG,
  readLastCheckpoint,
  writeLastCheckpoint,
  readCheckpoint,
  readCheckpointPart,
  readMultiPartCheckpoint,
  rowsToSnapshot,
  writeSingleCheckpoint,
  createMultiPartCheckpoint,
  actionsToColumns,
  estimateCheckpointSize,
  discoverCheckpoints,
  findLatestCheckpoint,
  validateCheckpoint,
  cleanupCheckpoints,
  getCleanableLogVersions,
  cleanupLogs,
  shouldCheckpoint,
} from './checkpoint.js'

// Table class
export { DeltaTable } from './table.js'

// Import types for use in this file
import type { DeltaAction, AddAction, RemoveAction, FileStats, DeletionVectorDescriptor } from './types.js'
import { isValidDeltaAction, isValidFileStats, isValidDeltaSchema } from './validators.js'

// =============================================================================
// CONSTANTS (re-exported from utils for backward compatibility)
// =============================================================================

/** Number of digits in version filename (padded with zeros) */
export const VERSION_DIGITS = SHARED_VERSION_DIGITS

/** Delta log directory name */
export const DELTA_LOG_DIR = SHARED_DELTA_LOG_DIR

/** Recognized action type keys in Delta Lake format */
const ACTION_TYPES = ['add', 'remove', 'metaData', 'protocol', 'commitInfo'] as const

// =============================================================================
// DELETION VECTOR UTILITIES (re-exported from deletion-vectors.ts)
// =============================================================================

export {
  Z85_CHARS,
  Z85_DECODE,
  z85Decode,
  z85DecodeUuid,
  getDeletionVectorPath,
  parseRoaringBitmap,
  parseRoaringBitmap32,
  loadDeletionVector,
} from './deletion-vectors.js'

// =============================================================================
// VERSION FORMATTING UTILITY (delegates to shared utils)
// =============================================================================

/**
 * Format version number as 20-digit zero-padded string.
 *
 * This is the canonical utility for formatting Delta Lake version numbers.
 * Use this function across all modules that need to format version numbers
 * for file paths (commit logs, CDC files, parquet files, etc.).
 *
 * @param version - Version number (supports both number and bigint)
 * @returns Zero-padded version string (e.g., "00000000000000000042")
 * @throws Error if version is negative or exceeds maximum digits
 *
 * @example
 * ```typescript
 * formatVersion(0)   // "00000000000000000000"
 * formatVersion(42)  // "00000000000000000042"
 * formatVersion(1n)  // "00000000000000000001"
 * ```
 */
export function formatVersion(version: number | bigint): string {
  return formatVersionUtil(version)
}

// =============================================================================
// PARTITION VALUE EXTRACTION
// =============================================================================

/**
 * Extract partition values from a file path.
 *
 * Delta Lake uses Hive-style partitioning where partition values are encoded
 * in directory names as `column=value/`. This function parses such paths and
 * extracts partition column names and their values.
 *
 * @param filePath - The file path to parse (e.g., "letter=a/number=1/part-00000.parquet")
 * @returns Record of partition column names to their string values
 *
 * @example
 * ```typescript
 * extractPartitionValuesFromPath("letter=a/number=1/part-00000.parquet")
 * // Returns: { letter: "a", number: "1" }
 *
 * extractPartitionValuesFromPath("year=2024/month=01/day=15/data.parquet")
 * // Returns: { year: "2024", month: "01", day: "15" }
 *
 * extractPartitionValuesFromPath("data.parquet")
 * // Returns: {}
 * ```
 */
export function extractPartitionValuesFromPath(filePath: string): Record<string, string> {
  const partitions: Record<string, string> = {}
  const parts = filePath.split('/')

  for (const part of parts) {
    // Match Hive-style partition format: column=value
    const match = part.match(/^([^=]+)=(.+)$/)
    if (match) {
      const [, column, value] = match
      if (column && value !== undefined) {
        // Decode URL-encoded values (e.g., spaces, special characters)
        partitions[column] = decodeURIComponent(value)
      }
    }
  }

  return partitions
}

/**
 * Decode URL-encoded characters in a file path from the Delta log.
 *
 * Delta Lake stores partition paths with URL encoding (e.g., %20 for space, %3A for colon).
 * When reading from filesystem storage, the path needs to be decoded to match the actual
 * directory names on disk.
 *
 * Note: This performs a single decode pass. The Delta log may contain double-encoded
 * characters (e.g., %253A which decodes to %3A), and the filesystem often stores paths
 * with the single-decoded version.
 *
 * @param filePath - The URL-encoded file path from the Delta log
 * @returns The decoded file path suitable for filesystem access
 *
 * @example
 * ```typescript
 * decodeFilePath("bool=true/time=1970-01-01%2000%253A00%253A00/data.parquet")
 * // Returns: "bool=true/time=1970-01-01 00%3A00%3A00/data.parquet"
 * ```
 */
export function decodeFilePath(filePath: string): string {
  try {
    return decodeURIComponent(filePath)
  } catch {
    // If decoding fails (malformed encoding), return original path
    return filePath
  }
}

// =============================================================================
// TRANSACTION LOG UTILITIES
// =============================================================================

/**
 * Transaction log utilities for serialization, parsing, and validation
 */
export const transactionLog = {
  /**
   * Serialize a single action to JSON
   */
  serializeAction(action: DeltaAction): string {
    return JSON.stringify(action)
  },

  /**
   * Parse a single action from JSON
   *
   * @param json - JSON string containing a single Delta action
   * @returns Parsed DeltaAction object
   * @throws Error if JSON is empty, invalid, or missing a recognized action type
   */
  parseAction(json: string): DeltaAction {
    if (!json || json.trim() === '') {
      throw new ValidationError('Cannot parse empty JSON string', 'json', json)
    }

    const parsed: unknown = JSON.parse(json)

    // Validate that it's an object
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new ValidationError('Action must be a JSON object', 'json', typeof parsed)
    }

    // Validate that it has a recognized action type using the constant
    const hasValidAction = ACTION_TYPES.some(type => type in parsed)

    if (!hasValidAction) {
      throw new ValidationError('JSON must contain a recognized action type', 'json', Object.keys(parsed as object))
    }

    // Use type guard to validate the complete action structure
    if (!isValidDeltaAction(parsed)) {
      throw new ValidationError('Invalid Delta action structure: missing or invalid required fields', 'json', parsed)
    }

    return parsed
  },

  /**
   * Serialize multiple actions to NDJSON format
   */
  serializeCommit(actions: DeltaAction[]): string {
    return actions.map(action => this.serializeAction(action)).join('\n')
  },

  /**
   * Parse NDJSON content to actions
   */
  parseCommit(content: string): DeltaAction[] {
    const lines = content.split(/\r?\n/).filter(line => line.trim() !== '')
    return lines.map(line => this.parseAction(line))
  },

  /**
   * Format version number as 20-digit zero-padded string
   *
   * @param version - Version number (supports both number and bigint)
   * @returns Zero-padded version string
   * @throws Error if version is negative or exceeds maximum digits
   * @see formatVersion - The standalone function this delegates to
   */
  formatVersion(version: number | bigint): string {
    return formatVersion(version)
  },

  /**
   * Parse version number from filename
   *
   * @param filename - Log filename (can include path)
   * @returns Parsed version number
   * @throws Error if filename doesn't match expected format
   * @see parseVersionFromFilenameUtil - The shared function this delegates to
   */
  parseVersionFromFilename(filename: string): number {
    return parseVersionFromFilenameUtil(filename)
  },

  /**
   * Get the log file path for a version
   *
   * @param tablePath - Base path of the Delta table
   * @param version - Version number
   * @returns Full path to the log file
   * @see getLogFilePathUtil - The shared function this delegates to
   */
  getLogFilePath(tablePath: string, version: number): string {
    return getLogFilePathUtil(tablePath, version)
  },

  /**
   * Validate an action structure
   */
  validateAction(action: DeltaAction): { valid: boolean; errors: string[] } {
    const errors: string[] = []

    if ('add' in action) {
      const add = action.add

      if (!add.path || add.path === '') {
        errors.push('add.path must not be empty')
      }

      if (add.size < 0) {
        errors.push('add.size must be non-negative')
      }

      if (add.modificationTime < 0) {
        errors.push('add.modificationTime must be non-negative')
      }

      if (add.stats !== undefined) {
        try {
          const parsed: unknown = JSON.parse(add.stats)
          // Accept empty objects as valid stats (for minimal metadata)
          const isEmpty = typeof parsed === 'object' && parsed !== null && Object.keys(parsed).length === 0
          if (!isEmpty && !isValidFileStats(parsed)) {
            errors.push('add.stats must be a valid FileStats object')
          }
        } catch {
          // Intentionally silent: validation catch - error is captured in errors array
          errors.push('add.stats must be valid JSON')
        }
      }
    } else if ('remove' in action) {
      const remove = action.remove

      if (!remove.path || remove.path === '') {
        errors.push('remove.path must not be empty')
      }

      if (remove.deletionTimestamp < 0) {
        errors.push('remove.deletionTimestamp must be non-negative')
      }
    } else if ('metaData' in action) {
      const metaData = action.metaData

      if (!metaData.id || metaData.id === '') {
        errors.push('metaData.id must not be empty')
      }

      if (!metaData.format.provider || metaData.format.provider === '') {
        errors.push('metaData.format.provider must not be empty')
      }

      if (metaData.schemaString !== undefined) {
        try {
          const parsed: unknown = JSON.parse(metaData.schemaString)
          // Accept empty objects as valid schema (for initial/minimal metadata)
          const isEmpty = typeof parsed === 'object' && parsed !== null && Object.keys(parsed).length === 0
          if (!isEmpty && !isValidDeltaSchema(parsed)) {
            errors.push('metaData.schemaString must be a valid DeltaSchema object')
          }
        } catch {
          // Intentionally silent: validation catch - error is captured in errors array
          errors.push('metaData.schemaString must be valid JSON')
        }
      }
    } else if ('protocol' in action) {
      const protocol = action.protocol

      if (protocol.minReaderVersion < 1) {
        errors.push('protocol.minReaderVersion must be at least 1')
      }

      if (protocol.minWriterVersion < 1) {
        errors.push('protocol.minWriterVersion must be at least 1')
      }

      if (!Number.isInteger(protocol.minReaderVersion)) {
        errors.push('protocol.minReaderVersion must be an integer')
      }

      if (!Number.isInteger(protocol.minWriterVersion)) {
        errors.push('protocol.minWriterVersion must be an integer')
      }
    } else if ('commitInfo' in action) {
      const commitInfo = action.commitInfo

      if (commitInfo.timestamp < 0) {
        errors.push('commitInfo.timestamp must be non-negative')
      }

      if (!commitInfo.operation || commitInfo.operation === '') {
        errors.push('commitInfo.operation must not be empty')
      }

      if (commitInfo.readVersion !== undefined && commitInfo.readVersion < 0) {
        errors.push('commitInfo.readVersion must be non-negative')
      }
    }

    return {
      valid: errors.length === 0,
      errors,
    }
  },
}

// =============================================================================
// ACTION CREATION FUNCTIONS
// =============================================================================

/**
 * Create an AddAction with validation
 */
export function createAddAction(params: {
  path: string
  size: number
  modificationTime: number
  dataChange: boolean
  partitionValues?: Record<string, string>
  stats?: FileStats
  tags?: Record<string, string>
}): AddAction {
  // Validate path
  if (params.path.startsWith('/')) {
    throw new ValidationError('path must be relative', 'path', params.path)
  }
  if (params.path.includes('../')) {
    throw new ValidationError('path cannot contain parent directory traversal', 'path', params.path)
  }
  if (params.path.startsWith('./')) {
    throw new ValidationError('path should not start with ./', 'path', params.path)
  }

  // Validate size
  if (!Number.isInteger(params.size)) {
    throw new ValidationError('size must be an integer', 'size', params.size)
  }
  if (params.size > Number.MAX_SAFE_INTEGER) {
    throw new ValidationError('size exceeds maximum safe integer', 'size', params.size)
  }

  // Validate modificationTime
  if (!Number.isInteger(params.modificationTime)) {
    throw new ValidationError('modificationTime must be an integer', 'modificationTime', params.modificationTime)
  }

  // Validate stats if provided
  if (params.stats) {
    if (params.stats.numRecords < 0) {
      throw new ValidationError('numRecords must be non-negative', 'stats.numRecords', params.stats.numRecords)
    }
    for (const [key, value] of Object.entries(params.stats.nullCount)) {
      if (value < 0) {
        throw new ValidationError('nullCount values must be non-negative', `stats.nullCount.${key}`, value)
      }
      if (value > params.stats.numRecords) {
        throw new ValidationError('nullCount cannot exceed numRecords', `stats.nullCount.${key}`, { value, numRecords: params.stats.numRecords })
      }
    }
  }

  const action: AddAction = {
    add: {
      path: params.path,
      size: params.size,
      modificationTime: params.modificationTime,
      dataChange: params.dataChange,
    },
  }

  if (params.partitionValues !== undefined) {
    action.add.partitionValues = params.partitionValues
  }

  if (params.stats !== undefined) {
    action.add.stats = encodeStats(params.stats)
  }

  if (params.tags !== undefined) {
    action.add.tags = params.tags
  }

  return action
}

/**
 * Create a RemoveAction with validation
 */
export function createRemoveAction(params: {
  path: string
  deletionTimestamp: number
  dataChange: boolean
  partitionValues?: Record<string, string>
  extendedFileMetadata?: boolean
  size?: number
}): RemoveAction {
  const action: RemoveAction = {
    remove: {
      path: params.path,
      deletionTimestamp: params.deletionTimestamp,
      dataChange: params.dataChange,
    },
  }

  if (params.partitionValues !== undefined) {
    action.remove.partitionValues = params.partitionValues
  }

  if (params.extendedFileMetadata !== undefined) {
    action.remove.extendedFileMetadata = params.extendedFileMetadata
  }

  if (params.size !== undefined) {
    action.remove.size = params.size
  }

  return action
}

// =============================================================================
// SERIALIZATION
// =============================================================================

/**
 * Serialize action to JSON string (single line for Delta log)
 */
export function serializeAction(action: DeltaAction): string {
  return JSON.stringify(action)
}

/**
 * Deserialize JSON string to DeltaAction
 */
export function deserializeAction(json: string): DeltaAction {
  const parsed: unknown = JSON.parse(json)
  if (!isValidDeltaAction(parsed)) {
    throw new ValidationError('Invalid Delta action structure', 'json', parsed)
  }
  return parsed
}

// =============================================================================
// VALIDATION
// =============================================================================

/**
 * Validate an AddAction
 */
export function validateAddAction(action: AddAction): { valid: boolean; errors: string[] } {
  const errors: string[] = []

  if (action.add.path === undefined || action.add.path === null) {
    errors.push('path is required')
  } else if (action.add.path === '') {
    errors.push('path cannot be empty')
  }

  if (typeof action.add.size !== 'number') {
    errors.push('size must be a number')
  } else if (action.add.size < 0) {
    errors.push('size must be non-negative')
  }

  if (action.add.modificationTime === undefined || action.add.modificationTime === null) {
    errors.push('modificationTime is required')
  } else if (action.add.modificationTime < 0) {
    errors.push('modificationTime must be non-negative')
  }

  if (typeof action.add.dataChange !== 'boolean') {
    errors.push('dataChange must be a boolean')
  }

  if (action.add.stats) {
    try {
      const parsed: unknown = JSON.parse(action.add.stats)
      // Accept empty objects as valid stats (for minimal metadata)
      const isEmpty = typeof parsed === 'object' && parsed !== null && Object.keys(parsed).length === 0
      if (!isEmpty && !isValidFileStats(parsed)) {
        errors.push('stats must be a valid FileStats object')
      }
    } catch {
      // Intentionally silent: validation catch - error is captured in errors array
      errors.push('stats must be valid JSON')
    }
  }

  // Validate partition value consistency
  if (action.add.partitionValues && action.add.path) {
    for (const [key, value] of Object.entries(action.add.partitionValues)) {
      const partPattern = `${key}=${value}`
      if (!action.add.path.includes(partPattern) && value !== '') {
        errors.push(`partition value mismatch: expected ${partPattern} in path`)
      }
    }
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Validate a RemoveAction
 */
export function validateRemoveAction(action: RemoveAction): { valid: boolean; errors: string[] } {
  const errors: string[] = []

  if (action.remove.path === undefined || action.remove.path === null) {
    errors.push('path is required')
  } else if (action.remove.path === '') {
    errors.push('path cannot be empty')
  }

  if (action.remove.deletionTimestamp === undefined || action.remove.deletionTimestamp === null) {
    errors.push('deletionTimestamp is required')
  } else if (action.remove.deletionTimestamp < 0) {
    errors.push('deletionTimestamp must be non-negative')
  }

  if (typeof action.remove.dataChange !== 'boolean') {
    errors.push('dataChange must be a boolean')
  }

  return { valid: errors.length === 0, errors }
}

// =============================================================================
// STATS PARSING
// =============================================================================

/**
 * Parse stats JSON string from AddAction
 */
export function parseStats(statsJson: string): FileStats {
  const parsed: unknown = JSON.parse(statsJson)

  if (!isValidFileStats(parsed)) {
    throw new ValidationError('Invalid file stats format', 'FileStats', parsed)
  }

  return parsed
}

/**
 * Encode stats object to JSON string for AddAction
 */
export function encodeStats(stats: FileStats): string {
  return JSON.stringify(stats)
}

// =============================================================================
// RETRY EXPORTS
// =============================================================================

export {
  withRetry,
  isRetryableError,
  AbortError,
  DEFAULT_RETRY_CONFIG,
  type RetryConfig,
  type RetryMetrics,
  type RetryInfo,
  type SuccessInfo,
  type FailureInfo,
  type RetryResultWithMetrics,
} from './retry.js'

// =============================================================================
// VACUUM EXPORTS
// =============================================================================

export {
  vacuum,
  formatBytes,
  formatDuration,
  type VacuumConfig,
  type VacuumMetrics,
} from './vacuum.js'
