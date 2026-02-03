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
// Z85 ENCODING/DECODING (for Deletion Vectors)
// =============================================================================

/**
 * Z85 character set used by Delta Lake for encoding deletion vector UUIDs.
 * Z85 is a Base85 variant that is JSON-friendly (no quotes or backslashes).
 * @see https://rfc.zeromq.org/spec:32/Z85/
 */
const Z85_CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#'

/**
 * Decode table for Z85: maps character code to value (0-84)
 */
const Z85_DECODE: number[] = new Array(128).fill(-1)
for (let i = 0; i < Z85_CHARS.length; i++) {
  Z85_DECODE[Z85_CHARS.charCodeAt(i)] = i
}

/**
 * Decode a Z85-encoded string to bytes.
 *
 * Z85 encodes 4 bytes as 5 ASCII characters. The input length must be a multiple of 5.
 *
 * @param encoded - Z85-encoded string
 * @returns Decoded bytes as Uint8Array
 * @throws Error if input length is not a multiple of 5 or contains invalid characters
 */
export function z85Decode(encoded: string): Uint8Array {
  if (encoded.length % 5 !== 0) {
    throw new ValidationError(`Z85 input length must be a multiple of 5, got ${encoded.length}`, 'encoded', encoded.length)
  }

  const outputLen = (encoded.length / 5) * 4
  const result = new Uint8Array(outputLen)
  let outIdx = 0

  for (let i = 0; i < encoded.length; i += 5) {
    // Decode 5 chars to a 32-bit value
    let value = 0
    for (let j = 0; j < 5; j++) {
      const charCode = encoded.charCodeAt(i + j)
      const decoded = charCode < 128 ? Z85_DECODE[charCode] : -1
      if (decoded === undefined || decoded < 0) {
        throw new ValidationError(`Invalid Z85 character at position ${i + j}`, 'encoded', encoded.charAt(i + j))
      }
      value = value * 85 + decoded
    }

    // Write 4 bytes (big-endian)
    result[outIdx++] = (value >>> 24) & 0xff
    result[outIdx++] = (value >>> 16) & 0xff
    result[outIdx++] = (value >>> 8) & 0xff
    result[outIdx++] = value & 0xff
  }

  return result
}

/**
 * Convert a Z85-encoded UUID string to a UUID string format.
 *
 * Delta Lake uses Z85 to encode 16-byte UUIDs as 20-character strings.
 * The pathOrInlineDv field may have an optional prefix before the UUID.
 *
 * @param pathOrInlineDv - The pathOrInlineDv field from a deletion vector descriptor
 * @returns The UUID in standard format (8-4-4-4-12 hex)
 */
export function z85DecodeUuid(pathOrInlineDv: string): string {
  // The last 20 characters are the Z85-encoded UUID
  // Any characters before that are an optional random prefix
  const uuidEncoded = pathOrInlineDv.slice(-20)
  const prefix = pathOrInlineDv.slice(0, -20)

  const bytes = z85Decode(uuidEncoded)

  // Convert 16 bytes to UUID format
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
  const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`

  return prefix ? `${prefix}${uuid}` : uuid
}

/**
 * Get the file path for a deletion vector based on its descriptor.
 *
 * @param tablePath - Base path of the Delta table
 * @param dv - Deletion vector descriptor
 * @returns Full path to the deletion vector file
 */
export function getDeletionVectorPath(tablePath: string, dv: DeletionVectorDescriptor): string {
  if (dv.storageType === 'p') {
    // Absolute path
    return dv.pathOrInlineDv
  } else if (dv.storageType === 'u') {
    // UUID-based relative path
    const uuid = z85DecodeUuid(dv.pathOrInlineDv)
    // The prefix from the pathOrInlineDv becomes part of the filename
    const prefix = dv.pathOrInlineDv.length > 20 ? dv.pathOrInlineDv.slice(0, -20) : ''
    const filename = `deletion_vector_${uuid}.bin`
    return tablePath ? `${tablePath}/${filename}` : filename
  } else {
    // Inline - no file path
    throw new ValidationError('Inline deletion vectors do not have a file path', 'storageType', dv.storageType)
  }
}

// =============================================================================
// ROARING BITMAP PARSING (for Deletion Vectors)
// =============================================================================

/**
 * Roaring Bitmap serialization format constants.
 * @see https://github.com/RoaringBitmap/RoaringFormatSpec
 */
const SERIAL_COOKIE_NO_RUNCONTAINER = 12346
const SERIAL_COOKIE = 12347

/**
 * Parse a serialized RoaringBitmap (or RoaringTreemap for 64-bit) and return the set of deleted row indices.
 *
 * Delta Lake uses RoaringTreemap (64-bit extension) for deletion vectors.
 * The format is:
 * - Number of 32-bit buckets (as uint64, little-endian)
 * - For each bucket:
 *   - High 32 bits key (as uint32, little-endian)
 *   - Serialized 32-bit RoaringBitmap for low 32 bits
 *
 * @param data - Raw bytes of the serialized deletion vector (after magic/size/checksum header)
 * @returns Set of row indices that are marked as deleted
 */
export function parseRoaringBitmap(data: Uint8Array): Set<number> {
  const deletedRows = new Set<number>()

  // Handle empty or insufficient data
  if (!data || data.byteLength < 4) {
    return deletedRows
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = 0

  // Delta Lake uses a magic number at the start: 0x64 0x01 0x00 0x00 (little-endian: 0x164 = 356 for DeletionVectorStorageType)
  // Actually, looking at the data: the magic number is 4 bytes at the start
  // Format: magic (4 bytes) + roaring treemap data
  const magic = view.getUint32(offset, true)
  offset += 4

  // Magic number 0x00000164 (356) indicates RoaringTreemap format
  // Magic number 0x3a300000 would be something else
  if (magic !== 0x64 && magic !== 0x3a300000 && magic !== 0x303a) {
    // Try parsing without magic - might be a different format
    offset = 0
  }

  // For RoaringTreemap: first 8 bytes are the number of buckets
  // Need at least 8 bytes for the bucket count
  if (offset + 8 > data.byteLength) {
    return deletedRows
  }
  const numBuckets = Number(view.getBigUint64(offset, true))
  offset += 8

  // Sanity check: avoid processing unreasonable number of buckets
  // which could indicate malformed data
  const maxReasonableBuckets = 1000000
  if (numBuckets > maxReasonableBuckets || numBuckets < 0) {
    return deletedRows
  }

  for (let bucket = 0; bucket < numBuckets; bucket++) {
    // Need at least 4 bytes for the high bits key
    if (offset + 4 > data.byteLength) {
      break
    }

    // High 32 bits key
    const highBits = view.getUint32(offset, true)
    offset += 4

    // Parse the 32-bit RoaringBitmap for this bucket
    const remaining = data.subarray(offset)
    if (remaining.byteLength === 0) {
      break
    }
    const { values, bytesRead } = parseRoaringBitmap32(remaining)
    offset += bytesRead

    // Combine high bits with low bits to get full 64-bit row index
    // For most cases, highBits will be 0 (first 4 billion rows)
    const highOffset = highBits * 0x100000000
    for (const lowBits of values) {
      deletedRows.add(highOffset + lowBits)
    }
  }

  return deletedRows
}

/**
 * Parse a 32-bit RoaringBitmap from serialized data.
 *
 * @param data - Raw bytes starting at the RoaringBitmap
 * @returns Object with parsed values and bytes consumed
 */
function parseRoaringBitmap32(data: Uint8Array): { values: number[]; bytesRead: number } {
  const values: number[] = []

  // Handle empty or insufficient data (need at least 4 bytes for cookie)
  if (!data || data.byteLength < 4) {
    return { values: [], bytesRead: 0 }
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = 0

  // Read cookie (first 4 bytes)
  const cookie = view.getUint32(offset, true)
  offset += 4

  let numContainers: number
  let hasRunContainers = false
  let runContainerBitset: Uint8Array | null = null

  if ((cookie & 0xffff) === SERIAL_COOKIE) {
    // Has potential run containers
    hasRunContainers = true
    numContainers = ((cookie >>> 16) & 0xffff) + 1

    // Read run container bitset
    const bitsetBytes = Math.ceil(numContainers / 8)
    // Bounds check before reading bitset
    if (offset + bitsetBytes > data.byteLength) {
      return { values: [], bytesRead: offset }
    }
    runContainerBitset = data.subarray(offset, offset + bitsetBytes)
    offset += bitsetBytes
  } else if (cookie === SERIAL_COOKIE_NO_RUNCONTAINER) {
    // No run containers - need 4 more bytes for container count
    if (offset + 4 > data.byteLength) {
      return { values: [], bytesRead: offset }
    }
    numContainers = view.getUint32(offset, true)
    offset += 4
  } else {
    // Unknown format - might be empty or different encoding
    return { values: [], bytesRead: offset }
  }

  if (numContainers === 0) {
    return { values: [], bytesRead: offset }
  }

  // Sanity check: limit containers to prevent memory issues
  const maxContainers = 65536 // Max possible with 16-bit keys
  if (numContainers > maxContainers) {
    return { values: [], bytesRead: offset }
  }

  // Read descriptive header: for each container, 16-bit key and 16-bit (cardinality - 1)
  const keys: number[] = []
  const cardinalities: number[] = []

  // Need 4 bytes per container for the header
  const headerSize = numContainers * 4
  if (offset + headerSize > data.byteLength) {
    return { values: [], bytesRead: offset }
  }

  for (let i = 0; i < numContainers; i++) {
    keys.push(view.getUint16(offset, true))
    offset += 2
    cardinalities.push(view.getUint16(offset, true) + 1)
    offset += 2
  }

  // Read offset header if needed (only for SERIAL_COOKIE_NO_RUNCONTAINER or if containers >= 4)
  const hasOffsets = cookie === SERIAL_COOKIE_NO_RUNCONTAINER || numContainers >= 4
  if (hasOffsets) {
    // Need 4 bytes per container for offsets
    const offsetsSize = numContainers * 4
    if (offset + offsetsSize > data.byteLength) {
      return { values: [], bytesRead: offset }
    }
    // Skip reading offsets into array since we don't use containerOffsets
    offset += offsetsSize
  }

  // Read containers
  for (let i = 0; i < numContainers; i++) {
    const key = keys[i]
    if (key === undefined) continue
    const cardinality = cardinalities[i]
    if (cardinality === undefined) continue
    const highBits = key << 16 // key represents the high 16 bits

    // Determine container type
    const bitsetByteIndex = Math.floor(i / 8)
    const isRunContainer = hasRunContainers && runContainerBitset && bitsetByteIndex < runContainerBitset.length && (runContainerBitset[bitsetByteIndex]! & (1 << (i % 8))) !== 0
    const isBitsetContainer = !isRunContainer && cardinality > 4096

    if (isRunContainer) {
      // Run container: number of runs, then pairs of (start, length-1)
      // Need at least 2 bytes for numRuns
      if (offset + 2 > data.byteLength) {
        break
      }
      const numRuns = view.getUint16(offset, true)
      offset += 2

      // Sanity check numRuns (max 2048 runs for 65536 values)
      if (numRuns > 2048) {
        break
      }

      // Need 4 bytes per run
      if (offset + numRuns * 4 > data.byteLength) {
        break
      }

      for (let r = 0; r < numRuns; r++) {
        const start = view.getUint16(offset, true)
        offset += 2
        const length = view.getUint16(offset, true) + 1
        offset += 2

        // Bounds check: start + length should not exceed 65536
        const safeLength = Math.min(length, 65536 - start)
        for (let v = 0; v < safeLength; v++) {
          values.push(highBits | (start + v))
        }
      }
    } else if (isBitsetContainer) {
      // Bitset container: 8KB = 65536 bits = 1024 * 64-bit words
      const bitsetSize = 1024 * 8 // 8KB
      if (offset + bitsetSize > data.byteLength) {
        break
      }

      for (let wordIdx = 0; wordIdx < 1024; wordIdx++) {
        // Use BigInt for 64-bit word to handle all 64 bits correctly
        // JavaScript bitwise ops (<<, &) only work on 32-bit signed integers
        const word = view.getBigUint64(offset, true)
        offset += 8

        // Skip empty words for efficiency
        if (word === 0n) continue

        // Check each bit using BigInt operations
        for (let bit = 0; bit < 64; bit++) {
          if ((word & (1n << BigInt(bit))) !== 0n) {
            values.push(highBits | (wordIdx * 64 + bit))
          }
        }
      }
    } else {
      // Array container: sorted 16-bit values
      // Need 2 bytes per value
      const containerSize = cardinality * 2
      if (offset + containerSize > data.byteLength) {
        break
      }

      for (let j = 0; j < cardinality; j++) {
        const lowBits = view.getUint16(offset, true)
        offset += 2
        values.push(highBits | lowBits)
      }
    }
  }

  return { values, bytesRead: offset }
}

/**
 * Load and parse a deletion vector from storage.
 *
 * @param storage - Storage backend
 * @param tablePath - Base path of the Delta table
 * @param dv - Deletion vector descriptor
 * @returns Set of row indices that are marked as deleted
 */
export async function loadDeletionVector(
  storage: StorageBackend,
  tablePath: string,
  dv: DeletionVectorDescriptor
): Promise<Set<number>> {
  if (dv.storageType === 'i') {
    // Inline: decode Base85 data directly
    const decoded = z85Decode(dv.pathOrInlineDv)
    return parseRoaringBitmap(decoded)
  }

  // Load from file
  const dvPath = getDeletionVectorPath(tablePath, dv)
  const fileData = await storage.read(dvPath)

  // The deletion vector file format is:
  // - [startOffset bytes to skip (from dv.offset)]
  // - 4 bytes: size (little-endian uint32) - this is redundant with sizeInBytes
  // - 4 bytes: checksum
  // - The actual RoaringTreemap data
  //
  // sizeInBytes includes only the RoaringTreemap data (not size/checksum)
  // so we need to skip the 8-byte header
  const startOffset = dv.offset ?? 0
  const headerSize = 8

  // Skip the header (size + checksum) to get to the actual bitmap data
  const dvData = fileData.subarray(startOffset + headerSize)

  return parseRoaringBitmap(dvData)
}

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
