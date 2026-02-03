/**
 * Shared Utilities
 *
 * Common utility functions used across delta, cdc, and other modules.
 * Centralizes reusable logic to eliminate DRY violations.
 *
 * @module utils
 */

import type { StorageBackend } from '../storage/index.js'
import { ValidationError } from '../errors.js'

// =============================================================================
// LOGGING ABSTRACTION
// =============================================================================

/**
 * Logger interface for the deltalake library.
 *
 * Provides a pluggable logging abstraction that allows library consumers
 * to integrate with their own logging infrastructure (e.g., pino, winston,
 * Cloudflare Workers logging).
 *
 * @example
 * ```typescript
 * import { setLogger } from '@dotdo/deltalake'
 *
 * // Use custom logger
 * setLogger({
 *   debug: (msg, ...args) => myLogger.debug(msg, ...args),
 *   info: (msg, ...args) => myLogger.info(msg, ...args),
 *   warn: (msg, ...args) => myLogger.warn(msg, ...args),
 *   error: (msg, ...args) => myLogger.error(msg, ...args),
 * })
 *
 * // Or silence all logging
 * setLogger({
 *   debug: () => {},
 *   info: () => {},
 *   warn: () => {},
 *   error: () => {},
 * })
 * ```
 */
export interface Logger {
  /** Log debug-level messages (not shown by default) */
  debug(message: string, ...args: unknown[]): void
  /** Log info-level messages */
  info(message: string, ...args: unknown[]): void
  /** Log warning-level messages */
  warn(message: string, ...args: unknown[]): void
  /** Log error-level messages */
  error(message: string, ...args: unknown[]): void
}

/**
 * Default logger implementation.
 *
 * - debug: silenced (no-op)
 * - info: console.info
 * - warn: console.warn
 * - error: console.error
 */
export const defaultLogger: Logger = {
  debug: () => {},
  info: console.info.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
}

/** Current active logger instance */
let currentLogger: Logger = defaultLogger

/**
 * Set a custom logger for the deltalake library.
 *
 * @param logger - The logger implementation to use
 *
 * @example
 * ```typescript
 * // Integrate with pino
 * import pino from 'pino'
 * const logger = pino()
 * setLogger({
 *   debug: (msg, ...args) => logger.debug({ args }, msg),
 *   info: (msg, ...args) => logger.info({ args }, msg),
 *   warn: (msg, ...args) => logger.warn({ args }, msg),
 *   error: (msg, ...args) => logger.error({ args }, msg),
 * })
 * ```
 */
export function setLogger(logger: Logger): void {
  currentLogger = logger
}

/**
 * Get the current logger instance.
 *
 * Used internally by deltalake modules for logging operations.
 *
 * @returns The current logger instance
 */
export function getLogger(): Logger {
  return currentLogger
}

// =============================================================================
// EXHAUSTIVENESS CHECKING
// =============================================================================

/**
 * Utility for exhaustiveness checking in switch statements.
 * TypeScript will error at compile time if a case is not handled.
 *
 * @example
 * ```typescript
 * type Status = 'active' | 'inactive'
 * function handle(s: Status) {
 *   switch (s) {
 *     case 'active': return 'A'
 *     case 'inactive': return 'I'
 *     default: return assertNever(s)
 *   }
 * }
 * ```
 */
export function assertNever(x: never, message?: string): never {
  throw new Error(message ?? `Unexpected value: ${JSON.stringify(x)}`)
}

import { createAsyncBuffer } from '../storage/index.js'
import { parquetReadObjects } from '@dotdo/hyparquet'
import type { BasicType } from '@dotdo/hyparquet-writer'

// =============================================================================
// CONSTANTS
// =============================================================================

/** Number of digits in version filename (padded with zeros) */
export const VERSION_DIGITS = 20

/** Delta log directory name */
export const DELTA_LOG_DIR = '_delta_log'

/** Regular expression for matching version filenames (e.g., 00000000000000000042.json) */
export const VERSION_FILENAME_REGEX = /^(\d{20})\.json$/

// =============================================================================
// VERSION FORMATTING AND PARSING
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
  const versionStr = version.toString()

  // Check for negative numbers
  if (versionStr.startsWith('-')) {
    throw new ValidationError('Version number cannot be negative', 'version', version)
  }

  // Check if exceeds maximum digits
  if (versionStr.length > VERSION_DIGITS) {
    throw new ValidationError(`Version number exceeds ${VERSION_DIGITS} digits`, 'version', version)
  }

  return versionStr.padStart(VERSION_DIGITS, '0')
}

/**
 * Parse version number from a log filename.
 *
 * Extracts the version number from a Delta Lake log filename.
 * Handles both bare filenames and full paths.
 *
 * @param filename - Log filename (can include path, e.g., "_delta_log/00000000000000000042.json")
 * @returns Parsed version number
 * @throws Error if filename doesn't match expected format
 *
 * @example
 * ```typescript
 * parseVersionFromFilename('00000000000000000042.json')  // 42
 * parseVersionFromFilename('_delta_log/00000000000000000000.json')  // 0
 * ```
 */
export function parseVersionFromFilename(filename: string): number {
  // Extract just the filename if a full path is provided
  const basename = filename.split('/').pop() || filename

  // Match version filename using the regex
  const match = basename.match(VERSION_FILENAME_REGEX)

  if (!match || match[1] === undefined) {
    throw new ValidationError('Invalid log file name format', 'filename', filename)
  }

  return parseInt(match[1], 10)
}

/**
 * Get the latest version from a list of log files.
 *
 * Parses version numbers from Delta Lake log filenames and returns
 * the highest version found. This is the canonical way to determine
 * the current table version from storage.
 *
 * @param files - Array of file paths from storage.list()
 * @returns The latest version number, or -1 if no valid log files found
 *
 * @example
 * ```typescript
 * const files = await storage.list('_delta_log')
 * const version = getLatestVersionFromFiles(files)  // e.g., 42
 * ```
 */
export function getLatestVersionFromFiles(files: string[]): number {
  const versions = files
    .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
    .map(f => {
      const parts = f.replace('.json', '').split('/')
      const filename = parts[parts.length - 1] ?? ''
      return parseInt(filename, 10)
    })
    .filter(v => !isNaN(v))
    .sort((a, b) => b - a)

  return versions[0] ?? -1
}

/**
 * Get the log file path for a specific version.
 *
 * @param tablePath - Base path of the Delta table
 * @param version - Version number
 * @returns Full path to the log file
 *
 * @example
 * ```typescript
 * getLogFilePath('my-table', 42)  // "my-table/_delta_log/00000000000000000042.json"
 * ```
 */
export function getLogFilePath(tablePath: string, version: number): string {
  // Remove trailing slash if present
  const cleanPath = tablePath.endsWith('/') ? tablePath.slice(0, -1) : tablePath
  const baseDir = cleanPath ? `${cleanPath}/${DELTA_LOG_DIR}` : DELTA_LOG_DIR
  return `${baseDir}/${formatVersion(version)}.json`
}

// =============================================================================
// DATE UTILITIES
// =============================================================================

/**
 * Format a date as a partition string (YYYY-MM-DD).
 *
 * Used for date-based partitioning in CDC files and other time-partitioned data.
 *
 * @param date - The date to format
 * @returns Date string in YYYY-MM-DD format
 *
 * @example
 * ```typescript
 * formatDatePartition(new Date('2024-03-15'))  // "2024-03-15"
 * ```
 */
export function formatDatePartition(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// =============================================================================
// PARQUET FILE READING UTILITIES
// =============================================================================

/**
 * Options for reading a Parquet file.
 */
export interface ParquetReadOptions {
  /** Columns to read (default: all) */
  columns?: string[]
}

/**
 * Post-process a row to parse JSON strings back to arrays/objects.
 *
 * When writing Parquet files, complex types (arrays, objects) are serialized
 * to JSON strings. This function detects and parses these strings back to
 * their original types.
 *
 * @param row - The row to process
 * @returns The processed row with parsed JSON fields
 */
function parseJsonFields<T extends Record<string, unknown>>(row: T): T {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(row)) {
    if (typeof value === 'string') {
      // Check if the string looks like a JSON array or object
      const trimmed = value.trim()
      if ((trimmed.startsWith('[') && trimmed.endsWith(']')) ||
          (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
        try {
          result[key] = JSON.parse(value)
        } catch {
          // Not valid JSON, keep as string
          result[key] = value
        }
      } else {
        result[key] = value
      }
    } else {
      result[key] = value
    }
  }

  return result as T
}

/**
 * Read rows from a Parquet file using storage backend.
 *
 * This is the canonical way to read Parquet files from storage.
 * Handles async buffer creation and proper type casting.
 *
 * @param storage - The storage backend to read from
 * @param path - Path to the Parquet file
 * @param options - Optional read options (e.g., column selection)
 * @returns Array of rows from the Parquet file
 *
 * @example
 * ```typescript
 * const rows = await readParquetFile<MyRow>(storage, 'data/part-00000.parquet')
 * ```
 */
export async function readParquetFile<T = Record<string, unknown>>(
  storage: StorageBackend,
  path: string,
  _options?: ParquetReadOptions
): Promise<T[]> {
  const asyncBuffer = await createAsyncBuffer(storage, path)
  const rows = await parquetReadObjects({ file: asyncBuffer })
  // Post-process rows to parse JSON strings back to arrays/objects
  return (rows as T[]).map(row => parseJsonFields(row as Record<string, unknown>) as T)
}

/**
 * Read rows from an ArrayBuffer containing Parquet data.
 *
 * Used when the Parquet data is already in memory (e.g., checkpoint files).
 * Note: This function does NOT parse JSON strings because it's used for
 * Delta checkpoint files which have their own JSON parsing logic in rowsToSnapshot.
 *
 * @param data - The raw Parquet data as Uint8Array
 * @returns Array of rows from the Parquet file
 *
 * @example
 * ```typescript
 * const data = await storage.read('checkpoint.parquet')
 * const rows = await readParquetFromBuffer<MyRow>(data)
 * ```
 */
export async function readParquetFromBuffer<T = Record<string, unknown>>(
  data: Uint8Array
): Promise<T[]> {
  const arrayBuffer = data.buffer.slice(
    data.byteOffset,
    data.byteOffset + data.byteLength
  ) as ArrayBuffer
  const rows = await parquetReadObjects({ file: arrayBuffer })
  return rows as T[]
}

// =============================================================================
// PARQUET TYPE INFERENCE
// =============================================================================

/** Int32 minimum value for type inference */
const INT32_MIN = -2147483648

/** Int32 maximum value for type inference */
const INT32_MAX = 2147483647

/**
 * Infer Parquet type from a JavaScript value.
 *
 * This function maps JavaScript types to appropriate Parquet BasicTypes:
 * - null/undefined -> STRING (default for null values)
 * - boolean -> BOOLEAN
 * - bigint -> INT64
 * - number (integer in int32 range) -> INT32
 * - number (other) -> DOUBLE
 * - string -> STRING
 * - Date -> TIMESTAMP
 * - Uint8Array/ArrayBuffer -> BYTE_ARRAY
 * - Array/Object -> JSON
 *
 * @param value - The JavaScript value to infer type from
 * @returns The corresponding Parquet BasicType
 *
 * @example
 * ```typescript
 * inferParquetType(42)           // 'INT32'
 * inferParquetType(3.14)         // 'DOUBLE'
 * inferParquetType('hello')      // 'STRING'
 * inferParquetType(new Date())   // 'TIMESTAMP'
 * inferParquetType([1, 2, 3])    // 'JSON'
 * ```
 */
export function inferParquetType(value: unknown): BasicType {
  if (value === null || value === undefined) {
    return 'STRING' // Default type for null values
  }

  if (typeof value === 'boolean') {
    return 'BOOLEAN'
  }

  if (typeof value === 'bigint') {
    return 'INT64'
  }

  if (typeof value === 'number') {
    // Check if it's an integer that fits in int32 range
    if (Number.isInteger(value) && value >= INT32_MIN && value <= INT32_MAX) {
      return 'INT32'
    }
    return 'DOUBLE'
  }

  if (typeof value === 'string') {
    return 'STRING'
  }

  if (value instanceof Date) {
    return 'TIMESTAMP'
  }

  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    return 'BYTE_ARRAY'
  }

  if (Array.isArray(value) || (typeof value === 'object' && value !== null)) {
    return 'JSON' // Use JSON type for complex objects
  }

  return 'STRING' // Fallback
}

/**
 * Infer Parquet schema from an array of row objects.
 *
 * Scans all rows to find the first non-null value for each field and infers
 * the appropriate Parquet type. This handles cases where the first row might
 * have null values for certain fields.
 *
 * @param rows - Array of row objects to infer schema from
 * @returns Array of field definitions with name and Parquet BasicType
 *
 * @example
 * ```typescript
 * const rows = [
 *   { id: '1', name: null, value: 100 },
 *   { id: '2', name: 'Alice', value: 200 }
 * ]
 * const schema = inferSchemaFromRows(rows)
 * // Returns: [
 * //   { name: 'id', type: 'STRING' },
 * //   { name: 'name', type: 'STRING' },
 * //   { name: 'value', type: 'INT32' }
 * // ]
 * ```
 */
export function inferSchemaFromRows<T extends Record<string, unknown>>(rows: T[]): Array<{ name: string; type: BasicType }> {
  if (rows.length === 0) {
    return []
  }

  // Collect all field names from all rows (handles sparse schemas)
  const fieldNames = new Set<string>()
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      fieldNames.add(key)
    }
  }

  const fields: Array<{ name: string; type: BasicType }> = []

  for (const name of fieldNames) {
    let inferredType: BasicType = 'STRING' // Default fallback for all-null columns

    // Scan rows to find the first non-null value for type inference
    for (const row of rows) {
      const value = row[name]

      if (value === null || value === undefined) {
        continue
      }

      // Found a non-null value, infer its Parquet type
      inferredType = inferParquetType(value)
      break
    }

    fields.push({ name, type: inferredType })
  }

  return fields
}
