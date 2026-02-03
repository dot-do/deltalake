/**
 * Delta Lake Table
 *
 * Main DeltaTable class for reading and writing Delta Lake tables.
 *
 * ## Memory Usage and Scalability
 *
 * DeltaTable uses a transient in-memory cache during write operations that is
 * automatically cleared after each commit. Data is persisted to Parquet files
 * in storage, and queries read directly from these files.
 *
 * **Cache Behavior:**
 * - Write operations temporarily cache data until commit
 * - Cache is cleared automatically after successful commits
 * - Query operations fall back to reading Parquet files from storage
 *
 * **For Large Datasets:**
 * - Use `queryStream()` for memory-efficient iteration over single rows
 * - Use `queryBatch()` for processing rows in configurable batch sizes
 * - Write data in smaller batches rather than one large write
 * - Delete/update operations load affected files into memory one at a time
 *
 * **Scalability Limits:**
 * - Individual Parquet files are loaded fully into memory when read
 * - Tables with many small files may have higher read overhead
 * - Use compaction to merge small files into larger ones
 *
 * @module delta/table
 */

import type { StorageBackend } from '../storage/index.js'
import type { ColumnSource, BasicType } from '@dotdo/hyparquet-writer'
import type {
  DeltaAction,
  AddAction,
  MetadataAction,
  DeltaCommit,
  DeltaSnapshot,
  CheckpointConfig,
  CompactionContext,
  QueryOptions,
  WriteOptions,
  FileStats,
  InferredFieldType,
  InferredField,
  InferredSchema,
  Filter,
} from './types.js'
import {
  isValidDeltaSchema,
  isValidPartitionValues,
  isCommitInfoAction,
} from './validators.js'
import { buildColumnMapping, applyColumnMapping } from './snapshot.js'
import {
  readLastCheckpoint,
  writeLastCheckpoint,
  readCheckpoint,
  readCheckpointPart,
  writeSingleCheckpoint,
  createMultiPartCheckpoint,
  estimateCheckpointSize,
  cleanupCheckpoints,
  cleanupLogs,
  getCleanableLogVersions,
  discoverCheckpoints,
  findLatestCheckpoint,
  validateCheckpoint,
  shouldCheckpoint as shouldCheckpointFn,
  DEFAULT_CHECKPOINT_CONFIG,
} from './checkpoint.js'
import type { LastCheckpoint } from './types.js'
import { ConcurrencyError, VersionMismatchError, ValidationError, FileNotFoundError } from '../errors.js'
import { matchesFilter, getNestedValue, applyProjection, applyProjectionToDoc, getProjectionColumns } from '../query/index.js'
import { parquetWriteBuffer } from '@dotdo/hyparquet-writer'
import {
  formatVersion,
  getLatestVersionFromFiles,
  readParquetFile,
  getLogger,
  DELTA_LOG_DIR,
  parseVersionFromFilename as parseVersionFromFilenameUtil,
  getLogFilePath as getLogFilePathUtil,
} from '../utils/index.js'
import { isValidDeltaAction, isValidFileStats } from './validators.js'
import type { DeletionVectorDescriptor } from './types.js'
import { loadDeletionVector } from './deletion-vectors.js'

// =============================================================================
// LOCAL HELPER FUNCTIONS (to avoid circular dependency with index.js)
// =============================================================================

/** Recognized action type keys in Delta Lake format */
const ACTION_TYPES = ['add', 'remove', 'metaData', 'protocol', 'commitInfo'] as const

/**
 * Encode stats object to JSON string for AddAction
 */
function encodeStats(stats: FileStats): string {
  return JSON.stringify(stats)
}

/**
 * Decode URL-encoded characters in a file path from the Delta log.
 */
function decodeFilePath(filePath: string): string {
  try {
    return decodeURIComponent(filePath)
  } catch {
    return filePath
  }
}

/**
 * Extract partition values from a file path.
 */
function extractPartitionValuesFromPath(filePath: string): Record<string, string> {
  const partitions: Record<string, string> = {}
  const parts = filePath.split('/')

  for (const part of parts) {
    const match = part.match(/^([^=]+)=(.+)$/)
    if (match) {
      const [, column, value] = match
      if (column && value !== undefined) {
        partitions[column] = decodeURIComponent(value)
      }
    }
  }

  return partitions
}

/**
 * Transaction log utilities for serialization, parsing, and validation
 */
const transactionLog = {
  serializeAction(action: DeltaAction): string {
    return JSON.stringify(action)
  },

  parseAction(json: string): DeltaAction {
    if (!json || json.trim() === '') {
      throw new ValidationError('Cannot parse empty JSON string', 'json', json)
    }
    const parsed: unknown = JSON.parse(json)
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new ValidationError('Action must be a JSON object', 'json', typeof parsed)
    }
    const hasValidAction = ACTION_TYPES.some(type => type in parsed)
    if (!hasValidAction) {
      throw new ValidationError('JSON must contain a recognized action type', 'json', Object.keys(parsed as object))
    }
    if (!isValidDeltaAction(parsed)) {
      throw new ValidationError('Invalid Delta action structure: missing or invalid required fields', 'json', parsed)
    }
    return parsed
  },

  serializeCommit(actions: DeltaAction[]): string {
    return actions.map(action => this.serializeAction(action)).join('\n')
  },

  parseCommit(content: string): DeltaAction[] {
    const lines = content.split(/\r?\n/).filter(line => line.trim() !== '')
    return lines.map(line => this.parseAction(line))
  },

  formatVersion(version: number | bigint): string {
    return formatVersion(version)
  },

  parseVersionFromFilename(filename: string): number {
    return parseVersionFromFilenameUtil(filename)
  },

  getLogFilePath(tablePath: string, version: number): string {
    return getLogFilePathUtil(tablePath, version)
  },

  validateAction(action: DeltaAction): { valid: boolean; errors: string[] } {
    const errors: string[] = []
    // Simplified validation - full implementation is in index.ts
    if ('add' in action && (!action.add.path || action.add.path === '')) {
      errors.push('add.path must not be empty')
    }
    if ('remove' in action && (!action.remove.path || action.remove.path === '')) {
      errors.push('remove.path must not be empty')
    }
    return { valid: errors.length === 0, errors }
  },
}

// =============================================================================
// CONSTANTS
// =============================================================================

/** Default protocol versions for new tables */
const DEFAULT_PROTOCOL = {
  minReaderVersion: 1,
  minWriterVersion: 1,
} as const

/** Int32 range for type inference */
const INT32_MIN = -2147483648
const INT32_MAX = 2147483647

/** Valid comparison operators for filter validation */
const VALID_COMPARISON_OPS = ['$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin', '$exists', '$type', '$regex'] as const

// =============================================================================
// VALIDATION HELPERS
// =============================================================================

/**
 * Validate that a value is not null or undefined.
 * @throws {ValidationError} If value is null or undefined
 */
function validateRequired<T>(value: T | null | undefined, fieldName: string): asserts value is T {
  if (value === null || value === undefined) {
    throw new ValidationError(`${fieldName} is required and cannot be null or undefined`, fieldName, value)
  }
}

/**
 * Validate that a value is an array.
 * @throws {ValidationError} If value is not an array
 */
function validateArray<T>(value: unknown, fieldName: string): asserts value is T[] {
  if (!Array.isArray(value)) {
    throw new ValidationError(`${fieldName} must be an array`, fieldName, value)
  }
}

/**
 * Validate that a number is a non-negative integer.
 * @throws {ValidationError} If value is not a non-negative integer
 */
function validateNonNegativeInteger(value: number, fieldName: string): void {
  if (typeof value !== 'number' || Number.isNaN(value) || !Number.isInteger(value) || value < 0) {
    throw new ValidationError(`${fieldName} must be a non-negative integer`, fieldName, value)
  }
}

// =============================================================================
// DELTA TABLE
// =============================================================================

export class DeltaTable<T extends Record<string, unknown> = Record<string, unknown>> {
  private storage: StorageBackend
  private tablePath: string
  private currentVersion: number = -1
  private versionCached: boolean = false
  /**
   * In-memory cache for row data during write operations.
   *
   * This cache serves as a write-through buffer that holds data temporarily
   * between writing Parquet files and committing the transaction. The cache
   * is cleared after each successful commit to prevent memory growth.
   *
   * **Memory Usage Patterns:**
   * - During write(): Data is cached until commit completes
   * - After commit(): Cache is cleared automatically
   * - During query(): Falls back to reading Parquet files if not in cache
   * - During delete/update: Files are read into memory one at a time
   *
   * **Scalability Considerations:**
   * - For very large writes, consider using smaller batches
   * - For queries on large tables, use queryStream() or queryBatch()
   * - Delete/update operations load affected file data into memory
   *
   * @internal
   */
  private dataStore: Map<string, T[]> = new Map()
  private checkpointConfig: CheckpointConfig
  private checkpointTimestamps = new Map<number, number>()
  private tableSchema: InferredSchema | null = null
  private partitionColumns: string[] = []
  private metadataLoaded: boolean = false
  private tableConfiguration: Record<string, string> = {}

  /**
   * Create a new DeltaTable instance.
   *
   * @param storage - Storage backend for reading/writing data
   * @param tablePath - Base path for the Delta table
   * @param config - Optional checkpoint configuration
   *
   * @example
   * ```typescript
   * const storage = new MemoryStorage()
   * const table = new DeltaTable(storage, 'my-table')
   * ```
   */
  constructor(storage: StorageBackend, tablePath: string, config?: CheckpointConfig) {
    // Validate required parameters
    validateRequired(storage, 'storage')
    validateRequired(tablePath, 'tablePath')

    // Validate tablePath is a non-empty string
    if (typeof tablePath !== 'string') {
      throw new ValidationError('tablePath must be a string', 'tablePath', tablePath)
    }
    // Note: empty string is allowed for tablePath (root table)

    // Validate storage has required methods
    if (typeof storage.read !== 'function' ||
        typeof storage.write !== 'function' ||
        typeof storage.list !== 'function') {
      throw new ValidationError(
        'storage must implement StorageBackend interface (read, write, list methods)',
        'storage',
        storage
      )
    }

    // Validate optional config parameters if provided
    if (config) {
      if (config.checkpointInterval !== undefined) {
        validateNonNegativeInteger(config.checkpointInterval, 'config.checkpointInterval')
      }
      if (config.maxActionsPerCheckpoint !== undefined) {
        validateNonNegativeInteger(config.maxActionsPerCheckpoint, 'config.maxActionsPerCheckpoint')
      }
      if (config.numRetainedCheckpoints !== undefined) {
        validateNonNegativeInteger(config.numRetainedCheckpoints, 'config.numRetainedCheckpoints')
      }
      if (config.checkpointRetentionMs !== undefined) {
        validateNonNegativeInteger(config.checkpointRetentionMs, 'config.checkpointRetentionMs')
      }
      if (config.maxCheckpointSizeBytes !== undefined) {
        validateNonNegativeInteger(config.maxCheckpointSizeBytes, 'config.maxCheckpointSizeBytes')
      }
    }

    this.storage = storage
    this.tablePath = tablePath
    this.checkpointConfig = {
      checkpointInterval: config?.checkpointInterval ?? DEFAULT_CHECKPOINT_CONFIG.checkpointInterval,
      maxActionsPerCheckpoint: config?.maxActionsPerCheckpoint ?? DEFAULT_CHECKPOINT_CONFIG.maxActionsPerCheckpoint,
      numRetainedCheckpoints: config?.numRetainedCheckpoints ?? DEFAULT_CHECKPOINT_CONFIG.numRetainedCheckpoints,
      checkpointRetentionMs: config?.checkpointRetentionMs,
      maxCheckpointSizeBytes: config?.maxCheckpointSizeBytes,
    }
  }

  /**
   * Set table configuration properties.
   *
   * These properties will be included in the table metadata when the first commit is made.
   * For example, setting `delta.enableChangeDataFeed` to `'true'` enables CDC.
   *
   * @param config - Configuration properties to set (values must be strings per Delta protocol)
   *
   * @example
   * ```typescript
   * table.setTableConfiguration({ 'delta.enableChangeDataFeed': 'true' })
   * ```
   */
  setTableConfiguration(config: Record<string, string>): void {
    this.tableConfiguration = { ...this.tableConfiguration, ...config }
  }

  /**
   * Get the current table configuration.
   *
   * @returns The table configuration properties
   */
  getTableConfiguration(): Record<string, string> {
    return { ...this.tableConfiguration }
  }

  /**
   * Get the path to the Delta log directory
   */
  private get logPath(): string {
    return this.tablePath ? `${this.tablePath}/${DELTA_LOG_DIR}` : DELTA_LOG_DIR
  }

  /**
   * Get the current table version.
   *
   * Uses cached value if available. Call `refreshVersion()` to force a fresh read.
   *
   * @returns The current version number, or -1 if the table doesn't exist
   *
   * @example
   * ```typescript
   * const version = await table.version()
   * console.log(`Table is at version ${version}`)
   * ```
   */
  async version(): Promise<number> {
    if (this.versionCached) return this.currentVersion

    return this.getVersionFromStorage()
  }

  /**
   * Get the current version directly from storage (bypasses cache)
   * Uses the shared getLatestVersionFromFiles utility.
   */
  private async getVersionFromStorage(): Promise<number> {
    const files = await this.storage.list(this.logPath)
    const version = getLatestVersionFromFiles(files)
    this.currentVersion = version
    this.versionCached = true
    return version
  }

  /**
   * Force re-read version from storage and clear internal caches.
   *
   * Use this after catching a ConcurrencyError to refresh state before retrying.
   * This ensures you have the latest version information from storage.
   *
   * @returns The current version number from storage
   *
   * @example
   * ```typescript
   * try {
   *   await table.write(rows)
   * } catch (error) {
   *   if (error instanceof ConcurrencyError) {
   *     // Refresh and retry
   *     await table.refreshVersion()
   *     await table.write(rows)
   *   }
   * }
   * ```
   */
  async refreshVersion(): Promise<number> {
    // Clear cached version
    this.currentVersion = -1
    this.versionCached = false

    // Clear cached metadata so it will be reloaded
    this.metadataLoaded = false

    // Get fresh version from storage
    const version = await this.getVersionFromStorage()

    return version
  }

  /**
   * Check for version conflicts before committing.
   * Throws ConcurrencyError if the actual version differs from expected.
   */
  private async checkVersionConflict(): Promise<void> {
    const expectedVersion = this.currentVersion
    const actualVersion = await this.getVersionFromStorage()

    if (actualVersion !== expectedVersion) {
      throw new ConcurrencyError({
        expectedVersion,
        actualVersion,
      })
    }
  }

  /**
   * Ensure table metadata (partitionColumns, schema) is loaded from an existing table.
   * This is called lazily when metadata is needed for operations on existing tables.
   */
  private async ensureMetadataLoaded(): Promise<void> {
    if (this.metadataLoaded) return

    const currentVersion = await this.version()
    if (currentVersion < 0) {
      // No existing table, nothing to load
      this.metadataLoaded = true
      return
    }

    const snapshot = await this.snapshot()
    if (snapshot.metadata) {
      // Load partition columns from metadata
      if (snapshot.metadata.partitionColumns && snapshot.metadata.partitionColumns.length > 0) {
        this.partitionColumns = snapshot.metadata.partitionColumns
      }

      // Load schema from metadata if available
      if (snapshot.metadata.schemaString && !this.tableSchema) {
        try {
          const parsed: unknown = JSON.parse(snapshot.metadata.schemaString)
          if (isValidDeltaSchema(parsed)) {
            // Convert Delta schema to InferredSchema format
            this.tableSchema = {
              fields: parsed.fields.map(field => ({
                name: field.name,
                type: this.deltaTypeToInferredType(field.type),
                optional: field.nullable ?? true,
              })),
            }
          }
        } catch {
          // Schema parsing failed, will be inferred on next write
        }
      }
    }

    this.metadataLoaded = true
  }

  /**
   * Convert Delta Lake schema type to InferredSchema type.
   */
  private deltaTypeToInferredType(deltaType: unknown): InferredFieldType {
    if (typeof deltaType === 'string') {
      switch (deltaType) {
        case 'string': return 'string'
        case 'integer':
        case 'int32': return 'int32'  // Support both Delta Lake and internal naming
        case 'long':
        case 'int64': return 'int64'  // Support both Delta Lake and internal naming
        case 'double':
        case 'float': return 'double'
        case 'boolean': return 'boolean'
        case 'binary': return 'binary'
        case 'timestamp': return 'timestamp'
        case 'variant': return 'variant'
        default: return 'string'
      }
    }
    // Complex types (arrays, structs, maps) are stored as variant
    if (typeof deltaType === 'object' && deltaType !== null) {
      return 'variant'
    }
    return 'string'
  }

  /**
   * Get a snapshot of the table at a specific version.
   *
   * A snapshot represents the complete state of the table at a point in time,
   * including all active files, metadata, and protocol information.
   * This enables time travel queries and consistent reads.
   *
   * @param version - Optional version to get snapshot at (defaults to current version)
   * @returns The table snapshot including files, metadata, and protocol
   *
   * @example
   * ```typescript
   * // Get current snapshot
   * const current = await table.snapshot()
   *
   * // Get snapshot at version 5 (time travel)
   * const historical = await table.snapshot(5)
   *
   * // Use snapshot for consistent reads
   * const snapshot = await table.snapshot()
   * const results1 = await table.query({ type: 'A' }, { snapshot })
   * const results2 = await table.query({ type: 'B' }, { snapshot })
   * ```
   */
  async snapshot(version?: number): Promise<DeltaSnapshot> {
    // Validate version if provided
    if (version !== undefined) {
      if (typeof version !== 'number' || Number.isNaN(version)) {
        throw new ValidationError('version must be a valid number', 'version', version)
      }
      if (!Number.isInteger(version)) {
        throw new ValidationError('version must be an integer', 'version', version)
      }
      // Note: version can be -1 to indicate no version exists
    }

    const targetVersion = version ?? await this.version()
    if (targetVersion < 0) {
      return { version: -1, files: [] }
    }

    // Try to find and use the latest checkpoint
    const lastCheckpoint = await readLastCheckpoint(this.storage, this.logPath)
    let startVersion = 0
    let files: AddAction['add'][] = []
    let metadata: MetadataAction['metaData'] | undefined
    let protocol: DeltaSnapshot['protocol'] | undefined

    if (lastCheckpoint && lastCheckpoint.version <= targetVersion) {
      // Read from checkpoint
      try {
        const checkpointSnapshot = await readCheckpoint(this.storage, this.logPath, lastCheckpoint.version, lastCheckpoint)
        files = checkpointSnapshot.files
        metadata = checkpointSnapshot.metadata
        protocol = checkpointSnapshot.protocol
        startVersion = lastCheckpoint.version + 1
      } catch (e) {
        // Checkpoint read failed, fall back to full log replay
        getLogger().warn(`[DeltaTable] Checkpoint read failed for version ${lastCheckpoint.version}, falling back to full log replay:`, e)
        startVersion = 0
        files = []
      }
    }

    // Apply remaining commits
    for (let v = startVersion; v <= targetVersion; v++) {
      const commit = await this.readCommit(v)
      if (!commit) continue

      // Per Delta Lake protocol, actions within a single commit need special reconciliation:
      // 1. When a remove and add for the same path appear in the same commit, the add wins
      // 2. When multiple adds for the same path appear, the LAST one wins
      // 3. Actions are processed in order within the commit

      // First pass: collect the final state for each path within this commit
      // Map from path to either the final AddAction or null (meaning removed)
      const pathActions = new Map<string, AddAction['add'] | null>()

      for (const action of commit.actions) {
        if ('add' in action) {
          // Add always sets/overwrites the path state (latest add wins)
          pathActions.set(action.add.path, action.add)
        } else if ('remove' in action) {
          // Remove sets the path state to null (file should be removed)
          // But a subsequent add in the same commit will override this
          pathActions.set(action.remove.path, null)
        } else if ('metaData' in action) {
          metadata = action.metaData
        } else if ('protocol' in action) {
          protocol = action.protocol
        }
      }

      // Second pass: apply the reconciled actions to the files list
      for (const [path, addData] of pathActions) {
        // First, remove any existing file with this path
        const idx = files.findIndex(f => f.path === path)
        if (idx >= 0) {
          files.splice(idx, 1)
        }

        // Then, if the final state is an add, add the file
        if (addData !== null) {
          files.push(addData)
        }
      }
    }

    return { version: targetVersion, files, metadata, protocol }
  }

  /**
   * Read a specific commit from storage
   *
   * @param version - Version number to read
   * @returns DeltaCommit or null if not found or corrupted
   */
  private async readCommit(version: number): Promise<DeltaCommit | null> {
    const path = `${this.logPath}/${formatVersion(version)}.json`
    try {
      const data = await this.storage.read(path)
      const content = new TextDecoder().decode(data)

      // Use transactionLog utility for parsing NDJSON
      const actions = transactionLog.parseCommit(content)

      // Find commit info to get timestamp
      const commitInfoAction = actions.find(isCommitInfoAction)

      return {
        version,
        timestamp: commitInfoAction?.commitInfo.timestamp ?? Date.now(),
        actions,
      }
    } catch (e) {
      // Log entry read/parse failed - expected for missing versions
      getLogger().warn(`[DeltaTable] Failed to read commit version ${version}:`, e)
      return null
    }
  }
  /**
   * Write data to the table
   *
   * Optimized implementation that processes rows in a single pass:
   * - Schema inference, validation, column conversion, and stats computed together
   * - Reduced memory allocations
   * - Single timestamp for consistency
   *
   * @param rows - Array of rows to write
   * @param options - Optional write options including partition columns
   * @returns The commit information
   *
   * @example
   * ```typescript
   * // Write without partitioning
   * await table.write(rows)
   *
   * // Write with partitioning
   * await table.write(rows, { partitionColumns: ['year', 'month'] })
   * ```
   */
  async write(rows: T[], options?: WriteOptions): Promise<DeltaCommit> {
    // Validate rows parameter
    validateRequired(rows, 'rows')
    validateArray(rows, 'rows')

    if (rows.length === 0) {
      throw new ValidationError('Cannot write empty data: rows array must contain at least one element', 'rows', rows)
    }

    // Validate each row is an object
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]
      if (row === null || row === undefined) {
        throw new ValidationError(`Row at index ${i} cannot be null or undefined`, `rows[${i}]`, row)
      }
      if (typeof row !== 'object' || Array.isArray(row)) {
        throw new ValidationError(`Row at index ${i} must be an object`, `rows[${i}]`, row)
      }
    }

    // Validate partition columns if provided
    if (options?.partitionColumns) {
      validateArray(options.partitionColumns, 'options.partitionColumns')
      for (const col of options.partitionColumns) {
        if (typeof col !== 'string' || col.trim() === '') {
          throw new ValidationError(
            'Partition column names must be non-empty strings',
            'options.partitionColumns',
            col
          )
        }
      }
    }

    const currentVersion = await this.version()
    const newVersion = currentVersion + 1

    // Check for version conflict before committing
    await this.checkVersionConflict()

    // Ensure metadata (including partitionColumns) is loaded from existing table
    await this.ensureMetadataLoaded()

    // Capture timestamp once for consistency across all operations
    const timestamp = Date.now()

    // Determine partition columns - use options if provided, otherwise use table's partition columns
    const partitionCols = options?.partitionColumns ?? this.partitionColumns

    // Process all rows in a single pass: infer schema, validate, convert to columns, compute stats
    let { schema: inferredSchema, columnData, stats } = this.processRowsOptimized(rows)

    // If this is not the first write, check schema compatibility and merge schemas
    if (currentVersion >= 0 && this.tableSchema) {
      this.validateAndMergeSchemas(inferredSchema, this.tableSchema)
      // Re-process using the existing table schema to ensure correct types
      // This handles cases where int32 values (like 72.0) need to be written as double
      const reprocessed = this.processRowsForWrite(rows)
      columnData = reprocessed.columnData
    } else {
      this.tableSchema = inferredSchema
    }

    // Update partition columns from options if this is the first write
    if (currentVersion < 0 && options?.partitionColumns) {
      this.partitionColumns = [...options.partitionColumns]
    }

    let actions: DeltaAction[]

    if (partitionCols && partitionCols.length > 0) {
      // Partitioned write - group rows by partition values
      actions = await this.writePartitioned(rows, partitionCols, inferredSchema, newVersion, timestamp)
    } else {
      // Non-partitioned write
      const dataPath = `${this.tablePath}/part-${formatVersion(newVersion)}.parquet`
      const parquetBuffer = parquetWriteBuffer({
        columnData,
        codec: 'UNCOMPRESSED',
      })

      await this.storage.write(dataPath, new Uint8Array(parquetBuffer))

      // Store data in memory for query() to work
      this.dataStore.set(dataPath, rows)

      // Get actual file size
      const actualSize = parquetBuffer.byteLength

      // Create commit actions
      actions = this.buildWriteActions(
        dataPath,
        actualSize,
        stats,
        inferredSchema,
        newVersion,
        timestamp,
        partitionCols
      )
    }

    // Write commit file using conditional write (create-if-not-exists)
    // This ensures only one concurrent writer can succeed
    const commitPath = `${this.logPath}/${formatVersion(newVersion)}.json`
    const commitData = transactionLog.serializeCommit(actions)

    try {
      await this.storage.writeConditional(commitPath, new TextEncoder().encode(commitData), null)
    } catch (error) {
      // Clear dataStore entries for files that were written before commit failed
      // This prevents memory inconsistency where dataStore has entries for uncommitted data
      this.clearDataStoreForActions(actions)

      if (error instanceof VersionMismatchError) {
        // Another writer created this version - read actual version and throw ConcurrencyError
        const actualVersion = await this.getVersionFromStorage()
        throw new ConcurrencyError({
          expectedVersion: this.currentVersion,
          actualVersion,
        })
      }
      throw error
    }

    this.currentVersion = newVersion

    // Clear in-memory cache after successful commit
    // Data has been persisted to Parquet files, so cache is no longer needed
    // This prevents memory growth and ensures reads go to actual files
    this.dataStore.clear()

    // Check if we should create a checkpoint
    if (shouldCheckpointFn(newVersion, this.checkpointConfig)) {
      try {
        await this.createCheckpoint(newVersion)
      } catch (e) {
        // Checkpoint creation failed, but commit succeeded
        // Log but don't fail the write operation
        getLogger().warn(`Failed to create checkpoint at version ${newVersion}:`, e)
      }
    }

    return { version: newVersion, timestamp, actions }
  }

  /**
   * Write rows to partitioned paths.
   * Groups rows by partition values and writes each group to a separate file.
   */
  private async writePartitioned(
    rows: T[],
    partitionCols: readonly string[],
    schema: InferredSchema,
    newVersion: number,
    timestamp: number
  ): Promise<DeltaAction[]> {
    // Group rows by partition values
    const partitionGroups = this.groupRowsByPartition(rows, partitionCols)

    const actions: DeltaAction[] = []
    let fileIndex = 0

    for (const [partitionKey, partitionRows] of partitionGroups) {
      // Parse partition values from key
      const parsedPartition: unknown = JSON.parse(partitionKey)
      if (!isValidPartitionValues(parsedPartition)) {
        throw new ValidationError('Invalid partition values format', 'partitionValues', parsedPartition)
      }
      const partitionValues = parsedPartition

      // Build partition path: table/col1=val1/col2=val2/part-*.parquet
      const partitionPath = this.buildPartitionPath(partitionValues)
      const dataPath = `${this.tablePath}/${partitionPath}/part-${formatVersion(newVersion)}-${fileIndex}.parquet`

      // Process partition rows
      const { columnData, stats } = this.processRowsOptimized(partitionRows as T[])

      // Write Parquet file
      const parquetBuffer = parquetWriteBuffer({
        columnData,
        codec: 'UNCOMPRESSED',
      })

      await this.storage.write(dataPath, new Uint8Array(parquetBuffer))

      // Store data in memory for query() to work
      this.dataStore.set(dataPath, partitionRows as T[])

      // Create add action with partition values
      const addAction: AddAction = {
        add: {
          path: dataPath,
          size: parquetBuffer.byteLength,
          modificationTime: timestamp,
          dataChange: true,
          partitionValues,
          stats: encodeStats(stats),
        },
      }
      actions.push(addAction)

      fileIndex++
    }

    // Add commit info
    actions.push({
      commitInfo: {
        timestamp,
        operation: 'WRITE',
        isBlindAppend: true,
      },
    })

    // Add protocol and metadata for first commit
    if (newVersion === 0) {
      const hasConfiguration = Object.keys(this.tableConfiguration).length > 0
      actions.unshift(
        {
          protocol: DEFAULT_PROTOCOL,
        },
        {
          metaData: {
            id: crypto.randomUUID(),
            format: { provider: 'parquet' },
            schemaString: JSON.stringify(this.schemaToParquetSchema(schema)),
            partitionColumns: partitionCols ? [...partitionCols] : [],
            createdTime: timestamp,
            ...(hasConfiguration ? { configuration: { ...this.tableConfiguration } } : {}),
          },
        }
      )
    }

    return actions
  }

  /**
   * Group rows by their partition column values.
   * Returns a Map from partition key (JSON stringified) to array of rows.
   */
  private groupRowsByPartition(
    rows: T[],
    partitionCols: readonly string[]
  ): Map<string, Record<string, unknown>[]> {
    const groups = new Map<string, Record<string, unknown>[]>()

    for (const row of rows) {
      // Extract partition values
      const partitionValues: Record<string, string> = {}
      for (const col of partitionCols) {
        const value = row[col]
        // Convert value to string for partition path
        partitionValues[col] = value === null || value === undefined ? '__HIVE_DEFAULT_PARTITION__' : String(value)
      }

      const key = JSON.stringify(partitionValues)

      if (!groups.has(key)) {
        groups.set(key, [])
      }
      const group = groups.get(key)
      if (group) {
        group.push(row)
      }
    }

    return groups
  }

  /**
   * Build the partition directory path from partition values.
   * Example: { year: '2024', month: '01' } -> 'year=2024/month=01'
   */
  private buildPartitionPath(partitionValues: Record<string, string>): string {
    return Object.entries(partitionValues)
      .map(([key, value]) => `${key}=${this.encodePartitionValue(value)}`)
      .join('/')
  }

  /**
   * URL-encode a partition value for safe use in file paths.
   * Handles special characters that may appear in partition values.
   */
  private encodePartitionValue(value: string): string {
    // Encode special characters but keep alphanumeric and some safe chars
    return value.replace(/[^a-zA-Z0-9_\-.]/g, (char) => {
      return '%' + char.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0')
    })
  }


  /**
   * Build the commit actions for a write operation
   */
  private buildWriteActions(
    dataPath: string,
    fileSize: number,
    stats: FileStats,
    schema: InferredSchema,
    newVersion: number,
    timestamp: number,
    partitionColumns?: readonly string[]
  ): DeltaAction[] {
    const actions: DeltaAction[] = [
      {
        add: {
          path: dataPath,
          size: fileSize,
          modificationTime: timestamp,
          dataChange: true,
          stats: encodeStats(stats),
        },
      },
      {
        commitInfo: {
          timestamp,
          operation: 'WRITE',
          isBlindAppend: true,
        },
      },
    ]

    // Add protocol and metadata for first commit
    if (newVersion === 0) {
      const hasConfiguration = Object.keys(this.tableConfiguration).length > 0
      actions.unshift(
        {
          protocol: DEFAULT_PROTOCOL,
        },
        {
          metaData: {
            id: crypto.randomUUID(),
            format: { provider: 'parquet' },
            schemaString: JSON.stringify(this.schemaToParquetSchema(schema)),
            partitionColumns: partitionColumns ? [...partitionColumns] : [],
            createdTime: timestamp,
            ...(hasConfiguration ? { configuration: { ...this.tableConfiguration } } : {}),
          },
        }
      )
    }

    return actions
  }

  // =============================================================================
  // SCHEMA INFERENCE AND VALIDATION HELPERS
  // =============================================================================

  /**
   * Process all rows in a single pass: infer schema, validate, convert to columns, compute stats.
   *
   * This optimized method reduces memory allocations and iteration overhead by:
   * 1. Inferring schema from the first row
   * 2. Validating and collecting column data in a single iteration
   * 3. Computing min/max/nullCount statistics during the same pass
   *
   * @param rows - Array of rows to process
   * @returns Object containing inferred schema, column data for Parquet, and file statistics
   */
  private processRowsOptimized(rows: T[]): {
    schema: InferredSchema
    columnData: ColumnSource[]
    stats: FileStats
  } {
    // Infer schema from all rows, looking at subsequent rows for null fields
    const schema = this.inferSchemaFromRows(rows)

    // Initialize column data arrays and stats tracking
    const columnArrays: Map<string, unknown[]> = new Map()
    const minValues: Record<string, unknown> = {}
    const maxValues: Record<string, unknown> = {}
    const nullCount: Record<string, number> = {}

    for (const field of schema.fields) {
      columnArrays.set(field.name, [])
      nullCount[field.name] = 0
    }

    // Process all rows in a single pass
    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
      const row = rows[rowIndex]
      if (row === undefined) continue

      for (const field of schema.fields) {
        let value = row[field.name]
        const columnData = columnArrays.get(field.name)
        if (!columnData) continue
        const currentNullCount = nullCount[field.name] ?? 0

        // Validate type consistency (skip first row as it defines the schema)
        if (rowIndex > 0 && value !== null && value !== undefined) {
          const valueType = this.inferType(value)
          if (valueType !== field.type && !this.areTypesCompatible(field.type, valueType)) {
            throw new ValidationError(
              `Type mismatch for field "${field.name}" at row ${rowIndex}: expected ${field.type} but got ${valueType}`,
              field.name,
              { expected: field.type, got: valueType, row: rowIndex }
            )
          }
        }

        // Handle null/undefined
        if (value === null || value === undefined) {
          nullCount[field.name] = currentNullCount + 1
          columnData.push(null)
          continue
        }

        // Handle binary conversion
        if (field.type === 'binary' && value instanceof ArrayBuffer) {
          value = new Uint8Array(value)
        }

        columnData.push(value)

        // Update statistics (skip for variant types)
        if (field.type !== 'variant') {
          const comparableValue = this.getComparableValue(value)
          if (comparableValue !== undefined) {
            const currentMin = minValues[field.name]
            const currentMax = maxValues[field.name]

            if (currentMin === undefined || comparableValue < (currentMin as number | string)) {
              minValues[field.name] = comparableValue
            }
            if (currentMax === undefined || comparableValue > (currentMax as number | string)) {
              maxValues[field.name] = comparableValue
            }
          }
        }
      }
    }

    // Build column data for Parquet writer
    const columnData = schema.fields.map(field => {
      const data = columnArrays.get(field.name)
      if (!data) {
        throw new ValidationError(`Missing column data for field: ${field.name}`, field.name)
      }
      return {
        name: field.name,
        data,
        type: this.mapTypeToParquet(field.type),
        nullable: true,
      }
    })

    const stats: FileStats = {
      numRecords: rows.length,
      minValues,
      maxValues,
      nullCount,
    }

    return { schema, columnData, stats }
  }

  /**
   * Process rows for writing during update/delete operations.
   * Uses existing table schema if available to ensure consistent types.
   * This handles the case where data read from Parquet might have different types
   * (e.g., BigInt vs Number) than the original schema.
   */
  private processRowsForWrite(rows: T[]): { columnData: ColumnSource[] } {
    // Use existing table schema if available, otherwise infer from rows
    const schema = this.tableSchema ?? this.inferSchemaFromRows(rows)

    const columnArrays: Map<string, unknown[]> = new Map()
    for (const field of schema.fields) {
      columnArrays.set(field.name, [])
    }

    // Process all rows
    for (const row of rows) {
      if (row === undefined) continue

      for (const field of schema.fields) {
        let value = row[field.name]
        const fieldColumnData = columnArrays.get(field.name)
        if (!fieldColumnData) continue

        // Handle null/undefined
        if (value === null || value === undefined) {
          fieldColumnData.push(null)
          continue
        }

        // Convert BigInt to Number for int32/int64 types to match schema
        if (typeof value === 'bigint') {
          if (field.type === 'int32' || field.type === 'int64' || field.type === 'double') {
            value = Number(value)
          }
        }

        // Handle binary conversion
        if (field.type === 'binary' && value instanceof ArrayBuffer) {
          value = new Uint8Array(value)
        }

        fieldColumnData.push(value)
      }
    }

    // Build column data for Parquet writer
    const columnData = schema.fields.map(field => {
      const data = columnArrays.get(field.name)
      if (!data) {
        throw new ValidationError(`Missing column data for field: ${field.name}`, field.name)
      }
      return {
        name: field.name,
        data,
        type: this.mapTypeToParquet(field.type),
        nullable: true,
      }
    })

    return { columnData }
  }

  /**
   * Get a comparable value for statistics computation.
   * Returns undefined for non-comparable types.
   */
  private getComparableValue(value: unknown): number | string | undefined {
    if (value instanceof Date) {
      return value.getTime()
    }
    if (typeof value === 'bigint') {
      return Number(value) // May lose precision but sufficient for stats
    }
    if (typeof value === 'number' || typeof value === 'string') {
      return value
    }
    if (typeof value === 'boolean') {
      return value ? 1 : 0
    }
    return undefined
  }

  /**
   * Validate schema compatibility and merge schemas in one operation.
   * This consolidates validateSchemaCompatibility and mergeSchema into a single method.
   */
  private validateAndMergeSchemas(newSchema: InferredSchema, existingSchema: InferredSchema): void {
    const existingFields = new Map(existingSchema.fields.map(f => [f.name, f]))
    const existingFieldNames = new Set(existingSchema.fields.map(f => f.name))

    // Validate and add missing fields to new schema
    for (const newField of newSchema.fields) {
      const existingField = existingFields.get(newField.name)

      if (existingField) {
        // Check type compatibility for existing fields
        if (!this.areTypesCompatible(existingField.type, newField.type)) {
          throw new ValidationError(
            `Schema mismatch: field "${newField.name}" has incompatible type (existing: ${existingField.type}, new: ${newField.type})`,
            newField.name,
            { existing: existingField.type, new: newField.type }
          )
        }
      }
    }

    // Add missing fields from existing schema to new schema (as optional)
    for (const existingField of existingSchema.fields) {
      if (!newSchema.fields.some(f => f.name === existingField.name)) {
        newSchema.fields.push({
          name: existingField.name,
          type: existingField.type,
          optional: true,
        })
      }
    }

    // Merge new fields into existing schema
    for (const newField of newSchema.fields) {
      if (!existingFieldNames.has(newField.name)) {
        existingSchema.fields.push({
          name: newField.name,
          type: newField.type,
          optional: true,
        })
      }
    }
  }

  /**
   * Infer schema from all rows, handling null values by looking at subsequent rows.
   *
   * When the first row has null/undefined for a field, we scan subsequent rows
   * to find the first non-null value and use its type. This prevents defaulting
   * to 'string' when the actual type is something else (e.g., integer).
   */
  private inferSchemaFromRows(rows: T[]): InferredSchema {
    if (rows.length === 0) {
      return { fields: [] }
    }

    // Collect all field names from ALL rows (not just the first row)
    // This handles sparse schemas where different rows may have different fields
    const fieldNames = new Set<string>()
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        fieldNames.add(key)
      }
    }

    const fields: InferredField[] = []

    for (const name of fieldNames) {
      let inferredType: InferredFieldType = 'string' // Default fallback
      let hasNull = false
      let foundNonNull = false

      // Scan all rows to find the first non-null value for this field
      for (const row of rows) {
        const value = row[name]

        if (value === null || value === undefined) {
          hasNull = true
          continue
        }

        // Found a non-null value, infer its type
        inferredType = this.inferType(value)
        foundNonNull = true
        break
      }

      // If all values are null, mark as optional with 'string' default type
      fields.push({
        name,
        type: inferredType,
        optional: hasNull || !foundNonNull,
      })
    }

    return { fields }
  }

  /**
   * Infer Parquet type from JavaScript value.
   *
   * Type mapping:
   * - null/undefined -> 'string' (default)
   * - boolean -> 'boolean'
   * - bigint -> 'int64'
   * - number (integer in int32 range) -> 'int32'
   * - number (other) -> 'double'
   * - string -> 'string'
   * - Date -> 'timestamp'
   * - Uint8Array/ArrayBuffer -> 'binary'
   * - Array/Object -> 'variant' (stored as JSON)
   */
  private inferType(value: unknown): InferredFieldType {
    if (value === null || value === undefined) {
      return 'string' // Default type for null values
    }

    if (typeof value === 'boolean') {
      return 'boolean'
    }

    if (typeof value === 'bigint') {
      return 'int64'
    }

    if (typeof value === 'number') {
      // Check if it's an integer that fits in int32 range
      if (Number.isInteger(value) && value >= INT32_MIN && value <= INT32_MAX) {
        return 'int32'
      }
      return 'double'
    }

    if (typeof value === 'string') {
      return 'string'
    }

    if (value instanceof Date) {
      return 'timestamp'
    }

    if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
      return 'binary'
    }

    if (Array.isArray(value) || (typeof value === 'object' && value !== null)) {
      return 'variant'
    }

    return 'string' // Fallback
  }

  /**
   * Check if two types are compatible (e.g., int32 -> double)
   */
  private areTypesCompatible(type1: InferredFieldType, type2: InferredFieldType): boolean {
    // Same types are compatible
    if (type1 === type2) return true

    // Numeric promotions
    const numericTypes = ['int32', 'int64', 'float', 'double']
    if (numericTypes.includes(type1) && numericTypes.includes(type2)) {
      return true
    }

    return false
  }

  /**
   * Map internal type to hyparquet-writer BasicType
   */
  private mapTypeToParquet(internalType: InferredFieldType): BasicType {
    switch (internalType) {
      case 'boolean':
        return 'BOOLEAN'
      case 'int32':
        return 'INT32'
      case 'int64':
        return 'INT64'
      case 'float':
        return 'FLOAT'
      case 'double':
        return 'DOUBLE'
      case 'string':
        return 'STRING'
      case 'binary':
        return 'BYTE_ARRAY'
      case 'variant':
        return 'JSON' // Use JSON type for variant - hyparquet will auto-decode on read
      case 'timestamp':
        return 'TIMESTAMP'
      default:
        return 'STRING'
    }
  }

  /**
   * Convert internal schema to Parquet schema format
   */
  private schemaToParquetSchema(schema: InferredSchema): Record<string, unknown> {
    return {
      type: 'struct',
      fields: schema.fields.map(f => ({
        name: f.name,
        type: f.type,
        nullable: f.optional,
      })),
    }
  }

  /**
   * Query the table with a filter
   *
   * @param filter - Optional MongoDB-style filter to apply
   * @param options - Optional query options for snapshot isolation and projection
   * @returns Array of matching rows (may be partial if projection is specified)
   *
   * @example
   * // Query all rows
   * const allRows = await table.query()
   *
   * // Query with equality filter
   * const activeUsers = await table.query({ active: true })
   *
   * // Query with comparison operators
   * const adults = await table.query({ age: { $gte: 18 } })
   *
   * // Query with logical operators
   * const results = await table.query({
   *   $or: [{ status: 'active' }, { role: 'admin' }]
   * })
   *
   * // Query at a specific version (time travel)
   * const oldData = await table.query({ active: true }, { version: 5 })
   *
   * // Query with a pre-fetched snapshot for consistent reads
   * const snapshot = await table.snapshot(5)
   * const result1 = await table.query({ type: 'A' }, { snapshot })
   * const result2 = await table.query({ type: 'B' }, { snapshot })
   *
   * // Query with projection (return only specified fields)
   * const names = await table.query({}, { projection: ['name', 'age'] })
   * const withMetadata = await table.query({}, { projection: { name: 1, 'metadata.tier': 1 } })
   */
  async query(filter?: Filter<T>, options?: QueryOptions<T>): Promise<Partial<T>[]> {
    // Validate filter if provided - must be an object (not primitive, not array)
    if (filter !== undefined && filter !== null) {
      if (typeof filter !== 'object' || Array.isArray(filter)) {
        throw new ValidationError(
          'filter must be an object',
          'filter',
          filter
        )
      }
    }

    // Validate filter structure if provided
    if (filter && !this.isEmptyFilter(filter)) {
      this.validateFilter(filter)
    }

    // Validate options if provided
    if (options) {
      // Validate version is a non-negative integer if provided
      if (options.version !== undefined) {
        if (typeof options.version !== 'number' || !Number.isInteger(options.version) || options.version < 0) {
          throw new ValidationError(
            'options.version must be a non-negative integer',
            'options.version',
            options.version
          )
        }
      }

      // Validate snapshot if provided
      if (options.snapshot !== undefined) {
        if (typeof options.snapshot !== 'object' || options.snapshot === null) {
          throw new ValidationError(
            'options.snapshot must be a valid DeltaSnapshot object',
            'options.snapshot',
            options.snapshot
          )
        }
        if (typeof options.snapshot.version !== 'number' || !Array.isArray(options.snapshot.files)) {
          throw new ValidationError(
            'options.snapshot must have valid version (number) and files (array) properties',
            'options.snapshot',
            options.snapshot
          )
        }
      }

      // Validate projection if provided
      if (options.projection !== undefined && options.projection !== null) {
        if (typeof options.projection !== 'object') {
          throw new ValidationError(
            'options.projection must be an array of field names or an object with inclusion/exclusion flags',
            'options.projection',
            options.projection
          )
        }
      }
    }

    // Ensure metadata (including partitionColumns) is loaded for fallback
    await this.ensureMetadataLoaded()

    // Determine which snapshot to use
    let snapshot: DeltaSnapshot
    if (options?.snapshot) {
      // Use the provided snapshot directly
      snapshot = options.snapshot
    } else if (options?.version !== undefined) {
      // Get snapshot at the specified version
      snapshot = await this.snapshot(options.version)
    } else {
      // Get current table snapshot
      snapshot = await this.snapshot()
    }

    // Early return for empty table
    if (snapshot.files.length === 0) {
      return []
    }

    // Track columns needed for projection (for future Parquet optimization)
    if (options?.projection) {
      const columns = getProjectionColumns(options.projection)
      // columns array can be used to optimize Parquet column reads
      // When columns.length > 0, we only need to read those columns from Parquet
      // This will be implemented when we add real Parquet reading
      this.lastQueryProjectionColumns = columns
    } else {
      this.lastQueryProjectionColumns = undefined
    }

    // Apply partition pruning - filter out files that don't match partition predicates
    const partitionColumns = snapshot.metadata?.partitionColumns ?? this.partitionColumns
    let filesToRead = snapshot.files
    this.lastQuerySkippedFiles = 0
    if (filter && partitionColumns.length > 0) {
      const partitionPredicates = this.extractPartitionPredicates(filter, partitionColumns)
      if (partitionPredicates) {
        filesToRead = this.pruneFilesByPartition(snapshot.files, partitionPredicates)
        // Track which files were pruned for debugging
        this.lastQuerySkippedFiles = snapshot.files.length - filesToRead.length
      }
    }

    // Load rows from filtered data files (reads actual Parquet files)
    // Pass metadata for column mapping support
    const allRows = await this.loadRowsFromFiles(filesToRead, snapshot.metadata)

    // Apply filter to rows
    let results: T[]
    if (!filter || this.isEmptyFilter(filter)) {
      results = allRows
    } else {
      results = this.applyFilter(allRows, filter)
    }

    // Apply projection if specified
    if (options?.projection) {
      return applyProjection(results, options.projection)
    }

    return results
  }

  /**
   * Columns identified for projection during the last query
   * Used for testing/debugging Parquet column pruning optimization
   */
  lastQueryProjectionColumns?: string[] | undefined

  /**
   * Number of files skipped by partition pruning in the last query.
   * Used for testing/debugging partition pruning optimization.
   */
  lastQuerySkippedFiles?: number | undefined

  /**
   * Extract partition predicates from a filter.
   * Only extracts simple equality and $in predicates on partition columns.
   * Returns null if no partition predicates can be extracted.
   */
  private extractPartitionPredicates(
    filter: Filter<T>,
    partitionColumns: string[]
  ): Map<string, Set<string>> | null {
    const predicates = new Map<string, Set<string>>()

    for (const [key, condition] of Object.entries(filter)) {
      // Skip logical operators
      if (key.startsWith('$')) continue

      // Check if this is a partition column
      if (!partitionColumns.includes(key)) continue

      // Handle equality predicate
      if (typeof condition !== 'object' || condition === null) {
        // Direct equality: { partCol: 'value' }
        predicates.set(key, new Set([String(condition)]))
        continue
      }

      // Handle comparison operators
      const conditionObj = condition as Record<string, unknown>

      if ('$eq' in conditionObj) {
        predicates.set(key, new Set([String(conditionObj.$eq)]))
      } else if ('$in' in conditionObj && Array.isArray(conditionObj.$in)) {
        predicates.set(key, new Set(conditionObj.$in.map(v => String(v))))
      }
      // Note: We don't handle $ne, $nin, range operators for partition pruning
      // as they would require checking all non-matching partitions
    }

    return predicates.size > 0 ? predicates : null
  }

  /**
   * Prune files that don't match partition predicates.
   * A file is skipped if any of its partition values don't match the corresponding predicate.
   */
  private pruneFilesByPartition(
    files: AddAction['add'][],
    predicates: Map<string, Set<string>>
  ): AddAction['add'][] {
    return files.filter(file => {
      // Non-partitioned files pass through
      if (!file.partitionValues) return true

      // Check each predicate against the file's partition values
      for (const [column, allowedValues] of predicates) {
        const fileValue = file.partitionValues[column]

        // If the file has no value for this partition column, include it
        // (might be a non-partitioned file or missing partition value)
        if (fileValue === undefined) continue

        // If the file's partition value is not in the allowed set, skip it
        if (!allowedValues.has(fileValue)) {
          return false
        }
      }

      return true
    })
  }

  /**
   * Load rows from specific files (used after partition pruning).
   * First checks in-memory dataStore, then falls back to reading actual Parquet files.
   * Partition values are extracted from file metadata or path and merged into each row.
   * Column mapping is applied when enabled via table configuration.
   *
   * @param files - List of files to read
   * @param metadata - Table metadata (used for column mapping)
   */
  private async loadRowsFromFiles(
    files: AddAction['add'][],
    metadata?: MetadataAction['metaData']
  ): Promise<T[]> {
    const allRows: T[] = []

    // Build column mapping if enabled (physical name -> logical name)
    const columnMapping = buildColumnMapping(metadata)

    for (const file of files) {
      // Get partition values from file metadata or extract from path
      // The metadata partitionValues takes precedence, but we fall back to path extraction
      // for cases like reading external Delta tables where metadata might not include it
      const partitionValues = file.partitionValues && Object.keys(file.partitionValues).length > 0
        ? file.partitionValues
        : extractPartitionValuesFromPath(file.path)

      const hasPartitions = Object.keys(partitionValues).length > 0

      // Load deletion vector if present (marks rows as soft-deleted)
      let deletedRows: Set<number> | null = null
      if (file.deletionVector) {
        try {
          deletedRows = await loadDeletionVector(this.storage, this.tablePath, file.deletionVector)
        } catch (error) {
          getLogger().warn(`Failed to load deletion vector for ${file.path}:`, error)
          // Continue without deletion vector - all rows will be included
        }
      }

      // First check in-memory dataStore for recently written data
      const cachedRows = this.dataStore.get(file.path)
      if (cachedRows && cachedRows.length > 0) {
        for (let rowIdx = 0; rowIdx < cachedRows.length; rowIdx++) {
          // Skip rows marked as deleted by the deletion vector
          if (deletedRows && deletedRows.has(rowIdx)) {
            continue
          }
          const row = cachedRows[rowIdx]
          if (row === undefined) continue
          if (hasPartitions) {
            allRows.push({ ...partitionValues, ...row } as T)
          } else {
            allRows.push(row)
          }
        }
        continue
      }

      // Read from actual Parquet file using shared utility
      // Decode URL-encoded characters in path (e.g., %20 for space, %3A for colon)
      try {
        const decodedPath = decodeFilePath(file.path)
        let rows = await readParquetFile<T>(this.storage, decodedPath)

        // Apply column mapping if enabled (rename physical columns to logical names)
        if (columnMapping) {
          rows = rows.map(row => applyColumnMapping(row, columnMapping))
        }

        // Filter out deleted rows and add to results
        for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
          // Skip rows marked as deleted by the deletion vector
          if (deletedRows && deletedRows.has(rowIdx)) {
            continue
          }
          const row = rows[rowIdx]
          if (row === undefined) continue
          if (hasPartitions) {
            allRows.push({ ...partitionValues, ...row } as T)
          } else {
            allRows.push(row)
          }
        }
      } catch (error) {
        // File might not exist (e.g., removed or not yet written)
        // Skip and continue to next file
        getLogger().warn(`Failed to read Parquet file ${file.path}:`, error)
      }
    }

    return allRows
  }

  /**
   * Check if a filter is empty (no conditions)
   * Uses a type predicate to narrow the type for TypeScript
   */
  private isEmptyFilter(filter: Filter<T>): filter is Record<string, never> {
    return Object.keys(filter).length === 0
  }

  /**
   * Apply a filter to an array of rows
   */
  private applyFilter(rows: T[], filter: Filter<T>): T[] {
    // Fast path for simple equality filters (most common case)
    if (this.isSimpleEqualityFilter(filter)) {
      return this.applySimpleEqualityFilter(rows, filter)
    }

    // Full filter evaluation for complex filters
    return rows.filter(row => matchesFilter(row, filter))
  }

  /**
   * Check if filter contains only simple equality conditions (no operators)
   * e.g., { name: 'Alice', active: true }
   */
  private isSimpleEqualityFilter(filter: Filter<T>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      // Logical operators require full evaluation
      if (key.startsWith('$')) return false

      // Nested objects with operators require full evaluation
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const keys = Object.keys(value)
        if (keys.some(k => k.startsWith('$'))) return false
      }
    }
    return true
  }

  /**
   * Optimized filter for simple equality conditions
   * Avoids function call overhead of matchesFilter for basic cases
   */
  private applySimpleEqualityFilter(rows: T[], filter: Filter<T>): T[] {
    const conditions = Object.entries(filter)
    const numConditions = conditions.length

    return rows.filter(row => {
      for (let i = 0; i < numConditions; i++) {
        const condition = conditions[i]
        if (condition === undefined) continue
        const [key, value] = condition
        // Support nested path access for simple equality too
        const rowValue = key.includes('.') ? getNestedValue(row, key) : row[key]
        if (rowValue !== value) return false
      }
      return true
    })
  }

  /**
   * Validate filter structure and provide helpful error messages
   *
   * @throws Error if filter contains invalid operators or structure
   */
  private validateFilter(filter: Filter<T>): void {
    this.validateFilterRecursive(filter, '')
  }

  /**
   * Recursively validate filter structure
   */
  private validateFilterRecursive(filter: Filter<T>, path: string): void {
    if (typeof filter !== 'object' || filter === null) {
      const location = path ? ` at "${path}"` : ''
      throw new ValidationError(`Filter${location} must be an object, got ${typeof filter}`, 'filter', filter)
    }

    for (const [key, value] of Object.entries(filter)) {
      const currentPath = path ? `${path}.${key}` : key

      // Validate logical operators
      if (key === '$and' || key === '$or' || key === '$nor') {
        if (!Array.isArray(value)) {
          throw new ValidationError(`Filter operator ${key} requires an array, got ${typeof value}`, key, value)
        }
        for (let i = 0; i < value.length; i++) {
          this.validateFilterRecursive(value[i] as Filter<T>, `${currentPath}[${i}]`)
        }
        continue
      }

      if (key === '$not') {
        if (typeof value !== 'object' || value === null || Array.isArray(value)) {
          throw new ValidationError(`Filter operator $not requires an object, got ${Array.isArray(value) ? 'array' : typeof value}`, '$not', value)
        }
        this.validateFilterRecursive(value as Filter<T>, currentPath)
        continue
      }

      // Validate comparison operators in field conditions
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        for (const [op] of Object.entries(value)) {
          if (op.startsWith('$')) {
            if (!VALID_COMPARISON_OPS.includes(op as typeof VALID_COMPARISON_OPS[number])) {
              throw new ValidationError(`Unknown comparison operator "${op}" at "${currentPath}". Valid operators: ${VALID_COMPARISON_OPS.join(', ')}`, currentPath, op)
            }
          }
        }
      }
    }
  }

  // =============================================================================
  // STREAMING QUERY METHODS
  // =============================================================================

  /**
   * Query rows as an async iterator, yielding one row at a time.
   *
   * This method provides memory-efficient streaming access to query results.
   * Rows are yielded one at a time, allowing early termination with `break`.
   *
   * **Recommended for large tables** - Unlike `query()` which loads all results
   * into memory, this method streams rows one at a time, keeping memory usage
   * constant regardless of result set size.
   *
   * @param filter - Optional MongoDB-style filter for matching rows
   * @param options - Optional query options (version, projection)
   * @returns AsyncIterableIterator that yields rows one at a time
   * @throws ValidationError if filter is not an object
   *
   * @example
   * ```typescript
   * // Iterate through all rows - memory efficient for large tables
   * for await (const row of table.queryIterator()) {
   *   console.log(row)
   * }
   *
   * // With filter and early termination
   * for await (const row of table.queryIterator({ active: true })) {
   *   if (row.id === 'target') break
   * }
   * ```
   */
  async *queryIterator(
    filter?: Filter<T>,
    options?: Omit<QueryOptions<T>, 'snapshot'>
  ): AsyncIterableIterator<Partial<T>> {
    // Validate filter if provided
    if (filter !== undefined && filter !== null) {
      if (typeof filter !== 'object' || Array.isArray(filter)) {
        throw new ValidationError('filter must be an object', 'filter', filter)
      }
      this.validateFilter(filter)
    }

    // Get snapshot
    const targetVersion = options?.version
    const snapshot = targetVersion !== undefined
      ? await this.snapshot(targetVersion)
      : await this.snapshot()

    if (snapshot.files.length === 0) {
      return
    }

    // Get projection columns
    const projectionColumns = options?.projection
      ? getProjectionColumns(options.projection)
      : undefined

    // Process each file and yield rows
    for (const file of snapshot.files) {
      let rows = this.dataStore.get(file.path)
      if (!rows || rows.length === 0) {
        try {
          const decodedPath = decodeFilePath(file.path)
          rows = await readParquetFile<T>(this.storage, decodedPath)
        } catch (e) {
          getLogger().warn(`[DeltaTable] Failed to read Parquet file ${file.path}:`, e)
          continue
        }
      }

      for (const row of rows) {
        // Apply filter
        if (filter && !matchesFilter(row, filter)) {
          continue
        }

        // Apply projection and yield
        if (projectionColumns) {
          yield applyProjectionToDoc(row as Record<string, unknown>, projectionColumns) as Partial<T>
        } else {
          yield row as Partial<T>
        }
      }
    }
  }

  /**
   * Query rows as an async iterator, yielding batches of rows.
   *
   * This method provides memory-efficient streaming access to query results
   * in configurable batch sizes. Useful for processing large datasets
   * in chunks without loading everything into memory.
   *
   * **Recommended for large tables** - Unlike `query()` which loads all results
   * into memory at once, this method streams results in configurable batches,
   * limiting peak memory usage to approximately `batchSize` rows.
   *
   * @param filter - Optional MongoDB-style filter for matching rows
   * @param batchSize - Number of rows per batch (default: 1000). Smaller batches
   *   use less memory but may have higher overhead. Adjust based on row size.
   * @param options - Optional query options (version, projection)
   * @returns AsyncIterableIterator that yields arrays of rows
   * @throws ValidationError if filter is not an object or batchSize is invalid
   *
   * @example
   * ```typescript
   * // Process in batches of 100 - memory efficient for large tables
   * for await (const batch of table.queryBatch({}, 100)) {
   *   await processBatch(batch)
   * }
   *
   * // With filter
   * for await (const batch of table.queryBatch({ status: 'pending' }, 50)) {
   *   console.log(`Processing ${batch.length} pending items`)
   * }
   * ```
   */
  async *queryBatch(
    filter?: Filter<T>,
    batchSize: number = 1000,
    options?: Omit<QueryOptions<T>, 'snapshot'>
  ): AsyncIterableIterator<Partial<T>[]> {
    // Validate batchSize
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new ValidationError('batchSize must be a positive integer', 'batchSize', batchSize)
    }

    // Validate filter if provided
    if (filter !== undefined && filter !== null) {
      if (typeof filter !== 'object' || Array.isArray(filter)) {
        throw new ValidationError('filter must be an object', 'filter', filter)
      }
      this.validateFilter(filter)
    }

    // Get snapshot
    const targetVersion = options?.version
    const snapshot = targetVersion !== undefined
      ? await this.snapshot(targetVersion)
      : await this.snapshot()

    if (snapshot.files.length === 0) {
      return
    }

    // Get projection columns
    const projectionColumns = options?.projection
      ? getProjectionColumns(options.projection)
      : undefined

    // Collect rows into batches
    let currentBatch: Partial<T>[] = []

    for (const file of snapshot.files) {
      let rows = this.dataStore.get(file.path)
      if (!rows || rows.length === 0) {
        try {
          const decodedPath = decodeFilePath(file.path)
          rows = await readParquetFile<T>(this.storage, decodedPath)
        } catch (e) {
          getLogger().warn(`[DeltaTable] Failed to read Parquet file ${file.path}:`, e)
          continue
        }
      }

      for (const row of rows) {
        // Apply filter
        if (filter && !matchesFilter(row, filter)) {
          continue
        }

        // Apply projection
        const processedRow = projectionColumns
          ? applyProjectionToDoc(row as Record<string, unknown>, projectionColumns) as Partial<T>
          : row as Partial<T>

        currentBatch.push(processedRow)

        // Yield batch when full
        if (currentBatch.length >= batchSize) {
          yield currentBatch
          currentBatch = []
        }
      }
    }

    // Yield remaining rows if any
    if (currentBatch.length > 0) {
      yield currentBatch
    }
  }

  // =============================================================================
  // CHECKPOINT WRAPPER METHOD (for backwards compatibility with tests)
  // =============================================================================

  /**
   * Read checkpoint data at a specific version.
   *
   * This method is provided for testing and debugging purposes.
   * It reads the checkpoint file at the specified version and returns
   * the snapshot data.
   *
   * @param version - The version number of the checkpoint to read
   * @returns The snapshot reconstructed from the checkpoint
   * @internal
   */
  async readCheckpoint(version: number): Promise<DeltaSnapshot> {
    return readCheckpoint(this.storage, this.logPath, version)
  }

  /**
   * Read a specific part of a multi-part checkpoint.
   *
   * @param version - The checkpoint version
   * @param part - The part number (1-indexed)
   * @param totalParts - The total number of parts
   * @returns The snapshot from the checkpoint part
   * @internal
   */
  async readCheckpointPart(version: number, part: number, totalParts: number): Promise<DeltaSnapshot> {
    return readCheckpointPart(this.storage, this.logPath, version, part, totalParts)
  }

  /**
   * Read the _last_checkpoint file to find the latest checkpoint info.
   *
   * @returns The last checkpoint info, or null if no checkpoint exists
   * @internal
   */
  async readLastCheckpoint(): Promise<LastCheckpoint | null> {
    return readLastCheckpoint(this.storage, this.logPath)
  }

  /**
   * Discover all checkpoint versions by scanning the _delta_log directory.
   *
   * @returns Array of checkpoint versions sorted by version descending
   * @internal
   */
  async discoverCheckpoints(): Promise<number[]> {
    return discoverCheckpoints(this.storage, this.logPath)
  }

  /**
   * Find the latest checkpoint version.
   *
   * @returns The latest checkpoint version, or null if no checkpoints exist
   * @internal
   */
  async findLatestCheckpoint(): Promise<number | null> {
    return findLatestCheckpoint(this.storage, this.logPath)
  }

  /**
   * Validate that a checkpoint file exists and is readable.
   *
   * @param version - The checkpoint version to validate
   * @returns true if the checkpoint is valid, false otherwise
   * @internal
   */
  async validateCheckpoint(version: number): Promise<boolean> {
    return validateCheckpoint(this.storage, this.logPath, version)
  }

  /**
   * Clean up old checkpoints based on the retention policy.
   *
   * @internal
   */
  async cleanupCheckpoints(): Promise<void> {
    // Note: This uses an empty Map for checkpoint timestamps since we don't
    // track them internally. Time-based retention won't work without timestamps.
    return cleanupCheckpoints(this.storage, this.logPath, this.checkpointConfig, new Map())
  }

  /**
   * Clean up old log files (older than the oldest checkpoint).
   *
   * @internal
   */
  async cleanupLogs(): Promise<void> {
    return cleanupLogs(this.storage, this.logPath)
  }

  /**
   * Get log file versions that can be safely cleaned up.
   * These are versions older than the oldest retained checkpoint.
   *
   * @returns Array of version numbers that can be cleaned up
   * @internal
   */
  async getCleanableLogVersions(): Promise<number[]> {
    return getCleanableLogVersions(this.storage, this.logPath)
  }

  /**
   * Delete rows matching a filter.
   *
   * Removes all rows that match the provided filter from the table.
   * This creates a new commit with remove actions for affected files
   * and add actions for files containing remaining rows.
   *
   * @param filter - MongoDB-style filter to identify rows to delete
   * @returns The commit information
   * @throws Error if no rows match the filter
   * @throws ConcurrencyError if another writer modified the table
   *
   * @example
   * ```typescript
   * // Delete all inactive users
   * await table.delete({ active: false })
   *
   * // Delete a specific user
   * await table.delete({ _id: 'user-123' })
   * ```
   */
  async delete(filter: Filter<T>): Promise<DeltaCommit> {
    // Validate filter is required and must be an object
    validateRequired(filter, 'filter')
    if (typeof filter !== 'object' || Array.isArray(filter)) {
      throw new ValidationError('filter must be an object', 'filter', filter)
    }

    // Check for version conflict before proceeding
    await this.checkVersionConflict()

    const snapshot = await this.snapshot()
    const actions: DeltaAction[] = []

    // Find files that contain matching rows
    for (const file of snapshot.files) {
      // Load rows from dataStore or from Parquet file
      let rows = this.dataStore.get(file.path)
      if (!rows || rows.length === 0) {
        // Read from actual Parquet file using shared utility
        // Decode URL-encoded characters in path (e.g., %20 for space, %3A for colon)
        try {
          const decodedPath = decodeFilePath(file.path)
          rows = await readParquetFile<T>(this.storage, decodedPath)
        } catch (e) {
          // File might not exist or be corrupted - log and continue
          getLogger().warn(`[DeltaTable] Failed to read Parquet file ${file.path} for delete operation:`, e)
          rows = []
        }
      }

      const matchingRows = rows.filter(row => matchesFilter(row, filter))

      if (matchingRows.length > 0) {
        // Remove the old file
        actions.push({
          remove: {
            path: file.path,
            deletionTimestamp: Date.now(),
            dataChange: true,
            size: file.size,
          },
        })

        // Add back non-matching rows if any
        const remainingRows = rows.filter(row => !matchesFilter(row, filter))
        if (remainingRows.length > 0) {
          const currentVersion = await this.version()
          const newVersion = currentVersion + 1
          const newPath = `${this.tablePath}/part-${formatVersion(newVersion)}-${Date.now()}.parquet`

          // Write the actual Parquet file to storage
          // Use existing table schema if available to ensure consistent types
          const { columnData } = this.processRowsForWrite(remainingRows)
          const parquetBuffer = parquetWriteBuffer({
            columnData,
            codec: 'UNCOMPRESSED',
          })
          await this.storage.write(newPath, new Uint8Array(parquetBuffer))

          // Update in-memory store for fast subsequent reads
          this.dataStore.set(newPath, remainingRows)
          const actualSize = parquetBuffer.byteLength

          actions.push({
            add: {
              path: newPath,
              size: actualSize,
              modificationTime: Date.now(),
              dataChange: true,
            },
          })
        }

        // Remove from in-memory store
        this.dataStore.delete(file.path)
      }
    }

    if (actions.length === 0) {
      throw new ValidationError('No rows matched the filter', 'filter', filter)
    }

    return this.commitInternal(actions)
  }

  /**
   * Update rows matching a filter.
   *
   * Applies the provided updates to all rows that match the filter.
   * This creates a new commit that removes affected files and adds
   * new files containing the updated data.
   *
   * @param filter - MongoDB-style filter to identify rows to update
   * @param updates - Partial object with fields to update
   * @returns The commit information
   * @throws Error if no rows match the filter
   * @throws ConcurrencyError if another writer modified the table
   *
   * @example
   * ```typescript
   * // Set all users as inactive
   * await table.update({}, { active: false })
   *
   * // Update a specific user's score
   * await table.update({ _id: 'user-123' }, { score: 100 })
   * ```
   */
  async update(filter: Filter<T>, updates: Partial<T>): Promise<DeltaCommit> {
    // Validate filter is required and must be an object
    validateRequired(filter, 'filter')
    if (typeof filter !== 'object' || Array.isArray(filter)) {
      throw new ValidationError('filter must be an object', 'filter', filter)
    }

    // Validate updates is required and must be an object
    validateRequired(updates, 'updates')
    if (typeof updates !== 'object' || Array.isArray(updates)) {
      throw new ValidationError('updates must be an object', 'updates', updates)
    }

    // Check for version conflict before proceeding
    await this.checkVersionConflict()

    const snapshot = await this.snapshot()
    const actions: DeltaAction[] = []

    // Find files that contain matching rows
    for (const file of snapshot.files) {
      // Load rows from dataStore or from Parquet file
      let rows = this.dataStore.get(file.path)
      if (!rows || rows.length === 0) {
        // Read from actual Parquet file using shared utility
        // Decode URL-encoded characters in path (e.g., %20 for space, %3A for colon)
        try {
          const decodedPath = decodeFilePath(file.path)
          rows = await readParquetFile<T>(this.storage, decodedPath)
        } catch (e) {
          // File might not exist or be corrupted - log and continue
          getLogger().warn(`[DeltaTable] Failed to read Parquet file ${file.path} for update operation:`, e)
          rows = []
        }
      }

      const hasMatch = rows.some(row => matchesFilter(row, filter))

      if (hasMatch) {
        // Remove the old file
        actions.push({
          remove: {
            path: file.path,
            deletionTimestamp: Date.now(),
            dataChange: true,
            size: file.size,
          },
        })

        // Update matching rows
        const updatedRows = rows.map(row => {
          if (matchesFilter(row, filter)) {
            return { ...row, ...updates }
          }
          return row
        })

        const currentVersion = await this.version()
        const newVersion = currentVersion + 1
        const newPath = `${this.tablePath}/part-${formatVersion(newVersion)}-${Date.now()}.parquet`

        // Write the actual Parquet file to storage
        // Use existing table schema if available to ensure consistent types
        const { columnData } = this.processRowsForWrite(updatedRows)
        const parquetBuffer = parquetWriteBuffer({
          columnData,
          codec: 'UNCOMPRESSED',
        })
        await this.storage.write(newPath, new Uint8Array(parquetBuffer))

        // Update in-memory store for fast subsequent reads
        this.dataStore.set(newPath, updatedRows)
        const actualSize = parquetBuffer.byteLength

        actions.push({
          add: {
            path: newPath,
            size: actualSize,
            modificationTime: Date.now(),
            dataChange: true,
          },
        })

        // Remove from in-memory store
        this.dataStore.delete(file.path)
      }
    }

    if (actions.length === 0) {
      throw new ValidationError('No rows matched the filter', 'filter', filter)
    }

    return this.commitInternal(actions)
  }

  /**
   * Update table metadata.
   *
   * Updates the table's metadata action with the provided values.
   * This is useful for changing table name, description, or configuration.
   *
   * @param metadata - Partial metadata object with fields to update
   * @returns The commit information
   * @throws ConcurrencyError if another writer modified the table
   *
   * @example
   * ```typescript
   * await table.updateMetadata({
   *   name: 'users',
   *   description: 'Table containing user records',
   *   configuration: { 'delta.appendOnly': 'true' }
   * })
   * ```
   */
  async updateMetadata(metadata: Partial<MetadataAction['metaData']>): Promise<DeltaCommit> {
    // Check for version conflict before proceeding
    await this.checkVersionConflict()

    const snapshot = await this.snapshot()
    const currentMetadata = snapshot.metadata || {
      id: crypto.randomUUID(),
      format: { provider: 'parquet' },
      schemaString: '{}',
      partitionColumns: [],
      createdTime: Date.now(),
    }

    const actions: DeltaAction[] = [
      {
        metaData: {
          ...currentMetadata,
          ...metadata,
        },
      },
    ]

    return this.commitInternal(actions)
  }

  /**
   * Create a commit with custom actions.
   *
   * This is a low-level API for advanced operations like compaction,
   * custom file management, or direct action manipulation.
   * Checks for version conflicts before committing.
   *
   * @param actions - Array of Delta actions to commit
   * @returns The commit information
   * @throws ConcurrencyError if another writer modified the table
   *
   * @example
   * ```typescript
   * // Custom compaction commit
   * await table.commit([
   *   { remove: { path: 'old-file-1.parquet', deletionTimestamp: Date.now(), dataChange: false } },
   *   { remove: { path: 'old-file-2.parquet', deletionTimestamp: Date.now(), dataChange: false } },
   *   { add: { path: 'compacted-file.parquet', size: 1024, modificationTime: Date.now(), dataChange: false } },
   * ])
   * ```
   */
  async commit(actions: DeltaAction[]): Promise<DeltaCommit> {
    // Check for version conflict before committing
    await this.checkVersionConflict()

    return this.commitInternal(actions)
  }

  /**
   * Internal commit method that skips version conflict check.
   * Used by methods that have already checked for conflicts.
   */
  private async commitInternal(actions: DeltaAction[]): Promise<DeltaCommit> {
    const currentVersion = await this.version()
    const newVersion = currentVersion + 1

    // Process actions to update dataStore
    for (const action of actions) {
      if ('remove' in action) {
        // Remove data from dataStore when file is removed
        this.dataStore.delete(action.remove.path)
      }
    }

    // Add commitInfo if not present
    const hasCommitInfo = actions.some(a => 'commitInfo' in a)
    if (!hasCommitInfo) {
      actions.push({
        commitInfo: {
          timestamp: Date.now(),
          operation: 'COMPACTION',
        },
      })
    }

    // Write commit file using conditional write (create-if-not-exists)
    // This ensures only one concurrent writer can succeed
    const commitPath = `${this.logPath}/${formatVersion(newVersion)}.json`
    const commitData = transactionLog.serializeCommit(actions)

    try {
      await this.storage.writeConditional(commitPath, new TextEncoder().encode(commitData), null)
    } catch (error) {
      if (error instanceof VersionMismatchError) {
        // Another writer created this version - read actual version and throw ConcurrencyError
        const actualVersion = await this.getVersionFromStorage()
        throw new ConcurrencyError({
          expectedVersion: this.currentVersion,
          actualVersion,
        })
      }
      throw error
    }

    this.currentVersion = newVersion

    // Clear in-memory cache after successful commit
    // Data has been persisted to Parquet files, so cache is no longer needed
    // This prevents memory growth and ensures reads go to actual files
    this.dataStore.clear()

    // Check if we should create a checkpoint
    if (shouldCheckpointFn(newVersion, this.checkpointConfig)) {
      try {
        await this.createCheckpoint(newVersion)
      } catch (e) {
        // Checkpoint creation failed, but commit succeeded
        getLogger().warn(`Failed to create checkpoint at version ${newVersion}:`, e)
      }
    }

    return { version: newVersion, timestamp: Date.now(), actions }
  }

  // =============================================================================
  // COMPACTION SUPPORT METHODS (LEGACY)
  // =============================================================================
  //
  // Note: These methods are kept for backward compatibility.
  // For new code, prefer using getCompactionContext() which provides
  // better encapsulation and a cleaner API for maintenance operations.
  //

  /**
   * Get the storage backend for direct file operations.
   *
   * @deprecated Use `getCompactionContext().storage` instead for better encapsulation.
   *
   * This method is primarily intended for compaction and other maintenance
   * operations that need direct access to the underlying storage.
   *
   * @returns The storage backend instance
   *
   * @example
   * ```typescript
   * // Preferred approach:
   * const ctx = table.getCompactionContext()
   * const data = await ctx.storage.read('path/to/file.parquet')
   *
   * // Legacy approach (deprecated):
   * const storage = table.getStorage()
   * const data = await storage.read('path/to/file.parquet')
   * ```
   */
  getStorage(): StorageBackend {
    return this.storage
  }

  /**
   * Get the base path of the table.
   *
   * @deprecated Use `getCompactionContext().tablePath` instead for better encapsulation.
   *
   * This method is primarily intended for compaction and other maintenance
   * operations that need to construct file paths.
   *
   * @returns The table path
   *
   * @example
   * ```typescript
   * // Preferred approach:
   * const ctx = table.getCompactionContext()
   * const dataPath = `${ctx.tablePath}/part-00001.parquet`
   *
   * // Legacy approach (deprecated):
   * const path = table.getTablePath()
   * const dataPath = `${path}/part-00001.parquet`
   * ```
   */
  getTablePath(): string {
    return this.tablePath
  }

  /**
   * Read rows stored for a specific file path (cache only).
   *
   * @deprecated Use `getCompactionContext().readFile()` instead which provides
   * automatic fallback to Parquet files and better encapsulation.
   *
   * This method provides access to in-memory cached data for a file.
   * Returns undefined if the data is not in cache.
   *
   * @param path - The file path to read rows for
   * @returns Array of rows for the file, or undefined if not in cache
   *
   * @example
   * ```typescript
   * // Preferred approach:
   * const ctx = table.getCompactionContext()
   * const rows = await ctx.readFile('data/part-00000.parquet')
   *
   * // Legacy approach (deprecated):
   * const rows = table.readFileRows('data/part-00000.parquet')
   * ```
   */
  readFileRows(path: string): T[] | undefined {
    return this.dataStore.get(path)
  }

  /**
   * Read rows for a specific file path with fallback to Parquet files.
   *
   * @deprecated Use `getCompactionContext().readFile()` instead for better encapsulation.
   *
   * This method first checks the in-memory cache, then falls back to reading
   * from the actual Parquet file if the data is not cached. This is the
   * recommended method for reading file data, especially after commits
   * which clear the cache.
   *
   * @param path - The file path to read rows for
   * @returns Array of rows for the file, or empty array if file doesn't exist
   *
   * @example
   * ```typescript
   * // Preferred approach:
   * const ctx = table.getCompactionContext()
   * const rows = await ctx.readFile('data/part-00000.parquet')
   *
   * // Legacy approach (deprecated):
   * const rows = await table.readFileRowsAsync('data/part-00000.parquet')
   * ```
   */
  async readFileRowsAsync(path: string): Promise<T[]> {
    // First check in-memory cache
    const cached = this.dataStore.get(path)
    if (cached) {
      return cached
    }

    // Fall back to reading from Parquet file
    try {
      const decodedPath = decodeFilePath(path)
      const rows = await readParquetFile<T>(this.storage, decodedPath)
      return rows
    } catch (e) {
      // File might not exist or be corrupted
      getLogger().warn(`[DeltaTable] Failed to read Parquet file ${path}:`, e)
      return []
    }
  }

  /**
   * Store rows for a specific file path.
   *
   * @deprecated Use `getCompactionContext().cacheFile()` instead for better encapsulation.
   *
   * This method caches row data in memory for a file path,
   * primarily used by compaction and write operations.
   *
   * @param path - The file path to store rows for
   * @param rows - Array of rows to store
   *
   * @example
   * ```typescript
   * // Preferred approach:
   * const ctx = table.getCompactionContext()
   * ctx.cacheFile('data/part-00000.parquet', [
   *   { id: 1, name: 'Alice' },
   *   { id: 2, name: 'Bob' }
   * ])
   *
   * // Legacy approach (deprecated):
   * table.writeFileRows('data/part-00000.parquet', [
   *   { id: 1, name: 'Alice' },
   *   { id: 2, name: 'Bob' }
   * ])
   * ```
   */
  writeFileRows(path: string, rows: T[]): void {
    this.dataStore.set(path, rows)
  }

  /**
   * Delete cached rows for a specific file path.
   *
   * @deprecated Use `getCompactionContext().uncacheFile()` instead for better encapsulation.
   *
   * This method removes cached row data for a file path,
   * primarily used by compaction when removing old files.
   *
   * @param path - The file path to delete rows for
   * @returns true if the file was found and deleted, false otherwise
   *
   * @example
   * ```typescript
   * // Preferred approach:
   * const ctx = table.getCompactionContext()
   * const deleted = ctx.uncacheFile('data/part-00000.parquet')
   *
   * // Legacy approach (deprecated):
   * const deleted = table.deleteFileRows('data/part-00000.parquet')
   * if (deleted) {
   *   console.log('File data removed from cache')
   * }
   * ```
   */
  deleteFileRows(path: string): boolean {
    return this.dataStore.delete(path)
  }

  /**
   * Clear dataStore entries for all add actions.
   * Used to clean up memory when a commit fails after data was written.
   *
   * @internal
   * @param actions - The Delta actions that were attempted to be committed
   */
  private clearDataStoreForActions(actions: DeltaAction[]): void {
    for (const action of actions) {
      if ('add' in action && action.add.path) {
        this.dataStore.delete(action.add.path)
      }
    }
  }

  // =============================================================================
  // COMPACTION CONTEXT
  // =============================================================================

  /**
   * Get a compaction context for maintenance operations.
   *
   * This method returns a CompactionContext object that provides a clean
   * abstraction for compaction, vacuum, and other maintenance operations
   * that need access to storage and table internals.
   *
   * The returned context is the preferred way to access table internals
   * for maintenance operations, as it provides better encapsulation than
   * the legacy methods (getStorage, getTablePath, readFileRowsAsync, etc.).
   *
   * @returns A CompactionContext bound to this table
   *
   * @example
   * ```typescript
   * const ctx = table.getCompactionContext()
   *
   * // Read file data
   * const rows = await ctx.readFile('part-00000.parquet')
   *
   * // Cache new file data
   * ctx.cacheFile('part-00001.parquet', newRows)
   *
   * // Commit changes
   * await ctx.commit(actions)
   * ```
   */
  getCompactionContext(): CompactionContext<T> {
    const table = this
    return {
      storage: this.storage,
      tablePath: this.tablePath,
      readFile: (path: string) => table.readFileRowsAsync(path),
      cacheFile: (path: string, rows: T[]) => table.writeFileRows(path, rows),
      uncacheFile: (path: string) => table.deleteFileRows(path),
      version: () => table.version(),
      snapshot: (version?: number) => table.snapshot(version),
      commit: (actions: DeltaAction[]) => table.commit(actions),
      queryAll: () => table.query() as Promise<T[]>,
    }
  }

  // =============================================================================
  // CHECKPOINT METHODS
  // =============================================================================

  /**
   * Create a checkpoint at the specified version
   */
  private async createCheckpoint(version: number): Promise<void> {
    // Check if checkpoint already exists
    const checkpointPath = `${this.logPath}/${formatVersion(version)}.checkpoint.parquet`
    if (await this.storage.exists(checkpointPath)) {
      return
    }

    // Check if version exists
    const commitPath = `${this.logPath}/${formatVersion(version)}.json`
    if (!(await this.storage.exists(commitPath))) {
      throw new FileNotFoundError(commitPath, 'createCheckpoint')
    }

    // Get snapshot at this version
    const snapshot = await this.snapshot(version)

    // Collect all actions
    const actions: Record<string, unknown>[] = []

    // Add protocol
    if (snapshot.protocol) {
      actions.push({
        protocol: snapshot.protocol
      })
    }

    // Add metadata
    if (snapshot.metadata) {
      actions.push({
        metaData: snapshot.metadata
      })
    }

    // Add all active files
    for (const file of snapshot.files) {
      actions.push({
        add: file
      })
    }

    const maxActions = this.checkpointConfig.maxActionsPerCheckpoint ?? DEFAULT_CHECKPOINT_CONFIG.maxActionsPerCheckpoint!
    const maxSize = this.checkpointConfig.maxCheckpointSizeBytes

    // Determine if we need multi-part checkpoint
    const needsMultiPart = actions.length > maxActions ||
                           (maxSize && estimateCheckpointSize(actions) > maxSize)

    if (needsMultiPart) {
      const totalParts = await createMultiPartCheckpoint(
        this.storage,
        this.logPath,
        version,
        actions,
        this.checkpointConfig
      )

      // Update _last_checkpoint with parts info
      await writeLastCheckpoint(this.storage, this.logPath, {
        version,
        size: actions.length,
        parts: totalParts,
        sizeInBytes: estimateCheckpointSize(actions),
      })
    } else {
      await writeSingleCheckpoint(this.storage, this.logPath, version, actions)

      // Update _last_checkpoint
      await writeLastCheckpoint(this.storage, this.logPath, {
        version,
        size: actions.length,
        sizeInBytes: estimateCheckpointSize(actions),
        numOfAddFiles: snapshot.files.length,
      })
    }

    // Track checkpoint timestamp for cleanup
    this.checkpointTimestamps.set(version, Date.now())

    // Cleanup old checkpoints
    await cleanupCheckpoints(this.storage, this.logPath, this.checkpointConfig, this.checkpointTimestamps)
  }
}
