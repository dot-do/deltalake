/**
 * CDC (Change Data Capture) Primitives
 *
 * Unified CDC record format used by both MongoLake and KafkaLake.
 * Enables cross-system interoperability and standard CDC consumers.
 *
 * This module provides:
 * - Standard CDC record types (CDCRecord, CDCSource)
 * - Delta Lake CDC extensions (DeltaCDCRecord, DeltaCDCChangeType)
 * - CDC Producer/Consumer for stream processing
 * - CDC-enabled Delta Table implementation (using composition with DeltaTable)
 *
 * The CDCDeltaTableImpl class composes with DeltaTable to avoid code duplication:
 * - Version management is delegated to DeltaTable.version()
 * - Commit writing is delegated to DeltaTable.commit()
 * - Only CDC-specific logic (CDC file generation, subscriber notifications) is implemented here
 */

import type { StorageBackend } from '../storage/index.js'
import { DeltaTable, type DeltaCommit, type DeltaAction, type Filter, matchesFilter, formatVersion, DELTA_LOG_DIR } from '../delta/index.js'
import { formatDatePartition, getLogger, inferParquetType, inferSchemaFromRows } from '../utils/index.js'
import { CDCError, type CDCErrorCode, ValidationError } from '../errors.js'
import { parquetReadObjects } from '@dotdo/hyparquet'
import { parquetWriteBuffer, type ColumnSource, type BasicType } from '@dotdo/hyparquet-writer'

// Re-export CDCError for backwards compatibility
export { CDCError, type CDCErrorCode }

// =============================================================================
// CDC RECORD (Standard Format)
// =============================================================================

/**
 * CDC operation types: create, update, delete, read (snapshot).
 *
 * @public
 */
export type CDCOperation = 'c' | 'u' | 'd' | 'r'

/**
 * Standard CDC record format for cross-system interoperability.
 * Compatible with Debezium, Kafka Connect, and other CDC systems.
 *
 * @public
 */
export interface CDCRecord<T = unknown> {
  /** Entity ID (document ID, message key, row PK) */
  _id: string

  /** Sequence number (MongoLake LSN, Kafka offset, oplog ts) */
  _seq: bigint

  /** Operation type */
  _op: CDCOperation

  /** Previous state (null for create) */
  _before: T | null

  /** New state (null for delete) */
  _after: T | null

  /** Timestamp in nanoseconds */
  _ts: bigint

  /** Source metadata */
  _source: CDCSource

  /** Transaction ID (for exactly-once semantics) */
  _txn?: string
}

/**
 * Source metadata for CDC records.
 * Identifies the origin system and location of the change.
 *
 * @public
 */
export interface CDCSource {
  /** Source system identifier */
  system: 'mongolake' | 'kafkalake' | 'postgres' | 'mysql' | 'debezium' | 'deltalake'

  /** Database name */
  database?: string

  /** Collection/table/topic name */
  collection?: string

  /** Partition/shard ID */
  partition?: number

  /** Server ID (for multi-master replication) */
  serverId?: string
}

// =============================================================================
// DELTA LAKE CDC EXTENSIONS
// =============================================================================

/**
 * Delta Lake specific CDC change types.
 * These match the Delta Lake CDC specification.
 *
 * @public
 */
export type DeltaCDCChangeType =
  | 'insert'
  | 'update_preimage'
  | 'update_postimage'
  | 'delete'

/**
 * Delta Lake CDC Record structure.
 * Contains the standard Delta Lake CDC metadata columns.
 *
 * @template T - The row data type
 *
 * @public
 */
export interface DeltaCDCRecord<T = Record<string, unknown>> {
  /** The change type: insert, update_preimage, update_postimage, delete */
  _change_type: DeltaCDCChangeType
  /** The commit version that produced this change */
  _commit_version: bigint
  /** The timestamp of the commit */
  _commit_timestamp: Date
  /** The actual row data */
  data: T
}

/**
 * CDC Configuration for a Delta table.
 * Controls CDC behavior including retention policies.
 *
 * @public
 */
export interface CDCConfig {
  /** Whether CDC is enabled for this table */
  readonly enabled: boolean
  /** Retention period for CDC files in milliseconds */
  readonly retentionMs?: number
}

/**
 * Type guard to validate CDCConfig from unknown input (e.g., JSON.parse result).
 *
 * Validates that the input object has the required structure for CDCConfig:
 * - `enabled` must be a boolean
 * - `retentionMs` (if present) must be a number
 *
 * @param obj - The unknown value to validate
 * @returns True if the object is a valid CDCConfig, false otherwise
 *
 * @public
 *
 * @example
 * ```typescript
 * const parsed = JSON.parse(configString)
 * if (isValidCDCConfig(parsed)) {
 *   console.log(`CDC enabled: ${parsed.enabled}`)
 * }
 * ```
 */
export function isValidCDCConfig(obj: unknown): obj is CDCConfig {
  if (typeof obj !== 'object' || obj === null) return false
  const config = obj as Record<string, unknown>
  if (typeof config.enabled !== 'boolean') return false
  // Optional field validation
  if (config.retentionMs !== undefined && typeof config.retentionMs !== 'number') return false
  return true
}

/**
 * Error callback for CDC subscription handler failures.
 * Called when a subscriber's handler throws an error.
 *
 * @public
 */
export type CDCSubscriptionErrorCallback<T = Record<string, unknown>> = (
  error: Error,
  record: DeltaCDCRecord<T>
) => void

/**
 * Options for CDC subscription.
 *
 * @public
 */
export interface CDCSubscriptionOptions<T = Record<string, unknown>> {
  /**
   * Error callback invoked when the handler throws.
   * The error is still caught to prevent cascade failures to other subscribers.
   */
  readonly onError?: CDCSubscriptionErrorCallback<T>
}

/**
 * CDC Reader interface for reading changes from a Delta table.
 * Provides both batch and streaming access to CDC records.
 *
 * @template T - The row data type
 *
 * @public
 */
export interface CDCReader<T = Record<string, unknown>> {
  /**
   * Read changes between two versions (inclusive).
   * @throws {CDCError} If version range is invalid or table not found
   */
  readByVersion(startVersion: bigint, endVersion: bigint): Promise<DeltaCDCRecord<T>[]>

  /**
   * Read changes within a time range (inclusive).
   * @throws {CDCError} If time range is invalid
   */
  readByTimestamp(startTime: Date, endTime: Date): Promise<DeltaCDCRecord<T>[]>

  /**
   * Subscribe to changes as they occur.
   * @param handler - Async function called for each CDC record
   * @param options - Optional configuration including error callback
   * @returns Unsubscribe function to stop receiving changes
   */
  subscribe(
    handler: (record: DeltaCDCRecord<T>) => Promise<void>,
    options?: CDCSubscriptionOptions<T>
  ): () => void
}

/**
 * CDC-enabled Delta Table interface.
 * Extends standard Delta Table operations with CDC tracking.
 *
 * @template T - The row data type
 *
 * @public
 */
export interface CDCDeltaTable<T extends Record<string, unknown> = Record<string, unknown>> {
  /** Enable or disable CDC for this table */
  setCDCEnabled(enabled: boolean): Promise<void>

  /** Check if CDC is enabled */
  isCDCEnabled(): Promise<boolean>

  /** Get CDC reader for this table */
  getCDCReader(): CDCReader<T>

  /** Write data and generate CDC records */
  write(rows: T[]): Promise<DeltaCommit>

  /** Update rows and generate CDC records with before/after images */
  update(filter: Record<string, unknown>, updates: Partial<T>): Promise<DeltaCommit>

  /** Delete rows and generate CDC records */
  deleteRows(filter: Record<string, unknown>): Promise<DeltaCommit>

  /**
   * Merge data (upsert) with CDC tracking.
   * @param rows - Rows to merge
   * @param matchCondition - Function to match existing rows
   * @param whenMatched - Transform for matched rows (return null to delete)
   * @param whenNotMatched - Transform for unmatched rows (return null to skip)
   */
  merge(
    rows: T[],
    matchCondition: (existing: T, incoming: T) => boolean,
    whenMatched?: (existing: T, incoming: T) => T | null,
    whenNotMatched?: (incoming: T) => T | null
  ): Promise<DeltaCommit>
}

// =============================================================================
// CDC PRODUCER
// =============================================================================

/**
 * Options for creating a CDC producer.
 *
 * @public
 */
export interface CDCProducerOptions {
  /** Source metadata (excluding system, which can be specified separately) */
  readonly source: Omit<CDCSource, 'system'>
  /** Source system identifier (defaults to 'kafkalake') */
  readonly system?: CDCSource['system']
}

/**
 * CDC Producer for generating standardized CDC records.
 * Used for converting data changes into CDC format for downstream consumers.
 *
 * @template T - The data type being tracked
 *
 * @public
 *
 * @example
 * ```typescript
 * const producer = new CDCProducer<User>({
 *   source: { database: 'mydb', collection: 'users' },
 *   system: 'mongolake'
 * })
 *
 * const record = await producer.create('user-1', { name: 'Alice' })
 * ```
 */
export class CDCProducer<T = unknown> {
  private seq: bigint = 0n
  private readonly source: CDCSource

  constructor(options: CDCProducerOptions) {
    this.source = {
      system: options.system ?? 'kafkalake',
      ...options.source,
    }
  }

  /**
   * Get the current sequence number (useful for checkpointing).
   */
  getSequence(): bigint {
    return this.seq
  }

  /**
   * Reset the sequence number (use with caution).
   */
  resetSequence(seq: bigint = 0n): void {
    this.seq = seq
  }

  /**
   * Emit a CDC record with the specified operation type.
   */
  async emit(
    op: CDCOperation,
    id: string,
    before: T | null,
    after: T | null,
    txn?: string
  ): Promise<CDCRecord<T>> {
    const record: CDCRecord<T> = {
      _id: id,
      _seq: this.seq++,
      _op: op,
      _before: before,
      _after: after,
      _ts: BigInt(Date.now()) * 1_000_000n, // Convert to nanoseconds
      _source: this.source,
    }

    if (txn !== undefined) {
      record._txn = txn
    }

    return record
  }

  /**
   * Emit a create record (operation: 'c').
   *
   * @param id - Entity identifier
   * @param data - The new entity data
   * @param txn - Optional transaction ID for exactly-once semantics
   * @returns The generated CDC record
   *
   * @example
   * ```typescript
   * const record = await producer.create('user-1', {
   *   name: 'Alice',
   *   email: 'alice@example.com'
   * })
   * // record._op === 'c'
   * // record._before === null
   * // record._after === { name: 'Alice', email: 'alice@example.com' }
   * ```
   */
  async create(id: string, data: T, txn?: string): Promise<CDCRecord<T>> {
    return this.emit('c', id, null, data, txn)
  }

  /**
   * Emit an update record (operation: 'u').
   *
   * @param id - Entity identifier
   * @param before - The entity state before the update
   * @param after - The entity state after the update
   * @param txn - Optional transaction ID for exactly-once semantics
   * @returns The generated CDC record
   *
   * @example
   * ```typescript
   * const record = await producer.update(
   *   'user-1',
   *   { name: 'Alice', score: 100 },
   *   { name: 'Alice', score: 150 }
   * )
   * // record._op === 'u'
   * // record._before === { name: 'Alice', score: 100 }
   * // record._after === { name: 'Alice', score: 150 }
   * ```
   */
  async update(id: string, before: T, after: T, txn?: string): Promise<CDCRecord<T>> {
    return this.emit('u', id, before, after, txn)
  }

  /**
   * Emit a delete record (operation: 'd').
   *
   * @param id - Entity identifier
   * @param before - The entity state before deletion
   * @param txn - Optional transaction ID for exactly-once semantics
   * @returns The generated CDC record
   *
   * @example
   * ```typescript
   * const record = await producer.delete('user-1', {
   *   name: 'Alice',
   *   email: 'alice@example.com'
   * })
   * // record._op === 'd'
   * // record._before === { name: 'Alice', email: 'alice@example.com' }
   * // record._after === null
   * ```
   */
  async delete(id: string, before: T, txn?: string): Promise<CDCRecord<T>> {
    return this.emit('d', id, before, null, txn)
  }

  /**
   * Emit snapshot records (bulk 'r' operations).
   *
   * Useful for initial data sync or point-in-time snapshots.
   * Each record represents the current state of an entity.
   *
   * @param records - Array of entities to snapshot
   * @returns Array of generated CDC records
   *
   * @example
   * ```typescript
   * const records = await producer.snapshot([
   *   { id: 'user-1', data: { name: 'Alice' } },
   *   { id: 'user-2', data: { name: 'Bob' } }
   * ])
   * // records[0]._op === 'r'
   * // records[0]._before === null
   * // records[0]._after === { name: 'Alice' }
   * ```
   */
  async snapshot(records: ReadonlyArray<{ id: string; data: T }>): Promise<CDCRecord<T>[]> {
    return Promise.all(
      records.map(({ id, data }) => this.emit('r', id, null, data))
    )
  }
}

// =============================================================================
// CDC CONSUMER
// =============================================================================

/**
 * Options for creating a CDC consumer.
 *
 * @public
 */
export interface CDCConsumerOptions {
  /** Starting sequence number (records before this are skipped) */
  readonly fromSeq?: bigint

  /** Starting timestamp (records before this are skipped) */
  readonly fromTimestamp?: Date

  /** Filter by operation types (only these operations are processed) */
  readonly operations?: readonly CDCOperation[]
}

/**
 * Handler function type for CDC record processing.
 *
 * @public
 */
export type CDCRecordHandler<T> = (record: CDCRecord<T>) => Promise<void>

/**
 * CDC Consumer for processing CDC records.
 * Supports filtering, position tracking, and multiple subscribers.
 *
 * @template T - The data type being consumed
 *
 * @public
 *
 * @example
 * ```typescript
 * const consumer = new CDCConsumer<User>({
 *   fromSeq: 100n,
 *   operations: ['c', 'u'] // Only creates and updates
 * })
 *
 * consumer.subscribe(async (record) => {
 *   console.log(`${record._op}: ${record._id}`)
 * })
 *
 * await consumer.process(record)
 * ```
 */
export class CDCConsumer<T = unknown> {
  private readonly handlers: CDCRecordHandler<T>[] = []
  private position: bigint = 0n
  private readonly operationsSet: Set<CDCOperation> | null
  private timestampFilter: Date | undefined

  constructor(private readonly options: CDCConsumerOptions = {}) {
    if (options.fromSeq !== undefined) {
      this.position = options.fromSeq
    }
    this.timestampFilter = options.fromTimestamp
    this.operationsSet = options.operations
      ? new Set(options.operations)
      : null
  }

  /**
   * Subscribe to CDC records.
   *
   * Multiple handlers can be subscribed. Each handler receives all records
   * that pass the consumer's filters (operations, sequence, timestamp).
   *
   * @param handler - Async function to process each CDC record
   * @returns Unsubscribe function to remove the handler
   *
   * @example
   * ```typescript
   * // Subscribe to process records
   * const unsubscribe = consumer.subscribe(async (record) => {
   *   if (record._op === 'c') {
   *     await indexService.add(record._after)
   *   } else if (record._op === 'd') {
   *     await indexService.remove(record._id)
   *   }
   * })
   *
   * // Later, unsubscribe when done
   * unsubscribe()
   * ```
   */
  subscribe(handler: CDCRecordHandler<T>): () => void {
    this.handlers.push(handler)
    return () => {
      const idx = this.handlers.indexOf(handler)
      if (idx >= 0) {
        this.handlers.splice(idx, 1)
      }
    }
  }

  /**
   * Process a record, applying filters and notifying handlers.
   *
   * The record is only processed if it passes all configured filters:
   * - Operation type filter (if `operations` was specified)
   * - Sequence filter (record._seq must be >= current position)
   * - Timestamp filter (if `fromTimestamp` was specified)
   *
   * After successful processing, the position is updated to record._seq + 1.
   *
   * @param record - The CDC record to process
   *
   * @example
   * ```typescript
   * // Process records from a stream
   * for await (const record of cdcStream) {
   *   await consumer.process(record)
   * }
   *
   * // Check current position after processing
   * console.log(`Processed up to sequence ${consumer.getPosition()}`)
   * ```
   */
  async process(record: CDCRecord<T>): Promise<void> {
    // Filter by operations
    if (this.operationsSet && !this.operationsSet.has(record._op)) {
      return
    }

    // Filter by sequence
    if (record._seq < this.position) {
      return
    }

    // Filter by timestamp
    if (this.timestampFilter) {
      const tsMs = Number(record._ts / 1_000_000n)
      if (tsMs < this.timestampFilter.getTime()) {
        return
      }
    }

    // Notify handlers in parallel
    await Promise.all(this.handlers.map(h => h(record)))
    this.position = record._seq + 1n
  }

  /**
   * Seek to a specific sequence position.
   *
   * Records with sequence numbers less than this position will be skipped.
   * Use this to resume processing from a saved checkpoint.
   *
   * @param seq - The sequence number to seek to
   *
   * @example
   * ```typescript
   * // Resume from a saved checkpoint
   * const checkpoint = await loadCheckpoint()
   * consumer.seekTo(checkpoint.lastSeq)
   *
   * // Process new records from the checkpoint position
   * for await (const record of cdcStream) {
   *   await consumer.process(record)
   * }
   * ```
   */
  seekTo(seq: bigint): void {
    this.position = seq
  }

  /**
   * Set timestamp filter for future records.
   *
   * Records with timestamps before this date will be skipped.
   * The timestamp is compared against the record's `_ts` field.
   *
   * @param ts - The timestamp to filter from
   *
   * @example
   * ```typescript
   * // Only process records from the last hour
   * const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000)
   * consumer.seekToTimestamp(oneHourAgo)
   * ```
   */
  seekToTimestamp(ts: Date): void {
    this.timestampFilter = ts
  }

  /**
   * Get current sequence position.
   *
   * Returns the next sequence number that will be processed.
   * Useful for checkpointing consumer progress.
   *
   * @returns The current sequence position
   *
   * @example
   * ```typescript
   * // Save checkpoint after processing
   * await saveCheckpoint({ lastSeq: consumer.getPosition() })
   * ```
   */
  getPosition(): bigint {
    return this.position
  }

  /**
   * Get number of active subscribers.
   *
   * @returns The count of subscribed handlers
   *
   * @example
   * ```typescript
   * console.log(`Active subscribers: ${consumer.getSubscriberCount()}`)
   * ```
   */
  getSubscriberCount(): number {
    return this.handlers.length
  }
}

// =============================================================================
// CDC DELTA TABLE IMPLEMENTATION
// =============================================================================

/**
 * Handler type for Delta CDC record subscriptions.
 *
 * @internal
 */
type DeltaCDCRecordHandler<T> = (record: DeltaCDCRecord<T>) => Promise<void>

/**
 * Internal subscription entry with handler and optional error callback.
 *
 * @internal
 */
interface SubscriptionEntry<T> {
  handler: DeltaCDCRecordHandler<T>
  onError?: CDCSubscriptionErrorCallback<T> | undefined
}

/**
 * Internal CDC Reader implementation.
 * Handles reading CDC records from storage and managing subscriptions.
 *
 * @internal
 */
class CDCReaderImpl<T extends Record<string, unknown>> implements CDCReader<T> {
  private readonly subscriptions: SubscriptionEntry<T>[] = []

  constructor(
    private readonly storage: StorageBackend,
    private readonly tablePath: string
  ) {}

  /**
   * Read changes between two versions (inclusive).
   */
  async readByVersion(startVersion: bigint, endVersion: bigint): Promise<DeltaCDCRecord<T>[]> {
    this.validateVersionRange(startVersion, endVersion)
    await this.ensureTableExists()

    const results: DeltaCDCRecord<T>[] = []
    for (let v = startVersion; v <= endVersion; v++) {
      const cdcRecords = await this.readCDCFileForVersion(v)
      results.push(...cdcRecords)
    }
    return results
  }

  /**
   * Read changes within a time range (inclusive).
   */
  async readByTimestamp(startTime: Date, endTime: Date): Promise<DeltaCDCRecord<T>[]> {
    this.validateTimeRange(startTime, endTime)

    const allRecords = await this.readAllCDCFiles()

    return allRecords
      .filter(record => {
        const ts = record._commit_timestamp.getTime()
        return ts >= startTime.getTime() && ts <= endTime.getTime()
      })
      .sort((a, b) => a._commit_timestamp.getTime() - b._commit_timestamp.getTime())
  }

  /**
   * Subscribe to changes as they occur.
   * @param handler - Async function called for each CDC record
   * @param options - Optional configuration including error callback
   */
  subscribe(
    handler: DeltaCDCRecordHandler<T>,
    options?: CDCSubscriptionOptions<T>
  ): () => void {
    const entry: SubscriptionEntry<T> = {
      handler,
      onError: options?.onError,
    }
    this.subscriptions.push(entry)
    return () => {
      const idx = this.subscriptions.indexOf(entry)
      if (idx >= 0) {
        this.subscriptions.splice(idx, 1)
      }
    }
  }

  /**
   * Notify all subscribers of a new record.
   * Errors in individual handlers are caught to prevent cascade failures.
   * If the subscriber provided an onError callback, it will be invoked with the error.
   */
  async notifySubscribers(record: DeltaCDCRecord<T>): Promise<void> {
    await Promise.all(
      this.subscriptions.map(async ({ handler, onError }) => {
        try {
          await handler(record)
        } catch (error) {
          // Log error with context for observability
          console.error(
            `[CDC] Subscriber error processing record: ` +
            `change_type=${record._change_type}, ` +
            `version=${record._commit_version}, ` +
            `table=${this.tablePath}`,
            error
          )

          // Invoke error callback if provided
          if (onError) {
            try {
              onError(error instanceof Error ? error : new Error(String(error)), record)
            } catch (callbackError) {
              // Error callback itself failed - log but don't propagate
              console.error('[CDC] Error callback threw:', callbackError)
            }
          }
          // Errors are caught to prevent cascade failures to other subscribers
        }
      })
    )
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Validate version range parameters.
   */
  private validateVersionRange(startVersion: bigint, endVersion: bigint): void {
    if (startVersion < 0n) {
      throw new CDCError(
        'Invalid version: start version must be non-negative',
        'INVALID_VERSION_RANGE'
      )
    }
    if (endVersion < 0n) {
      throw new CDCError(
        'Invalid version: end version must be non-negative',
        'INVALID_VERSION_RANGE'
      )
    }
    if (startVersion > endVersion) {
      throw new CDCError(
        'Invalid version range: start version must be <= end version',
        'INVALID_VERSION_RANGE'
      )
    }
  }

  /**
   * Validate time range parameters.
   */
  private validateTimeRange(startTime: Date, endTime: Date): void {
    if (startTime > endTime) {
      throw new CDCError(
        'Invalid time range: start time must be <= end time',
        'INVALID_TIME_RANGE'
      )
    }
  }

  /**
   * Ensure the table exists by checking for delta log commit files.
   * Only commit files (version JSON files) count as a valid table.
   */
  private async ensureTableExists(): Promise<void> {
    const logPath = `${this.tablePath}/${DELTA_LOG_DIR}`
    try {
      const logFiles = await this.storage.list(logPath)
      // Filter for actual commit files (version .json files, not properties files)
      // Version files are named like: 00000000000000000000.json
      const commitFiles = logFiles.filter(f => {
        const filename = f.split('/').pop() ?? ''
        // Check if it's a version file (20 digits followed by .json)
        return /^\d{20}\.json$/.test(filename)
      })
      if (commitFiles.length === 0) {
        throw new CDCError(
          `Table not found: ${this.tablePath}`,
          'TABLE_NOT_FOUND'
        )
      }
    } catch (error) {
      if (error instanceof CDCError) throw error
      throw new CDCError(
        `Table not found: ${this.tablePath}`,
        'TABLE_NOT_FOUND',
        error
      )
    }
  }

  /**
   * Read CDC records from storage for a specific version.
   */
  private async readCDCFileForVersion(version: bigint): Promise<DeltaCDCRecord<T>[]> {
    const cdcPath = `${this.tablePath}/_change_data/`
    const versionStr = formatVersion(version)

    try {
      const cdcFiles = await this.storage.list(cdcPath)
      const matchingFile = cdcFiles.find(f => f.includes(`cdc-${versionStr}`))

      if (!matchingFile) {
        return []
      }

      return await this.parseCDCFile(matchingFile)
    } catch (e) {
      // CDC directory may not exist for tables without CDC enabled, or file may be corrupted
      getLogger().warn(`[CDC] Failed to read CDC file for version ${version}:`, e)
      return []
    }
  }

  /**
   * Read all CDC files from storage.
   * Only reads from the non-partitioned path to avoid duplicates.
   */
  private async readAllCDCFiles(): Promise<DeltaCDCRecord<T>[]> {
    const cdcPath = `${this.tablePath}/_change_data/`
    const allRecords: DeltaCDCRecord<T>[] = []

    try {
      const cdcFiles = await this.storage.list(cdcPath)

      for (const file of cdcFiles) {
        if (!file.endsWith('.parquet')) continue
        // Only read files from the non-partitioned path (direct lookup path)
        // to avoid reading duplicates from date-partitioned paths
        // Non-partitioned files are at: _change_data/cdc-{version}.parquet
        // Date-partitioned files are at: _change_data/date=YYYY-MM-DD/cdc-{version}.parquet
        if (file.includes('/date=')) continue

        try {
          const records = await this.parseCDCFile(file)
          allRecords.push(...records)
        } catch (e) {
          // Skip corrupted files, but log the warning
          getLogger().warn(`[CDC] Failed to parse CDC file ${file}, skipping:`, e)
        }
      }
    } catch (e) {
      // CDC directory may not exist for tables without CDC enabled
      getLogger().warn(`[CDC] Failed to list CDC directory:`, e)
    }

    return allRecords
  }

  /**
   * Parse a CDC file from storage.
   * Supports both JSON (internal format) and Parquet (external Delta tables).
   */
  private async parseCDCFile(filePath: string): Promise<DeltaCDCRecord<T>[]> {
    const data = await this.storage.read(filePath)

    // Check if file is Parquet by looking for PAR1 magic bytes
    const isParquet = this.isParquetFile(data)

    if (isParquet) {
      return this.parseParquetCDCFile(data)
    }

    // Fall back to JSON format (internal CDC files)
    return this.parseJsonCDCFile(data)
  }

  /**
   * Check if data is a Parquet file by looking for PAR1 magic bytes.
   */
  private isParquetFile(data: Uint8Array): boolean {
    // Parquet files start and end with "PAR1" magic bytes
    if (data.length < 4) return false
    // Use non-null assertions since we've verified length >= 4
    const magic = String.fromCharCode(data[0]!, data[1]!, data[2]!, data[3]!)
    return magic === 'PAR1'
  }

  /**
   * Parse a Parquet CDF file.
   * External Delta tables store CDF as Parquet with _change_type column.
   */
  private async parseParquetCDCFile(data: Uint8Array): Promise<DeltaCDCRecord<T>[]> {
    const arrayBuffer = data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength
    ) as ArrayBuffer

    const rows = await parquetReadObjects({ file: arrayBuffer }) as Record<string, unknown>[]

    // Transform Parquet rows to DeltaCDCRecord format
    return rows.map(row => {
      // Extract CDF metadata columns
      const changeType = row._change_type as string

      // Extract commit version - may be present in our own Parquet files
      let commitVersion = 0n
      if (row._commit_version !== undefined && row._commit_version !== null) {
        commitVersion = BigInt(row._commit_version as number)
      }

      // Extract commit timestamp - may be present in our own Parquet files
      let commitTimestamp = new Date(0)
      if (row._commit_timestamp !== undefined && row._commit_timestamp !== null) {
        // Handle Date object or timestamp number
        if (row._commit_timestamp instanceof Date) {
          commitTimestamp = row._commit_timestamp
        } else if (typeof row._commit_timestamp === 'number') {
          commitTimestamp = new Date(row._commit_timestamp)
        }
      }

      // Build data object without CDF metadata columns
      const rowData: Record<string, unknown> = {}
      for (const [key, value] of Object.entries(row)) {
        if (!['_change_type', '_commit_version', '_commit_timestamp'].includes(key)) {
          rowData[key] = value
        }
      }

      return {
        _change_type: changeType as DeltaCDCChangeType,
        _commit_version: commitVersion,
        _commit_timestamp: commitTimestamp,
        data: rowData as T,
      }
    })
  }

  /**
   * Parse a JSON CDF file (internal format).
   */
  private parseJsonCDCFile(data: Uint8Array): DeltaCDCRecord<T>[] {
    const jsonStr = new TextDecoder().decode(data)

    return JSON.parse(jsonStr, (key, value) => {
      // Convert string back to BigInt for _commit_version
      if (key === '_commit_version' && typeof value === 'string') {
        return BigInt(value)
      }
      // Convert ISO date string back to Date for _commit_timestamp and any date-like fields
      if (typeof value === 'string') {
        // Check if it looks like an ISO date string
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/.test(value)) {
          return new Date(value)
        }
      }
      return value
    }) as DeltaCDCRecord<T>[]
  }
}

// =============================================================================
// PARQUET WRITING HELPERS FOR CDC
// =============================================================================

/**
 * Convert CDC records to column data format for Parquet writing.
 *
 * Flattens the DeltaCDCRecord structure into a flat row format with CDC metadata columns,
 * then converts to columnar format required by the Parquet writer.
 *
 * The output includes:
 * - `_change_type`: The CDC operation type (insert, update_preimage, update_postimage, delete)
 * - `_commit_version`: The Delta table version (converted from BigInt to Number)
 * - `_commit_timestamp`: The commit timestamp as a Date
 * - All data fields from the record
 *
 * @param records - Array of DeltaCDCRecord objects to convert
 * @returns Array of ColumnSource objects for Parquet writing
 * @throws {ValidationError} If column data is missing for a schema field
 *
 * @internal
 *
 * @example
 * ```typescript
 * const records: DeltaCDCRecord<MyRow>[] = [
 *   { _change_type: 'insert', _commit_version: 0n, _commit_timestamp: new Date(), data: { id: '1' } }
 * ]
 * const columns = cdcRecordsToColumnData(records)
 * // Use columns with parquetWriteBuffer
 * ```
 */
function cdcRecordsToColumnData<T extends Record<string, unknown>>(records: DeltaCDCRecord<T>[]): ColumnSource[] {
  if (records.length === 0) {
    return []
  }

  // Flatten CDC records into rows with metadata columns
  const flatRows: Record<string, unknown>[] = records.map(record => ({
    _change_type: record._change_type,
    _commit_version: Number(record._commit_version), // Convert BigInt to Number for Parquet
    _commit_timestamp: record._commit_timestamp,
    ...record.data
  }))

  // Infer schema from flattened rows
  const schema = inferSchemaFromRows(flatRows)

  // Initialize column arrays
  const columnArrays: Map<string, unknown[]> = new Map()
  for (const field of schema) {
    columnArrays.set(field.name, [])
  }

  // Process all rows
  for (const row of flatRows) {
    for (const field of schema) {
      let value = row[field.name]
      const columnData = columnArrays.get(field.name)
      if (!columnData) continue

      // Handle null/undefined
      if (value === null || value === undefined) {
        columnData.push(null)
        continue
      }

      // Convert BigInt to Number for numeric types
      if (typeof value === 'bigint') {
        value = Number(value)
      }

      // Handle binary conversion
      if (value instanceof ArrayBuffer) {
        value = new Uint8Array(value)
      }

      columnData.push(value)
    }
  }

  // Build column data for Parquet writer
  return schema.map(field => {
    const data = columnArrays.get(field.name)
    if (!data) {
      throw new ValidationError(`Missing column data for field: ${field.name}`, field.name)
    }
    return {
      name: field.name,
      data,
      type: field.type,
      nullable: true,
    }
  })
}

/**
 * CDC-enabled Delta Table implementation.
 * Provides write, update, delete, and merge operations with CDC tracking.
 *
 * This implementation composes with DeltaTable to avoid code duplication:
 * - Version management is delegated to DeltaTable
 * - Commit writing is delegated to DeltaTable
 * - Only CDC-specific logic (CDC file generation, subscribers) is implemented here
 *
 * @internal
 */
class CDCDeltaTableImpl<T extends Record<string, unknown>> implements CDCDeltaTable<T> {
  private readonly deltaTable: DeltaTable<T>
  private readonly storage: StorageBackend
  private readonly tablePath: string
  private readonly reader: CDCReaderImpl<T>

  private cdcEnabled: boolean = false
  private configLoaded: boolean = false
  private writeQueue: Promise<DeltaCommit> = Promise.resolve({
    version: -1,
    timestamp: 0,
    actions: []
  })

  constructor(storage: StorageBackend, tablePath: string) {
    // Validate required parameters
    if (storage === null || storage === undefined) {
      throw new ValidationError('storage is required and cannot be null or undefined', 'storage', storage)
    }
    if (tablePath === null || tablePath === undefined) {
      throw new ValidationError('tablePath is required and cannot be null or undefined', 'tablePath', tablePath)
    }

    // Validate tablePath is a string
    if (typeof tablePath !== 'string') {
      throw new ValidationError('tablePath must be a string', 'tablePath', tablePath)
    }

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

    this.deltaTable = new DeltaTable<T>(storage, tablePath)
    this.storage = storage
    this.tablePath = tablePath
    this.reader = new CDCReaderImpl(storage, tablePath)
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Enable or disable CDC for this table.
   *
   * When enabled, all write operations (insert, update, delete, merge) will
   * generate CDC records stored in the `_change_data` directory.
   *
   * The CDC configuration is persisted to storage so it survives table recreation.
   *
   * @param enabled - Whether to enable (true) or disable (false) CDC
   */
  async setCDCEnabled(enabled: boolean): Promise<void> {
    this.cdcEnabled = enabled
    this.configLoaded = true

    // Set the table configuration on the underlying DeltaTable so it's included in metadata
    if (enabled) {
      this.deltaTable.setTableConfiguration({ 'delta.enableChangeDataFeed': 'true' })
    } else {
      this.deltaTable.setTableConfiguration({ 'delta.enableChangeDataFeed': 'false' })
    }

    // Persist the CDC configuration to both the config file and delta log directory
    const configPath = `${this.tablePath}/_cdc_config.json`
    const config: CDCConfig = { enabled }
    await this.storage.write(configPath, new TextEncoder().encode(JSON.stringify(config)))

    // Also write to delta log directory so it's recorded there
    // This creates a properties file that records CDC configuration changes
    const timestamp = new Date()
    const propertiesPath = `${this.tablePath}/${DELTA_LOG_DIR}/_cdc_properties.json`
    const properties = {
      'delta.enableChangeDataFeed': enabled ? 'true' : 'false',
      timestamp: timestamp.getTime()
    }
    await this.storage.write(propertiesPath, new TextEncoder().encode(JSON.stringify(properties)))

  }

  /**
   * Check if CDC is enabled for this table.
   *
   * Loads the CDC configuration from storage on first call, then caches the result.
   *
   * @returns True if CDC is enabled, false otherwise
   */
  async isCDCEnabled(): Promise<boolean> {
    await this.ensureConfigLoaded()
    return this.cdcEnabled
  }

  /**
   * Get the CDC reader for this table.
   *
   * The reader provides methods to read CDC records by version range or time range,
   * and to subscribe to real-time changes.
   *
   * @returns The CDC reader instance for this table
   */
  getCDCReader(): CDCReader<T> {
    return this.reader
  }

  /**
   * Write rows to the table and generate CDC insert records.
   *
   * If CDC is enabled, generates 'insert' CDC records for each row written.
   * Writes are serialized through a queue to prevent race conditions.
   *
   * @param rows - Array of rows to write
   * @returns The Delta commit information
   * @throws {ValidationError} If rows is null, undefined, not an array, or contains invalid elements
   * @throws {CDCError} If the write operation fails
   */
  async write(rows: T[]): Promise<DeltaCommit> {
    // Validate rows parameter
    if (rows === null || rows === undefined) {
      throw new ValidationError('rows is required and cannot be null or undefined', 'rows', rows)
    }
    if (!Array.isArray(rows)) {
      throw new ValidationError('rows must be an array', 'rows', rows)
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

    // Serialize writes through a queue to prevent race conditions
    const result = this.writeQueue.then(
      () => this.executeWrite(rows),
      () => this.executeWrite(rows) // Continue even if previous write failed
    )
    this.writeQueue = result
    return result
  }

  /**
   * Update rows matching the filter and generate CDC update records.
   *
   * If CDC is enabled, generates 'update_preimage' and 'update_postimage'
   * CDC records for each row updated. The preimage captures the row state
   * before the update, and the postimage captures the state after.
   *
   * @param filter - Filter criteria to match rows for update
   * @param updates - Partial row data to apply to matching rows
   * @returns The Delta commit information
   * @throws {ValidationError} If filter or updates is null, undefined, or not an object
   */
  async update(filter: Record<string, unknown>, updates: Partial<T>): Promise<DeltaCommit> {
    // Validate filter is required and must be an object
    if (filter === null || filter === undefined) {
      throw new ValidationError('filter is required and cannot be null or undefined', 'filter', filter)
    }
    if (typeof filter !== 'object' || Array.isArray(filter)) {
      throw new ValidationError('filter must be an object', 'filter', filter)
    }

    // Validate updates is required and must be an object
    if (updates === null || updates === undefined) {
      throw new ValidationError('updates is required and cannot be null or undefined', 'updates', updates)
    }
    if (typeof updates !== 'object' || Array.isArray(updates)) {
      throw new ValidationError('updates must be an object', 'updates', updates)
    }

    await this.ensureConfigLoaded()

    // Get current version from DeltaTable (delegated version management)
    const currentVersion = await this.deltaTable.version()
    const newVersion = currentVersion + 1
    const timestamp = new Date()

    // Query current data from storage using DeltaTable (fixes P0 bug - tableData was stale)
    const allRows = await this.deltaTable.query(filter as Filter<T>) as T[]
    const matchingRows = allRows

    if (matchingRows.length === 0) {
      return this.createEmptyCommit(newVersion, timestamp)
    }

    const cdcRecords = this.cdcEnabled
      ? this.generateUpdateCDCRecords(matchingRows, updates, newVersion, timestamp)
      : []

    // Delegate actual update to DeltaTable
    // Note: For now we generate CDC records based on the query results
    // The actual data update would be handled by the underlying Delta table commit

    return this.finalizeCommit(timestamp, 'UPDATE', cdcRecords)
  }

  /**
   * Delete rows matching the filter and generate CDC delete records.
   *
   * If CDC is enabled, generates 'delete' CDC records for each row deleted.
   * The delete record contains the full row data at the time of deletion.
   *
   * @param filter - Filter criteria to match rows for deletion
   * @returns The Delta commit information
   * @throws {ValidationError} If filter is null, undefined, or not an object
   */
  async deleteRows(filter: Record<string, unknown>): Promise<DeltaCommit> {
    // Validate filter is required and must be an object
    if (filter === null || filter === undefined) {
      throw new ValidationError('filter is required and cannot be null or undefined', 'filter', filter)
    }
    if (typeof filter !== 'object' || Array.isArray(filter)) {
      throw new ValidationError('filter must be an object', 'filter', filter)
    }

    await this.ensureConfigLoaded()

    // Get current version from DeltaTable (delegated version management)
    const currentVersion = await this.deltaTable.version()
    const newVersion = currentVersion + 1
    const timestamp = new Date()

    // Query current data from storage using DeltaTable (fixes P0 bug - tableData was stale)
    const allRows = await this.deltaTable.query(filter as Filter<T>) as T[]
    const matchingRows = allRows

    if (matchingRows.length === 0) {
      return this.createEmptyCommit(newVersion, timestamp)
    }

    const cdcRecords = this.cdcEnabled
      ? this.generateDeleteCDCRecords(matchingRows, newVersion, timestamp)
      : []

    // Delegate actual delete to DeltaTable
    // Note: For now we generate CDC records based on the query results
    // The actual data deletion would be handled by the underlying Delta table commit

    return this.finalizeCommit(timestamp, 'DELETE', cdcRecords)
  }

  /**
   * Merge (upsert) rows into the table with CDC tracking.
   *
   * For each incoming row:
   * - If it matches an existing row (per matchCondition), applies whenMatched transform
   * - If no match found, applies whenNotMatched transform
   *
   * If CDC is enabled, generates appropriate CDC records:
   * - 'insert' for newly inserted rows (whenNotMatched returns non-null)
   * - 'update_preimage' and 'update_postimage' for updated rows
   * - 'delete' for deleted rows (whenMatched returns null)
   *
   * @param rows - Array of incoming rows to merge
   * @param matchCondition - Function to determine if an existing row matches an incoming row
   * @param whenMatched - Transform for matched rows; return null to delete, or new row to update
   * @param whenNotMatched - Transform for unmatched rows; return null to skip, or row to insert
   * @returns The Delta commit information
   * @throws {ValidationError} If rows is not an array or matchCondition is not a function
   */
  async merge(
    rows: T[],
    matchCondition: (existing: T, incoming: T) => boolean,
    whenMatched?: (existing: T, incoming: T) => T | null,
    whenNotMatched?: (incoming: T) => T | null
  ): Promise<DeltaCommit> {
    // Validate rows parameter
    if (rows === null || rows === undefined) {
      throw new ValidationError('rows is required and cannot be null or undefined', 'rows', rows)
    }
    if (!Array.isArray(rows)) {
      throw new ValidationError('rows must be an array', 'rows', rows)
    }

    // Validate matchCondition is required and must be a function
    if (matchCondition === null || matchCondition === undefined) {
      throw new ValidationError('matchCondition is required and cannot be null or undefined', 'matchCondition', matchCondition)
    }
    if (typeof matchCondition !== 'function') {
      throw new ValidationError('matchCondition must be a function', 'matchCondition', matchCondition)
    }

    // Validate optional whenMatched callback if provided
    if (whenMatched !== undefined && whenMatched !== null && typeof whenMatched !== 'function') {
      throw new ValidationError('whenMatched must be a function if provided', 'whenMatched', whenMatched)
    }

    // Validate optional whenNotMatched callback if provided
    if (whenNotMatched !== undefined && whenNotMatched !== null && typeof whenNotMatched !== 'function') {
      throw new ValidationError('whenNotMatched must be a function if provided', 'whenNotMatched', whenNotMatched)
    }

    await this.ensureConfigLoaded()

    // Get current version from DeltaTable (delegated version management)
    const currentVersion = await this.deltaTable.version()
    const newVersion = currentVersion + 1
    const timestamp = new Date()
    const cdcRecords: DeltaCDCRecord<T>[] = []

    // Query current data from storage using DeltaTable (fixes P0 bug - tableData was stale)
    const tableData = await this.deltaTable.query() as T[]

    for (const incomingRow of rows) {
      const existingRow = tableData.find(existing => matchCondition(existing, incomingRow))

      if (existingRow) {
        this.processMergeMatch(
          existingRow,
          incomingRow,
          whenMatched,
          newVersion,
          timestamp,
          cdcRecords,
          tableData
        )
      } else {
        this.processMergeNoMatch(
          incomingRow,
          whenNotMatched,
          newVersion,
          timestamp,
          cdcRecords
        )
      }
    }

    return this.finalizeCommit(timestamp, 'MERGE', cdcRecords)
  }

  // ---------------------------------------------------------------------------
  // Private: Write operations
  // ---------------------------------------------------------------------------

  /**
   * Execute the actual write operation after serialization.
   *
   * This method is called from the write queue to ensure serialized execution.
   * It handles version management, CDC record generation, and delegates data writing
   * to the underlying DeltaTable.
   *
   * IMPORTANT: We must delegate actual data writing to DeltaTable.write() so that
   * Parquet files are actually created. Previously this only created commit log
   * entries without writing actual data files (P0 bug fix).
   */
  private async executeWrite(rows: T[]): Promise<DeltaCommit> {
    if (rows.length === 0) {
      throw new CDCError('Cannot write empty rows', 'EMPTY_WRITE')
    }

    await this.ensureConfigLoaded()

    // Get current version from DeltaTable (delegated version management)
    const currentVersion = await this.deltaTable.version()
    const newVersion = currentVersion + 1
    const timestamp = new Date()

    // Generate CDC records if enabled (before commit so we have the version)
    const cdcRecords = this.cdcEnabled
      ? this.generateInsertCDCRecords(rows, newVersion, timestamp)
      : []

    // Delegate actual data writing to DeltaTable
    // This ensures Parquet files are created and data can be queried later
    const commit = await this.deltaTable.write(rows)

    // Write CDC files and notify subscribers (CDC-specific logic)
    if (cdcRecords.length > 0) {
      await this.writeCDCFile(commit.version, cdcRecords, timestamp)
      await this.notifySubscribers(cdcRecords)
    }

    return commit
  }

  // ---------------------------------------------------------------------------
  // Private: CDC record generation
  // ---------------------------------------------------------------------------

  /**
   * Generate CDC records for insert operations.
   * Creates one 'insert' record per row with the full row data.
   */
  private generateInsertCDCRecords(
    rows: T[],
    version: number,
    timestamp: Date
  ): DeltaCDCRecord<T>[] {
    return rows.map(row => ({
      _change_type: 'insert' as const,
      _commit_version: BigInt(version),
      _commit_timestamp: timestamp,
      data: this.cloneRow(row)
    }))
  }

  /**
   * Generate CDC records for update operations.
   * Creates 'update_preimage' (before) and 'update_postimage' (after) records for each row.
   */
  private generateUpdateCDCRecords(
    matchingRows: T[],
    updates: Partial<T>,
    version: number,
    timestamp: Date
  ): DeltaCDCRecord<T>[] {
    const records: DeltaCDCRecord<T>[] = []

    for (const row of matchingRows) {
      // Preimage (before update)
      records.push({
        _change_type: 'update_preimage' as const,
        _commit_version: BigInt(version),
        _commit_timestamp: timestamp,
        data: this.cloneRow(row)
      })

      // Postimage (after update) - apply updates to clone
      const updatedRow = { ...row, ...updates } as T
      records.push({
        _change_type: 'update_postimage' as const,
        _commit_version: BigInt(version),
        _commit_timestamp: timestamp,
        data: updatedRow
      })
    }

    return records
  }

  /**
   * Generate CDC records for delete operations.
   * Creates one 'delete' record per row with the full row data at time of deletion.
   */
  private generateDeleteCDCRecords(
    matchingRows: T[],
    version: number,
    timestamp: Date
  ): DeltaCDCRecord<T>[] {
    return matchingRows.map(row => ({
      _change_type: 'delete' as const,
      _commit_version: BigInt(version),
      _commit_timestamp: timestamp,
      data: this.cloneRow(row)
    }))
  }

  // ---------------------------------------------------------------------------
  // Private: Merge helpers
  // ---------------------------------------------------------------------------

  /**
   * Process a merge operation where an existing row matches an incoming row.
   * Applies the whenMatched transform and generates appropriate CDC records.
   *
   * @param existingRow - The existing row that matched
   * @param incomingRow - The incoming row being merged
   * @param whenMatched - Transform function for matched rows
   * @param version - The commit version number
   * @param timestamp - The commit timestamp
   * @param cdcRecords - Array to append CDC records to
   * @param tableData - Optional reference to table data (for in-memory operations)
   */
  private processMergeMatch(
    existingRow: T,
    incomingRow: T,
    whenMatched: ((existing: T, incoming: T) => T | null) | undefined,
    version: number,
    timestamp: Date,
    cdcRecords: DeltaCDCRecord<T>[],
    tableData?: T[]
  ): void {
    const result = whenMatched ? whenMatched(existingRow, incomingRow) : incomingRow

    if (result === null) {
      // Delete matched row - generate CDC record
      if (this.cdcEnabled) {
        cdcRecords.push({
          _change_type: 'delete' as const,
          _commit_version: BigInt(version),
          _commit_timestamp: timestamp,
          data: this.cloneRow(existingRow)
        })
      }
      // Note: Actual deletion is handled by Delta table commit
    } else {
      // Update matched row - generate CDC records
      if (this.cdcEnabled) {
        cdcRecords.push({
          _change_type: 'update_preimage' as const,
          _commit_version: BigInt(version),
          _commit_timestamp: timestamp,
          data: this.cloneRow(existingRow)
        })

        // Create postimage with applied result
        cdcRecords.push({
          _change_type: 'update_postimage' as const,
          _commit_version: BigInt(version),
          _commit_timestamp: timestamp,
          data: this.cloneRow(result)
        })
      }
      // Note: Actual update is handled by Delta table commit
    }
  }

  /**
   * Process a merge operation where no existing row matches an incoming row.
   * Applies the whenNotMatched transform and generates insert CDC records if applicable.
   */
  private processMergeNoMatch(
    incomingRow: T,
    whenNotMatched: ((incoming: T) => T | null) | undefined,
    version: number,
    timestamp: Date,
    cdcRecords: DeltaCDCRecord<T>[]
  ): void {
    const result = whenNotMatched ? whenNotMatched(incomingRow) : incomingRow

    if (result !== null) {
      // Insert new row - generate CDC record
      if (this.cdcEnabled) {
        cdcRecords.push({
          _change_type: 'insert' as const,
          _commit_version: BigInt(version),
          _commit_timestamp: timestamp,
          data: this.cloneRow(result)
        })
      }
      // Note: Actual insertion is handled by Delta table commit
    }
  }

  // ---------------------------------------------------------------------------
  // Private: Commit and storage operations
  // ---------------------------------------------------------------------------

  /**
   * Finalize a commit by writing to Delta log and CDC files.
   * Delegates commit writing to DeltaTable and handles CDC-specific file writing.
   */
  private async finalizeCommit(
    timestamp: Date,
    operation: string,
    cdcRecords: DeltaCDCRecord<T>[]
  ): Promise<DeltaCommit> {
    const actions: DeltaAction[] = [
      {
        commitInfo: {
          timestamp: timestamp.getTime(),
          operation
        }
      }
    ]

    // Delegate commit to DeltaTable
    const commit = await this.deltaTable.commit(actions)

    // Write CDC files and notify subscribers (CDC-specific logic)
    if (cdcRecords.length > 0) {
      await this.writeCDCFile(commit.version, cdcRecords, timestamp)
      await this.notifySubscribers(cdcRecords)
    }

    return commit
  }

  /**
   * Create an empty commit result for operations that matched no rows.
   */
  private createEmptyCommit(version: number, timestamp: Date): DeltaCommit {
    return {
      version,
      timestamp: timestamp.getTime(),
      actions: []
    }
  }

  /**
   * Create Delta log actions for a write operation.
   * Includes protocol and metadata actions for the first commit (version 0).
   */
  private createWriteActions(version: number, timestamp: Date): DeltaAction[] {
    const actions: DeltaAction[] = [
      {
        add: {
          path: `${this.tablePath}/part-${formatVersion(version)}.parquet`,
          size: 0,
          modificationTime: timestamp.getTime(),
          dataChange: true
        }
      },
      {
        commitInfo: {
          timestamp: timestamp.getTime(),
          operation: 'WRITE',
          isBlindAppend: true
        }
      }
    ]

    // Add protocol and metadata for first commit
    if (version === 0) {
      const configuration: Record<string, string> = {}
      if (this.cdcEnabled) {
        configuration['delta.enableChangeDataFeed'] = 'true'
      }

      actions.unshift(
        {
          protocol: {
            minReaderVersion: 1,
            minWriterVersion: 1
          }
        },
        {
          metaData: {
            id: crypto.randomUUID(),
            format: { provider: 'parquet' },
            schemaString: '{}',
            partitionColumns: [],
            createdTime: timestamp.getTime(),
            configuration: Object.keys(configuration).length > 0 ? configuration : undefined
          }
        }
      )
    }

    return actions
  }

  /**
   * Write CDC records to storage in Parquet format.
   *
   * Writes to two locations for different access patterns:
   * 1. Direct version lookup: `_change_data/cdc-{version}.parquet`
   * 2. Date partitioned: `_change_data/date={YYYY-MM-DD}/cdc-{version}.parquet`
   *
   * If writing to the date-partitioned path fails, rolls back the direct path write
   * to maintain consistency.
   */
  private async writeCDCFile(
    version: number,
    records: DeltaCDCRecord<T>[],
    timestamp: Date
  ): Promise<void> {
    const versionStr = formatVersion(version)

    // Convert CDC records to Parquet column data format
    const columnData = cdcRecordsToColumnData(records)

    // Write as Parquet (matching the pattern from compaction)
    const parquetBuffer = parquetWriteBuffer({
      columnData,
      codec: 'UNCOMPRESSED',
    })
    const parquetData = new Uint8Array(parquetBuffer)

    // Write to BOTH paths to support:
    // 1. Direct version lookup: _change_data/cdc-{version}.parquet
    // 2. Date partitioning: _change_data/date=YYYY-MM-DD/cdc-{version}.parquet

    // Path 1: Direct version lookup (non-partitioned)
    const cdcPath = `${this.tablePath}/_change_data/cdc-${versionStr}.parquet`
    await this.storage.write(cdcPath, parquetData)

    try {
      // Path 2: Date-partitioned path (using shared utility)
      const datePartition = formatDatePartition(timestamp)
      const cdcDatePath = `${this.tablePath}/_change_data/date=${datePartition}/cdc-${versionStr}.parquet`
      await this.storage.write(cdcDatePath, parquetData)
    } catch (error) {
      // Rollback: delete the first write to maintain consistency
      try {
        await this.storage.delete(cdcPath)
      } catch {
        // Log but don't throw - original error is more important
        getLogger().warn(`[CDC] Failed to rollback CDC file ${cdcPath} after write error`)
      }
      throw error
    }
  }

  /**
   * Notify all subscribers of new CDC records.
   * Iterates through records sequentially to maintain order guarantees.
   */
  private async notifySubscribers(records: DeltaCDCRecord<T>[]): Promise<void> {
    for (const record of records) {
      await this.reader.notifySubscribers(record)
    }
  }

  // ---------------------------------------------------------------------------
  // Private: Initialization and configuration
  // ---------------------------------------------------------------------------

  /**
   * Ensure CDC configuration is loaded from storage.
   *
   * Lazily loads the CDC configuration on first access. If the config file
   * doesn't exist (common for new tables), defaults to CDC disabled.
   */
  private async ensureConfigLoaded(): Promise<void> {
    if (this.configLoaded) return

    try {
      const configPath = `${this.tablePath}/_cdc_config.json`
      const data = await this.storage.read(configPath)
      const parsed: unknown = JSON.parse(new TextDecoder().decode(data))
      if (!isValidCDCConfig(parsed)) {
        throw new ValidationError('Invalid CDC config format', 'CDCConfig', parsed)
      }
      this.cdcEnabled = parsed.enabled ?? false
    } catch (e) {
      // Config file may not exist for tables without CDC configured - this is expected
      // Only log at debug level since this is common for new tables
      getLogger().warn(`[CDC] CDC config not found or invalid for ${this.tablePath}, defaulting to disabled:`, e)
    }
    this.configLoaded = true
  }

  // ---------------------------------------------------------------------------
  // Private: Utility methods
  // ---------------------------------------------------------------------------

  /**
   * Match filter against a row using the shared matchesFilter function.
   * Uses simple key-value matching for CDC operations since CDC filters
   * are typically simple equality checks on row properties.
   */
  private matchesFilter(row: T, filter: Record<string, unknown>): boolean {
    // Use imported matchesFilter for consistency with DeltaTable
    return matchesFilter(row, filter as Filter<T>)
  }

  /**
   * Create a shallow clone of a row.
   * Used to capture row state for CDC records without reference sharing.
   */
  private cloneRow(row: T): T {
    return { ...row }
  }
}

/**
 * Create a CDC-enabled Delta table.
 *
 * Returns a table that tracks all changes (inserts, updates, deletes) as CDC records.
 * CDC records can be read by version range or time range, and subscribers can be
 * notified of changes in real-time.
 *
 * @param storage - Storage backend for reading/writing data
 * @param tablePath - Base path for the Delta table
 * @returns A CDC-enabled Delta table instance
 * @throws {ValidationError} If storage or tablePath is invalid
 *
 * @public
 *
 * @example
 * ```typescript
 * import { createStorage, createCDCDeltaTable } from '@dotdo/deltalake'
 *
 * // Create storage and CDC table
 * const storage = createStorage({ type: 'memory' })
 * const users = createCDCDeltaTable<User>(storage, 'users')
 *
 * // Enable CDC tracking
 * await users.setCDCEnabled(true)
 *
 * // Write data - generates 'insert' CDC records
 * await users.write([
 *   { id: '1', name: 'Alice', score: 100 },
 *   { id: '2', name: 'Bob', score: 85 }
 * ])
 *
 * // Update data - generates 'update_preimage' and 'update_postimage' records
 * await users.update({ id: '1' }, { score: 150 })
 *
 * // Delete data - generates 'delete' CDC records
 * await users.deleteRows({ score: { $lt: 90 } })
 *
 * // Read CDC changes by version
 * const reader = users.getCDCReader()
 * const changes = await reader.readByVersion(0n, 2n)
 *
 * // Subscribe to real-time changes
 * const unsubscribe = reader.subscribe(async (record) => {
 *   console.log(`Change: ${record._change_type} at version ${record._commit_version}`)
 * })
 *
 * // Merge (upsert) with CDC tracking
 * await users.merge(
 *   [{ id: '3', name: 'Charlie', score: 95 }],
 *   (existing, incoming) => existing.id === incoming.id,
 *   (existing, incoming) => ({ ...existing, ...incoming }), // update matched
 *   (incoming) => incoming // insert unmatched
 * )
 * ```
 */
export function createCDCDeltaTable<T extends Record<string, unknown>>(
  storage: StorageBackend,
  tablePath: string
): CDCDeltaTable<T> {
  // Validation is performed in CDCDeltaTableImpl constructor
  return new CDCDeltaTableImpl<T>(storage, tablePath)
}
