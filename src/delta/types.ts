/**
 * Delta Lake Type Definitions
 *
 * This module contains all interfaces and type definitions for Delta Lake operations.
 *
 * @module delta/types
 */

import type { StorageBackend } from '../storage/index.js'
import type { Filter, Projection } from '../query/index.js'

// =============================================================================
// DELETION VECTORS
// =============================================================================

/**
 * Descriptor for a deletion vector associated with a data file.
 *
 * Deletion vectors mark individual rows as deleted without rewriting Parquet files.
 * They are stored as RoaringBitmap data indicating which row indices are deleted.
 *
 * @see https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors
 */
export interface DeletionVectorDescriptor {
  /**
   * Storage type indicator:
   * - 'u': UUID-based relative path (most common)
   * - 'p': Absolute path
   * - 'i': Inline (Base85-encoded bitmap data)
   */
  storageType: 'u' | 'p' | 'i'

  /**
   * For storageType 'u': Base85-encoded UUID (and optional prefix)
   * For storageType 'p': Absolute file path
   * For storageType 'i': Base85-encoded RoaringBitmap data
   */
  pathOrInlineDv: string

  /**
   * Byte offset within the deletion vector file where this DV's data begins.
   * Only applicable for storageType 'u' and 'p'.
   */
  offset?: number

  /**
   * Size of the serialized deletion vector in bytes (before Base85 encoding if inline).
   */
  sizeInBytes: number

  /**
   * Number of rows marked as deleted by this deletion vector.
   */
  cardinality: number
}

// =============================================================================
// DELTA ACTIONS
// =============================================================================

export interface AddAction {
  add: {
    path: string
    size: number
    modificationTime: number
    dataChange: boolean
    partitionValues?: Record<string, string> | undefined
    stats?: string | undefined  // JSON encoded statistics
    tags?: Record<string, string> | undefined
    /** Deletion vector descriptor if this file has soft-deleted rows */
    deletionVector?: DeletionVectorDescriptor | undefined
  }
}

export interface RemoveAction {
  remove: {
    path: string
    deletionTimestamp: number
    dataChange: boolean
    partitionValues?: Record<string, string> | undefined
    extendedFileMetadata?: boolean | undefined
    size?: number | undefined
  }
}

export interface MetadataAction {
  metaData: {
    id: string
    name?: string | undefined
    description?: string | undefined
    format: { provider: string; options?: Record<string, string> | undefined }
    schemaString: string
    partitionColumns: string[]
    configuration?: Record<string, string> | undefined
    createdTime?: number | undefined
  }
}

export interface ProtocolAction {
  protocol: {
    minReaderVersion: number
    minWriterVersion: number
    /** Reader features required for protocol version 3+ */
    readerFeatures?: string[]
    /** Writer features required for protocol version 7+ */
    writerFeatures?: string[]
  }
}

export interface CommitInfoAction {
  commitInfo: {
    timestamp: number
    operation: string
    operationParameters?: Record<string, string> | undefined
    readVersion?: number | undefined
    isolationLevel?: string | undefined
    isBlindAppend?: boolean | undefined
  }
}

export type DeltaAction =
  | AddAction
  | RemoveAction
  | MetadataAction
  | ProtocolAction
  | CommitInfoAction

// =============================================================================
// DELTA COMMIT
// =============================================================================

export interface DeltaCommit {
  version: number
  timestamp: number
  actions: DeltaAction[]
}

// =============================================================================
// DELTA SNAPSHOT
// =============================================================================

export interface DeltaSnapshot {
  version: number
  files: AddAction['add'][]
  metadata?: MetadataAction['metaData'] | undefined
  protocol?: ProtocolAction['protocol'] | undefined
}

// =============================================================================
// COLUMN MAPPING SUPPORT
// =============================================================================

/**
 * Delta schema field with column mapping metadata
 */
export interface DeltaSchemaField {
  name: string
  type: string | object
  nullable?: boolean
  metadata?: {
    'delta.columnMapping.id'?: number
    'delta.columnMapping.physicalName'?: string
  }
}

/**
 * Delta schema structure
 */
export interface DeltaSchema {
  type: string
  fields: DeltaSchemaField[]
}

// =============================================================================
// QUERY OPTIONS
// =============================================================================

export interface QueryOptions<T = unknown> {
  /**
   * Query at a specific table version (time travel)
   * If provided, the query will use the snapshot at this version
   */
  readonly version?: number
  /**
   * Use a pre-fetched snapshot for the query
   * Useful for consistent reads across multiple queries
   */
  readonly snapshot?: DeltaSnapshot
  /**
   * Field projection to return only specified fields
   * - Array format: ['name', 'age'] - include only these fields
   * - Object format: { name: 1, age: 1 } - include only these fields
   * - Exclusion: { password: 0 } - exclude these fields
   * - Nested fields supported: 'address.city' or { 'address.city': 1 }
   */
  readonly projection?: Projection<T>
}

// =============================================================================
// WRITE OPTIONS
// =============================================================================

export interface WriteOptions {
  /**
   * Partition columns for this write operation.
   * If specified, rows will be grouped by these columns and written to
   * partitioned paths: table/col=value/part-*.parquet
   *
   * For subsequent writes, partition columns should match the table's
   * existing partition configuration.
   */
  readonly partitionColumns?: readonly string[]
}

// =============================================================================
// CHECKPOINT CONFIG
// =============================================================================

export interface CheckpointConfig {
  readonly checkpointInterval?: number | undefined
  readonly maxActionsPerCheckpoint?: number | undefined
  readonly checkpointRetentionMs?: number | undefined
  readonly numRetainedCheckpoints?: number | undefined
  readonly maxCheckpointSizeBytes?: number | undefined
}

// =============================================================================
// COMPACTION CONTEXT
// =============================================================================

/**
 * Context interface for compaction and maintenance operations.
 *
 * This interface provides a clean abstraction for operations that need
 * direct access to storage and table internals, without exposing the
 * internal implementation details of DeltaTable.
 *
 * Use `table.getCompactionContext()` to obtain this context for compaction,
 * vacuum, and other maintenance operations.
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
export interface CompactionContext<T extends Record<string, unknown> = Record<string, unknown>> {
  /**
   * The storage backend for direct file operations.
   */
  readonly storage: StorageBackend

  /**
   * The base path of the Delta table.
   */
  readonly tablePath: string

  /**
   * Read rows from a data file.
   *
   * This method reads data from the in-memory cache if available,
   * falling back to reading from the actual Parquet file.
   *
   * @param path - The file path relative to the table root
   * @returns Array of rows from the file
   */
  readFile(path: string): Promise<T[]>

  /**
   * Write rows to the in-memory cache for a file path.
   *
   * Note: This only caches the data in memory. The actual Parquet file
   * must be written separately using the storage backend.
   *
   * @param path - The file path relative to the table root
   * @param rows - The rows to cache
   */
  cacheFile(path: string, rows: T[]): void

  /**
   * Remove a file from the in-memory cache.
   *
   * Note: This only removes the data from cache. The actual file
   * deletion must be handled separately.
   *
   * @param path - The file path to remove from cache
   * @returns true if the file was in cache and removed, false otherwise
   */
  uncacheFile(path: string): boolean

  /**
   * Get the current table version.
   *
   * @returns The current version number
   */
  version(): Promise<number>

  /**
   * Get a snapshot of the table at a specific version.
   *
   * @param version - Optional version to get snapshot at (defaults to current)
   * @returns The table snapshot
   */
  snapshot(version?: number): Promise<DeltaSnapshot>

  /**
   * Commit a set of Delta actions atomically.
   *
   * @param actions - The actions to commit
   * @returns The commit information
   */
  commit(actions: DeltaAction[]): Promise<DeltaCommit>

  /**
   * Query all rows from the table.
   *
   * @returns All rows in the table
   */
  queryAll(): Promise<T[]>
}

// =============================================================================
// LAST CHECKPOINT
// =============================================================================

export interface LastCheckpoint {
  version: number
  size: number
  parts?: number
  sizeInBytes?: number
  numOfAddFiles?: number
}

// =============================================================================
// FILE STATISTICS
// =============================================================================

export interface FileStats {
  numRecords: number
  minValues: Record<string, unknown>
  maxValues: Record<string, unknown>
  nullCount: Record<string, number>
}

// =============================================================================
// INTERNAL TYPES FOR SCHEMA INFERENCE
// =============================================================================

export type InferredFieldType = 'boolean' | 'int32' | 'int64' | 'float' | 'double' | 'string' | 'binary' | 'variant' | 'timestamp'

export interface InferredField {
  name: string
  type: InferredFieldType
  optional: boolean
}

export interface InferredSchema {
  fields: InferredField[]
}

// Re-export query types for convenience
export type { Filter, Projection }
