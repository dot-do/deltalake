/**
 * @dotdo/deltalake
 *
 * Delta Lake format implementation - shared foundation for MongoLake and KafkaLake.
 *
 * @example
 * ```typescript
 * import { createStorage, DeltaTable, CDCProducer, compact, deduplicate } from '@dotdo/deltalake'
 *
 * // Create storage (auto-detects: fs locally, R2 in production)
 * const storage = createStorage()
 *
 * // Create a Delta table
 * const events = new DeltaTable(storage, 'events')
 *
 * // Write data with ACID guarantees
 * await events.write([
 *   { _id: '1', user: 'alice', action: 'click' },
 *   { _id: '2', user: 'bob', action: 'purchase' },
 * ])
 *
 * // Query with predicate pushdown
 * const results = await events.query({
 *   action: { $in: ['click', 'purchase'] },
 *   _ts: { $gte: hourAgo }
 * })
 *
 * // Compact small files into larger ones
 * await compact(events, { targetFileSize: 128 * 1024 * 1024 })
 *
 * // Remove duplicate rows
 * await deduplicate(events, { primaryKey: ['_id'], keepStrategy: 'latest' })
 * ```
 */

// Error hierarchy (central error module)
export {
  // Base error
  DeltaLakeError,
  // Storage errors
  StorageError,
  FileNotFoundError,
  VersionMismatchError,
  S3Error,
  // Concurrency errors
  ConcurrencyError,
  type ConcurrencyErrorOptions,
  // CDC errors
  CDCError,
  type CDCErrorCode,
  // Validation errors
  ValidationError,
  // Type guards
  isDeltaLakeError,
  isStorageError,
  isConcurrencyError,
  isCDCError,
  isValidationError,
  isRetryableError,
} from './errors.js'

// Storage backends
export {
  type StorageBackend,
  type StorageOptions,
  type ParsedStorageUrl,
  createStorage,
  parseStorageUrl,
} from './storage/index.js'

// Parquet utilities
export {
  type VariantValue,
  type EncodedVariant,
  type AsyncBuffer,
  type ZoneMapFilter,
  type ParquetMetadata,
  type ParquetSchema,
  type ParquetField,
  encodeVariant,
  decodeVariant,
  createAsyncBuffer,
  canSkipZoneMap,
} from './parquet/index.js'

// Query layer
export {
  type Filter,
  type ComparisonOperators,
  matchesFilter,
  filterToParquetPredicate,
  // Aggregation pipeline
  aggregate,
  type AggregationPipeline,
  type AggregationStage,
  type AggregationResult,
  type GroupSpec,
  type SortSpec,
  type AccumulatorExpression,
  type AccumulatorOperator,
  type MatchStage,
  type GroupStage,
  type ProjectStage,
  type SortStage,
  type LimitStage,
  type SkipStage,
  type UnwindStage,
} from './query/index.js'

// CDC primitives
export {
  type CDCRecord,
  type CDCSource,
  type CDCOperation,
  CDCProducer,
  CDCConsumer,
  type CDCConsumerOptions,
  type CDCRecordHandler,
  // Delta Lake CDC extensions
  type DeltaCDCChangeType,
  type DeltaCDCRecord,
  type CDCConfig,
  type CDCReader,
  type CDCDeltaTable,
  createCDCDeltaTable,
  // Validation
  isValidCDCConfig,
  // Offset tracking (Kafka-style consumer semantics)
  type ConsumerOffset,
  type OffsetCommitOptions,
  type OffsetStorage,
  MemoryOffsetStorage,
} from './cdc/index.js'

// Delta Lake format
export {
  DeltaTable,
  type DeltaCommit,
  type DeltaSnapshot,
  type AddAction,
  type RemoveAction,
  // Write options (partitioning support)
  type WriteOptions,
  // Version formatting utility
  formatVersion,
  VERSION_DIGITS,
  // VACUUM operation
  vacuum,
  type VacuumConfig,
  type VacuumMetrics,
  formatBytes,
  formatDuration,
  // Validation type guards
  type FileStats,
  isValidFileStats,
  isValidPartitionValues,
  isValidDeltaAction,
  isValidAddAction,
  isValidRemoveAction,
  isValidMetadataAction,
  isValidProtocolAction,
  isValidCommitInfoAction,
} from './delta/index.js'

// Logging abstraction and utilities
export {
  type Logger,
  defaultLogger,
  setLogger,
  getLogger,
  assertNever,
} from './utils/index.js'

// Compaction and optimization utilities
export {
  // Core functions
  compact,
  deduplicate,
  zOrderCluster,
  // Configuration types
  type CompactionConfig,
  type DeduplicationConfig,
  type ClusteringConfig,
  // Metrics types
  type CompactionMetrics,
  type DeduplicationMetrics,
  type ClusteringMetrics,
} from './compaction/index.js'
