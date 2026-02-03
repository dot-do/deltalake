[**@dotdo/deltalake v0.0.1**](../README.md)

***

[@dotdo/deltalake](../modules.md) / delta

# delta

Delta Lake Transaction Log

JSON-based transaction log format for ACID operations.

This module provides:
- Action types (Add, Remove, Metadata, Protocol, CommitInfo)
- Transaction log utilities for parsing and serialization
- DeltaTable class for table operations
- Checkpoint management
- Concurrency control with optimistic locking
- Deletion vector support (soft-delete without rewriting Parquet files)

## Classes

- [AbortError](classes/AbortError.md)

## Interfaces

- [RetryInfo](interfaces/RetryInfo.md)
- [SuccessInfo](interfaces/SuccessInfo.md)
- [FailureInfo](interfaces/FailureInfo.md)
- [RetryMetrics](interfaces/RetryMetrics.md)
- [RetryConfig](interfaces/RetryConfig.md)
- [RetryResultWithMetrics](interfaces/RetryResultWithMetrics.md)
- [DeletionVectorDescriptor](interfaces/DeletionVectorDescriptor.md)
- [MetadataAction](interfaces/MetadataAction.md)
- [ProtocolAction](interfaces/ProtocolAction.md)
- [CommitInfoAction](interfaces/CommitInfoAction.md)
- [DeltaSchemaField](interfaces/DeltaSchemaField.md)
- [DeltaSchema](interfaces/DeltaSchema.md)
- [QueryOptions](interfaces/QueryOptions.md)
- [CheckpointConfig](interfaces/CheckpointConfig.md)
- [CompactionContext](interfaces/CompactionContext.md)
- [LastCheckpoint](interfaces/LastCheckpoint.md)
- [InferredField](interfaces/InferredField.md)
- [InferredSchema](interfaces/InferredSchema.md)

## Type Aliases

- [DeltaAction](type-aliases/DeltaAction.md)
- [InferredFieldType](type-aliases/InferredFieldType.md)

## Variables

- [LAST\_CHECKPOINT\_FILE](variables/LAST_CHECKPOINT_FILE.md)
- [PARQUET\_MAGIC](variables/PARQUET_MAGIC.md)
- [DEFAULT\_CHECKPOINT\_CONFIG](variables/DEFAULT_CHECKPOINT_CONFIG.md)
- [VERSION\_DIGITS](variables/VERSION_DIGITS.md)
- [DELTA\_LOG\_DIR](variables/DELTA_LOG_DIR.md)
- [transactionLog](variables/transactionLog.md)
- [DEFAULT\_RETRY\_CONFIG](variables/DEFAULT_RETRY_CONFIG.md)

## Functions

- [readLastCheckpoint](functions/readLastCheckpoint.md)
- [writeLastCheckpoint](functions/writeLastCheckpoint.md)
- [readCheckpoint](functions/readCheckpoint.md)
- [readCheckpointPart](functions/readCheckpointPart.md)
- [readMultiPartCheckpoint](functions/readMultiPartCheckpoint.md)
- [rowsToSnapshot](functions/rowsToSnapshot.md)
- [writeSingleCheckpoint](functions/writeSingleCheckpoint.md)
- [createMultiPartCheckpoint](functions/createMultiPartCheckpoint.md)
- [actionsToColumns](functions/actionsToColumns.md)
- [estimateCheckpointSize](functions/estimateCheckpointSize.md)
- [discoverCheckpoints](functions/discoverCheckpoints.md)
- [findLatestCheckpoint](functions/findLatestCheckpoint.md)
- [validateCheckpoint](functions/validateCheckpoint.md)
- [cleanupCheckpoints](functions/cleanupCheckpoints.md)
- [getCleanableLogVersions](functions/getCleanableLogVersions.md)
- [cleanupLogs](functions/cleanupLogs.md)
- [shouldCheckpoint](functions/shouldCheckpoint.md)
- [z85Decode](functions/z85Decode.md)
- [z85DecodeUuid](functions/z85DecodeUuid.md)
- [getDeletionVectorPath](functions/getDeletionVectorPath.md)
- [parseRoaringBitmap](functions/parseRoaringBitmap.md)
- [loadDeletionVector](functions/loadDeletionVector.md)
- [formatVersion](functions/formatVersion.md)
- [extractPartitionValuesFromPath](functions/extractPartitionValuesFromPath.md)
- [decodeFilePath](functions/decodeFilePath.md)
- [createAddAction](functions/createAddAction.md)
- [createRemoveAction](functions/createRemoveAction.md)
- [serializeAction](functions/serializeAction.md)
- [deserializeAction](functions/deserializeAction.md)
- [validateAddAction](functions/validateAddAction.md)
- [validateRemoveAction](functions/validateRemoveAction.md)
- [parseStats](functions/parseStats.md)
- [encodeStats](functions/encodeStats.md)
- [withRetry](functions/withRetry.md)
- [buildColumnMapping](functions/buildColumnMapping.md)
- [applyColumnMapping](functions/applyColumnMapping.md)
- [isValidDeltaSchemaField](functions/isValidDeltaSchemaField.md)
- [isValidDeltaSchema](functions/isValidDeltaSchema.md)
- [isValidLastCheckpoint](functions/isValidLastCheckpoint.md)
- [isAddAction](functions/isAddAction.md)
- [isRemoveAction](functions/isRemoveAction.md)
- [isMetadataAction](functions/isMetadataAction.md)
- [isProtocolAction](functions/isProtocolAction.md)
- [isCommitInfoAction](functions/isCommitInfoAction.md)

## References

### ConcurrencyError

Re-exports [ConcurrencyError](../errors/classes/ConcurrencyError.md)

***

### ConcurrencyErrorOptions

Re-exports [ConcurrencyErrorOptions](../errors/interfaces/ConcurrencyErrorOptions.md)

***

### ValidationError

Re-exports [ValidationError](../errors/classes/ValidationError.md)

***

### matchesFilter

Re-exports [matchesFilter](../query/functions/matchesFilter.md)

***

### AddAction

Re-exports [AddAction](../index/interfaces/AddAction.md)

***

### RemoveAction

Re-exports [RemoveAction](../index/interfaces/RemoveAction.md)

***

### DeltaCommit

Re-exports [DeltaCommit](../index/interfaces/DeltaCommit.md)

***

### DeltaSnapshot

Re-exports [DeltaSnapshot](../index/interfaces/DeltaSnapshot.md)

***

### WriteOptions

Re-exports [WriteOptions](../index/interfaces/WriteOptions.md)

***

### FileStats

Re-exports [FileStats](../index/interfaces/FileStats.md)

***

### Filter

Re-exports [Filter](../query/type-aliases/Filter.md)

***

### Projection

Re-exports [Projection](../query/type-aliases/Projection.md)

***

### isValidPartitionValues

Re-exports [isValidPartitionValues](../index/functions/isValidPartitionValues.md)

***

### isValidFileStats

Re-exports [isValidFileStats](../index/functions/isValidFileStats.md)

***

### isValidAddAction

Re-exports [isValidAddAction](../index/functions/isValidAddAction.md)

***

### isValidRemoveAction

Re-exports [isValidRemoveAction](../index/functions/isValidRemoveAction.md)

***

### isValidMetadataAction

Re-exports [isValidMetadataAction](../index/functions/isValidMetadataAction.md)

***

### isValidProtocolAction

Re-exports [isValidProtocolAction](../index/functions/isValidProtocolAction.md)

***

### isValidCommitInfoAction

Re-exports [isValidCommitInfoAction](../index/functions/isValidCommitInfoAction.md)

***

### isValidDeltaAction

Re-exports [isValidDeltaAction](../index/functions/isValidDeltaAction.md)

***

### DeltaTable

Re-exports [DeltaTable](../index/classes/DeltaTable.md)

***

### isRetryableError

Re-exports [isRetryableError](../errors/functions/isRetryableError.md)

***

### vacuum

Re-exports [vacuum](../index/functions/vacuum.md)

***

### formatBytes

Re-exports [formatBytes](../index/functions/formatBytes.md)

***

### formatDuration

Re-exports [formatDuration](../index/functions/formatDuration.md)

***

### VacuumConfig

Re-exports [VacuumConfig](../index/interfaces/VacuumConfig.md)

***

### VacuumMetrics

Re-exports [VacuumMetrics](../index/interfaces/VacuumMetrics.md)
