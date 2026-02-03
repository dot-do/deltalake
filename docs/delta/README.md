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

- [DeltaTable](classes/DeltaTable.md)
- [AbortError](classes/AbortError.md)

## Interfaces

- [DeletionVectorDescriptor](interfaces/DeletionVectorDescriptor.md)
- [AddAction](interfaces/AddAction.md)
- [RemoveAction](interfaces/RemoveAction.md)
- [MetadataAction](interfaces/MetadataAction.md)
- [ProtocolAction](interfaces/ProtocolAction.md)
- [CommitInfoAction](interfaces/CommitInfoAction.md)
- [DeltaCommit](interfaces/DeltaCommit.md)
- [DeltaSnapshot](interfaces/DeltaSnapshot.md)
- [QueryOptions](interfaces/QueryOptions.md)
- [WriteOptions](interfaces/WriteOptions.md)
- [CheckpointConfig](interfaces/CheckpointConfig.md)
- [FileStats](interfaces/FileStats.md)
- [RetryInfo](interfaces/RetryInfo.md)
- [SuccessInfo](interfaces/SuccessInfo.md)
- [FailureInfo](interfaces/FailureInfo.md)
- [RetryMetrics](interfaces/RetryMetrics.md)
- [RetryConfig](interfaces/RetryConfig.md)
- [RetryResultWithMetrics](interfaces/RetryResultWithMetrics.md)

## Type Aliases

- [DeltaAction](type-aliases/DeltaAction.md)

## Variables

- [VERSION\_DIGITS](variables/VERSION_DIGITS.md)
- [DELTA\_LOG\_DIR](variables/DELTA_LOG_DIR.md)
- [transactionLog](variables/transactionLog.md)
- [DEFAULT\_RETRY\_CONFIG](variables/DEFAULT_RETRY_CONFIG.md)

## Functions

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
- [isAddAction](functions/isAddAction.md)
- [isRemoveAction](functions/isRemoveAction.md)
- [isMetadataAction](functions/isMetadataAction.md)
- [isProtocolAction](functions/isProtocolAction.md)
- [isCommitInfoAction](functions/isCommitInfoAction.md)
- [isValidAddAction](functions/isValidAddAction.md)
- [isValidRemoveAction](functions/isValidRemoveAction.md)
- [isValidMetadataAction](functions/isValidMetadataAction.md)
- [isValidProtocolAction](functions/isValidProtocolAction.md)
- [isValidCommitInfoAction](functions/isValidCommitInfoAction.md)
- [isValidDeltaAction](functions/isValidDeltaAction.md)
- [serializeAction](functions/serializeAction.md)
- [deserializeAction](functions/deserializeAction.md)
- [validateAddAction](functions/validateAddAction.md)
- [validateRemoveAction](functions/validateRemoveAction.md)
- [parseStats](functions/parseStats.md)
- [encodeStats](functions/encodeStats.md)
- [withRetry](functions/withRetry.md)

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

### Filter

Re-exports [Filter](../query/type-aliases/Filter.md)

***

### Projection

Re-exports [Projection](../query/type-aliases/Projection.md)

***

### matchesFilter

Re-exports [matchesFilter](../query/functions/matchesFilter.md)

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
