[**@dotdo/deltalake v0.0.1**](../README.md)

***

[@dotdo/deltalake](../modules.md) / index

# index

## Interfaces

- [VacuumConfig](interfaces/VacuumConfig.md)
- [VacuumMetrics](interfaces/VacuumMetrics.md)
- [EncodedVariant](interfaces/EncodedVariant.md)

## Type Aliases

- [VariantValue](type-aliases/VariantValue.md)

## Functions

- [vacuum](functions/vacuum.md)
- [formatBytes](functions/formatBytes.md)
- [formatDuration](functions/formatDuration.md)
- [encodeVariant](functions/encodeVariant.md)
- [decodeVariant](functions/decodeVariant.md)

## References

### DeltaLakeError

Re-exports [DeltaLakeError](../errors/classes/DeltaLakeError.md)

***

### StorageError

Re-exports [StorageError](../errors/classes/StorageError.md)

***

### FileNotFoundError

Re-exports [FileNotFoundError](../errors/classes/FileNotFoundError.md)

***

### VersionMismatchError

Re-exports [VersionMismatchError](../errors/classes/VersionMismatchError.md)

***

### S3Error

Re-exports [S3Error](../errors/classes/S3Error.md)

***

### ConcurrencyError

Re-exports [ConcurrencyError](../errors/classes/ConcurrencyError.md)

***

### ConcurrencyErrorOptions

Re-exports [ConcurrencyErrorOptions](../errors/interfaces/ConcurrencyErrorOptions.md)

***

### CDCError

Re-exports [CDCError](../errors/classes/CDCError.md)

***

### CDCErrorCode

Re-exports [CDCErrorCode](../errors/type-aliases/CDCErrorCode.md)

***

### ValidationError

Re-exports [ValidationError](../errors/classes/ValidationError.md)

***

### isDeltaLakeError

Re-exports [isDeltaLakeError](../errors/functions/isDeltaLakeError.md)

***

### isStorageError

Re-exports [isStorageError](../errors/functions/isStorageError.md)

***

### isConcurrencyError

Re-exports [isConcurrencyError](../errors/functions/isConcurrencyError.md)

***

### isCDCError

Re-exports [isCDCError](../errors/functions/isCDCError.md)

***

### isValidationError

Re-exports [isValidationError](../errors/functions/isValidationError.md)

***

### isRetryableError

Re-exports [isRetryableError](../errors/functions/isRetryableError.md)

***

### StorageBackend

Re-exports [StorageBackend](../storage/interfaces/StorageBackend.md)

***

### StorageOptions

Re-exports [StorageOptions](../storage/type-aliases/StorageOptions.md)

***

### ParsedStorageUrl

Re-exports [ParsedStorageUrl](../storage/interfaces/ParsedStorageUrl.md)

***

### createStorage

Re-exports [createStorage](../storage/functions/createStorage.md)

***

### parseStorageUrl

Re-exports [parseStorageUrl](../storage/functions/parseStorageUrl.md)

***

### AsyncBuffer

Re-exports [AsyncBuffer](../storage/interfaces/AsyncBuffer.md)

***

### ZoneMapFilter

Re-exports [ZoneMapFilter](../parquet/interfaces/ZoneMapFilter.md)

***

### ParquetMetadata

Re-exports [ParquetMetadata](../parquet/interfaces/ParquetMetadata.md)

***

### ParquetSchema

Re-exports [ParquetSchema](../parquet/interfaces/ParquetSchema.md)

***

### ParquetField

Re-exports [ParquetField](../parquet/interfaces/ParquetField.md)

***

### createAsyncBuffer

Re-exports [createAsyncBuffer](../storage/functions/createAsyncBuffer.md)

***

### canSkipZoneMap

Re-exports [canSkipZoneMap](../parquet/functions/canSkipZoneMap.md)

***

### Filter

Re-exports [Filter](../query/type-aliases/Filter.md)

***

### ComparisonOperators

Re-exports [ComparisonOperators](../query/interfaces/ComparisonOperators.md)

***

### matchesFilter

Re-exports [matchesFilter](../query/functions/matchesFilter.md)

***

### filterToParquetPredicate

Re-exports [filterToParquetPredicate](../query/functions/filterToParquetPredicate.md)

***

### CDCRecord

Re-exports [CDCRecord](../cdc/interfaces/CDCRecord.md)

***

### CDCSource

Re-exports [CDCSource](../cdc/interfaces/CDCSource.md)

***

### CDCOperation

Re-exports [CDCOperation](../cdc/type-aliases/CDCOperation.md)

***

### CDCProducer

Re-exports [CDCProducer](../cdc/classes/CDCProducer.md)

***

### CDCConsumer

Re-exports [CDCConsumer](../cdc/classes/CDCConsumer.md)

***

### DeltaCDCChangeType

Re-exports [DeltaCDCChangeType](../cdc/type-aliases/DeltaCDCChangeType.md)

***

### DeltaCDCRecord

Re-exports [DeltaCDCRecord](../cdc/interfaces/DeltaCDCRecord.md)

***

### CDCConfig

Re-exports [CDCConfig](../cdc/interfaces/CDCConfig.md)

***

### CDCReader

Re-exports [CDCReader](../cdc/interfaces/CDCReader.md)

***

### CDCDeltaTable

Re-exports [CDCDeltaTable](../cdc/interfaces/CDCDeltaTable.md)

***

### createCDCDeltaTable

Re-exports [createCDCDeltaTable](../cdc/functions/createCDCDeltaTable.md)

***

### DeltaTable

Re-exports [DeltaTable](../delta/classes/DeltaTable.md)

***

### DeltaCommit

Re-exports [DeltaCommit](../delta/interfaces/DeltaCommit.md)

***

### DeltaSnapshot

Re-exports [DeltaSnapshot](../delta/interfaces/DeltaSnapshot.md)

***

### AddAction

Re-exports [AddAction](../delta/interfaces/AddAction.md)

***

### RemoveAction

Re-exports [RemoveAction](../delta/interfaces/RemoveAction.md)

***

### WriteOptions

Re-exports [WriteOptions](../delta/interfaces/WriteOptions.md)

***

### formatVersion

Re-exports [formatVersion](../delta/functions/formatVersion.md)

***

### VERSION\_DIGITS

Re-exports [VERSION_DIGITS](../delta/variables/VERSION_DIGITS.md)
