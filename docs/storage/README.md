[**@dotdo/deltalake v0.0.1**](../README.md)

***

[@dotdo/deltalake](../modules.md) / storage

# storage

## Classes

- [FileSystemStorage](classes/FileSystemStorage.md)
- [R2Storage](classes/R2Storage.md)
- [S3Storage](classes/S3Storage.md)
- [MemoryStorage](classes/MemoryStorage.md)

## Interfaces

- [AsyncBuffer](interfaces/AsyncBuffer.md)
- [StorageBackend](interfaces/StorageBackend.md)
- [FileStat](interfaces/FileStat.md)
- [S3Credentials](interfaces/S3Credentials.md)
- [ParsedStorageUrl](interfaces/ParsedStorageUrl.md)
- [S3ClientLike](interfaces/S3ClientLike.md)
- [S3StorageOptions](interfaces/S3StorageOptions.md)
- [LatencyConfig](interfaces/LatencyConfig.md)
- [MemoryStorageOptions](interfaces/MemoryStorageOptions.md)
- [OperationRecord](interfaces/OperationRecord.md)
- [StorageSnapshot](interfaces/StorageSnapshot.md)

## Type Aliases

- [StorageOptions](type-aliases/StorageOptions.md)
- [MemoryStorageOperation](type-aliases/MemoryStorageOperation.md)

## Functions

- [createAsyncBuffer](functions/createAsyncBuffer.md)
- [parseStorageUrl](functions/parseStorageUrl.md)
- [createStorageFromUrl](functions/createStorageFromUrl.md)
- [createStorage](functions/createStorage.md)

## References

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

### ValidationError

Re-exports [ValidationError](../errors/classes/ValidationError.md)
