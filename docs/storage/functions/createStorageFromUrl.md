[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / createStorageFromUrl

# Function: createStorageFromUrl()

> **createStorageFromUrl**(`url`, `options?`): [`StorageBackend`](../interfaces/StorageBackend.md)

Defined in: [src/storage/index.ts:619](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L619)

Create a storage backend from a URL/path string.

This function auto-detects the storage type from the URL scheme:
- `file:///path` or `/path` or `./path` -> FileSystemStorage
- `s3://bucket/path` -> S3Storage
- `r2://bucket/path` -> R2Storage (requires bucket binding)
- `memory://` -> MemoryStorage

Note: For R2 and S3 storage, you may need to provide additional configuration
that cannot be derived from the URL alone (e.g., R2 bucket binding, S3 credentials).
Use the overload with options for full configuration.

## Parameters

### url

`string`

Storage URL or path

### options?

Additional options to merge with URL-derived configuration

#### bucket?

`R2Bucket`

R2 bucket binding (required for r2:// URLs)

#### credentials?

[`S3Credentials`](../interfaces/S3Credentials.md)

S3 credentials (optional for s3:// URLs, uses environment if not provided)

#### region?

`string`

Override region for S3 (optional, extracted from URL or defaults to us-east-1)

## Returns

[`StorageBackend`](../interfaces/StorageBackend.md)

Configured StorageBackend instance

## Throws

If the URL format is not recognized

## Throws

If required configuration is missing (e.g., R2 bucket binding)

## Example

```typescript
// Filesystem storage from URL
const storage = createStorageFromUrl('file:///data/lake')

// Filesystem storage from path
const storage = createStorageFromUrl('/data/lake')

// S3 storage (credentials from environment)
const storage = createStorageFromUrl('s3://my-bucket/prefix')

// R2 storage (must provide bucket binding)
const storage = createStorageFromUrl('r2://my-bucket/prefix', {
  bucket: env.MY_BUCKET
})

// Memory storage
const storage = createStorageFromUrl('memory://')
```
