[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / createStorage

# Function: createStorage()

> **createStorage**(`optionsOrUrl?`, `urlOptions?`): [`StorageBackend`](../interfaces/StorageBackend.md)

Defined in: src/storage/index.ts:635

Create a storage backend instance.

Supports multiple calling conventions:

1. **No arguments**: Auto-detect environment
   - Node.js: Uses FileSystemStorage with `./.deltalake` path
   - Other environments: Throws error

2. **String URL/path**: Auto-detect from URL scheme
   - `file:///path` or `/path` -> FileSystemStorage
   - `s3://bucket/path` -> S3Storage
   - `r2://bucket/path` -> R2Storage (requires options.bucket)
   - `memory://` -> MemoryStorage

3. **Options object**: Explicit configuration
   - `{ type: 'memory' }`
   - `{ type: 'filesystem', path: './data' }`
   - `{ type: 's3', bucket: 'name', region: 'us-east-1' }`
   - `{ type: 'r2', bucket: r2BucketBinding }`

## Parameters

### optionsOrUrl?

Storage URL string or configuration options

`string` | [`StorageOptions`](../type-aliases/StorageOptions.md)

### urlOptions?

Additional options when using URL string (for R2 bucket, S3 credentials)

#### bucket?

`R2Bucket`

#### credentials?

[`S3Credentials`](../interfaces/S3Credentials.md)

#### region?

`string`

## Returns

[`StorageBackend`](../interfaces/StorageBackend.md)

Configured StorageBackend instance

## Throws

If options are required but not provided

## Throws

If unknown storage type is specified

## Example

```typescript
// Auto-detect (Node.js -> filesystem)
const storage = createStorage()

// From URL
const storage = createStorage('s3://my-bucket/data')
const storage = createStorage('file:///data/lake')
const storage = createStorage('/data/lake')
const storage = createStorage('memory://')

// Explicit options
const storage = createStorage({ type: 'memory' })
const storage = createStorage({ type: 'filesystem', path: './data' })
```
