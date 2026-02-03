[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / S3Storage

# Class: S3Storage

Defined in: [src/storage/index.ts:1550](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1550)

AWS S3 storage backend.

Uses AWS SDK v3 patterns for S3 operations. Supports:
- GetObject, PutObject, HeadObject, ListObjectsV2, DeleteObject
- Multipart upload for files >5MB
- Byte-range reads for efficient Parquet file access
- Proper error handling for NoSuchKey, AccessDenied, NoSuchBucket

## Version Tracking
Uses S3 object ETags as version identifiers for conditional writes.

## Multipart Uploads
Files larger than 5MB are automatically uploaded using multipart upload
with 5MB part sizes. Failed uploads are automatically aborted.

## Client Injection
The S3 client can be injected via constructor options (preferred) or set directly:
- Constructor injection: `new S3Storage({ bucket, region, client: myS3Client })`
- Direct assignment: `storage._client = mockClient`
Without a client, operations will throw "S3 client not initialized".

## Example

```typescript
// Basic usage (requires client injection before operations)
const storage = new S3Storage({
  bucket: 'my-bucket',
  region: 'us-east-1',
  credentials: {
    accessKeyId: 'AKIA...',
    secretAccessKey: '...'
  }
})

// With client injection (production use)
import { S3Client } from '@aws-sdk/client-s3'
const s3Client = new S3Client({ region: 'us-east-1' })
const storage = new S3Storage({
  bucket: 'my-bucket',
  region: 'us-east-1',
  client: s3Client  // Client injected via constructor
})

// With mock client (testing)
const storage = new S3Storage({
  bucket: 'test-bucket',
  region: 'us-east-1',
  client: mockS3Client
})
```

## Implements

- [`StorageBackend`](../interfaces/StorageBackend.md)

## Constructors

### Constructor

> **new S3Storage**(`options`): `S3Storage`

Defined in: [src/storage/index.ts:1561](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1561)

#### Parameters

##### options

[`S3StorageOptions`](../interfaces/S3StorageOptions.md)

#### Returns

`S3Storage`

## Properties

### \_client

> **\_client**: [`S3ClientLike`](../interfaces/S3ClientLike.md) \| `null` = `null`

Defined in: [src/storage/index.ts:1554](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1554)

***

### options

> `readonly` **options**: [`S3StorageOptions`](../interfaces/S3StorageOptions.md)

Defined in: [src/storage/index.ts:1561](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1561)

## Methods

### read()

> **read**(`path`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: [src/storage/index.ts:1676](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1676)

Read the entire contents of a file.

#### Parameters

##### path

`string`

Path to the file (relative to storage root)

#### Returns

`Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Promise resolving to file contents as Uint8Array

#### Throws

If file does not exist

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`read`](../interfaces/StorageBackend.md#read)

***

### write()

> **write**(`path`, `data`): `Promise`\<`void`\>

Defined in: [src/storage/index.ts:1699](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1699)

Write data to a file, creating it if it doesn't exist or overwriting if it does.

#### Parameters

##### path

`string`

Path to the file (relative to storage root)

##### data

`Uint8Array`

Data to write

#### Returns

`Promise`\<`void`\>

Promise that resolves when write is complete

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`write`](../interfaces/StorageBackend.md#write)

***

### list()

> **list**(`prefix`): `Promise`\<`string`[]\>

Defined in: [src/storage/index.ts:1805](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1805)

List all files matching a prefix.

#### Parameters

##### prefix

`string`

Prefix to match (e.g., "data/" for all files in data directory)

#### Returns

`Promise`\<`string`[]\>

Promise resolving to array of file paths (not directories)

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`list`](../interfaces/StorageBackend.md#list)

***

### delete()

> **delete**(`path`): `Promise`\<`void`\>

Defined in: [src/storage/index.ts:1840](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1840)

Delete a file. This operation is idempotent - deleting a non-existent file
does not throw an error.

#### Parameters

##### path

`string`

Path to the file to delete

#### Returns

`Promise`\<`void`\>

Promise that resolves when delete is complete

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`delete`](../interfaces/StorageBackend.md#delete)

***

### exists()

> **exists**(`path`): `Promise`\<`boolean`\>

Defined in: [src/storage/index.ts:1856](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1856)

Check if a file exists.

#### Parameters

##### path

`string`

Path to check

#### Returns

`Promise`\<`boolean`\>

Promise resolving to true if file exists, false otherwise

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`exists`](../interfaces/StorageBackend.md#exists)

***

### stat()

> **stat**(`path`): `Promise`\<[`FileStat`](../interfaces/FileStat.md) \| `null`\>

Defined in: [src/storage/index.ts:1876](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1876)

Get file metadata (size, last modified time, optional etag).

#### Parameters

##### path

`string`

Path to the file

#### Returns

`Promise`\<[`FileStat`](../interfaces/FileStat.md) \| `null`\>

Promise resolving to FileStat or null if file doesn't exist

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`stat`](../interfaces/StorageBackend.md#stat)

***

### readRange()

> **readRange**(`path`, `start`, `end`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: [src/storage/index.ts:1904](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1904)

Read a byte range from a file. Essential for efficient Parquet file reading
where metadata is stored at the end of the file.

#### Parameters

##### path

`string`

Path to the file

##### start

`number`

Starting byte offset (inclusive)

##### end

`number`

Ending byte offset (exclusive)

#### Returns

`Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Promise resolving to the requested byte range

#### Throws

If file does not exist

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`readRange`](../interfaces/StorageBackend.md#readrange)

***

### getVersion()

> **getVersion**(`path`): `Promise`\<`string` \| `null`\>

Defined in: [src/storage/index.ts:1934](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1934)

Get the current version of a file.

The version string format varies by implementation:
- MemoryStorage: Auto-generated unique strings
- FileSystemStorage: File modification time (mtime in ms)
- R2Storage: ETag
- S3Storage: ETag

#### Parameters

##### path

`string`

Path to the file

#### Returns

`Promise`\<`string` \| `null`\>

Promise resolving to version string, or null if file doesn't exist

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`getVersion`](../interfaces/StorageBackend.md#getversion)

***

### writeConditional()

> **writeConditional**(`path`, `data`, `expectedVersion`): `Promise`\<`string`\>

Defined in: [src/storage/index.ts:1939](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1939)

Conditionally write a file only if the version matches.
This enables optimistic concurrency control for Delta Lake transactions.

Use cases:
- `expectedVersion = null`: Create file only if it doesn't exist
- `expectedVersion = "version"`: Update file only if version matches

## Concurrency Note

The internal write locks are **process-local only**. For distributed
deployments, concurrent writes from different processes/instances may
result in VersionMismatchError when the version check fails. This is
the expected behavior for optimistic concurrency control - callers
should retry with the new version on conflict.

#### Parameters

##### path

`string`

Path to the file

##### data

`Uint8Array`

Data to write

##### expectedVersion

Expected version/etag, or null for create-if-not-exists

`string` | `null`

#### Returns

`Promise`\<`string`\>

The new version string after successful write

#### Throws

If the current version doesn't match expected

#### Implementation of

[`StorageBackend`](../interfaces/StorageBackend.md).[`writeConditional`](../interfaces/StorageBackend.md#writeconditional)
