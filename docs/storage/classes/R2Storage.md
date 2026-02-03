[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / R2Storage

# Class: R2Storage

Defined in: src/storage/index.ts:1018

Cloudflare R2 storage backend.

Uses the Cloudflare R2 API for all file operations. Always delegates
to the R2 bucket - no in-memory fallback.

## Version Tracking
Uses R2 object ETags as version identifiers for conditional writes.

## Features
- Efficient byte-range reads using R2's range parameter
- Automatic pagination for list operations
- ETag-based conditional writes for optimistic concurrency

## Example

```typescript
// In a Cloudflare Worker
const storage = new R2Storage({ bucket: env.MY_BUCKET })
```

## Implements

- [`StorageBackend`](../interfaces/StorageBackend.md)

## Constructors

### Constructor

> **new R2Storage**(`options`): `R2Storage`

Defined in: src/storage/index.ts:1021

#### Parameters

##### options

###### bucket

`R2Bucket`

#### Returns

`R2Storage`

## Methods

### read()

> **read**(`path`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: src/storage/index.ts:1023

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

Defined in: src/storage/index.ts:1032

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

Defined in: src/storage/index.ts:1036

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

Defined in: src/storage/index.ts:1057

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

Defined in: src/storage/index.ts:1061

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

Defined in: src/storage/index.ts:1066

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

Defined in: src/storage/index.ts:1078

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

Defined in: src/storage/index.ts:1090

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

Defined in: src/storage/index.ts:1098

Conditionally write a file only if the version matches.
This enables optimistic concurrency control for Delta Lake transactions.

Use cases:
- `expectedVersion = null`: Create file only if it doesn't exist
- `expectedVersion = "version"`: Update file only if version matches

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
