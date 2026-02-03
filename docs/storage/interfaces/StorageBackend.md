[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / StorageBackend

# Interface: StorageBackend

Defined in: [src/storage/index.ts:269](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L269)

Core storage backend interface.

All storage implementations (memory, filesystem, R2, S3) must implement
this interface to ensure consistent behavior across environments.

## Method Categories

### Basic Operations
- `read()` - Read entire file contents
- `write()` - Write/overwrite file contents
- `delete()` - Delete a file (idempotent)
- `exists()` - Check if file exists
- `stat()` - Get file metadata
- `list()` - List files with prefix

### Efficient Reads
- `readRange()` - Read byte range (for Parquet footer reading)

### Concurrency Control
- `writeConditional()` - Atomic write with version check
- `getVersion()` - Get current file version

## Concurrency Limitations

The write locks used by `writeConditional()` are **process-local only**.
They prevent concurrent writes within a single Node.js process or Worker
instance, but provide NO coordination across distributed deployments.

For multi-instance deployments (multiple Workers, multiple processes,
multiple servers), you should either:
- Use external distributed locking (Redis, DynamoDB, etc.)
- Rely on storage-level conditional writes (note: check-then-write is not
  atomic across the network)
- Design for single-writer access per table

The ETag/version-based conditional writes provide **optimistic concurrency
control** - they will detect and reject conflicting writes, but the
check-and-write is not atomic for cloud storage backends.

## Methods

### read()

> **read**(`path`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: [src/storage/index.ts:277](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L277)

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

***

### write()

> **write**(`path`, `data`): `Promise`\<`void`\>

Defined in: [src/storage/index.ts:286](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L286)

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

***

### list()

> **list**(`prefix`): `Promise`\<`string`[]\>

Defined in: [src/storage/index.ts:294](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L294)

List all files matching a prefix.

#### Parameters

##### prefix

`string`

Prefix to match (e.g., "data/" for all files in data directory)

#### Returns

`Promise`\<`string`[]\>

Promise resolving to array of file paths (not directories)

***

### delete()

> **delete**(`path`): `Promise`\<`void`\>

Defined in: [src/storage/index.ts:303](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L303)

Delete a file. This operation is idempotent - deleting a non-existent file
does not throw an error.

#### Parameters

##### path

`string`

Path to the file to delete

#### Returns

`Promise`\<`void`\>

Promise that resolves when delete is complete

***

### exists()

> **exists**(`path`): `Promise`\<`boolean`\>

Defined in: [src/storage/index.ts:311](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L311)

Check if a file exists.

#### Parameters

##### path

`string`

Path to check

#### Returns

`Promise`\<`boolean`\>

Promise resolving to true if file exists, false otherwise

***

### stat()

> **stat**(`path`): `Promise`\<[`FileStat`](FileStat.md) \| `null`\>

Defined in: [src/storage/index.ts:319](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L319)

Get file metadata (size, last modified time, optional etag).

#### Parameters

##### path

`string`

Path to the file

#### Returns

`Promise`\<[`FileStat`](FileStat.md) \| `null`\>

Promise resolving to FileStat or null if file doesn't exist

***

### readRange()

> **readRange**(`path`, `start`, `end`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: [src/storage/index.ts:331](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L331)

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

***

### writeConditional()

> **writeConditional**(`path`, `data`, `expectedVersion`): `Promise`\<`string`\>

Defined in: [src/storage/index.ts:355](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L355)

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

***

### getVersion()

> **getVersion**(`path`): `Promise`\<`string` \| `null`\>

Defined in: [src/storage/index.ts:369](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L369)

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
