[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / FileSystemStorage

# Class: FileSystemStorage

Defined in: [src/storage/index.ts:780](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L780)

File system storage backend for local development.

Uses Node.js fs/promises for file operations. All paths are relative to
the configured base path and are protected against path traversal attacks.

## Version Tracking
Uses file modification time (mtime in milliseconds) as the version identifier
for conditional writes.

## Concurrency
Uses in-memory write locks to serialize concurrent writes to the same file.
Atomic writes are performed using temp file + rename pattern.

## Example

```typescript
const storage = new FileSystemStorage({ path: './data' })
await storage.write('file.txt', new TextEncoder().encode('hello'))
```

## Implements

- [`StorageBackend`](../interfaces/StorageBackend.md)

## Constructors

### Constructor

> **new FileSystemStorage**(`options`): `FileSystemStorage`

Defined in: [src/storage/index.ts:784](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L784)

#### Parameters

##### options

###### path

`string`

#### Returns

`FileSystemStorage`

## Methods

### read()

> **read**(`path`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: [src/storage/index.ts:859](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L859)

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

Defined in: [src/storage/index.ts:874](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L874)

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

Defined in: [src/storage/index.ts:887](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L887)

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

Defined in: [src/storage/index.ts:923](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L923)

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

Defined in: [src/storage/index.ts:937](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L937)

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

Defined in: [src/storage/index.ts:952](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L952)

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

Defined in: [src/storage/index.ts:978](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L978)

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

Defined in: [src/storage/index.ts:1021](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1021)

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

> **writeConditional**(`filePath`, `data`, `expectedVersion`): `Promise`\<`string`\>

Defined in: [src/storage/index.ts:1039](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1039)

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

##### filePath

`string`

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
