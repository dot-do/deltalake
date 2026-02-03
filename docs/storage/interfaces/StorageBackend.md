[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / StorageBackend

# Interface: StorageBackend

Defined in: src/storage/index.ts:216

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

## Methods

### read()

> **read**(`path`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: src/storage/index.ts:224

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

Defined in: src/storage/index.ts:233

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

Defined in: src/storage/index.ts:241

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

Defined in: src/storage/index.ts:250

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

Defined in: src/storage/index.ts:258

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

Defined in: src/storage/index.ts:266

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

Defined in: src/storage/index.ts:278

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

Defined in: src/storage/index.ts:294

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

***

### getVersion()

> **getVersion**(`path`): `Promise`\<`string` \| `null`\>

Defined in: src/storage/index.ts:308

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
