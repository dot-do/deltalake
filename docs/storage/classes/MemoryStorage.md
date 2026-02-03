[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / MemoryStorage

# Class: MemoryStorage

Defined in: src/storage/index.ts:1831

In-memory storage backend for testing.

Stores all data in memory using Maps. Useful for unit testing and
development without external dependencies.

## Version Tracking
Generates unique version strings combining a counter, timestamp,
and random component for conditional writes.

## Instance Isolation
Each MemoryStorage instance has its own isolated storage.
Multiple instances do not share data.

## Testing Utilities
Includes optional features for testing:
- `snapshot()` / `restore()` - Save and restore state
- `clear()` - Remove all files
- `getOperationHistory()` - Track operations
- Latency simulation - Simulate slow storage
- Size limits - Simulate storage quotas

## Example

```typescript
const storage = new MemoryStorage()
await storage.write('test.txt', new TextEncoder().encode('hello'))
const data = await storage.read('test.txt')
```

## Implements

- [`StorageBackend`](../interfaces/StorageBackend.md)

## Constructors

### Constructor

> **new MemoryStorage**(`options`): `MemoryStorage`

Defined in: src/storage/index.ts:1848

#### Parameters

##### options

[`MemoryStorageOptions`](../interfaces/MemoryStorageOptions.md) = `{}`

#### Returns

`MemoryStorage`

## Methods

### read()

> **read**(`path`): `Promise`\<`Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: src/storage/index.ts:1856

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

Defined in: src/storage/index.ts:1869

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

Defined in: src/storage/index.ts:1881

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

Defined in: src/storage/index.ts:1894

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

Defined in: src/storage/index.ts:1904

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

Defined in: src/storage/index.ts:1909

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

Defined in: src/storage/index.ts:1923

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

Defined in: src/storage/index.ts:1936

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

Defined in: src/storage/index.ts:1940

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

***

### snapshot()

> **snapshot**(): [`StorageSnapshot`](../interfaces/StorageSnapshot.md)

Defined in: src/storage/index.ts:1960

Create a snapshot of the current storage state.
Useful for saving state before a test and restoring after.

#### Returns

[`StorageSnapshot`](../interfaces/StorageSnapshot.md)

***

### restore()

> **restore**(`snapshot`): `void`

Defined in: src/storage/index.ts:1976

Restore storage to a previously captured snapshot.

#### Parameters

##### snapshot

[`StorageSnapshot`](../interfaces/StorageSnapshot.md)

#### Returns

`void`

***

### clear()

> **clear**(): `void`

Defined in: src/storage/index.ts:1994

Clear all files from storage.

#### Returns

`void`

***

### getOperationHistory()

> **getOperationHistory**(): readonly [`OperationRecord`](../interfaces/OperationRecord.md)[]

Defined in: src/storage/index.ts:2009

Get the history of operations performed on this storage.
Returns a copy to prevent external mutation.

#### Returns

readonly [`OperationRecord`](../interfaces/OperationRecord.md)[]

***

### clearOperationHistory()

> **clearOperationHistory**(): `void`

Defined in: src/storage/index.ts:2016

Clear the operation history.

#### Returns

`void`

***

### getUsedSize()

> **getUsedSize**(): `number`

Defined in: src/storage/index.ts:2027

Get the current used storage size in bytes.

#### Returns

`number`

***

### getMaxSize()

> **getMaxSize**(): `number` \| `undefined`

Defined in: src/storage/index.ts:2034

Get the maximum storage size (if configured).

#### Returns

`number` \| `undefined`

***

### getAvailableSize()

> **getAvailableSize**(): `number`

Defined in: src/storage/index.ts:2042

Get the available storage size (maxSize - usedSize).
Returns Infinity if no maxSize is configured.

#### Returns

`number`

***

### getFileCount()

> **getFileCount**(): `number`

Defined in: src/storage/index.ts:2052

Get the total number of files stored.

#### Returns

`number`

***

### setFileTimestamp()

> **setFileTimestamp**(`path`, `timestamp`): `void`

Defined in: src/storage/index.ts:2068

Set the timestamp for a file (testing utility).
Useful for testing time-based operations like vacuum retention.

#### Parameters

##### path

`string`

Path to the file

##### timestamp

`Date`

The timestamp to set

#### Returns

`void`

#### Throws

Error if the file does not exist

***

### getFileTimestamp()

> **getFileTimestamp**(`path`): `Date` \| `undefined`

Defined in: src/storage/index.ts:2081

Get the timestamp for a file (testing utility).

#### Parameters

##### path

`string`

Path to the file

#### Returns

`Date` \| `undefined`

The file's timestamp, or undefined if not found
