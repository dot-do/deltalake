[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / DeltaTable

# Class: DeltaTable\<T\>

Defined in: src/delta/index.ts:1114

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\> = `Record`\<`string`, `unknown`\>

## Constructors

### Constructor

> **new DeltaTable**\<`T`\>(`storage`, `tablePath`, `config?`): `DeltaTable`\<`T`\>

Defined in: src/delta/index.ts:1139

Create a new DeltaTable instance.

#### Parameters

##### storage

[`StorageBackend`](../../storage/interfaces/StorageBackend.md)

Storage backend for reading/writing data

##### tablePath

`string`

Base path for the Delta table

##### config?

[`CheckpointConfig`](../interfaces/CheckpointConfig.md)

Optional checkpoint configuration

#### Returns

`DeltaTable`\<`T`\>

#### Example

```typescript
const storage = new MemoryStorage()
const table = new DeltaTable(storage, 'my-table')
```

## Properties

### lastQueryProjectionColumns?

> `optional` **lastQueryProjectionColumns**: `string`[]

Defined in: src/delta/index.ts:2321

Columns identified for projection during the last query
Used for testing/debugging Parquet column pruning optimization

***

### lastQuerySkippedFiles?

> `optional` **lastQuerySkippedFiles**: `number`

Defined in: src/delta/index.ts:2327

Number of files skipped by partition pruning in the last query.
Used for testing/debugging partition pruning optimization.

## Methods

### version()

> **version**(): `Promise`\<`number`\>

Defined in: src/delta/index.ts:1216

Get the current table version.

Uses cached value if available. Call `refreshVersion()` to force a fresh read.

#### Returns

`Promise`\<`number`\>

The current version number, or -1 if the table doesn't exist

#### Example

```typescript
const version = await table.version()
console.log(`Table is at version ${version}`)
```

***

### refreshVersion()

> **refreshVersion**(): `Promise`\<`number`\>

Defined in: src/delta/index.ts:1255

Force re-read version from storage and clear internal caches.

Use this after catching a ConcurrencyError to refresh state before retrying.
This ensures you have the latest version information from storage.

#### Returns

`Promise`\<`number`\>

The current version number from storage

#### Example

```typescript
try {
  await table.write(rows)
} catch (error) {
  if (error instanceof ConcurrencyError) {
    // Refresh and retry
    await table.refreshVersion()
    await table.write(rows)
  }
}
```

***

### snapshot()

> **snapshot**(`version?`): `Promise`\<[`DeltaSnapshot`](../interfaces/DeltaSnapshot.md)\>

Defined in: src/delta/index.ts:1306

Get a snapshot of the table at a specific version.

A snapshot represents the complete state of the table at a point in time,
including all active files, metadata, and protocol information.
This enables time travel queries and consistent reads.

#### Parameters

##### version?

`number`

Optional version to get snapshot at (defaults to current version)

#### Returns

`Promise`\<[`DeltaSnapshot`](../interfaces/DeltaSnapshot.md)\>

The table snapshot including files, metadata, and protocol

#### Example

```typescript
// Get current snapshot
const current = await table.snapshot()

// Get snapshot at version 5 (time travel)
const historical = await table.snapshot(5)

// Use snapshot for consistent reads
const snapshot = await table.snapshot()
const results1 = await table.query({ type: 'A' }, { snapshot })
const results2 = await table.query({ type: 'B' }, { snapshot })
```

***

### write()

> **write**(`rows`, `options?`): `Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

Defined in: src/delta/index.ts:1432

Write data to the table

Optimized implementation that processes rows in a single pass:
- Schema inference, validation, column conversion, and stats computed together
- Reduced memory allocations
- Single timestamp for consistency

#### Parameters

##### rows

`T`[]

Array of rows to write

##### options?

[`WriteOptions`](../interfaces/WriteOptions.md)

Optional write options including partition columns

#### Returns

`Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

The commit information

#### Example

```typescript
// Write without partitioning
await table.write(rows)

// Write with partitioning
await table.write(rows, { partitionColumns: ['year', 'month'] })
```

***

### query()

> **query**(`filter?`, `options?`): `Promise`\<`Partial`\<`T`\>[]\>

Defined in: src/delta/index.ts:2195

Query the table with a filter

#### Parameters

##### filter?

[`Filter`](../../query/type-aliases/Filter.md)\<`T`\>

Optional MongoDB-style filter to apply

##### options?

[`QueryOptions`](../interfaces/QueryOptions.md)\<`T`\>

Optional query options for snapshot isolation and projection

#### Returns

`Promise`\<`Partial`\<`T`\>[]\>

Array of matching rows (may be partial if projection is specified)

#### Example

```ts
// Query all rows
const allRows = await table.query()

// Query with equality filter
const activeUsers = await table.query({ active: true })

// Query with comparison operators
const adults = await table.query({ age: { $gte: 18 } })

// Query with logical operators
const results = await table.query({
  $or: [{ status: 'active' }, { role: 'admin' }]
})

// Query at a specific version (time travel)
const oldData = await table.query({ active: true }, { version: 5 })

// Query with a pre-fetched snapshot for consistent reads
const snapshot = await table.snapshot(5)
const result1 = await table.query({ type: 'A' }, { snapshot })
const result2 = await table.query({ type: 'B' }, { snapshot })

// Query with projection (return only specified fields)
const names = await table.query({}, { projection: ['name', 'age'] })
const withMetadata = await table.query({}, { projection: { name: 1, 'metadata.tier': 1 } })
```

***

### delete()

> **delete**(`filter`): `Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

Defined in: src/delta/index.ts:2626

Delete rows matching a filter.

Removes all rows that match the provided filter from the table.
This creates a new commit with remove actions for affected files
and add actions for files containing remaining rows.

#### Parameters

##### filter

[`Filter`](../../query/type-aliases/Filter.md)\<`T`\>

MongoDB-style filter to identify rows to delete

#### Returns

`Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

The commit information

#### Throws

Error if no rows match the filter

#### Throws

ConcurrencyError if another writer modified the table

#### Example

```typescript
// Delete all inactive users
await table.delete({ active: false })

// Delete a specific user
await table.delete({ _id: 'user-123' })
```

***

### update()

> **update**(`filter`, `updates`): `Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

Defined in: src/delta/index.ts:2727

Update rows matching a filter.

Applies the provided updates to all rows that match the filter.
This creates a new commit that removes affected files and adds
new files containing the updated data.

#### Parameters

##### filter

[`Filter`](../../query/type-aliases/Filter.md)\<`T`\>

MongoDB-style filter to identify rows to update

##### updates

`Partial`\<`T`\>

Partial object with fields to update

#### Returns

`Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

The commit information

#### Throws

Error if no rows match the filter

#### Throws

ConcurrencyError if another writer modified the table

#### Example

```typescript
// Set all users as inactive
await table.update({}, { active: false })

// Update a specific user's score
await table.update({ _id: 'user-123' }, { score: 100 })
```

***

### updateMetadata()

> **updateMetadata**(`metadata`): `Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

Defined in: src/delta/index.ts:2829

Update table metadata.

Updates the table's metadata action with the provided values.
This is useful for changing table name, description, or configuration.

#### Parameters

##### metadata

`Partial`\<[`MetadataAction`](../interfaces/MetadataAction.md)\[`"metaData"`\]\>

Partial metadata object with fields to update

#### Returns

`Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

The commit information

#### Throws

ConcurrencyError if another writer modified the table

#### Example

```typescript
await table.updateMetadata({
  name: 'users',
  description: 'Table containing user records',
  configuration: { 'delta.appendOnly': 'true' }
})
```

***

### commit()

> **commit**(`actions`): `Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

Defined in: src/delta/index.ts:2875

Create a commit with custom actions.

This is a low-level API for advanced operations like compaction,
custom file management, or direct action manipulation.
Checks for version conflicts before committing.

#### Parameters

##### actions

[`DeltaAction`](../type-aliases/DeltaAction.md)[]

Array of Delta actions to commit

#### Returns

`Promise`\<[`DeltaCommit`](../interfaces/DeltaCommit.md)\>

The commit information

#### Throws

ConcurrencyError if another writer modified the table

#### Example

```typescript
// Custom compaction commit
await table.commit([
  { remove: { path: 'old-file-1.parquet', deletionTimestamp: Date.now(), dataChange: false } },
  { remove: { path: 'old-file-2.parquet', deletionTimestamp: Date.now(), dataChange: false } },
  { add: { path: 'compacted-file.parquet', size: 1024, modificationTime: Date.now(), dataChange: false } },
])
```

***

### getStorage()

> **getStorage**(): [`StorageBackend`](../../storage/interfaces/StorageBackend.md)

Defined in: src/delta/index.ts:2966

Get the storage backend for direct file operations.

This method is primarily intended for compaction and other maintenance
operations that need direct access to the underlying storage.

#### Returns

[`StorageBackend`](../../storage/interfaces/StorageBackend.md)

The storage backend instance

#### Example

```typescript
const storage = table.getStorage()
const data = await storage.read('path/to/file.parquet')
```

***

### getTablePath()

> **getTablePath**(): `string`

Defined in: src/delta/index.ts:2984

Get the base path of the table.

This method is primarily intended for compaction and other maintenance
operations that need to construct file paths.

#### Returns

`string`

The table path

#### Example

```typescript
const path = table.getTablePath()
const dataPath = `${path}/part-00001.parquet`
```

***

### readFileRows()

> **readFileRows**(`path`): `T`[] \| `undefined`

Defined in: src/delta/index.ts:3005

Read rows stored for a specific file path.

This method provides access to in-memory cached data for a file,
primarily used by compaction and other maintenance operations.

#### Parameters

##### path

`string`

The file path to read rows for

#### Returns

`T`[] \| `undefined`

Array of rows for the file, or undefined if not found

#### Example

```typescript
const rows = table.readFileRows('data/part-00000.parquet')
if (rows) {
  console.log(`File has ${rows.length} rows`)
}
```

***

### writeFileRows()

> **writeFileRows**(`path`, `rows`): `void`

Defined in: src/delta/index.ts:3026

Store rows for a specific file path.

This method caches row data in memory for a file path,
primarily used by compaction and write operations.

#### Parameters

##### path

`string`

The file path to store rows for

##### rows

`T`[]

Array of rows to store

#### Returns

`void`

#### Example

```typescript
table.writeFileRows('data/part-00000.parquet', [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' }
])
```

***

### deleteFileRows()

> **deleteFileRows**(`path`): `boolean`

Defined in: src/delta/index.ts:3047

Delete cached rows for a specific file path.

This method removes cached row data for a file path,
primarily used by compaction when removing old files.

#### Parameters

##### path

`string`

The file path to delete rows for

#### Returns

`boolean`

true if the file was found and deleted, false otherwise

#### Example

```typescript
const deleted = table.deleteFileRows('data/part-00000.parquet')
if (deleted) {
  console.log('File data removed from cache')
}
```
