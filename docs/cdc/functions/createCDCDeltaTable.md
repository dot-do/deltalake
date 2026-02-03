[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / createCDCDeltaTable

# Function: createCDCDeltaTable()

> **createCDCDeltaTable**\<`T`\>(`storage`, `tablePath`): [`CDCDeltaTable`](../interfaces/CDCDeltaTable.md)\<`T`\>

Defined in: [src/cdc/index.ts:1928](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L1928)

Create a CDC-enabled Delta table.

Returns a table that tracks all changes (inserts, updates, deletes) as CDC records.
CDC records can be read by version range or time range, and subscribers can be
notified of changes in real-time.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### storage

[`StorageBackend`](../../storage/interfaces/StorageBackend.md)

Storage backend for reading/writing data

### tablePath

`string`

Base path for the Delta table

## Returns

[`CDCDeltaTable`](../interfaces/CDCDeltaTable.md)\<`T`\>

A CDC-enabled Delta table instance

## Throws

If storage or tablePath is invalid

## Example

```typescript
import { createStorage, createCDCDeltaTable } from '@dotdo/deltalake'

// Create storage and CDC table
const storage = createStorage({ type: 'memory' })
const users = createCDCDeltaTable<User>(storage, 'users')

// Enable CDC tracking
await users.setCDCEnabled(true)

// Write data - generates 'insert' CDC records
await users.write([
  { id: '1', name: 'Alice', score: 100 },
  { id: '2', name: 'Bob', score: 85 }
])

// Update data - generates 'update_preimage' and 'update_postimage' records
await users.update({ id: '1' }, { score: 150 })

// Delete data - generates 'delete' CDC records
await users.deleteRows({ score: { $lt: 90 } })

// Read CDC changes by version
const reader = users.getCDCReader()
const changes = await reader.readByVersion(0n, 2n)

// Subscribe to real-time changes
const unsubscribe = reader.subscribe(async (record) => {
  console.log(`Change: ${record._change_type} at version ${record._commit_version}`)
})

// Merge (upsert) with CDC tracking
await users.merge(
  [{ id: '3', name: 'Charlie', score: 95 }],
  (existing, incoming) => existing.id === incoming.id,
  (existing, incoming) => ({ ...existing, ...incoming }), // update matched
  (incoming) => incoming // insert unmatched
)
```
