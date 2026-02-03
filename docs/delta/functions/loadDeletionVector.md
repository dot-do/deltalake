[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / loadDeletionVector

# Function: loadDeletionVector()

> **loadDeletionVector**(`storage`, `tablePath`, `dv`): `Promise`\<`Set`\<`number`\>\>

Defined in: src/delta/index.ts:409

Load and parse a deletion vector from storage.

## Parameters

### storage

[`StorageBackend`](../../storage/interfaces/StorageBackend.md)

Storage backend

### tablePath

`string`

Base path of the Delta table

### dv

[`DeletionVectorDescriptor`](../interfaces/DeletionVectorDescriptor.md)

Deletion vector descriptor

## Returns

`Promise`\<`Set`\<`number`\>\>

Set of row indices that are marked as deleted
