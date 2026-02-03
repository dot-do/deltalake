[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / getDeletionVectorPath

# Function: getDeletionVectorPath()

> **getDeletionVectorPath**(`tablePath`, `dv`): `string`

Defined in: src/delta/index.ts:177

Get the file path for a deletion vector based on its descriptor.

## Parameters

### tablePath

`string`

Base path of the Delta table

### dv

[`DeletionVectorDescriptor`](../interfaces/DeletionVectorDescriptor.md)

Deletion vector descriptor

## Returns

`string`

Full path to the deletion vector file
