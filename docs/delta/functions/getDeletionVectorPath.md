[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / getDeletionVectorPath

# Function: getDeletionVectorPath()

> **getDeletionVectorPath**(`tablePath`, `dv`): `string`

Defined in: [src/delta/index.ts:225](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/index.ts#L225)

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
