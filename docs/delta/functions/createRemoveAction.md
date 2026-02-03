[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / createRemoveAction

# Function: createRemoveAction()

> **createRemoveAction**(`params`): [`RemoveAction`](../../index/interfaces/RemoveAction.md)

Defined in: [src/delta/index.ts:940](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/index.ts#L940)

Create a RemoveAction with validation

## Parameters

### params

#### path

`string`

#### deletionTimestamp

`number`

#### dataChange

`boolean`

#### partitionValues?

`Record`\<`string`, `string`\>

#### extendedFileMetadata?

`boolean`

#### size?

`number`

## Returns

[`RemoveAction`](../../index/interfaces/RemoveAction.md)
