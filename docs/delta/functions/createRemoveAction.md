[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / createRemoveAction

# Function: createRemoveAction()

> **createRemoveAction**(`params`): [`RemoveAction`](../interfaces/RemoveAction.md)

Defined in: src/delta/index.ts:3652

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

[`RemoveAction`](../interfaces/RemoveAction.md)
