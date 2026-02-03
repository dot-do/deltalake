[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / createAddAction

# Function: createAddAction()

> **createAddAction**(`params`): [`AddAction`](../../index/interfaces/AddAction.md)

Defined in: [src/delta/index.ts:865](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/index.ts#L865)

Create an AddAction with validation

## Parameters

### params

#### path

`string`

#### size

`number`

#### modificationTime

`number`

#### dataChange

`boolean`

#### partitionValues?

`Record`\<`string`, `string`\>

#### stats?

[`FileStats`](../../index/interfaces/FileStats.md)

#### tags?

`Record`\<`string`, `string`\>

## Returns

[`AddAction`](../../index/interfaces/AddAction.md)
