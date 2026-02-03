[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / createAddAction

# Function: createAddAction()

> **createAddAction**(`params`): [`AddAction`](../interfaces/AddAction.md)

Defined in: src/delta/index.ts:3577

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

[`FileStats`](../interfaces/FileStats.md)

#### tags?

`Record`\<`string`, `string`\>

## Returns

[`AddAction`](../interfaces/AddAction.md)
