[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / AddAction

# Interface: AddAction

Defined in: src/delta/index.ts:590

## Properties

### add

> **add**: `object`

Defined in: src/delta/index.ts:591

#### path

> **path**: `string`

#### size

> **size**: `number`

#### modificationTime

> **modificationTime**: `number`

#### dataChange

> **dataChange**: `boolean`

#### partitionValues?

> `optional` **partitionValues**: `Record`\<`string`, `string`\>

#### stats?

> `optional` **stats**: `string`

#### tags?

> `optional` **tags**: `Record`\<`string`, `string`\>

#### deletionVector?

> `optional` **deletionVector**: [`DeletionVectorDescriptor`](DeletionVectorDescriptor.md)

Deletion vector descriptor if this file has soft-deleted rows
