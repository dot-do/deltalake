[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / MetadataAction

# Interface: MetadataAction

Defined in: [src/delta/types.ts:86](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L86)

## Properties

### metaData

> **metaData**: `object`

Defined in: [src/delta/types.ts:87](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L87)

#### id

> **id**: `string`

#### name?

> `optional` **name**: `string`

#### description?

> `optional` **description**: `string`

#### format

> **format**: `object`

##### format.provider

> **provider**: `string`

##### format.options?

> `optional` **options**: `Record`\<`string`, `string`\>

#### schemaString

> **schemaString**: `string`

#### partitionColumns

> **partitionColumns**: `string`[]

#### configuration?

> `optional` **configuration**: `Record`\<`string`, `string`\>

#### createdTime?

> `optional` **createdTime**: `number`
