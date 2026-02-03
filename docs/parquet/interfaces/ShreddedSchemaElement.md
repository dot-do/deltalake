[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ShreddedSchemaElement

# Interface: ShreddedSchemaElement

Defined in: [src/parquet/index.ts:259](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L259)

Schema element from hyparquet for shredded columns

## Properties

### name

> **name**: `string`

Defined in: [src/parquet/index.ts:260](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L260)

***

### type?

> `optional` **type**: `string`

Defined in: [src/parquet/index.ts:261](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L261)

***

### repetition\_type?

> `optional` **repetition\_type**: `"OPTIONAL"` \| `"REQUIRED"` \| `"REPEATED"`

Defined in: [src/parquet/index.ts:262](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L262)

***

### num\_children?

> `optional` **num\_children**: `number`

Defined in: [src/parquet/index.ts:263](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L263)

***

### converted\_type?

> `optional` **converted\_type**: `string`

Defined in: [src/parquet/index.ts:264](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L264)

***

### logical\_type?

> `optional` **logical\_type**: `object`

Defined in: [src/parquet/index.ts:265](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L265)

#### type

> **type**: `string`
