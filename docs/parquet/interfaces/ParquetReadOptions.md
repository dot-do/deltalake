[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ParquetReadOptions

# Interface: ParquetReadOptions

Defined in: [src/parquet/index.ts:81](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L81)

## Properties

### columns?

> `readonly` `optional` **columns**: readonly `string`[]

Defined in: [src/parquet/index.ts:83](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L83)

Columns to read (default: all)

***

### rowGroups?

> `readonly` `optional` **rowGroups**: readonly `number`[]

Defined in: [src/parquet/index.ts:86](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L86)

Row group indices to read

***

### filter?

> `readonly` `optional` **filter**: [`ZoneMapFilter`](ZoneMapFilter.md)

Defined in: [src/parquet/index.ts:89](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L89)

Filter predicate for zone map pruning
