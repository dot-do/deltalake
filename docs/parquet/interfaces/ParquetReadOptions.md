[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ParquetReadOptions

# Interface: ParquetReadOptions

Defined in: src/parquet/index.ts:81

## Properties

### columns?

> `optional` **columns**: `string`[]

Defined in: src/parquet/index.ts:83

Columns to read (default: all)

***

### rowGroups?

> `optional` **rowGroups**: `number`[]

Defined in: src/parquet/index.ts:86

Row group indices to read

***

### filter?

> `optional` **filter**: [`ZoneMapFilter`](ZoneMapFilter.md)

Defined in: src/parquet/index.ts:89

Filter predicate for zone map pruning
