[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / filterToParquetPredicate

# Function: filterToParquetPredicate()

> **filterToParquetPredicate**\<`T`\>(`filter`): [`ZoneMapFilter`](../../parquet/interfaces/ZoneMapFilter.md)[]

Defined in: src/query/index.ts:307

Convert a MongoDB-style filter to Parquet zone map filters.
Only pushable predicates are converted; others are skipped.

## Type Parameters

### T

`T`

## Parameters

### filter

[`Filter`](../type-aliases/Filter.md)\<`T`\>

## Returns

[`ZoneMapFilter`](../../parquet/interfaces/ZoneMapFilter.md)[]
