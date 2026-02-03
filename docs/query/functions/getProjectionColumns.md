[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / getProjectionColumns

# Function: getProjectionColumns()

> **getProjectionColumns**\<`T`\>(`projection`): `string`[]

Defined in: [src/query/index.ts:939](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/query/index.ts#L939)

Extract column names from a projection for Parquet column pruning.
Returns empty array for exclusion projections (all columns needed).

## Type Parameters

### T

`T`

## Parameters

### projection

[`Projection`](../type-aliases/Projection.md)\<`T`\>

The projection specification

## Returns

`string`[]

Array of column names needed for the query
