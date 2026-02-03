[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / getProjectionColumns

# Function: getProjectionColumns()

> **getProjectionColumns**\<`T`\>(`projection`): `string`[]

Defined in: src/query/index.ts:593

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
