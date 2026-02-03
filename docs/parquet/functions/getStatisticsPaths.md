[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / getStatisticsPaths

# Function: getStatisticsPaths()

> **getStatisticsPaths**(`columnName`, `shredFields`): `string`[]

Defined in: src/parquet/index.ts:274

Get the column paths that have statistics available after shredding.
These can be used for predicate pushdown.

## Parameters

### columnName

`string`

### shredFields

`string`[]

## Returns

`string`[]
