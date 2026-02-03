[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / mapFilterPathToStats

# Function: mapFilterPathToStats()

> **mapFilterPathToStats**(`filterPath`, `columnName`, `shredFields`): `string` \| `null`

Defined in: [src/parquet/index.ts:310](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L310)

Map a user filter path to the statistics column path.
Returns null if the field is not shredded.

## Parameters

### filterPath

`string`

### columnName

`string`

### shredFields

`string`[]

## Returns

`string` \| `null`
