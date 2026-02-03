[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / mapFilterPathToStats

# Function: mapFilterPathToStats()

> **mapFilterPathToStats**(`filterPath`, `columnName`, `shredFields`): `string` \| `null`

Defined in: src/parquet/index.ts:282

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
