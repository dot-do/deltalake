[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / WriteResult

# Interface: WriteResult

Defined in: src/parquet/streaming-writer.ts:107

## Properties

### buffer

> **buffer**: `ArrayBuffer`

Defined in: src/parquet/streaming-writer.ts:108

***

### totalRows

> **totalRows**: `number`

Defined in: src/parquet/streaming-writer.ts:109

***

### rowGroups

> **rowGroups**: [`RowGroupStats`](RowGroupStats.md)[]

Defined in: src/parquet/streaming-writer.ts:110

***

### zoneMaps

> **zoneMaps**: [`ZoneMap`](ZoneMap.md)[][]

Defined in: src/parquet/streaming-writer.ts:111

***

### shreddedColumns

> **shreddedColumns**: `string`[]

Defined in: src/parquet/streaming-writer.ts:112

***

### metadata

> **metadata**: `WriteResultMetadata`

Defined in: src/parquet/streaming-writer.ts:113

## Methods

### createAsyncBuffer()

> **createAsyncBuffer**(): `Promise`\<[`AsyncBuffer`](../../storage/interfaces/AsyncBuffer.md)\>

Defined in: src/parquet/streaming-writer.ts:114

#### Returns

`Promise`\<[`AsyncBuffer`](../../storage/interfaces/AsyncBuffer.md)\>

***

### readRows()

> **readRows**(`asyncBuffer`): `Promise`\<`Record`\<`string`, `unknown`\>[]\>

Defined in: src/parquet/streaming-writer.ts:115

#### Parameters

##### asyncBuffer

[`AsyncBuffer`](../../storage/interfaces/AsyncBuffer.md)

#### Returns

`Promise`\<`Record`\<`string`, `unknown`\>[]\>
