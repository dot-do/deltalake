[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / WriteResult

# Interface: WriteResult

Defined in: [src/parquet/streaming-writer.ts:120](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L120)

## Properties

### buffer

> **buffer**: `ArrayBuffer`

Defined in: [src/parquet/streaming-writer.ts:121](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L121)

***

### totalRows

> **totalRows**: `number`

Defined in: [src/parquet/streaming-writer.ts:122](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L122)

***

### rowGroups

> **rowGroups**: [`RowGroupStats`](RowGroupStats.md)[]

Defined in: [src/parquet/streaming-writer.ts:123](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L123)

***

### zoneMaps

> **zoneMaps**: [`ZoneMap`](ZoneMap.md)[][]

Defined in: [src/parquet/streaming-writer.ts:124](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L124)

***

### shreddedColumns

> **shreddedColumns**: `string`[]

Defined in: [src/parquet/streaming-writer.ts:125](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L125)

***

### metadata

> **metadata**: `WriteResultMetadata`

Defined in: [src/parquet/streaming-writer.ts:126](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L126)

## Methods

### createAsyncBuffer()

> **createAsyncBuffer**(): `Promise`\<[`AsyncBuffer`](../../storage/interfaces/AsyncBuffer.md)\>

Defined in: [src/parquet/streaming-writer.ts:127](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L127)

#### Returns

`Promise`\<[`AsyncBuffer`](../../storage/interfaces/AsyncBuffer.md)\>

***

### readRows()

> **readRows**(`asyncBuffer`): `Promise`\<`Record`\<`string`, `unknown`\>[]\>

Defined in: [src/parquet/streaming-writer.ts:128](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L128)

#### Parameters

##### asyncBuffer

[`AsyncBuffer`](../../storage/interfaces/AsyncBuffer.md)

#### Returns

`Promise`\<`Record`\<`string`, `unknown`\>[]\>
