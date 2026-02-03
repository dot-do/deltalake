[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / RowGroupStats

# Interface: RowGroupStats

Defined in: [src/parquet/streaming-writer.ts:102](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L102)

## Properties

### numRows

> **numRows**: `number`

Defined in: [src/parquet/streaming-writer.ts:103](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L103)

***

### fileOffset

> **fileOffset**: `number`

Defined in: [src/parquet/streaming-writer.ts:104](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L104)

***

### compressedSize

> **compressedSize**: `number`

Defined in: [src/parquet/streaming-writer.ts:105](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L105)

***

### totalUncompressedSize

> **totalUncompressedSize**: `number`

Defined in: [src/parquet/streaming-writer.ts:106](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L106)

***

### columnStats

> **columnStats**: `Map`\<`string`, [`ColumnStats`](ColumnStats.md)\>

Defined in: [src/parquet/streaming-writer.ts:107](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L107)
