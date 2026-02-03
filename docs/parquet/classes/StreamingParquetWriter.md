[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / StreamingParquetWriter

# Class: StreamingParquetWriter

Defined in: [src/parquet/streaming-writer.ts:178](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L178)

## Constructors

### Constructor

> **new StreamingParquetWriter**(`options?`): `StreamingParquetWriter`

Defined in: [src/parquet/streaming-writer.ts:198](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L198)

#### Parameters

##### options?

[`StreamingParquetWriterOptions`](../interfaces/StreamingParquetWriterOptions.md)

#### Returns

`StreamingParquetWriter`

## Accessors

### options

#### Get Signature

> **get** **options**(): [`StreamingParquetWriterOptions`](../interfaces/StreamingParquetWriterOptions.md)

Defined in: [src/parquet/streaming-writer.ts:214](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L214)

##### Returns

[`StreamingParquetWriterOptions`](../interfaces/StreamingParquetWriterOptions.md)

***

### bufferedRowCount

#### Get Signature

> **get** **bufferedRowCount**(): `number`

Defined in: [src/parquet/streaming-writer.ts:222](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L222)

##### Returns

`number`

***

### completedRowGroupCount

#### Get Signature

> **get** **completedRowGroupCount**(): `number`

Defined in: [src/parquet/streaming-writer.ts:226](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L226)

##### Returns

`number`

***

### totalRowsWritten

#### Get Signature

> **get** **totalRowsWritten**(): `number`

Defined in: [src/parquet/streaming-writer.ts:230](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L230)

##### Returns

`number`

***

### bytesWritten

#### Get Signature

> **get** **bytesWritten**(): `number`

Defined in: [src/parquet/streaming-writer.ts:234](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L234)

##### Returns

`number`

***

### inferredSchema

#### Get Signature

> **get** **inferredSchema**(): [`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

Defined in: [src/parquet/streaming-writer.ts:238](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L238)

##### Returns

[`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

***

### schema

#### Get Signature

> **get** **schema**(): [`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

Defined in: [src/parquet/streaming-writer.ts:242](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L242)

##### Returns

[`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

***

### isAborted

#### Get Signature

> **get** **isAborted**(): `boolean`

Defined in: [src/parquet/streaming-writer.ts:246](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L246)

##### Returns

`boolean`

***

### bufferPoolStats

#### Get Signature

> **get** **bufferPoolStats**(): [`BufferPoolStats`](../interfaces/BufferPoolStats.md)

Defined in: [src/parquet/streaming-writer.ts:250](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L250)

##### Returns

[`BufferPoolStats`](../interfaces/BufferPoolStats.md)

## Methods

### writeRow()

> **writeRow**(`row`): `Promise`\<`void`\>

Defined in: [src/parquet/streaming-writer.ts:258](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L258)

#### Parameters

##### row

`Row`

#### Returns

`Promise`\<`void`\>

***

### finish()

> **finish**(): `Promise`\<[`WriteResult`](../interfaces/WriteResult.md)\>

Defined in: [src/parquet/streaming-writer.ts:282](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L282)

#### Returns

`Promise`\<[`WriteResult`](../interfaces/WriteResult.md)\>

***

### abort()

> **abort**(): `Promise`\<`void`\>

Defined in: [src/parquet/streaming-writer.ts:311](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L311)

#### Returns

`Promise`\<`void`\>
