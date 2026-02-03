[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / StreamingParquetWriter

# Class: StreamingParquetWriter

Defined in: src/parquet/streaming-writer.ts:165

## Constructors

### Constructor

> **new StreamingParquetWriter**(`options?`): `StreamingParquetWriter`

Defined in: src/parquet/streaming-writer.ts:185

#### Parameters

##### options?

[`StreamingParquetWriterOptions`](../interfaces/StreamingParquetWriterOptions.md)

#### Returns

`StreamingParquetWriter`

## Accessors

### options

#### Get Signature

> **get** **options**(): [`StreamingParquetWriterOptions`](../interfaces/StreamingParquetWriterOptions.md)

Defined in: src/parquet/streaming-writer.ts:201

##### Returns

[`StreamingParquetWriterOptions`](../interfaces/StreamingParquetWriterOptions.md)

***

### bufferedRowCount

#### Get Signature

> **get** **bufferedRowCount**(): `number`

Defined in: src/parquet/streaming-writer.ts:209

##### Returns

`number`

***

### completedRowGroupCount

#### Get Signature

> **get** **completedRowGroupCount**(): `number`

Defined in: src/parquet/streaming-writer.ts:213

##### Returns

`number`

***

### totalRowsWritten

#### Get Signature

> **get** **totalRowsWritten**(): `number`

Defined in: src/parquet/streaming-writer.ts:217

##### Returns

`number`

***

### bytesWritten

#### Get Signature

> **get** **bytesWritten**(): `number`

Defined in: src/parquet/streaming-writer.ts:221

##### Returns

`number`

***

### inferredSchema

#### Get Signature

> **get** **inferredSchema**(): [`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

Defined in: src/parquet/streaming-writer.ts:225

##### Returns

[`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

***

### schema

#### Get Signature

> **get** **schema**(): [`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

Defined in: src/parquet/streaming-writer.ts:229

##### Returns

[`ParquetSchema`](../interfaces/ParquetSchema.md) \| `null`

***

### isAborted

#### Get Signature

> **get** **isAborted**(): `boolean`

Defined in: src/parquet/streaming-writer.ts:233

##### Returns

`boolean`

***

### bufferPoolStats

#### Get Signature

> **get** **bufferPoolStats**(): [`BufferPoolStats`](../interfaces/BufferPoolStats.md)

Defined in: src/parquet/streaming-writer.ts:237

##### Returns

[`BufferPoolStats`](../interfaces/BufferPoolStats.md)

## Methods

### writeRow()

> **writeRow**(`row`): `Promise`\<`void`\>

Defined in: src/parquet/streaming-writer.ts:245

#### Parameters

##### row

`Row`

#### Returns

`Promise`\<`void`\>

***

### finish()

> **finish**(): `Promise`\<[`WriteResult`](../interfaces/WriteResult.md)\>

Defined in: src/parquet/streaming-writer.ts:269

#### Returns

`Promise`\<[`WriteResult`](../interfaces/WriteResult.md)\>

***

### abort()

> **abort**(): `Promise`\<`void`\>

Defined in: src/parquet/streaming-writer.ts:298

#### Returns

`Promise`\<`void`\>
