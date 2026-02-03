[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / StreamingParquetWriterOptions

# Interface: StreamingParquetWriterOptions

Defined in: [src/parquet/streaming-writer.ts:53](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L53)

## Properties

### rowGroupSize?

> `readonly` `optional` **rowGroupSize**: `number`

Defined in: [src/parquet/streaming-writer.ts:55](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L55)

Number of rows per row group (default: 10000)

***

### targetRowGroupBytes?

> `readonly` `optional` **targetRowGroupBytes**: `number`

Defined in: [src/parquet/streaming-writer.ts:57](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L57)

Target row group size in bytes

***

### maxBufferBytes?

> `readonly` `optional` **maxBufferBytes**: `number`

Defined in: [src/parquet/streaming-writer.ts:59](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L59)

Maximum buffer size in bytes before forcing flush

***

### maxPendingFlushes?

> `readonly` `optional` **maxPendingFlushes**: `number`

Defined in: [src/parquet/streaming-writer.ts:61](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L61)

Maximum number of pending flush operations

***

### enableBufferPooling?

> `readonly` `optional` **enableBufferPooling**: `boolean`

Defined in: [src/parquet/streaming-writer.ts:63](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L63)

Enable buffer pooling for memory efficiency

***

### schema?

> `readonly` `optional` **schema**: [`ParquetSchema`](ParquetSchema.md)

Defined in: [src/parquet/streaming-writer.ts:65](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L65)

Explicit schema (if not provided, inferred from first row)

***

### statistics?

> `readonly` `optional` **statistics**: `boolean`

Defined in: [src/parquet/streaming-writer.ts:67](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L67)

Enable column statistics

***

### distinctCountEnabled?

> `readonly` `optional` **distinctCountEnabled**: `boolean`

Defined in: [src/parquet/streaming-writer.ts:69](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L69)

Enable distinct count estimation

***

### shredFields?

> `readonly` `optional` **shredFields**: readonly `string`[]

Defined in: [src/parquet/streaming-writer.ts:71](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L71)

Fields to shred from variant columns for statistics

***

### compression?

> `readonly` `optional` **compression**: `CompressionCodec`

Defined in: [src/parquet/streaming-writer.ts:73](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L73)

Compression codec

***

### kvMetadata?

> `readonly` `optional` **kvMetadata**: readonly `KeyValueMetadata`[]

Defined in: [src/parquet/streaming-writer.ts:75](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/streaming-writer.ts#L75)

Key-value metadata to include in file
