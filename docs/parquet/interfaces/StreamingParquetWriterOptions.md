[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / StreamingParquetWriterOptions

# Interface: StreamingParquetWriterOptions

Defined in: src/parquet/streaming-writer.ts:40

## Properties

### rowGroupSize?

> `optional` **rowGroupSize**: `number`

Defined in: src/parquet/streaming-writer.ts:42

Number of rows per row group (default: 10000)

***

### targetRowGroupBytes?

> `optional` **targetRowGroupBytes**: `number`

Defined in: src/parquet/streaming-writer.ts:44

Target row group size in bytes

***

### maxBufferBytes?

> `optional` **maxBufferBytes**: `number`

Defined in: src/parquet/streaming-writer.ts:46

Maximum buffer size in bytes before forcing flush

***

### maxPendingFlushes?

> `optional` **maxPendingFlushes**: `number`

Defined in: src/parquet/streaming-writer.ts:48

Maximum number of pending flush operations

***

### enableBufferPooling?

> `optional` **enableBufferPooling**: `boolean`

Defined in: src/parquet/streaming-writer.ts:50

Enable buffer pooling for memory efficiency

***

### schema?

> `optional` **schema**: [`ParquetSchema`](ParquetSchema.md)

Defined in: src/parquet/streaming-writer.ts:52

Explicit schema (if not provided, inferred from first row)

***

### statistics?

> `optional` **statistics**: `boolean`

Defined in: src/parquet/streaming-writer.ts:54

Enable column statistics

***

### distinctCountEnabled?

> `optional` **distinctCountEnabled**: `boolean`

Defined in: src/parquet/streaming-writer.ts:56

Enable distinct count estimation

***

### shredFields?

> `optional` **shredFields**: `string`[]

Defined in: src/parquet/streaming-writer.ts:58

Fields to shred from variant columns for statistics

***

### compression?

> `optional` **compression**: `CompressionCodec`

Defined in: src/parquet/streaming-writer.ts:60

Compression codec

***

### kvMetadata?

> `optional` **kvMetadata**: `KeyValueMetadata`[]

Defined in: src/parquet/streaming-writer.ts:62

Key-value metadata to include in file
