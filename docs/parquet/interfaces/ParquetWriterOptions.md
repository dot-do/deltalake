[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ParquetWriterOptions

# Interface: ParquetWriterOptions

Defined in: src/parquet/index.ts:50

## Properties

### rowGroupSize?

> `optional` **rowGroupSize**: `number`

Defined in: src/parquet/index.ts:52

Row group size (default: 10000)

***

### targetFileSize?

> `optional` **targetFileSize**: `number`

Defined in: src/parquet/index.ts:55

Target file size in bytes (default: 128MB)

***

### compression?

> `optional` **compression**: `"UNCOMPRESSED"` \| `"SNAPPY"` \| `"LZ4"` \| `"LZ4_RAW"` \| `"GZIP"` \| `"ZSTD"`

Defined in: src/parquet/index.ts:58

Compression codec

***

### schema?

> `optional` **schema**: [`ParquetSchema`](ParquetSchema.md)

Defined in: src/parquet/index.ts:61

Schema (optional - inferred from first row if not provided)

***

### shredFields?

> `optional` **shredFields**: `string`[]

Defined in: src/parquet/index.ts:64

Fields to shred into typed columns for statistics
