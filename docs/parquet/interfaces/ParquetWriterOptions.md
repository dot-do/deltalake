[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ParquetWriterOptions

# Interface: ParquetWriterOptions

Defined in: [src/parquet/index.ts:50](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L50)

## Properties

### rowGroupSize?

> `readonly` `optional` **rowGroupSize**: `number`

Defined in: [src/parquet/index.ts:52](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L52)

Row group size (default: 10000)

***

### targetFileSize?

> `readonly` `optional` **targetFileSize**: `number`

Defined in: [src/parquet/index.ts:55](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L55)

Target file size in bytes (default: 128MB)

***

### compression?

> `readonly` `optional` **compression**: `"UNCOMPRESSED"` \| `"SNAPPY"` \| `"LZ4"` \| `"LZ4_RAW"` \| `"GZIP"` \| `"ZSTD"`

Defined in: [src/parquet/index.ts:58](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L58)

Compression codec

***

### schema?

> `readonly` `optional` **schema**: [`ParquetSchema`](ParquetSchema.md)

Defined in: [src/parquet/index.ts:61](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L61)

Schema (optional - inferred from first row if not provided)

***

### shredFields?

> `readonly` `optional` **shredFields**: readonly `string`[]

Defined in: [src/parquet/index.ts:64](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L64)

Fields to shred into typed columns for statistics
