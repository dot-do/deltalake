[**@dotdo/deltalake v0.0.1**](../README.md)

***

[@dotdo/deltalake](../modules.md) / parquet

# parquet

## Classes

- [StreamingParquetWriter](classes/StreamingParquetWriter.md)

## Interfaces

- [ParquetWriterOptions](interfaces/ParquetWriterOptions.md)
- [ParquetSchema](interfaces/ParquetSchema.md)
- [ParquetField](interfaces/ParquetField.md)
- [ParquetReadOptions](interfaces/ParquetReadOptions.md)
- [ParquetMetadata](interfaces/ParquetMetadata.md)
- [ZoneMap](interfaces/ZoneMap.md)
- [ZoneMapFilter](interfaces/ZoneMapFilter.md)
- [ShreddedSchemaElement](interfaces/ShreddedSchemaElement.md)
- [ShreddedColumnResult](interfaces/ShreddedColumnResult.md)
- [ShreddedColumnOptions](interfaces/ShreddedColumnOptions.md)
- [StreamingParquetWriterOptions](interfaces/StreamingParquetWriterOptions.md)
- [RowGroupStats](interfaces/RowGroupStats.md)
- [ColumnStats](interfaces/ColumnStats.md)
- [WriteResult](interfaces/WriteResult.md)
- [BufferPoolStats](interfaces/BufferPoolStats.md)

## Type Aliases

- [ColumnStatValue](type-aliases/ColumnStatValue.md)

## Functions

- [canSkipZoneMap](functions/canSkipZoneMap.md)
- [getStatisticsPaths](functions/getStatisticsPaths.md)
- [mapFilterPathToStats](functions/mapFilterPathToStats.md)
- [createShreddedVariantColumn](functions/createShreddedVariantColumn.md)

## References

### VariantValue

Re-exports [VariantValue](../index/type-aliases/VariantValue.md)

***

### EncodedVariant

Re-exports [EncodedVariant](../index/interfaces/EncodedVariant.md)

***

### encodeVariant

Re-exports [encodeVariant](../index/functions/encodeVariant.md)

***

### decodeVariant

Re-exports [decodeVariant](../index/functions/decodeVariant.md)

***

### AsyncBuffer

Re-exports [AsyncBuffer](../storage/interfaces/AsyncBuffer.md)

***

### createAsyncBuffer

Re-exports [createAsyncBuffer](../storage/functions/createAsyncBuffer.md)

***

### default

Renames and re-exports [ParquetMetadata](interfaces/ParquetMetadata.md)
