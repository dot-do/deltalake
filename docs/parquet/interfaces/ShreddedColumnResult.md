[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ShreddedColumnResult

# Interface: ShreddedColumnResult

Defined in: [src/parquet/index.ts:271](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L271)

Result from createShreddedVariantColumn

## Properties

### schema

> **schema**: [`ShreddedSchemaElement`](ShreddedSchemaElement.md)[]

Defined in: [src/parquet/index.ts:273](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L273)

Schema elements for the shredded VARIANT column

***

### columnData

> **columnData**: `Map`\<`string`, `unknown`[]\>

Defined in: [src/parquet/index.ts:275](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L275)

Column data by path (e.g., 'col.typed_value.field.typed_value' -> values)

***

### shredPaths

> **shredPaths**: `string`[]

Defined in: [src/parquet/index.ts:277](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L277)

Paths to shredded typed_value columns for statistics
