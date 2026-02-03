[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ShreddedColumnResult

# Interface: ShreddedColumnResult

Defined in: src/parquet/index.ts:243

Result from createShreddedVariantColumn

## Properties

### schema

> **schema**: [`ShreddedSchemaElement`](ShreddedSchemaElement.md)[]

Defined in: src/parquet/index.ts:245

Schema elements for the shredded VARIANT column

***

### columnData

> **columnData**: `Map`\<`string`, `unknown`[]\>

Defined in: src/parquet/index.ts:247

Column data by path (e.g., 'col.typed_value.field.typed_value' -> values)

***

### shredPaths

> **shredPaths**: `string`[]

Defined in: src/parquet/index.ts:249

Paths to shredded typed_value columns for statistics
