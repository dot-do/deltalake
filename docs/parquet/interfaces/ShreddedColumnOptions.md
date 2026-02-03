[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ShreddedColumnOptions

# Interface: ShreddedColumnOptions

Defined in: src/parquet/index.ts:255

Options for createShreddedVariantColumn

## Properties

### nullable?

> `optional` **nullable**: `boolean`

Defined in: src/parquet/index.ts:257

Whether the VARIANT column can be null (default: true)

***

### fieldTypes?

> `optional` **fieldTypes**: `Record`\<`string`, `string`\>

Defined in: src/parquet/index.ts:259

Override field types (e.g., { fieldName: 'STRING' | 'INT32' | 'INT64' | 'FLOAT' | 'DOUBLE' | 'BOOLEAN' | 'TIMESTAMP' })
