[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / ShreddedColumnOptions

# Interface: ShreddedColumnOptions

Defined in: [src/parquet/index.ts:283](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L283)

Options for createShreddedVariantColumn

## Properties

### nullable?

> `optional` **nullable**: `boolean`

Defined in: [src/parquet/index.ts:285](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L285)

Whether the VARIANT column can be null (default: true)

***

### fieldTypes?

> `optional` **fieldTypes**: `Record`\<`string`, `string`\>

Defined in: [src/parquet/index.ts:287](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L287)

Override field types (e.g., { fieldName: 'STRING' | 'INT32' | 'INT64' | 'FLOAT' | 'DOUBLE' | 'BOOLEAN' | 'TIMESTAMP' })
