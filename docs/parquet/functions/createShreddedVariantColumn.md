[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / createShreddedVariantColumn

# Function: createShreddedVariantColumn()

> **createShreddedVariantColumn**(`name`, `values`, `shredFields`, `options?`): `object`

Defined in: node\_modules/@dotdo/hyparquet-writer/types/variant-shredding.d.ts:16

Create a shredded VARIANT column schema and data.

## Parameters

### name

`string`

Column name (e.g., '$index')

### values

`any`[]

Array of JavaScript objects

### shredFields

`string`[]

Fields to shred into typed columns

### options?

#### nullable?

`boolean`

Whether the column can contain nulls

#### fieldTypes?

`Record`\<`string`, `string`\>

Override field types (default: auto-detect)

## Returns

`object`

### schema

> **schema**: `SchemaElement`[]

### columnData

> **columnData**: `Map`\<`string`, `any`[]\>

### shredPaths

> **shredPaths**: `string`[]
