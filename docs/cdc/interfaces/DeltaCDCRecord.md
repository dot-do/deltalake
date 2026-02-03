[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / DeltaCDCRecord

# Interface: DeltaCDCRecord\<T\>

Defined in: src/cdc/index.ts:106

Delta Lake CDC Record structure.
Contains the standard Delta Lake CDC metadata columns.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>

The row data type

## Properties

### \_change\_type

> **\_change\_type**: [`DeltaCDCChangeType`](../type-aliases/DeltaCDCChangeType.md)

Defined in: src/cdc/index.ts:108

The change type: insert, update_preimage, update_postimage, delete

***

### \_commit\_version

> **\_commit\_version**: `bigint`

Defined in: src/cdc/index.ts:110

The commit version that produced this change

***

### \_commit\_timestamp

> **\_commit\_timestamp**: `Date`

Defined in: src/cdc/index.ts:112

The timestamp of the commit

***

### data

> **data**: `T`

Defined in: src/cdc/index.ts:114

The actual row data
