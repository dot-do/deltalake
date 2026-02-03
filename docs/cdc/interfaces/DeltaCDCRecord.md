[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / DeltaCDCRecord

# Interface: DeltaCDCRecord\<T\>

Defined in: [src/cdc/index.ts:119](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L119)

Delta Lake CDC Record structure.
Contains the standard Delta Lake CDC metadata columns.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>

The row data type

## Properties

### \_change\_type

> **\_change\_type**: [`DeltaCDCChangeType`](../type-aliases/DeltaCDCChangeType.md)

Defined in: [src/cdc/index.ts:121](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L121)

The change type: insert, update_preimage, update_postimage, delete

***

### \_commit\_version

> **\_commit\_version**: `bigint`

Defined in: [src/cdc/index.ts:123](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L123)

The commit version that produced this change

***

### \_commit\_timestamp

> **\_commit\_timestamp**: `Date`

Defined in: [src/cdc/index.ts:125](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L125)

The timestamp of the commit

***

### data

> **data**: `T`

Defined in: [src/cdc/index.ts:127](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L127)

The actual row data
