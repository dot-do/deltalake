[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / OperationRecord

# Interface: OperationRecord

Defined in: [src/storage/index.ts:2031](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2031)

Operation record for tracking storage operations (testing utility).

## Properties

### operation

> `readonly` **operation**: [`MemoryStorageOperation`](../type-aliases/MemoryStorageOperation.md)

Defined in: [src/storage/index.ts:2033](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2033)

The type of operation performed

***

### path

> `readonly` **path**: `string`

Defined in: [src/storage/index.ts:2035](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2035)

The path that was operated on

***

### timestamp

> `readonly` **timestamp**: `number`

Defined in: [src/storage/index.ts:2037](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2037)

Unix timestamp when the operation occurred
