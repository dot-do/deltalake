[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / OperationRecord

# Interface: OperationRecord

Defined in: src/storage/index.ts:1779

Operation record for tracking storage operations (testing utility).

## Properties

### operation

> `readonly` **operation**: [`MemoryStorageOperation`](../type-aliases/MemoryStorageOperation.md)

Defined in: src/storage/index.ts:1781

The type of operation performed

***

### path

> `readonly` **path**: `string`

Defined in: src/storage/index.ts:1783

The path that was operated on

***

### timestamp

> `readonly` **timestamp**: `number`

Defined in: src/storage/index.ts:1785

Unix timestamp when the operation occurred
