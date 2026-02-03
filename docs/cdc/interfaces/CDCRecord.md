[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCRecord

# Interface: CDCRecord\<T\>

Defined in: [src/cdc/index.ts:46](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L46)

Standard CDC record format for cross-system interoperability.
Compatible with Debezium, Kafka Connect, and other CDC systems.

## Type Parameters

### T

`T` = `unknown`

## Properties

### \_id

> **\_id**: `string`

Defined in: [src/cdc/index.ts:48](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L48)

Entity ID (document ID, message key, row PK)

***

### \_seq

> **\_seq**: `bigint`

Defined in: [src/cdc/index.ts:51](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L51)

Sequence number (MongoLake LSN, Kafka offset, oplog ts)

***

### \_op

> **\_op**: [`CDCOperation`](../type-aliases/CDCOperation.md)

Defined in: [src/cdc/index.ts:54](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L54)

Operation type

***

### \_before

> **\_before**: `T` \| `null`

Defined in: [src/cdc/index.ts:57](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L57)

Previous state (null for create)

***

### \_after

> **\_after**: `T` \| `null`

Defined in: [src/cdc/index.ts:60](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L60)

New state (null for delete)

***

### \_ts

> **\_ts**: `bigint`

Defined in: [src/cdc/index.ts:63](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L63)

Timestamp in nanoseconds

***

### \_source

> **\_source**: [`CDCSource`](CDCSource.md)

Defined in: [src/cdc/index.ts:66](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L66)

Source metadata

***

### \_txn?

> `optional` **\_txn**: `string`

Defined in: [src/cdc/index.ts:69](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L69)

Transaction ID (for exactly-once semantics)
