[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCRecord

# Interface: CDCRecord\<T\>

Defined in: src/cdc/index.ts:39

Standard CDC record format for cross-system interoperability.
Compatible with Debezium, Kafka Connect, and other CDC systems.

## Type Parameters

### T

`T` = `unknown`

## Properties

### \_id

> **\_id**: `string`

Defined in: src/cdc/index.ts:41

Entity ID (document ID, message key, row PK)

***

### \_seq

> **\_seq**: `bigint`

Defined in: src/cdc/index.ts:44

Sequence number (MongoLake LSN, Kafka offset, oplog ts)

***

### \_op

> **\_op**: [`CDCOperation`](../type-aliases/CDCOperation.md)

Defined in: src/cdc/index.ts:47

Operation type

***

### \_before

> **\_before**: `T` \| `null`

Defined in: src/cdc/index.ts:50

Previous state (null for create)

***

### \_after

> **\_after**: `T` \| `null`

Defined in: src/cdc/index.ts:53

New state (null for delete)

***

### \_ts

> **\_ts**: `bigint`

Defined in: src/cdc/index.ts:56

Timestamp in nanoseconds

***

### \_source

> **\_source**: [`CDCSource`](CDCSource.md)

Defined in: src/cdc/index.ts:59

Source metadata

***

### \_txn?

> `optional` **\_txn**: `string`

Defined in: src/cdc/index.ts:62

Transaction ID (for exactly-once semantics)
