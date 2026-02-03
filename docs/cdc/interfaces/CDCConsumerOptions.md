[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCConsumerOptions

# Interface: CDCConsumerOptions

Defined in: src/cdc/index.ts:340

Options for creating a CDC consumer.

## Properties

### fromSeq?

> `optional` **fromSeq**: `bigint`

Defined in: src/cdc/index.ts:342

Starting sequence number (records before this are skipped)

***

### fromTimestamp?

> `optional` **fromTimestamp**: `Date`

Defined in: src/cdc/index.ts:345

Starting timestamp (records before this are skipped)

***

### operations?

> `optional` **operations**: readonly [`CDCOperation`](../type-aliases/CDCOperation.md)[]

Defined in: src/cdc/index.ts:348

Filter by operation types (only these operations are processed)
