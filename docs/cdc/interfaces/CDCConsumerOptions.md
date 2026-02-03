[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCConsumerOptions

# Interface: CDCConsumerOptions

Defined in: [src/cdc/index.ts:464](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L464)

Options for creating a CDC consumer.

## Properties

### fromSeq?

> `readonly` `optional` **fromSeq**: `bigint`

Defined in: [src/cdc/index.ts:466](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L466)

Starting sequence number (records before this are skipped)

***

### fromTimestamp?

> `readonly` `optional` **fromTimestamp**: `Date`

Defined in: [src/cdc/index.ts:469](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L469)

Starting timestamp (records before this are skipped)

***

### operations?

> `readonly` `optional` **operations**: readonly [`CDCOperation`](../type-aliases/CDCOperation.md)[]

Defined in: [src/cdc/index.ts:472](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L472)

Filter by operation types (only these operations are processed)
