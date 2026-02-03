[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / LatencyConfig

# Interface: LatencyConfig

Defined in: [src/storage/index.ts:2003](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2003)

Latency configuration for simulating slow storage operations.

## Properties

### read?

> `readonly` `optional` **read**: `number`

Defined in: [src/storage/index.ts:2005](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2005)

Simulated read latency in milliseconds

***

### write?

> `readonly` `optional` **write**: `number`

Defined in: [src/storage/index.ts:2007](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2007)

Simulated write latency in milliseconds

***

### delete?

> `readonly` `optional` **delete**: `number`

Defined in: [src/storage/index.ts:2009](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2009)

Simulated delete latency in milliseconds

***

### list?

> `readonly` `optional` **list**: `number`

Defined in: [src/storage/index.ts:2011](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2011)

Simulated list latency in milliseconds
