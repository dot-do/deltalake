[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCConfig

# Interface: CDCConfig

Defined in: [src/cdc/index.ts:136](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L136)

CDC Configuration for a Delta table.
Controls CDC behavior including retention policies.

## Properties

### enabled

> `readonly` **enabled**: `boolean`

Defined in: [src/cdc/index.ts:138](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L138)

Whether CDC is enabled for this table

***

### retentionMs?

> `readonly` `optional` **retentionMs**: `number`

Defined in: [src/cdc/index.ts:140](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L140)

Retention period for CDC files in milliseconds
