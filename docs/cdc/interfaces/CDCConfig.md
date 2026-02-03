[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCConfig

# Interface: CDCConfig

Defined in: src/cdc/index.ts:121

CDC Configuration for a Delta table.
Controls CDC behavior including retention policies.

## Properties

### enabled

> **enabled**: `boolean`

Defined in: src/cdc/index.ts:123

Whether CDC is enabled for this table

***

### retentionMs?

> `optional` **retentionMs**: `number`

Defined in: src/cdc/index.ts:125

Retention period for CDC files in milliseconds
