[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / compact

# Function: compact()

> **compact**\<`T`\>(`table`, `config`): `Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Defined in: src/compaction/index.ts:466

Compact multiple small files into larger files.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### table

[`DeltaTable`](../../delta/classes/DeltaTable.md)\<`T`\>

The Delta table to compact

### config

[`CompactionConfig`](../interfaces/CompactionConfig.md)

Compaction configuration

## Returns

`Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Metrics about the compaction operation
