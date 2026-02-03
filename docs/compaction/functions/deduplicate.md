[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / deduplicate

# Function: deduplicate()

> **deduplicate**\<`T`\>(`table`, `config`): `Promise`\<[`DeduplicationMetrics`](../interfaces/DeduplicationMetrics.md)\>

Defined in: src/compaction/index.ts:777

Remove duplicate rows from a Delta table.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### table

[`DeltaTable`](../../delta/classes/DeltaTable.md)\<`T`\>

The Delta table to deduplicate

### config

[`DeduplicationConfig`](../interfaces/DeduplicationConfig.md)

Deduplication configuration

## Returns

`Promise`\<[`DeduplicationMetrics`](../interfaces/DeduplicationMetrics.md)\>

Metrics about the deduplication operation
