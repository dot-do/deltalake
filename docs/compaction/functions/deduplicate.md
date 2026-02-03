[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / deduplicate

# Function: deduplicate()

> **deduplicate**\<`T`\>(`table`, `config`): `Promise`\<[`DeduplicationMetrics`](../interfaces/DeduplicationMetrics.md)\>

Defined in: [src/compaction/index.ts:930](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L930)

Remove duplicate rows from a Delta table.

Deduplication is essential for maintaining data quality, especially when:
- Multiple data sources may produce overlapping records
- Streaming pipelines may replay events
- CDC (Change Data Capture) streams contain duplicate operations

## Deduplication Strategies

### By Primary Key (default)
Groups rows by the specified primary key columns and keeps one row per group.
The `keepStrategy` determines which row to keep:
- `first`: Keep the first occurrence (in file/row order)
- `last`: Keep the last occurrence
- `latest`: Keep the row with the highest value in `orderByColumn`

### Exact Duplicates
When `exactDuplicates: true`, only removes rows that are identical across
all columns (full row comparison).

## Process

1. Read all rows from all files into memory
2. Group rows by key (primary key or full row hash)
3. Select one row per group based on strategy
4. Write deduplicated data to new file(s)
5. Atomic commit replaces old files with new ones

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

The row type of the table

## Parameters

### table

[`DeltaTable`](../../index/classes/DeltaTable.md)\<`T`\>

The Delta table to deduplicate

### config

[`DeduplicationConfig`](../interfaces/DeduplicationConfig.md)

Deduplication configuration options

## Returns

`Promise`\<[`DeduplicationMetrics`](../interfaces/DeduplicationMetrics.md)\>

Metrics about the deduplication operation

## Examples

```typescript
const metrics = await deduplicate(table, {
  primaryKey: ['id'],
  keepStrategy: 'latest',
  orderByColumn: 'timestamp',
})
```

```typescript
const metrics = await deduplicate(table, {
  exactDuplicates: true,
})
```
