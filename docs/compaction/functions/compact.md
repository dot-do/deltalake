[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / compact

# Function: compact()

> **compact**\<`T`\>(`table`, `config`): `Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Defined in: [src/compaction/index.ts:568](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L568)

Compact multiple small files into larger files.

Small files are common in streaming workloads where data arrives in frequent
small batches. This function merges these files to reduce storage overhead
and improve query performance.

## Compaction Process

1. **File Selection**: Files smaller than `targetFileSize` are selected
2. **Partition Grouping**: Files are grouped by partition (if partitionColumns specified)
3. **Data Reading**: All rows from selected files are read into memory
4. **File Writing**: Rows are written to new, larger Parquet files
5. **Atomic Commit**: A single transaction removes old files and adds new ones

## Error Handling

- If the commit fails, all newly created files are cleaned up
- If integrity verification is enabled and corruption is detected, a StorageError is thrown
- Concurrent modification is detected via optimistic concurrency control

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

The row type of the table

## Parameters

### table

[`DeltaTable`](../../index/classes/DeltaTable.md)\<`T`\>

The Delta table to compact

### config

[`CompactionConfig`](../interfaces/CompactionConfig.md)

Compaction configuration options

## Returns

`Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Metrics about the compaction operation including file counts, timing, and throughput

## Throws

If data integrity verification fails

## Throws

If partition values are invalid

## Examples

```typescript
const metrics = await compact(table, {
  targetFileSize: 128 * 1024 * 1024, // 128MB target
})
```

```typescript
const metrics = await compact(table, {
  targetFileSize: 64 * 1024 * 1024,
  strategy: 'bin-packing',
  partitionColumns: ['region', 'date'],
  verifyIntegrity: true,
})
```
