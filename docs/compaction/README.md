[**@dotdo/deltalake v0.0.1**](../README.md)

***

[@dotdo/deltalake](../modules.md) / compaction

# compaction

Compaction and Deduplication Module

This module provides functionality for optimizing Delta Lake tables:

## File Compaction
Merges multiple small files into larger files to reduce storage overhead
and improve query performance. Small files are common in streaming workloads
where data arrives in frequent small batches.

## Data Deduplication
Removes duplicate rows based on primary key or exact match. Supports multiple
strategies for choosing which duplicate to keep (first, last, latest).

## Z-Order Clustering
Reorders data using space-filling curves (Z-order or Hilbert) to co-locate
related data and improve data skipping effectiveness during queries.

## Examples

```typescript
import { compact } from '@dotdo/deltalake/compaction'

const metrics = await compact(table, {
  targetFileSize: 128 * 1024 * 1024, // 128MB
  strategy: 'bin-packing',
})

console.log(`Compacted ${metrics.filesCompacted} files into ${metrics.filesCreated}`)
```

```typescript
import { deduplicate } from '@dotdo/deltalake/compaction'

const metrics = await deduplicate(table, {
  primaryKey: ['id'],
  keepStrategy: 'latest',
  orderByColumn: 'timestamp',
})

console.log(`Removed ${metrics.duplicatesRemoved} duplicates`)
```

```typescript
import { zOrderCluster } from '@dotdo/deltalake/compaction'

const metrics = await zOrderCluster(table, {
  columns: ['region', 'category', 'timestamp'],
  curveType: 'z-order',
})

console.log(`Estimated skip rate: ${(metrics.estimatedSkipRate * 100).toFixed(1)}%`)
```

## Interfaces

- [CompactionConfig](interfaces/CompactionConfig.md)
- [DeduplicationConfig](interfaces/DeduplicationConfig.md)
- [ClusteringConfig](interfaces/ClusteringConfig.md)
- [CompactionMetrics](interfaces/CompactionMetrics.md)
- [DeduplicationMetrics](interfaces/DeduplicationMetrics.md)
- [ClusteringMetrics](interfaces/ClusteringMetrics.md)

## Functions

- [compact](functions/compact.md)
- [deduplicate](functions/deduplicate.md)
- [zOrderCluster](functions/zOrderCluster.md)
