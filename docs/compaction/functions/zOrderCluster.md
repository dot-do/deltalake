[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / zOrderCluster

# Function: zOrderCluster()

> **zOrderCluster**\<`T`\>(`table`, `config`): `Promise`\<[`ClusteringMetrics`](../interfaces/ClusteringMetrics.md)\>

Defined in: [src/compaction/index.ts:1149](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L1149)

Reorder data using Z-order or Hilbert curve for better data skipping.

## Z-Order Clustering Overview

Z-order (Morton) clustering is a technique for organizing data to improve
query performance when filtering on multiple columns. It works by:

1. **Normalizing Values**: Each clustering column's values are normalized to [0, 1]
2. **Computing Z-Values**: The normalized values are converted to integers and
   their bits are interleaved to produce a single Z-order value
3. **Sorting**: Rows are sorted by their Z-order values
4. **Writing**: Sorted data is written to new files

## Why Z-Order Improves Performance

Traditional single-column sorting only helps queries filtering on that column.
Z-order interleaves bits from multiple columns, so rows with similar values
across ALL clustering columns end up stored together.

For example, with columns (region, timestamp):
- Queries filtering on region benefit
- Queries filtering on timestamp benefit
- Queries filtering on BOTH region AND timestamp benefit most

## Zone Maps and Data Skipping

After Z-order clustering, each file contains rows with a narrow range of values
for each clustering column. The min/max values stored in file metadata (zone maps)
allow the query engine to skip entire files that cannot contain matching rows.

## Curve Types

- **z-order**: Standard Morton curve, good for most use cases
- **hilbert**: Better locality preservation, but more compute-intensive

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

The row type of the table

## Parameters

### table

[`DeltaTable`](../../index/classes/DeltaTable.md)\<`T`\>

The Delta table to cluster

### config

[`ClusteringConfig`](../interfaces/ClusteringConfig.md)

Clustering configuration specifying columns and curve type

## Returns

`Promise`\<[`ClusteringMetrics`](../interfaces/ClusteringMetrics.md)\>

Metrics about the clustering operation including data skipping estimates

## Examples

```typescript
const metrics = await zOrderCluster(table, {
  columns: ['region', 'category', 'timestamp'],
  curveType: 'z-order',
})

console.log(`Estimated skip rate: ${(metrics.estimatedSkipRate * 100).toFixed(1)}%`)
```

```typescript
const metrics = await zOrderCluster(table, {
  columns: ['x', 'y'],
  dryRun: true,
})

console.log(`Zone map stats:`, metrics.zoneMapStats)
```
