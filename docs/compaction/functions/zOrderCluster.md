[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / zOrderCluster

# Function: zOrderCluster()

> **zOrderCluster**\<`T`\>(`table`, `config`): `Promise`\<[`ClusteringMetrics`](../interfaces/ClusteringMetrics.md)\>

Defined in: src/compaction/index.ts:930

Reorder data using Z-order or Hilbert curve for better data skipping.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### table

[`DeltaTable`](../../delta/classes/DeltaTable.md)\<`T`\>

The Delta table to cluster

### config

[`ClusteringConfig`](../interfaces/ClusteringConfig.md)

Clustering configuration

## Returns

`Promise`\<[`ClusteringMetrics`](../interfaces/ClusteringMetrics.md)\>

Metrics about the clustering operation
