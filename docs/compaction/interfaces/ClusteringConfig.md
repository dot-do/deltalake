[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / ClusteringConfig

# Interface: ClusteringConfig

Defined in: src/compaction/index.ts:73

Configuration options for Z-order clustering

## Properties

### columns

> **columns**: `string`[]

Defined in: src/compaction/index.ts:75

Columns to use for clustering

***

### curveType?

> `optional` **curveType**: `"z-order"` \| `"hilbert"`

Defined in: src/compaction/index.ts:78

Type of space-filling curve

***

### dryRun?

> `optional` **dryRun**: `boolean`

Defined in: src/compaction/index.ts:81

Dry run mode - compute metrics without actually clustering
