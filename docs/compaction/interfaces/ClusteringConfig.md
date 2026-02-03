[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / ClusteringConfig

# Interface: ClusteringConfig

Defined in: [src/compaction/index.ts:146](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L146)

Configuration options for Z-order clustering

## Properties

### columns

> `readonly` **columns**: readonly `string`[]

Defined in: [src/compaction/index.ts:148](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L148)

Columns to use for clustering

***

### curveType?

> `readonly` `optional` **curveType**: `"z-order"` \| `"hilbert"`

Defined in: [src/compaction/index.ts:151](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L151)

Type of space-filling curve

***

### dryRun?

> `readonly` `optional` **dryRun**: `boolean`

Defined in: [src/compaction/index.ts:154](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L154)

Dry run mode - compute metrics without actually clustering
