[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / CompactionConfig

# Interface: CompactionConfig

Defined in: [src/compaction/index.ts:94](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L94)

Configuration options for file compaction

## Properties

### targetFileSize

> `readonly` **targetFileSize**: `number`

Defined in: [src/compaction/index.ts:96](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L96)

Target file size in bytes (default: 128MB)

***

### minFilesForCompaction?

> `readonly` `optional` **minFilesForCompaction**: `number`

Defined in: [src/compaction/index.ts:99](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L99)

Minimum number of files required to trigger compaction (default: 2)

***

### strategy?

> `readonly` `optional` **strategy**: `"bin-packing"` \| `"greedy"` \| `"sort-by-size"`

Defined in: [src/compaction/index.ts:102](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L102)

File selection strategy

***

### partitionColumns?

> `readonly` `optional` **partitionColumns**: readonly `string`[]

Defined in: [src/compaction/index.ts:105](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L105)

Columns used for partitioning

***

### preserveOrder?

> `readonly` `optional` **preserveOrder**: `boolean`

Defined in: [src/compaction/index.ts:108](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L108)

Whether to preserve row order during compaction

***

### verifyIntegrity?

> `readonly` `optional` **verifyIntegrity**: `boolean`

Defined in: [src/compaction/index.ts:111](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L111)

Whether to verify data integrity after compaction

***

### dryRun?

> `readonly` `optional` **dryRun**: `boolean`

Defined in: [src/compaction/index.ts:114](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L114)

Dry run mode - compute metrics without actually compacting

***

### baseVersion?

> `readonly` `optional` **baseVersion**: `number`

Defined in: [src/compaction/index.ts:117](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L117)

Base version for optimistic concurrency control

***

### onProgress()?

> `readonly` `optional` **onProgress**: (`phase`, `progress`) => `void`

Defined in: [src/compaction/index.ts:120](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L120)

Progress callback

#### Parameters

##### phase

`string`

##### progress

`number`

#### Returns

`void`
