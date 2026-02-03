[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / CompactionConfig

# Interface: CompactionConfig

Defined in: src/compaction/index.ts:21

Configuration options for file compaction

## Properties

### targetFileSize

> **targetFileSize**: `number`

Defined in: src/compaction/index.ts:23

Target file size in bytes (default: 128MB)

***

### minFilesForCompaction?

> `optional` **minFilesForCompaction**: `number`

Defined in: src/compaction/index.ts:26

Minimum number of files required to trigger compaction (default: 2)

***

### strategy?

> `optional` **strategy**: `"bin-packing"` \| `"greedy"` \| `"sort-by-size"`

Defined in: src/compaction/index.ts:29

File selection strategy

***

### partitionColumns?

> `optional` **partitionColumns**: `string`[]

Defined in: src/compaction/index.ts:32

Columns used for partitioning

***

### preserveOrder?

> `optional` **preserveOrder**: `boolean`

Defined in: src/compaction/index.ts:35

Whether to preserve row order during compaction

***

### verifyIntegrity?

> `optional` **verifyIntegrity**: `boolean`

Defined in: src/compaction/index.ts:38

Whether to verify data integrity after compaction

***

### dryRun?

> `optional` **dryRun**: `boolean`

Defined in: src/compaction/index.ts:41

Dry run mode - compute metrics without actually compacting

***

### baseVersion?

> `optional` **baseVersion**: `number`

Defined in: src/compaction/index.ts:44

Base version for optimistic concurrency control

***

### onProgress()?

> `optional` **onProgress**: (`phase`, `progress`) => `void`

Defined in: src/compaction/index.ts:47

Progress callback

#### Parameters

##### phase

`string`

##### progress

`number`

#### Returns

`void`
