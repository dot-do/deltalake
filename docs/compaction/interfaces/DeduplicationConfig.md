[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / DeduplicationConfig

# Interface: DeduplicationConfig

Defined in: [src/compaction/index.ts:126](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L126)

Configuration options for deduplication

## Properties

### primaryKey?

> `readonly` `optional` **primaryKey**: readonly `string`[]

Defined in: [src/compaction/index.ts:128](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L128)

Columns that form the primary key for deduplication

***

### keepStrategy?

> `readonly` `optional` **keepStrategy**: `"latest"` \| `"first"` \| `"last"`

Defined in: [src/compaction/index.ts:131](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L131)

Strategy for keeping rows when duplicates are found

***

### orderByColumn?

> `readonly` `optional` **orderByColumn**: `string`

Defined in: [src/compaction/index.ts:134](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L134)

Column to use for ordering when using 'latest' strategy

***

### exactDuplicates?

> `readonly` `optional` **exactDuplicates**: `boolean`

Defined in: [src/compaction/index.ts:137](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L137)

Remove only exact duplicates (all columns match)

***

### includeDistribution?

> `readonly` `optional` **includeDistribution**: `boolean`

Defined in: [src/compaction/index.ts:140](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L140)

Include distribution statistics in metrics
