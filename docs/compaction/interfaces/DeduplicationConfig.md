[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / DeduplicationConfig

# Interface: DeduplicationConfig

Defined in: src/compaction/index.ts:53

Configuration options for deduplication

## Properties

### primaryKey?

> `optional` **primaryKey**: `string`[]

Defined in: src/compaction/index.ts:55

Columns that form the primary key for deduplication

***

### keepStrategy?

> `optional` **keepStrategy**: `"latest"` \| `"first"` \| `"last"`

Defined in: src/compaction/index.ts:58

Strategy for keeping rows when duplicates are found

***

### orderByColumn?

> `optional` **orderByColumn**: `string`

Defined in: src/compaction/index.ts:61

Column to use for ordering when using 'latest' strategy

***

### exactDuplicates?

> `optional` **exactDuplicates**: `boolean`

Defined in: src/compaction/index.ts:64

Remove only exact duplicates (all columns match)

***

### includeDistribution?

> `optional` **includeDistribution**: `boolean`

Defined in: src/compaction/index.ts:67

Include distribution statistics in metrics
