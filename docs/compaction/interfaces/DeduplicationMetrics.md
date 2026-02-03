[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / DeduplicationMetrics

# Interface: DeduplicationMetrics

Defined in: [src/compaction/index.ts:262](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L262)

Metrics returned from deduplication operation

## Properties

### rowsBefore

> **rowsBefore**: `number`

Defined in: [src/compaction/index.ts:264](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L264)

Number of rows before deduplication

***

### rowsAfter

> **rowsAfter**: `number`

Defined in: [src/compaction/index.ts:267](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L267)

Number of rows after deduplication

***

### duplicatesRemoved

> **duplicatesRemoved**: `number`

Defined in: [src/compaction/index.ts:270](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L270)

Number of duplicate rows removed

***

### exactDuplicatesRemoved?

> `optional` **exactDuplicatesRemoved**: `number`

Defined in: [src/compaction/index.ts:273](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L273)

Number of exact duplicates removed (when exactDuplicates=true)

***

### deduplicationRatio

> **deduplicationRatio**: `number`

Defined in: [src/compaction/index.ts:276](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L276)

Ratio of duplicates to total rows

***

### duplicateDistribution?

> `optional` **duplicateDistribution**: `Record`\<`number`, `number`\>

Defined in: [src/compaction/index.ts:279](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L279)

Distribution of duplicate counts per key

***

### maxDuplicatesPerKey?

> `optional` **maxDuplicatesPerKey**: `number`

Defined in: [src/compaction/index.ts:282](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L282)

Maximum duplicates found for any single key
