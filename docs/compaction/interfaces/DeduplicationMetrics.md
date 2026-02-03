[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / DeduplicationMetrics

# Interface: DeduplicationMetrics

Defined in: src/compaction/index.ts:189

Metrics returned from deduplication operation

## Properties

### rowsBefore

> **rowsBefore**: `number`

Defined in: src/compaction/index.ts:191

Number of rows before deduplication

***

### rowsAfter

> **rowsAfter**: `number`

Defined in: src/compaction/index.ts:194

Number of rows after deduplication

***

### duplicatesRemoved

> **duplicatesRemoved**: `number`

Defined in: src/compaction/index.ts:197

Number of duplicate rows removed

***

### exactDuplicatesRemoved?

> `optional` **exactDuplicatesRemoved**: `number`

Defined in: src/compaction/index.ts:200

Number of exact duplicates removed (when exactDuplicates=true)

***

### deduplicationRatio

> **deduplicationRatio**: `number`

Defined in: src/compaction/index.ts:203

Ratio of duplicates to total rows

***

### duplicateDistribution?

> `optional` **duplicateDistribution**: `Record`\<`number`, `number`\>

Defined in: src/compaction/index.ts:206

Distribution of duplicate counts per key

***

### maxDuplicatesPerKey?

> `optional` **maxDuplicatesPerKey**: `number`

Defined in: src/compaction/index.ts:209

Maximum duplicates found for any single key
