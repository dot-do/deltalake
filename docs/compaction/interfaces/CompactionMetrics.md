[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / CompactionMetrics

# Interface: CompactionMetrics

Defined in: [src/compaction/index.ts:164](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L164)

Metrics returned from compaction operation

## Properties

### filesCompacted

> **filesCompacted**: `number`

Defined in: [src/compaction/index.ts:166](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L166)

Number of files that were compacted

***

### filesCreated

> **filesCreated**: `number`

Defined in: [src/compaction/index.ts:169](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L169)

Number of new files created

***

### filesSkippedLargeEnough

> **filesSkippedLargeEnough**: `number`

Defined in: [src/compaction/index.ts:172](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L172)

Number of files skipped because they're already large enough

***

### rowsRead

> **rowsRead**: `number`

Defined in: [src/compaction/index.ts:175](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L175)

Total rows read during compaction

***

### rowsWritten

> **rowsWritten**: `number`

Defined in: [src/compaction/index.ts:178](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L178)

Total rows written during compaction

***

### fileCountBefore

> **fileCountBefore**: `number`

Defined in: [src/compaction/index.ts:181](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L181)

Number of files before compaction

***

### fileCountAfter

> **fileCountAfter**: `number`

Defined in: [src/compaction/index.ts:184](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L184)

Number of files after compaction

***

### totalBytesBefore

> **totalBytesBefore**: `number`

Defined in: [src/compaction/index.ts:187](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L187)

Total bytes before compaction

***

### totalBytesAfter

> **totalBytesAfter**: `number`

Defined in: [src/compaction/index.ts:190](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L190)

Total bytes after compaction

***

### avgFileSizeBefore

> **avgFileSizeBefore**: `number`

Defined in: [src/compaction/index.ts:193](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L193)

Average file size before compaction

***

### avgFileSizeAfter

> **avgFileSizeAfter**: `number`

Defined in: [src/compaction/index.ts:196](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L196)

Average file size after compaction

***

### durationMs

> **durationMs**: `number`

Defined in: [src/compaction/index.ts:199](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L199)

Total duration of compaction in milliseconds

***

### readTimeMs

> **readTimeMs**: `number`

Defined in: [src/compaction/index.ts:202](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L202)

Time spent reading files in milliseconds

***

### writeTimeMs

> **writeTimeMs**: `number`

Defined in: [src/compaction/index.ts:205](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L205)

Time spent writing files in milliseconds

***

### commitTimeMs

> **commitTimeMs**: `number`

Defined in: [src/compaction/index.ts:208](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L208)

Time spent committing transaction in milliseconds

***

### rowsPerSecond

> **rowsPerSecond**: `number`

Defined in: [src/compaction/index.ts:211](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L211)

Rows processed per second

***

### bytesPerSecond

> **bytesPerSecond**: `number`

Defined in: [src/compaction/index.ts:214](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L214)

Bytes processed per second

***

### commitVersion

> **commitVersion**: `number`

Defined in: [src/compaction/index.ts:217](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L217)

Version number of the commit

***

### commitsCreated

> **commitsCreated**: `number`

Defined in: [src/compaction/index.ts:220](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L220)

Number of commits created

***

### skipped

> **skipped**: `boolean`

Defined in: [src/compaction/index.ts:223](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L223)

Whether compaction was skipped

***

### skipReason?

> `optional` **skipReason**: `string`

Defined in: [src/compaction/index.ts:226](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L226)

Reason for skipping compaction

***

### dryRun

> **dryRun**: `boolean`

Defined in: [src/compaction/index.ts:229](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L229)

Whether this was a dry run

***

### wouldCompact

> **wouldCompact**: `boolean`

Defined in: [src/compaction/index.ts:232](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L232)

Whether compaction would have occurred (dry run only)

***

### strategy

> **strategy**: `string`

Defined in: [src/compaction/index.ts:235](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L235)

Strategy used for file selection

***

### binPackingEfficiency

> **binPackingEfficiency**: `number`

Defined in: [src/compaction/index.ts:238](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L238)

Bin-packing efficiency (0-1)

***

### partitionsCompacted

> **partitionsCompacted**: `number`

Defined in: [src/compaction/index.ts:241](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L241)

Number of partitions compacted

***

### emptyPartitionsSkipped

> **emptyPartitionsSkipped**: `number`

Defined in: [src/compaction/index.ts:244](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L244)

Number of empty partitions skipped

***

### integrityVerified

> **integrityVerified**: `boolean`

Defined in: [src/compaction/index.ts:247](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L247)

Whether integrity was verified

***

### checksumMatch

> **checksumMatch**: `boolean`

Defined in: [src/compaction/index.ts:250](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L250)

Whether checksum matched

***

### conflictDetected

> **conflictDetected**: `boolean`

Defined in: [src/compaction/index.ts:253](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L253)

Whether a conflict was detected

***

### retried

> **retried**: `boolean`

Defined in: [src/compaction/index.ts:256](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L256)

Whether operation was retried
