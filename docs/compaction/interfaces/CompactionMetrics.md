[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / CompactionMetrics

# Interface: CompactionMetrics

Defined in: src/compaction/index.ts:91

Metrics returned from compaction operation

## Properties

### filesCompacted

> **filesCompacted**: `number`

Defined in: src/compaction/index.ts:93

Number of files that were compacted

***

### filesCreated

> **filesCreated**: `number`

Defined in: src/compaction/index.ts:96

Number of new files created

***

### filesSkippedLargeEnough

> **filesSkippedLargeEnough**: `number`

Defined in: src/compaction/index.ts:99

Number of files skipped because they're already large enough

***

### rowsRead

> **rowsRead**: `number`

Defined in: src/compaction/index.ts:102

Total rows read during compaction

***

### rowsWritten

> **rowsWritten**: `number`

Defined in: src/compaction/index.ts:105

Total rows written during compaction

***

### fileCountBefore

> **fileCountBefore**: `number`

Defined in: src/compaction/index.ts:108

Number of files before compaction

***

### fileCountAfter

> **fileCountAfter**: `number`

Defined in: src/compaction/index.ts:111

Number of files after compaction

***

### totalBytesBefore

> **totalBytesBefore**: `number`

Defined in: src/compaction/index.ts:114

Total bytes before compaction

***

### totalBytesAfter

> **totalBytesAfter**: `number`

Defined in: src/compaction/index.ts:117

Total bytes after compaction

***

### avgFileSizeBefore

> **avgFileSizeBefore**: `number`

Defined in: src/compaction/index.ts:120

Average file size before compaction

***

### avgFileSizeAfter

> **avgFileSizeAfter**: `number`

Defined in: src/compaction/index.ts:123

Average file size after compaction

***

### durationMs

> **durationMs**: `number`

Defined in: src/compaction/index.ts:126

Total duration of compaction in milliseconds

***

### readTimeMs

> **readTimeMs**: `number`

Defined in: src/compaction/index.ts:129

Time spent reading files in milliseconds

***

### writeTimeMs

> **writeTimeMs**: `number`

Defined in: src/compaction/index.ts:132

Time spent writing files in milliseconds

***

### commitTimeMs

> **commitTimeMs**: `number`

Defined in: src/compaction/index.ts:135

Time spent committing transaction in milliseconds

***

### rowsPerSecond

> **rowsPerSecond**: `number`

Defined in: src/compaction/index.ts:138

Rows processed per second

***

### bytesPerSecond

> **bytesPerSecond**: `number`

Defined in: src/compaction/index.ts:141

Bytes processed per second

***

### commitVersion

> **commitVersion**: `number`

Defined in: src/compaction/index.ts:144

Version number of the commit

***

### commitsCreated

> **commitsCreated**: `number`

Defined in: src/compaction/index.ts:147

Number of commits created

***

### skipped

> **skipped**: `boolean`

Defined in: src/compaction/index.ts:150

Whether compaction was skipped

***

### skipReason?

> `optional` **skipReason**: `string`

Defined in: src/compaction/index.ts:153

Reason for skipping compaction

***

### dryRun

> **dryRun**: `boolean`

Defined in: src/compaction/index.ts:156

Whether this was a dry run

***

### wouldCompact

> **wouldCompact**: `boolean`

Defined in: src/compaction/index.ts:159

Whether compaction would have occurred (dry run only)

***

### strategy

> **strategy**: `string`

Defined in: src/compaction/index.ts:162

Strategy used for file selection

***

### binPackingEfficiency

> **binPackingEfficiency**: `number`

Defined in: src/compaction/index.ts:165

Bin-packing efficiency (0-1)

***

### partitionsCompacted

> **partitionsCompacted**: `number`

Defined in: src/compaction/index.ts:168

Number of partitions compacted

***

### emptyPartitionsSkipped

> **emptyPartitionsSkipped**: `number`

Defined in: src/compaction/index.ts:171

Number of empty partitions skipped

***

### integrityVerified

> **integrityVerified**: `boolean`

Defined in: src/compaction/index.ts:174

Whether integrity was verified

***

### checksumMatch

> **checksumMatch**: `boolean`

Defined in: src/compaction/index.ts:177

Whether checksum matched

***

### conflictDetected

> **conflictDetected**: `boolean`

Defined in: src/compaction/index.ts:180

Whether a conflict was detected

***

### retried

> **retried**: `boolean`

Defined in: src/compaction/index.ts:183

Whether operation was retried
