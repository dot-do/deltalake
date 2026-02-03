[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / VacuumMetrics

# Interface: VacuumMetrics

Defined in: src/delta/vacuum.ts:61

Metrics returned after a vacuum operation.

## Properties

### filesDeleted

> **filesDeleted**: `number`

Defined in: src/delta/vacuum.ts:63

Number of files deleted

***

### bytesFreed

> **bytesFreed**: `number`

Defined in: src/delta/vacuum.ts:66

Total bytes freed (sum of deleted file sizes)

***

### filesRetained

> **filesRetained**: `number`

Defined in: src/delta/vacuum.ts:69

Number of files retained (still referenced or within retention period)

***

### dryRun

> **dryRun**: `boolean`

Defined in: src/delta/vacuum.ts:72

Whether this was a dry run

***

### filesToDelete?

> `optional` **filesToDelete**: `string`[]

Defined in: src/delta/vacuum.ts:75

Files that would be deleted (populated in dry run mode)

***

### durationMs

> **durationMs**: `number`

Defined in: src/delta/vacuum.ts:78

Duration of the vacuum operation in milliseconds

***

### filesScanned

> **filesScanned**: `number`

Defined in: src/delta/vacuum.ts:81

Number of files scanned

***

### errors?

> `optional` **errors**: `string`[]

Defined in: src/delta/vacuum.ts:84

Any errors encountered during deletion (non-fatal)
