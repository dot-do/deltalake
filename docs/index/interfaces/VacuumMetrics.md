[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / VacuumMetrics

# Interface: VacuumMetrics

Defined in: [src/delta/vacuum.ts:63](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L63)

Metrics returned after a vacuum operation.

## Properties

### filesDeleted

> **filesDeleted**: `number`

Defined in: [src/delta/vacuum.ts:65](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L65)

Number of files deleted

***

### bytesFreed

> **bytesFreed**: `number`

Defined in: [src/delta/vacuum.ts:68](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L68)

Total bytes freed (sum of deleted file sizes)

***

### filesRetained

> **filesRetained**: `number`

Defined in: [src/delta/vacuum.ts:71](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L71)

Number of files retained (still referenced or within retention period)

***

### dryRun

> **dryRun**: `boolean`

Defined in: [src/delta/vacuum.ts:74](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L74)

Whether this was a dry run

***

### filesToDelete?

> `optional` **filesToDelete**: `string`[]

Defined in: [src/delta/vacuum.ts:77](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L77)

Files that would be deleted (populated in dry run mode)

***

### durationMs

> **durationMs**: `number`

Defined in: [src/delta/vacuum.ts:80](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L80)

Duration of the vacuum operation in milliseconds

***

### filesScanned

> **filesScanned**: `number`

Defined in: [src/delta/vacuum.ts:83](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L83)

Number of files scanned

***

### errors?

> `optional` **errors**: `string`[]

Defined in: [src/delta/vacuum.ts:86](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L86)

Any errors encountered during deletion (non-fatal)
