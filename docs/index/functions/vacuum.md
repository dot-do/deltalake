[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / vacuum

# Function: vacuum()

> **vacuum**(`table`, `config?`): `Promise`\<[`VacuumMetrics`](../interfaces/VacuumMetrics.md)\>

Defined in: src/delta/vacuum.ts:125

Perform a VACUUM operation on a Delta table.

Removes orphaned files that are:
1. Not referenced by any active snapshot
2. Older than the retention period

## Parameters

### table

[`DeltaTable`](../../delta/classes/DeltaTable.md)

The DeltaTable to vacuum

### config?

[`VacuumConfig`](../interfaces/VacuumConfig.md)

Optional configuration for the vacuum operation

## Returns

`Promise`\<[`VacuumMetrics`](../interfaces/VacuumMetrics.md)\>

Metrics about the vacuum operation

## Example

```typescript
// Basic vacuum with defaults (7 day retention)
const metrics = await vacuum(table)
console.log(`Deleted ${metrics.filesDeleted} files, freed ${metrics.bytesFreed} bytes`)

// Dry run to preview deletions
const preview = await vacuum(table, { dryRun: true })
console.log(`Would delete: ${preview.filesToDelete}`)

// Custom retention period (24 hours)
const metrics = await vacuum(table, { retentionHours: 24 })
```
