[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / VacuumConfig

# Interface: VacuumConfig

Defined in: src/delta/vacuum.ts:38

Configuration options for the vacuum operation.

## Properties

### retentionHours?

> `optional` **retentionHours**: `number`

Defined in: src/delta/vacuum.ts:44

Retention period in hours.
Files older than this will be deleted.
Default: 168 hours (7 days)

***

### dryRun?

> `optional` **dryRun**: `boolean`

Defined in: src/delta/vacuum.ts:50

If true, only preview what would be deleted without actually deleting.
Default: false

***

### onProgress()?

> `optional` **onProgress**: (`phase`, `current`, `total`) => `void`

Defined in: src/delta/vacuum.ts:55

Optional progress callback for monitoring vacuum progress.

#### Parameters

##### phase

`string`

##### current

`number`

##### total

`number`

#### Returns

`void`
