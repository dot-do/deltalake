[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / VacuumConfig

# Interface: VacuumConfig

Defined in: [src/delta/vacuum.ts:40](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L40)

Configuration options for the vacuum operation.

## Properties

### retentionHours?

> `readonly` `optional` **retentionHours**: `number`

Defined in: [src/delta/vacuum.ts:46](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L46)

Retention period in hours.
Files older than this will be deleted.
Default: 168 hours (7 days)

***

### dryRun?

> `readonly` `optional` **dryRun**: `boolean`

Defined in: [src/delta/vacuum.ts:52](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L52)

If true, only preview what would be deleted without actually deleting.
Default: false

***

### onProgress()?

> `readonly` `optional` **onProgress**: (`phase`, `current`, `total`) => `void`

Defined in: [src/delta/vacuum.ts:57](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/vacuum.ts#L57)

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
