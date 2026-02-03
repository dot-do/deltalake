[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCReader

# Interface: CDCReader\<T\>

Defined in: src/cdc/index.ts:154

CDC Reader interface for reading changes from a Delta table.
Provides both batch and streaming access to CDC records.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>

The row data type

## Methods

### readByVersion()

> **readByVersion**(`startVersion`, `endVersion`): `Promise`\<[`DeltaCDCRecord`](DeltaCDCRecord.md)\<`T`\>[]\>

Defined in: src/cdc/index.ts:159

Read changes between two versions (inclusive).

#### Parameters

##### startVersion

`bigint`

##### endVersion

`bigint`

#### Returns

`Promise`\<[`DeltaCDCRecord`](DeltaCDCRecord.md)\<`T`\>[]\>

#### Throws

If version range is invalid or table not found

***

### readByTimestamp()

> **readByTimestamp**(`startTime`, `endTime`): `Promise`\<[`DeltaCDCRecord`](DeltaCDCRecord.md)\<`T`\>[]\>

Defined in: src/cdc/index.ts:165

Read changes within a time range (inclusive).

#### Parameters

##### startTime

`Date`

##### endTime

`Date`

#### Returns

`Promise`\<[`DeltaCDCRecord`](DeltaCDCRecord.md)\<`T`\>[]\>

#### Throws

If time range is invalid

***

### subscribe()

> **subscribe**(`handler`, `options?`): () => `void`

Defined in: src/cdc/index.ts:173

Subscribe to changes as they occur.

#### Parameters

##### handler

(`record`) => `Promise`\<`void`\>

Async function called for each CDC record

##### options?

[`CDCSubscriptionOptions`](CDCSubscriptionOptions.md)\<`T`\>

Optional configuration including error callback

#### Returns

Unsubscribe function to stop receiving changes

> (): `void`

##### Returns

`void`
