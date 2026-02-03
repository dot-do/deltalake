[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCDeltaTable

# Interface: CDCDeltaTable\<T\>

Defined in: [src/cdc/index.ts:237](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L237)

CDC-enabled Delta Table interface.
Extends standard Delta Table operations with CDC tracking.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\> = `Record`\<`string`, `unknown`\>

The row data type

## Methods

### setCDCEnabled()

> **setCDCEnabled**(`enabled`): `Promise`\<`void`\>

Defined in: [src/cdc/index.ts:239](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L239)

Enable or disable CDC for this table

#### Parameters

##### enabled

`boolean`

#### Returns

`Promise`\<`void`\>

***

### isCDCEnabled()

> **isCDCEnabled**(): `Promise`\<`boolean`\>

Defined in: [src/cdc/index.ts:242](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L242)

Check if CDC is enabled

#### Returns

`Promise`\<`boolean`\>

***

### getCDCReader()

> **getCDCReader**(): [`CDCReader`](CDCReader.md)\<`T`\>

Defined in: [src/cdc/index.ts:245](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L245)

Get CDC reader for this table

#### Returns

[`CDCReader`](CDCReader.md)\<`T`\>

***

### write()

> **write**(`rows`): `Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

Defined in: [src/cdc/index.ts:248](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L248)

Write data and generate CDC records

#### Parameters

##### rows

`T`[]

#### Returns

`Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

***

### update()

> **update**(`filter`, `updates`): `Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

Defined in: [src/cdc/index.ts:251](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L251)

Update rows and generate CDC records with before/after images

#### Parameters

##### filter

`Record`\<`string`, `unknown`\>

##### updates

`Partial`\<`T`\>

#### Returns

`Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

***

### deleteRows()

> **deleteRows**(`filter`): `Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

Defined in: [src/cdc/index.ts:254](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L254)

Delete rows and generate CDC records

#### Parameters

##### filter

`Record`\<`string`, `unknown`\>

#### Returns

`Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

***

### merge()

> **merge**(`rows`, `matchCondition`, `whenMatched?`, `whenNotMatched?`): `Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>

Defined in: [src/cdc/index.ts:263](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L263)

Merge data (upsert) with CDC tracking.

#### Parameters

##### rows

`T`[]

Rows to merge

##### matchCondition

(`existing`, `incoming`) => `boolean`

Function to match existing rows

##### whenMatched?

(`existing`, `incoming`) => `T` \| `null`

Transform for matched rows (return null to delete)

##### whenNotMatched?

(`incoming`) => `T` \| `null`

Transform for unmatched rows (return null to skip)

#### Returns

`Promise`\<[`DeltaCommit`](../../index/interfaces/DeltaCommit.md)\>
