[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCDeltaTable

# Interface: CDCDeltaTable\<T\>

Defined in: src/cdc/index.ts:185

CDC-enabled Delta Table interface.
Extends standard Delta Table operations with CDC tracking.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\> = `Record`\<`string`, `unknown`\>

The row data type

## Methods

### setCDCEnabled()

> **setCDCEnabled**(`enabled`): `Promise`\<`void`\>

Defined in: src/cdc/index.ts:187

Enable or disable CDC for this table

#### Parameters

##### enabled

`boolean`

#### Returns

`Promise`\<`void`\>

***

### isCDCEnabled()

> **isCDCEnabled**(): `Promise`\<`boolean`\>

Defined in: src/cdc/index.ts:190

Check if CDC is enabled

#### Returns

`Promise`\<`boolean`\>

***

### getCDCReader()

> **getCDCReader**(): [`CDCReader`](CDCReader.md)\<`T`\>

Defined in: src/cdc/index.ts:193

Get CDC reader for this table

#### Returns

[`CDCReader`](CDCReader.md)\<`T`\>

***

### write()

> **write**(`rows`): `Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

Defined in: src/cdc/index.ts:196

Write data and generate CDC records

#### Parameters

##### rows

`T`[]

#### Returns

`Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

***

### update()

> **update**(`filter`, `updates`): `Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

Defined in: src/cdc/index.ts:199

Update rows and generate CDC records with before/after images

#### Parameters

##### filter

`Record`\<`string`, `unknown`\>

##### updates

`Partial`\<`T`\>

#### Returns

`Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

***

### deleteRows()

> **deleteRows**(`filter`): `Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

Defined in: src/cdc/index.ts:202

Delete rows and generate CDC records

#### Parameters

##### filter

`Record`\<`string`, `unknown`\>

#### Returns

`Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

***

### merge()

> **merge**(`rows`, `matchCondition`, `whenMatched?`, `whenNotMatched?`): `Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>

Defined in: src/cdc/index.ts:211

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

`Promise`\<[`DeltaCommit`](../../delta/interfaces/DeltaCommit.md)\>
