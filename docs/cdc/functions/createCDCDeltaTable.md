[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / createCDCDeltaTable

# Function: createCDCDeltaTable()

> **createCDCDeltaTable**\<`T`\>(`storage`, `tablePath`): [`CDCDeltaTable`](../interfaces/CDCDeltaTable.md)\<`T`\>

Defined in: src/cdc/index.ts:1298

Create a CDC-enabled Delta table

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### storage

[`StorageBackend`](../../storage/interfaces/StorageBackend.md)

### tablePath

`string`

## Returns

[`CDCDeltaTable`](../interfaces/CDCDeltaTable.md)\<`T`\>
