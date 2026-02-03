[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCSubscriptionErrorCallback

# Type Alias: CDCSubscriptionErrorCallback()\<T\>

> **CDCSubscriptionErrorCallback**\<`T`\> = (`error`, `record`) => `void`

Defined in: src/cdc/index.ts:132

Error callback for CDC subscription handler failures.
Called when a subscriber's handler throws an error.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>

## Parameters

### error

`Error`

### record

[`DeltaCDCRecord`](../interfaces/DeltaCDCRecord.md)\<`T`\>

## Returns

`void`
