[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCSubscriptionOptions

# Interface: CDCSubscriptionOptions\<T\>

Defined in: src/cdc/index.ts:140

Options for CDC subscription.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>

## Properties

### onError?

> `optional` **onError**: [`CDCSubscriptionErrorCallback`](../type-aliases/CDCSubscriptionErrorCallback.md)\<`T`\>

Defined in: src/cdc/index.ts:145

Error callback invoked when the handler throws.
The error is still caught to prevent cascade failures to other subscribers.
