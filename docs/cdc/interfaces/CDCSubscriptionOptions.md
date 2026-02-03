[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCSubscriptionOptions

# Interface: CDCSubscriptionOptions\<T\>

Defined in: [src/cdc/index.ts:188](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L188)

Options for CDC subscription.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>

## Properties

### onError?

> `readonly` `optional` **onError**: [`CDCSubscriptionErrorCallback`](../type-aliases/CDCSubscriptionErrorCallback.md)\<`T`\>

Defined in: [src/cdc/index.ts:193](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L193)

Error callback invoked when the handler throws.
The error is still caught to prevent cascade failures to other subscribers.
