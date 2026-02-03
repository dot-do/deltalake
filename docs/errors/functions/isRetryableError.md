[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [errors](../README.md) / isRetryableError

# Function: isRetryableError()

> **isRetryableError**(`error`): `boolean`

Defined in: src/errors.ts:371

Check if an error is retryable.
Returns true for ConcurrencyError and any error with `retryable: true`.

## Parameters

### error

`unknown`

## Returns

`boolean`
