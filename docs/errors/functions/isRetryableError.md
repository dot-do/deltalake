[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [errors](../README.md) / isRetryableError

# Function: isRetryableError()

> **isRetryableError**(`error`): `boolean`

Defined in: [src/errors.ts:405](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/errors.ts#L405)

Check if an error is retryable.
Returns true for ConcurrencyError and any error with `retryable: true`.

## Parameters

### error

`unknown`

## Returns

`boolean`
