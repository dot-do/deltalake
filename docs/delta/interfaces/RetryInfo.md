[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / RetryInfo

# Interface: RetryInfo

Defined in: [src/delta/retry.ts:25](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L25)

Information passed to the onRetry callback

## Properties

### attempt

> **attempt**: `number`

Defined in: [src/delta/retry.ts:27](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L27)

The retry attempt number (1-indexed, so first retry is 1)

***

### error

> **error**: `Error`

Defined in: [src/delta/retry.ts:29](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L29)

The error that triggered the retry

***

### delay

> **delay**: `number`

Defined in: [src/delta/retry.ts:31](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L31)

The delay in milliseconds before this retry
