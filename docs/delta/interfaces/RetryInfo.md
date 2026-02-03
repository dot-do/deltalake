[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / RetryInfo

# Interface: RetryInfo

Defined in: src/delta/retry.ts:25

Information passed to the onRetry callback

## Properties

### attempt

> **attempt**: `number`

Defined in: src/delta/retry.ts:27

The retry attempt number (1-indexed, so first retry is 1)

***

### error

> **error**: `Error`

Defined in: src/delta/retry.ts:29

The error that triggered the retry

***

### delay

> **delay**: `number`

Defined in: src/delta/retry.ts:31

The delay in milliseconds before this retry
