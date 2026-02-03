[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / FailureInfo

# Interface: FailureInfo

Defined in: src/delta/retry.ts:47

Information passed to the onFailure callback

## Properties

### error

> **error**: `Error`

Defined in: src/delta/retry.ts:49

The final error after all retries were exhausted

***

### attempts

> **attempts**: `number`

Defined in: src/delta/retry.ts:51

Total number of attempts made
