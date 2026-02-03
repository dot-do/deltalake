[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / FailureInfo

# Interface: FailureInfo

Defined in: [src/delta/retry.ts:47](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L47)

Information passed to the onFailure callback

## Properties

### error

> **error**: `Error`

Defined in: [src/delta/retry.ts:49](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L49)

The final error after all retries were exhausted

***

### attempts

> **attempts**: `number`

Defined in: [src/delta/retry.ts:51](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L51)

Total number of attempts made
