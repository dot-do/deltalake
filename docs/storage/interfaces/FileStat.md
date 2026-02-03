[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / FileStat

# Interface: FileStat

Defined in: [src/storage/index.ts:377](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L377)

File metadata returned by stat().

## Properties

### size

> **size**: `number`

Defined in: [src/storage/index.ts:379](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L379)

File size in bytes

***

### lastModified

> **lastModified**: `Date`

Defined in: [src/storage/index.ts:382](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L382)

Last modification time

***

### etag?

> `optional` **etag**: `string`

Defined in: [src/storage/index.ts:385](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L385)

Optional ETag/version identifier (present for R2/S3)
