[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / FileStat

# Interface: FileStat

Defined in: src/storage/index.ts:314

File metadata returned by stat().

## Properties

### size

> **size**: `number`

Defined in: src/storage/index.ts:316

File size in bytes

***

### lastModified

> **lastModified**: `Date`

Defined in: src/storage/index.ts:319

Last modification time

***

### etag?

> `optional` **etag**: `string`

Defined in: src/storage/index.ts:322

Optional ETag/version identifier (present for R2/S3)
