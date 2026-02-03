[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / ParsedStorageUrl

# Interface: ParsedStorageUrl

Defined in: [src/storage/index.ts:446](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L446)

Parsed storage URL result.
Contains the storage type and extracted configuration from the URL.

## Properties

### type

> **type**: `"s3"` \| `"filesystem"` \| `"r2"` \| `"memory"`

Defined in: [src/storage/index.ts:448](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L448)

Detected storage type

***

### path

> **path**: `string`

Defined in: [src/storage/index.ts:450](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L450)

Path within the storage (for filesystem, bucket prefix for cloud storage)

***

### bucket?

> `optional` **bucket**: `string`

Defined in: [src/storage/index.ts:452](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L452)

Bucket name (for s3:// and r2:// URLs)

***

### region?

> `optional` **region**: `string`

Defined in: [src/storage/index.ts:454](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L454)

AWS region (for s3:// URLs, extracted from hostname or defaulted to us-east-1)
