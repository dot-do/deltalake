[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / ParsedStorageUrl

# Interface: ParsedStorageUrl

Defined in: src/storage/index.ts:377

Parsed storage URL result.
Contains the storage type and extracted configuration from the URL.

## Properties

### type

> **type**: `"s3"` \| `"filesystem"` \| `"r2"` \| `"memory"`

Defined in: src/storage/index.ts:379

Detected storage type

***

### path

> **path**: `string`

Defined in: src/storage/index.ts:381

Path within the storage (for filesystem, bucket prefix for cloud storage)

***

### bucket?

> `optional` **bucket**: `string`

Defined in: src/storage/index.ts:383

Bucket name (for s3:// and r2:// URLs)

***

### region?

> `optional` **region**: `string`

Defined in: src/storage/index.ts:385

AWS region (for s3:// URLs, extracted from hostname or defaulted to us-east-1)
