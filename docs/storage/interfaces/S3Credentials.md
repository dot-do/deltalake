[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / S3Credentials

# Interface: S3Credentials

Defined in: [src/storage/index.ts:429](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L429)

AWS S3 credentials for S3Storage.
If not provided, S3Storage will use environment credentials or IAM roles.

## Properties

### accessKeyId

> `readonly` **accessKeyId**: `string`

Defined in: [src/storage/index.ts:431](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L431)

AWS Access Key ID

***

### secretAccessKey

> `readonly` **secretAccessKey**: `string`

Defined in: [src/storage/index.ts:433](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L433)

AWS Secret Access Key
