[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / S3ClientLike

# Interface: S3ClientLike

Defined in: [src/storage/index.ts:1477](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1477)

S3Client interface - matches AWS SDK v3 S3Client.send() pattern.
Uses discriminated union with conditional type for type-safe command/response mapping.
This allows for easy mocking in tests while maintaining full type safety.

The send() method uses the command's _type discriminator to infer the correct
response type at compile time, eliminating the need for type assertions.

## Methods

### send()

> **send**\<`T`\>(`command`): `Promise`\<`S3ResponseMap`\[`T`\[`"_type"`\]\]\>

Defined in: [src/storage/index.ts:1478](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L1478)

#### Type Parameters

##### T

`T` *extends* `S3Command`

#### Parameters

##### command

`T`

#### Returns

`Promise`\<`S3ResponseMap`\[`T`\[`"_type"`\]\]\>
