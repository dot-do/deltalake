[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / z85Decode

# Function: z85Decode()

> **z85Decode**(`encoded`): `Uint8Array`

Defined in: [src/delta/index.ts:163](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/index.ts#L163)

Decode a Z85-encoded string to bytes.

Z85 encodes 4 bytes as 5 ASCII characters. The input length must be a multiple of 5.

## Parameters

### encoded

`string`

Z85-encoded string

## Returns

`Uint8Array`

Decoded bytes as Uint8Array

## Throws

Error if input length is not a multiple of 5 or contains invalid characters
