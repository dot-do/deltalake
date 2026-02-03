[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / z85DecodeUuid

# Function: z85DecodeUuid()

> **z85DecodeUuid**(`pathOrInlineDv`): `string`

Defined in: [src/delta/index.ts:203](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/index.ts#L203)

Convert a Z85-encoded UUID string to a UUID string format.

Delta Lake uses Z85 to encode 16-byte UUIDs as 20-character strings.
The pathOrInlineDv field may have an optional prefix before the UUID.

## Parameters

### pathOrInlineDv

`string`

The pathOrInlineDv field from a deletion vector descriptor

## Returns

`string`

The UUID in standard format (8-4-4-4-12 hex)
