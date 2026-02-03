[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / decodeVariant

# Function: decodeVariant()

> **decodeVariant**(`encoded`): [`VariantValue`](../type-aliases/VariantValue.md)

Defined in: [src/parquet/variant.ts:447](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/variant.ts#L447)

Decode a Parquet VARIANT back to JavaScript value.

Reconstructs the original JavaScript value from the encoded VARIANT format.
The string dictionary in metadata is used to restore object key names.

## Parameters

### encoded

[`EncodedVariant`](../interfaces/EncodedVariant.md)

The encoded variant (metadata and value bytes)

## Returns

[`VariantValue`](../type-aliases/VariantValue.md)

The decoded JavaScript value

## Example

```typescript
// Encode and decode roundtrip
const original = { name: 'Alice', tags: ['admin', 'active'] }
const encoded = encodeVariant(original)
const decoded = decodeVariant(encoded)
// decoded === { name: 'Alice', tags: ['admin', 'active'] }

// Handles all supported types
const complex = encodeVariant({
  id: 12345n,                    // bigint
  created: new Date(),           // Date
  data: new Uint8Array([1,2,3]), // binary
  metadata: { version: 1 }       // nested object
})
const result = decodeVariant(complex)
```
