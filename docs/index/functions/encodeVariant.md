[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [index](../README.md) / encodeVariant

# Function: encodeVariant()

> **encodeVariant**(`value`): [`EncodedVariant`](../interfaces/EncodedVariant.md)

Defined in: [src/parquet/variant.ts:80](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/variant.ts#L80)

Encode a JavaScript value as a Parquet VARIANT.

VARIANT is a semi-structured data type that can store any JSON-compatible value
in a self-describing binary format. This enables storing heterogeneous data
(objects with varying schemas) efficiently in Parquet files.

Supported types:
- Primitives: null, boolean, number, bigint, string
- Dates: Date objects (stored as timestamp with microsecond precision)
- Binary: Uint8Array
- Collections: Arrays and objects (nested structures supported)

## Parameters

### value

[`VariantValue`](../type-aliases/VariantValue.md)

The JavaScript value to encode

## Returns

[`EncodedVariant`](../interfaces/EncodedVariant.md)

Encoded variant with metadata (string dictionary) and value bytes

## Example

```typescript
// Encode a simple object
const encoded = encodeVariant({
  name: 'Alice',
  age: 30,
  active: true
})
// encoded.metadata contains the string dictionary (keys: 'name', 'age', 'active')
// encoded.value contains the self-describing binary representation

// Encode nested structures
const nested = encodeVariant({
  user: { name: 'Bob', scores: [95, 87, 92] },
  timestamp: new Date()
})

// Decode back to JavaScript
const decoded = decodeVariant(encoded) // { name: 'Alice', age: 30, active: true }
```
