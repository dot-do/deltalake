[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / parseRoaringBitmap

# Function: parseRoaringBitmap()

> **parseRoaringBitmap**(`data`): `Set`\<`number`\>

Defined in: src/delta/index.ts:218

Parse a serialized RoaringBitmap (or RoaringTreemap for 64-bit) and return the set of deleted row indices.

Delta Lake uses RoaringTreemap (64-bit extension) for deletion vectors.
The format is:
- Number of 32-bit buckets (as uint64, little-endian)
- For each bucket:
  - High 32 bits key (as uint32, little-endian)
  - Serialized 32-bit RoaringBitmap for low 32 bits

## Parameters

### data

`Uint8Array`

Raw bytes of the serialized deletion vector (after magic/size/checksum header)

## Returns

`Set`\<`number`\>

Set of row indices that are marked as deleted
