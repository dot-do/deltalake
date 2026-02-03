[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / DeletionVectorDescriptor

# Interface: DeletionVectorDescriptor

Defined in: [src/delta/types.ts:24](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L24)

Descriptor for a deletion vector associated with a data file.

Deletion vectors mark individual rows as deleted without rewriting Parquet files.
They are stored as RoaringBitmap data indicating which row indices are deleted.

## See

https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors

## Properties

### storageType

> **storageType**: `"u"` \| `"p"` \| `"i"`

Defined in: [src/delta/types.ts:31](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L31)

Storage type indicator:
- 'u': UUID-based relative path (most common)
- 'p': Absolute path
- 'i': Inline (Base85-encoded bitmap data)

***

### pathOrInlineDv

> **pathOrInlineDv**: `string`

Defined in: [src/delta/types.ts:38](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L38)

For storageType 'u': Base85-encoded UUID (and optional prefix)
For storageType 'p': Absolute file path
For storageType 'i': Base85-encoded RoaringBitmap data

***

### offset?

> `optional` **offset**: `number`

Defined in: [src/delta/types.ts:44](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L44)

Byte offset within the deletion vector file where this DV's data begins.
Only applicable for storageType 'u' and 'p'.

***

### sizeInBytes

> **sizeInBytes**: `number`

Defined in: [src/delta/types.ts:49](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L49)

Size of the serialized deletion vector in bytes (before Base85 encoding if inline).

***

### cardinality

> **cardinality**: `number`

Defined in: [src/delta/types.ts:54](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L54)

Number of rows marked as deleted by this deletion vector.
