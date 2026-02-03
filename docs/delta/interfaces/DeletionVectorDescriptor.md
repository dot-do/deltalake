[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / DeletionVectorDescriptor

# Interface: DeletionVectorDescriptor

Defined in: src/delta/index.ts:553

Descriptor for a deletion vector associated with a data file.

Deletion vectors mark individual rows as deleted without rewriting Parquet files.
They are stored as RoaringBitmap data indicating which row indices are deleted.

## See

https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors

## Properties

### storageType

> **storageType**: `"u"` \| `"p"` \| `"i"`

Defined in: src/delta/index.ts:560

Storage type indicator:
- 'u': UUID-based relative path (most common)
- 'p': Absolute path
- 'i': Inline (Base85-encoded bitmap data)

***

### pathOrInlineDv

> **pathOrInlineDv**: `string`

Defined in: src/delta/index.ts:567

For storageType 'u': Base85-encoded UUID (and optional prefix)
For storageType 'p': Absolute file path
For storageType 'i': Base85-encoded RoaringBitmap data

***

### offset?

> `optional` **offset**: `number`

Defined in: src/delta/index.ts:573

Byte offset within the deletion vector file where this DV's data begins.
Only applicable for storageType 'u' and 'p'.

***

### sizeInBytes

> **sizeInBytes**: `number`

Defined in: src/delta/index.ts:578

Size of the serialized deletion vector in bytes (before Base85 encoding if inline).

***

### cardinality

> **cardinality**: `number`

Defined in: src/delta/index.ts:583

Number of rows marked as deleted by this deletion vector.
