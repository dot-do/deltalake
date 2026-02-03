[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / StorageSnapshot

# Interface: StorageSnapshot

Defined in: [src/storage/index.ts:2045](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2045)

Snapshot of storage state for testing (returned by snapshot()).

## Properties

### files

> `readonly` **files**: `Map`\<`string`, `Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: [src/storage/index.ts:2047](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2047)

Map of file paths to their contents

***

### versions

> `readonly` **versions**: `Map`\<`string`, `string`\>

Defined in: [src/storage/index.ts:2049](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L2049)

Map of file paths to their version strings
