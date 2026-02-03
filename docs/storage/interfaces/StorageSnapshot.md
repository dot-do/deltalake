[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / StorageSnapshot

# Interface: StorageSnapshot

Defined in: src/storage/index.ts:1791

Snapshot of storage state for testing (returned by snapshot()).

## Properties

### files

> `readonly` **files**: `Map`\<`string`, `Uint8Array`\<`ArrayBufferLike`\>\>

Defined in: src/storage/index.ts:1793

Map of file paths to their contents

***

### versions

> `readonly` **versions**: `Map`\<`string`, `string`\>

Defined in: src/storage/index.ts:1795

Map of file paths to their version strings
