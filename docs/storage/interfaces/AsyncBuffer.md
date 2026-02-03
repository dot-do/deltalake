[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / AsyncBuffer

# Interface: AsyncBuffer

Defined in: src/storage/index.ts:62

AsyncBuffer interface for byte-range reads.
This is the interface expected by hyparquet for reading Parquet files.

The interface mimics ArrayBuffer's slice() method but returns a Promise,
allowing for lazy loading of file contents.

## Properties

### byteLength

> **byteLength**: `number`

Defined in: src/storage/index.ts:64

Total byte length of the file

## Methods

### slice()

> **slice**(`start`, `end?`): `ArrayBuffer` \| `Promise`\<`ArrayBuffer`\>

Defined in: src/storage/index.ts:73

Read a byte range [start, end) from the file.

#### Parameters

##### start

`number`

Starting byte offset (inclusive)

##### end?

`number`

Ending byte offset (exclusive), defaults to end of file

#### Returns

`ArrayBuffer` \| `Promise`\<`ArrayBuffer`\>

Promise resolving to the requested byte range as ArrayBuffer
