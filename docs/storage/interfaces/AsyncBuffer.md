[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / AsyncBuffer

# Interface: AsyncBuffer

Defined in: [src/storage/index.ts:65](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L65)

AsyncBuffer interface for byte-range reads.
This is the interface expected by hyparquet for reading Parquet files.

The interface mimics ArrayBuffer's slice() method but returns a Promise,
allowing for lazy loading of file contents.

## Properties

### byteLength

> **byteLength**: `number`

Defined in: [src/storage/index.ts:67](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L67)

Total byte length of the file

## Methods

### slice()

> **slice**(`start`, `end?`): `ArrayBuffer` \| `Promise`\<`ArrayBuffer`\>

Defined in: [src/storage/index.ts:76](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/storage/index.ts#L76)

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
