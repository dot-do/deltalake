[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / ProtocolAction

# Interface: ProtocolAction

Defined in: [src/delta/types.ts:99](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L99)

## Properties

### protocol

> **protocol**: `object`

Defined in: [src/delta/types.ts:100](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L100)

#### minReaderVersion

> **minReaderVersion**: `number`

#### minWriterVersion

> **minWriterVersion**: `number`

#### readerFeatures?

> `optional` **readerFeatures**: `string`[]

Reader features required for protocol version 3+

#### writerFeatures?

> `optional` **writerFeatures**: `string`[]

Writer features required for protocol version 7+
