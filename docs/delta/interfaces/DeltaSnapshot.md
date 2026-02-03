[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / DeltaSnapshot

# Interface: DeltaSnapshot

Defined in: src/delta/index.ts:671

## Properties

### version

> **version**: `number`

Defined in: src/delta/index.ts:672

***

### files

> **files**: `object`[]

Defined in: src/delta/index.ts:673

#### path

> **path**: `string`

#### size

> **size**: `number`

#### modificationTime

> **modificationTime**: `number`

#### dataChange

> **dataChange**: `boolean`

#### partitionValues?

> `optional` **partitionValues**: `Record`\<`string`, `string`\>

#### stats?

> `optional` **stats**: `string`

#### tags?

> `optional` **tags**: `Record`\<`string`, `string`\>

#### deletionVector?

> `optional` **deletionVector**: [`DeletionVectorDescriptor`](DeletionVectorDescriptor.md)

Deletion vector descriptor if this file has soft-deleted rows

***

### metadata?

> `optional` **metadata**: `object`

Defined in: src/delta/index.ts:674

#### id

> **id**: `string`

#### name?

> `optional` **name**: `string`

#### description?

> `optional` **description**: `string`

#### format

> **format**: `object`

##### format.provider

> **provider**: `string`

##### format.options?

> `optional` **options**: `Record`\<`string`, `string`\>

#### schemaString

> **schemaString**: `string`

#### partitionColumns

> **partitionColumns**: `string`[]

#### configuration?

> `optional` **configuration**: `Record`\<`string`, `string`\>

#### createdTime?

> `optional` **createdTime**: `number`

***

### protocol?

> `optional` **protocol**: `object`

Defined in: src/delta/index.ts:675

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
