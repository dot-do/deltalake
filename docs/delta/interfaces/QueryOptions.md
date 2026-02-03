[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / QueryOptions

# Interface: QueryOptions\<T\>

Defined in: [src/delta/types.ts:178](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L178)

## Type Parameters

### T

`T` = `unknown`

## Properties

### version?

> `readonly` `optional` **version**: `number`

Defined in: [src/delta/types.ts:183](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L183)

Query at a specific table version (time travel)
If provided, the query will use the snapshot at this version

***

### snapshot?

> `readonly` `optional` **snapshot**: [`DeltaSnapshot`](../../index/interfaces/DeltaSnapshot.md)

Defined in: [src/delta/types.ts:188](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L188)

Use a pre-fetched snapshot for the query
Useful for consistent reads across multiple queries

***

### projection?

> `readonly` `optional` **projection**: [`Projection`](../../query/type-aliases/Projection.md)\<`T`\>

Defined in: [src/delta/types.ts:196](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L196)

Field projection to return only specified fields
- Array format: ['name', 'age'] - include only these fields
- Object format: { name: 1, age: 1 } - include only these fields
- Exclusion: { password: 0 } - exclude these fields
- Nested fields supported: 'address.city' or { 'address.city': 1 }
