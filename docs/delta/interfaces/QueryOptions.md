[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / QueryOptions

# Interface: QueryOptions\<T\>

Defined in: src/delta/index.ts:775

## Type Parameters

### T

`T` = `unknown`

## Properties

### version?

> `optional` **version**: `number`

Defined in: src/delta/index.ts:780

Query at a specific table version (time travel)
If provided, the query will use the snapshot at this version

***

### snapshot?

> `optional` **snapshot**: [`DeltaSnapshot`](DeltaSnapshot.md)

Defined in: src/delta/index.ts:785

Use a pre-fetched snapshot for the query
Useful for consistent reads across multiple queries

***

### projection?

> `optional` **projection**: [`Projection`](../../query/type-aliases/Projection.md)\<`T`\>

Defined in: src/delta/index.ts:793

Field projection to return only specified fields
- Array format: ['name', 'age'] - include only these fields
- Object format: { name: 1, age: 1 } - include only these fields
- Exclusion: { password: 0 } - exclude these fields
- Nested fields supported: 'address.city' or { 'address.city': 1 }
