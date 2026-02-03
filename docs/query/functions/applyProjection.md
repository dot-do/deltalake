[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / applyProjection

# Function: applyProjection()

> **applyProjection**\<`T`\>(`docs`, `projection`): `Partial`\<`T`\>[]

Defined in: src/query/index.ts:579

Apply a projection to an array of documents

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### docs

`T`[]

Array of documents

### projection

[`Projection`](../type-aliases/Projection.md)\<`T`\>

The projection specification

## Returns

`Partial`\<`T`\>[]

Array of documents with only the projected fields
