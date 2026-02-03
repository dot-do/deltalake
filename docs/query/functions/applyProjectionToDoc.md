[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / applyProjectionToDoc

# Function: applyProjectionToDoc()

> **applyProjectionToDoc**\<`T`\>(`doc`, `projection`): `Partial`\<`T`\>

Defined in: [src/query/index.ts:768](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/query/index.ts#L768)

Apply a projection to a single document

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### doc

`T`

The document to apply projection to

### projection

[`Projection`](../type-aliases/Projection.md)\<`T`\>

The projection specification

## Returns

`Partial`\<`T`\>

A new document with only the projected fields

## Example

```ts
// Include only name and age
applyProjectionToDoc({ name: 'Alice', age: 30, email: 'a@b.com' }, ['name', 'age'])
// Returns: { name: 'Alice', age: 30 }

// Nested field projection
applyProjectionToDoc({ user: { name: 'Alice', age: 30 } }, ['user.name'])
// Returns: { user: { name: 'Alice' } }
```
