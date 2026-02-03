[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / applyProjection

# Function: applyProjection()

> **applyProjection**\<`T`\>(`docs`, `projection`): `Partial`\<`T`\>[]

Defined in: [src/query/index.ts:925](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/query/index.ts#L925)

Apply a projection to an array of documents.

Supports MongoDB-style projections:
- Array format: `['name', 'age']` - include only specified fields
- Object format (inclusion): `{ name: 1, age: 1 }` - include only specified fields
- Object format (exclusion): `{ password: 0 }` - exclude specified fields
- Nested fields: `['user.name']` or `{ 'user.name': 1 }`

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

## Example

```typescript
const users = [
  { name: 'Alice', age: 30, password: 'secret', profile: { tier: 'premium' } },
  { name: 'Bob', age: 25, password: 'secret', profile: { tier: 'free' } }
]

// Array inclusion projection
applyProjection(users, ['name', 'age'])
// => [{ name: 'Alice', age: 30 }, { name: 'Bob', age: 25 }]

// Object exclusion projection
applyProjection(users, { password: 0 })
// => [
//   { name: 'Alice', age: 30, profile: { tier: 'premium' } },
//   { name: 'Bob', age: 25, profile: { tier: 'free' } }
// ]

// Nested field projection
applyProjection(users, ['name', 'profile.tier'])
// => [
//   { name: 'Alice', profile: { tier: 'premium' } },
//   { name: 'Bob', profile: { tier: 'free' } }
// ]
```
