[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / matchesFilter

# Function: matchesFilter()

> **matchesFilter**\<`T`\>(`doc`, `filter`): `boolean`

Defined in: [src/query/index.ts:255](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/query/index.ts#L255)

Check if a document matches a MongoDB-style filter.

Supports comparison operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex)
and logical operators ($and, $or, $not, $nor). Also supports nested field access
using dot notation.

## Type Parameters

### T

`T` *extends* `Record`\<`string`, `unknown`\>

## Parameters

### doc

`T`

The document to check

### filter

[`Filter`](../type-aliases/Filter.md)\<`T`\>

MongoDB-style filter object

## Returns

`boolean`

True if the document matches the filter

## Example

```typescript
// Simple equality
matchesFilter({ name: 'Alice', age: 30 }, { name: 'Alice' }) // true

// Comparison operators
matchesFilter({ age: 30 }, { age: { $gte: 18 } }) // true
matchesFilter({ status: 'active' }, { status: { $in: ['active', 'pending'] } }) // true

// Logical operators
matchesFilter(
  { role: 'admin', active: true },
  { $and: [{ role: 'admin' }, { active: true }] }
) // true

// Nested field access
matchesFilter(
  { user: { profile: { age: 25 } } },
  { 'user.profile.age': { $gte: 18 } }
) // true

// Regex matching
matchesFilter({ email: 'alice@example.com' }, { email: { $regex: '@example\\.com$' } }) // true
```
