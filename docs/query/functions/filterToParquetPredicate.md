[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / filterToParquetPredicate

# Function: filterToParquetPredicate()

> **filterToParquetPredicate**\<`T`\>(`filter`): [`ZoneMapFilter`](../../parquet/interfaces/ZoneMapFilter.md)[]

Defined in: [src/query/index.ts:556](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/query/index.ts#L556)

Convert a MongoDB-style filter to Parquet zone map filters.

This enables predicate pushdown for Parquet files by translating
MongoDB-style filters into zone map filters that can skip row groups
that don't contain matching data.

Only pushable predicates are converted; logical operators like $or, $not,
and $nor are skipped as they cannot be efficiently pushed to zone maps.

## Type Parameters

### T

`T`

## Parameters

### filter

[`Filter`](../type-aliases/Filter.md)\<`T`\>

MongoDB-style filter object

## Returns

[`ZoneMapFilter`](../../parquet/interfaces/ZoneMapFilter.md)[]

Array of zone map filters for predicate pushdown

## Example

```typescript
// Simple equality -> eq predicate
filterToParquetPredicate({ status: 'active' })
// => [{ column: 'status', operator: 'eq', value: 'active' }]

// Range query -> gte and lte predicates with 'between' optimization
filterToParquetPredicate({ age: { $gte: 18, $lte: 65 } })
// => [
//   { column: 'age', operator: 'gte', value: 18 },
//   { column: 'age', operator: 'lte', value: 65 },
//   { column: 'age', operator: 'between', value: 18, value2: 65 }
// ]

// $in operator -> in predicate
filterToParquetPredicate({ role: { $in: ['admin', 'moderator'] } })
// => [{ column: 'role', operator: 'in', value: ['admin', 'moderator'] }]

// $and combines predicates from all subfilters
filterToParquetPredicate({
  $and: [{ status: 'active' }, { age: { $gte: 18 } }]
})
// => [
//   { column: 'status', operator: 'eq', value: 'active' },
//   { column: 'age', operator: 'gte', value: 18 }
// ]
```
