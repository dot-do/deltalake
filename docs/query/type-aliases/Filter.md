[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / Filter

# Type Alias: Filter\<T\>

> **Filter**\<`T`\> = \{ \[K in keyof T\]?: T\[K\] \| ComparisonOperators\<T\[K\]\> \} & [`LogicalOperators`](../interfaces/LogicalOperators.md)\<`T`\>

Defined in: [src/query/index.ts:82](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/query/index.ts#L82)

MongoDB-style filter for querying records.

## Type Parameters

### T

`T` = `Record`\<`string`, `unknown`\>
