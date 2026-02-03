[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / Filter

# Type Alias: Filter\<T\>

> **Filter**\<`T`\> = \{ \[K in keyof T\]?: T\[K\] \| ComparisonOperators\<T\[K\]\> \} & [`LogicalOperators`](../interfaces/LogicalOperators.md)\<`T`\>

Defined in: src/query/index.ts:44

## Type Parameters

### T

`T` = `unknown`
