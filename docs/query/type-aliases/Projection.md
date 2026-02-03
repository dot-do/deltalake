[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / Projection

# Type Alias: Projection\<T\>

> **Projection**\<`T`\> = (keyof `T` \| `string`)[] \| \{ \[K in keyof T \| string\]?: 1 \| 0 \}

Defined in: src/query/index.ts:59

MongoDB-style projection specification
- Array of field names: ['name', 'age'] - include only these fields
- Object with 1 values: { name: 1, age: 1 } - include only these fields
- Object with 0 values: { password: 0 } - exclude these fields
- Supports nested fields: 'address.city' or { 'address.city': 1 }

## Type Parameters

### T

`T` = `unknown`
