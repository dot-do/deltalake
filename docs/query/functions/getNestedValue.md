[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [query](../README.md) / getNestedValue

# Function: getNestedValue()

> **getNestedValue**(`obj`, `path`): `unknown`

Defined in: src/query/index.ts:172

Get a value from an object using a dot-notation path.
Optimized for common case of non-nested paths.

## Parameters

### obj

`Record`\<`string`, `unknown`\>

The object to get the value from

### path

`string`

The dot-notation path (e.g., 'user.address.city')

## Returns

`unknown`

The value at the path, or undefined if not found

## Example

```ts
getNestedValue({ user: { name: 'Alice' } }, 'user.name') // 'Alice'
getNestedValue({ a: { b: { c: 1 } } }, 'a.b.c') // 1
getNestedValue({ a: 1 }, 'a.b') // undefined
```
