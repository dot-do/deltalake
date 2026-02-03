[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / SuccessInfo

# Interface: SuccessInfo\<T\>

Defined in: src/delta/retry.ts:37

Information passed to the onSuccess callback

## Type Parameters

### T

`T` = `unknown`

## Properties

### result

> **result**: `T`

Defined in: src/delta/retry.ts:39

The result of the successful operation

***

### attempts

> **attempts**: `number`

Defined in: src/delta/retry.ts:41

Total number of attempts (including the successful one)
