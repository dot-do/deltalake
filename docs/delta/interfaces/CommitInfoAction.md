[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / CommitInfoAction

# Interface: CommitInfoAction

Defined in: [src/delta/types.ts:110](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L110)

## Properties

### commitInfo

> **commitInfo**: `object`

Defined in: [src/delta/types.ts:111](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/types.ts#L111)

#### timestamp

> **timestamp**: `number`

#### operation

> **operation**: `string`

#### operationParameters?

> `optional` **operationParameters**: `Record`\<`string`, `string`\>

#### readVersion?

> `optional` **readVersion**: `number`

#### isolationLevel?

> `optional` **isolationLevel**: `string`

#### isBlindAppend?

> `optional` **isBlindAppend**: `boolean`
