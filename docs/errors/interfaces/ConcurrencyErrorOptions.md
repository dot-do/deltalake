[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [errors](../README.md) / ConcurrencyErrorOptions

# Interface: ConcurrencyErrorOptions

Defined in: [src/errors.ts:183](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/errors.ts#L183)

Options for creating a ConcurrencyError with version information.

## Properties

### expectedVersion?

> `optional` **expectedVersion**: `number`

Defined in: [src/errors.ts:185](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/errors.ts#L185)

The expected version (what the writer thought the version was)

***

### actualVersion?

> `optional` **actualVersion**: `number`

Defined in: [src/errors.ts:187](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/errors.ts#L187)

The actual version found (current version in storage)
