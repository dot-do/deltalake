[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [errors](../README.md) / ConcurrencyErrorOptions

# Interface: ConcurrencyErrorOptions

Defined in: src/errors.ts:171

Options for creating a ConcurrencyError with version information.

## Properties

### expectedVersion?

> `optional` **expectedVersion**: `number`

Defined in: src/errors.ts:173

The expected version (what the writer thought the version was)

***

### actualVersion?

> `optional` **actualVersion**: `number`

Defined in: src/errors.ts:175

The actual version found (current version in storage)
