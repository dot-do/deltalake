[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCProducerOptions

# Interface: CDCProducerOptions

Defined in: [src/cdc/index.ts:280](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L280)

Options for creating a CDC producer.

## Properties

### source

> `readonly` **source**: `Omit`\<[`CDCSource`](CDCSource.md), `"system"`\>

Defined in: [src/cdc/index.ts:282](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L282)

Source metadata (excluding system, which can be specified separately)

***

### system?

> `readonly` `optional` **system**: `"mongolake"` \| `"kafkalake"` \| `"postgres"` \| `"mysql"` \| `"debezium"` \| `"deltalake"`

Defined in: [src/cdc/index.ts:284](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L284)

Source system identifier (defaults to 'kafkalake')
