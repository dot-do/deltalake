[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCProducerOptions

# Interface: CDCProducerOptions

Defined in: src/cdc/index.ts:226

Options for creating a CDC producer.

## Properties

### source

> **source**: `Omit`\<[`CDCSource`](CDCSource.md), `"system"`\>

Defined in: src/cdc/index.ts:228

Source metadata (excluding system, which can be specified separately)

***

### system?

> `optional` **system**: `"mongolake"` \| `"kafkalake"` \| `"postgres"` \| `"mysql"` \| `"debezium"` \| `"deltalake"`

Defined in: src/cdc/index.ts:230

Source system identifier (defaults to 'kafkalake')
