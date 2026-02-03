[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / WriteOptions

# Interface: WriteOptions

Defined in: src/delta/index.ts:800

## Properties

### partitionColumns?

> `optional` **partitionColumns**: `string`[]

Defined in: src/delta/index.ts:809

Partition columns for this write operation.
If specified, rows will be grouped by these columns and written to
partitioned paths: table/col=value/part-*.parquet

For subsequent writes, partition columns should match the table's
existing partition configuration.
