[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCSource

# Interface: CDCSource

Defined in: [src/cdc/index.ts:78](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L78)

Source metadata for CDC records.
Identifies the origin system and location of the change.

## Properties

### system

> **system**: `"mongolake"` \| `"kafkalake"` \| `"postgres"` \| `"mysql"` \| `"debezium"` \| `"deltalake"`

Defined in: [src/cdc/index.ts:80](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L80)

Source system identifier

***

### database?

> `optional` **database**: `string`

Defined in: [src/cdc/index.ts:83](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L83)

Database name

***

### collection?

> `optional` **collection**: `string`

Defined in: [src/cdc/index.ts:86](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L86)

Collection/table/topic name

***

### partition?

> `optional` **partition**: `number`

Defined in: [src/cdc/index.ts:89](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L89)

Partition/shard ID

***

### serverId?

> `optional` **serverId**: `string`

Defined in: [src/cdc/index.ts:92](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L92)

Server ID (for multi-master replication)
