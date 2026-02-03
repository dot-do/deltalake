[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCSource

# Interface: CDCSource

Defined in: src/cdc/index.ts:69

Source metadata for CDC records.
Identifies the origin system and location of the change.

## Properties

### system

> **system**: `"mongolake"` \| `"kafkalake"` \| `"postgres"` \| `"mysql"` \| `"debezium"` \| `"deltalake"`

Defined in: src/cdc/index.ts:71

Source system identifier

***

### database?

> `optional` **database**: `string`

Defined in: src/cdc/index.ts:74

Database name

***

### collection?

> `optional` **collection**: `string`

Defined in: src/cdc/index.ts:77

Collection/table/topic name

***

### partition?

> `optional` **partition**: `number`

Defined in: src/cdc/index.ts:80

Partition/shard ID

***

### serverId?

> `optional` **serverId**: `string`

Defined in: src/cdc/index.ts:83

Server ID (for multi-master replication)
