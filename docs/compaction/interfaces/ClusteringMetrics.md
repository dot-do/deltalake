[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / ClusteringMetrics

# Interface: ClusteringMetrics

Defined in: [src/compaction/index.ts:288](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L288)

Metrics returned from clustering operation

## Properties

### columnsUsed

> **columnsUsed**: `string`[]

Defined in: [src/compaction/index.ts:290](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L290)

Columns used for clustering

***

### rowsProcessed

> **rowsProcessed**: `number`

Defined in: [src/compaction/index.ts:293](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L293)

Number of rows processed

***

### filesCreated

> **filesCreated**: `number`

Defined in: [src/compaction/index.ts:296](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L296)

Number of files created

***

### dataskippingImprovement

> **dataskippingImprovement**: `number`

Defined in: [src/compaction/index.ts:299](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L299)

Estimated improvement in data skipping (0-1)

***

### avgZoneWidth

> **avgZoneWidth**: `number`

Defined in: [src/compaction/index.ts:302](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L302)

Average width of zone maps

***

### clusteringRatio

> **clusteringRatio**: `number`

Defined in: [src/compaction/index.ts:305](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L305)

Clustering ratio (0-1)

***

### estimatedSkipRate

> **estimatedSkipRate**: `number`

Defined in: [src/compaction/index.ts:308](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L308)

Estimated skip rate for typical queries

***

### zOrderCurveComputed

> **zOrderCurveComputed**: `boolean`

Defined in: [src/compaction/index.ts:311](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L311)

Whether Z-order curve was computed

***

### curveType

> **curveType**: `string`

Defined in: [src/compaction/index.ts:314](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L314)

Type of curve used

***

### zoneMapStats

> **zoneMapStats**: `object`

Defined in: [src/compaction/index.ts:317](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/compaction/index.ts#L317)

Zone map statistics

#### avgZoneWidth

> **avgZoneWidth**: `number`

#### minZoneWidth

> **minZoneWidth**: `number`

#### maxZoneWidth

> **maxZoneWidth**: `number`
