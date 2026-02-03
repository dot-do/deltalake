[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [compaction](../README.md) / ClusteringMetrics

# Interface: ClusteringMetrics

Defined in: src/compaction/index.ts:215

Metrics returned from clustering operation

## Properties

### columnsUsed

> **columnsUsed**: `string`[]

Defined in: src/compaction/index.ts:217

Columns used for clustering

***

### rowsProcessed

> **rowsProcessed**: `number`

Defined in: src/compaction/index.ts:220

Number of rows processed

***

### filesCreated

> **filesCreated**: `number`

Defined in: src/compaction/index.ts:223

Number of files created

***

### dataskippingImprovement

> **dataskippingImprovement**: `number`

Defined in: src/compaction/index.ts:226

Estimated improvement in data skipping (0-1)

***

### avgZoneWidth

> **avgZoneWidth**: `number`

Defined in: src/compaction/index.ts:229

Average width of zone maps

***

### clusteringRatio

> **clusteringRatio**: `number`

Defined in: src/compaction/index.ts:232

Clustering ratio (0-1)

***

### estimatedSkipRate

> **estimatedSkipRate**: `number`

Defined in: src/compaction/index.ts:235

Estimated skip rate for typical queries

***

### zOrderCurveComputed

> **zOrderCurveComputed**: `boolean`

Defined in: src/compaction/index.ts:238

Whether Z-order curve was computed

***

### curveType

> **curveType**: `string`

Defined in: src/compaction/index.ts:241

Type of curve used

***

### zoneMapStats

> **zoneMapStats**: `object`

Defined in: src/compaction/index.ts:244

Zone map statistics

#### avgZoneWidth

> **avgZoneWidth**: `number`

#### minZoneWidth

> **minZoneWidth**: `number`

#### maxZoneWidth

> **maxZoneWidth**: `number`
