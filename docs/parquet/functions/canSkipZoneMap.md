[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / canSkipZoneMap

# Function: canSkipZoneMap()

> **canSkipZoneMap**(`zoneMap`, `filter`): `boolean`

Defined in: src/parquet/index.ts:132

Check if a zone map can be skipped based on filter.
Returns true if the entire row group can be skipped (no matching rows).

Null/undefined handling:
- If zoneMap.min or zoneMap.max is null/undefined, we cannot determine bounds,
  so we conservatively return false (do not skip) to avoid missing matches.
- If filter.value is null/undefined, we cannot perform comparison,
  so we conservatively return false (do not skip).
- For 'between' operator, if filter.value2 is null/undefined, we return false.
- For 'in' operator, null/undefined values in the array are treated as non-comparable
  and won't contribute to skipping the row group.

## Parameters

### zoneMap

[`ZoneMap`](../interfaces/ZoneMap.md)

### filter

[`ZoneMapFilter`](../interfaces/ZoneMapFilter.md)

## Returns

`boolean`
