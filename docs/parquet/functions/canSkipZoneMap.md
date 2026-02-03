[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [parquet](../README.md) / canSkipZoneMap

# Function: canSkipZoneMap()

> **canSkipZoneMap**(`zoneMap`, `filter`): `boolean`

Defined in: [src/parquet/index.ts:160](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/parquet/index.ts#L160)

Check if a zone map can be skipped based on filter.

Zone maps store min/max statistics for each column in a row group.
This function determines if a row group can be safely skipped because
no rows could possibly match the filter based on these statistics.

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

Zone map statistics for a column in a row group

### filter

[`ZoneMapFilter`](../interfaces/ZoneMapFilter.md)

Filter predicate to check

## Returns

`boolean`

True if the row group can be skipped (no matching rows possible)

## Example

```typescript
// Zone map shows age column has values between 25 and 45
const zoneMap = { column: 'age', min: 25, max: 45, nullCount: 0 }

// Can skip: looking for age < 20, but min is 25
canSkipZoneMap(zoneMap, { column: 'age', operator: 'lt', value: 20 }) // true

// Cannot skip: looking for age > 30, some values in [25,45] are > 30
canSkipZoneMap(zoneMap, { column: 'age', operator: 'gt', value: 30 }) // false

// Can skip: looking for age = 50, but max is 45
canSkipZoneMap(zoneMap, { column: 'age', operator: 'eq', value: 50 }) // true

// Can skip: looking for age in [10, 15, 20], all outside [25, 45]
canSkipZoneMap(zoneMap, { column: 'age', operator: 'in', value: [10, 15, 20] }) // true

// Between operator: skip if ranges don't overlap
canSkipZoneMap(zoneMap, { column: 'age', operator: 'between', value: 50, value2: 60 }) // true
```
