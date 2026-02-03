[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / extractPartitionValuesFromPath

# Function: extractPartitionValuesFromPath()

> **extractPartitionValuesFromPath**(`filePath`): `Record`\<`string`, `string`\>

Defined in: src/delta/index.ts:493

Extract partition values from a file path.

Delta Lake uses Hive-style partitioning where partition values are encoded
in directory names as `column=value/`. This function parses such paths and
extracts partition column names and their values.

## Parameters

### filePath

`string`

The file path to parse (e.g., "letter=a/number=1/part-00000.parquet")

## Returns

`Record`\<`string`, `string`\>

Record of partition column names to their string values

## Example

```typescript
extractPartitionValuesFromPath("letter=a/number=1/part-00000.parquet")
// Returns: { letter: "a", number: "1" }

extractPartitionValuesFromPath("year=2024/month=01/day=15/data.parquet")
// Returns: { year: "2024", month: "01", day: "15" }

extractPartitionValuesFromPath("data.parquet")
// Returns: {}
```
