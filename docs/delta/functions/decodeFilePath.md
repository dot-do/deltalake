[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / decodeFilePath

# Function: decodeFilePath()

> **decodeFilePath**(`filePath`): `string`

Defined in: src/delta/index.ts:532

Decode URL-encoded characters in a file path from the Delta log.

Delta Lake stores partition paths with URL encoding (e.g., %20 for space, %3A for colon).
When reading from filesystem storage, the path needs to be decoded to match the actual
directory names on disk.

Note: This performs a single decode pass. The Delta log may contain double-encoded
characters (e.g., %253A which decodes to %3A), and the filesystem often stores paths
with the single-decoded version.

## Parameters

### filePath

`string`

The URL-encoded file path from the Delta log

## Returns

`string`

The decoded file path suitable for filesystem access

## Example

```typescript
decodeFilePath("bool=true/time=1970-01-01%2000%253A00%253A00/data.parquet")
// Returns: "bool=true/time=1970-01-01 00%3A00%3A00/data.parquet"
```
