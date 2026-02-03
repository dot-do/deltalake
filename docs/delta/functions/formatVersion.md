[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / formatVersion

# Function: formatVersion()

> **formatVersion**(`version`): `string`

Defined in: src/delta/index.ts:463

Format version number as 20-digit zero-padded string.

This is the canonical utility for formatting Delta Lake version numbers.
Use this function across all modules that need to format version numbers
for file paths (commit logs, CDC files, parquet files, etc.).

## Parameters

### version

Version number (supports both number and bigint)

`number` | `bigint`

## Returns

`string`

Zero-padded version string (e.g., "00000000000000000042")

## Throws

Error if version is negative or exceeds maximum digits

## Example

```typescript
formatVersion(0)   // "00000000000000000000"
formatVersion(42)  // "00000000000000000042"
formatVersion(1n)  // "00000000000000000001"
```
