[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / createAsyncBuffer

# Function: createAsyncBuffer()

> **createAsyncBuffer**(`storage`, `path`): `Promise`\<[`AsyncBuffer`](../interfaces/AsyncBuffer.md)\>

Defined in: src/storage/index.ts:92

Create an AsyncBuffer from a StorageBackend.
This allows hyparquet to read Parquet files efficiently using byte ranges.

## Parameters

### storage

[`StorageBackend`](../interfaces/StorageBackend.md)

The storage backend to read from

### path

`string`

Path to the file

## Returns

`Promise`\<[`AsyncBuffer`](../interfaces/AsyncBuffer.md)\>

Promise resolving to an AsyncBuffer for the file

## Throws

If the file does not exist

## Example

```typescript
const storage = createStorage({ type: 'memory' })
const buffer = await createAsyncBuffer(storage, 'data/table.parquet')
const data = await parquetReadObjects({ file: buffer })
```
