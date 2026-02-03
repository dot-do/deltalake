[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / parseStorageUrl

# Function: parseStorageUrl()

> **parseStorageUrl**(`url`): [`ParsedStorageUrl`](../interfaces/ParsedStorageUrl.md)

Defined in: src/storage/index.ts:419

Parse a storage URL/path and extract configuration.

Supported URL formats:
- `file:///path/to/dir` or `/path/to/dir` or `./relative/path` -> FileSystemStorage
- `s3://bucket/path` or `s3://bucket.s3.region.amazonaws.com/path` -> S3Storage
- `r2://bucket/path` -> R2Storage
- `memory://` or `memory://name` -> MemoryStorage

## Parameters

### url

`string`

Storage URL or path to parse

## Returns

[`ParsedStorageUrl`](../interfaces/ParsedStorageUrl.md)

Parsed storage configuration

## Throws

If the URL format is not recognized

## Example

```typescript
parseStorageUrl('file:///data/lake')
// => { type: 'filesystem', path: '/data/lake' }

parseStorageUrl('/data/lake')
// => { type: 'filesystem', path: '/data/lake' }

parseStorageUrl('s3://my-bucket/prefix')
// => { type: 's3', bucket: 'my-bucket', path: 'prefix', region: 'us-east-1' }

parseStorageUrl('r2://my-bucket/prefix')
// => { type: 'r2', bucket: 'my-bucket', path: 'prefix' }

parseStorageUrl('memory://')
// => { type: 'memory', path: '' }
```
