[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [storage](../README.md) / StorageOptions

# Type Alias: StorageOptions

> **StorageOptions** = \{ `type`: `"filesystem"`; `path`: `string`; \} \| \{ `type`: `"r2"`; `bucket`: `R2Bucket`; \} \| \{ `type`: `"s3"`; `bucket`: `string`; `region`: `string`; `credentials?`: [`S3Credentials`](../interfaces/S3Credentials.md); \} \| \{ `type`: `"memory"`; \}

Defined in: src/storage/index.ts:352

Configuration options for creating a storage backend.

## Example

```typescript
// Memory storage (for testing)
createStorage({ type: 'memory' })

// Filesystem storage (for local development)
createStorage({ type: 'filesystem', path: './data' })

// R2 storage (for Cloudflare Workers)
createStorage({ type: 'r2', bucket: env.MY_BUCKET })

// S3 storage (for AWS)
createStorage({
  type: 's3',
  bucket: 'my-bucket',
  region: 'us-east-1',
  credentials: { accessKeyId: '...', secretAccessKey: '...' }
})
```
