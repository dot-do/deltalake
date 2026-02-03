**@dotdo/deltalake v0.0.1**

***

# @dotdo/deltalake

Delta Lake format implementation - the shared foundation for MongoLake and KafkaLake.

## Overview

`@dotdo/deltalake` provides:

- **Delta Lake Transaction Log**: JSON-based transaction log for ACID operations
- **Storage Backends**: R2, S3, filesystem, and memory implementations
- **Parquet Utilities**: Streaming writer, variant encoding, zone maps
- **Query Layer**: MongoDB-style operators with Parquet predicate pushdown
- **CDC Primitives**: Unified change data capture format

## Getting Started

### Installation

```bash
npm install @dotdo/deltalake
```

### Basic Usage

```typescript
import { DeltaTable } from '@dotdo/deltalake/delta'
import { createStorage } from '@dotdo/deltalake/storage'

// Create a storage backend
// In Node.js, defaults to FileSystemStorage with ./.deltalake path
// In other environments (like Cloudflare Workers), you must specify the storage type explicitly
const storage = createStorage({ type: 'memory' })  // or { type: 'r2', bucket: env.MY_BUCKET }

// Create a new Delta table
const table = new DeltaTable(storage, 'my-table')

// Write data to the table
await table.write([
  { _id: '1', name: 'Alice', age: 30 },
  { _id: '2', name: 'Bob', age: 25 },
  { _id: '3', name: 'Charlie', age: 35 },
])

// Read data from the table
const records = await table.query()
console.log(records)
// [
//   { _id: '1', name: 'Alice', age: 30 },
//   { _id: '2', name: 'Bob', age: 25 },
//   { _id: '3', name: 'Charlie', age: 35 },
// ]

// Query with filters
const filtered = await table.query({ age: { $gte: 30 } })
console.log(filtered)
// [
//   { _id: '1', name: 'Alice', age: 30 },
//   { _id: '3', name: 'Charlie', age: 35 },
// ]
```

## Architecture

```
@dotdo/deltalake              ← This package (foundation)
    ↑
    ├── @dotdo/mongolake      ← MongoDB API on Delta Lake
    └── @dotdo/kafkalake      ← Kafka API on Delta Lake
```

## Integration with MongoLake and KafkaLake

This package provides the core Delta Lake foundation that powers both MongoLake and KafkaLake.

### MongoLake

MongoLake (coming soon) uses `@dotdo/deltalake` for MongoDB-compatible document storage. It provides:

- A MongoDB wire protocol implementation on top of Delta Lake
- Document CRUD operations stored as Parquet files
- MongoDB query operators translated to Parquet predicates via the query layer
- Change streams powered by the CDC primitives

```typescript
// MongoLake internally uses deltalake for storage
import { DeltaTable } from '@dotdo/deltalake/delta'
import { matchesFilter } from '@dotdo/deltalake/query'

// Documents are written to Delta tables
await deltaTable.write([{ _id: '1', name: 'Alice' }])
```

### KafkaLake

KafkaLake (coming soon) uses `@dotdo/deltalake` for Kafka-compatible event streaming. It provides:

- A Kafka protocol implementation backed by Delta Lake
- Topics stored as partitioned Delta tables
- Consumer groups with offset tracking via the transaction log
- Event retention and compaction using Delta Lake's time travel

```typescript
// KafkaLake internally uses deltalake for event storage
import { DeltaTable } from '@dotdo/deltalake/delta'
import { CDCRecord } from '@dotdo/deltalake/cdc'

// Events are stored as CDC records in Delta tables
await deltaTable.write([
  { _id: 'evt-1', _op: 'c', _after: { topic: 'orders', value: '...' } }
])
```

## Key Components

### Storage Backends

```typescript
import { createStorage } from '@dotdo/deltalake/storage'

// Explicit configuration (recommended):
const r2Storage = createStorage({ type: 'r2', bucket: env.MY_BUCKET })
const fsStorage = createStorage({ type: 'filesystem', path: './.data' })
const memStorage = createStorage({ type: 'memory' })
const s3Storage = createStorage({ type: 's3', bucket: 'my-bucket', region: 'us-east-1' })

// URL-based configuration:
const storage = createStorage('s3://my-bucket/prefix')
const storage2 = createStorage('/data/lake')  // filesystem
const storage3 = createStorage('memory://')

// Default (Node.js only): uses FileSystemStorage with ./.deltalake path
const storage = createStorage()
```

### Delta Lake Transaction Log

```typescript
import { DeltaTable } from '@dotdo/deltalake/delta'

const table = new DeltaTable(storage, 'events')

// Write with ACID guarantees
await table.write([
  { _id: '1', user: 'alice', action: 'click' },
  { _id: '2', user: 'bob', action: 'purchase' },
])

// Time travel
const snapshot = await table.snapshot(5)
```

### CDC Records

```typescript
import { CDCRecord } from '@dotdo/deltalake/cdc'

// Unified CDC format for all systems
interface CDCRecord<T> {
  _id: string          // Entity ID
  _seq: bigint         // Sequence number
  _op: 'c' | 'u' | 'd' | 'r'  // create/update/delete/read
  _before: T | null    // Previous state
  _after: T | null     // New state
  _ts: bigint          // Nanosecond timestamp
  _source: CDCSource   // Origin metadata
}
```

### Query Layer

```typescript
import { matchesFilter, filterToParquetPredicate } from '@dotdo/deltalake/query'

// MongoDB-style operators
const filter = {
  user: 'alice',
  amount: { $gte: 100 },
  action: { $in: ['purchase', 'refund'] }
}

// Predicate pushdown for efficient Parquet reads
const predicate = filterToParquetPredicate(filter)
```

## Local Development

```bash
# Zero config - just works
npm install @dotdo/deltalake

# Data stored in .deltalake/
.deltalake/
├── events/
│   ├── _delta_log/
│   │   ├── 00000000000000000000.json
│   │   └── 00000000000000000001.json
│   └── part-00000.parquet
└── users/
    ├── _delta_log/
    └── part-00000.parquet
```

## Testing

```bash
# Unit tests (vitest-pool-workers)
npm run test:unit

# Integration tests (multi-worker miniflare)
npm run test:integration

# E2E tests (against deployed service)
npm run test:e2e
```

## API Documentation

Generate API documentation using TypeDoc:

```bash
# Install typedoc (dev dependency)
npm install --save-dev typedoc typedoc-plugin-markdown

# Generate documentation
npm run typedoc
```

This generates markdown documentation in the `docs/` directory. The source code also contains extensive JSDoc comments for inline documentation.

## Roadmap to 1.0

**Current Status: v0.0.1 (early development)**

This package is in early development. The API may change significantly before 1.0.

### Criteria for 1.0 Release

1. **Complete Delta Lake read/write with real Parquet files**
   - Full Delta Lake protocol support for reads and writes
   - Production-ready Parquet file handling via hyparquet

2. **Published to npm with stable API**
   - Stable, documented public API
   - Semantic versioning from 1.0 onward
   - No breaking changes without major version bump

3. **Documentation complete**
   - Comprehensive API reference
   - Usage guides for common patterns
   - Integration examples for MongoLake and KafkaLake

4. **Core operations implemented**
   - Read: Query tables with filter pushdown
   - Write: Append data with ACID guarantees
   - Time travel: Access historical table versions
   - Vacuum: Clean up old files and optimize storage

5. **Storage backends production-ready**
   - Memory: For testing and development
   - R2: Cloudflare R2 object storage
   - S3: AWS S3 compatible storage
   - FileSystem: Local development and edge cases

## Cloudflare Workers Compatibility

This package is designed for edge runtime compatibility and works seamlessly with Cloudflare Workers.

### Edge Runtime Design

- **Pure JavaScript**: All dependencies are pure JavaScript with no native Node.js modules, ensuring compatibility with Workers runtime
- **R2 Storage Backend**: First-class support for Cloudflare R2 as the storage backend
- **Streaming APIs**: Uses standard Web APIs (ReadableStream, Uint8Array) that work in both Node.js and Workers

### Testing with vitest-pool-workers

We use `vitest-pool-workers` to ensure Workers compatibility:

```bash
# Run unit tests in miniflare environment
npm run test:unit
```

The test suite runs against the actual Workers runtime via miniflare, catching compatibility issues before deployment.

### Using with Cloudflare R2

```typescript
import { createStorage } from '@dotdo/deltalake/storage'
import { DeltaTable } from '@dotdo/deltalake/delta'

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Create R2 storage backend
    const storage = createStorage({ type: 'r2', bucket: env.MY_R2_BUCKET })

    // Use Delta tables normally
    const table = new DeltaTable(storage, 'events')
    await table.write([{ _id: '1', data: 'example' }])

    return new Response('OK')
  }
}
```

### Pure JavaScript Dependencies

Key dependencies that ensure Workers compatibility:

- **hyparquet**: Pure JavaScript Parquet reader/writer (no native bindings)
- No dependencies on `fs`, `path`, or other Node.js-specific modules in the core library

## Known Limitations

### Write Lock Scope (Single-Instance Only)

The conditional write mechanism (`writeConditional()`) uses in-memory locks to prevent concurrent writes to the same file. **These locks only work within a single process/instance.**

In distributed deployments (multiple Workers, multiple Node.js processes, multiple servers), the locks provide NO coordination. Concurrent writes from different instances will:

1. Both acquire their local lock (no contention)
2. Both read the current version
3. Both attempt to write
4. One will succeed, one will fail with `VersionMismatchError`

This is **optimistic concurrency control** - conflicts are detected and rejected, but not prevented.

#### Recommendations for Distributed Deployments

1. **Single-writer design**: Route all writes for a given table to the same instance
2. **External locking**: Use Redis, DynamoDB, or similar for distributed coordination
3. **Retry on conflict**: Catch `VersionMismatchError` and retry with exponential backoff
4. **Accept occasional conflicts**: For low-write-frequency tables, conflicts may be rare enough to handle via retries

```typescript
// Example: Retry on version conflict
async function writeWithRetry(table, data, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      await table.write(data)
      return
    } catch (e) {
      if (e instanceof VersionMismatchError && i < maxRetries - 1) {
        await sleep(Math.pow(2, i) * 100) // Exponential backoff
        continue
      }
      throw e
    }
  }
}
```

## Design Principles

1. **Simple Local Development**: In Node.js, works without configuration by defaulting to filesystem storage
2. **Parquet Native**: All data stored as Parquet with Variant type for schema-free
3. **Delta Lake Format**: Standard format for ecosystem compatibility
4. **Streaming First**: Designed for high-frequency appends (Kafka/CDC use cases)
5. **Query Pushdown**: MongoDB operators translate to Parquet predicates
