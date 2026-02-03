# Operations Guide

This guide covers operational concerns for deploying `@dotdo/deltalake` in production environments.

## Table of Contents

- [Storage Backend Configuration](#storage-backend-configuration)
  - [AWS S3](#aws-s3)
  - [Cloudflare R2](#cloudflare-r2)
  - [Local Filesystem](#local-filesystem)
  - [Memory (Testing)](#memory-testing)
- [Performance Tuning](#performance-tuning)
  - [Write Performance](#write-performance)
  - [Read Performance](#read-performance)
  - [Compaction](#compaction)
- [Monitoring and Observability](#monitoring-and-observability)
  - [Logging](#logging)
  - [Metrics Collection](#metrics-collection)
  - [Health Checks](#health-checks)
- [Data Maintenance](#data-maintenance)
  - [Vacuum Operations](#vacuum-operations)
  - [Checkpoint Management](#checkpoint-management)
  - [Log Cleanup](#log-cleanup)
- [Concurrency and Reliability](#concurrency-and-reliability)
  - [Optimistic Concurrency Control](#optimistic-concurrency-control)
  - [Retry Configuration](#retry-configuration)
  - [Distributed Deployments](#distributed-deployments)
- [Troubleshooting](#troubleshooting)
  - [Common Errors](#common-errors)
  - [Debugging Tips](#debugging-tips)
  - [Recovery Procedures](#recovery-procedures)

---

## Storage Backend Configuration

### AWS S3

S3Storage supports AWS S3 and S3-compatible object stores. It uses the AWS SDK v3 command patterns internally.

#### Configuration

```typescript
import { createStorage } from '@dotdo/deltalake/storage'

// Basic configuration
const storage = createStorage({
  type: 's3',
  bucket: 'my-delta-lake-bucket',
  region: 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
})

// URL-based configuration
const storage = createStorage('s3://my-bucket/prefix')

// With region in URL
const storage = createStorage('s3://my-bucket.s3.us-west-2.amazonaws.com/prefix')
```

#### IAM Permissions

Required S3 permissions for the IAM role/user:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:HeadObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-delta-lake-bucket",
        "arn:aws:s3:::my-delta-lake-bucket/*"
      ]
    }
  ]
}
```

For large file uploads (>5MB), multipart upload permissions are also required:

```json
{
  "Action": [
    "s3:CreateMultipartUpload",
    "s3:UploadPart",
    "s3:CompleteMultipartUpload",
    "s3:AbortMultipartUpload"
  ]
}
```

#### Production Recommendations

- **Bucket Versioning**: Enable S3 bucket versioning for additional data protection
- **Lifecycle Rules**: Configure lifecycle rules to automatically clean up incomplete multipart uploads
- **Cross-Region Replication**: For disaster recovery, enable cross-region replication
- **Server-Side Encryption**: Enable SSE-S3 or SSE-KMS for data at rest encryption

### Cloudflare R2

R2Storage is optimized for Cloudflare Workers and provides low-latency access from edge locations.

#### Configuration

```typescript
import { createStorage } from '@dotdo/deltalake/storage'

// In a Cloudflare Worker
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const storage = createStorage({
      type: 'r2',
      bucket: env.MY_R2_BUCKET,
    })

    // Use storage with Delta tables
    const table = new DeltaTable(storage, 'events')
    // ...
  }
}
```

#### Wrangler Configuration

```toml
# wrangler.toml
[[r2_buckets]]
binding = "MY_R2_BUCKET"
bucket_name = "delta-lake-data"
```

#### Production Recommendations

- **Worker Limits**: Be aware of Worker CPU time limits (50ms default, 30s max)
- **Memory**: Workers have 128MB memory limit; large table operations may need to be chunked
- **Caching**: R2 supports CDN caching for read-heavy workloads
- **Jurisdictions**: Use R2 jurisdictional restrictions for data residency requirements

### Local Filesystem

FileSystemStorage is ideal for local development and testing.

#### Configuration

```typescript
import { createStorage } from '@dotdo/deltalake/storage'

// Explicit path
const storage = createStorage({
  type: 'filesystem',
  path: '/data/delta-lake',
})

// URL-based
const storage = createStorage('file:///data/delta-lake')
const storage = createStorage('/data/delta-lake')
const storage = createStorage('./data')

// Auto-detect (Node.js only, defaults to ./.deltalake)
const storage = createStorage()
```

#### Security Notes

- Path traversal attacks are prevented via canonicalization and validation
- Null bytes in paths are rejected
- URL-encoded characters are decoded and validated
- Paths are constrained to the configured base directory

### Memory (Testing)

MemoryStorage provides an in-memory backend for testing and development.

#### Configuration

```typescript
import { createStorage, MemoryStorage } from '@dotdo/deltalake/storage'

// Basic usage
const storage = createStorage({ type: 'memory' })

// With testing utilities
const storage = new MemoryStorage({
  // Simulate slow storage
  latency: {
    read: 10,    // 10ms read latency
    write: 20,   // 20ms write latency
    list: 5,     // 5ms list latency
    delete: 5,   // 5ms delete latency
  },
  // Simulate storage quota
  maxSize: 100 * 1024 * 1024, // 100MB limit
})

// Snapshot and restore for testing
const snapshot = storage.snapshot()
// ... run tests ...
storage.restore(snapshot)

// Operation tracking
const history = storage.getOperationHistory()
console.log(`Operations: ${history.length}`)
```

---

## Performance Tuning

### Write Performance

#### Batch Size

Group writes into appropriate batch sizes for optimal throughput:

```typescript
// Good: Batch multiple rows per write
await table.write(largeArrayOfRows) // Single commit

// Avoid: Many small writes
for (const row of rows) {
  await table.write([row]) // Many commits, slow
}
```

#### Parquet Configuration

The library uses `hyparquet-writer` for Parquet output. Default configuration is optimized for general use, but you can tune via the streaming writer for custom needs.

### Read Performance

#### Filter Pushdown

Use MongoDB-style filters for efficient data skipping:

```typescript
// Efficient: Parquet predicate pushdown
const results = await table.query({
  timestamp: { $gte: startDate, $lt: endDate },
  status: { $in: ['active', 'pending'] },
})

// Zone maps enable file skipping when min/max bounds don't overlap the filter
```

#### Projection

Request only the columns you need:

```typescript
const results = await table.query(
  { status: 'active' },
  { projection: { id: 1, name: 1 } } // Only fetch id and name columns
)
```

#### Byte-Range Reads

The storage backends support efficient byte-range reads, which are essential for Parquet files where metadata is stored at the end:

```typescript
// Internal: The library automatically uses range reads for Parquet metadata
const asyncBuffer = await createAsyncBuffer(storage, path)
// This fetches only the footer, not the entire file
```

### Compaction

Regular compaction improves read performance by merging small files:

```typescript
import { compact } from '@dotdo/deltalake/compaction'

const metrics = await compact(table, {
  // Target 128MB files
  targetFileSize: 128 * 1024 * 1024,

  // Minimum files before compaction triggers
  minFilesForCompaction: 4,

  // File selection strategy
  // 'greedy': Fast, default
  // 'bin-packing': Better size distribution
  // 'sort-by-size': Predictable order
  strategy: 'bin-packing',

  // Partition-aware compaction
  partitionColumns: ['date', 'region'],

  // Verify data integrity
  verifyIntegrity: true,

  // Progress callback
  onProgress: (phase, progress) => {
    console.log(`${phase}: ${(progress * 100).toFixed(1)}%`)
  },
})

console.log(`Compacted ${metrics.filesCompacted} files into ${metrics.filesCreated}`)
console.log(`Throughput: ${metrics.rowsPerSecond.toFixed(0)} rows/sec`)
```

#### Z-Order Clustering

For tables queried on multiple columns, Z-order clustering improves data skipping:

```typescript
import { zOrderCluster } from '@dotdo/deltalake/compaction'

const metrics = await zOrderCluster(table, {
  columns: ['region', 'timestamp', 'category'],
  curveType: 'z-order', // or 'hilbert'
})

console.log(`Estimated skip rate: ${(metrics.estimatedSkipRate * 100).toFixed(1)}%`)
```

#### Deduplication

Remove duplicate rows based on primary key:

```typescript
import { deduplicate } from '@dotdo/deltalake/compaction'

const metrics = await deduplicate(table, {
  primaryKey: ['id'],
  keepStrategy: 'latest', // 'first', 'last', or 'latest'
  orderByColumn: 'updated_at', // Required for 'latest'
})

console.log(`Removed ${metrics.duplicatesRemoved} duplicates`)
```

---

## Monitoring and Observability

### Logging

The library provides a pluggable logging interface:

```typescript
import { setLogger } from '@dotdo/deltalake'

// Integrate with your logging infrastructure
setLogger({
  debug: (msg, ...args) => myLogger.debug(msg, ...args),
  info: (msg, ...args) => myLogger.info(msg, ...args),
  warn: (msg, ...args) => myLogger.warn(msg, ...args),
  error: (msg, ...args) => myLogger.error(msg, ...args),
})

// Example: Integration with pino
import pino from 'pino'
const logger = pino({ level: 'info' })

setLogger({
  debug: (msg, ...args) => logger.debug({ args }, msg),
  info: (msg, ...args) => logger.info({ args }, msg),
  warn: (msg, ...args) => logger.warn({ args }, msg),
  error: (msg, ...args) => logger.error({ args }, msg),
})

// Silence logging entirely
setLogger({
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
})
```

### Metrics Collection

#### Compaction Metrics

Compaction operations return detailed metrics:

```typescript
interface CompactionMetrics {
  filesCompacted: number
  filesCreated: number
  filesSkippedLargeEnough: number
  rowsRead: number
  rowsWritten: number
  fileCountBefore: number
  fileCountAfter: number
  totalBytesBefore: number
  totalBytesAfter: number
  avgFileSizeBefore: number
  avgFileSizeAfter: number
  durationMs: number
  readTimeMs: number
  writeTimeMs: number
  commitTimeMs: number
  rowsPerSecond: number
  bytesPerSecond: number
  commitVersion: number
  strategy: string
  binPackingEfficiency: number
  partitionsCompacted: number
  integrityVerified: boolean
  conflictDetected: boolean
  retried: boolean
}
```

#### Vacuum Metrics

```typescript
interface VacuumMetrics {
  filesDeleted: number
  bytesFreed: number
  filesRetained: number
  dryRun: boolean
  filesToDelete?: string[]
  durationMs: number
  filesScanned: number
  errors?: string[]
}
```

#### Retry Metrics

```typescript
const { result, metrics } = await withRetry(
  () => table.write(rows),
  { returnMetrics: true }
)

console.log({
  attempts: metrics.attempts,
  retries: metrics.retries,
  succeeded: metrics.succeeded,
  totalDelayMs: metrics.totalDelayMs,
  elapsedMs: metrics.elapsedMs,
})
```

### Health Checks

Implement health checks by verifying storage connectivity and table state:

```typescript
async function healthCheck(table: DeltaTable): Promise<{
  healthy: boolean
  version: number
  fileCount: number
  latencyMs: number
}> {
  const start = Date.now()

  try {
    const snapshot = await table.snapshot()
    return {
      healthy: true,
      version: snapshot.version,
      fileCount: snapshot.files.length,
      latencyMs: Date.now() - start,
    }
  } catch (error) {
    return {
      healthy: false,
      version: -1,
      fileCount: 0,
      latencyMs: Date.now() - start,
    }
  }
}
```

---

## Data Maintenance

### Vacuum Operations

VACUUM removes orphaned files that are no longer referenced by any snapshot:

```typescript
import { vacuum, formatBytes, formatDuration } from '@dotdo/deltalake/delta'

// Preview what would be deleted (dry run)
const preview = await vacuum(table, {
  retentionHours: 168, // 7 days (default)
  dryRun: true,
})
console.log(`Would delete: ${preview.filesToDelete?.length} files`)
console.log(`Would free: ${formatBytes(preview.bytesFreed)}`)

// Execute vacuum
const metrics = await vacuum(table, {
  retentionHours: 168, // Minimum: 1 hour
  dryRun: false,
  onProgress: (phase, current, total) => {
    console.log(`${phase}: ${current}/${total}`)
  },
})

console.log(`Deleted ${metrics.filesDeleted} files`)
console.log(`Freed ${formatBytes(metrics.bytesFreed)}`)
console.log(`Duration: ${formatDuration(metrics.durationMs)}`)

if (metrics.errors?.length) {
  console.warn('Non-fatal errors:', metrics.errors)
}
```

#### Retention Period

The retention period determines how long deleted files are kept for time-travel queries:

- **Default**: 168 hours (7 days)
- **Minimum**: 1 hour (safety limit to prevent accidental data loss)
- **Recommendation**: Match your backup/recovery requirements

#### Scheduling

Run vacuum on a regular schedule:

```typescript
// Example: Daily vacuum job
import { CronJob } from 'cron'

const vacuumJob = new CronJob('0 3 * * *', async () => {
  const metrics = await vacuum(table, {
    retentionHours: 168,
    dryRun: false,
  })

  // Send metrics to monitoring
  await sendMetrics('vacuum', metrics)
})

vacuumJob.start()
```

### Checkpoint Management

Checkpoints are Parquet files that consolidate transaction log state for faster table initialization:

```typescript
import {
  shouldCheckpoint,
  writeSingleCheckpoint,
  cleanupCheckpoints,
  DEFAULT_CHECKPOINT_CONFIG,
} from '@dotdo/deltalake/delta'

// Check if checkpoint is needed
const needsCheckpoint = await shouldCheckpoint(storage, tablePath, {
  interval: 10, // Checkpoint every 10 commits (default)
})

// Write checkpoint after operations
if (needsCheckpoint) {
  await writeSingleCheckpoint(storage, tablePath, currentVersion)
}

// Clean up old checkpoints
await cleanupCheckpoints(storage, tablePath, {
  keepLast: 3, // Keep last 3 checkpoints
})
```

### Log Cleanup

Transaction log files can be cleaned up after checkpointing:

```typescript
import { cleanupLogs, getCleanableLogVersions } from '@dotdo/deltalake/delta'

// Get versions that can be safely cleaned
const cleanable = await getCleanableLogVersions(storage, tablePath)

// Clean up old log files
await cleanupLogs(storage, tablePath, {
  keepVersions: 10, // Keep last 10 versions
})
```

---

## Concurrency and Reliability

### Optimistic Concurrency Control

The library uses optimistic concurrency control via version checking:

```typescript
try {
  await table.write(rows)
} catch (error) {
  if (error instanceof ConcurrencyError) {
    // Another writer modified the table
    console.log(`Expected version ${error.expectedVersion}, found ${error.actualVersion}`)

    // Refresh and retry
    await table.refresh()
    await table.write(rows)
  }
  throw error
}
```

### Retry Configuration

The `withRetry` function provides exponential backoff with jitter:

```typescript
import { withRetry, DEFAULT_RETRY_CONFIG } from '@dotdo/deltalake/delta'

const result = await withRetry(
  () => table.write(rows),
  {
    // Number of retry attempts
    maxRetries: 5, // default: 3

    // Exponential backoff configuration
    baseDelay: 100,   // Initial delay in ms (default: 100)
    maxDelay: 10000,  // Maximum delay in ms (default: 10000)
    multiplier: 2,    // Exponential multiplier (default: 2)

    // Jitter prevents thundering herd
    jitter: true,        // Enable jitter (default: true)
    jitterFactor: 0.5,   // +/- 50% randomization (default: 0.5)

    // Callbacks for observability
    onRetry: ({ attempt, error, delay }) => {
      console.log(`Retry ${attempt}: ${error.message} (waiting ${delay}ms)`)
      // Return false to abort retries
    },

    onSuccess: ({ result, attempts }) => {
      console.log(`Succeeded after ${attempts} attempt(s)`)
    },

    onFailure: ({ error, attempts }) => {
      console.error(`Failed after ${attempts} attempts: ${error.message}`)
    },

    // Custom retry predicate
    isRetryable: (error) => {
      return error instanceof ConcurrencyError
    },

    // Abort signal for cancellation
    signal: abortController.signal,
  }
)
```

### Distributed Deployments

**Important**: The write locks used by storage backends are process-local only. In distributed environments:

1. **Concurrent writes from different instances** will both proceed to the storage backend
2. The storage's ETag/version check will detect the conflict
3. One writer succeeds, the other receives `VersionMismatchError`

#### Strategies for Distributed Writes

**Single-Writer Design** (Recommended)
Route all writes for a table to a single instance:

```typescript
// Using consistent hashing or sticky sessions
const writerId = hash(tablePath) % numWriters
if (myId === writerId) {
  await table.write(rows)
}
```

**External Locking**
Use Redis or DynamoDB for distributed coordination:

```typescript
import { acquireLock, releaseLock } from './distributed-lock'

const lockKey = `delta-lock:${tablePath}`
const lock = await acquireLock(lockKey, { ttl: 30000 })

try {
  await table.write(rows)
} finally {
  await releaseLock(lock)
}
```

**Retry on Conflict**
For low-write-frequency tables, handle conflicts with retries:

```typescript
await withRetry(
  async () => {
    await table.refresh() // Get latest state
    await table.write(rows)
  },
  {
    maxRetries: 5,
    isRetryable: (e) => e instanceof ConcurrencyError,
  }
)
```

---

## Troubleshooting

### Common Errors

#### ConcurrencyError

**Cause**: Another writer modified the table between reading and writing.

**Solution**: Refresh and retry the operation.

```typescript
if (error instanceof ConcurrencyError) {
  await table.refresh()
  // Retry the operation
}
```

#### VersionMismatchError

**Cause**: Conditional write failed because the file version changed.

**Solution**: Read the new version and retry.

```typescript
if (error instanceof VersionMismatchError) {
  const currentVersion = error.actualVersion
  // Update your state and retry
}
```

#### FileNotFoundError

**Cause**: Attempting to read a file that doesn't exist.

**Solution**: Check file existence before reading, or handle the error gracefully.

```typescript
if (error instanceof FileNotFoundError) {
  console.log(`File not found: ${error.path}`)
  // Handle missing file
}
```

#### S3Error

**Cause**: AWS S3 operation failed.

**Solution**: Check the error code and status:

```typescript
if (error instanceof S3Error) {
  switch (error.s3Code) {
    case 'NoSuchKey':
      // File doesn't exist
      break
    case 'AccessDenied':
      // Check IAM permissions
      break
    case 'NoSuchBucket':
      // Bucket doesn't exist
      break
  }
}
```

#### ValidationError

**Cause**: Invalid input provided to an operation.

**Solution**: Check the error message and field:

```typescript
if (error instanceof ValidationError) {
  console.log(`Validation failed for ${error.field}: ${error.message}`)
  // Fix the input and retry
}
```

### Debugging Tips

#### Enable Debug Logging

```typescript
import { setLogger } from '@dotdo/deltalake'

setLogger({
  debug: console.debug.bind(console), // Enable debug output
  info: console.info.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
})
```

#### Inspect Table State

```typescript
// Get current snapshot
const snapshot = await table.snapshot()
console.log({
  version: snapshot.version,
  fileCount: snapshot.files.length,
  totalSize: snapshot.files.reduce((sum, f) => sum + f.size, 0),
})

// List files
for (const file of snapshot.files) {
  console.log(`${file.path}: ${file.size} bytes`)
}
```

#### Check Storage Connectivity

```typescript
try {
  const exists = await storage.exists('_delta_log/00000000000000000000.json')
  console.log(`Table exists: ${exists}`)
} catch (error) {
  console.error('Storage connectivity issue:', error)
}
```

#### Memory Storage Diagnostics

```typescript
const memStorage = new MemoryStorage()
// ... operations ...

// Check state
console.log({
  fileCount: memStorage.getFileCount(),
  usedSize: memStorage.getUsedSize(),
  operations: memStorage.getOperationHistory().length,
})
```

### Recovery Procedures

#### Corrupted Transaction Log

If the transaction log is corrupted, you may need to:

1. **Identify the last valid checkpoint**
2. **Reconstruct state from the checkpoint**
3. **Manually fix or remove corrupted log entries**

```typescript
import { readLastCheckpoint, readCheckpoint, discoverCheckpoints } from '@dotdo/deltalake/delta'

// Find available checkpoints
const checkpoints = await discoverCheckpoints(storage, tablePath)
console.log('Available checkpoints:', checkpoints)

// Read from the last valid checkpoint
const lastCheckpoint = await readLastCheckpoint(storage, tablePath)
if (lastCheckpoint) {
  const checkpointData = await readCheckpoint(storage, tablePath, lastCheckpoint)
  // Reconstruct table state from checkpoint
}
```

#### Orphaned Files

If vacuum wasn't run and storage is filling up:

```typescript
// Preview orphaned files
const preview = await vacuum(table, { dryRun: true })
console.log(`Orphaned files: ${preview.filesToDelete?.length}`)
console.log(`Space to recover: ${formatBytes(preview.bytesFreed)}`)

// Clean up
await vacuum(table, { dryRun: false })
```

#### Stuck Multipart Uploads (S3)

For S3, incomplete multipart uploads can accumulate. Configure lifecycle rules:

```xml
<LifecycleConfiguration>
  <Rule>
    <ID>AbortIncompleteMultipartUpload</ID>
    <Status>Enabled</Status>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>1</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>
```

---

## Quick Reference

### Environment Variables

| Variable | Description | Used By |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key | S3Storage |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | S3Storage |
| `AWS_REGION` | AWS region (fallback) | S3Storage |

### Default Values

| Setting | Default | Notes |
|---------|---------|-------|
| Retry max attempts | 3 | `DEFAULT_RETRY_CONFIG.maxRetries` |
| Retry base delay | 100ms | `DEFAULT_RETRY_CONFIG.baseDelay` |
| Retry max delay | 10000ms | `DEFAULT_RETRY_CONFIG.maxDelay` |
| Vacuum retention | 168 hours (7 days) | Minimum: 1 hour |
| Checkpoint interval | 10 commits | `DEFAULT_CHECKPOINT_CONFIG.interval` |
| Compaction target size | 128MB | Recommended default |
| Multipart threshold | 5MB | S3 multipart upload cutoff |

### Error Hierarchy

```
DeltaLakeError (base)
  - StorageError
    - FileNotFoundError
    - VersionMismatchError
    - S3Error
  - ConcurrencyError
  - CDCError
  - ValidationError
```

### Type Guards

```typescript
import {
  isDeltaLakeError,
  isStorageError,
  isConcurrencyError,
  isCDCError,
  isValidationError,
  isRetryableError,
  isFileNotFoundError,
  isVersionMismatchError,
} from '@dotdo/deltalake/errors'
```
