/**
 * Storage Backend Abstraction
 *
 * Unified interface for R2, S3, filesystem, and memory storage.
 * Auto-detects environment: local dev -> filesystem, production -> R2.
 *
 * ## Key Features
 *
 * - **readRange()**: Efficient byte-range reads for Parquet file access.
 *   Parquet files store metadata at the end, so we need byte-range reads
 *   to avoid downloading entire files.
 *
 * - **writeConditional()**: Optimistic concurrency control for Delta Lake
 *   transactions. Enables atomic check-and-write operations.
 *
 * ## Implementations
 *
 * - `MemoryStorage`: In-memory storage for testing
 * - `FileSystemStorage`: Local filesystem storage for development
 * - `R2Storage`: Cloudflare R2 storage for production
 * - `S3Storage`: AWS S3 storage for production
 *
 * ## Concurrency Model and Limitations
 *
 * ### Process-Local Locks
 *
 * The write locks used internally by `writeConditional()` are **process-local only**.
 * They serialize concurrent writes within a single Node.js process or Cloudflare
 * Worker instance, but provide **NO coordination across**:
 *
 * - Multiple Node.js processes on the same machine
 * - Multiple Cloudflare Workers instances
 * - Multiple servers/containers in a distributed deployment
 *
 * ### Optimistic Concurrency Control
 *
 * Delta Lake transactions use `writeConditional()` with `expectedVersion: null`
 * (create-if-not-exists) for commit files. This provides optimistic concurrency
 * control via storage-level versioning (ETags for R2/S3, mtime for filesystem):
 *
 * 1. Writer reads current state and determines next version number
 * 2. Writer attempts to create commit file (e.g., `00000000000000000005.json`)
 * 3. If another writer already created that version, a `VersionMismatchError` is
 *    thrown, which becomes a `ConcurrencyError` at the Delta Table level
 * 4. Writer should refresh table state and retry with the new version number
 *
 * This approach is standard for Delta Lake and works correctly across distributed
 * deployments - conflicting writes are detected and rejected.
 *
 * ### Recommendations for Multi-Instance Deployments
 *
 * For production deployments with multiple writers to the same table:
 *
 * 1. **Rely on optimistic concurrency (recommended)**: The default behavior works
 *    correctly. Wrap writes in retry logic that catches `ConcurrencyError`:
 *
 *    ```typescript
 *    import { withRetry } from '@dotdo/deltalake'
 *
 *    await withRetry(async () => {
 *      await table.refresh()
 *      await table.write(rows)
 *    }, { maxRetries: 5 })
 *    ```
 *
 * 2. **Single-writer per table**: Design your system so each table has only one
 *    writer process. Use separate tables for different services/instances.
 *
 * 3. **External distributed locks**: For strict serialization across instances,
 *    implement external locking using Redis, DynamoDB, or similar. This is rarely
 *    needed since optimistic concurrency handles most use cases.
 *
 * ### Why Not Storage-Based Distributed Locks?
 *
 * While cloud storage supports conditional operations (R2 `onlyIf`, S3 `If-Match`),
 * implementing distributed locks on top of object storage has significant drawbacks:
 *
 * - **Lock acquisition is not atomic**: The check-then-set pattern across network
 *   boundary is susceptible to race conditions
 * - **No automatic lock expiration**: Crashed processes leave stale locks
 * - **Performance overhead**: Each lock operation requires a network round-trip
 * - **Complexity**: Implementing correct distributed locking is error-prone
 *
 * The Delta Lake optimistic concurrency protocol (version-based commit files)
 * provides the same correctness guarantees with better performance.
 *
 * @example
 * ```typescript
 * // Create storage (auto-detects environment)
 * const storage = createStorage()
 *
 * // Or explicitly specify type
 * const storage = createStorage({ type: 'memory' })
 *
 * // Basic operations
 * await storage.write('path/to/file.txt', data)
 * const content = await storage.read('path/to/file.txt')
 * const files = await storage.list('path/')
 *
 * // Conditional writes for concurrency control
 * const version = await storage.writeConditional('file.json', data, null)
 * await storage.writeConditional('file.json', newData, version)
 * ```
 */

// =============================================================================
// ERROR TYPES (imported from centralized errors module)
// =============================================================================

import { StorageError, FileNotFoundError, VersionMismatchError, S3Error, ValidationError } from '../errors.js'

// Re-export for backwards compatibility
export { StorageError, FileNotFoundError, VersionMismatchError, S3Error, ValidationError }

// =============================================================================
// TYPES
// =============================================================================

export type {
  AsyncBuffer,
  FileStat,
  StorageBackend,
  S3Credentials,
  StorageOptions,
  ParsedStorageUrl,
  S3StreamingBody,
  S3GetObjectCommand,
  S3GetObjectOutput,
  S3PutObjectCommand,
  S3HeadObjectCommand,
  S3HeadObjectOutput,
  S3DeleteObjectCommand,
  S3ListObjectsV2Command,
  S3ListObjectsV2Output,
  S3CreateMultipartUploadCommand,
  S3CreateMultipartUploadOutput,
  S3UploadPartCommand,
  S3UploadPartOutput,
  S3CompleteMultipartUploadCommand,
  S3AbortMultipartUploadCommand,
  S3Command,
  S3ResponseMap,
  S3ClientLike,
  S3StorageOptions,
  MemoryStorageOperation,
  LatencyConfig,
  MemoryStorageOptions,
  OperationRecord,
  StorageSnapshot,
} from './types.js'

// =============================================================================
// UTILITIES
// =============================================================================

export { createAsyncBuffer } from './utils.js'

// =============================================================================
// IMPLEMENTATIONS
// =============================================================================

export { MemoryStorage } from './memory.js'
export { FileSystemStorage } from './filesystem.js'
export { R2Storage } from './r2.js'
export { S3Storage } from './s3.js'

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

import type { StorageBackend, StorageOptions, S3Credentials, ParsedStorageUrl } from './types.js'
import { MemoryStorage } from './memory.js'
import { FileSystemStorage } from './filesystem.js'
import { R2Storage } from './r2.js'
import { S3Storage } from './s3.js'

/**
 * Parse a storage URL/path and extract configuration.
 *
 * Supported URL formats:
 * - `file:///path/to/dir` or `/path/to/dir` or `./relative/path` -> FileSystemStorage
 * - `s3://bucket/path` or `s3://bucket.s3.region.amazonaws.com/path` -> S3Storage
 * - `r2://bucket/path` -> R2Storage
 * - `memory://` or `memory://name` -> MemoryStorage
 *
 * @param url - Storage URL or path to parse
 * @returns Parsed storage configuration
 * @throws {Error} If the URL format is not recognized
 *
 * @example
 * ```typescript
 * parseStorageUrl('file:///data/lake')
 * // => { type: 'filesystem', path: '/data/lake' }
 *
 * parseStorageUrl('/data/lake')
 * // => { type: 'filesystem', path: '/data/lake' }
 *
 * parseStorageUrl('s3://my-bucket/prefix')
 * // => { type: 's3', bucket: 'my-bucket', path: 'prefix', region: 'us-east-1' }
 *
 * parseStorageUrl('r2://my-bucket/prefix')
 * // => { type: 'r2', bucket: 'my-bucket', path: 'prefix' }
 *
 * parseStorageUrl('memory://')
 * // => { type: 'memory', path: '' }
 * ```
 *
 * @public
 */
export function parseStorageUrl(url: string): ParsedStorageUrl {
  // Handle memory:// URLs
  if (url.startsWith('memory://')) {
    const path = url.slice('memory://'.length)
    return { type: 'memory', path }
  }

  // Handle file:// URLs
  if (url.startsWith('file://')) {
    // file:///path/to/dir -> /path/to/dir
    // file://localhost/path -> /path
    let path = url.slice('file://'.length)

    // Handle Windows-style file:///C:/path
    if (path.startsWith('/') && /^\/[A-Za-z]:/.test(path)) {
      // file:///C:/path -> C:/path
      path = path.slice(1)
    } else if (path.startsWith('localhost/')) {
      // file://localhost/path -> /path
      path = path.slice('localhost'.length)
    }

    return { type: 'filesystem', path }
  }

  // Handle s3:// URLs
  if (url.startsWith('s3://')) {
    const urlWithoutScheme = url.slice('s3://'.length)
    const slashIndex = urlWithoutScheme.indexOf('/')
    let bucket: string
    let path: string
    let region = 'us-east-1' // Default region

    if (slashIndex === -1) {
      bucket = urlWithoutScheme
      path = ''
    } else {
      bucket = urlWithoutScheme.slice(0, slashIndex)
      path = urlWithoutScheme.slice(slashIndex + 1)
    }

    // Check for S3 path-style URL: s3://bucket.s3.region.amazonaws.com/path
    // or virtual-hosted style: s3://bucket.s3-region.amazonaws.com/path
    const s3HostMatch = bucket.match(/^(.+?)\.s3[.-]([a-z0-9-]+)\.amazonaws\.com$/i)
    if (s3HostMatch && s3HostMatch[1] !== undefined && s3HostMatch[2] !== undefined) {
      bucket = s3HostMatch[1]
      region = s3HostMatch[2]
    }

    return { type: 's3', bucket, path, region }
  }

  // Handle r2:// URLs
  if (url.startsWith('r2://')) {
    const urlWithoutScheme = url.slice('r2://'.length)
    const slashIndex = urlWithoutScheme.indexOf('/')
    let bucket: string
    let path: string

    if (slashIndex === -1) {
      bucket = urlWithoutScheme
      path = ''
    } else {
      bucket = urlWithoutScheme.slice(0, slashIndex)
      path = urlWithoutScheme.slice(slashIndex + 1)
    }

    return { type: 'r2', bucket, path }
  }

  // Handle absolute paths (starting with /)
  if (url.startsWith('/')) {
    return { type: 'filesystem', path: url }
  }

  // Handle relative paths (starting with ./ or ../ or just a name)
  if (url.startsWith('./') || url.startsWith('../') || !url.includes('://')) {
    return { type: 'filesystem', path: url }
  }

  throw new ValidationError(
    `Unrecognized storage URL format: ${url}. ` +
      'Supported formats: file:///path, /path, ./path, s3://bucket/path, r2://bucket/path, memory://',
    'url',
    url
  )
}

/**
 * Create a storage backend from a URL/path string.
 *
 * This function auto-detects the storage type from the URL scheme:
 * - `file:///path` or `/path` or `./path` -> FileSystemStorage
 * - `s3://bucket/path` -> S3Storage
 * - `r2://bucket/path` -> R2Storage (requires bucket binding)
 * - `memory://` -> MemoryStorage
 *
 * Note: For R2 and S3 storage, you may need to provide additional configuration
 * that cannot be derived from the URL alone (e.g., R2 bucket binding, S3 credentials).
 * Use the overload with options for full configuration.
 *
 * @param url - Storage URL or path
 * @param options - Additional options to merge with URL-derived configuration
 * @returns Configured StorageBackend instance
 * @throws {Error} If the URL format is not recognized
 * @throws {Error} If required configuration is missing (e.g., R2 bucket binding)
 *
 * @example
 * ```typescript
 * // Filesystem storage from URL
 * const storage = createStorageFromUrl('file:///data/lake')
 *
 * // Filesystem storage from path
 * const storage = createStorageFromUrl('/data/lake')
 *
 * // S3 storage (credentials from environment)
 * const storage = createStorageFromUrl('s3://my-bucket/prefix')
 *
 * // R2 storage (must provide bucket binding)
 * const storage = createStorageFromUrl('r2://my-bucket/prefix', {
 *   bucket: env.MY_BUCKET
 * })
 *
 * // Memory storage
 * const storage = createStorageFromUrl('memory://')
 * ```
 *
 * @public
 */
export function createStorageFromUrl(
  url: string,
  options?: {
    /** R2 bucket binding (required for r2:// URLs) */
    bucket?: R2Bucket
    /** S3 credentials (optional for s3:// URLs, uses environment if not provided) */
    credentials?: S3Credentials
    /** Override region for S3 (optional, extracted from URL or defaults to us-east-1) */
    region?: string
  }
): StorageBackend {
  const parsed = parseStorageUrl(url)

  switch (parsed.type) {
    case 'memory':
      return new MemoryStorage()

    case 'filesystem':
      return new FileSystemStorage({ path: parsed.path })

    case 's3': {
      if (!parsed.bucket) {
        throw new ValidationError('S3 URL must include a bucket name: s3://bucket/path', 'url', url)
      }
      const s3Options: { bucket: string; region: string; credentials?: S3Credentials } = {
        bucket: parsed.bucket,
        region: options?.region ?? parsed.region ?? 'us-east-1',
      }
      if (options?.credentials) {
        s3Options.credentials = options.credentials
      }
      return new S3Storage(s3Options)
    }

    case 'r2':
      if (!options?.bucket) {
        throw new ValidationError(
          `R2 storage requires a bucket binding. ` +
            `Use createStorageFromUrl('${url}', { bucket: env.MY_BUCKET })`,
          'options.bucket'
        )
      }
      return new R2Storage({ bucket: options.bucket })

    default:
      throw new ValidationError(`Unknown storage type: ${parsed.type}`, 'type', parsed.type)
  }
}

/**
 * Create a storage backend instance.
 *
 * Supports multiple calling conventions:
 *
 * 1. **No arguments**: Auto-detect environment
 *    - Node.js: Uses FileSystemStorage with `./.deltalake` path
 *    - Other environments: Throws error
 *
 * 2. **String URL/path**: Auto-detect from URL scheme
 *    - `file:///path` or `/path` -> FileSystemStorage
 *    - `s3://bucket/path` -> S3Storage
 *    - `r2://bucket/path` -> R2Storage (requires options.bucket)
 *    - `memory://` -> MemoryStorage
 *
 * 3. **Options object**: Explicit configuration
 *    - `{ type: 'memory' }`
 *    - `{ type: 'filesystem', path: './data' }`
 *    - `{ type: 's3', bucket: 'name', region: 'us-east-1' }`
 *    - `{ type: 'r2', bucket: r2BucketBinding }`
 *
 * @param optionsOrUrl - Storage URL string or configuration options
 * @param urlOptions - Additional options when using URL string (for R2 bucket, S3 credentials)
 * @returns Configured StorageBackend instance
 * @throws {Error} If options are required but not provided
 * @throws {Error} If unknown storage type is specified
 *
 * @example
 * ```typescript
 * // Auto-detect (Node.js -> filesystem)
 * const storage = createStorage()
 *
 * // From URL
 * const storage = createStorage('s3://my-bucket/data')
 * const storage = createStorage('file:///data/lake')
 * const storage = createStorage('/data/lake')
 * const storage = createStorage('memory://')
 *
 * // Explicit options
 * const storage = createStorage({ type: 'memory' })
 * const storage = createStorage({ type: 'filesystem', path: './data' })
 * ```
 *
 * @public
 */
export function createStorage(
  optionsOrUrl?: StorageOptions | string,
  urlOptions?: {
    bucket?: R2Bucket
    credentials?: S3Credentials
    region?: string
  }
): StorageBackend {
  // Handle string URL
  if (typeof optionsOrUrl === 'string') {
    return createStorageFromUrl(optionsOrUrl, urlOptions)
  }

  const options = optionsOrUrl

  if (!options) {
    // Auto-detect environment
    if (typeof process !== 'undefined' && typeof process.cwd === 'function') {
      return new FileSystemStorage({ path: './.deltalake' })
    }
    throw new ValidationError(
      'Cannot auto-detect storage environment. Please provide explicit options: ' +
        "createStorage({ type: 'memory' }) or createStorage({ type: 'r2', bucket })",
      'options'
    )
  }

  switch (options.type) {
    case 'filesystem':
      return new FileSystemStorage(options)
    case 'r2':
      return new R2Storage(options)
    case 's3':
      return new S3Storage(options)
    case 'memory':
      return new MemoryStorage()
    default:
      throw new ValidationError(`Unknown storage type: ${(options as { type: string }).type}`, 'options.type', (options as { type: string }).type)
  }
}
