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
import { getLogger } from '../utils/index.js'

// Re-export for backwards compatibility
export { StorageError, FileNotFoundError, VersionMismatchError, S3Error, ValidationError }

// =============================================================================
// ASYNC BUFFER (for Parquet integration)
// =============================================================================

/**
 * AsyncBuffer interface for byte-range reads.
 * This is the interface expected by hyparquet for reading Parquet files.
 *
 * The interface mimics ArrayBuffer's slice() method but returns a Promise,
 * allowing for lazy loading of file contents.
 *
 * @public
 */
export interface AsyncBuffer {
  /** Total byte length of the file */
  byteLength: number

  /**
   * Read a byte range [start, end) from the file.
   *
   * @param start - Starting byte offset (inclusive)
   * @param end - Ending byte offset (exclusive), defaults to end of file
   * @returns Promise resolving to the requested byte range as ArrayBuffer
   */
  slice(start: number, end?: number): Promise<ArrayBuffer> | ArrayBuffer
}

/**
 * Create an AsyncBuffer from a StorageBackend.
 * This allows hyparquet to read Parquet files efficiently using byte ranges.
 *
 * @param storage - The storage backend to read from
 * @param path - Path to the file
 * @returns Promise resolving to an AsyncBuffer for the file
 * @throws {FileNotFoundError} If the file does not exist
 *
 * @public
 *
 * @example
 * ```typescript
 * const storage = createStorage({ type: 'memory' })
 * const buffer = await createAsyncBuffer(storage, 'data/table.parquet')
 * const data = await parquetReadObjects({ file: buffer })
 * ```
 */
export async function createAsyncBuffer(
  storage: StorageBackend,
  path: string
): Promise<AsyncBuffer> {
  const stat = await storage.stat(path)
  if (!stat) throw new FileNotFoundError(path, 'createAsyncBuffer')

  return {
    byteLength: stat.size,
    slice: async (start: number, end?: number) => {
      const uint8Array = await storage.readRange(path, start, end ?? stat.size)
      // Return ArrayBuffer for hyparquet compatibility
      return uint8Array.buffer.slice(
        uint8Array.byteOffset,
        uint8Array.byteOffset + uint8Array.byteLength
      ) as ArrayBuffer
    },
  }
}

// =============================================================================
// WRITE LOCK UTILITY
// =============================================================================

/**
 * Write locks for preventing concurrent writes within a single process.
 *
 * IMPORTANT LIMITATION: These locks only work within a single process/instance.
 * They are NOT distributed locks and provide NO coordination across:
 * - Multiple Node.js processes
 * - Multiple Cloudflare Workers instances
 * - Multiple servers/containers
 *
 * For distributed deployments, you must either:
 * 1. Use external coordination (Redis, DynamoDB, etc.)
 * 2. Rely on storage-level conditional writes (R2 onlyIf, S3 ETags)
 * 3. Design your system for single-writer access per table
 *
 * The storage backends (R2Storage, S3Storage) do use ETags/version checks,
 * which provide some protection against concurrent writes, but the check-then-write
 * is not atomic across the network boundary.
 */

/**
 * Lock object for managing write serialization.
 * Stores both the promise (for awaiting) and the release function (for unlocking).
 *
 * @internal
 */
interface Lock {
  /** Promise that resolves when the lock is released */
  promise: Promise<void>
  /** Function to release the lock */
  release: () => void
}

/**
 * Creates a new Lock object.
 * The promise resolves when release() is called.
 *
 * @internal
 */
function createLock(): Lock {
  let releaseRef: (() => void) | undefined
  const promise = new Promise<void>(resolve => {
    releaseRef = resolve
  })
  // The Promise executor runs synchronously, so releaseRef is guaranteed to be assigned
  // But we use a wrapper function to satisfy TypeScript without non-null assertion
  const release = () => {
    if (releaseRef) releaseRef()
  }
  return { promise, release }
}

/**
 * Acquires a write lock for a given path, waiting if another operation holds the lock.
 * This provides proper mutex semantics without TOCTOU race conditions.
 *
 * NOTE: This lock is process-local only. It does NOT provide distributed locking.
 * See the write lock documentation above for distributed deployment considerations.
 *
 * @param locks - The Map storing current locks by path
 * @param path - The path to acquire a lock for
 * @returns The Lock object that must be released after the operation
 *
 * @internal
 */
async function acquireWriteLock(locks: Map<string, Lock>, path: string): Promise<Lock> {
  while (true) {
    const existingLock = locks.get(path)
    if (existingLock) {
      await existingLock.promise
      // After the lock is released, loop again to try to acquire
      // (another waiter might have acquired it first)
      continue
    }
    // SAFETY: No TOCTOU race here because JavaScript is single-threaded.
    // The check (locks.get) and set (locks.set) are synchronous with no await
    // in between, so no other code can interleave. The only await boundary
    // is above when waiting for an existing lock, after which we loop back
    // to re-check the lock state.
    const newLock = createLock()
    locks.set(path, newLock)
    return newLock
  }
}

/**
 * Releases a write lock for a given path.
 *
 * @param locks - The Map storing current locks by path
 * @param path - The path to release the lock for
 * @param lock - The Lock object to release
 *
 * @internal
 */
function releaseWriteLock(locks: Map<string, Lock>, path: string, lock: Lock): void {
  // Only delete if the lock in the map is the same one we're releasing
  // This prevents a race where another operation might have already set a new lock
  if (locks.get(path) === lock) {
    locks.delete(path)
  }
  lock.release()
}

// =============================================================================
// STORAGE BACKEND INTERFACE
// =============================================================================

/**
 * Core storage backend interface.
 *
 * All storage implementations (memory, filesystem, R2, S3) must implement
 * this interface to ensure consistent behavior across environments.
 *
 * ## Method Categories
 *
 * ### Basic Operations
 * - `read()` - Read entire file contents
 * - `write()` - Write/overwrite file contents
 * - `delete()` - Delete a file (idempotent)
 * - `exists()` - Check if file exists
 * - `stat()` - Get file metadata
 * - `list()` - List files with prefix
 *
 * ### Efficient Reads
 * - `readRange()` - Read byte range (for Parquet footer reading)
 *
 * ### Concurrency Control
 * - `writeConditional()` - Atomic write with version check
 * - `getVersion()` - Get current file version
 *
 * ## Concurrency Limitations
 *
 * The write locks used by `writeConditional()` are **process-local only**.
 * They prevent concurrent writes within a single Node.js process or Worker
 * instance, but provide NO coordination across distributed deployments.
 *
 * For multi-instance deployments (multiple Workers, multiple processes,
 * multiple servers), you should either:
 * - Use external distributed locking (Redis, DynamoDB, etc.)
 * - Rely on storage-level conditional writes (note: check-then-write is not
 *   atomic across the network)
 * - Design for single-writer access per table
 *
 * The ETag/version-based conditional writes provide **optimistic concurrency
 * control** - they will detect and reject conflicting writes, but the
 * check-and-write is not atomic for cloud storage backends.
 *
 * @public
 */
export interface StorageBackend {
  /**
   * Read the entire contents of a file.
   *
   * @param path - Path to the file (relative to storage root)
   * @returns Promise resolving to file contents as Uint8Array
   * @throws {FileNotFoundError} If file does not exist
   */
  read(path: string): Promise<Uint8Array>

  /**
   * Write data to a file, creating it if it doesn't exist or overwriting if it does.
   *
   * @param path - Path to the file (relative to storage root)
   * @param data - Data to write
   * @returns Promise that resolves when write is complete
   */
  write(path: string, data: Uint8Array): Promise<void>

  /**
   * List all files matching a prefix.
   *
   * @param prefix - Prefix to match (e.g., "data/" for all files in data directory)
   * @returns Promise resolving to array of file paths (not directories)
   */
  list(prefix: string): Promise<string[]>

  /**
   * Delete a file. This operation is idempotent - deleting a non-existent file
   * does not throw an error.
   *
   * @param path - Path to the file to delete
   * @returns Promise that resolves when delete is complete
   */
  delete(path: string): Promise<void>

  /**
   * Check if a file exists.
   *
   * @param path - Path to check
   * @returns Promise resolving to true if file exists, false otherwise
   */
  exists(path: string): Promise<boolean>

  /**
   * Get file metadata (size, last modified time, optional etag).
   *
   * @param path - Path to the file
   * @returns Promise resolving to FileStat or null if file doesn't exist
   */
  stat(path: string): Promise<FileStat | null>

  /**
   * Read a byte range from a file. Essential for efficient Parquet file reading
   * where metadata is stored at the end of the file.
   *
   * @param path - Path to the file
   * @param start - Starting byte offset (inclusive)
   * @param end - Ending byte offset (exclusive)
   * @returns Promise resolving to the requested byte range
   * @throws {FileNotFoundError} If file does not exist
   */
  readRange(path: string, start: number, end: number): Promise<Uint8Array>

  /**
   * Conditionally write a file only if the version matches.
   * This enables optimistic concurrency control for Delta Lake transactions.
   *
   * Use cases:
   * - `expectedVersion = null`: Create file only if it doesn't exist
   * - `expectedVersion = "version"`: Update file only if version matches
   *
   * ## Concurrency Note
   *
   * The internal write locks are **process-local only**. For distributed
   * deployments, concurrent writes from different processes/instances may
   * result in VersionMismatchError when the version check fails. This is
   * the expected behavior for optimistic concurrency control - callers
   * should retry with the new version on conflict.
   *
   * @param path - Path to the file
   * @param data - Data to write
   * @param expectedVersion - Expected version/etag, or null for create-if-not-exists
   * @returns The new version string after successful write
   * @throws {VersionMismatchError} If the current version doesn't match expected
   */
  writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string>

  /**
   * Get the current version of a file.
   *
   * The version string format varies by implementation:
   * - MemoryStorage: Auto-generated unique strings
   * - FileSystemStorage: File modification time (mtime in ms)
   * - R2Storage: ETag
   * - S3Storage: ETag
   *
   * @param path - Path to the file
   * @returns Promise resolving to version string, or null if file doesn't exist
   */
  getVersion(path: string): Promise<string | null>
}

/**
 * File metadata returned by stat().
 *
 * @public
 */
export interface FileStat {
  /** File size in bytes */
  size: number

  /** Last modification time */
  lastModified: Date

  /** Optional ETag/version identifier (present for R2/S3) */
  etag?: string
}

// =============================================================================
// STORAGE OPTIONS
// =============================================================================

/**
 * Configuration options for creating a storage backend.
 *
 * @public
 *
 * @example
 * ```typescript
 * // Memory storage (for testing)
 * createStorage({ type: 'memory' })
 *
 * // Filesystem storage (for local development)
 * createStorage({ type: 'filesystem', path: './data' })
 *
 * // R2 storage (for Cloudflare Workers)
 * createStorage({ type: 'r2', bucket: env.MY_BUCKET })
 *
 * // S3 storage (for AWS)
 * createStorage({
 *   type: 's3',
 *   bucket: 'my-bucket',
 *   region: 'us-east-1',
 *   credentials: { accessKeyId: '...', secretAccessKey: '...' }
 * })
 * ```
 */
export type StorageOptions =
  | { readonly type: 'filesystem'; readonly path: string }
  | { readonly type: 'r2'; readonly bucket: R2Bucket }
  | { readonly type: 's3'; readonly bucket: string; readonly region: string; readonly credentials?: S3Credentials }
  | { readonly type: 'memory' }

/**
 * AWS S3 credentials for S3Storage.
 * If not provided, S3Storage will use environment credentials or IAM roles.
 *
 * @public
 */
export interface S3Credentials {
  /** AWS Access Key ID */
  readonly accessKeyId: string
  /** AWS Secret Access Key */
  readonly secretAccessKey: string
}

// =============================================================================
// FACTORY
// =============================================================================

/**
 * Parsed storage URL result.
 * Contains the storage type and extracted configuration from the URL.
 *
 * @public
 */
export interface ParsedStorageUrl {
  /** Detected storage type */
  type: 'filesystem' | 'memory' | 's3' | 'r2'
  /** Path within the storage (for filesystem, bucket prefix for cloud storage) */
  path: string
  /** Bucket name (for s3:// and r2:// URLs) */
  bucket?: string
  /** AWS region (for s3:// URLs, extracted from hostname or defaulted to us-east-1) */
  region?: string
}

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

// =============================================================================
// IMPLEMENTATIONS
// =============================================================================

/**
 * File system storage backend for local development.
 *
 * Uses Node.js fs/promises for file operations. All paths are relative to
 * the configured base path and are protected against path traversal attacks.
 *
 * ## Version Tracking
 * Uses file modification time (mtime in milliseconds) as the version identifier
 * for conditional writes.
 *
 * ## Concurrency
 * Uses in-memory write locks to serialize concurrent writes to the same file.
 * Atomic writes are performed using temp file + rename pattern.
 *
 * @public
 *
 * @example
 * ```typescript
 * const storage = new FileSystemStorage({ path: './data' })
 * await storage.write('file.txt', new TextEncoder().encode('hello'))
 * ```
 */
export class FileSystemStorage implements StorageBackend {
  private basePath: string
  private writeLocks = new Map<string, Lock>()

  constructor(private options: { path: string }) {
    // Validate options
    if (!options || typeof options !== 'object') {
      throw new ValidationError('options is required and must be an object', 'options', options)
    }

    // Validate path is required and is a string
    if (options.path === null || options.path === undefined) {
      throw new ValidationError('options.path is required', 'options.path', options.path)
    }
    if (typeof options.path !== 'string') {
      throw new ValidationError('options.path must be a string', 'options.path', options.path)
    }
    // Note: Empty path is allowed (root directory)

    this.basePath = options.path
  }

  /**
   * Resolve and validate a path relative to basePath.
   * Prevents path traversal attacks and normalizes paths.
   *
   * Security measures:
   * 1. Reject null bytes (can truncate paths in some systems)
   * 2. Decode URL-encoded characters (prevents %2e%2e bypass)
   * 3. Canonicalize path using path.resolve()
   * 4. Verify resolved path is within base directory (startsWith check)
   */
  private async resolvePath(relativePath: string): Promise<string> {
    const path = await import('node:path')

    // Check for null bytes (before any decoding to catch all variants)
    if (relativePath.includes('\0')) {
      throw new ValidationError('Invalid path: contains null byte', 'path', relativePath)
    }

    // Decode URL-encoded characters to prevent bypass attacks
    // e.g., %2e%2e = .., %2f = /, %00 = null byte
    let decodedPath: string
    try {
      decodedPath = decodeURIComponent(relativePath)
    } catch (e) {
      // If decoding fails (malformed encoding), use original path
      // This is a security measure - log but don't fail
      getLogger().warn(`[Storage] Failed to decode URI component for path "${relativePath}", using original:`, e)
      decodedPath = relativePath
    }

    // Check for null bytes again after decoding (catches %00)
    if (decodedPath.includes('\0')) {
      throw new ValidationError('Invalid path: contains null byte (after decoding)', 'path', relativePath)
    }

    // Canonicalize the base path first
    const resolvedBasePath = path.resolve(this.basePath)

    // Normalize the path (removes double slashes, resolves . and ..)
    let normalizedPath = path.normalize(decodedPath)

    // Remove leading slashes to ensure it's treated as relative
    normalizedPath = normalizedPath.replace(/^\/+/, '')

    // Resolve full path - this canonicalizes the path and resolves all . and ..
    const fullPath = path.resolve(resolvedBasePath, normalizedPath)

    // Final security check: ensure the resolved path is within the base directory
    // Use startsWith with path separator to prevent prefix attacks:
    // e.g., basePath="/data" should not allow "/data-evil/file"
    if (!fullPath.startsWith(resolvedBasePath + path.sep) && fullPath !== resolvedBasePath) {
      throw new ValidationError('Invalid path: outside base directory', 'path', relativePath)
    }

    return fullPath
  }

  async read(path: string): Promise<Uint8Array> {
    const fs = await import('node:fs/promises')
    const fullPath = await this.resolvePath(path)

    try {
      const buffer = await fs.readFile(fullPath)
      return new Uint8Array(buffer)
    } catch (error: unknown) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
        throw new FileNotFoundError(path, 'read')
      }
      throw error
    }
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    const fs = await import('node:fs/promises')
    const nodePath = await import('node:path')
    const fullPath = await this.resolvePath(path)

    // Create parent directories if they don't exist
    const dirPath = nodePath.dirname(fullPath)
    await fs.mkdir(dirPath, { recursive: true })

    // Write the file
    await fs.writeFile(fullPath, data)
  }

  async list(prefix: string): Promise<string[]> {
    const fs = await import('node:fs/promises')
    const path = await import('node:path')

    const results: string[] = []

    // Recursively walk directory tree
    const walk = async (dir: string, relativePrefix: string = '') => {
      try {
        const entries = await fs.readdir(dir, { withFileTypes: true })

        for (const entry of entries) {
          const relativePath = relativePrefix ? `${relativePrefix}/${entry.name}` : entry.name
          const fullPath = path.join(dir, entry.name)

          if (entry.isDirectory()) {
            await walk(fullPath, relativePath)
          } else if (entry.isFile()) {
            // Only include files that start with the prefix
            if (relativePath.startsWith(prefix)) {
              results.push(relativePath)
            }
          }
        }
      } catch (error: unknown) {
        // If directory doesn't exist, just return empty results
        if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
          throw error
        }
      }
    }

    await walk(this.basePath)
    return results
  }

  async delete(path: string): Promise<void> {
    const fs = await import('node:fs/promises')
    const fullPath = await this.resolvePath(path)

    try {
      await fs.unlink(fullPath)
    } catch (error: unknown) {
      // Idempotent delete - don't throw if file doesn't exist
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
        throw error
      }
    }
  }

  async exists(path: string): Promise<boolean> {
    const fs = await import('node:fs/promises')
    const fullPath = await this.resolvePath(path)

    try {
      const stats = await fs.stat(fullPath)
      // Only return true for files, not directories
      return stats.isFile()
    } catch {
      // Intentionally silent: ENOENT is expected when file doesn't exist
      // Returning false is the correct behavior for exists()
      return false
    }
  }

  async stat(path: string): Promise<FileStat | null> {
    const fs = await import('node:fs/promises')
    const fullPath = await this.resolvePath(path)

    try {
      const stats = await fs.stat(fullPath)

      // Only return stats for files, not directories
      if (!stats.isFile()) {
        return null
      }

      // Use mtimeMs for more reliable timestamp in some environments
      const lastModified = new Date(stats.mtimeMs)

      return {
        size: stats.size,
        lastModified,
      }
    } catch {
      // Intentionally silent: ENOENT is expected when file doesn't exist
      // Returning null is the correct behavior for stat()
      return null
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const fs = await import('node:fs/promises')
    const fullPath = await this.resolvePath(path)

    const length = end - start

    // Handle zero-length range
    if (length <= 0) {
      return new Uint8Array(0)
    }

    // Check if file exists first (workaround for fs.open creating files in some environments)
    try {
      await fs.access(fullPath)
    } catch (error: unknown) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
        throw new FileNotFoundError(path, 'readRange')
      }
      throw error
    }

    // Get file size to clamp the end position
    const stats = await fs.stat(fullPath)
    const clampedEnd = Math.min(end, stats.size)
    const clampedLength = Math.max(0, clampedEnd - start)

    // Handle case where start is beyond file size
    if (clampedLength <= 0) {
      return new Uint8Array(0)
    }

    // Use file handle for efficient byte-range reads
    const fileHandle = await fs.open(fullPath, 'r')
    try {
      const buffer = Buffer.allocUnsafe(clampedLength)
      const { bytesRead } = await fileHandle.read(buffer, 0, clampedLength, start)
      // Return only the bytes that were actually read
      return new Uint8Array(buffer.subarray(0, bytesRead))
    } finally {
      await fileHandle.close()
    }
  }

  async getVersion(path: string): Promise<string | null> {
    const fs = await import('node:fs/promises')
    const fullPath = await this.resolvePath(path)

    try {
      const stats = await fs.stat(fullPath)
      if (!stats.isFile()) {
        return null
      }
      // Use mtime in milliseconds as version string
      return stats.mtimeMs.toString()
    } catch {
      // Intentionally silent: ENOENT is expected when file doesn't exist
      // Returning null indicates no version (file not found)
      return null
    }
  }

  async writeConditional(filePath: string, data: Uint8Array, expectedVersion: string | null): Promise<string> {
    const fs = await import('node:fs/promises')
    const nodePath = await import('node:path')
    const fullPath = await this.resolvePath(filePath)

    // Acquire lock for this path - properly handles concurrent access
    const lock = await acquireWriteLock(this.writeLocks, filePath)

    try {
      // Check current version
      let currentVersion: string | null = null
      try {
        const stats = await fs.stat(fullPath)
        if (stats.isFile()) {
          currentVersion = stats.mtimeMs.toString()
        }
      } catch (error: unknown) {
        if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
          throw error
        }
        // File doesn't exist, currentVersion remains null
      }

      if (expectedVersion === null) {
        // Create-if-not-exists: file should not exist
        if (currentVersion !== null) {
          throw new VersionMismatchError(filePath, expectedVersion, currentVersion)
        }
      } else {
        // Update: version should match
        if (currentVersion !== expectedVersion) {
          throw new VersionMismatchError(filePath, expectedVersion, currentVersion)
        }
      }

      // Version matches, perform the write
      // Create parent directories if they don't exist
      const dirPath = nodePath.dirname(fullPath)
      await fs.mkdir(dirPath, { recursive: true })

      // Write to a temp file first, then rename for atomicity
      const tempPath = `${fullPath}.tmp.${Date.now()}.${Math.random().toString(36).substring(2, 9)}`
      await fs.writeFile(tempPath, data)
      await fs.rename(tempPath, fullPath)

      // Get the new version after write
      const newStats = await fs.stat(fullPath)
      return newStats.mtimeMs.toString()
    } finally {
      releaseWriteLock(this.writeLocks, filePath, lock)
    }
  }
}

/**
 * Cloudflare R2 storage backend.
 *
 * Uses the Cloudflare R2 API for all file operations. Always delegates
 * to the R2 bucket - no in-memory fallback.
 *
 * ## Version Tracking
 * Uses R2 object ETags as version identifiers for conditional writes.
 *
 * ## Features
 * - Efficient byte-range reads using R2's range parameter
 * - Automatic pagination for list operations
 * - ETag-based conditional writes for optimistic concurrency
 *
 * @public
 *
 * @example
 * ```typescript
 * // In a Cloudflare Worker
 * const storage = new R2Storage({ bucket: env.MY_BUCKET })
 * ```
 */
export class R2Storage implements StorageBackend {
  private writeLocks = new Map<string, Lock>()

  constructor(private options: { bucket: R2Bucket }) {
    // Validate options
    if (!options || typeof options !== 'object') {
      throw new ValidationError('options is required and must be an object', 'options', options)
    }

    // Validate bucket is required
    if (options.bucket === null || options.bucket === undefined) {
      throw new ValidationError('options.bucket is required (R2Bucket instance)', 'options.bucket', options.bucket)
    }

    // Validate bucket has required R2Bucket methods
    if (typeof options.bucket.get !== 'function' ||
        typeof options.bucket.put !== 'function' ||
        typeof options.bucket.list !== 'function') {
      throw new ValidationError(
        'options.bucket must be a valid R2Bucket instance with get, put, and list methods',
        'options.bucket',
        options.bucket
      )
    }
  }

  async read(path: string): Promise<Uint8Array> {
    const object = await this.options.bucket.get(path)
    if (!object) {
      throw new FileNotFoundError(path, 'read')
    }
    const arrayBuffer = await object.arrayBuffer()
    return new Uint8Array(arrayBuffer)
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    await this.options.bucket.put(path, data)
  }

  async list(prefix: string): Promise<string[]> {
    const results: string[] = []
    let cursor: string | undefined = undefined
    let truncated = true

    while (truncated) {
      const response = await this.options.bucket.list({
        prefix,
        ...(cursor ? { cursor } : {}),
      })

      results.push(...response.objects.map(obj => obj.key))
      truncated = response.truncated
      if (response.truncated) {
        cursor = response.cursor
      }
    }

    return results
  }

  async delete(path: string): Promise<void> {
    await this.options.bucket.delete(path)
  }

  async exists(path: string): Promise<boolean> {
    const object = await this.options.bucket.head(path)
    return object !== null
  }

  async stat(path: string): Promise<FileStat | null> {
    const object = await this.options.bucket.head(path)
    if (!object) {
      return null
    }
    return {
      size: object.size,
      lastModified: object.uploaded,
      etag: object.etag,
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const length = end - start
    const object = await this.options.bucket.get(path, {
      range: { offset: start, length },
    })
    if (!object) {
      throw new FileNotFoundError(path, 'readRange')
    }
    const arrayBuffer = await object.arrayBuffer()
    return new Uint8Array(arrayBuffer)
  }

  async getVersion(path: string): Promise<string | null> {
    const object = await this.options.bucket.head(path)
    if (!object) {
      return null
    }
    return object.etag
  }

  async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string> {
    // Acquire lock for this path - properly handles concurrent access
    const lock = await acquireWriteLock(this.writeLocks, path)

    try {
      // Check current version from R2
      const currentVersion = await this.getVersion(path)

      if (expectedVersion === null) {
        // Create-if-not-exists: file should not exist
        if (currentVersion !== null) {
          throw new VersionMismatchError(path, expectedVersion, currentVersion)
        }
      } else {
        // Update: version should match
        if (currentVersion !== expectedVersion) {
          throw new VersionMismatchError(path, expectedVersion, currentVersion)
        }
      }

      // Version matches, perform the write
      await this.options.bucket.put(path, data)

      // Get the new version (ETag) after write
      const newVersion = await this.getVersion(path)
      return newVersion ?? ''
    } finally {
      releaseWriteLock(this.writeLocks, path, lock)
    }
  }
}

// =============================================================================
// S3 COMMAND AND RESPONSE TYPES
// =============================================================================

/**
 * S3 streaming body interface - matches AWS SDK v3 streaming body.
 *
 * @internal
 */
interface S3StreamingBody {
  transformToByteArray(): Promise<Uint8Array>
}

/**
 * GetObject command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3GetObjectCommand {
  readonly _type: 'GetObject'
  Bucket: string
  Key: string
  Range?: string
}

/**
 * GetObject command output.
 *
 * @internal
 */
interface S3GetObjectOutput {
  Body?: S3StreamingBody
  ContentLength?: number
  LastModified?: Date
  ETag?: string
}

/**
 * PutObject command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3PutObjectCommand {
  readonly _type: 'PutObject'
  Bucket: string
  Key: string
  Body: Uint8Array
}

/**
 * HeadObject command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3HeadObjectCommand {
  readonly _type: 'HeadObject'
  Bucket: string
  Key: string
}

/**
 * HeadObject command output.
 *
 * @internal
 */
interface S3HeadObjectOutput {
  ContentLength?: number
  LastModified?: Date
  ETag?: string
}

/**
 * DeleteObject command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3DeleteObjectCommand {
  readonly _type: 'DeleteObject'
  Bucket: string
  Key: string
}

/**
 * ListObjectsV2 command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3ListObjectsV2Command {
  readonly _type: 'ListObjectsV2'
  Bucket: string
  Prefix: string
  ContinuationToken?: string
}

/**
 * ListObjectsV2 command output.
 *
 * @internal
 */
interface S3ListObjectsV2Output {
  Contents?: Array<{ Key?: string }>
  IsTruncated?: boolean
  NextContinuationToken?: string
}

/**
 * CreateMultipartUpload command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3CreateMultipartUploadCommand {
  readonly _type: 'CreateMultipartUpload'
  Bucket: string
  Key: string
}

/**
 * CreateMultipartUpload command output.
 *
 * @internal
 */
interface S3CreateMultipartUploadOutput {
  UploadId?: string
}

/**
 * UploadPart command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3UploadPartCommand {
  readonly _type: 'UploadPart'
  Bucket: string
  Key: string
  UploadId: string
  PartNumber: number
  Body: Uint8Array
}

/**
 * UploadPart command output.
 *
 * @internal
 */
interface S3UploadPartOutput {
  ETag?: string
}

/**
 * CompleteMultipartUpload command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3CompleteMultipartUploadCommand {
  readonly _type: 'CompleteMultipartUpload'
  Bucket: string
  Key: string
  UploadId: string
  MultipartUpload: {
    Parts: Array<{ ETag: string; PartNumber: number }>
  }
}

/**
 * AbortMultipartUpload command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
interface S3AbortMultipartUploadCommand {
  readonly _type: 'AbortMultipartUpload'
  Bucket: string
  Key: string
  UploadId: string
}

/**
 * Union of all S3 command types.
 * Uses discriminated union pattern with _type field for type safety.
 *
 * @internal
 */
type S3Command =
  | S3GetObjectCommand
  | S3PutObjectCommand
  | S3HeadObjectCommand
  | S3DeleteObjectCommand
  | S3ListObjectsV2Command
  | S3CreateMultipartUploadCommand
  | S3UploadPartCommand
  | S3CompleteMultipartUploadCommand
  | S3AbortMultipartUploadCommand

/**
 * Maps S3 command types to their corresponding response types.
 * This type-level mapping enables type-safe send() with proper inference.
 *
 * @internal
 */
type S3ResponseMap = {
  GetObject: S3GetObjectOutput
  PutObject: void
  HeadObject: S3HeadObjectOutput
  DeleteObject: void
  ListObjectsV2: S3ListObjectsV2Output
  CreateMultipartUpload: S3CreateMultipartUploadOutput
  UploadPart: S3UploadPartOutput
  CompleteMultipartUpload: void
  AbortMultipartUpload: void
}

/**
 * S3Client interface - matches AWS SDK v3 S3Client.send() pattern.
 * Uses discriminated union with conditional type for type-safe command/response mapping.
 * This allows for easy mocking in tests while maintaining full type safety.
 *
 * The send() method uses the command's _type discriminator to infer the correct
 * response type at compile time, eliminating the need for type assertions.
 *
 * @public
 */
export interface S3ClientLike {
  send<T extends S3Command>(command: T): Promise<S3ResponseMap[T['_type']]>
}

/**
 * Configuration options for S3Storage.
 *
 * @public
 */
export interface S3StorageOptions {
  /** S3 bucket name */
  readonly bucket: string
  /** AWS region (e.g., 'us-east-1') */
  readonly region: string
  /** Optional AWS credentials. If not provided, uses environment credentials or IAM roles. */
  readonly credentials?: S3Credentials
  /** Optional S3 client instance for dependency injection (useful for testing or custom clients) */
  readonly client?: S3ClientLike
}

/**
 * AWS S3 storage backend.
 *
 * Uses AWS SDK v3 patterns for S3 operations. Supports:
 * - GetObject, PutObject, HeadObject, ListObjectsV2, DeleteObject
 * - Multipart upload for files >5MB
 * - Byte-range reads for efficient Parquet file access
 * - Proper error handling for NoSuchKey, AccessDenied, NoSuchBucket
 *
 * ## Version Tracking
 * Uses S3 object ETags as version identifiers for conditional writes.
 *
 * ## Multipart Uploads
 * Files larger than 5MB are automatically uploaded using multipart upload
 * with 5MB part sizes. Failed uploads are automatically aborted.
 *
 * ## Client Injection
 * The S3 client can be injected via constructor options (preferred) or set directly:
 * - Constructor injection: `new S3Storage({ bucket, region, client: myS3Client })`
 * - Direct assignment: `storage._client = mockClient`
 * Without a client, operations will throw "S3 client not initialized".
 *
 * @example
 * ```typescript
 * // Basic usage (requires client injection before operations)
 * const storage = new S3Storage({
 *   bucket: 'my-bucket',
 *   region: 'us-east-1',
 *   credentials: {
 *     accessKeyId: 'AKIA...',
 *     secretAccessKey: '...'
 *   }
 * })
 *
 * // With client injection (production use)
 * import { S3Client } from '@aws-sdk/client-s3'
 * const s3Client = new S3Client({ region: 'us-east-1' })
 * const storage = new S3Storage({
 *   bucket: 'my-bucket',
 *   region: 'us-east-1',
 *   client: s3Client  // Client injected via constructor
 * })
 *
 * // With mock client (testing)
 * const storage = new S3Storage({
 *   bucket: 'test-bucket',
 *   region: 'us-east-1',
 *   client: mockS3Client
 * })
 * ```
 *
 * @public
 */
export class S3Storage implements StorageBackend {
  private writeLocks = new Map<string, Lock>()

  // Internal client - can be injected via constructor options or set directly
  public _client: S3ClientLike | null = null

  // Multipart upload threshold: >5MB uses multipart
  private static readonly MULTIPART_THRESHOLD = 5 * 1024 * 1024
  // Part size for multipart uploads: 5MB
  private static readonly PART_SIZE = 5 * 1024 * 1024

  constructor(public readonly options: S3StorageOptions) {
    // Validate options
    if (!options || typeof options !== 'object') {
      throw new ValidationError('options is required and must be an object', 'options', options)
    }

    // Validate bucket is required and is a non-empty string
    if (options.bucket === null || options.bucket === undefined) {
      throw new ValidationError('options.bucket is required', 'options.bucket', options.bucket)
    }
    if (typeof options.bucket !== 'string') {
      throw new ValidationError('options.bucket must be a string', 'options.bucket', options.bucket)
    }
    if (options.bucket.trim() === '') {
      throw new ValidationError('options.bucket cannot be an empty string', 'options.bucket', options.bucket)
    }

    // Validate region is required and is a non-empty string
    if (options.region === null || options.region === undefined) {
      throw new ValidationError('options.region is required', 'options.region', options.region)
    }
    if (typeof options.region !== 'string') {
      throw new ValidationError('options.region must be a string', 'options.region', options.region)
    }
    if (options.region.trim() === '') {
      throw new ValidationError('options.region cannot be an empty string', 'options.region', options.region)
    }

    // Validate credentials if provided
    if (options.credentials !== undefined && options.credentials !== null) {
      if (typeof options.credentials !== 'object') {
        throw new ValidationError('options.credentials must be an object', 'options.credentials', options.credentials)
      }
      if (typeof options.credentials.accessKeyId !== 'string' || options.credentials.accessKeyId.trim() === '') {
        throw new ValidationError(
          'options.credentials.accessKeyId must be a non-empty string',
          'options.credentials.accessKeyId',
          options.credentials.accessKeyId
        )
      }
      if (typeof options.credentials.secretAccessKey !== 'string' || options.credentials.secretAccessKey.trim() === '') {
        throw new ValidationError(
          'options.credentials.secretAccessKey must be a non-empty string',
          'options.credentials.secretAccessKey',
          options.credentials.secretAccessKey
        )
      }
    }

    // Use client from options if provided
    if (options.client) {
      this._client = options.client
    }
  }

  /**
   * Get the S3 client, throwing if not initialized
   */
  private getClient(): S3ClientLike {
    if (!this._client) {
      throw new StorageError('S3 client not initialized. Set _client before performing operations.', '', 'getClient')
    }
    return this._client
  }

  /**
   * Normalize a path for S3 (remove leading slashes, handle special cases)
   */
  private normalizePath(path: string): string {
    // Remove leading slashes - S3 keys shouldn't start with /
    return path.replace(/^\/+/, '')
  }

  /**
   * Check if an error is a NoSuchKey error
   */
  private isNoSuchKeyError(error: unknown): boolean {
    if (typeof error !== 'object' || error === null) return false
    const err = error as { name?: string; $metadata?: { httpStatusCode?: number } }
    return err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404
  }

  /**
   * Check if an error is an AccessDenied error
   */
  private isAccessDeniedError(error: unknown): boolean {
    if (typeof error !== 'object' || error === null) return false
    const err = error as { name?: string; $metadata?: { httpStatusCode?: number } }
    return err.name === 'AccessDenied' || err.$metadata?.httpStatusCode === 403
  }

  /**
   * Wrap an S3 error with a meaningful message
   */
  private wrapError(error: unknown, operation: string, path: string): Error {
    const errorName = (typeof error === 'object' && error !== null && 'name' in error)
      ? (error as { name: string }).name
      : 'UnknownError'

    if (this.isAccessDeniedError(error)) {
      return new S3Error(`Access denied: ${path}`, 'AccessDenied', 403, path)
    }

    if (errorName === 'NoSuchBucket') {
      return new S3Error(`Bucket does not exist: ${this.options.bucket}`, 'NoSuchBucket', 404, path)
    }

    if (this.isNoSuchKeyError(error)) {
      return new S3Error(`File not found: ${path} (NoSuchKey)`, 'NoSuchKey', 404, path)
    }

    // Return original error for other cases
    return error instanceof Error ? error : new Error(String(error))
  }

  async read(path: string): Promise<Uint8Array> {
    const client = this.getClient()
    const key = this.normalizePath(path)

    try {
      const command: S3GetObjectCommand = {
        _type: 'GetObject',
        Bucket: this.options.bucket,
        Key: key,
      }
      const response = await client.send(command)

      if (!response.Body) {
        throw new StorageError(`Empty response body from S3`, path, 'read')
      }

      // AWS SDK v3 returns a streaming body that needs to be converted
      return await response.Body.transformToByteArray()
    } catch (error: unknown) {
      throw this.wrapError(error, 'read', path)
    }
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    const client = this.getClient()
    const key = this.normalizePath(path)

    try {
      // Use multipart upload for large files (>5MB)
      if (data.length > S3Storage.MULTIPART_THRESHOLD) {
        await this.multipartUpload(key, data)
      } else {
        // Simple PutObject for small files
        const command: S3PutObjectCommand = {
          _type: 'PutObject',
          Bucket: this.options.bucket,
          Key: key,
          Body: data,
        }
        await client.send(command)
      }
    } catch (error: unknown) {
      throw this.wrapError(error, 'write', path)
    }
  }

  /**
   * Perform a multipart upload for large files
   */
  private async multipartUpload(key: string, data: Uint8Array): Promise<void> {
    const client = this.getClient()
    let uploadId: string | undefined

    try {
      // Step 1: Create multipart upload
      const createCommand: S3CreateMultipartUploadCommand = {
        _type: 'CreateMultipartUpload',
        Bucket: this.options.bucket,
        Key: key,
      }
      const createResponse = await client.send(createCommand)
      uploadId = createResponse.UploadId

      if (!uploadId) {
        throw new StorageError('Failed to initiate multipart upload: no upload ID returned', key, 'multipartUpload')
      }

      // Step 2: Upload parts
      const parts: Array<{ ETag: string; PartNumber: number }> = []
      const partSize = S3Storage.PART_SIZE
      const numParts = Math.ceil(data.length / partSize)

      for (let i = 0; i < numParts; i++) {
        const start = i * partSize
        const end = Math.min(start + partSize, data.length)
        const partData = data.slice(start, end)

        const uploadPartCommand: S3UploadPartCommand = {
          _type: 'UploadPart',
          Bucket: this.options.bucket,
          Key: key,
          UploadId: uploadId,
          PartNumber: i + 1,
          Body: partData,
        }

        const partResponse = await client.send(uploadPartCommand)

        if (!partResponse.ETag) {
          throw new StorageError(`Failed to upload part ${i + 1}: no ETag returned`, key, 'multipartUpload')
        }

        parts.push({
          ETag: partResponse.ETag,
          PartNumber: i + 1,
        })
      }

      // Step 3: Complete multipart upload
      const completeCommand: S3CompleteMultipartUploadCommand = {
        _type: 'CompleteMultipartUpload',
        Bucket: this.options.bucket,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: parts,
        },
      }
      await client.send(completeCommand)
    } catch (error) {
      // Abort multipart upload on failure
      if (uploadId) {
        try {
          const abortCommand: S3AbortMultipartUploadCommand = {
            _type: 'AbortMultipartUpload',
            Bucket: this.options.bucket,
            Key: key,
            UploadId: uploadId,
          }
          await client.send(abortCommand)
        } catch (abortError) {
          // Abort failed - log but still propagate original error
          getLogger().warn(`[S3Storage] Failed to abort multipart upload ${uploadId}:`, abortError)
        }
      }
      throw error
    }
  }

  async list(prefix: string): Promise<string[]> {
    const client = this.getClient()
    const normalizedPrefix = this.normalizePath(prefix)
    const results: string[] = []
    let continuationToken: string | undefined

    try {
      do {
        const command: S3ListObjectsV2Command = {
          _type: 'ListObjectsV2',
          Bucket: this.options.bucket,
          Prefix: normalizedPrefix,
          ...(continuationToken && { ContinuationToken: continuationToken }),
        }

        const response = await client.send(command)

        if (response.Contents) {
          for (const obj of response.Contents) {
            // Filter out directory markers (keys ending with /)
            if (obj.Key && !obj.Key.endsWith('/')) {
              results.push(obj.Key)
            }
          }
        }

        continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined
      } while (continuationToken)

      return results
    } catch (error: unknown) {
      throw this.wrapError(error, 'list', prefix)
    }
  }

  async delete(path: string): Promise<void> {
    const client = this.getClient()
    const key = this.normalizePath(path)

    try {
      const command: S3DeleteObjectCommand = {
        _type: 'DeleteObject',
        Bucket: this.options.bucket,
        Key: key,
      }
      await client.send(command)
    } catch (error: unknown) {
      throw this.wrapError(error, 'delete', path)
    }
  }

  async exists(path: string): Promise<boolean> {
    const client = this.getClient()
    const key = this.normalizePath(path)

    try {
      const command: S3HeadObjectCommand = {
        _type: 'HeadObject',
        Bucket: this.options.bucket,
        Key: key,
      }
      await client.send(command)
      return true
    } catch (error: unknown) {
      if (this.isNoSuchKeyError(error)) {
        return false
      }
      throw this.wrapError(error, 'exists', path)
    }
  }

  async stat(path: string): Promise<FileStat | null> {
    const client = this.getClient()
    const key = this.normalizePath(path)

    try {
      const command: S3HeadObjectCommand = {
        _type: 'HeadObject',
        Bucket: this.options.bucket,
        Key: key,
      }
      const response = await client.send(command)

      const result: FileStat = {
        size: response.ContentLength ?? 0,
        lastModified: response.LastModified ?? new Date(),
      }
      if (response.ETag !== undefined) {
        result.etag = response.ETag
      }
      return result
    } catch (error: unknown) {
      if (this.isNoSuchKeyError(error)) {
        return null
      }
      throw this.wrapError(error, 'stat', path)
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    const client = this.getClient()
    const key = this.normalizePath(path)

    // Handle zero-length range
    if (start >= end) {
      return new Uint8Array(0)
    }

    try {
      // S3 Range header is inclusive on both ends, so we subtract 1 from end
      // Range: bytes=start-end (inclusive)
      const command: S3GetObjectCommand = {
        _type: 'GetObject',
        Bucket: this.options.bucket,
        Key: key,
        Range: `bytes=${start}-${end - 1}`,
      }
      const response = await client.send(command)

      if (!response.Body) {
        throw new StorageError(`Empty response body from S3 for range read`, path, 'readRange')
      }

      return await response.Body.transformToByteArray()
    } catch (error: unknown) {
      throw this.wrapError(error, 'readRange', path)
    }
  }

  async getVersion(path: string): Promise<string | null> {
    const stat = await this.stat(path)
    return stat?.etag ?? null
  }

  async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string> {
    // Acquire lock for this path - properly handles concurrent access
    const lock = await acquireWriteLock(this.writeLocks, path)

    try {
      // Get current version
      const currentVersion = await this.getVersion(path)

      if (expectedVersion === null) {
        // Create-if-not-exists: file should not exist
        if (currentVersion !== null) {
          throw new VersionMismatchError(path, expectedVersion, currentVersion)
        }
      } else {
        // Update: version should match
        if (currentVersion !== expectedVersion) {
          throw new VersionMismatchError(path, expectedVersion, currentVersion)
        }
      }

      // Version matches, perform the write
      await this.write(path, data)

      // Return the new version
      const newVersion = await this.getVersion(path)
      return newVersion ?? ''
    } finally {
      releaseWriteLock(this.writeLocks, path, lock)
    }
  }
}

// =============================================================================
// MEMORY STORAGE CONSTANTS
// =============================================================================

/** Version string prefix for MemoryStorage version identifiers */
const VERSION_PREFIX = 'v'

/** Number of random characters in version string suffix */
const VERSION_RANDOM_LENGTH = 7

/** Base for random string generation */
const RANDOM_STRING_BASE = 36

/** Start index for substring extraction in random generation */
const RANDOM_SUBSTRING_START = 2

// =============================================================================
// MEMORY STORAGE TYPES
// =============================================================================

/**
 * Supported operation types for tracking.
 *
 * @public
 */
export type MemoryStorageOperation = 'read' | 'write' | 'delete' | 'list' | 'exists' | 'stat' | 'readRange'

/**
 * Latency configuration for simulating slow storage operations.
 *
 * @public
 */
export interface LatencyConfig {
  /** Simulated read latency in milliseconds */
  readonly read?: number
  /** Simulated write latency in milliseconds */
  readonly write?: number
  /** Simulated delete latency in milliseconds */
  readonly delete?: number
  /** Simulated list latency in milliseconds */
  readonly list?: number
}

/**
 * Configuration options for MemoryStorage testing utilities.
 *
 * @public
 */
export interface MemoryStorageOptions {
  /** Optional simulated latency for operations in milliseconds */
  readonly latency?: LatencyConfig
  /** Optional maximum storage size in bytes */
  readonly maxSize?: number
}

/**
 * Operation record for tracking storage operations (testing utility).
 *
 * @public
 */
export interface OperationRecord {
  /** The type of operation performed */
  readonly operation: MemoryStorageOperation
  /** The path that was operated on */
  readonly path: string
  /** Unix timestamp when the operation occurred */
  readonly timestamp: number
}

/**
 * Snapshot of storage state for testing (returned by snapshot()).
 *
 * @public
 */
export interface StorageSnapshot {
  /** Map of file paths to their contents */
  readonly files: Map<string, Uint8Array>
  /** Map of file paths to their version strings */
  readonly versions: Map<string, string>
}

// =============================================================================
// MEMORY STORAGE IMPLEMENTATION
// =============================================================================

/**
 * In-memory storage backend for testing.
 *
 * Stores all data in memory using Maps. Useful for unit testing and
 * development without external dependencies.
 *
 * ## Version Tracking
 * Generates unique version strings combining a counter, timestamp,
 * and random component for conditional writes.
 *
 * ## Instance Isolation
 * Each MemoryStorage instance has its own isolated storage.
 * Multiple instances do not share data.
 *
 * ## Testing Utilities
 * Includes optional features for testing:
 * - `snapshot()` / `restore()` - Save and restore state
 * - `clear()` - Remove all files
 * - `getOperationHistory()` - Track operations
 * - Latency simulation - Simulate slow storage
 * - Size limits - Simulate storage quotas
 *
 * @public
 *
 * @example
 * ```typescript
 * const storage = new MemoryStorage()
 * await storage.write('test.txt', new TextEncoder().encode('hello'))
 * const data = await storage.read('test.txt')
 * ```
 */
export class MemoryStorage implements StorageBackend {
  // Storage state
  private readonly files = new Map<string, Uint8Array>()
  private readonly versions = new Map<string, string>()
  private readonly timestamps = new Map<string, Date>()

  // Concurrency control
  private readonly writeLocks = new Map<string, Lock>()

  // Pending latency timers (for cleanup)
  private readonly pendingTimers = new Set<ReturnType<typeof setTimeout>>()

  // Version generation
  private versionCounter = 0

  // Testing utilities state
  private readonly operationHistory: OperationRecord[] = []
  private readonly options: MemoryStorageOptions
  private currentSize = 0

  constructor(options: MemoryStorageOptions = {}) {
    // Validate options if provided (options has default value, so it's always defined)
    if (options !== null && typeof options === 'object') {
      // Validate latency options if provided
      if (options.latency !== undefined && options.latency !== null) {
        if (typeof options.latency !== 'object') {
          throw new ValidationError('options.latency must be an object', 'options.latency', options.latency)
        }
        // Validate individual latency values are non-negative numbers if provided
        for (const op of ['read', 'write', 'delete', 'list'] as const) {
          const value = options.latency[op]
          if (value !== undefined && value !== null) {
            if (typeof value !== 'number' || value < 0 || Number.isNaN(value)) {
              throw new ValidationError(
                `options.latency.${op} must be a non-negative number`,
                `options.latency.${op}`,
                value
              )
            }
          }
        }
      }

      // Validate maxSize if provided
      if (options.maxSize !== undefined && options.maxSize !== null) {
        if (typeof options.maxSize !== 'number' || options.maxSize < 0 || Number.isNaN(options.maxSize) || !Number.isInteger(options.maxSize)) {
          throw new ValidationError(
            'options.maxSize must be a non-negative integer',
            'options.maxSize',
            options.maxSize
          )
        }
      }
    }

    this.options = options
  }

  // ===========================================================================
  // Core StorageBackend Operations
  // ===========================================================================

  async read(path: string): Promise<Uint8Array> {
    await this.applyLatency('read')
    this.recordOperation('read', path)

    const data = this.files.get(path)
    if (!data) {
      throw new FileNotFoundError(path, 'read')
    }

    // Return a copy to prevent external mutation
    return new Uint8Array(data)
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    await this.applyLatency('write')
    this.recordOperation('write', path)

    this.checkAndUpdateSizeLimit(path, data)

    // Store a copy to prevent external mutation
    this.files.set(path, new Uint8Array(data))
    this.versions.set(path, this.generateVersion())
    this.timestamps.set(path, new Date())
  }

  async list(prefix: string): Promise<string[]> {
    await this.applyLatency('list')
    this.recordOperation('list', prefix)

    const results: string[] = []
    for (const key of this.files.keys()) {
      if (key.startsWith(prefix)) {
        results.push(key)
      }
    }
    return results
  }

  async delete(path: string): Promise<void> {
    await this.applyLatency('delete')
    this.recordOperation('delete', path)

    this.updateSizeOnDelete(path)
    this.files.delete(path)
    this.versions.delete(path)
    this.timestamps.delete(path)
  }

  async exists(path: string): Promise<boolean> {
    this.recordOperation('exists', path)
    return this.files.has(path)
  }

  async stat(path: string): Promise<FileStat | null> {
    this.recordOperation('stat', path)

    const data = this.files.get(path)
    if (!data) {
      return null
    }

    return {
      size: data.length,
      lastModified: this.timestamps.get(path) ?? new Date(),
    }
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    await this.applyLatency('read')
    this.recordOperation('readRange', path)

    const data = this.files.get(path)
    if (!data) {
      throw new FileNotFoundError(path, 'readRange')
    }

    // slice() already returns a new Uint8Array (copy of the range)
    return data.slice(start, end)
  }

  async getVersion(path: string): Promise<string | null> {
    return this.versions.get(path) ?? null
  }

  async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string> {
    // Acquire lock for this path - properly handles concurrent access
    const lock = await acquireWriteLock(this.writeLocks, path)

    try {
      this.validateVersion(path, expectedVersion)
      return this.performConditionalWrite(path, data)
    } finally {
      releaseWriteLock(this.writeLocks, path, lock)
    }
  }

  // ===========================================================================
  // Testing Utilities - Snapshot/Restore
  // ===========================================================================

  /**
   * Create a snapshot of the current storage state.
   * Useful for saving state before a test and restoring after.
   */
  snapshot(): StorageSnapshot {
    // Deep copy files to ensure snapshot isolation
    const filesCopy = new Map<string, Uint8Array>()
    for (const [key, value] of this.files) {
      filesCopy.set(key, new Uint8Array(value))
    }

    return {
      files: filesCopy,
      versions: new Map(this.versions),
    }
  }

  /**
   * Restore storage to a previously captured snapshot.
   */
  restore(snapshot: StorageSnapshot): void {
    this.files.clear()
    this.versions.clear()

    // Deep copy to maintain isolation
    for (const [key, value] of snapshot.files) {
      this.files.set(key, new Uint8Array(value))
    }
    for (const [key, value] of snapshot.versions) {
      this.versions.set(key, value)
    }

    this.recalculateSize()
  }

  /**
   * Clear all files from storage.
   */
  clear(): void {
    // Clear pending latency timers to prevent memory leaks
    for (const timerId of this.pendingTimers) {
      clearTimeout(timerId)
    }
    this.pendingTimers.clear()

    this.files.clear()
    this.versions.clear()
    this.timestamps.clear()
    this.currentSize = 0
  }

  // ===========================================================================
  // Testing Utilities - Operation Tracking
  // ===========================================================================

  /**
   * Get the history of operations performed on this storage.
   * Returns a copy to prevent external mutation.
   */
  getOperationHistory(): readonly OperationRecord[] {
    return [...this.operationHistory]
  }

  /**
   * Clear the operation history.
   */
  clearOperationHistory(): void {
    this.operationHistory.length = 0
  }

  // ===========================================================================
  // Testing Utilities - Size Management
  // ===========================================================================

  /**
   * Get the current used storage size in bytes.
   */
  getUsedSize(): number {
    return this.currentSize
  }

  /**
   * Get the maximum storage size (if configured).
   */
  getMaxSize(): number | undefined {
    return this.options.maxSize
  }

  /**
   * Get the available storage size (maxSize - usedSize).
   * Returns Infinity if no maxSize is configured.
   */
  getAvailableSize(): number {
    if (this.options.maxSize === undefined) {
      return Infinity
    }
    return this.options.maxSize - this.currentSize
  }

  /**
   * Get the total number of files stored.
   */
  getFileCount(): number {
    return this.files.size
  }

  // ===========================================================================
  // Testing Utilities - Timestamp Management
  // ===========================================================================

  /**
   * Set the timestamp for a file (testing utility).
   * Useful for testing time-based operations like vacuum retention.
   *
   * @param path - Path to the file
   * @param timestamp - The timestamp to set
   * @throws Error if the file does not exist
   */
  setFileTimestamp(path: string, timestamp: Date): void {
    if (!this.files.has(path)) {
      throw new FileNotFoundError(path, 'setFileTimestamp')
    }
    this.timestamps.set(path, timestamp)
  }

  /**
   * Get the timestamp for a file (testing utility).
   *
   * @param path - Path to the file
   * @returns The file's timestamp, or undefined if not found
   */
  getFileTimestamp(path: string): Date | undefined {
    return this.timestamps.get(path)
  }

  // ===========================================================================
  // Private Helpers - Version Generation
  // ===========================================================================

  /**
   * Generate a unique version string for a file.
   * Format: v{counter}-{timestamp}-{random}
   */
  private generateVersion(): string {
    const counter = ++this.versionCounter
    const timestamp = Date.now()
    const random = Math.random()
      .toString(RANDOM_STRING_BASE)
      .substring(RANDOM_SUBSTRING_START, RANDOM_SUBSTRING_START + VERSION_RANDOM_LENGTH)

    return `${VERSION_PREFIX}${counter}-${timestamp}-${random}`
  }

  // ===========================================================================
  // Private Helpers - Latency Simulation
  // ===========================================================================

  /**
   * Apply simulated latency if configured.
   * Tracks timers so they can be cleaned up on clear().
   */
  private async applyLatency(operation: keyof LatencyConfig): Promise<void> {
    const delay = this.options.latency?.[operation]
    if (delay && delay > 0) {
      await new Promise<void>(resolve => {
        const timerId = setTimeout(() => {
          this.pendingTimers.delete(timerId)
          resolve()
        }, delay)
        this.pendingTimers.add(timerId)
      })
    }
  }

  // ===========================================================================
  // Private Helpers - Operation Recording
  // ===========================================================================

  /**
   * Record an operation for tracking.
   */
  private recordOperation(operation: MemoryStorageOperation, path: string): void {
    this.operationHistory.push({
      operation,
      path,
      timestamp: Date.now(),
    })
  }

  // ===========================================================================
  // Private Helpers - Size Management
  // ===========================================================================

  /**
   * Check size limit and update currentSize for a write operation.
   * Throws StorageError if the size limit would be exceeded.
   */
  private checkAndUpdateSizeLimit(path: string, data: Uint8Array): void {
    if (this.options.maxSize === undefined) {
      return
    }

    const existingSize = this.files.get(path)?.length ?? 0
    const newTotalSize = this.currentSize - existingSize + data.length

    if (newTotalSize > this.options.maxSize) {
      throw new StorageError(
        `Storage size limit exceeded: ${newTotalSize} > ${this.options.maxSize}`,
        path,
        'write'
      )
    }

    this.currentSize = newTotalSize
  }

  /**
   * Update size tracking when a file is deleted.
   */
  private updateSizeOnDelete(path: string): void {
    const existingSize = this.files.get(path)?.length ?? 0
    this.currentSize -= existingSize
  }

  /**
   * Recalculate the total size from all stored files.
   */
  private recalculateSize(): void {
    this.currentSize = 0
    for (const data of this.files.values()) {
      this.currentSize += data.length
    }
  }

  // ===========================================================================
  // Private Helpers - Conditional Write
  // ===========================================================================

  /**
   * Validate that the expected version matches the current version.
   * Throws VersionMismatchError if versions don't match.
   */
  private validateVersion(path: string, expectedVersion: string | null): void {
    const currentVersion = this.versions.get(path) ?? null

    if (expectedVersion === null) {
      // Create-if-not-exists: file should not exist
      if (currentVersion !== null) {
        throw new VersionMismatchError(path, expectedVersion, currentVersion)
      }
    } else {
      // Update: version should match
      if (currentVersion !== expectedVersion) {
        throw new VersionMismatchError(path, expectedVersion, currentVersion)
      }
    }
  }

  /**
   * Perform the actual conditional write after version validation.
   * Updates size tracking and stores the data.
   */
  private performConditionalWrite(path: string, data: Uint8Array): string {
    // Check and update size limit
    this.checkAndUpdateSizeLimit(path, data)

    // Generate new version and store data
    const newVersion = this.generateVersion()
    this.files.set(path, new Uint8Array(data))
    this.versions.set(path, newVersion)

    return newVersion
  }
}
