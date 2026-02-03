/**
 * Storage Types and Interfaces
 *
 * Core type definitions for the storage backend abstraction.
 */

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

// =============================================================================
// FILE METADATA
// =============================================================================

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

// =============================================================================
// STORAGE OPTIONS
// =============================================================================

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

// =============================================================================
// S3 COMMAND AND RESPONSE TYPES
// =============================================================================

/**
 * S3 streaming body interface - matches AWS SDK v3 streaming body.
 *
 * @internal
 */
export interface S3StreamingBody {
  transformToByteArray(): Promise<Uint8Array>
}

/**
 * GetObject command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
export interface S3GetObjectCommand {
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
export interface S3GetObjectOutput {
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
export interface S3PutObjectCommand {
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
export interface S3HeadObjectCommand {
  readonly _type: 'HeadObject'
  Bucket: string
  Key: string
}

/**
 * HeadObject command output.
 *
 * @internal
 */
export interface S3HeadObjectOutput {
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
export interface S3DeleteObjectCommand {
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
export interface S3ListObjectsV2Command {
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
export interface S3ListObjectsV2Output {
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
export interface S3CreateMultipartUploadCommand {
  readonly _type: 'CreateMultipartUpload'
  Bucket: string
  Key: string
}

/**
 * CreateMultipartUpload command output.
 *
 * @internal
 */
export interface S3CreateMultipartUploadOutput {
  UploadId?: string
}

/**
 * UploadPart command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
export interface S3UploadPartCommand {
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
export interface S3UploadPartOutput {
  ETag?: string
}

/**
 * CompleteMultipartUpload command input.
 * Discriminated by _type field for type-safe send() dispatch.
 *
 * @internal
 */
export interface S3CompleteMultipartUploadCommand {
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
export interface S3AbortMultipartUploadCommand {
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
export type S3Command =
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
export type S3ResponseMap = {
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
