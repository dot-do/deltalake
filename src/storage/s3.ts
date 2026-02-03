/**
 * AWS S3 Storage Backend
 *
 * S3Storage implementation for AWS S3.
 */

import { StorageError, S3Error, VersionMismatchError, ValidationError } from '../errors.js'
import { getLogger } from '../utils/index.js'
import type {
  StorageBackend,
  FileStat,
  S3ClientLike,
  S3StorageOptions,
  S3GetObjectCommand,
  S3PutObjectCommand,
  S3HeadObjectCommand,
  S3DeleteObjectCommand,
  S3ListObjectsV2Command,
  S3CreateMultipartUploadCommand,
  S3UploadPartCommand,
  S3CompleteMultipartUploadCommand,
  S3AbortMultipartUploadCommand,
} from './types.js'
import { type Lock, acquireWriteLock, releaseWriteLock } from './utils.js'

// =============================================================================
// S3 STORAGE IMPLEMENTATION
// =============================================================================

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
