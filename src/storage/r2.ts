/**
 * Cloudflare R2 Storage Backend
 *
 * R2Storage implementation for Cloudflare Workers.
 */

import { FileNotFoundError, VersionMismatchError, ValidationError } from '../errors.js'
import type { StorageBackend, FileStat } from './types.js'
import { type Lock, acquireWriteLock, releaseWriteLock } from './utils.js'

// =============================================================================
// R2 STORAGE IMPLEMENTATION
// =============================================================================

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
  private prefix: string

  constructor(private options: { bucket: R2Bucket; prefix?: string }) {
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

    // Normalize prefix (ensure it ends with / if provided)
    this.prefix = options.prefix ? (options.prefix.endsWith('/') ? options.prefix : options.prefix + '/') : ''
  }

  private prefixPath(path: string): string {
    return this.prefix + path
  }

  async read(path: string): Promise<Uint8Array> {
    const fullPath = this.prefixPath(path)
    const object = await this.options.bucket.get(fullPath)
    if (!object) {
      throw new FileNotFoundError(path, 'read')
    }
    const arrayBuffer = await object.arrayBuffer()
    return new Uint8Array(arrayBuffer)
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    await this.options.bucket.put(this.prefixPath(path), data)
  }

  async list(prefix: string): Promise<string[]> {
    const results: string[] = []
    let cursor: string | undefined = undefined
    let truncated = true
    const fullPrefix = this.prefixPath(prefix)

    while (truncated) {
      const response = await this.options.bucket.list({
        prefix: fullPrefix,
        ...(cursor ? { cursor } : {}),
      })

      // Strip the storage prefix from returned keys
      results.push(...response.objects.map(obj =>
        this.prefix ? obj.key.slice(this.prefix.length) : obj.key
      ))
      truncated = response.truncated
      if (response.truncated) {
        cursor = response.cursor
      }
    }

    return results
  }

  async delete(path: string): Promise<void> {
    await this.options.bucket.delete(this.prefixPath(path))
  }

  async exists(path: string): Promise<boolean> {
    const object = await this.options.bucket.head(this.prefixPath(path))
    return object !== null
  }

  async stat(path: string): Promise<FileStat | null> {
    const object = await this.options.bucket.head(this.prefixPath(path))
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
    const fullPath = this.prefixPath(path)
    const length = end - start
    const object = await this.options.bucket.get(fullPath, {
      range: { offset: start, length },
    })
    if (!object) {
      throw new FileNotFoundError(path, 'readRange')
    }
    const arrayBuffer = await object.arrayBuffer()
    return new Uint8Array(arrayBuffer)
  }

  async getVersion(path: string): Promise<string | null> {
    const object = await this.options.bucket.head(this.prefixPath(path))
    if (!object) {
      return null
    }
    return object.etag
  }

  async writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string> {
    const fullPath = this.prefixPath(path)
    // Acquire lock for this path - properly handles concurrent access
    const lock = await acquireWriteLock(this.writeLocks, fullPath)

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
      await this.options.bucket.put(fullPath, data)

      // Get the new version (ETag) after write
      const newVersion = await this.getVersion(path)
      return newVersion ?? ''
    } finally {
      releaseWriteLock(this.writeLocks, fullPath, lock)
    }
  }
}
