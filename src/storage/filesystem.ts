/**
 * File System Storage Backend
 *
 * FileSystemStorage implementation for local development.
 */

import { FileNotFoundError, VersionMismatchError, ValidationError } from '../errors.js'
import { getLogger } from '../utils/index.js'
import type { StorageBackend, FileStat } from './types.js'
import { type Lock, acquireWriteLock, releaseWriteLock } from './utils.js'

// =============================================================================
// FILESYSTEM STORAGE IMPLEMENTATION
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
