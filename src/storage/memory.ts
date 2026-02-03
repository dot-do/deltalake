/**
 * In-Memory Storage Backend
 *
 * MemoryStorage implementation for testing and development.
 */

import { StorageError, FileNotFoundError, VersionMismatchError, ValidationError } from '../errors.js'
import type {
  StorageBackend,
  FileStat,
  MemoryStorageOptions,
  MemoryStorageOperation,
  LatencyConfig,
  OperationRecord,
  StorageSnapshot,
} from './types.js'
import { type Lock, acquireWriteLock, releaseWriteLock } from './utils.js'

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
