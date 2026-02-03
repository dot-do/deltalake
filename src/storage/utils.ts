/**
 * Storage Utilities
 *
 * Write lock utilities for concurrency control and AsyncBuffer creation for Parquet integration.
 */

import { FileNotFoundError } from '../errors.js'
import type { AsyncBuffer, StorageBackend } from './types.js'

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
export interface Lock {
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
export function createLock(): Lock {
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
export async function acquireWriteLock(locks: Map<string, Lock>, path: string): Promise<Lock> {
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
export function releaseWriteLock(locks: Map<string, Lock>, path: string, lock: Lock): void {
  // Only delete if the lock in the map is the same one we're releasing
  // This prevents a race where another operation might have already set a new lock
  if (locks.get(path) === lock) {
    locks.delete(path)
  }
  lock.release()
}

// =============================================================================
// ASYNC BUFFER (for Parquet integration)
// =============================================================================

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
