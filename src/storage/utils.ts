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
 * ## Purpose
 *
 * These locks serialize concurrent `writeConditional()` calls to the same path
 * within a single process. This prevents multiple in-flight writes from racing
 * on the version check, which could cause spurious `VersionMismatchError`s.
 *
 * ## Scope and Limitations
 *
 * These locks are **process-local only**. They do NOT provide coordination across:
 * - Multiple Node.js processes on the same machine
 * - Multiple Cloudflare Workers instances
 * - Multiple servers/containers in a distributed deployment
 *
 * ## Why This Is Safe for Delta Lake
 *
 * Delta Lake's transaction protocol uses version-numbered commit files
 * (e.g., `00000000000000000005.json`) with create-if-not-exists semantics.
 * Even without distributed locks, the protocol is correct:
 *
 * 1. Process A reads table at version 4, prepares commit for version 5
 * 2. Process B reads table at version 4, prepares commit for version 5
 * 3. Process A writes `00000000000000000005.json` - succeeds
 * 4. Process B tries to write `00000000000000000005.json` - fails (file exists)
 * 5. Process B catches `VersionMismatchError`, refreshes, retries with version 6
 *
 * The process-local locks are an **optimization**, not a correctness requirement.
 * They reduce the overhead of version mismatch errors within a single process.
 *
 * ## When You Might Need External Distributed Locking
 *
 * External distributed locking (Redis, DynamoDB, etc.) is rarely needed but may
 * be useful for:
 * - Preventing duplicate work when retry overhead is expensive
 * - Implementing leader election for single-writer patterns
 * - Coordinating complex multi-table transactions
 *
 * For most Delta Lake write workloads, optimistic concurrency with retry is
 * sufficient and simpler.
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
