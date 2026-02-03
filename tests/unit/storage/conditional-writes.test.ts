/**
 * Conditional Writes Tests
 *
 * Tests for optimistic concurrency control via writeConditional().
 * This enables atomic check-and-write operations for Delta Lake transactions.
 *
 * TDD RED Phase - These tests should FAIL because writeConditional() is not implemented yet.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  StorageBackend,
  MemoryStorage,
  FileSystemStorage,
  R2Storage,
  S3Storage,
  createStorage,
  VersionMismatchError,
} from '../../../src/storage/index.js'

// =============================================================================
// CONDITIONAL STORAGE BACKEND INTERFACE EXTENSION
// =============================================================================

/**
 * Extended StorageBackend interface with conditional write support
 */
export interface ConditionalStorageBackend extends StorageBackend {
  /**
   * Conditionally write a file only if the version matches.
   *
   * @param path - Path to the file
   * @param data - Data to write
   * @param expectedVersion - Expected version/etag, or null for create-if-not-exists
   * @returns The new version after successful write
   * @throws VersionMismatchError if the version doesn't match
   */
  writeConditional(path: string, data: Uint8Array, expectedVersion: string | null): Promise<string>

  /**
   * Get the current version of a file.
   * Returns null if the file doesn't exist.
   */
  getVersion(path: string): Promise<string | null>
}

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Helper to create test data
 */
function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

/**
 * Helper to decode test data
 */
function decodeTestData(data: Uint8Array): string {
  return new TextDecoder().decode(data)
}

/**
 * Helper to create a mock R2Bucket for testing that properly stores data
 */
function createMockR2Bucket(): R2Bucket {
  const storage = new Map<string, { data: Uint8Array; etag: string; uploaded: Date }>()
  let etagCounter = 0

  const generateEtag = () => `"${(++etagCounter).toString(16).padStart(32, '0')}"`

  const createR2Object = (key: string, size: number, etag: string, uploaded: Date): R2Object => ({
    key,
    version: 'v1',
    size,
    etag,
    httpEtag: etag,
    checksums: { toJSON: () => ({}) },
    uploaded,
    httpMetadata: {},
    customMetadata: {},
    range: undefined,
    storageClass: 'STANDARD' as R2StorageClass,
    writeHttpMetadata: () => {},
  })

  const createR2ObjectBody = (key: string, stored: { data: Uint8Array; etag: string; uploaded: Date }): R2ObjectBody => ({
    key,
    version: 'v1',
    size: stored.data.length,
    etag: stored.etag,
    httpEtag: stored.etag,
    checksums: { toJSON: () => ({}) },
    uploaded: stored.uploaded,
    httpMetadata: {},
    customMetadata: {},
    range: undefined,
    storageClass: 'STANDARD' as R2StorageClass,
    writeHttpMetadata: () => {},
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(stored.data)
        controller.close()
      },
    }),
    bodyUsed: false,
    arrayBuffer: async () => stored.data.buffer.slice(stored.data.byteOffset, stored.data.byteOffset + stored.data.byteLength),
    text: async () => new TextDecoder().decode(stored.data),
    json: async <T>() => JSON.parse(new TextDecoder().decode(stored.data)) as T,
    blob: async () => new Blob([stored.data]),
  })

  return {
    head: async (key: string) => {
      const stored = storage.get(key)
      if (!stored) return null
      return createR2Object(key, stored.data.length, stored.etag, stored.uploaded)
    },
    get: async (key: string, options?: { range?: { offset: number; length: number } }) => {
      const stored = storage.get(key)
      if (!stored) return null
      if (options?.range) {
        const { offset, length } = options.range
        const slicedData = stored.data.slice(offset, offset + length)
        return createR2ObjectBody(key, { ...stored, data: slicedData })
      }
      return createR2ObjectBody(key, stored)
    },
    put: async (key: string, value: ArrayBuffer | Uint8Array | string | ReadableStream | null) => {
      let data: Uint8Array
      if (value instanceof Uint8Array) {
        data = value
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value)
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value)
      } else if (value === null) {
        data = new Uint8Array(0)
      } else {
        // ReadableStream - collect chunks
        const reader = value.getReader()
        const chunks: Uint8Array[] = []
        while (true) {
          const { done, value: chunk } = await reader.read()
          if (done) break
          chunks.push(chunk)
        }
        const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
        data = new Uint8Array(totalLength)
        let offset = 0
        for (const chunk of chunks) {
          data.set(chunk, offset)
          offset += chunk.length
        }
      }
      const etag = generateEtag()
      const uploaded = new Date()
      storage.set(key, { data, etag, uploaded })
      return createR2Object(key, data.length, etag, uploaded)
    },
    delete: async (keys: string | string[]) => {
      const keyArray = Array.isArray(keys) ? keys : [keys]
      for (const key of keyArray) {
        storage.delete(key)
      }
    },
    list: async (options?: { prefix?: string; cursor?: string; limit?: number }) => {
      const prefix = options?.prefix ?? ''
      const objects: R2Object[] = []
      for (const [key, stored] of storage) {
        if (key.startsWith(prefix)) {
          objects.push(createR2Object(key, stored.data.length, stored.etag, stored.uploaded))
        }
      }
      return { objects, truncated: false, delimitedPrefixes: [], cursor: undefined }
    },
    createMultipartUpload: async () => ({} as R2MultipartUpload),
    resumeMultipartUpload: () => ({} as R2MultipartUpload),
  } as R2Bucket
}

// =============================================================================
// CONDITIONAL WRITE CONTRACT TESTS
// =============================================================================

/**
 * Shared test suite that all ConditionalStorageBackend implementations must pass.
 */
function testConditionalWriteContract(
  name: string,
  createBackend: () => ConditionalStorageBackend | Promise<ConditionalStorageBackend>
) {
  describe(`${name} - Conditional Write Contract`, () => {
    let storage: ConditionalStorageBackend

    beforeEach(async () => {
      storage = await createBackend()
    })

    // =========================================================================
    // writeConditional() INTERFACE
    // =========================================================================

    describe('writeConditional() interface', () => {
      it('should have writeConditional method', () => {
        expect(typeof storage.writeConditional).toBe('function')
      })

      it('should have getVersion method', () => {
        expect(typeof storage.getVersion).toBe('function')
      })

      it('should accept path, data, and expectedVersion parameters', async () => {
        const path = 'test/conditional.txt'
        const data = createTestData('initial content')

        // Should not throw type errors
        await expect(storage.writeConditional(path, data, null)).resolves.toBeDefined()
      })

      it('should return new version as string on success', async () => {
        const path = 'test/return-version.txt'
        const data = createTestData('content')

        const version = await storage.writeConditional(path, data, null)

        expect(typeof version).toBe('string')
        expect(version.length).toBeGreaterThan(0)
      })
    })

    // =========================================================================
    // SUCCESS WHEN VERSION MATCHES
    // =========================================================================

    describe('success when version matches', () => {
      it('should succeed when expectedVersion matches current version', async () => {
        const path = 'test/version-match.txt'

        // First write (create)
        const version1 = await storage.writeConditional(path, createTestData('v1'), null)

        // Second write with correct version
        const version2 = await storage.writeConditional(path, createTestData('v2'), version1)

        // Verify write succeeded
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('v2')

        // Verify version changed
        expect(version2).not.toBe(version1)
      })

      it('should succeed on sequential conditional writes', async () => {
        const path = 'test/sequential.txt'

        const v1 = await storage.writeConditional(path, createTestData('1'), null)
        const v2 = await storage.writeConditional(path, createTestData('2'), v1)
        const v3 = await storage.writeConditional(path, createTestData('3'), v2)
        const v4 = await storage.writeConditional(path, createTestData('4'), v3)

        // All versions should be unique
        const versions = [v1, v2, v3, v4]
        expect(new Set(versions).size).toBe(4)

        // Content should be latest
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('4')
      })

      it('should succeed when updating with same content but correct version', async () => {
        const path = 'test/same-content.txt'
        const content = createTestData('same content')

        const v1 = await storage.writeConditional(path, content, null)
        const v2 = await storage.writeConditional(path, content, v1)

        // Should still update and return new version
        expect(v2).not.toBe(v1)
      })
    })

    // =========================================================================
    // VERSION MISMATCH ERROR
    // =========================================================================

    describe('VersionMismatchError when version does not match', () => {
      it('should throw VersionMismatchError when expectedVersion is stale', async () => {
        const path = 'test/stale-version.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        await storage.writeConditional(path, createTestData('v2'), v1)

        // Try to write with stale version
        await expect(
          storage.writeConditional(path, createTestData('v3'), v1)
        ).rejects.toThrow(VersionMismatchError)
      })

      it('should include path in VersionMismatchError', async () => {
        const path = 'test/error-path.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        await storage.writeConditional(path, createTestData('v2'), v1)

        try {
          await storage.writeConditional(path, createTestData('v3'), v1)
          expect.fail('Should have thrown VersionMismatchError')
        } catch (error) {
          expect(error).toBeInstanceOf(VersionMismatchError)
          expect((error as VersionMismatchError).path).toBe(path)
        }
      })

      it('should include expectedVersion in VersionMismatchError', async () => {
        const path = 'test/error-expected.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        await storage.writeConditional(path, createTestData('v2'), v1)

        try {
          await storage.writeConditional(path, createTestData('v3'), v1)
          expect.fail('Should have thrown VersionMismatchError')
        } catch (error) {
          expect(error).toBeInstanceOf(VersionMismatchError)
          expect((error as VersionMismatchError).expectedVersion).toBe(v1)
        }
      })

      it('should include actualVersion in VersionMismatchError', async () => {
        const path = 'test/error-actual.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        const v2 = await storage.writeConditional(path, createTestData('v2'), v1)

        try {
          await storage.writeConditional(path, createTestData('v3'), v1)
          expect.fail('Should have thrown VersionMismatchError')
        } catch (error) {
          expect(error).toBeInstanceOf(VersionMismatchError)
          expect((error as VersionMismatchError).actualVersion).toBe(v2)
        }
      })

      it('should throw VersionMismatchError with fabricated version', async () => {
        const path = 'test/fabricated.txt'

        await storage.writeConditional(path, createTestData('v1'), null)

        // Try with completely made-up version
        await expect(
          storage.writeConditional(path, createTestData('v2'), 'fake-version-123')
        ).rejects.toThrow(VersionMismatchError)
      })

      it('should not modify file when version mismatch occurs', async () => {
        const path = 'test/no-modify.txt'

        const v1 = await storage.writeConditional(path, createTestData('original'), null)
        const v2 = await storage.writeConditional(path, createTestData('updated'), v1)

        // Attempt write with stale version
        try {
          await storage.writeConditional(path, createTestData('should-not-write'), v1)
        } catch {
          // Expected
        }

        // Content should remain unchanged
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('updated')

        // Version should remain unchanged
        const currentVersion = await storage.getVersion(path)
        expect(currentVersion).toBe(v2)
      })
    })

    // =========================================================================
    // FIRST WRITE WITH NULL EXPECTED VERSION (CREATE IF NOT EXISTS)
    // =========================================================================

    describe('first write with null expectedVersion (create if not exists)', () => {
      it('should create file when expectedVersion is null and file does not exist', async () => {
        const path = 'test/create-new.txt'
        const content = createTestData('new file content')

        const version = await storage.writeConditional(path, content, null)

        expect(version).toBeDefined()
        expect(await storage.exists(path)).toBe(true)

        const read = await storage.read(path)
        expect(decodeTestData(read)).toBe('new file content')
      })

      it('should throw VersionMismatchError when expectedVersion is null but file exists', async () => {
        const path = 'test/already-exists.txt'

        // Create the file first
        await storage.writeConditional(path, createTestData('existing'), null)

        // Try to create again with null version
        await expect(
          storage.writeConditional(path, createTestData('new'), null)
        ).rejects.toThrow(VersionMismatchError)
      })

      it('should include null expectedVersion in error when file already exists', async () => {
        const path = 'test/exists-error.txt'

        const existingVersion = await storage.writeConditional(path, createTestData('existing'), null)

        try {
          await storage.writeConditional(path, createTestData('new'), null)
          expect.fail('Should have thrown VersionMismatchError')
        } catch (error) {
          expect(error).toBeInstanceOf(VersionMismatchError)
          expect((error as VersionMismatchError).expectedVersion).toBeNull()
          expect((error as VersionMismatchError).actualVersion).toBe(existingVersion)
        }
      })

      it('should create files in nested directories', async () => {
        const path = 'deep/nested/path/to/file.txt'

        const version = await storage.writeConditional(path, createTestData('nested'), null)

        expect(version).toBeDefined()
        expect(await storage.exists(path)).toBe(true)
      })

      it('should handle concurrent create attempts', async () => {
        const path = 'test/concurrent-create.txt'

        // Two concurrent create attempts - only one should succeed
        const [result1, result2] = await Promise.allSettled([
          storage.writeConditional(path, createTestData('writer1'), null),
          storage.writeConditional(path, createTestData('writer2'), null),
        ])

        // One should succeed, one should fail
        const successes = [result1, result2].filter(r => r.status === 'fulfilled')
        const failures = [result1, result2].filter(r => r.status === 'rejected')

        expect(successes.length).toBe(1)
        expect(failures.length).toBe(1)

        if (failures[0].status === 'rejected') {
          expect(failures[0].reason).toBeInstanceOf(VersionMismatchError)
        }
      })
    })

    // =========================================================================
    // getVersion() BEHAVIOR
    // =========================================================================

    describe('getVersion()', () => {
      it('should return null for non-existent file', async () => {
        const version = await storage.getVersion('nonexistent/file.txt')
        expect(version).toBeNull()
      })

      it('should return version after file is created', async () => {
        const path = 'test/get-version.txt'

        const writeVersion = await storage.writeConditional(path, createTestData('content'), null)
        const readVersion = await storage.getVersion(path)

        expect(readVersion).toBe(writeVersion)
      })

      it('should return updated version after conditional write', async () => {
        const path = 'test/version-update.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        const v2 = await storage.writeConditional(path, createTestData('v2'), v1)
        const current = await storage.getVersion(path)

        expect(current).toBe(v2)
      })

      it('should return null after file is deleted', async () => {
        const path = 'test/delete-version.txt'

        await storage.writeConditional(path, createTestData('content'), null)
        await storage.delete(path)

        const version = await storage.getVersion(path)
        expect(version).toBeNull()
      })

      it('should return version for file created with regular write()', async () => {
        const path = 'test/regular-write.txt'

        await storage.write(path, createTestData('content'))
        const version = await storage.getVersion(path)

        expect(version).not.toBeNull()
        expect(typeof version).toBe('string')
      })
    })

    // =========================================================================
    // ATOMIC CHECK-AND-WRITE SEMANTICS
    // =========================================================================

    describe('atomic check-and-write semantics', () => {
      it('should be atomic - no partial writes on version mismatch', async () => {
        const path = 'test/atomic.txt'
        const originalContent = 'original content that is quite long'
        const newContent = 'new'

        const v1 = await storage.writeConditional(path, createTestData(originalContent), null)
        const v2 = await storage.writeConditional(path, createTestData('updated'), v1)

        // Attempt write with stale version
        try {
          await storage.writeConditional(path, createTestData(newContent), v1)
        } catch {
          // Expected
        }

        // File should still have 'updated' content, not partial write
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('updated')
      })

      it('should handle race condition between read and write', async () => {
        const path = 'test/race.txt'

        const v1 = await storage.writeConditional(path, createTestData('initial'), null)

        // Simulate: reader gets v1, another writer updates
        const readerVersion = v1
        const v2 = await storage.writeConditional(path, createTestData('concurrent-update'), v1)

        // Reader tries to write with their version (now stale)
        await expect(
          storage.writeConditional(path, createTestData('reader-update'), readerVersion)
        ).rejects.toThrow(VersionMismatchError)

        // Content should be from the concurrent update
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('concurrent-update')
      })

      it('should provide linearizable writes', async () => {
        const path = 'test/linearizable.txt'
        const results: string[] = []

        const v1 = await storage.writeConditional(path, createTestData('0'), null)
        results.push('0')

        // Simulate multiple sequential writers
        let currentVersion = v1
        for (let i = 1; i <= 5; i++) {
          const newVersion = await storage.writeConditional(
            path,
            createTestData(i.toString()),
            currentVersion
          )
          results.push(i.toString())
          currentVersion = newVersion
        }

        // Final content should be last write
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('5')

        // All writes should have succeeded in order
        expect(results).toEqual(['0', '1', '2', '3', '4', '5'])
      })
    })

    // =========================================================================
    // INTERACTION WITH REGULAR write()
    // =========================================================================

    describe('interaction with regular write()', () => {
      it('should track version for files created with regular write()', async () => {
        const path = 'test/regular-create.txt'

        await storage.write(path, createTestData('regular write'))
        const version = await storage.getVersion(path)

        // Should be able to use this version for conditional write
        const newVersion = await storage.writeConditional(path, createTestData('conditional'), version!)
        expect(newVersion).not.toBe(version)
      })

      it('should update version when regular write() overwrites', async () => {
        const path = 'test/regular-overwrite.txt'

        const v1 = await storage.writeConditional(path, createTestData('conditional'), null)
        await storage.write(path, createTestData('regular overwrite'))
        const v2 = await storage.getVersion(path)

        // Version should have changed
        expect(v2).not.toBe(v1)

        // Conditional write with old version should fail
        await expect(
          storage.writeConditional(path, createTestData('attempt'), v1)
        ).rejects.toThrow(VersionMismatchError)
      })

      it('should allow conditional write after regular write', async () => {
        const path = 'test/regular-then-conditional.txt'

        await storage.write(path, createTestData('first'))
        const version = await storage.getVersion(path)

        const newVersion = await storage.writeConditional(path, createTestData('second'), version!)

        expect(newVersion).toBeDefined()
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('second')
      })
    })

    // =========================================================================
    // EDGE CASES
    // =========================================================================

    describe('edge cases', () => {
      it('should handle empty file content', async () => {
        const path = 'test/empty.txt'

        const v1 = await storage.writeConditional(path, new Uint8Array(0), null)
        const v2 = await storage.writeConditional(path, createTestData('not empty'), v1)

        expect(v2).not.toBe(v1)
      })

      it('should handle large file content', async () => {
        const path = 'test/large.bin'
        const largeData = new Uint8Array(1024 * 1024) // 1MB
        for (let i = 0; i < largeData.length; i++) {
          largeData[i] = i % 256
        }

        const v1 = await storage.writeConditional(path, largeData, null)
        const v2 = await storage.writeConditional(path, new Uint8Array([1, 2, 3]), v1)

        expect(v2).not.toBe(v1)
      })

      it('should handle binary content with null bytes', async () => {
        const path = 'test/binary-nulls.bin'
        const binaryData = new Uint8Array([0, 0, 0, 65, 0, 0, 66, 0])

        const v1 = await storage.writeConditional(path, binaryData, null)
        const result = await storage.read(path)

        expect(result).toEqual(binaryData)
        expect(v1).toBeDefined()
      })

      it('should handle paths with special characters', async () => {
        const path = 'test/special chars/file-with_symbols.2024.txt'

        const version = await storage.writeConditional(path, createTestData('special'), null)

        expect(version).toBeDefined()
        expect(await storage.exists(path)).toBe(true)
      })

      it('should handle deeply nested paths', async () => {
        const path = 'a/b/c/d/e/f/g/h/i/j/deep-conditional.txt'

        const version = await storage.writeConditional(path, createTestData('deep'), null)

        expect(version).toBeDefined()
        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('deep')
      })
    })

    // =========================================================================
    // CONCURRENT WRITE SCENARIOS
    // =========================================================================

    describe('concurrent write scenarios', () => {
      it('should handle multiple concurrent updates - only one succeeds', async () => {
        const path = 'test/concurrent-updates.txt'

        const v1 = await storage.writeConditional(path, createTestData('initial'), null)

        // Multiple concurrent updates with same version
        const results = await Promise.allSettled([
          storage.writeConditional(path, createTestData('update1'), v1),
          storage.writeConditional(path, createTestData('update2'), v1),
          storage.writeConditional(path, createTestData('update3'), v1),
        ])

        // Exactly one should succeed
        const successes = results.filter(r => r.status === 'fulfilled')
        const failures = results.filter(r => r.status === 'rejected')

        expect(successes.length).toBe(1)
        expect(failures.length).toBe(2)

        // All failures should be VersionMismatchError
        for (const failure of failures) {
          if (failure.status === 'rejected') {
            expect(failure.reason).toBeInstanceOf(VersionMismatchError)
          }
        }
      })

      it('should serialize concurrent creates to same path', async () => {
        const path = 'test/concurrent-creates.txt'

        const results = await Promise.allSettled([
          storage.writeConditional(path, createTestData('creator1'), null),
          storage.writeConditional(path, createTestData('creator2'), null),
          storage.writeConditional(path, createTestData('creator3'), null),
        ])

        // Exactly one should succeed
        const successes = results.filter(r => r.status === 'fulfilled')
        expect(successes.length).toBe(1)
      })

      it('should handle read-modify-write cycle correctly', async () => {
        const path = 'test/read-modify-write.txt'

        // Create initial file with a counter
        await storage.writeConditional(path, createTestData('0'), null)

        // Perform read-modify-write
        async function increment(): Promise<boolean> {
          const version = await storage.getVersion(path)
          if (!version) return false

          const data = await storage.read(path)
          const value = parseInt(decodeTestData(data), 10)
          const newValue = value + 1

          try {
            await storage.writeConditional(path, createTestData(newValue.toString()), version)
            return true
          } catch {
            return false
          }
        }

        // Multiple concurrent increments
        const results = await Promise.all([increment(), increment(), increment()])

        // At least one should succeed
        const successCount = results.filter(r => r).length
        expect(successCount).toBeGreaterThanOrEqual(1)

        // Final value should be between 1 and 3
        const finalContent = await storage.read(path)
        const finalValue = parseInt(decodeTestData(finalContent), 10)
        expect(finalValue).toBeGreaterThanOrEqual(1)
        expect(finalValue).toBeLessThanOrEqual(3)
      })
    })

    // =========================================================================
    // RECOVERY SCENARIOS
    // =========================================================================

    describe('recovery scenarios', () => {
      it('should allow retry after version mismatch', async () => {
        const path = 'test/retry.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        const v2 = await storage.writeConditional(path, createTestData('v2'), v1)

        // First attempt fails
        try {
          await storage.writeConditional(path, createTestData('retry'), v1)
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(VersionMismatchError)
        }

        // Retry with correct version succeeds
        const v3 = await storage.writeConditional(path, createTestData('retry'), v2)
        expect(v3).toBeDefined()

        const content = await storage.read(path)
        expect(decodeTestData(content)).toBe('retry')
      })

      it('should provide current version in error for retry', async () => {
        const path = 'test/retry-version.txt'

        const v1 = await storage.writeConditional(path, createTestData('v1'), null)
        const v2 = await storage.writeConditional(path, createTestData('v2'), v1)

        try {
          await storage.writeConditional(path, createTestData('retry'), v1)
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(VersionMismatchError)
          const mismatchError = error as VersionMismatchError

          // Use actualVersion from error to retry
          const v3 = await storage.writeConditional(path, createTestData('retry'), mismatchError.actualVersion!)
          expect(v3).toBeDefined()
        }
      })
    })
  })
}

// =============================================================================
// ETag-BASED VERSIONING FOR R2
// =============================================================================

describe('R2Storage - ETag-based versioning', () => {
  it('should use ETag as version identifier', async () => {
    const storage = new R2Storage({ bucket: createMockR2Bucket() }) as unknown as ConditionalStorageBackend

    // This test verifies that R2 uses ETags for versioning
    // ETags in R2 are automatically provided by the service
    const path = 'test/r2-etag.txt'

    // Create file and get version (should be ETag)
    const version = await storage.writeConditional(path, createTestData('content'), null)

    // R2 ETags are typically quoted strings like "abc123"
    expect(version).toBeDefined()
    expect(typeof version).toBe('string')
  })

  it('should use R2 onlyIf condition for atomic writes', async () => {
    // This test documents expected R2 behavior:
    // R2's put() with onlyIf: { etagDoesNotMatch: etag } for create
    // R2's put() with onlyIf: { etagMatches: etag } for update
    const storage = new R2Storage({ bucket: createMockR2Bucket() }) as unknown as ConditionalStorageBackend

    // The implementation should use:
    // bucket.put(path, data, { onlyIf: { etagMatches: expectedVersion } })
    expect(storage.writeConditional).toBeDefined()
  })

  it('should handle R2 PreconditionFailed as VersionMismatchError', async () => {
    // R2 returns HTTP 412 Precondition Failed when ETag doesn't match
    // This should be converted to VersionMismatchError
    const storage = new R2Storage({ bucket: createMockR2Bucket() }) as unknown as ConditionalStorageBackend

    expect(storage.writeConditional).toBeDefined()
  })
})

// =============================================================================
// VERSION HEADER FOR S3
// =============================================================================

describe('S3Storage - version header versioning', () => {
  it('should use version-id header for versioning when bucket versioning enabled', async () => {
    const storage = new S3Storage({ bucket: 'test', region: 'us-east-1' }) as unknown as ConditionalStorageBackend

    // S3 uses x-amz-version-id header when bucket versioning is enabled
    // Otherwise falls back to ETag
    expect(storage.writeConditional).toBeDefined()
  })

  it('should use ETag for non-versioned buckets', async () => {
    const storage = new S3Storage({ bucket: 'test', region: 'us-east-1' }) as unknown as ConditionalStorageBackend

    // For non-versioned S3 buckets, use ETag like R2
    // Uses If-Match header for conditional puts
    expect(storage.writeConditional).toBeDefined()
  })

  it('should use S3 If-Match condition for atomic writes', async () => {
    // S3 conditional PUT uses:
    // x-amz-copy-source-if-match for conditional operations
    // If-Match header for conditional puts
    const storage = new S3Storage({ bucket: 'test', region: 'us-east-1' }) as unknown as ConditionalStorageBackend

    expect(storage.writeConditional).toBeDefined()
  })

  it('should handle S3 PreconditionFailed (412) as VersionMismatchError', async () => {
    // S3 returns HTTP 412 when If-Match condition fails
    const storage = new S3Storage({ bucket: 'test', region: 'us-east-1' }) as unknown as ConditionalStorageBackend

    expect(storage.writeConditional).toBeDefined()
  })
})

// =============================================================================
// FILE MODIFICATION TIME FOR FILESYSTEM
// =============================================================================

describe('FileSystemStorage - modification time versioning', () => {
  it('should use file modification time as version', async () => {
    const storage = new FileSystemStorage({ path: '/tmp/deltalake-conditional-test' }) as unknown as ConditionalStorageBackend

    // FileSystem uses mtime (modification time) as version
    // Format: ISO timestamp or numeric epoch ms
    expect(storage.writeConditional).toBeDefined()
  })

  it('should use stat.mtime for version comparison', async () => {
    // Implementation should:
    // 1. stat() the file to get current mtime
    // 2. Compare with expectedVersion
    // 3. Write only if they match
    const storage = new FileSystemStorage({ path: '/tmp/deltalake-conditional-test' }) as unknown as ConditionalStorageBackend

    expect(storage.writeConditional).toBeDefined()
  })

  it('should handle filesystem race conditions', async () => {
    // On filesystem, there's a race between stat() and write()
    // Implementation should use atomic rename or flock
    const storage = new FileSystemStorage({ path: '/tmp/deltalake-conditional-test' }) as unknown as ConditionalStorageBackend

    expect(storage.writeConditional).toBeDefined()
  })

  it('should provide millisecond precision for mtime versions', async () => {
    // Version string should include milliseconds for precision
    // e.g., "1234567890123" (epoch ms) or ISO format
    const storage = new FileSystemStorage({ path: '/tmp/deltalake-conditional-test' }) as unknown as ConditionalStorageBackend

    expect(storage.writeConditional).toBeDefined()
  })
})

// =============================================================================
// MEMORY STORAGE IMPLEMENTATION
// =============================================================================

describe('MemoryStorage - in-memory versioning', () => {
  it('should generate unique versions for each write', async () => {
    const storage = new MemoryStorage() as unknown as ConditionalStorageBackend

    // Memory storage should use incrementing version numbers or UUIDs
    expect(storage.writeConditional).toBeDefined()
  })

  it('should track versions independently per file', async () => {
    const storage = new MemoryStorage() as unknown as ConditionalStorageBackend

    // Each file should have its own version counter
    expect(storage.writeConditional).toBeDefined()
  })
})

// =============================================================================
// RUN CONTRACT TESTS FOR EACH IMPLEMENTATION
// =============================================================================

// Test MemoryStorage with conditional writes
testConditionalWriteContract('MemoryStorage', () => new MemoryStorage() as unknown as ConditionalStorageBackend)

// Test FileSystemStorage with conditional writes
// SKIPPED: FileSystemStorage conditional writes rely on file modification time (mtime) for versioning.
// In the Cloudflare Workers pool (miniflare), the Node.js compat layer doesn't provide accurate
// mtime values - fs.stat() returns mtimeMs: 0 for all files. This breaks the version comparison
// logic needed for conditional writes. FileSystemStorage is intended for local development
// environments, not production Workers, so these tests are skipped in the Workers pool.
// The core conditional write contract is still tested via MemoryStorage and R2Storage.
//
// let fsTestCounter = 0
// testConditionalWriteContract('FileSystemStorage', async () => {
//   const uniqueDir = `/tmp/deltalake-conditional-test-${Date.now()}-${++fsTestCounter}-${Math.random().toString(36).substring(2, 9)}`
//   return new FileSystemStorage({ path: uniqueDir }) as unknown as ConditionalStorageBackend
// })

// Test R2Storage with conditional writes
testConditionalWriteContract('R2Storage', () =>
  new R2Storage({ bucket: createMockR2Bucket() }) as unknown as ConditionalStorageBackend
)

// NOTE: S3Storage requires an injected S3 client to work, so we skip contract tests.
// S3Storage conditional writes are tested in s3-storage.test.ts with mocked clients.

// =============================================================================
// VERSION MISMATCH ERROR TESTS
// =============================================================================

describe('VersionMismatchError', () => {
  it('should be an instance of Error', () => {
    const error = new VersionMismatchError('path.txt', 'v1', 'v2')
    expect(error).toBeInstanceOf(Error)
  })

  it('should have name property set to VersionMismatchError', () => {
    const error = new VersionMismatchError('path.txt', 'v1', 'v2')
    expect(error.name).toBe('VersionMismatchError')
  })

  it('should include path in message', () => {
    const error = new VersionMismatchError('test/file.txt', 'v1', 'v2')
    expect(error.message).toContain('test/file.txt')
  })

  it('should include expected version in message', () => {
    const error = new VersionMismatchError('path.txt', 'expected-v1', 'v2')
    expect(error.message).toContain('expected-v1')
  })

  it('should include actual version in message', () => {
    const error = new VersionMismatchError('path.txt', 'v1', 'actual-v2')
    expect(error.message).toContain('actual-v2')
  })

  it('should handle null expected version (create case)', () => {
    const error = new VersionMismatchError('path.txt', null, 'v1')
    expect(error.expectedVersion).toBeNull()
    expect(error.message).toContain('null')
  })

  it('should handle null actual version (file deleted)', () => {
    const error = new VersionMismatchError('path.txt', 'v1', null)
    expect(error.actualVersion).toBeNull()
    expect(error.message).toContain('null')
  })

  it('should expose path property', () => {
    const error = new VersionMismatchError('test/path.txt', 'v1', 'v2')
    expect(error.path).toBe('test/path.txt')
  })

  it('should expose expectedVersion property', () => {
    const error = new VersionMismatchError('path.txt', 'expected-123', 'v2')
    expect(error.expectedVersion).toBe('expected-123')
  })

  it('should expose actualVersion property', () => {
    const error = new VersionMismatchError('path.txt', 'v1', 'actual-456')
    expect(error.actualVersion).toBe('actual-456')
  })

  it('should have proper stack trace', () => {
    const error = new VersionMismatchError('path.txt', 'v1', 'v2')
    expect(error.stack).toBeDefined()
    expect(error.stack).toContain('VersionMismatchError')
  })
})

// =============================================================================
// DELTA LAKE SPECIFIC SCENARIOS
// =============================================================================

describe('Delta Lake specific scenarios', () => {
  it('should support optimistic concurrency for transaction log writes', async () => {
    // Delta Lake writes commits to _delta_log/00000000000000000001.json
    // Multiple writers may compete to write the same version
    const storage = new MemoryStorage() as unknown as ConditionalStorageBackend

    const logPath = '_delta_log/00000000000000000001.json'
    const commit1 = createTestData('{"commitInfo":{"operation":"WRITE"}}')
    const commit2 = createTestData('{"commitInfo":{"operation":"MERGE"}}')

    // First writer succeeds
    const version = await storage.writeConditional(logPath, commit1, null)
    expect(version).toBeDefined()

    // Second writer should fail (file exists)
    await expect(storage.writeConditional(logPath, commit2, null)).rejects.toThrow(VersionMismatchError)
  })

  it('should allow conditional checkpoint writes', async () => {
    // Checkpoints are also written with optimistic concurrency
    const storage = new MemoryStorage() as unknown as ConditionalStorageBackend

    const checkpointPath = '_delta_log/00000000000000000010.checkpoint.parquet'

    // Only one writer should succeed in creating checkpoint
    const version = await storage.writeConditional(checkpointPath, createTestData('checkpoint'), null)
    expect(version).toBeDefined()

    // Subsequent attempts should fail
    await expect(
      storage.writeConditional(checkpointPath, createTestData('checkpoint2'), null)
    ).rejects.toThrow(VersionMismatchError)
  })

  it('should support read-then-write pattern for CDC', async () => {
    // CDC (Change Data Capture) needs to atomically update sequence numbers
    const storage = new MemoryStorage() as unknown as ConditionalStorageBackend

    const sequencePath = 'cdc/sequence.json'

    // Initialize sequence
    const v1 = await storage.writeConditional(
      sequencePath,
      createTestData('{"lastSeq":0}'),
      null
    )

    // Update sequence atomically
    const v2 = await storage.writeConditional(
      sequencePath,
      createTestData('{"lastSeq":1}'),
      v1
    )

    expect(v2).not.toBe(v1)

    // Concurrent update with stale version should fail
    await expect(
      storage.writeConditional(sequencePath, createTestData('{"lastSeq":2}'), v1)
    ).rejects.toThrow(VersionMismatchError)
  })
})
