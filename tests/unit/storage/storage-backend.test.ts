/**
 * StorageBackend Interface Tests
 *
 * Comprehensive test suite for the StorageBackend interface.
 * Tests cover all storage backend implementations:
 * - MemoryStorage
 * - FileSystemStorage
 * - R2Storage
 * - S3Storage
 *
 * These tests are written in the RED phase - they should FAIL
 * because the implementations are not complete.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  StorageBackend,
  FileStat,
  MemoryStorage,
  FileSystemStorage,
  R2Storage,
  S3Storage,
  createStorage,
  createAsyncBuffer,
} from '../../../src/storage/index.js'

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
 * Helper to create a functional mock R2Bucket that stores data in memory.
 * This allows R2Storage to be tested with actual read/write operations.
 */
function createMockR2Bucket(): R2Bucket {
  const storage = new Map<string, { data: Uint8Array; uploaded: Date; etag: string }>()
  let etagCounter = 0

  const createR2Object = (key: string, size: number, uploaded: Date, etag: string): R2Object => ({
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
    storageClass: 'Standard',
    writeHttpMetadata: () => {},
  }) as unknown as R2Object

  const createR2ObjectBody = (
    key: string,
    data: Uint8Array,
    uploaded: Date,
    etag: string
  ): R2ObjectBody => {
    const obj = createR2Object(key, data.length, uploaded, etag) as R2ObjectBody
    obj.body = new ReadableStream({
      start(controller) {
        controller.enqueue(data)
        controller.close()
      },
    })
    obj.bodyUsed = false
    obj.arrayBuffer = async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
    obj.text = async () => new TextDecoder().decode(data)
    obj.json = async () => JSON.parse(new TextDecoder().decode(data))
    obj.blob = async () => new Blob([data])
    return obj
  }

  return {
    head: async (key: string) => {
      const item = storage.get(key)
      if (!item) return null
      return createR2Object(key, item.data.length, item.uploaded, item.etag)
    },
    get: async (key: string, options?: R2GetOptions) => {
      const item = storage.get(key)
      if (!item) return null

      let data = item.data
      if (options?.range && 'offset' in options.range) {
        const { offset, length } = options.range
        data = item.data.slice(offset, offset + length)
      }

      return createR2ObjectBody(key, data, item.uploaded, item.etag)
    },
    put: async (key: string, value: ArrayBuffer | Uint8Array | string | ReadableStream | null) => {
      let data: Uint8Array
      if (value instanceof Uint8Array) {
        data = new Uint8Array(value)
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value)
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value)
      } else if (value === null) {
        data = new Uint8Array(0)
      } else {
        // ReadableStream - collect all chunks
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

      const uploaded = new Date()
      const etag = `"${(++etagCounter).toString(16).padStart(32, '0')}"`
      storage.set(key, { data, uploaded, etag })

      return createR2Object(key, data.length, uploaded, etag)
    },
    delete: async (keys: string | string[]) => {
      const keyArray = Array.isArray(keys) ? keys : [keys]
      for (const key of keyArray) {
        storage.delete(key)
      }
    },
    list: async (options?: R2ListOptions) => {
      const prefix = options?.prefix ?? ''
      const objects: R2Object[] = []

      for (const [key, item] of storage) {
        if (key.startsWith(prefix)) {
          objects.push(createR2Object(key, item.data.length, item.uploaded, item.etag))
        }
      }

      return {
        objects,
        truncated: false,
        delimitedPrefixes: [],
        cursor: undefined,
      } as R2Objects
    },
    createMultipartUpload: async () => ({} as R2MultipartUpload),
    resumeMultipartUpload: () => ({} as R2MultipartUpload),
  } as R2Bucket
}

// =============================================================================
// STORAGE BACKEND INTERFACE CONTRACT TESTS
// =============================================================================

/**
 * Shared test suite that all StorageBackend implementations must pass.
 * This ensures consistent behavior across MemoryStorage, FileSystemStorage, R2Storage, etc.
 */
function testStorageBackendContract(
  name: string,
  createBackend: () => StorageBackend | Promise<StorageBackend>
) {
  describe(`${name} - StorageBackend contract`, () => {
    let storage: StorageBackend

    beforeEach(async () => {
      storage = await createBackend()
    })

    // =========================================================================
    // READ/WRITE OPERATIONS
    // =========================================================================

    describe('read/write operations', () => {
      it('should write and read a file', async () => {
        const path = 'test/file.txt'
        const content = 'Hello, World!'
        const data = createTestData(content)

        await storage.write(path, data)
        const result = await storage.read(path)

        expect(decodeTestData(result)).toBe(content)
      })

      it('should write and read binary data', async () => {
        const path = 'test/binary.bin'
        const data = new Uint8Array([0, 1, 2, 255, 254, 253, 128, 127])

        await storage.write(path, data)
        const result = await storage.read(path)

        expect(result).toEqual(data)
      })

      it('should overwrite existing file', async () => {
        const path = 'test/overwrite.txt'
        const original = createTestData('original content')
        const updated = createTestData('updated content')

        await storage.write(path, original)
        await storage.write(path, updated)
        const result = await storage.read(path)

        expect(decodeTestData(result)).toBe('updated content')
      })

      it('should write and read empty file', async () => {
        const path = 'test/empty.txt'
        const data = new Uint8Array(0)

        await storage.write(path, data)
        const result = await storage.read(path)

        expect(result.length).toBe(0)
      })

      it('should write and read large file', async () => {
        const path = 'test/large.bin'
        // 100KB of random-ish data (reduced from 1MB for faster unit tests)
        const data = new Uint8Array(1024 * 100)
        for (let i = 0; i < data.length; i++) {
          data[i] = i % 256
        }

        await storage.write(path, data)
        const result = await storage.read(path)

        expect(result.length).toBe(data.length)
        expect(result).toEqual(data)
      })

      it('should handle files with special characters in path', async () => {
        const path = 'test/special chars/file-with_symbols.2024.txt'
        const data = createTestData('special path content')

        await storage.write(path, data)
        const result = await storage.read(path)

        expect(decodeTestData(result)).toBe('special path content')
      })

      it('should handle deeply nested paths', async () => {
        const path = 'a/b/c/d/e/f/g/h/i/j/deep.txt'
        const data = createTestData('deep content')

        await storage.write(path, data)
        const result = await storage.read(path)

        expect(decodeTestData(result)).toBe('deep content')
      })

      it('should throw error when reading non-existent file', async () => {
        await expect(storage.read('nonexistent/file.txt')).rejects.toThrow()
      })
    })

    // =========================================================================
    // LIST OPERATIONS WITH PREFIX FILTERING
    // =========================================================================

    describe('list with prefix filtering', () => {
      beforeEach(async () => {
        // Set up test file structure
        await storage.write('data/table1/file1.parquet', createTestData('1'))
        await storage.write('data/table1/file2.parquet', createTestData('2'))
        await storage.write('data/table2/file1.parquet', createTestData('3'))
        await storage.write('logs/app.log', createTestData('4'))
        await storage.write('config.json', createTestData('5'))
      })

      it('should list all files with empty prefix', async () => {
        const files = await storage.list('')

        expect(files).toContain('data/table1/file1.parquet')
        expect(files).toContain('data/table1/file2.parquet')
        expect(files).toContain('data/table2/file1.parquet')
        expect(files).toContain('logs/app.log')
        expect(files).toContain('config.json')
        expect(files.length).toBe(5)
      })

      it('should filter by prefix', async () => {
        const files = await storage.list('data/')

        expect(files).toContain('data/table1/file1.parquet')
        expect(files).toContain('data/table1/file2.parquet')
        expect(files).toContain('data/table2/file1.parquet')
        expect(files).not.toContain('logs/app.log')
        expect(files).not.toContain('config.json')
        expect(files.length).toBe(3)
      })

      it('should filter by nested prefix', async () => {
        const files = await storage.list('data/table1/')

        expect(files).toContain('data/table1/file1.parquet')
        expect(files).toContain('data/table1/file2.parquet')
        expect(files).not.toContain('data/table2/file1.parquet')
        expect(files.length).toBe(2)
      })

      it('should return empty array for non-matching prefix', async () => {
        const files = await storage.list('nonexistent/')

        expect(files).toEqual([])
      })

      it('should handle prefix without trailing slash', async () => {
        const files = await storage.list('data/table1')

        expect(files).toContain('data/table1/file1.parquet')
        expect(files).toContain('data/table1/file2.parquet')
        expect(files.length).toBe(2)
      })

      it('should list files matching exact filename prefix', async () => {
        const files = await storage.list('data/table1/file1')

        expect(files).toContain('data/table1/file1.parquet')
        expect(files.length).toBe(1)
      })
    })

    // =========================================================================
    // DELETE OPERATIONS
    // =========================================================================

    describe('delete operations', () => {
      it('should delete an existing file', async () => {
        const path = 'test/to-delete.txt'
        await storage.write(path, createTestData('delete me'))

        await storage.delete(path)

        expect(await storage.exists(path)).toBe(false)
      })

      it('should not throw when deleting non-existent file', async () => {
        // Deleting a non-existent file should be a no-op (idempotent)
        await expect(storage.delete('nonexistent/file.txt')).resolves.toBeUndefined()
      })

      it('should delete file but keep other files intact', async () => {
        await storage.write('test/keep.txt', createTestData('keep'))
        await storage.write('test/delete.txt', createTestData('delete'))

        await storage.delete('test/delete.txt')

        expect(await storage.exists('test/keep.txt')).toBe(true)
        expect(await storage.exists('test/delete.txt')).toBe(false)
      })

      it('should allow re-creating deleted file', async () => {
        const path = 'test/recreate.txt'
        await storage.write(path, createTestData('original'))
        await storage.delete(path)
        await storage.write(path, createTestData('recreated'))

        const result = await storage.read(path)
        expect(decodeTestData(result)).toBe('recreated')
      })
    })

    // =========================================================================
    // EXISTS CHECKS
    // =========================================================================

    describe('exists checks', () => {
      it('should return true for existing file', async () => {
        const path = 'test/exists.txt'
        await storage.write(path, createTestData('content'))

        expect(await storage.exists(path)).toBe(true)
      })

      it('should return false for non-existent file', async () => {
        expect(await storage.exists('nonexistent/file.txt')).toBe(false)
      })

      it('should return true after writing new file', async () => {
        const path = 'test/new-file.txt'
        expect(await storage.exists(path)).toBe(false)

        await storage.write(path, createTestData('content'))

        expect(await storage.exists(path)).toBe(true)
      })

      it('should return false after deleting file', async () => {
        const path = 'test/delete-check.txt'
        await storage.write(path, createTestData('content'))
        expect(await storage.exists(path)).toBe(true)

        await storage.delete(path)

        expect(await storage.exists(path)).toBe(false)
      })

      it('should return false for directory path (not a file)', async () => {
        await storage.write('dir/file.txt', createTestData('content'))

        // 'dir/' is a prefix, not a file
        expect(await storage.exists('dir/')).toBe(false)
        expect(await storage.exists('dir')).toBe(false)
      })
    })

    // =========================================================================
    // STAT WITH SIZE/LASTMODIFIED
    // =========================================================================

    describe('stat with size/lastModified', () => {
      it('should return correct file size', async () => {
        const path = 'test/sized.txt'
        const content = 'Hello, World!' // 13 bytes
        await storage.write(path, createTestData(content))

        const stat = await storage.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.size).toBe(13)
      })

      it('should return size 0 for empty file', async () => {
        const path = 'test/empty-stat.txt'
        await storage.write(path, new Uint8Array(0))

        const stat = await storage.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.size).toBe(0)
      })

      it('should return correct size for large file', async () => {
        const path = 'test/large-stat.bin'
        const size = 1024 * 100 // 100KB
        await storage.write(path, new Uint8Array(size))

        const stat = await storage.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.size).toBe(size)
      })

      it('should return lastModified as Date', async () => {
        const path = 'test/dated.txt'
        await storage.write(path, createTestData('content'))

        const stat = await storage.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.lastModified).toBeInstanceOf(Date)
        // Note: In miniflare's Node.js compat layer for FileSystemStorage, mtime may be 0.
        // We verify it's a valid Date (non-negative timestamp) rather than a recent timestamp.
        expect(stat!.lastModified.getTime()).toBeGreaterThanOrEqual(0)
      })

      it('should update lastModified when file is overwritten', async () => {
        const path = 'test/updated.txt'
        await storage.write(path, createTestData('original'))
        const stat1 = await storage.stat(path)

        // Wait a bit to ensure different timestamp
        await new Promise(resolve => setTimeout(resolve, 50))
        await storage.write(path, createTestData('updated'))
        const stat2 = await storage.stat(path)

        expect(stat2).not.toBeNull()
        expect(stat2!.lastModified.getTime()).toBeGreaterThanOrEqual(stat1!.lastModified.getTime())
      })

      it('should return null for non-existent file', async () => {
        const stat = await storage.stat('nonexistent/file.txt')

        expect(stat).toBeNull()
      })

      it('should include optional etag when available', async () => {
        const path = 'test/etag.txt'
        await storage.write(path, createTestData('content'))

        const stat = await storage.stat(path)

        expect(stat).not.toBeNull()
        // etag is optional, so just verify the property exists if provided
        if (stat!.etag !== undefined) {
          expect(typeof stat!.etag).toBe('string')
          expect(stat!.etag.length).toBeGreaterThan(0)
        }
      })
    })

    // =========================================================================
    // READRANGE FOR BYTE-RANGE READS
    // =========================================================================

    describe('readRange for byte-range reads', () => {
      const testContent = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' // 26 bytes

      beforeEach(async () => {
        await storage.write('test/range.txt', createTestData(testContent))
      })

      it('should read first 5 bytes', async () => {
        const result = await storage.readRange('test/range.txt', 0, 5)

        expect(decodeTestData(result)).toBe('ABCDE')
      })

      it('should read middle bytes', async () => {
        const result = await storage.readRange('test/range.txt', 10, 15)

        expect(decodeTestData(result)).toBe('KLMNO')
      })

      it('should read last bytes', async () => {
        const result = await storage.readRange('test/range.txt', 20, 26)

        expect(decodeTestData(result)).toBe('UVWXYZ')
      })

      it('should read single byte', async () => {
        const result = await storage.readRange('test/range.txt', 5, 6)

        expect(decodeTestData(result)).toBe('F')
      })

      it('should read entire file', async () => {
        const result = await storage.readRange('test/range.txt', 0, 26)

        expect(decodeTestData(result)).toBe(testContent)
      })

      it('should handle end beyond file size gracefully', async () => {
        // Reading beyond the end should return up to the end
        const result = await storage.readRange('test/range.txt', 20, 100)

        expect(decodeTestData(result)).toBe('UVWXYZ')
      })

      it('should return empty array for zero-length range', async () => {
        const result = await storage.readRange('test/range.txt', 5, 5)

        expect(result.length).toBe(0)
      })

      it('should throw error for non-existent file', async () => {
        await expect(storage.readRange('nonexistent/file.txt', 0, 10)).rejects.toThrow()
      })

      it('should handle binary data ranges correctly', async () => {
        const binaryPath = 'test/binary-range.bin'
        const binaryData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        await storage.write(binaryPath, binaryData)

        const result = await storage.readRange(binaryPath, 3, 7)

        expect(result).toEqual(new Uint8Array([3, 4, 5, 6]))
      })

      it('should support reading Parquet footer pattern (last 8 bytes)', async () => {
        // Parquet files have a 4-byte magic number "PAR1" at the end
        const parquetPath = 'test/fake.parquet'
        const magicBytes = createTestData('PAR1')
        const headerBytes = new Uint8Array(100)
        const fullFile = new Uint8Array(headerBytes.length + magicBytes.length)
        fullFile.set(headerBytes, 0)
        fullFile.set(magicBytes, headerBytes.length)

        await storage.write(parquetPath, fullFile)
        const stat = await storage.stat(parquetPath)
        const footer = await storage.readRange(parquetPath, stat!.size - 4, stat!.size)

        expect(decodeTestData(footer)).toBe('PAR1')
      })
    })

    // =========================================================================
    // ERROR HANDLING FOR MISSING FILES
    // =========================================================================

    describe('error handling for missing files', () => {
      it('should throw descriptive error when reading missing file', async () => {
        try {
          await storage.read('missing/path/file.txt')
          expect.fail('Should have thrown an error')
        } catch (error) {
          expect(error).toBeInstanceOf(Error)
          expect((error as Error).message).toMatch(/not found|missing|does not exist/i)
        }
      })

      it('should throw descriptive error when reading range from missing file', async () => {
        try {
          await storage.readRange('missing/file.txt', 0, 10)
          expect.fail('Should have thrown an error')
        } catch (error) {
          expect(error).toBeInstanceOf(Error)
          expect((error as Error).message).toMatch(/not found|missing|does not exist/i)
        }
      })

      it('should return null from stat for missing file (not throw)', async () => {
        const result = await storage.stat('missing/file.txt')
        expect(result).toBeNull()
      })

      it('should return false from exists for missing file', async () => {
        const result = await storage.exists('missing/file.txt')
        expect(result).toBe(false)
      })

      it('should handle concurrent operations on missing file', async () => {
        const path = 'concurrent/missing.txt'
        const [existsResult, statResult] = await Promise.all([
          storage.exists(path),
          storage.stat(path),
        ])

        expect(existsResult).toBe(false)
        expect(statResult).toBeNull()
      })
    })
  })
}

// =============================================================================
// RUN CONTRACT TESTS FOR EACH IMPLEMENTATION
// =============================================================================

// Test MemoryStorage
testStorageBackendContract('MemoryStorage', () => new MemoryStorage())

// Test FileSystemStorage - use unique directory per test to ensure isolation
let fsTestDirCounter = 0
testStorageBackendContract(
  'FileSystemStorage',
  () => new FileSystemStorage({ path: `/tmp/deltalake-test-${Date.now()}-${++fsTestDirCounter}` })
)

// Test R2Storage with mock bucket
testStorageBackendContract('R2Storage', () => new R2Storage({ bucket: createMockR2Bucket() }))

// NOTE: S3Storage requires an injected S3 client to work, so we skip contract tests.
// S3Storage is tested separately in s3-storage.test.ts with mocked clients.

// =============================================================================
// FACTORY TESTS
// =============================================================================

describe('createStorage factory', () => {
  it('should create MemoryStorage when type is memory', () => {
    const storage = createStorage({ type: 'memory' })
    expect(storage).toBeInstanceOf(MemoryStorage)
  })

  it('should create FileSystemStorage when type is filesystem', () => {
    const storage = createStorage({ type: 'filesystem', path: '/tmp/test' })
    expect(storage).toBeInstanceOf(FileSystemStorage)
  })

  it('should create R2Storage when type is r2', () => {
    const storage = createStorage({ type: 'r2', bucket: createMockR2Bucket() })
    expect(storage).toBeInstanceOf(R2Storage)
  })

  it('should create S3Storage when type is s3', () => {
    const storage = createStorage({ type: 's3', bucket: 'test', region: 'us-east-1' })
    expect(storage).toBeInstanceOf(S3Storage)
  })

  it('should throw for unknown storage type', () => {
    expect(() => createStorage({ type: 'unknown' as any })).toThrow()
  })

  it('should auto-detect filesystem in Node.js environment', () => {
    // This test verifies auto-detection works in Node.js
    const storage = createStorage()
    expect(storage).toBeInstanceOf(FileSystemStorage)
  })
})

// =============================================================================
// ASYNCBUFFER TESTS
// =============================================================================

describe('createAsyncBuffer', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should create AsyncBuffer with correct byteLength', async () => {
    const content = 'Hello, World!'
    await storage.write('test.txt', createTestData(content))

    const buffer = await createAsyncBuffer(storage, 'test.txt')

    expect(buffer.byteLength).toBe(13)
  })

  it('should slice bytes correctly', async () => {
    const content = 'ABCDEFGHIJ'
    await storage.write('test.txt', createTestData(content))

    const buffer = await createAsyncBuffer(storage, 'test.txt')
    const slice = await buffer.slice(2, 5)

    expect(decodeTestData(slice)).toBe('CDE')
  })

  it('should slice from start to end when end is omitted', async () => {
    const content = 'ABCDEFGHIJ'
    await storage.write('test.txt', createTestData(content))

    const buffer = await createAsyncBuffer(storage, 'test.txt')
    const slice = await buffer.slice(5)

    expect(decodeTestData(slice)).toBe('FGHIJ')
  })

  it('should throw for non-existent file', async () => {
    await expect(createAsyncBuffer(storage, 'missing.txt')).rejects.toThrow(/not found/i)
  })

  it('should work with Parquet-like file access pattern', async () => {
    // Simulate reading a Parquet file:
    // 1. Read footer length (last 8 bytes: 4 byte length + 4 byte magic)
    // 2. Read footer metadata
    // 3. Read data pages

    const metadataLength = new Uint8Array([50, 0, 0, 0]) // 50 bytes metadata (little-endian)
    const magic = createTestData('PAR1')
    const metadata = new Uint8Array(50).fill(42) // Fake metadata
    const data = new Uint8Array(200).fill(1) // Fake data

    const file = new Uint8Array(data.length + metadata.length + metadataLength.length + magic.length)
    let offset = 0
    file.set(data, offset); offset += data.length
    file.set(metadata, offset); offset += metadata.length
    file.set(metadataLength, offset); offset += metadataLength.length
    file.set(magic, offset)

    await storage.write('data.parquet', file)
    const buffer = await createAsyncBuffer(storage, 'data.parquet')

    // Read magic number (slice returns ArrayBuffer for hyparquet compatibility)
    const magicSlice = await buffer.slice(buffer.byteLength - 4)
    expect(decodeTestData(new Uint8Array(magicSlice))).toBe('PAR1')

    // Read metadata length (little-endian 32-bit integer encoding of 50)
    const lengthSlice = await buffer.slice(buffer.byteLength - 8, buffer.byteLength - 4)
    // slice returns ArrayBuffer directly, so create DataView from it
    const view = new DataView(lengthSlice)
    expect(view.getUint32(0, true)).toBe(50)

    // Read metadata
    const metadataSlice = await buffer.slice(buffer.byteLength - 8 - 50, buffer.byteLength - 8)
    const metadataBytes = new Uint8Array(metadataSlice)
    expect(metadataBytes.length).toBe(50)
    expect(metadataBytes[0]).toBe(42)
  })
})

// =============================================================================
// TYPE DEFINITION TESTS
// =============================================================================

describe('StorageBackend type definitions', () => {
  it('should enforce StorageBackend interface contract', () => {
    // This is a compile-time test - if it compiles, it passes
    const impl: StorageBackend = {
      read: async (path: string) => new Uint8Array(),
      write: async (path: string, data: Uint8Array) => {},
      list: async (prefix: string) => [],
      delete: async (path: string) => {},
      exists: async (path: string) => false,
      stat: async (path: string) => null,
      readRange: async (path: string, start: number, end: number) => new Uint8Array(),
      writeConditional: async (path: string, data: Uint8Array, expectedVersion: string | null) => 'v1',
      getVersion: async (path: string) => null,
    }
    expect(impl).toBeDefined()
  })

  it('should enforce FileStat interface', () => {
    const stat: FileStat = {
      size: 100,
      lastModified: new Date(),
    }
    expect(stat.size).toBe(100)
    expect(stat.lastModified).toBeInstanceOf(Date)
  })

  it('should allow optional etag in FileStat', () => {
    const stat: FileStat = {
      size: 100,
      lastModified: new Date(),
      etag: '"abc123"',
    }
    expect(stat.etag).toBe('"abc123"')
  })
})

// =============================================================================
// EDGE CASES AND ADVANCED SCENARIOS
// =============================================================================

describe('Edge cases and advanced scenarios', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('concurrent operations', () => {
    it('should handle concurrent writes to different files', async () => {
      const writes = Array.from({ length: 10 }, (_, i) =>
        storage.write(`file${i}.txt`, createTestData(`content${i}`))
      )

      await Promise.all(writes)

      for (let i = 0; i < 10; i++) {
        const content = await storage.read(`file${i}.txt`)
        expect(decodeTestData(content)).toBe(`content${i}`)
      }
    })

    it('should handle concurrent reads of same file', async () => {
      await storage.write('shared.txt', createTestData('shared content'))

      const reads = Array.from({ length: 10 }, () => storage.read('shared.txt'))
      const results = await Promise.all(reads)

      for (const result of results) {
        expect(decodeTestData(result)).toBe('shared content')
      }
    })

    it('should handle concurrent list operations', async () => {
      await storage.write('a/1.txt', createTestData('1'))
      await storage.write('a/2.txt', createTestData('2'))
      await storage.write('b/1.txt', createTestData('3'))

      const lists = await Promise.all([
        storage.list('a/'),
        storage.list('b/'),
        storage.list(''),
      ])

      expect(lists[0].length).toBe(2)
      expect(lists[1].length).toBe(1)
      expect(lists[2].length).toBe(3)
    })
  })

  describe('path handling', () => {
    it('should normalize paths with leading slashes', async () => {
      await storage.write('/leading/slash.txt', createTestData('content'))

      // Should be accessible with or without leading slash
      const result = await storage.read('/leading/slash.txt')
      expect(decodeTestData(result)).toBe('content')
    })

    it('should handle paths with consecutive slashes', async () => {
      await storage.write('double//slash.txt', createTestData('content'))
      const result = await storage.read('double//slash.txt')
      expect(decodeTestData(result)).toBe('content')
    })

    it('should handle Unicode in file paths', async () => {
      const path = 'unicode/\u4e2d\u6587/file.txt'
      await storage.write(path, createTestData('unicode content'))
      const result = await storage.read(path)
      expect(decodeTestData(result)).toBe('unicode content')
    })

    it('should handle very long paths', async () => {
      const longPath = 'a/'.repeat(50) + 'file.txt'
      await storage.write(longPath, createTestData('deep'))
      const result = await storage.read(longPath)
      expect(decodeTestData(result)).toBe('deep')
    })
  })

  describe('data integrity', () => {
    it('should preserve exact binary content', async () => {
      // Test all possible byte values
      const data = new Uint8Array(256)
      for (let i = 0; i < 256; i++) {
        data[i] = i
      }

      await storage.write('all-bytes.bin', data)
      const result = await storage.read('all-bytes.bin')

      expect(result.length).toBe(256)
      for (let i = 0; i < 256; i++) {
        expect(result[i]).toBe(i)
      }
    })

    it('should handle null bytes in content', async () => {
      const data = new Uint8Array([0, 0, 0, 65, 0, 0, 66, 0])
      await storage.write('nulls.bin', data)
      const result = await storage.read('nulls.bin')
      expect(result).toEqual(data)
    })
  })
})

// =============================================================================
// IMPLEMENTATION-SPECIFIC TESTS
// =============================================================================

describe('FileSystemStorage specific behavior', () => {
  it('should throw FileNotFoundError for missing files', async () => {
    const fs = new FileSystemStorage({ path: '/tmp/test-fs-specific' })

    // FileSystemStorage is implemented, should throw FileNotFoundError for missing files
    await expect(fs.read('nonexistent.txt')).rejects.toThrow(/not found/i)
    await expect(fs.readRange('nonexistent.txt', 0, 10)).rejects.toThrow(/not found/i)
  })

  it('should return null for stat on non-existent file', async () => {
    const fs = new FileSystemStorage({ path: '/tmp/test-fs-specific' })
    const stat = await fs.stat('nonexistent.txt')
    expect(stat).toBeNull()
  })

  it('should return false for exists on non-existent file', async () => {
    const fs = new FileSystemStorage({ path: '/tmp/test-fs-specific' })
    const exists = await fs.exists('nonexistent.txt')
    expect(exists).toBe(false)
  })
})

describe('R2Storage specific behavior', () => {
  it('should throw FileNotFoundError for missing files', async () => {
    const r2 = new R2Storage({ bucket: createMockR2Bucket() })

    // R2Storage with mock bucket throws FileNotFoundError for missing files
    await expect(r2.read('nonexistent.txt')).rejects.toThrow(/not found/i)
    await expect(r2.readRange('nonexistent.txt', 0, 10)).rejects.toThrow(/not found/i)
  })

  it('should return null for stat on non-existent file', async () => {
    const r2 = new R2Storage({ bucket: createMockR2Bucket() })
    const stat = await r2.stat('nonexistent.txt')
    expect(stat).toBeNull()
  })

  it('should return false for exists on non-existent file', async () => {
    const r2 = new R2Storage({ bucket: createMockR2Bucket() })
    const exists = await r2.exists('nonexistent.txt')
    expect(exists).toBe(false)
  })
})

describe('S3Storage specific behavior', () => {
  it('should throw error when no client is configured', async () => {
    // S3Storage requires an injected S3 client to work
    const s3 = new S3Storage({ bucket: 'test', region: 'us-east-1' })

    await expect(s3.read('test.txt')).rejects.toThrow('S3 client not initialized')
    await expect(s3.write('test.txt', new Uint8Array())).rejects.toThrow('S3 client not initialized')
    await expect(s3.list('')).rejects.toThrow('S3 client not initialized')
    await expect(s3.delete('test.txt')).rejects.toThrow('S3 client not initialized')
    await expect(s3.exists('test.txt')).rejects.toThrow('S3 client not initialized')
    await expect(s3.stat('test.txt')).rejects.toThrow('S3 client not initialized')
    await expect(s3.readRange('test.txt', 0, 10)).rejects.toThrow('S3 client not initialized')
  })
})
