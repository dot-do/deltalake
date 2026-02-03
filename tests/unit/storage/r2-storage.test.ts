import { describe, it, expect, vi, beforeEach } from 'vitest'
import { R2Storage, FileStat } from '../../../src/storage/index'

/**
 * Mock R2 types that mirror Cloudflare's R2 API
 */
interface MockR2Object {
  key: string
  version: string
  size: number
  etag: string
  httpEtag: string
  uploaded: Date
  httpMetadata?: Record<string, string>
  customMetadata?: Record<string, string>
  range?: { offset: number; length: number }
  storageClass: string
}

interface MockR2ObjectBody extends MockR2Object {
  body: ReadableStream<Uint8Array>
  bodyUsed: boolean
  arrayBuffer(): Promise<ArrayBuffer>
  bytes(): Promise<Uint8Array>
  text(): Promise<string>
  json<T>(): Promise<T>
  blob(): Promise<Blob>
}

interface MockR2Objects {
  objects: MockR2Object[]
  delimitedPrefixes: string[]
  truncated: boolean
  cursor?: string
}

interface MockR2Bucket {
  head(key: string): Promise<MockR2Object | null>
  get(key: string, options?: { range?: { offset: number; length: number } | { suffix: number } }): Promise<MockR2ObjectBody | null>
  put(key: string, value: ArrayBuffer | Uint8Array | string | ReadableStream | null): Promise<MockR2Object>
  delete(keys: string | string[]): Promise<void>
  list(options?: { prefix?: string; cursor?: string; limit?: number; delimiter?: string }): Promise<MockR2Objects>
}

/**
 * Helper to create a mock R2ObjectBody with data
 */
function createMockR2ObjectBody(
  key: string,
  data: Uint8Array,
  metadata?: Partial<MockR2Object>
): MockR2ObjectBody {
  const uploaded = metadata?.uploaded ?? new Date()
  return {
    key,
    version: metadata?.version ?? 'v1',
    size: data.length,
    etag: metadata?.etag ?? `"${key}-etag"`,
    httpEtag: metadata?.httpEtag ?? `"${key}-etag"`,
    uploaded,
    storageClass: metadata?.storageClass ?? 'STANDARD',
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(data)
        controller.close()
      },
    }),
    bodyUsed: false,
    async arrayBuffer() {
      return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
    },
    async bytes() {
      return data
    },
    async text() {
      return new TextDecoder().decode(data)
    },
    async json<T>() {
      return JSON.parse(new TextDecoder().decode(data)) as T
    },
    async blob() {
      return new Blob([data])
    },
  }
}

/**
 * Helper to create a mock R2Object (metadata only, no body)
 */
function createMockR2Object(
  key: string,
  size: number,
  metadata?: Partial<MockR2Object>
): MockR2Object {
  return {
    key,
    version: metadata?.version ?? 'v1',
    size,
    etag: metadata?.etag ?? `"${key}-etag"`,
    httpEtag: metadata?.httpEtag ?? `"${key}-etag"`,
    uploaded: metadata?.uploaded ?? new Date(),
    storageClass: metadata?.storageClass ?? 'STANDARD',
  }
}

/**
 * Create a mock R2Bucket for testing
 */
function createMockBucket(): MockR2Bucket {
  return {
    head: vi.fn(),
    get: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
  }
}

describe('R2Storage', () => {
  let mockBucket: MockR2Bucket
  let storage: R2Storage

  beforeEach(() => {
    mockBucket = createMockBucket()
    storage = new R2Storage({ bucket: mockBucket as unknown as R2Bucket })
  })

  // ===========================================================================
  // read() tests - using bucket.get()
  // ===========================================================================
  describe('read()', () => {
    it('should read a file and return Uint8Array', async () => {
      const testData = new TextEncoder().encode('Hello, R2!')
      const mockBody = createMockR2ObjectBody('test/file.txt', testData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.read('test/file.txt')

      expect(mockBucket.get).toHaveBeenCalledWith('test/file.txt')
      expect(result).toBeInstanceOf(Uint8Array)
      expect(result).toEqual(testData)
    })

    it('should read binary data correctly', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      const mockBody = createMockR2ObjectBody('binary/data.bin', binaryData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.read('binary/data.bin')

      expect(result).toEqual(binaryData)
    })

    it('should read large files', async () => {
      const largeData = new Uint8Array(1024 * 1024) // 1MB
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }
      const mockBody = createMockR2ObjectBody('large/file.bin', largeData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.read('large/file.bin')

      expect(result.length).toBe(1024 * 1024)
      expect(result[0]).toBe(0)
      expect(result[255]).toBe(255)
    })

    it('should throw error when file does not exist', async () => {
      vi.mocked(mockBucket.get).mockResolvedValue(null)

      await expect(storage.read('nonexistent/file.txt')).rejects.toThrow()
    })

    it('should handle paths with special characters', async () => {
      const testData = new TextEncoder().encode('special')
      const mockBody = createMockR2ObjectBody('path/with spaces/and-dashes/file.txt', testData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.read('path/with spaces/and-dashes/file.txt')

      expect(mockBucket.get).toHaveBeenCalledWith('path/with spaces/and-dashes/file.txt')
      expect(result).toEqual(testData)
    })

    it('should handle deeply nested paths', async () => {
      const testData = new TextEncoder().encode('nested')
      const deepPath = 'a/b/c/d/e/f/g/file.txt'
      const mockBody = createMockR2ObjectBody(deepPath, testData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.read(deepPath)

      expect(mockBucket.get).toHaveBeenCalledWith(deepPath)
      expect(result).toEqual(testData)
    })
  })

  // ===========================================================================
  // write() tests - using bucket.put()
  // ===========================================================================
  describe('write()', () => {
    it('should write data to a file', async () => {
      const testData = new TextEncoder().encode('Hello, R2!')
      const mockObject = createMockR2Object('test/file.txt', testData.length)
      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

      await storage.write('test/file.txt', testData)

      expect(mockBucket.put).toHaveBeenCalledWith('test/file.txt', testData)
    })

    it('should write binary data', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      const mockObject = createMockR2Object('binary/data.bin', binaryData.length)
      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

      await storage.write('binary/data.bin', binaryData)

      expect(mockBucket.put).toHaveBeenCalledWith('binary/data.bin', binaryData)
    })

    it('should write empty file', async () => {
      const emptyData = new Uint8Array(0)
      const mockObject = createMockR2Object('empty/file.txt', 0)
      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

      await storage.write('empty/file.txt', emptyData)

      expect(mockBucket.put).toHaveBeenCalledWith('empty/file.txt', emptyData)
    })

    it('should write large files', async () => {
      const largeData = new Uint8Array(5 * 1024 * 1024) // 5MB
      const mockObject = createMockR2Object('large/file.bin', largeData.length)
      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

      await storage.write('large/file.bin', largeData)

      // Verify put was called with correct path and data size
      // (avoid deep equality check on 5MB buffer which is very slow)
      expect(mockBucket.put).toHaveBeenCalledTimes(1)
      const [path, data] = vi.mocked(mockBucket.put).mock.calls[0]
      expect(path).toBe('large/file.bin')
      expect(data).toBeInstanceOf(Uint8Array)
      expect((data as Uint8Array).length).toBe(5 * 1024 * 1024)
    })

    it('should overwrite existing files', async () => {
      const newData = new TextEncoder().encode('New content')
      const mockObject = createMockR2Object('existing/file.txt', newData.length)
      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

      await storage.write('existing/file.txt', newData)

      expect(mockBucket.put).toHaveBeenCalledWith('existing/file.txt', newData)
    })

    it('should create intermediate paths automatically', async () => {
      const testData = new TextEncoder().encode('data')
      const mockObject = createMockR2Object('new/nested/path/file.txt', testData.length)
      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

      await storage.write('new/nested/path/file.txt', testData)

      expect(mockBucket.put).toHaveBeenCalledWith('new/nested/path/file.txt', testData)
    })
  })

  // ===========================================================================
  // list() tests - using bucket.list() with prefix
  // ===========================================================================
  describe('list()', () => {
    it('should list files with a prefix', async () => {
      const mockObjects: MockR2Objects = {
        objects: [
          createMockR2Object('data/file1.txt', 100),
          createMockR2Object('data/file2.txt', 200),
          createMockR2Object('data/file3.txt', 300),
        ],
        delimitedPrefixes: [],
        truncated: false,
      }
      vi.mocked(mockBucket.list).mockResolvedValue(mockObjects)

      const result = await storage.list('data/')

      expect(mockBucket.list).toHaveBeenCalledWith({ prefix: 'data/' })
      expect(result).toEqual(['data/file1.txt', 'data/file2.txt', 'data/file3.txt'])
    })

    it('should return empty array when no files match prefix', async () => {
      const mockObjects: MockR2Objects = {
        objects: [],
        delimitedPrefixes: [],
        truncated: false,
      }
      vi.mocked(mockBucket.list).mockResolvedValue(mockObjects)

      const result = await storage.list('nonexistent/')

      expect(mockBucket.list).toHaveBeenCalledWith({ prefix: 'nonexistent/' })
      expect(result).toEqual([])
    })

    it('should list all files with empty prefix', async () => {
      const mockObjects: MockR2Objects = {
        objects: [
          createMockR2Object('file1.txt', 100),
          createMockR2Object('dir/file2.txt', 200),
        ],
        delimitedPrefixes: [],
        truncated: false,
      }
      vi.mocked(mockBucket.list).mockResolvedValue(mockObjects)

      const result = await storage.list('')

      expect(mockBucket.list).toHaveBeenCalledWith({ prefix: '' })
      expect(result).toEqual(['file1.txt', 'dir/file2.txt'])
    })

    it('should handle pagination with continuation tokens', async () => {
      const firstPage: MockR2Objects = {
        objects: [createMockR2Object('data/file1.txt', 100)],
        delimitedPrefixes: [],
        truncated: true,
        cursor: 'cursor123',
      }
      const secondPage: MockR2Objects = {
        objects: [createMockR2Object('data/file2.txt', 200)],
        delimitedPrefixes: [],
        truncated: false,
      }
      vi.mocked(mockBucket.list)
        .mockResolvedValueOnce(firstPage)
        .mockResolvedValueOnce(secondPage)

      const result = await storage.list('data/')

      expect(mockBucket.list).toHaveBeenCalledTimes(2)
      expect(mockBucket.list).toHaveBeenNthCalledWith(1, { prefix: 'data/' })
      expect(mockBucket.list).toHaveBeenNthCalledWith(2, { prefix: 'data/', cursor: 'cursor123' })
      expect(result).toEqual(['data/file1.txt', 'data/file2.txt'])
    })

    it('should handle many pages of results', async () => {
      const pages: MockR2Objects[] = []
      for (let i = 0; i < 5; i++) {
        pages.push({
          objects: [createMockR2Object(`data/file${i}.txt`, 100)],
          delimitedPrefixes: [],
          truncated: i < 4,
          cursor: i < 4 ? `cursor${i}` : undefined,
        })
      }
      for (const page of pages) {
        vi.mocked(mockBucket.list).mockResolvedValueOnce(page)
      }

      const result = await storage.list('data/')

      expect(mockBucket.list).toHaveBeenCalledTimes(5)
      expect(result).toHaveLength(5)
    })

    it('should handle prefix with no trailing slash', async () => {
      const mockObjects: MockR2Objects = {
        objects: [createMockR2Object('datafile.txt', 100)],
        delimitedPrefixes: [],
        truncated: false,
      }
      vi.mocked(mockBucket.list).mockResolvedValue(mockObjects)

      const result = await storage.list('data')

      expect(mockBucket.list).toHaveBeenCalledWith({ prefix: 'data' })
      expect(result).toEqual(['datafile.txt'])
    })
  })

  // ===========================================================================
  // delete() tests - using bucket.delete()
  // ===========================================================================
  describe('delete()', () => {
    it('should delete a file', async () => {
      vi.mocked(mockBucket.delete).mockResolvedValue(undefined)

      await storage.delete('test/file.txt')

      expect(mockBucket.delete).toHaveBeenCalledWith('test/file.txt')
    })

    it('should not throw when deleting non-existent file', async () => {
      vi.mocked(mockBucket.delete).mockResolvedValue(undefined)

      await expect(storage.delete('nonexistent/file.txt')).resolves.toBeUndefined()
      expect(mockBucket.delete).toHaveBeenCalledWith('nonexistent/file.txt')
    })

    it('should delete files with special characters in path', async () => {
      vi.mocked(mockBucket.delete).mockResolvedValue(undefined)

      await storage.delete('path/with spaces/file.txt')

      expect(mockBucket.delete).toHaveBeenCalledWith('path/with spaces/file.txt')
    })

    it('should delete deeply nested files', async () => {
      vi.mocked(mockBucket.delete).mockResolvedValue(undefined)
      const deepPath = 'a/b/c/d/e/f/g/file.txt'

      await storage.delete(deepPath)

      expect(mockBucket.delete).toHaveBeenCalledWith(deepPath)
    })
  })

  // ===========================================================================
  // exists() tests - checking object presence
  // ===========================================================================
  describe('exists()', () => {
    it('should return true when file exists', async () => {
      const mockObject = createMockR2Object('test/file.txt', 100)
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.exists('test/file.txt')

      expect(mockBucket.head).toHaveBeenCalledWith('test/file.txt')
      expect(result).toBe(true)
    })

    it('should return false when file does not exist', async () => {
      vi.mocked(mockBucket.head).mockResolvedValue(null)

      const result = await storage.exists('nonexistent/file.txt')

      expect(mockBucket.head).toHaveBeenCalledWith('nonexistent/file.txt')
      expect(result).toBe(false)
    })

    it('should return true for empty files', async () => {
      const mockObject = createMockR2Object('empty/file.txt', 0)
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.exists('empty/file.txt')

      expect(result).toBe(true)
    })

    it('should check existence efficiently without downloading content', async () => {
      const mockObject = createMockR2Object('large/file.bin', 1024 * 1024 * 100)
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      await storage.exists('large/file.bin')

      // Should use head() not get()
      expect(mockBucket.head).toHaveBeenCalledWith('large/file.bin')
      expect(mockBucket.get).not.toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // stat() tests - extracting size, lastModified, etag from R2Object
  // ===========================================================================
  describe('stat()', () => {
    it('should return file metadata', async () => {
      const uploadDate = new Date('2025-01-15T10:30:00Z')
      const mockObject = createMockR2Object('test/file.txt', 12345, {
        etag: '"abc123"',
        uploaded: uploadDate,
      })
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.stat('test/file.txt')

      expect(mockBucket.head).toHaveBeenCalledWith('test/file.txt')
      expect(result).not.toBeNull()
      expect(result?.size).toBe(12345)
      expect(result?.lastModified).toEqual(uploadDate)
      expect(result?.etag).toBe('"abc123"')
    })

    it('should return null for non-existent file', async () => {
      vi.mocked(mockBucket.head).mockResolvedValue(null)

      const result = await storage.stat('nonexistent/file.txt')

      expect(mockBucket.head).toHaveBeenCalledWith('nonexistent/file.txt')
      expect(result).toBeNull()
    })

    it('should return correct size for empty file', async () => {
      const mockObject = createMockR2Object('empty/file.txt', 0)
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.stat('empty/file.txt')

      expect(result?.size).toBe(0)
    })

    it('should return correct size for large files', async () => {
      const largeSize = 5 * 1024 * 1024 * 1024 // 5GB
      const mockObject = createMockR2Object('large/file.bin', largeSize)
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.stat('large/file.bin')

      expect(result?.size).toBe(largeSize)
    })

    it('should handle files without etag', async () => {
      const mockObject: MockR2Object = {
        key: 'test/file.txt',
        version: 'v1',
        size: 100,
        etag: '',
        httpEtag: '',
        uploaded: new Date(),
        storageClass: 'STANDARD',
      }
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.stat('test/file.txt')

      expect(result).not.toBeNull()
      expect(result?.size).toBe(100)
    })

    it('should return lastModified from uploaded field', async () => {
      const uploadDate = new Date('2024-06-15T14:30:00Z')
      const mockObject = createMockR2Object('test/file.txt', 100, {
        uploaded: uploadDate,
      })
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)

      const result = await storage.stat('test/file.txt')

      expect(result?.lastModified).toEqual(uploadDate)
    })
  })

  // ===========================================================================
  // readRange() tests - using Range header for byte ranges
  // ===========================================================================
  describe('readRange()', () => {
    it('should read a byte range from a file', async () => {
      const fullData = new TextEncoder().encode('Hello, World!')
      const rangeData = fullData.slice(0, 5) // "Hello"
      const mockBody = createMockR2ObjectBody('test/file.txt', rangeData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.readRange('test/file.txt', 0, 5)

      expect(mockBucket.get).toHaveBeenCalledWith('test/file.txt', {
        range: { offset: 0, length: 5 },
      })
      expect(result).toEqual(rangeData)
    })

    it('should read range from middle of file', async () => {
      const fullData = new TextEncoder().encode('Hello, World!')
      const rangeData = fullData.slice(7, 12) // "World"
      const mockBody = createMockR2ObjectBody('test/file.txt', rangeData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.readRange('test/file.txt', 7, 12)

      expect(mockBucket.get).toHaveBeenCalledWith('test/file.txt', {
        range: { offset: 7, length: 5 },
      })
      expect(result).toEqual(rangeData)
    })

    it('should read range at end of file (Parquet footer)', async () => {
      // Parquet files store metadata at the end
      const fileSize = 1024
      const footerSize = 100
      const footerData = new Uint8Array(footerSize)
      const mockBody = createMockR2ObjectBody('data/table.parquet', footerData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.readRange('data/table.parquet', fileSize - footerSize, fileSize)

      expect(mockBucket.get).toHaveBeenCalledWith('data/table.parquet', {
        range: { offset: fileSize - footerSize, length: footerSize },
      })
      expect(result.length).toBe(footerSize)
    })

    it('should handle single byte range', async () => {
      const singleByte = new Uint8Array([0x42])
      const mockBody = createMockR2ObjectBody('test/file.txt', singleByte)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.readRange('test/file.txt', 10, 11)

      expect(mockBucket.get).toHaveBeenCalledWith('test/file.txt', {
        range: { offset: 10, length: 1 },
      })
      expect(result).toEqual(singleByte)
    })

    it('should throw error when file does not exist', async () => {
      vi.mocked(mockBucket.get).mockResolvedValue(null)

      await expect(storage.readRange('nonexistent/file.txt', 0, 10)).rejects.toThrow()
    })

    it('should handle zero-length range', async () => {
      const emptyData = new Uint8Array(0)
      const mockBody = createMockR2ObjectBody('test/file.txt', emptyData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.readRange('test/file.txt', 0, 0)

      expect(result.length).toBe(0)
    })

    it('should read binary data correctly in range', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0x04])
      const mockBody = createMockR2ObjectBody('binary/data.bin', binaryData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      const result = await storage.readRange('binary/data.bin', 0, 5)

      expect(result).toEqual(binaryData)
    })

    it('should use efficient range request without downloading full file', async () => {
      const rangeData = new Uint8Array(100)
      const mockBody = createMockR2ObjectBody('large/file.bin', rangeData)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      await storage.readRange('large/file.bin', 1000000, 1000100)

      // Should pass range option, not download full file
      expect(mockBucket.get).toHaveBeenCalledWith('large/file.bin', {
        range: { offset: 1000000, length: 100 },
      })
    })
  })

  // ===========================================================================
  // Error handling tests
  // ===========================================================================
  describe('error handling', () => {
    describe('missing objects', () => {
      it('should throw descriptive error when reading non-existent file', async () => {
        vi.mocked(mockBucket.get).mockResolvedValue(null)

        await expect(storage.read('missing/file.txt')).rejects.toThrow(/not found|does not exist/i)
      })

      it('should throw descriptive error when reading range from non-existent file', async () => {
        vi.mocked(mockBucket.get).mockResolvedValue(null)

        await expect(storage.readRange('missing/file.txt', 0, 10)).rejects.toThrow(/not found|does not exist/i)
      })

      it('should include file path in error message', async () => {
        vi.mocked(mockBucket.get).mockResolvedValue(null)

        await expect(storage.read('specific/path/to/file.txt')).rejects.toThrow(/specific\/path\/to\/file\.txt/)
      })
    })

    describe('R2 API errors', () => {
      it('should propagate R2 bucket errors on read', async () => {
        const r2Error = new Error('R2 internal error')
        vi.mocked(mockBucket.get).mockRejectedValue(r2Error)

        await expect(storage.read('test/file.txt')).rejects.toThrow('R2 internal error')
      })

      it('should propagate R2 bucket errors on write', async () => {
        const r2Error = new Error('Bucket quota exceeded')
        vi.mocked(mockBucket.put).mockRejectedValue(r2Error)

        await expect(storage.write('test/file.txt', new Uint8Array([1, 2, 3]))).rejects.toThrow('Bucket quota exceeded')
      })

      it('should propagate R2 bucket errors on list', async () => {
        const r2Error = new Error('Access denied')
        vi.mocked(mockBucket.list).mockRejectedValue(r2Error)

        await expect(storage.list('prefix/')).rejects.toThrow('Access denied')
      })

      it('should propagate R2 bucket errors on delete', async () => {
        const r2Error = new Error('Delete not permitted')
        vi.mocked(mockBucket.delete).mockRejectedValue(r2Error)

        await expect(storage.delete('test/file.txt')).rejects.toThrow('Delete not permitted')
      })

      it('should propagate R2 bucket errors on exists check', async () => {
        const r2Error = new Error('Network timeout')
        vi.mocked(mockBucket.head).mockRejectedValue(r2Error)

        await expect(storage.exists('test/file.txt')).rejects.toThrow('Network timeout')
      })

      it('should propagate R2 bucket errors on stat', async () => {
        const r2Error = new Error('Service unavailable')
        vi.mocked(mockBucket.head).mockRejectedValue(r2Error)

        await expect(storage.stat('test/file.txt')).rejects.toThrow('Service unavailable')
      })

      it('should propagate R2 bucket errors on readRange', async () => {
        const r2Error = new Error('Invalid range')
        vi.mocked(mockBucket.get).mockRejectedValue(r2Error)

        await expect(storage.readRange('test/file.txt', 0, 10)).rejects.toThrow('Invalid range')
      })
    })

    describe('invalid input handling', () => {
      it('should handle empty path for read', async () => {
        vi.mocked(mockBucket.get).mockResolvedValue(null)

        // Either throw error or return not found
        await expect(storage.read('')).rejects.toThrow()
      })

      it('should handle empty path for write', async () => {
        const testData = new Uint8Array([1, 2, 3])
        // R2 may accept or reject empty keys - test the behavior
        const mockObject = createMockR2Object('', testData.length)
        vi.mocked(mockBucket.put).mockResolvedValue(mockObject)

        // Implementation should either accept it or throw a validation error
        await storage.write('', testData)
        expect(mockBucket.put).toHaveBeenCalled()
      })
    })
  })

  // ===========================================================================
  // Integration scenarios
  // ===========================================================================
  describe('integration scenarios', () => {
    it('should support write then read workflow', async () => {
      const testData = new TextEncoder().encode('Test content')
      const mockObject = createMockR2Object('test/file.txt', testData.length)
      const mockBody = createMockR2ObjectBody('test/file.txt', testData)

      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      // Write
      await storage.write('test/file.txt', testData)
      expect(mockBucket.put).toHaveBeenCalledWith('test/file.txt', testData)

      // Read
      const result = await storage.read('test/file.txt')
      expect(result).toEqual(testData)
    })

    it('should support write, check exists, then delete workflow', async () => {
      const testData = new TextEncoder().encode('To be deleted')
      const mockObject = createMockR2Object('temp/file.txt', testData.length)

      vi.mocked(mockBucket.put).mockResolvedValue(mockObject)
      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)
      vi.mocked(mockBucket.delete).mockResolvedValue(undefined)

      // Write
      await storage.write('temp/file.txt', testData)

      // Check exists
      const exists = await storage.exists('temp/file.txt')
      expect(exists).toBe(true)

      // Delete
      await storage.delete('temp/file.txt')
      expect(mockBucket.delete).toHaveBeenCalledWith('temp/file.txt')
    })

    it('should support stat before readRange for Parquet files', async () => {
      // This is the typical Parquet reading pattern
      const fileSize = 1024 * 1024 // 1MB file
      const footerSize = 8 // Parquet footer magic size
      const footer = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // "PAR1"

      const mockObject = createMockR2Object('data/table.parquet', fileSize)
      const mockBody = createMockR2ObjectBody('data/table.parquet', footer)

      vi.mocked(mockBucket.head).mockResolvedValue(mockObject)
      vi.mocked(mockBucket.get).mockResolvedValue(mockBody)

      // Get file size first
      const stat = await storage.stat('data/table.parquet')
      expect(stat?.size).toBe(fileSize)

      // Read footer using size
      const result = await storage.readRange('data/table.parquet', fileSize - footerSize, fileSize)
      expect(mockBucket.get).toHaveBeenCalledWith('data/table.parquet', {
        range: { offset: fileSize - footerSize, length: footerSize },
      })
    })

    it('should support listing and batch operations', async () => {
      const mockObjects: MockR2Objects = {
        objects: [
          createMockR2Object('batch/file1.txt', 100),
          createMockR2Object('batch/file2.txt', 200),
          createMockR2Object('batch/file3.txt', 300),
        ],
        delimitedPrefixes: [],
        truncated: false,
      }
      vi.mocked(mockBucket.list).mockResolvedValue(mockObjects)
      vi.mocked(mockBucket.delete).mockResolvedValue(undefined)

      // List files
      const files = await storage.list('batch/')
      expect(files).toHaveLength(3)

      // Delete each (in real code, might batch delete)
      for (const file of files) {
        await storage.delete(file)
      }

      expect(mockBucket.delete).toHaveBeenCalledTimes(3)
    })
  })
})
