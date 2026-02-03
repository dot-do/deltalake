/**
 * S3Storage Tests (RED Phase - TDD)
 *
 * Comprehensive tests for the S3Storage backend implementation.
 * These tests verify AWS S3 operations using mocks since we can't hit real S3 in unit tests.
 *
 * Tests cover:
 * - Basic CRUD operations (read/write/delete/exists/stat)
 * - readRange for byte-range reads (critical for Parquet)
 * - Multipart uploads for large files
 * - Pagination for list with many objects
 * - Credentials configuration (access key, secret, region)
 * - Error handling (not found, access denied, etc.)
 * - Path handling and prefix operations
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { S3Storage, type FileStat, type S3Credentials, type S3StorageOptions } from '../../../src/storage/index'

// =============================================================================
// AWS SDK MOCK TYPES
// =============================================================================

/**
 * Mock S3 GetObjectOutput
 */
interface MockS3GetObjectOutput {
  Body?: {
    transformToByteArray(): Promise<Uint8Array>
  }
  ContentLength?: number
  LastModified?: Date
  ETag?: string
  ContentRange?: string
}

/**
 * Mock S3 HeadObjectOutput
 */
interface MockS3HeadObjectOutput {
  ContentLength?: number
  LastModified?: Date
  ETag?: string
}

/**
 * Mock S3 PutObjectOutput
 */
interface MockS3PutObjectOutput {
  ETag?: string
  VersionId?: string
}

/**
 * Mock S3 ListObjectsV2Output
 */
interface MockS3ListObjectsV2Output {
  Contents?: Array<{
    Key?: string
    Size?: number
    LastModified?: Date
    ETag?: string
  }>
  IsTruncated?: boolean
  NextContinuationToken?: string
  KeyCount?: number
}

/**
 * Mock S3 DeleteObjectOutput
 */
interface MockS3DeleteObjectOutput {
  DeleteMarker?: boolean
  VersionId?: string
}

/**
 * Mock S3 CreateMultipartUploadOutput
 */
interface MockS3CreateMultipartUploadOutput {
  UploadId?: string
  Bucket?: string
  Key?: string
}

/**
 * Mock S3 UploadPartOutput
 */
interface MockS3UploadPartOutput {
  ETag?: string
}

/**
 * Mock S3 CompleteMultipartUploadOutput
 */
interface MockS3CompleteMultipartUploadOutput {
  Location?: string
  Bucket?: string
  Key?: string
  ETag?: string
}

/**
 * Mock S3 AbortMultipartUploadOutput
 */
interface MockS3AbortMultipartUploadOutput {}

/**
 * Mock S3Client interface
 */
interface MockS3Client {
  send: ReturnType<typeof vi.fn>
}

/**
 * Mock command classes
 */
const mockGetObjectCommand = vi.fn()
const mockPutObjectCommand = vi.fn()
const mockDeleteObjectCommand = vi.fn()
const mockHeadObjectCommand = vi.fn()
const mockListObjectsV2Command = vi.fn()
const mockCreateMultipartUploadCommand = vi.fn()
const mockUploadPartCommand = vi.fn()
const mockCompleteMultipartUploadCommand = vi.fn()
const mockAbortMultipartUploadCommand = vi.fn()

// =============================================================================
// MOCK FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a mock S3Client for testing
 */
function createMockS3Client(): MockS3Client {
  return {
    send: vi.fn(),
  }
}

/**
 * Create a mock GetObject response
 */
function createMockGetObjectResponse(
  data: Uint8Array,
  metadata?: Partial<MockS3GetObjectOutput>
): MockS3GetObjectOutput {
  return {
    Body: {
      async transformToByteArray() {
        return data
      },
    },
    ContentLength: data.length,
    LastModified: metadata?.LastModified ?? new Date(),
    ETag: metadata?.ETag ?? '"mock-etag"',
    ...metadata,
  }
}

/**
 * Create a mock HeadObject response
 */
function createMockHeadObjectResponse(
  size: number,
  metadata?: Partial<MockS3HeadObjectOutput>
): MockS3HeadObjectOutput {
  return {
    ContentLength: size,
    LastModified: metadata?.LastModified ?? new Date(),
    ETag: metadata?.ETag ?? '"mock-etag"',
    ...metadata,
  }
}

/**
 * Create a mock ListObjectsV2 response
 */
function createMockListResponse(
  objects: Array<{ Key: string; Size: number }>,
  options?: { isTruncated?: boolean; nextToken?: string }
): MockS3ListObjectsV2Output {
  return {
    Contents: objects.map(obj => ({
      Key: obj.Key,
      Size: obj.Size,
      LastModified: new Date(),
      ETag: `"${obj.Key}-etag"`,
    })),
    IsTruncated: options?.isTruncated ?? false,
    NextContinuationToken: options?.nextToken,
    KeyCount: objects.length,
  }
}

/**
 * Create an S3 NoSuchKey error
 */
function createNoSuchKeyError(): Error {
  const error = new Error('The specified key does not exist.')
  ;(error as any).name = 'NoSuchKey'
  ;(error as any).$metadata = { httpStatusCode: 404 }
  return error
}

/**
 * Create an S3 AccessDenied error
 */
function createAccessDeniedError(): Error {
  const error = new Error('Access Denied')
  ;(error as any).name = 'AccessDenied'
  ;(error as any).$metadata = { httpStatusCode: 403 }
  return error
}

/**
 * Create an S3 NoSuchBucket error
 */
function createNoSuchBucketError(): Error {
  const error = new Error('The specified bucket does not exist')
  ;(error as any).name = 'NoSuchBucket'
  ;(error as any).$metadata = { httpStatusCode: 404 }
  return error
}

/**
 * Create an S3 ServiceUnavailable error
 */
function createServiceUnavailableError(): Error {
  const error = new Error('Service Unavailable')
  ;(error as any).name = 'ServiceUnavailable'
  ;(error as any).$metadata = { httpStatusCode: 503 }
  return error
}

// =============================================================================
// S3STORAGE TESTS
// =============================================================================

describe('S3Storage', () => {
  let storage: S3Storage
  let mockS3Client: MockS3Client

  beforeEach(() => {
    mockS3Client = createMockS3Client()
    storage = new S3Storage({
      bucket: 'test-bucket',
      region: 'us-east-1',
      credentials: {
        accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
        secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      },
    })
    // Inject mock client (implementation would need to support this)
    ;(storage as any)._client = mockS3Client
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  // ===========================================================================
  // CONSTRUCTOR AND CONFIGURATION TESTS
  // ===========================================================================

  describe('constructor and configuration', () => {
    it('should create S3Storage with bucket and region', () => {
      const s3 = new S3Storage({
        bucket: 'my-bucket',
        region: 'us-west-2',
      })
      expect(s3).toBeDefined()
    })

    it('should create S3Storage with explicit credentials', () => {
      const credentials: S3Credentials = {
        accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
        secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      }
      const s3 = new S3Storage({
        bucket: 'my-bucket',
        region: 'us-west-2',
        credentials,
      })
      expect(s3).toBeDefined()
    })

    it('should create S3Storage without credentials (uses environment/IAM)', () => {
      const s3 = new S3Storage({
        bucket: 'my-bucket',
        region: 'eu-west-1',
      })
      expect(s3).toBeDefined()
    })

    it('should handle various AWS regions', () => {
      const regions = [
        'us-east-1',
        'us-west-2',
        'eu-west-1',
        'eu-central-1',
        'ap-northeast-1',
        'ap-southeast-1',
        'sa-east-1',
      ]
      for (const region of regions) {
        const s3 = new S3Storage({ bucket: 'test', region })
        expect(s3).toBeDefined()
      }
    })

    it('should store bucket name correctly', () => {
      const s3 = new S3Storage({
        bucket: 'my-data-bucket',
        region: 'us-east-1',
      })
      // Access internal property (implementation dependent)
      expect((s3 as any).options.bucket).toBe('my-data-bucket')
    })

    it('should store region correctly', () => {
      const s3 = new S3Storage({
        bucket: 'test',
        region: 'ap-southeast-2',
      })
      expect((s3 as any).options.region).toBe('ap-southeast-2')
    })

    it('should accept client via constructor injection', () => {
      const mockClient = createMockS3Client()
      const s3 = new S3Storage({
        bucket: 'my-bucket',
        region: 'us-east-1',
        client: mockClient,
      })
      expect(s3).toBeDefined()
      expect(s3._client).toBe(mockClient)
    })

    it('should use injected client for operations', async () => {
      const mockClient = createMockS3Client()
      const testData = new TextEncoder().encode('Hello via constructor injection!')
      mockClient.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const s3 = new S3Storage({
        bucket: 'my-bucket',
        region: 'us-east-1',
        client: mockClient,
      })

      const result = await s3.read('test/file.txt')

      expect(result).toEqual(testData)
      expect(mockClient.send).toHaveBeenCalledTimes(1)
    })
  })

  // ===========================================================================
  // READ TESTS
  // ===========================================================================

  describe('read()', () => {
    it('should read a file and return Uint8Array', async () => {
      const testData = new TextEncoder().encode('Hello, S3!')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const result = await storage.read('test/file.txt')

      expect(result).toBeInstanceOf(Uint8Array)
      expect(result).toEqual(testData)
    })

    it('should read binary data correctly', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(binaryData))

      const result = await storage.read('binary/data.bin')

      expect(result).toEqual(binaryData)
    })

    it('should read large files', async () => {
      const largeData = new Uint8Array(1024 * 1024) // 1MB
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(largeData))

      const result = await storage.read('large/file.bin')

      expect(result.length).toBe(1024 * 1024)
      expect(result[0]).toBe(0)
      expect(result[255]).toBe(255)
    })

    it('should read empty file', async () => {
      const emptyData = new Uint8Array(0)
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(emptyData))

      const result = await storage.read('empty/file.txt')

      expect(result.length).toBe(0)
    })

    it('should throw error when file does not exist', async () => {
      mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

      await expect(storage.read('nonexistent/file.txt')).rejects.toThrow()
    })

    it('should throw descriptive error including file path', async () => {
      mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

      await expect(storage.read('specific/path/to/file.txt')).rejects.toThrow(
        /not found|does not exist|NoSuchKey/i
      )
    })

    it('should propagate access denied errors', async () => {
      mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

      await expect(storage.read('private/file.txt')).rejects.toThrow(/access denied/i)
    })

    it('should handle paths with special characters', async () => {
      const testData = new TextEncoder().encode('special')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const result = await storage.read('path/with spaces/and-dashes/file.txt')

      expect(result).toEqual(testData)
    })

    it('should handle deeply nested paths', async () => {
      const testData = new TextEncoder().encode('nested')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const result = await storage.read('a/b/c/d/e/f/g/file.txt')

      expect(result).toEqual(testData)
    })

    it('should send correct bucket and key to S3', async () => {
      const testData = new TextEncoder().encode('test')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      await storage.read('my/key/path.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
      // Verify command was called with correct parameters (implementation specific)
    })
  })

  // ===========================================================================
  // WRITE TESTS
  // ===========================================================================

  describe('write()', () => {
    it('should write data to a file', async () => {
      const testData = new TextEncoder().encode('Hello, S3!')
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await storage.write('test/file.txt', testData)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should write binary data', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await storage.write('binary/data.bin', binaryData)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should write empty file', async () => {
      const emptyData = new Uint8Array(0)
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await storage.write('empty/file.txt', emptyData)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should overwrite existing files', async () => {
      const newData = new TextEncoder().encode('New content')
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"new-etag"' } as MockS3PutObjectOutput)

      await storage.write('existing/file.txt', newData)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should create intermediate paths automatically (S3 has no directories)', async () => {
      const testData = new TextEncoder().encode('data')
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await storage.write('new/nested/path/file.txt', testData)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should propagate access denied errors on write', async () => {
      mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

      await expect(storage.write('protected/file.txt', new Uint8Array([1]))).rejects.toThrow(
        /access denied/i
      )
    })

    it('should propagate bucket not found errors', async () => {
      mockS3Client.send.mockRejectedValueOnce(createNoSuchBucketError())

      await expect(storage.write('file.txt', new Uint8Array([1]))).rejects.toThrow(
        /bucket|NoSuchBucket/i
      )
    })

    it('should handle service unavailable errors', async () => {
      mockS3Client.send.mockRejectedValueOnce(createServiceUnavailableError())

      await expect(storage.write('file.txt', new Uint8Array([1]))).rejects.toThrow()
    })
  })

  // ===========================================================================
  // WRITE WITH MULTIPART UPLOAD TESTS
  // ===========================================================================

  describe('write() with multipart upload for large files', () => {
    const MULTIPART_THRESHOLD = 5 * 1024 * 1024 // 5MB - typical S3 multipart threshold

    it('should use multipart upload for files larger than threshold', async () => {
      const largeData = new Uint8Array(10 * 1024 * 1024) // 10MB

      // Mock multipart upload sequence
      mockS3Client.send
        .mockResolvedValueOnce({ UploadId: 'upload-123' } as MockS3CreateMultipartUploadOutput)
        .mockResolvedValueOnce({ ETag: '"part-1-etag"' } as MockS3UploadPartOutput)
        .mockResolvedValueOnce({ ETag: '"part-2-etag"' } as MockS3UploadPartOutput)
        .mockResolvedValueOnce({ ETag: '"final-etag"' } as MockS3CompleteMultipartUploadOutput)

      await storage.write('large/file.bin', largeData)

      // Should have called: CreateMultipartUpload, UploadPart (x2), CompleteMultipartUpload
      expect(mockS3Client.send).toHaveBeenCalledTimes(4)
    })

    it('should handle multipart upload with many parts', async () => {
      const veryLargeData = new Uint8Array(50 * 1024 * 1024) // 50MB = 10 parts at 5MB each

      // Mock multipart upload sequence
      const mockCalls = [
        { UploadId: 'upload-456' } as MockS3CreateMultipartUploadOutput,
        ...Array(10).fill({ ETag: '"part-etag"' } as MockS3UploadPartOutput),
        { ETag: '"final-etag"' } as MockS3CompleteMultipartUploadOutput,
      ]

      for (const response of mockCalls) {
        mockS3Client.send.mockResolvedValueOnce(response)
      }

      await storage.write('very-large/file.bin', veryLargeData)

      // CreateMultipartUpload + 10 UploadPart + CompleteMultipartUpload = 12 calls
      expect(mockS3Client.send).toHaveBeenCalledTimes(12)
    })

    it('should abort multipart upload on failure', async () => {
      const largeData = new Uint8Array(10 * 1024 * 1024) // 10MB

      mockS3Client.send
        .mockResolvedValueOnce({ UploadId: 'upload-789' } as MockS3CreateMultipartUploadOutput)
        .mockResolvedValueOnce({ ETag: '"part-1-etag"' } as MockS3UploadPartOutput)
        .mockRejectedValueOnce(new Error('Upload failed'))
        .mockResolvedValueOnce({} as MockS3AbortMultipartUploadOutput)

      await expect(storage.write('failing/file.bin', largeData)).rejects.toThrow('Upload failed')

      // Should have called AbortMultipartUpload
      expect(mockS3Client.send).toHaveBeenCalledTimes(4)
    })

    it('should use simple PutObject for small files', async () => {
      const smallData = new Uint8Array(1024) // 1KB
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await storage.write('small/file.txt', smallData)

      // Should use single PutObject, not multipart
      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should use correct part size for multipart uploads', async () => {
      const largeData = new Uint8Array(15 * 1024 * 1024) // 15MB = 3 parts at 5MB each

      mockS3Client.send
        .mockResolvedValueOnce({ UploadId: 'upload-abc' } as MockS3CreateMultipartUploadOutput)
        .mockResolvedValueOnce({ ETag: '"part-1"' } as MockS3UploadPartOutput)
        .mockResolvedValueOnce({ ETag: '"part-2"' } as MockS3UploadPartOutput)
        .mockResolvedValueOnce({ ETag: '"part-3"' } as MockS3UploadPartOutput)
        .mockResolvedValueOnce({ ETag: '"final"' } as MockS3CompleteMultipartUploadOutput)

      await storage.write('medium-large/file.bin', largeData)

      expect(mockS3Client.send).toHaveBeenCalledTimes(5)
    })
  })

  // ===========================================================================
  // LIST TESTS
  // ===========================================================================

  describe('list()', () => {
    it('should list files with a prefix', async () => {
      mockS3Client.send.mockResolvedValueOnce(
        createMockListResponse([
          { Key: 'data/file1.txt', Size: 100 },
          { Key: 'data/file2.txt', Size: 200 },
          { Key: 'data/file3.txt', Size: 300 },
        ])
      )

      const result = await storage.list('data/')

      expect(result).toEqual(['data/file1.txt', 'data/file2.txt', 'data/file3.txt'])
    })

    it('should return empty array when no files match prefix', async () => {
      mockS3Client.send.mockResolvedValueOnce(createMockListResponse([]))

      const result = await storage.list('nonexistent/')

      expect(result).toEqual([])
    })

    it('should list all files with empty prefix', async () => {
      mockS3Client.send.mockResolvedValueOnce(
        createMockListResponse([
          { Key: 'file1.txt', Size: 100 },
          { Key: 'dir/file2.txt', Size: 200 },
        ])
      )

      const result = await storage.list('')

      expect(result).toEqual(['file1.txt', 'dir/file2.txt'])
    })

    it('should handle pagination with continuation tokens', async () => {
      mockS3Client.send
        .mockResolvedValueOnce(
          createMockListResponse([{ Key: 'data/file1.txt', Size: 100 }], {
            isTruncated: true,
            nextToken: 'token123',
          })
        )
        .mockResolvedValueOnce(
          createMockListResponse([{ Key: 'data/file2.txt', Size: 200 }], {
            isTruncated: false,
          })
        )

      const result = await storage.list('data/')

      expect(mockS3Client.send).toHaveBeenCalledTimes(2)
      expect(result).toEqual(['data/file1.txt', 'data/file2.txt'])
    })

    it('should handle many pages of results', async () => {
      // Simulate 5 pages of results
      for (let i = 0; i < 4; i++) {
        mockS3Client.send.mockResolvedValueOnce(
          createMockListResponse([{ Key: `data/file${i}.txt`, Size: 100 }], {
            isTruncated: true,
            nextToken: `token${i}`,
          })
        )
      }
      mockS3Client.send.mockResolvedValueOnce(
        createMockListResponse([{ Key: 'data/file4.txt', Size: 100 }])
      )

      const result = await storage.list('data/')

      expect(mockS3Client.send).toHaveBeenCalledTimes(5)
      expect(result).toHaveLength(5)
    })

    it('should handle prefix without trailing slash', async () => {
      mockS3Client.send.mockResolvedValueOnce(
        createMockListResponse([{ Key: 'datafile.txt', Size: 100 }])
      )

      const result = await storage.list('data')

      expect(result).toEqual(['datafile.txt'])
    })

    it('should handle S3 list with 1000 objects per page (default max)', async () => {
      const objects = Array.from({ length: 1000 }, (_, i) => ({
        Key: `prefix/file${i.toString().padStart(4, '0')}.txt`,
        Size: 100,
      }))

      mockS3Client.send
        .mockResolvedValueOnce(createMockListResponse(objects, { isTruncated: true, nextToken: 'next' }))
        .mockResolvedValueOnce(createMockListResponse([{ Key: 'prefix/file1000.txt', Size: 100 }]))

      const result = await storage.list('prefix/')

      expect(result).toHaveLength(1001)
    })

    it('should propagate access denied errors on list', async () => {
      mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

      await expect(storage.list('protected/')).rejects.toThrow(/access denied/i)
    })

    it('should filter out directory markers (keys ending with /)', async () => {
      mockS3Client.send.mockResolvedValueOnce(
        createMockListResponse([
          { Key: 'data/', Size: 0 },
          { Key: 'data/file.txt', Size: 100 },
          { Key: 'data/subdir/', Size: 0 },
        ])
      )

      const result = await storage.list('data')

      // Should only include actual files, not directory markers
      expect(result).toContain('data/file.txt')
      expect(result).not.toContain('data/')
      expect(result).not.toContain('data/subdir/')
    })
  })

  // ===========================================================================
  // DELETE TESTS
  // ===========================================================================

  describe('delete()', () => {
    it('should delete a file', async () => {
      mockS3Client.send.mockResolvedValueOnce({} as MockS3DeleteObjectOutput)

      await storage.delete('test/file.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should not throw when deleting non-existent file (S3 is idempotent)', async () => {
      mockS3Client.send.mockResolvedValueOnce({} as MockS3DeleteObjectOutput)

      await expect(storage.delete('nonexistent/file.txt')).resolves.toBeUndefined()
    })

    it('should delete files with special characters in path', async () => {
      mockS3Client.send.mockResolvedValueOnce({} as MockS3DeleteObjectOutput)

      await storage.delete('path/with spaces/file.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should delete deeply nested files', async () => {
      mockS3Client.send.mockResolvedValueOnce({} as MockS3DeleteObjectOutput)

      await storage.delete('a/b/c/d/e/f/g/file.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should propagate access denied errors on delete', async () => {
      mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

      await expect(storage.delete('protected/file.txt')).rejects.toThrow(/access denied/i)
    })
  })

  // ===========================================================================
  // EXISTS TESTS
  // ===========================================================================

  describe('exists()', () => {
    it('should return true when file exists', async () => {
      mockS3Client.send.mockResolvedValueOnce(createMockHeadObjectResponse(100))

      const result = await storage.exists('test/file.txt')

      expect(result).toBe(true)
    })

    it('should return false when file does not exist', async () => {
      mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

      const result = await storage.exists('nonexistent/file.txt')

      expect(result).toBe(false)
    })

    it('should return true for empty files (size 0)', async () => {
      mockS3Client.send.mockResolvedValueOnce(createMockHeadObjectResponse(0))

      const result = await storage.exists('empty/file.txt')

      expect(result).toBe(true)
    })

    it('should use HeadObject for efficient existence check', async () => {
      mockS3Client.send.mockResolvedValueOnce(createMockHeadObjectResponse(1024 * 1024 * 100))

      await storage.exists('large/file.bin')

      // Should use HEAD, not GET
      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should propagate non-NotFound errors', async () => {
      mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

      await expect(storage.exists('private/file.txt')).rejects.toThrow(/access denied/i)
    })

    it('should handle service unavailable errors', async () => {
      mockS3Client.send.mockRejectedValueOnce(createServiceUnavailableError())

      await expect(storage.exists('file.txt')).rejects.toThrow()
    })
  })

  // ===========================================================================
  // STAT TESTS
  // ===========================================================================

  describe('stat()', () => {
    it('should return file metadata', async () => {
      const uploadDate = new Date('2025-01-15T10:30:00Z')
      mockS3Client.send.mockResolvedValueOnce(
        createMockHeadObjectResponse(12345, {
          LastModified: uploadDate,
          ETag: '"abc123"',
        })
      )

      const result = await storage.stat('test/file.txt')

      expect(result).not.toBeNull()
      expect(result?.size).toBe(12345)
      expect(result?.lastModified).toEqual(uploadDate)
      expect(result?.etag).toBe('"abc123"')
    })

    it('should return null for non-existent file', async () => {
      mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

      const result = await storage.stat('nonexistent/file.txt')

      expect(result).toBeNull()
    })

    it('should return correct size for empty file', async () => {
      mockS3Client.send.mockResolvedValueOnce(createMockHeadObjectResponse(0))

      const result = await storage.stat('empty/file.txt')

      expect(result?.size).toBe(0)
    })

    it('should return correct size for large files (5GB+)', async () => {
      const largeSize = 5 * 1024 * 1024 * 1024 // 5GB
      mockS3Client.send.mockResolvedValueOnce(createMockHeadObjectResponse(largeSize))

      const result = await storage.stat('large/file.bin')

      expect(result?.size).toBe(largeSize)
    })

    it('should propagate non-NotFound errors', async () => {
      mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

      await expect(storage.stat('private/file.txt')).rejects.toThrow(/access denied/i)
    })

    it('should return lastModified as Date object', async () => {
      const uploadDate = new Date('2024-06-15T14:30:00Z')
      mockS3Client.send.mockResolvedValueOnce(
        createMockHeadObjectResponse(100, { LastModified: uploadDate })
      )

      const result = await storage.stat('test/file.txt')

      expect(result?.lastModified).toBeInstanceOf(Date)
      expect(result?.lastModified).toEqual(uploadDate)
    })
  })

  // ===========================================================================
  // READRANGE TESTS (Critical for Parquet)
  // ===========================================================================

  describe('readRange()', () => {
    it('should read a byte range from a file using Range header', async () => {
      const rangeData = new TextEncoder().encode('Hello')
      mockS3Client.send.mockResolvedValueOnce(
        createMockGetObjectResponse(rangeData, { ContentRange: 'bytes 0-4/13' })
      )

      const result = await storage.readRange('test/file.txt', 0, 5)

      expect(result).toEqual(rangeData)
    })

    it('should read range from middle of file', async () => {
      const rangeData = new TextEncoder().encode('World')
      mockS3Client.send.mockResolvedValueOnce(
        createMockGetObjectResponse(rangeData, { ContentRange: 'bytes 7-11/13' })
      )

      const result = await storage.readRange('test/file.txt', 7, 12)

      expect(result).toEqual(rangeData)
    })

    it('should read range at end of file (Parquet footer pattern)', async () => {
      const fileSize = 1024 * 1024 // 1MB
      const footerSize = 100
      const footerData = new Uint8Array(footerSize)
      mockS3Client.send.mockResolvedValueOnce(
        createMockGetObjectResponse(footerData, {
          ContentRange: `bytes ${fileSize - footerSize}-${fileSize - 1}/${fileSize}`,
        })
      )

      const result = await storage.readRange('data/table.parquet', fileSize - footerSize, fileSize)

      expect(result.length).toBe(footerSize)
    })

    it('should handle single byte range', async () => {
      const singleByte = new Uint8Array([0x42])
      mockS3Client.send.mockResolvedValueOnce(
        createMockGetObjectResponse(singleByte, { ContentRange: 'bytes 10-10/100' })
      )

      const result = await storage.readRange('test/file.txt', 10, 11)

      expect(result).toEqual(singleByte)
    })

    it('should throw error when file does not exist', async () => {
      mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

      await expect(storage.readRange('nonexistent/file.txt', 0, 10)).rejects.toThrow()
    })

    it('should handle zero-length range', async () => {
      const emptyData = new Uint8Array(0)
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(emptyData))

      const result = await storage.readRange('test/file.txt', 0, 0)

      expect(result.length).toBe(0)
    })

    it('should read binary data correctly in range', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0x04])
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(binaryData))

      const result = await storage.readRange('binary/data.bin', 0, 5)

      expect(result).toEqual(binaryData)
    })

    it('should send correct Range header format', async () => {
      const rangeData = new Uint8Array(100)
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(rangeData))

      await storage.readRange('large/file.bin', 1000000, 1000100)

      // Verify Range header was sent correctly (implementation specific)
      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle Parquet magic number read pattern', async () => {
      // Parquet files start and end with "PAR1" magic bytes
      const magic = new TextEncoder().encode('PAR1')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(magic))

      const result = await storage.readRange('data.parquet', 0, 4)

      expect(new TextDecoder().decode(result)).toBe('PAR1')
    })

    it('should support reading footer length + magic (last 8 bytes)', async () => {
      // Common Parquet pattern: read last 8 bytes to get footer length + magic
      const footer = new Uint8Array([0x00, 0x10, 0x00, 0x00, 0x50, 0x41, 0x52, 0x31]) // length + PAR1
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(footer))

      const result = await storage.readRange('data.parquet', 992, 1000)

      expect(result.length).toBe(8)
      expect(result.slice(4)).toEqual(new TextEncoder().encode('PAR1'))
    })
  })

  // ===========================================================================
  // ERROR HANDLING TESTS
  // ===========================================================================

  describe('error handling', () => {
    describe('NoSuchKey errors', () => {
      it('should throw descriptive error for read', async () => {
        mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

        await expect(storage.read('missing.txt')).rejects.toThrow(/not found|NoSuchKey/i)
      })

      it('should return null from stat for missing file', async () => {
        mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

        const result = await storage.stat('missing.txt')
        expect(result).toBeNull()
      })

      it('should return false from exists for missing file', async () => {
        mockS3Client.send.mockRejectedValueOnce(createNoSuchKeyError())

        const result = await storage.exists('missing.txt')
        expect(result).toBe(false)
      })
    })

    describe('AccessDenied errors', () => {
      it('should propagate on read', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.read('protected.txt')).rejects.toThrow(/access denied/i)
      })

      it('should propagate on write', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.write('protected.txt', new Uint8Array([1]))).rejects.toThrow(
          /access denied/i
        )
      })

      it('should propagate on delete', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.delete('protected.txt')).rejects.toThrow(/access denied/i)
      })

      it('should propagate on list', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.list('')).rejects.toThrow(/access denied/i)
      })

      it('should propagate on exists', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.exists('protected.txt')).rejects.toThrow(/access denied/i)
      })

      it('should propagate on stat', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.stat('protected.txt')).rejects.toThrow(/access denied/i)
      })

      it('should propagate on readRange', async () => {
        mockS3Client.send.mockRejectedValueOnce(createAccessDeniedError())

        await expect(storage.readRange('protected.txt', 0, 10)).rejects.toThrow(/access denied/i)
      })
    })

    describe('NoSuchBucket errors', () => {
      it('should provide clear error message', async () => {
        mockS3Client.send.mockRejectedValueOnce(createNoSuchBucketError())

        await expect(storage.read('file.txt')).rejects.toThrow(/bucket/i)
      })
    })

    describe('ServiceUnavailable errors', () => {
      it('should propagate service errors', async () => {
        mockS3Client.send.mockRejectedValueOnce(createServiceUnavailableError())

        await expect(storage.read('file.txt')).rejects.toThrow()
      })
    })

    describe('network errors', () => {
      it('should handle network timeout', async () => {
        const timeoutError = new Error('Request timeout')
        ;(timeoutError as any).name = 'TimeoutError'
        mockS3Client.send.mockRejectedValueOnce(timeoutError)

        await expect(storage.read('file.txt')).rejects.toThrow()
      })

      it('should handle connection reset', async () => {
        const connectionError = new Error('Connection reset')
        ;(connectionError as any).code = 'ECONNRESET'
        mockS3Client.send.mockRejectedValueOnce(connectionError)

        await expect(storage.read('file.txt')).rejects.toThrow()
      })
    })
  })

  // ===========================================================================
  // PATH HANDLING TESTS
  // ===========================================================================

  describe('path handling', () => {
    it('should handle paths with leading slash', async () => {
      const testData = new TextEncoder().encode('data')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      // S3 keys shouldn't start with slash, implementation should handle this
      await storage.read('/leading/slash.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle paths with consecutive slashes', async () => {
      const testData = new TextEncoder().encode('data')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      await storage.read('double//slash.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle Unicode in paths', async () => {
      const testData = new TextEncoder().encode('data')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const unicodePath = 'files/\u4e2d\u6587/\u0442\u0435\u0441\u0442.txt'
      await storage.read(unicodePath)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle very long paths', async () => {
      const testData = new TextEncoder().encode('data')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const longPath = 'a/'.repeat(100) + 'file.txt'
      await storage.read(longPath)

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle paths with URL-encoded characters', async () => {
      const testData = new TextEncoder().encode('data')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      await storage.read('path%20with%20spaces/file.txt')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle Delta Lake path patterns', async () => {
      const testData = new TextEncoder().encode('{}')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      await storage.read('my_table/_delta_log/00000000000000000000.json')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle Parquet file paths', async () => {
      const testData = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // PAR1
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      await storage.read('data/year=2024/month=01/part-00000-uuid.parquet')

      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })
  })

  // ===========================================================================
  // INTEGRATION SCENARIOS
  // ===========================================================================

  describe('integration scenarios', () => {
    it('should support write then read workflow', async () => {
      const testData = new TextEncoder().encode('Test content')
      mockS3Client.send
        .mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)
        .mockResolvedValueOnce(createMockGetObjectResponse(testData))

      await storage.write('test/file.txt', testData)
      const result = await storage.read('test/file.txt')

      expect(result).toEqual(testData)
    })

    it('should support write, check exists, then delete workflow', async () => {
      const testData = new TextEncoder().encode('To be deleted')
      mockS3Client.send
        .mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)
        .mockResolvedValueOnce(createMockHeadObjectResponse(testData.length))
        .mockResolvedValueOnce({} as MockS3DeleteObjectOutput)

      await storage.write('temp/file.txt', testData)
      const exists = await storage.exists('temp/file.txt')
      expect(exists).toBe(true)

      await storage.delete('temp/file.txt')
      expect(mockS3Client.send).toHaveBeenCalledTimes(3)
    })

    it('should support stat before readRange for Parquet files', async () => {
      const fileSize = 1024 * 1024 // 1MB
      const footerSize = 8
      const footer = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0xde, 0xad, 0xbe, 0xef])

      mockS3Client.send
        .mockResolvedValueOnce(createMockHeadObjectResponse(fileSize))
        .mockResolvedValueOnce(createMockGetObjectResponse(footer))

      const stat = await storage.stat('data/table.parquet')
      expect(stat?.size).toBe(fileSize)

      const result = await storage.readRange('data/table.parquet', fileSize - footerSize, fileSize)
      expect(result.length).toBe(footerSize)
    })

    it('should support listing and reading multiple files', async () => {
      mockS3Client.send
        .mockResolvedValueOnce(
          createMockListResponse([
            { Key: 'batch/file1.txt', Size: 100 },
            { Key: 'batch/file2.txt', Size: 200 },
          ])
        )
        .mockResolvedValueOnce(createMockGetObjectResponse(new TextEncoder().encode('content1')))
        .mockResolvedValueOnce(createMockGetObjectResponse(new TextEncoder().encode('content2')))

      const files = await storage.list('batch/')
      expect(files).toHaveLength(2)

      const contents = await Promise.all(files.map(f => storage.read(f)))
      expect(contents).toHaveLength(2)
    })
  })

  // ===========================================================================
  // CONCURRENT OPERATIONS
  // ===========================================================================

  describe('concurrent operations', () => {
    it('should handle concurrent reads', async () => {
      mockS3Client.send
        .mockResolvedValueOnce(createMockGetObjectResponse(new TextEncoder().encode('content1')))
        .mockResolvedValueOnce(createMockGetObjectResponse(new TextEncoder().encode('content2')))
        .mockResolvedValueOnce(createMockGetObjectResponse(new TextEncoder().encode('content3')))

      const results = await Promise.all([
        storage.read('file1.txt'),
        storage.read('file2.txt'),
        storage.read('file3.txt'),
      ])

      expect(results).toHaveLength(3)
      expect(mockS3Client.send).toHaveBeenCalledTimes(3)
    })

    it('should handle concurrent writes', async () => {
      mockS3Client.send
        .mockResolvedValue({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await Promise.all([
        storage.write('file1.txt', new Uint8Array([1])),
        storage.write('file2.txt', new Uint8Array([2])),
        storage.write('file3.txt', new Uint8Array([3])),
      ])

      expect(mockS3Client.send).toHaveBeenCalledTimes(3)
    })

    it('should handle mixed concurrent operations', async () => {
      mockS3Client.send
        .mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)
        .mockResolvedValueOnce(createMockGetObjectResponse(new TextEncoder().encode('data')))
        .mockResolvedValueOnce(createMockHeadObjectResponse(100))
        .mockResolvedValueOnce(createMockListResponse([{ Key: 'file.txt', Size: 100 }]))

      await Promise.all([
        storage.write('new.txt', new Uint8Array([1])),
        storage.read('existing.txt'),
        storage.exists('check.txt'),
        storage.list('prefix/'),
      ])

      expect(mockS3Client.send).toHaveBeenCalledTimes(4)
    })
  })

  // ===========================================================================
  // EDGE CASES
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle file with no extension', async () => {
      const testData = new TextEncoder().encode('no extension')
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(testData))

      const result = await storage.read('path/to/file')

      expect(result).toEqual(testData)
    })

    it('should handle file that is exactly 5MB (multipart threshold)', async () => {
      const exactThresholdData = new Uint8Array(5 * 1024 * 1024)
      mockS3Client.send.mockResolvedValueOnce({ ETag: '"test-etag"' } as MockS3PutObjectOutput)

      await storage.write('exactly5mb.bin', exactThresholdData)

      // Should use single PutObject at exactly 5MB (threshold is >5MB for multipart)
      expect(mockS3Client.send).toHaveBeenCalledTimes(1)
    })

    it('should handle empty bucket (no files)', async () => {
      mockS3Client.send.mockResolvedValueOnce(createMockListResponse([]))

      const result = await storage.list('')

      expect(result).toEqual([])
    })

    it('should preserve all byte values (0x00 to 0xFF)', async () => {
      const allBytes = new Uint8Array(256)
      for (let i = 0; i < 256; i++) {
        allBytes[i] = i
      }
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(allBytes))

      const result = await storage.read('all-bytes.bin')

      expect(result).toEqual(allBytes)
    })

    it('should handle null byte in binary content', async () => {
      const withNulls = new Uint8Array([0, 0, 0, 65, 0, 0, 66, 0])
      mockS3Client.send.mockResolvedValueOnce(createMockGetObjectResponse(withNulls))

      const result = await storage.read('nulls.bin')

      expect(result).toEqual(withNulls)
    })
  })
})

// =============================================================================
// S3 CREDENTIALS CONFIGURATION TESTS
// =============================================================================

describe('S3Storage credentials configuration', () => {
  it('should accept access key ID and secret access key', () => {
    const storage = new S3Storage({
      bucket: 'test-bucket',
      region: 'us-east-1',
      credentials: {
        accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
        secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      },
    })

    expect(storage).toBeDefined()
    expect((storage as any).options.credentials.accessKeyId).toBe('AKIAIOSFODNN7EXAMPLE')
  })

  it('should work without explicit credentials (IAM role/environment)', () => {
    const storage = new S3Storage({
      bucket: 'test-bucket',
      region: 'us-east-1',
    })

    expect(storage).toBeDefined()
    expect((storage as any).options.credentials).toBeUndefined()
  })

  it('should store region for endpoint construction', () => {
    const storage = new S3Storage({
      bucket: 'test-bucket',
      region: 'eu-central-1',
    })

    expect((storage as any).options.region).toBe('eu-central-1')
  })

  it('should handle different bucket naming patterns', () => {
    const bucketNames = [
      'simple-bucket',
      'bucket.with.dots',
      'bucket-with-dashes-123',
      'a'.repeat(63), // max length
    ]

    for (const bucket of bucketNames) {
      const storage = new S3Storage({
        bucket,
        region: 'us-east-1',
      })
      expect((storage as any).options.bucket).toBe(bucket)
    }
  })
})

// =============================================================================
// S3 SPECIFIC BEHAVIOR TESTS (Requires client to be set before operations)
// =============================================================================

describe('S3Storage without client configured', () => {
  let storage: S3Storage

  beforeEach(() => {
    storage = new S3Storage({
      bucket: 'test-bucket',
      region: 'us-east-1',
    })
  })

  it('should throw "S3 client not initialized" for read()', async () => {
    await expect(storage.read('test.txt')).rejects.toThrow('S3 client not initialized')
  })

  it('should throw "S3 client not initialized" for write()', async () => {
    await expect(storage.write('test.txt', new Uint8Array([1]))).rejects.toThrow('S3 client not initialized')
  })

  it('should throw "S3 client not initialized" for list()', async () => {
    await expect(storage.list('')).rejects.toThrow('S3 client not initialized')
  })

  it('should throw "S3 client not initialized" for delete()', async () => {
    await expect(storage.delete('test.txt')).rejects.toThrow('S3 client not initialized')
  })

  it('should throw "S3 client not initialized" for exists()', async () => {
    await expect(storage.exists('test.txt')).rejects.toThrow('S3 client not initialized')
  })

  it('should throw "S3 client not initialized" for stat()', async () => {
    await expect(storage.stat('test.txt')).rejects.toThrow('S3 client not initialized')
  })

  it('should throw "S3 client not initialized" for readRange()', async () => {
    await expect(storage.readRange('test.txt', 0, 10)).rejects.toThrow('S3 client not initialized')
  })
})
