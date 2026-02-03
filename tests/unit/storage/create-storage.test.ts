/**
 * Tests for createStorage() factory with URL auto-detection.
 *
 * Tests cover:
 * - URL parsing (parseStorageUrl)
 * - Factory creation from URLs (createStorageFromUrl)
 * - Factory creation with overloaded createStorage()
 */

import { describe, it, expect } from 'vitest'
import {
  createStorage,
  createStorageFromUrl,
  parseStorageUrl,
  MemoryStorage,
  FileSystemStorage,
  R2Storage,
  S3Storage,
} from '../../../src/storage/index.js'

// =============================================================================
// HELPER: Mock R2 Bucket
// =============================================================================

function createMockR2Bucket(): R2Bucket {
  return {
    head: async () => null,
    get: async () => null,
    put: async () => ({} as R2Object),
    delete: async () => {},
    list: async () => ({ objects: [], truncated: false, delimitedPrefixes: [] } as R2Objects),
    createMultipartUpload: async () => ({} as R2MultipartUpload),
    resumeMultipartUpload: () => ({} as R2MultipartUpload),
  } as R2Bucket
}

// =============================================================================
// parseStorageUrl() TESTS
// =============================================================================

describe('parseStorageUrl', () => {
  describe('memory:// URLs', () => {
    it('should parse memory:// as memory storage', () => {
      const result = parseStorageUrl('memory://')
      expect(result.type).toBe('memory')
      expect(result.path).toBe('')
    })

    it('should parse memory://name with path', () => {
      const result = parseStorageUrl('memory://test-storage')
      expect(result.type).toBe('memory')
      expect(result.path).toBe('test-storage')
    })
  })

  describe('file:// URLs', () => {
    it('should parse file:///path as filesystem storage', () => {
      const result = parseStorageUrl('file:///data/lake')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('/data/lake')
    })

    it('should parse file:// with localhost', () => {
      const result = parseStorageUrl('file://localhost/data/lake')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('/data/lake')
    })

    it('should parse Windows-style file:///C:/path', () => {
      const result = parseStorageUrl('file:///C:/Users/data')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('C:/Users/data')
    })

    it('should handle file:// with nested paths', () => {
      const result = parseStorageUrl('file:///a/b/c/d/e')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('/a/b/c/d/e')
    })
  })

  describe('absolute paths (no scheme)', () => {
    it('should parse /path as filesystem storage', () => {
      const result = parseStorageUrl('/data/lake')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('/data/lake')
    })

    it('should parse root path /', () => {
      const result = parseStorageUrl('/')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('/')
    })
  })

  describe('relative paths', () => {
    it('should parse ./path as filesystem storage', () => {
      const result = parseStorageUrl('./data/lake')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('./data/lake')
    })

    it('should parse ../path as filesystem storage', () => {
      const result = parseStorageUrl('../data/lake')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('../data/lake')
    })

    it('should parse plain directory name as filesystem storage', () => {
      const result = parseStorageUrl('data')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('data')
    })

    it('should parse nested relative path as filesystem storage', () => {
      const result = parseStorageUrl('data/lake/tables')
      expect(result.type).toBe('filesystem')
      expect(result.path).toBe('data/lake/tables')
    })
  })

  describe('s3:// URLs', () => {
    it('should parse s3://bucket as S3 storage', () => {
      const result = parseStorageUrl('s3://my-bucket')
      expect(result.type).toBe('s3')
      expect(result.bucket).toBe('my-bucket')
      expect(result.path).toBe('')
      expect(result.region).toBe('us-east-1')
    })

    it('should parse s3://bucket/path with prefix', () => {
      const result = parseStorageUrl('s3://my-bucket/data/tables')
      expect(result.type).toBe('s3')
      expect(result.bucket).toBe('my-bucket')
      expect(result.path).toBe('data/tables')
      expect(result.region).toBe('us-east-1')
    })

    it('should parse s3://bucket.s3.region.amazonaws.com/path', () => {
      const result = parseStorageUrl('s3://my-bucket.s3.us-west-2.amazonaws.com/data')
      expect(result.type).toBe('s3')
      expect(result.bucket).toBe('my-bucket')
      expect(result.path).toBe('data')
      expect(result.region).toBe('us-west-2')
    })

    it('should parse s3://bucket.s3-region.amazonaws.com/path (hyphen style)', () => {
      const result = parseStorageUrl('s3://my-bucket.s3-eu-central-1.amazonaws.com/prefix')
      expect(result.type).toBe('s3')
      expect(result.bucket).toBe('my-bucket')
      expect(result.path).toBe('prefix')
      expect(result.region).toBe('eu-central-1')
    })

    it('should handle bucket names with dots', () => {
      const result = parseStorageUrl('s3://my.bucket.name/path')
      expect(result.type).toBe('s3')
      expect(result.bucket).toBe('my.bucket.name')
      expect(result.path).toBe('path')
    })

    it('should handle deeply nested paths', () => {
      const result = parseStorageUrl('s3://bucket/a/b/c/d/e/f')
      expect(result.type).toBe('s3')
      expect(result.bucket).toBe('bucket')
      expect(result.path).toBe('a/b/c/d/e/f')
    })
  })

  describe('r2:// URLs', () => {
    it('should parse r2://bucket as R2 storage', () => {
      const result = parseStorageUrl('r2://my-bucket')
      expect(result.type).toBe('r2')
      expect(result.bucket).toBe('my-bucket')
      expect(result.path).toBe('')
    })

    it('should parse r2://bucket/path with prefix', () => {
      const result = parseStorageUrl('r2://my-bucket/data/tables')
      expect(result.type).toBe('r2')
      expect(result.bucket).toBe('my-bucket')
      expect(result.path).toBe('data/tables')
    })

    it('should handle bucket names with hyphens', () => {
      const result = parseStorageUrl('r2://my-r2-bucket/prefix')
      expect(result.type).toBe('r2')
      expect(result.bucket).toBe('my-r2-bucket')
      expect(result.path).toBe('prefix')
    })
  })

  describe('error handling', () => {
    it('should throw for unrecognized URL scheme', () => {
      expect(() => parseStorageUrl('ftp://server/path')).toThrow(/Unrecognized storage URL format/)
    })

    it('should throw for http:// URLs', () => {
      expect(() => parseStorageUrl('http://example.com/data')).toThrow(/Unrecognized storage URL format/)
    })

    it('should throw for https:// URLs', () => {
      expect(() => parseStorageUrl('https://example.com/data')).toThrow(/Unrecognized storage URL format/)
    })
  })
})

// =============================================================================
// createStorageFromUrl() TESTS
// =============================================================================

describe('createStorageFromUrl', () => {
  describe('memory storage', () => {
    it('should create MemoryStorage from memory://', () => {
      const storage = createStorageFromUrl('memory://')
      expect(storage).toBeInstanceOf(MemoryStorage)
    })

    it('should create MemoryStorage from memory://name', () => {
      const storage = createStorageFromUrl('memory://test')
      expect(storage).toBeInstanceOf(MemoryStorage)
    })
  })

  describe('filesystem storage', () => {
    it('should create FileSystemStorage from file:///path', () => {
      const storage = createStorageFromUrl('file:///data/lake')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should create FileSystemStorage from absolute path', () => {
      const storage = createStorageFromUrl('/data/lake')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should create FileSystemStorage from relative path', () => {
      const storage = createStorageFromUrl('./data/lake')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should create FileSystemStorage from plain directory name', () => {
      const storage = createStorageFromUrl('data')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })
  })

  describe('S3 storage', () => {
    it('should create S3Storage from s3://bucket', () => {
      const storage = createStorageFromUrl('s3://my-bucket')
      expect(storage).toBeInstanceOf(S3Storage)
      expect((storage as S3Storage).options.bucket).toBe('my-bucket')
      expect((storage as S3Storage).options.region).toBe('us-east-1')
    })

    it('should create S3Storage from s3://bucket/path', () => {
      const storage = createStorageFromUrl('s3://my-bucket/data')
      expect(storage).toBeInstanceOf(S3Storage)
      expect((storage as S3Storage).options.bucket).toBe('my-bucket')
    })

    it('should extract region from S3 URL', () => {
      const storage = createStorageFromUrl('s3://my-bucket.s3.eu-west-1.amazonaws.com/data')
      expect(storage).toBeInstanceOf(S3Storage)
      expect((storage as S3Storage).options.bucket).toBe('my-bucket')
      expect((storage as S3Storage).options.region).toBe('eu-west-1')
    })

    it('should allow overriding region via options', () => {
      const storage = createStorageFromUrl('s3://my-bucket/data', { region: 'ap-southeast-1' })
      expect(storage).toBeInstanceOf(S3Storage)
      expect((storage as S3Storage).options.region).toBe('ap-southeast-1')
    })

    it('should pass credentials via options', () => {
      const credentials = { accessKeyId: 'AKIA...', secretAccessKey: 'secret' }
      const storage = createStorageFromUrl('s3://my-bucket/data', { credentials })
      expect(storage).toBeInstanceOf(S3Storage)
      expect((storage as S3Storage).options.credentials).toBe(credentials)
    })
  })

  describe('R2 storage', () => {
    it('should throw when r2:// URL is used without bucket binding', () => {
      expect(() => createStorageFromUrl('r2://my-bucket')).toThrow(/R2 storage requires a bucket binding/)
    })

    it('should create R2Storage when bucket binding is provided', () => {
      const mockBucket = createMockR2Bucket()
      const storage = createStorageFromUrl('r2://my-bucket/data', { bucket: mockBucket })
      expect(storage).toBeInstanceOf(R2Storage)
    })
  })
})

// =============================================================================
// createStorage() with URL TESTS
// =============================================================================

describe('createStorage with URL string', () => {
  describe('auto-detect from URL', () => {
    it('should create MemoryStorage from memory://', () => {
      const storage = createStorage('memory://')
      expect(storage).toBeInstanceOf(MemoryStorage)
    })

    it('should create FileSystemStorage from file:///path', () => {
      const storage = createStorage('file:///data/lake')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should create FileSystemStorage from absolute path', () => {
      const storage = createStorage('/data/lake')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should create FileSystemStorage from relative path', () => {
      const storage = createStorage('./data')
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should create S3Storage from s3://bucket', () => {
      const storage = createStorage('s3://my-bucket')
      expect(storage).toBeInstanceOf(S3Storage)
    })

    it('should pass URL options to createStorageFromUrl', () => {
      const mockBucket = createMockR2Bucket()
      const storage = createStorage('r2://my-bucket', { bucket: mockBucket })
      expect(storage).toBeInstanceOf(R2Storage)
    })
  })

  describe('backward compatibility with options object', () => {
    it('should still work with { type: "memory" }', () => {
      const storage = createStorage({ type: 'memory' })
      expect(storage).toBeInstanceOf(MemoryStorage)
    })

    it('should still work with { type: "filesystem", path }', () => {
      const storage = createStorage({ type: 'filesystem', path: '/tmp/test' })
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })

    it('should still work with { type: "s3", bucket, region }', () => {
      const storage = createStorage({ type: 's3', bucket: 'test', region: 'us-west-2' })
      expect(storage).toBeInstanceOf(S3Storage)
    })

    it('should still work with { type: "r2", bucket }', () => {
      const mockBucket = createMockR2Bucket()
      const storage = createStorage({ type: 'r2', bucket: mockBucket })
      expect(storage).toBeInstanceOf(R2Storage)
    })

    it('should still auto-detect environment when called without arguments', () => {
      const storage = createStorage()
      expect(storage).toBeInstanceOf(FileSystemStorage)
    })
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('edge cases', () => {
  it('should handle empty string path for memory storage', () => {
    const storage = createStorage('memory://')
    expect(storage).toBeInstanceOf(MemoryStorage)
  })

  it('should handle paths with special characters', () => {
    const storage = createStorage('file:///data/my-lake_2024/tables')
    expect(storage).toBeInstanceOf(FileSystemStorage)
  })

  it('should handle S3 bucket names with numbers', () => {
    const storage = createStorage('s3://bucket123/path')
    expect(storage).toBeInstanceOf(S3Storage)
    expect((storage as S3Storage).options.bucket).toBe('bucket123')
  })

  it('should handle R2 bucket names with underscores', () => {
    const mockBucket = createMockR2Bucket()
    const storage = createStorage('r2://my_r2_bucket/path', { bucket: mockBucket })
    expect(storage).toBeInstanceOf(R2Storage)
  })

  it('should preserve trailing slashes in paths', () => {
    const result = parseStorageUrl('s3://bucket/path/')
    expect(result.path).toBe('path/')
  })
})
