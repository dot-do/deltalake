/**
 * FileSystemStorage Tests (RED Phase)
 *
 * Comprehensive tests for the FileSystemStorage backend implementation.
 * These tests verify file operations against the local filesystem.
 *
 * Note: These tests run in a Workers environment with nodejs_compat.
 * Uses dynamically imported Node.js fs APIs.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { FileSystemStorage, type FileStat } from '../../../src/storage/index.js'

// Dynamic imports for Node.js fs APIs (available via nodejs_compat)
const getFs = async () => {
  const fs = await import('node:fs/promises')
  return fs
}

const getPath = async () => {
  const path = await import('node:path')
  return path
}

// Use a deterministic base path for tests
const TEST_BASE_DIR = '/tmp/deltalake-filesystem-storage-tests'

describe('FileSystemStorage', () => {
  let storage: FileSystemStorage
  let testDir: string
  let testCounter = 0

  beforeEach(async () => {
    const fs = await getFs()
    const path = await getPath()

    // Create a unique test directory for each test using a counter and timestamp
    testCounter++
    testDir = path.join(TEST_BASE_DIR, `test-${Date.now()}-${testCounter}`)

    // Ensure the test directory exists
    await fs.mkdir(testDir, { recursive: true })

    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    // Clean up test directory
    const fs = await getFs()
    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // READ/WRITE TESTS
  // ===========================================================================

  describe('read() and write()', () => {
    it('should write and read a file', async () => {
      const content = new TextEncoder().encode('Hello, World!')
      await storage.write('test.txt', content)

      const result = await storage.read('test.txt')
      expect(result).toEqual(content)
    })

    it('should write and read binary data', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      await storage.write('binary.bin', binaryData)

      const result = await storage.read('binary.bin')
      expect(result).toEqual(binaryData)
    })

    it('should handle empty files', async () => {
      const emptyData = new Uint8Array(0)
      await storage.write('empty.txt', emptyData)

      const result = await storage.read('empty.txt')
      expect(result).toEqual(emptyData)
      expect(result.length).toBe(0)
    })

    it('should overwrite existing files', async () => {
      const originalContent = new TextEncoder().encode('Original')
      const newContent = new TextEncoder().encode('New Content')

      await storage.write('file.txt', originalContent)
      await storage.write('file.txt', newContent)

      const result = await storage.read('file.txt')
      expect(result).toEqual(newContent)
    })

    it('should throw error when reading non-existent file', async () => {
      await expect(storage.read('nonexistent.txt')).rejects.toThrow()
    })

    it('should handle large files', async () => {
      // Create a 1MB file
      const largeData = new Uint8Array(1024 * 1024)
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }

      await storage.write('large.bin', largeData)
      const result = await storage.read('large.bin')
      expect(result).toEqual(largeData)
    }, 30000) // 30s timeout for large file operations

    it('should handle files with special characters in content', async () => {
      const specialContent = new TextEncoder().encode('Special: \n\r\t\0 Unicode: \u4e2d\u6587')
      await storage.write('special.txt', specialContent)

      const result = await storage.read('special.txt')
      expect(result).toEqual(specialContent)
    })
  })

  // ===========================================================================
  // LIST TESTS
  // ===========================================================================

  describe('list()', () => {
    it('should list files with matching prefix', async () => {
      await storage.write('data/file1.txt', new Uint8Array([1]))
      await storage.write('data/file2.txt', new Uint8Array([2]))
      await storage.write('other/file3.txt', new Uint8Array([3]))

      const result = await storage.list('data/')
      expect(result).toHaveLength(2)
      expect(result).toContain('data/file1.txt')
      expect(result).toContain('data/file2.txt')
      expect(result).not.toContain('other/file3.txt')
    })

    it('should return empty array for non-matching prefix', async () => {
      await storage.write('data/file.txt', new Uint8Array([1]))

      const result = await storage.list('nonexistent/')
      expect(result).toEqual([])
    })

    it('should list all files with empty prefix', async () => {
      await storage.write('file1.txt', new Uint8Array([1]))
      await storage.write('dir/file2.txt', new Uint8Array([2]))

      const result = await storage.list('')
      expect(result).toHaveLength(2)
      expect(result).toContain('file1.txt')
      expect(result).toContain('dir/file2.txt')
    })

    it('should list files in nested directories', async () => {
      await storage.write('a/b/c/file.txt', new Uint8Array([1]))
      await storage.write('a/b/other.txt', new Uint8Array([2]))
      await storage.write('a/root.txt', new Uint8Array([3]))

      const result = await storage.list('a/b/')
      expect(result).toHaveLength(2)
      expect(result).toContain('a/b/c/file.txt')
      expect(result).toContain('a/b/other.txt')
    })

    it('should handle prefix without trailing slash', async () => {
      await storage.write('data-v1/file.txt', new Uint8Array([1]))
      await storage.write('data-v2/file.txt', new Uint8Array([2]))
      await storage.write('data/file.txt', new Uint8Array([3]))

      const result = await storage.list('data-v1')
      expect(result).toHaveLength(1)
      expect(result).toContain('data-v1/file.txt')
    })

    it('should return empty array for empty directory', async () => {
      const result = await storage.list('')
      expect(result).toEqual([])
    })
  })

  // ===========================================================================
  // DELETE TESTS
  // ===========================================================================

  describe('delete()', () => {
    it('should delete an existing file', async () => {
      await storage.write('to-delete.txt', new Uint8Array([1]))
      expect(await storage.exists('to-delete.txt')).toBe(true)

      await storage.delete('to-delete.txt')
      expect(await storage.exists('to-delete.txt')).toBe(false)
    })

    it('should not throw when deleting non-existent file', async () => {
      // Idempotent delete - should not throw
      await expect(storage.delete('nonexistent.txt')).resolves.toBeUndefined()
    })

    it('should only delete the specified file', async () => {
      await storage.write('keep.txt', new Uint8Array([1]))
      await storage.write('delete.txt', new Uint8Array([2]))

      await storage.delete('delete.txt')

      expect(await storage.exists('keep.txt')).toBe(true)
      expect(await storage.exists('delete.txt')).toBe(false)
    })

    it('should delete file from nested directory', async () => {
      await storage.write('deep/nested/file.txt', new Uint8Array([1]))

      await storage.delete('deep/nested/file.txt')
      expect(await storage.exists('deep/nested/file.txt')).toBe(false)
    })
  })

  // ===========================================================================
  // EXISTS TESTS
  // ===========================================================================

  describe('exists()', () => {
    it('should return true for existing file', async () => {
      await storage.write('exists.txt', new Uint8Array([1]))

      const result = await storage.exists('exists.txt')
      expect(result).toBe(true)
    })

    it('should return false for non-existent file', async () => {
      const result = await storage.exists('nonexistent.txt')
      expect(result).toBe(false)
    })

    it('should return false after file is deleted', async () => {
      await storage.write('temp.txt', new Uint8Array([1]))
      await storage.delete('temp.txt')

      const result = await storage.exists('temp.txt')
      expect(result).toBe(false)
    })

    it('should distinguish between file and directory', async () => {
      await storage.write('dir/file.txt', new Uint8Array([1]))

      // The file should exist
      expect(await storage.exists('dir/file.txt')).toBe(true)
      // The directory path should NOT be reported as existing (storage stores files, not directories)
      expect(await storage.exists('dir')).toBe(false)
      expect(await storage.exists('dir/')).toBe(false)
    })

    it('should handle paths with special characters', async () => {
      await storage.write('file with spaces.txt', new Uint8Array([1]))
      await storage.write('file-with-dashes.txt', new Uint8Array([2]))
      await storage.write('file_with_underscores.txt', new Uint8Array([3]))

      expect(await storage.exists('file with spaces.txt')).toBe(true)
      expect(await storage.exists('file-with-dashes.txt')).toBe(true)
      expect(await storage.exists('file_with_underscores.txt')).toBe(true)
    })
  })

  // ===========================================================================
  // STAT TESTS
  // ===========================================================================

  describe('stat()', () => {
    it('should return size and lastModified for existing file', async () => {
      const content = new TextEncoder().encode('Hello, World!')
      await storage.write('stats.txt', content)

      const stat = await storage.stat('stats.txt')

      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(content.length)
      expect(stat!.lastModified).toBeInstanceOf(Date)
      // Note: In miniflare's Node.js compat layer, mtime may be 0 or Unix epoch
      // We only verify it's a valid Date, not that it's recent
      expect(stat!.lastModified.getTime()).toBeGreaterThanOrEqual(0)
    })

    it('should return null for non-existent file', async () => {
      const stat = await storage.stat('nonexistent.txt')
      expect(stat).toBeNull()
    })

    it('should return correct size for various file sizes', async () => {
      const sizes = [0, 1, 100, 1024, 10240]

      for (const size of sizes) {
        const data = new Uint8Array(size)
        await storage.write(`size-${size}.bin`, data)

        const stat = await storage.stat(`size-${size}.bin`)
        expect(stat).not.toBeNull()
        expect(stat!.size).toBe(size)
      }
    })

    it('should update lastModified after file modification', async () => {
      await storage.write('modified.txt', new Uint8Array([1]))
      const stat1 = await storage.stat('modified.txt')

      // Wait a small amount to ensure timestamp difference
      await new Promise((resolve) => setTimeout(resolve, 10))

      await storage.write('modified.txt', new Uint8Array([2, 3]))
      const stat2 = await storage.stat('modified.txt')

      expect(stat2!.lastModified.getTime()).toBeGreaterThanOrEqual(stat1!.lastModified.getTime())
    })

    it('should return size from filesystem stat, not cached', async () => {
      const path = await getPath()
      const fs = await getFs()

      await storage.write('fsstat.txt', new Uint8Array([1, 2, 3]))

      // Write directly to filesystem, bypassing storage
      const fullPath = path.join(testDir, 'fsstat.txt')
      await fs.writeFile(fullPath, new Uint8Array([1, 2, 3, 4, 5]))

      const stat = await storage.stat('fsstat.txt')
      expect(stat!.size).toBe(5)
    })
  })

  // ===========================================================================
  // READRANGE TESTS
  // ===========================================================================

  describe('readRange()', () => {
    it('should read a byte range from file', async () => {
      const content = new TextEncoder().encode('Hello, World!')
      await storage.write('range.txt', content)

      // Read "World"
      const result = await storage.readRange('range.txt', 7, 12)
      expect(new TextDecoder().decode(result)).toBe('World')
    })

    it('should read from start of file', async () => {
      const content = new TextEncoder().encode('ABCDEFGHIJ')
      await storage.write('range.txt', content)

      const result = await storage.readRange('range.txt', 0, 3)
      expect(new TextDecoder().decode(result)).toBe('ABC')
    })

    it('should read to end of file', async () => {
      const content = new TextEncoder().encode('ABCDEFGHIJ')
      await storage.write('range.txt', content)

      const result = await storage.readRange('range.txt', 7, 10)
      expect(new TextDecoder().decode(result)).toBe('HIJ')
    })

    it('should handle single byte read', async () => {
      const content = new Uint8Array([0x00, 0x11, 0x22, 0x33, 0x44])
      await storage.write('bytes.bin', content)

      const result = await storage.readRange('bytes.bin', 2, 3)
      expect(result.length).toBe(1)
      expect(result[0]).toBe(0x22)
    })

    it('should read entire file when range covers full content', async () => {
      const content = new TextEncoder().encode('Full Content')
      await storage.write('full.txt', content)

      const result = await storage.readRange('full.txt', 0, content.length)
      expect(result).toEqual(content)
    })

    it('should throw error when reading range from non-existent file', async () => {
      await expect(storage.readRange('nonexistent.txt', 0, 10)).rejects.toThrow()
    })

    it('should handle reading last bytes (Parquet footer pattern)', async () => {
      // Parquet files have metadata in the last 8 bytes
      const data = new Uint8Array(1000)
      // Set last 8 bytes to magic values
      data[992] = 0x50 // P
      data[993] = 0x41 // A
      data[994] = 0x52 // R
      data[995] = 0x31 // 1
      data[996] = 0xde
      data[997] = 0xad
      data[998] = 0xbe
      data[999] = 0xef

      await storage.write('data.parquet', data)

      const footer = await storage.readRange('data.parquet', 992, 1000)
      expect(footer.length).toBe(8)
      expect(footer[0]).toBe(0x50)
      expect(footer[7]).toBe(0xef)
    })

    it('should handle zero-length range', async () => {
      const content = new TextEncoder().encode('Some content')
      await storage.write('zero.txt', content)

      const result = await storage.readRange('zero.txt', 5, 5)
      expect(result.length).toBe(0)
    })
  })

  // ===========================================================================
  // NESTED DIRECTORY CREATION TESTS
  // ===========================================================================

  describe('nested directory creation', () => {
    it('should create nested directories on write', async () => {
      await storage.write('a/b/c/d/file.txt', new Uint8Array([1]))

      const result = await storage.read('a/b/c/d/file.txt')
      expect(result).toEqual(new Uint8Array([1]))
    })

    it('should handle multiple files in same nested directory', async () => {
      await storage.write('deep/dir/file1.txt', new Uint8Array([1]))
      await storage.write('deep/dir/file2.txt', new Uint8Array([2]))

      expect(await storage.exists('deep/dir/file1.txt')).toBe(true)
      expect(await storage.exists('deep/dir/file2.txt')).toBe(true)
    })

    it('should handle Delta Lake directory structure', async () => {
      // Delta Lake typical structure
      await storage.write('my_table/_delta_log/00000000000000000000.json', new Uint8Array([1]))
      await storage.write('my_table/_delta_log/00000000000000000001.json', new Uint8Array([2]))
      await storage.write('my_table/part-00000.parquet', new Uint8Array([3]))
      await storage.write('my_table/part-00001.parquet', new Uint8Array([4]))

      const logFiles = await storage.list('my_table/_delta_log/')
      expect(logFiles).toHaveLength(2)

      const dataFiles = await storage.list('my_table/')
      expect(dataFiles).toHaveLength(4) // 2 log files + 2 parquet files
    })
  })

  // ===========================================================================
  // PATH NORMALIZATION AND SECURITY TESTS
  // ===========================================================================

  describe('path normalization and security', () => {
    it('should normalize paths with multiple slashes', async () => {
      await storage.write('dir//file.txt', new Uint8Array([1]))

      // Should be accessible with normalized path
      expect(await storage.exists('dir/file.txt')).toBe(true)
    })

    it('should prevent path traversal attacks', async () => {
      // Attempting to write outside basePath should fail or be normalized
      await expect(storage.write('../outside.txt', new Uint8Array([1]))).rejects.toThrow()
      await expect(storage.write('dir/../../outside.txt', new Uint8Array([1]))).rejects.toThrow()
    })

    it('should normalize absolute paths to be relative to basePath', async () => {
      const path = await getPath()
      const fs = await getFs()

      // Absolute paths like /etc/passwd have leading slashes stripped and become relative
      // This is safe because the final path is still within basePath
      await storage.write('/etc/passwd', new Uint8Array([1]))

      // Should be stored under basePath/etc/passwd, NOT /etc/passwd
      const fsPath = path.join(testDir, 'etc', 'passwd')
      const exists = await fs
        .access(fsPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)

      // Note: In miniflare's sandbox, /etc/passwd doesn't exist, so we can't verify
      // the system file wasn't modified. The important assertion is that our file
      // was created in the right place (basePath/etc/passwd), which is verified above.
    })

    it('should handle paths starting with dot', async () => {
      await storage.write('.hidden', new Uint8Array([1]))
      await storage.write('.config/settings', new Uint8Array([2]))

      expect(await storage.exists('.hidden')).toBe(true)
      expect(await storage.exists('.config/settings')).toBe(true)
    })

    it('should handle paths with consecutive dots', async () => {
      // Valid path with dots in filename
      await storage.write('file...txt', new Uint8Array([1]))
      expect(await storage.exists('file...txt')).toBe(true)
    })

    it('should reject null bytes in paths', async () => {
      await expect(storage.write('file\0.txt', new Uint8Array([1]))).rejects.toThrow()
    })

    it('should reject URL-encoded null bytes (%00)', async () => {
      await expect(storage.write('file%00.txt', new Uint8Array([1]))).rejects.toThrow(/null byte/i)
    })

    it('should prevent URL-encoded path traversal (%2e%2e)', async () => {
      // %2e = '.' so %2e%2e = '..'
      await expect(storage.write('%2e%2e/outside.txt', new Uint8Array([1]))).rejects.toThrow(/outside base directory/i)
      await expect(storage.write('%2e%2e%2f%2e%2e/outside.txt', new Uint8Array([1]))).rejects.toThrow(/outside base directory/i)
    })

    it('should prevent URL-encoded slash path traversal (%2f)', async () => {
      // %2f = '/'
      await expect(storage.write('..%2f..%2fetc/passwd', new Uint8Array([1]))).rejects.toThrow(/outside base directory/i)
    })

    it('should handle double URL-encoded paths safely', async () => {
      // %252e%252e decodes to %2e%2e (first decode)
      // When we write, it creates a file at literal path "%2e%2e/outside.txt"
      await storage.write('%252e%252e/outside.txt', new Uint8Array([1]))

      // When we try to access '%2e%2e/outside.txt', it gets decoded to '../outside.txt'
      // which should be blocked as a path traversal attempt
      await expect(storage.exists('%2e%2e/outside.txt')).rejects.toThrow(/outside base directory/i)

      // But we can access the original file using the same encoded path
      expect(await storage.exists('%252e%252e/outside.txt')).toBe(true)
    })

    it('should prevent mixed encoding path traversal', async () => {
      // Mix of encoded and literal characters
      await expect(storage.write('.%2e/outside.txt', new Uint8Array([1]))).rejects.toThrow(/outside base directory/i)
      await expect(storage.write('%2e./outside.txt', new Uint8Array([1]))).rejects.toThrow(/outside base directory/i)
    })

    it('should handle malformed URL encoding gracefully', async () => {
      // Invalid percent encoding should not cause crashes
      await storage.write('%GG/file.txt', new Uint8Array([1]))
      expect(await storage.exists('%GG/file.txt')).toBe(true)
    })

    it('should handle leading slash by treating as relative', async () => {
      const path = await getPath()
      const fs = await getFs()

      // Leading slash should be stripped or treated as relative to basePath
      await storage.write('/subdir/file.txt', new Uint8Array([1]))

      // Should be stored under basePath, not at root
      const fsPath = path.join(testDir, 'subdir', 'file.txt')
      const exists = await fs
        .access(fsPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })
  })

  // ===========================================================================
  // ERROR HANDLING TESTS
  // ===========================================================================

  describe('error handling', () => {
    it('should throw descriptive error for file not found on read', async () => {
      await expect(storage.read('missing.txt')).rejects.toThrow(/not found|ENOENT/i)
    })

    it('should provide meaningful error for stat on non-existent file', async () => {
      // stat should return null, not throw, for non-existent files
      const result = await storage.stat('nonexistent.txt')
      expect(result).toBeNull()
    })

    it('should handle concurrent writes to same file', async () => {
      const content1 = new TextEncoder().encode('Content 1')
      const content2 = new TextEncoder().encode('Content 2')

      // Both writes should succeed (last one wins)
      await Promise.all([storage.write('concurrent.txt', content1), storage.write('concurrent.txt', content2)])

      const result = await storage.read('concurrent.txt')
      // Result should be one of the two contents
      const decoded = new TextDecoder().decode(result)
      expect(['Content 1', 'Content 2']).toContain(decoded)
    })

    it('should handle very long file paths', async () => {
      // Create a path that approaches OS limits
      const longName = 'a'.repeat(200)
      const longPath = `${longName}/${longName}/file.txt`

      // This may fail due to OS path length limits, which is expected
      try {
        await storage.write(longPath, new Uint8Array([1]))
        const exists = await storage.exists(longPath)
        expect(exists).toBe(true)
      } catch (error: unknown) {
        // Path too long error is acceptable
        const message = error instanceof Error ? error.message : String(error)
        expect(message).toMatch(/path|name|ENAMETOOLONG/i)
      }
    })
  })

  // ===========================================================================
  // BASEPATH ISOLATION TESTS
  // ===========================================================================

  describe('basePath isolation', () => {
    it('should only access files within basePath', async () => {
      const path = await getPath()
      const fs = await getFs()

      await storage.write('internal.txt', new Uint8Array([1]))

      // Verify file is actually within testDir
      const fsPath = path.join(testDir, 'internal.txt')
      const exists = await fs
        .access(fsPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should create basePath directory if it does not exist', async () => {
      const path = await getPath()
      const fs = await getFs()

      const newBasePath = path.join(testDir, 'new', 'nested', 'base')
      const newStorage = new FileSystemStorage({ path: newBasePath })

      await newStorage.write('file.txt', new Uint8Array([1]))

      const fsPath = path.join(newBasePath, 'file.txt')
      const exists = await fs
        .access(fsPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should use different basePaths independently', async () => {
      const path = await getPath()

      const basePath1 = path.join(testDir, 'storage1')
      const basePath2 = path.join(testDir, 'storage2')

      const storage1 = new FileSystemStorage({ path: basePath1 })
      const storage2 = new FileSystemStorage({ path: basePath2 })

      await storage1.write('file.txt', new TextEncoder().encode('Storage 1'))
      await storage2.write('file.txt', new TextEncoder().encode('Storage 2'))

      const content1 = await storage1.read('file.txt')
      const content2 = await storage2.read('file.txt')

      expect(new TextDecoder().decode(content1)).toBe('Storage 1')
      expect(new TextDecoder().decode(content2)).toBe('Storage 2')
    })
  })
})
