/**
 * MemoryStorage Tests
 *
 * Comprehensive tests for the MemoryStorage implementation.
 * These tests verify that MemoryStorage is suitable for testing other components.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryStorage, createStorage, createAsyncBuffer } from '../../../src/storage/index'

describe('MemoryStorage', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  // ===========================================================================
  // Basic read/write/delete operations
  // ===========================================================================

  describe('basic read/write/delete operations', () => {
    it('should write and read a file', async () => {
      const data = new TextEncoder().encode('hello world')
      await storage.write('test.txt', data)
      const result = await storage.read('test.txt')
      expect(result).toEqual(data)
    })

    it('should write and read binary data', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      await storage.write('binary.bin', binaryData)
      const result = await storage.read('binary.bin')
      expect(result).toEqual(binaryData)
    })

    it('should overwrite existing file', async () => {
      const data1 = new TextEncoder().encode('first')
      const data2 = new TextEncoder().encode('second')
      await storage.write('file.txt', data1)
      await storage.write('file.txt', data2)
      const result = await storage.read('file.txt')
      expect(new TextDecoder().decode(result)).toBe('second')
    })

    it('should delete a file', async () => {
      const data = new TextEncoder().encode('to delete')
      await storage.write('delete-me.txt', data)
      await storage.delete('delete-me.txt')
      const exists = await storage.exists('delete-me.txt')
      expect(exists).toBe(false)
    })

    it('should handle deleting non-existent file gracefully', async () => {
      // Should not throw
      await expect(storage.delete('non-existent.txt')).resolves.toBeUndefined()
    })

    it('should handle empty files', async () => {
      const emptyData = new Uint8Array(0)
      await storage.write('empty.txt', emptyData)
      const result = await storage.read('empty.txt')
      expect(result.length).toBe(0)
    })

    it('should handle large files', async () => {
      const largeData = new Uint8Array(1024 * 1024) // 1MB
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }
      await storage.write('large.bin', largeData)
      const result = await storage.read('large.bin')
      expect(result.length).toBe(largeData.length)
      expect(result[0]).toBe(0)
      expect(result[255]).toBe(255)
      expect(result[256]).toBe(0)
    })
  })

  // ===========================================================================
  // Error handling for missing files
  // ===========================================================================

  describe('error handling for missing files', () => {
    it('should throw error when reading non-existent file', async () => {
      await expect(storage.read('missing.txt')).rejects.toThrow('File not found: missing.txt')
    })

    it('should throw error when reading range from non-existent file', async () => {
      await expect(storage.readRange('missing.txt', 0, 10)).rejects.toThrow(
        'File not found: missing.txt'
      )
    })

    it('should return null stat for non-existent file', async () => {
      const stat = await storage.stat('missing.txt')
      expect(stat).toBeNull()
    })

    it('should return false for exists() on non-existent file', async () => {
      const exists = await storage.exists('missing.txt')
      expect(exists).toBe(false)
    })
  })

  // ===========================================================================
  // exists() for present and missing files
  // ===========================================================================

  describe('exists()', () => {
    it('should return true for existing file', async () => {
      await storage.write('exists.txt', new Uint8Array([1, 2, 3]))
      const exists = await storage.exists('exists.txt')
      expect(exists).toBe(true)
    })

    it('should return false for non-existing file', async () => {
      const exists = await storage.exists('does-not-exist.txt')
      expect(exists).toBe(false)
    })

    it('should return false after file is deleted', async () => {
      await storage.write('temp.txt', new Uint8Array([1, 2, 3]))
      await storage.delete('temp.txt')
      const exists = await storage.exists('temp.txt')
      expect(exists).toBe(false)
    })

    it('should return true for empty files', async () => {
      await storage.write('empty.txt', new Uint8Array(0))
      const exists = await storage.exists('empty.txt')
      expect(exists).toBe(true)
    })
  })

  // ===========================================================================
  // stat() returns correct size and lastModified
  // ===========================================================================

  describe('stat()', () => {
    it('should return correct size for file', async () => {
      const data = new TextEncoder().encode('hello')
      await storage.write('sized.txt', data)
      const stat = await storage.stat('sized.txt')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(5)
    })

    it('should return size 0 for empty file', async () => {
      await storage.write('empty.txt', new Uint8Array(0))
      const stat = await storage.stat('empty.txt')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(0)
    })

    it('should return lastModified as a Date', async () => {
      const before = new Date()
      await storage.write('timed.txt', new Uint8Array([1]))
      const stat = await storage.stat('timed.txt')
      const after = new Date()

      expect(stat).not.toBeNull()
      expect(stat!.lastModified).toBeInstanceOf(Date)
      expect(stat!.lastModified.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(stat!.lastModified.getTime()).toBeLessThanOrEqual(after.getTime())
    })

    it('should update lastModified when file is overwritten', async () => {
      await storage.write('update.txt', new Uint8Array([1]))
      const stat1 = await storage.stat('update.txt')

      // Wait a bit to ensure time difference
      await new Promise(resolve => setTimeout(resolve, 10))

      await storage.write('update.txt', new Uint8Array([2]))
      const stat2 = await storage.stat('update.txt')

      expect(stat2!.lastModified.getTime()).toBeGreaterThanOrEqual(stat1!.lastModified.getTime())
    })

    it('should return null for non-existent file', async () => {
      const stat = await storage.stat('nope.txt')
      expect(stat).toBeNull()
    })

    it('should return correct size for binary data', async () => {
      const data = new Uint8Array([0x00, 0xff, 0x00, 0xff])
      await storage.write('binary.bin', data)
      const stat = await storage.stat('binary.bin')
      expect(stat!.size).toBe(4)
    })
  })

  // ===========================================================================
  // list() with various prefixes
  // ===========================================================================

  describe('list()', () => {
    beforeEach(async () => {
      await storage.write('data/users/1.json', new Uint8Array([1]))
      await storage.write('data/users/2.json', new Uint8Array([2]))
      await storage.write('data/posts/1.json', new Uint8Array([3]))
      await storage.write('config.json', new Uint8Array([4]))
      await storage.write('data-backup.json', new Uint8Array([5]))
    })

    it('should list all files with empty prefix', async () => {
      const files = await storage.list('')
      expect(files).toHaveLength(5)
      expect(files).toContain('data/users/1.json')
      expect(files).toContain('data/users/2.json')
      expect(files).toContain('data/posts/1.json')
      expect(files).toContain('config.json')
      expect(files).toContain('data-backup.json')
    })

    it('should list files with directory prefix', async () => {
      const files = await storage.list('data/')
      expect(files).toHaveLength(3)
      expect(files).toContain('data/users/1.json')
      expect(files).toContain('data/users/2.json')
      expect(files).toContain('data/posts/1.json')
    })

    it('should list files with nested directory prefix', async () => {
      const files = await storage.list('data/users/')
      expect(files).toHaveLength(2)
      expect(files).toContain('data/users/1.json')
      expect(files).toContain('data/users/2.json')
    })

    it('should list files with partial prefix', async () => {
      const files = await storage.list('data')
      expect(files).toHaveLength(4)
      expect(files).toContain('data/users/1.json')
      expect(files).toContain('data/users/2.json')
      expect(files).toContain('data/posts/1.json')
      expect(files).toContain('data-backup.json')
    })

    it('should return empty array for non-matching prefix', async () => {
      const files = await storage.list('nonexistent/')
      expect(files).toEqual([])
    })

    it('should return empty array when storage is empty', async () => {
      const emptyStorage = new MemoryStorage()
      const files = await emptyStorage.list('')
      expect(files).toEqual([])
    })

    it('should handle exact file path as prefix', async () => {
      const files = await storage.list('config.json')
      expect(files).toHaveLength(1)
      expect(files).toContain('config.json')
    })
  })

  // ===========================================================================
  // readRange() for byte slicing
  // ===========================================================================

  describe('readRange()', () => {
    it('should read a byte range from the beginning', async () => {
      const data = new TextEncoder().encode('hello world')
      await storage.write('range.txt', data)
      const slice = await storage.readRange('range.txt', 0, 5)
      expect(new TextDecoder().decode(slice)).toBe('hello')
    })

    it('should read a byte range from the middle', async () => {
      const data = new TextEncoder().encode('hello world')
      await storage.write('range.txt', data)
      const slice = await storage.readRange('range.txt', 6, 11)
      expect(new TextDecoder().decode(slice)).toBe('world')
    })

    it('should read to the end of file', async () => {
      const data = new TextEncoder().encode('abcdefghij')
      await storage.write('range.txt', data)
      const slice = await storage.readRange('range.txt', 5, 10)
      expect(new TextDecoder().decode(slice)).toBe('fghij')
    })

    it('should handle single byte read', async () => {
      const data = new Uint8Array([0, 1, 2, 3, 4])
      await storage.write('range.bin', data)
      const slice = await storage.readRange('range.bin', 2, 3)
      expect(slice.length).toBe(1)
      expect(slice[0]).toBe(2)
    })

    it('should handle reading beyond file bounds gracefully', async () => {
      const data = new Uint8Array([1, 2, 3])
      await storage.write('short.bin', data)
      const slice = await storage.readRange('short.bin', 0, 10)
      expect(slice.length).toBe(3)
    })

    it('should throw error for non-existent file', async () => {
      await expect(storage.readRange('missing.txt', 0, 10)).rejects.toThrow(
        'File not found: missing.txt'
      )
    })

    it('should handle binary data ranges', async () => {
      const data = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      await storage.write('binary.bin', data)
      const slice = await storage.readRange('binary.bin', 3, 6)
      expect(slice).toEqual(new Uint8Array([0xff, 0xfe, 0xfd]))
    })

    it('should return empty array for zero-length range', async () => {
      const data = new Uint8Array([1, 2, 3])
      await storage.write('range.bin', data)
      const slice = await storage.readRange('range.bin', 1, 1)
      expect(slice.length).toBe(0)
    })
  })

  // ===========================================================================
  // Multiple files isolation
  // ===========================================================================

  describe('multiple files isolation', () => {
    it('should store multiple files independently', async () => {
      const data1 = new TextEncoder().encode('file one')
      const data2 = new TextEncoder().encode('file two')
      const data3 = new TextEncoder().encode('file three')

      await storage.write('one.txt', data1)
      await storage.write('two.txt', data2)
      await storage.write('three.txt', data3)

      const result1 = await storage.read('one.txt')
      const result2 = await storage.read('two.txt')
      const result3 = await storage.read('three.txt')

      expect(new TextDecoder().decode(result1)).toBe('file one')
      expect(new TextDecoder().decode(result2)).toBe('file two')
      expect(new TextDecoder().decode(result3)).toBe('file three')
    })

    it('should not affect other files when deleting one', async () => {
      await storage.write('keep.txt', new Uint8Array([1]))
      await storage.write('delete.txt', new Uint8Array([2]))
      await storage.write('also-keep.txt', new Uint8Array([3]))

      await storage.delete('delete.txt')

      expect(await storage.exists('keep.txt')).toBe(true)
      expect(await storage.exists('delete.txt')).toBe(false)
      expect(await storage.exists('also-keep.txt')).toBe(true)
    })

    it('should not affect other files when overwriting one', async () => {
      await storage.write('static.txt', new TextEncoder().encode('static'))
      await storage.write('changing.txt', new TextEncoder().encode('original'))

      await storage.write('changing.txt', new TextEncoder().encode('updated'))

      const staticResult = await storage.read('static.txt')
      const changingResult = await storage.read('changing.txt')

      expect(new TextDecoder().decode(staticResult)).toBe('static')
      expect(new TextDecoder().decode(changingResult)).toBe('updated')
    })
  })

  // ===========================================================================
  // Instance isolation
  // ===========================================================================

  describe('instance isolation', () => {
    it('should have separate storage per instance', async () => {
      const storage1 = new MemoryStorage()
      const storage2 = new MemoryStorage()

      await storage1.write('file.txt', new TextEncoder().encode('storage1'))
      await storage2.write('file.txt', new TextEncoder().encode('storage2'))

      const result1 = await storage1.read('file.txt')
      const result2 = await storage2.read('file.txt')

      expect(new TextDecoder().decode(result1)).toBe('storage1')
      expect(new TextDecoder().decode(result2)).toBe('storage2')
    })

    it('should not share files between instances', async () => {
      const storage1 = new MemoryStorage()
      const storage2 = new MemoryStorage()

      await storage1.write('only-in-1.txt', new Uint8Array([1]))

      expect(await storage1.exists('only-in-1.txt')).toBe(true)
      expect(await storage2.exists('only-in-1.txt')).toBe(false)
    })
  })

  // ===========================================================================
  // Factory function integration
  // ===========================================================================

  describe('createStorage factory', () => {
    it('should create MemoryStorage with type: memory', () => {
      const storage = createStorage({ type: 'memory' })
      expect(storage).toBeInstanceOf(MemoryStorage)
    })

    it('should work correctly through factory', async () => {
      const storage = createStorage({ type: 'memory' })
      await storage.write('test.txt', new TextEncoder().encode('via factory'))
      const result = await storage.read('test.txt')
      expect(new TextDecoder().decode(result)).toBe('via factory')
    })
  })

  // ===========================================================================
  // AsyncBuffer integration
  // ===========================================================================

  describe('createAsyncBuffer integration', () => {
    it('should create AsyncBuffer from MemoryStorage', async () => {
      await storage.write('test.parquet', new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
      const buffer = await createAsyncBuffer(storage, 'test.parquet')

      expect(buffer.byteLength).toBe(10)
    })

    it('should read slices through AsyncBuffer', async () => {
      const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      await storage.write('test.parquet', data)
      const buffer = await createAsyncBuffer(storage, 'test.parquet')

      const slice = await buffer.slice(3, 7)
      // createAsyncBuffer returns ArrayBuffer for hyparquet compatibility
      expect(new Uint8Array(slice)).toEqual(new Uint8Array([3, 4, 5, 6]))
    })

    it('should throw error for non-existent file', async () => {
      await expect(createAsyncBuffer(storage, 'missing.parquet')).rejects.toThrow(
        'File not found: missing.parquet'
      )
    })
  })

  // ===========================================================================
  // Snapshot and restore functionality
  // ===========================================================================

  describe('snapshot()', () => {
    it('should capture current state', async () => {
      await storage.write('file1.txt', new TextEncoder().encode('content1'))
      await storage.write('file2.txt', new TextEncoder().encode('content2'))

      const snapshot = await storage.snapshot()

      expect(snapshot).toBeDefined()
      expect(typeof snapshot).toBe('object')
    })

    it('should return snapshot that can be restored', async () => {
      await storage.write('original.txt', new TextEncoder().encode('original'))
      const snapshot = await storage.snapshot()

      await storage.write('original.txt', new TextEncoder().encode('modified'))
      await storage.restore(snapshot)

      const result = await storage.read('original.txt')
      expect(new TextDecoder().decode(result)).toBe('original')
    })
  })

  describe('restore()', () => {
    it('should restore to previous snapshot state', async () => {
      await storage.write('keep.txt', new TextEncoder().encode('keep'))
      const snapshot = await storage.snapshot()

      await storage.write('new.txt', new TextEncoder().encode('new'))
      await storage.delete('keep.txt')

      await storage.restore(snapshot)

      expect(await storage.exists('keep.txt')).toBe(true)
      expect(await storage.exists('new.txt')).toBe(false)
    })

    it('should restore file contents exactly', async () => {
      const originalData = new Uint8Array([1, 2, 3, 4, 5])
      await storage.write('data.bin', originalData)
      const snapshot = await storage.snapshot()

      await storage.write('data.bin', new Uint8Array([9, 8, 7]))
      await storage.restore(snapshot)

      const result = await storage.read('data.bin')
      expect(result).toEqual(originalData)
    })
  })

  // ===========================================================================
  // Clear functionality
  // ===========================================================================

  describe('clear()', () => {
    it('should remove all files', async () => {
      await storage.write('file1.txt', new Uint8Array([1]))
      await storage.write('file2.txt', new Uint8Array([2]))
      await storage.write('dir/file3.txt', new Uint8Array([3]))

      await storage.clear()

      const files = await storage.list('')
      expect(files).toEqual([])
    })

    it('should allow new writes after clearing', async () => {
      await storage.write('old.txt', new Uint8Array([1]))
      await storage.clear()
      await storage.write('new.txt', new Uint8Array([2]))

      expect(await storage.exists('old.txt')).toBe(false)
      expect(await storage.exists('new.txt')).toBe(true)
    })
  })

  // ===========================================================================
  // Operation history tracking
  // ===========================================================================

  describe('getOperationHistory()', () => {
    it('should track write operations', async () => {
      await storage.write('test.txt', new Uint8Array([1]))

      const history = storage.getOperationHistory()

      expect(history).toContainEqual(
        expect.objectContaining({
          operation: 'write',
          path: 'test.txt',
        })
      )
    })

    it('should track read operations', async () => {
      await storage.write('test.txt', new Uint8Array([1]))
      await storage.read('test.txt')

      const history = storage.getOperationHistory()

      expect(history).toContainEqual(
        expect.objectContaining({
          operation: 'read',
          path: 'test.txt',
        })
      )
    })

    it('should track delete operations', async () => {
      await storage.write('test.txt', new Uint8Array([1]))
      await storage.delete('test.txt')

      const history = storage.getOperationHistory()

      expect(history).toContainEqual(
        expect.objectContaining({
          operation: 'delete',
          path: 'test.txt',
        })
      )
    })

    it('should track list operations', async () => {
      await storage.list('prefix/')

      const history = storage.getOperationHistory()

      expect(history).toContainEqual(
        expect.objectContaining({
          operation: 'list',
          path: 'prefix/',
        })
      )
    })

    it('should track operations in order', async () => {
      await storage.write('a.txt', new Uint8Array([1]))
      await storage.read('a.txt')
      await storage.delete('a.txt')

      const history = storage.getOperationHistory()
      const paths = history.map(h => h.operation)

      expect(paths.indexOf('write')).toBeLessThan(paths.indexOf('read'))
      expect(paths.indexOf('read')).toBeLessThan(paths.indexOf('delete'))
    })

    it('should include timestamps', async () => {
      const before = Date.now()
      await storage.write('test.txt', new Uint8Array([1]))
      const after = Date.now()

      const history = storage.getOperationHistory()
      const writeOp = history.find(h => h.operation === 'write')

      expect(writeOp).toBeDefined()
      expect(writeOp!.timestamp).toBeGreaterThanOrEqual(before)
      expect(writeOp!.timestamp).toBeLessThanOrEqual(after)
    })
  })

  // ===========================================================================
  // Latency simulation
  // ===========================================================================

  describe('latency simulation', () => {
    it('should support configurable read latency', async () => {
      const slowStorage = new MemoryStorage({ latency: { read: 50 } })
      await slowStorage.write('test.txt', new Uint8Array([1]))

      const start = Date.now()
      await slowStorage.read('test.txt')
      const elapsed = Date.now() - start

      expect(elapsed).toBeGreaterThanOrEqual(45) // Allow small timing variance
    })

    it('should support configurable write latency', async () => {
      const slowStorage = new MemoryStorage({ latency: { write: 50 } })

      const start = Date.now()
      await slowStorage.write('test.txt', new Uint8Array([1]))
      const elapsed = Date.now() - start

      expect(elapsed).toBeGreaterThanOrEqual(45)
    })

    it('should apply latency to all operations', async () => {
      const slowStorage = new MemoryStorage({
        latency: { read: 20, write: 20, delete: 20, list: 20 },
      })

      await slowStorage.write('test.txt', new Uint8Array([1]))

      const start = Date.now()
      await slowStorage.read('test.txt')
      await slowStorage.list('')
      await slowStorage.delete('test.txt')
      const elapsed = Date.now() - start

      expect(elapsed).toBeGreaterThanOrEqual(55) // 3 operations * 20ms - some variance
    })
  })

  // ===========================================================================
  // Size limits
  // ===========================================================================

  describe('size limits', () => {
    it('should enforce maximum storage size', async () => {
      const limitedStorage = new MemoryStorage({ maxSize: 100 })

      await expect(limitedStorage.write('large.bin', new Uint8Array(101))).rejects.toThrow(
        /size limit|storage full|exceeded/i
      )
    })

    it('should track cumulative size across files', async () => {
      const limitedStorage = new MemoryStorage({ maxSize: 100 })

      await limitedStorage.write('file1.bin', new Uint8Array(50))
      await limitedStorage.write('file2.bin', new Uint8Array(40))

      await expect(limitedStorage.write('file3.bin', new Uint8Array(20))).rejects.toThrow(
        /size limit|storage full|exceeded/i
      )
    })

    it('should reclaim space when files are deleted', async () => {
      const limitedStorage = new MemoryStorage({ maxSize: 100 })

      await limitedStorage.write('file1.bin', new Uint8Array(60))
      await limitedStorage.delete('file1.bin')
      await limitedStorage.write('file2.bin', new Uint8Array(80))

      expect(await limitedStorage.exists('file2.bin')).toBe(true)
    })

    it('should reclaim space when files are overwritten', async () => {
      const limitedStorage = new MemoryStorage({ maxSize: 100 })

      await limitedStorage.write('file.bin', new Uint8Array(60))
      await limitedStorage.write('file.bin', new Uint8Array(30)) // Smaller replacement

      await limitedStorage.write('other.bin', new Uint8Array(60))
      expect(await limitedStorage.exists('other.bin')).toBe(true)
    })

    it('should report current usage', async () => {
      const limitedStorage = new MemoryStorage({ maxSize: 1000 })

      await limitedStorage.write('file1.bin', new Uint8Array(100))
      await limitedStorage.write('file2.bin', new Uint8Array(200))

      expect(limitedStorage.getUsedSize()).toBe(300)
      expect(limitedStorage.getMaxSize()).toBe(1000)
      expect(limitedStorage.getAvailableSize()).toBe(700)
    })
  })

  // ===========================================================================
  // Concurrent operations
  // ===========================================================================

  describe('concurrent operations', () => {
    it('should handle concurrent writes to different files', async () => {
      const writes = Promise.all([
        storage.write('file1.txt', new TextEncoder().encode('content1')),
        storage.write('file2.txt', new TextEncoder().encode('content2')),
        storage.write('file3.txt', new TextEncoder().encode('content3')),
      ])

      await expect(writes).resolves.not.toThrow()

      const [r1, r2, r3] = await Promise.all([
        storage.read('file1.txt'),
        storage.read('file2.txt'),
        storage.read('file3.txt'),
      ])

      expect(new TextDecoder().decode(r1)).toBe('content1')
      expect(new TextDecoder().decode(r2)).toBe('content2')
      expect(new TextDecoder().decode(r3)).toBe('content3')
    })

    it('should handle concurrent reads', async () => {
      await storage.write('shared.txt', new TextEncoder().encode('shared content'))

      const reads = await Promise.all([
        storage.read('shared.txt'),
        storage.read('shared.txt'),
        storage.read('shared.txt'),
      ])

      reads.forEach(result => {
        expect(new TextDecoder().decode(result)).toBe('shared content')
      })
    })

    it('should handle mixed concurrent operations', async () => {
      await storage.write('existing.txt', new TextEncoder().encode('existing'))

      const operations = Promise.all([
        storage.write('new1.txt', new TextEncoder().encode('new1')),
        storage.read('existing.txt'),
        storage.exists('existing.txt'),
        storage.list(''),
        storage.stat('existing.txt'),
      ])

      await expect(operations).resolves.not.toThrow()
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle paths with special characters', async () => {
      const paths = ['file with spaces.txt', 'file-with-dashes.txt', 'file_with_underscores.txt']

      for (const path of paths) {
        await storage.write(path, new Uint8Array([1]))
        expect(await storage.exists(path)).toBe(true)
      }
    })

    it('should handle deeply nested paths', async () => {
      const deepPath = 'a/b/c/d/e/f/g/h/i/j/file.txt'
      await storage.write(deepPath, new Uint8Array([1]))
      expect(await storage.exists(deepPath)).toBe(true)

      const listed = await storage.list('a/b/c/')
      expect(listed).toContain(deepPath)
    })

    it('should handle paths starting with slash', async () => {
      await storage.write('/absolute/path.txt', new Uint8Array([1]))
      expect(await storage.exists('/absolute/path.txt')).toBe(true)
    })

    it('should handle unicode in paths', async () => {
      const unicodePath = 'files/\u4e2d\u6587/\u0442\u0435\u0441\u0442.txt'
      await storage.write(unicodePath, new Uint8Array([1]))
      expect(await storage.exists(unicodePath)).toBe(true)
    })

    it('should preserve binary data integrity', async () => {
      // Create a Uint8Array with all possible byte values
      const allBytes = new Uint8Array(256)
      for (let i = 0; i < 256; i++) {
        allBytes[i] = i
      }

      await storage.write('all-bytes.bin', allBytes)
      const result = await storage.read('all-bytes.bin')

      expect(result).toEqual(allBytes)
    })
  })
})
