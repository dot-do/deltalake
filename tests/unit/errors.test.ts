/**
 * Error Type Guards Tests
 *
 * Tests for all error type guard functions in errors.ts.
 * Covers positive cases (correct error types) and negative cases
 * (wrong types, plain objects, null/undefined).
 */

import { describe, it, expect } from 'vitest'
import {
  // Error classes
  DeltaLakeError,
  StorageError,
  FileNotFoundError,
  VersionMismatchError,
  S3Error,
  ConcurrencyError,
  CDCError,
  ValidationError,
  // Type guards
  isDeltaLakeError,
  isStorageError,
  isConcurrencyError,
  isValidationError,
  isCDCError,
  isRetryableError,
  isFileNotFoundError,
  isVersionMismatchError,
} from '../../src/errors'

// =============================================================================
// isDeltaLakeError
// =============================================================================

describe('isDeltaLakeError', () => {
  describe('positive cases', () => {
    it('should return true for DeltaLakeError', () => {
      const error = new DeltaLakeError('test message', 'TEST_CODE')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for StorageError (subclass)', () => {
      const error = new StorageError('storage error', '/path', 'read')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for FileNotFoundError (subclass)', () => {
      const error = new FileNotFoundError('/missing/path')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for VersionMismatchError (subclass)', () => {
      const error = new VersionMismatchError('/path', 'v1', 'v2')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for S3Error (subclass)', () => {
      const error = new S3Error('s3 error', 'NoSuchKey', 404, '/path')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for ConcurrencyError (subclass)', () => {
      const error = new ConcurrencyError('concurrent write')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for CDCError (subclass)', () => {
      const error = new CDCError('cdc error', 'TABLE_NOT_FOUND')
      expect(isDeltaLakeError(error)).toBe(true)
    })

    it('should return true for ValidationError (subclass)', () => {
      const error = new ValidationError('validation failed', 'field')
      expect(isDeltaLakeError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for standard Error', () => {
      const error = new Error('standard error')
      expect(isDeltaLakeError(error)).toBe(false)
    })

    it('should return false for TypeError', () => {
      const error = new TypeError('type error')
      expect(isDeltaLakeError(error)).toBe(false)
    })

    it('should return false for plain object with similar shape', () => {
      const obj = { message: 'test', code: 'TEST_CODE', name: 'DeltaLakeError' }
      expect(isDeltaLakeError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isDeltaLakeError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isDeltaLakeError(undefined)).toBe(false)
    })

    it('should return false for string', () => {
      expect(isDeltaLakeError('error string')).toBe(false)
    })

    it('should return false for number', () => {
      expect(isDeltaLakeError(42)).toBe(false)
    })

    it('should return false for empty object', () => {
      expect(isDeltaLakeError({})).toBe(false)
    })
  })
})

// =============================================================================
// isStorageError
// =============================================================================

describe('isStorageError', () => {
  describe('positive cases', () => {
    it('should return true for StorageError', () => {
      const error = new StorageError('storage error', '/path', 'read')
      expect(isStorageError(error)).toBe(true)
    })

    it('should return true for FileNotFoundError (subclass)', () => {
      const error = new FileNotFoundError('/missing/path')
      expect(isStorageError(error)).toBe(true)
    })

    it('should return true for VersionMismatchError (subclass)', () => {
      const error = new VersionMismatchError('/path', 'v1', 'v2')
      expect(isStorageError(error)).toBe(true)
    })

    it('should return true for S3Error (subclass)', () => {
      const error = new S3Error('s3 error', 'NoSuchKey', 404, '/path')
      expect(isStorageError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isStorageError(error)).toBe(false)
    })

    it('should return false for ConcurrencyError', () => {
      const error = new ConcurrencyError('concurrent')
      expect(isStorageError(error)).toBe(false)
    })

    it('should return false for CDCError', () => {
      const error = new CDCError('cdc error', 'TABLE_NOT_FOUND')
      expect(isStorageError(error)).toBe(false)
    })

    it('should return false for ValidationError', () => {
      const error = new ValidationError('validation')
      expect(isStorageError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('standard')
      expect(isStorageError(error)).toBe(false)
    })

    it('should return false for plain object with storage error shape', () => {
      const obj = { message: 'test', path: '/path', operation: 'read', name: 'StorageError' }
      expect(isStorageError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isStorageError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isStorageError(undefined)).toBe(false)
    })
  })
})

// =============================================================================
// isConcurrencyError
// =============================================================================

describe('isConcurrencyError', () => {
  describe('positive cases', () => {
    it('should return true for ConcurrencyError with message', () => {
      const error = new ConcurrencyError('concurrent write detected')
      expect(isConcurrencyError(error)).toBe(true)
    })

    it('should return true for ConcurrencyError with options', () => {
      const error = new ConcurrencyError({ expectedVersion: 1, actualVersion: 2 })
      expect(isConcurrencyError(error)).toBe(true)
    })

    it('should return true for ConcurrencyError with message and version info', () => {
      const error = new ConcurrencyError('conflict', { expectedVersion: 5, actualVersion: 6 })
      expect(isConcurrencyError(error)).toBe(true)
    })

    it('should return true for Error with name ConcurrencyError', () => {
      const error = new Error('custom concurrent error')
      error.name = 'ConcurrencyError'
      expect(isConcurrencyError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isConcurrencyError(error)).toBe(false)
    })

    it('should return false for StorageError', () => {
      const error = new StorageError('storage', '/path', 'read')
      expect(isConcurrencyError(error)).toBe(false)
    })

    it('should return false for CDCError', () => {
      const error = new CDCError('cdc', 'TABLE_NOT_FOUND')
      expect(isConcurrencyError(error)).toBe(false)
    })

    it('should return false for ValidationError', () => {
      const error = new ValidationError('validation')
      expect(isConcurrencyError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('standard')
      expect(isConcurrencyError(error)).toBe(false)
    })

    it('should return false for plain object', () => {
      const obj = { message: 'concurrent', name: 'ConcurrencyError', retryable: true }
      expect(isConcurrencyError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isConcurrencyError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isConcurrencyError(undefined)).toBe(false)
    })
  })
})

// =============================================================================
// isValidationError
// =============================================================================

describe('isValidationError', () => {
  describe('positive cases', () => {
    it('should return true for ValidationError with message only', () => {
      const error = new ValidationError('invalid input')
      expect(isValidationError(error)).toBe(true)
    })

    it('should return true for ValidationError with field', () => {
      const error = new ValidationError('field is required', 'username')
      expect(isValidationError(error)).toBe(true)
    })

    it('should return true for ValidationError with field and value', () => {
      const error = new ValidationError('invalid value', 'age', -5)
      expect(isValidationError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isValidationError(error)).toBe(false)
    })

    it('should return false for StorageError', () => {
      const error = new StorageError('storage', '/path', 'read')
      expect(isValidationError(error)).toBe(false)
    })

    it('should return false for ConcurrencyError', () => {
      const error = new ConcurrencyError('concurrent')
      expect(isValidationError(error)).toBe(false)
    })

    it('should return false for CDCError', () => {
      const error = new CDCError('cdc', 'TABLE_NOT_FOUND')
      expect(isValidationError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('standard')
      expect(isValidationError(error)).toBe(false)
    })

    it('should return false for plain object', () => {
      const obj = { message: 'validation', field: 'test', name: 'ValidationError' }
      expect(isValidationError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isValidationError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isValidationError(undefined)).toBe(false)
    })
  })
})

// =============================================================================
// isCDCError
// =============================================================================

describe('isCDCError', () => {
  describe('positive cases', () => {
    it('should return true for CDCError with TABLE_NOT_FOUND', () => {
      const error = new CDCError('table not found', 'TABLE_NOT_FOUND')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with INVALID_VERSION_RANGE', () => {
      const error = new CDCError('invalid range', 'INVALID_VERSION_RANGE')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with INVALID_TIME_RANGE', () => {
      const error = new CDCError('invalid time', 'INVALID_TIME_RANGE')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with CDC_NOT_ENABLED', () => {
      const error = new CDCError('cdc not enabled', 'CDC_NOT_ENABLED')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with STORAGE_ERROR', () => {
      const error = new CDCError('storage failed', 'STORAGE_ERROR')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with PARSE_ERROR', () => {
      const error = new CDCError('parse failed', 'PARSE_ERROR')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with EMPTY_WRITE', () => {
      const error = new CDCError('empty write', 'EMPTY_WRITE')
      expect(isCDCError(error)).toBe(true)
    })

    it('should return true for CDCError with cause', () => {
      const cause = new Error('underlying cause')
      const error = new CDCError('cdc error', 'STORAGE_ERROR', cause)
      expect(isCDCError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isCDCError(error)).toBe(false)
    })

    it('should return false for StorageError', () => {
      const error = new StorageError('storage', '/path', 'read')
      expect(isCDCError(error)).toBe(false)
    })

    it('should return false for ConcurrencyError', () => {
      const error = new ConcurrencyError('concurrent')
      expect(isCDCError(error)).toBe(false)
    })

    it('should return false for ValidationError', () => {
      const error = new ValidationError('validation')
      expect(isCDCError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('standard')
      expect(isCDCError(error)).toBe(false)
    })

    it('should return false for plain object', () => {
      const obj = { message: 'cdc', cdcCode: 'TABLE_NOT_FOUND', name: 'CDCError' }
      expect(isCDCError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isCDCError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isCDCError(undefined)).toBe(false)
    })
  })
})

// =============================================================================
// isRetryableError
// =============================================================================

describe('isRetryableError', () => {
  describe('positive cases', () => {
    it('should return true for ConcurrencyError', () => {
      const error = new ConcurrencyError('concurrent write')
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for ConcurrencyError with version info', () => {
      const error = new ConcurrencyError({ expectedVersion: 1, actualVersion: 2 })
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for Error with retryable: true', () => {
      const error = new Error('transient error') as Error & { retryable: boolean }
      error.retryable = true
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for Error with name ConcurrencyError', () => {
      const error = new Error('custom concurrent')
      error.name = 'ConcurrencyError'
      expect(isRetryableError(error)).toBe(true)
    })

    it('should return true for custom error with retryable property', () => {
      class CustomError extends Error {
        retryable = true
      }
      const error = new CustomError('custom retryable')
      expect(isRetryableError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for StorageError', () => {
      const error = new StorageError('storage', '/path', 'read')
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for FileNotFoundError', () => {
      const error = new FileNotFoundError('/path')
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for ValidationError', () => {
      const error = new ValidationError('validation')
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for CDCError', () => {
      const error = new CDCError('cdc', 'TABLE_NOT_FOUND')
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('standard')
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for Error with retryable: false', () => {
      const error = new Error('non-retryable') as Error & { retryable: boolean }
      error.retryable = false
      expect(isRetryableError(error)).toBe(false)
    })

    it('should return false for plain object with retryable: true', () => {
      const obj = { message: 'test', retryable: true }
      expect(isRetryableError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isRetryableError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isRetryableError(undefined)).toBe(false)
    })

    it('should return false for string', () => {
      expect(isRetryableError('error')).toBe(false)
    })

    it('should return false for number', () => {
      expect(isRetryableError(42)).toBe(false)
    })

    it('should return false for empty object', () => {
      expect(isRetryableError({})).toBe(false)
    })
  })
})

// =============================================================================
// isFileNotFoundError
// =============================================================================

describe('isFileNotFoundError', () => {
  describe('positive cases', () => {
    it('should return true for FileNotFoundError', () => {
      const error = new FileNotFoundError('/missing/file.txt')
      expect(isFileNotFoundError(error)).toBe(true)
    })

    it('should return true for FileNotFoundError with custom operation', () => {
      const error = new FileNotFoundError('/path', 'stat')
      expect(isFileNotFoundError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for StorageError', () => {
      const error = new StorageError('storage', '/path', 'read')
      expect(isFileNotFoundError(error)).toBe(false)
    })

    it('should return false for VersionMismatchError', () => {
      const error = new VersionMismatchError('/path', 'v1', 'v2')
      expect(isFileNotFoundError(error)).toBe(false)
    })

    it('should return false for S3Error', () => {
      const error = new S3Error('not found', 'NoSuchKey', 404, '/path')
      expect(isFileNotFoundError(error)).toBe(false)
    })

    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isFileNotFoundError(error)).toBe(false)
    })

    it('should return false for ConcurrencyError', () => {
      const error = new ConcurrencyError('concurrent')
      expect(isFileNotFoundError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('file not found')
      expect(isFileNotFoundError(error)).toBe(false)
    })

    it('should return false for plain object', () => {
      const obj = { message: 'File not found', path: '/path', name: 'FileNotFoundError' }
      expect(isFileNotFoundError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isFileNotFoundError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isFileNotFoundError(undefined)).toBe(false)
    })
  })
})

// =============================================================================
// isVersionMismatchError
// =============================================================================

describe('isVersionMismatchError', () => {
  describe('positive cases', () => {
    it('should return true for VersionMismatchError', () => {
      const error = new VersionMismatchError('/path', 'v1', 'v2')
      expect(isVersionMismatchError(error)).toBe(true)
    })

    it('should return true for VersionMismatchError with null expected', () => {
      const error = new VersionMismatchError('/path', null, 'v2')
      expect(isVersionMismatchError(error)).toBe(true)
    })

    it('should return true for VersionMismatchError with null actual', () => {
      const error = new VersionMismatchError('/path', 'v1', null)
      expect(isVersionMismatchError(error)).toBe(true)
    })

    it('should return true for VersionMismatchError with both null', () => {
      const error = new VersionMismatchError('/path', null, null)
      expect(isVersionMismatchError(error)).toBe(true)
    })
  })

  describe('negative cases', () => {
    it('should return false for StorageError', () => {
      const error = new StorageError('storage', '/path', 'read')
      expect(isVersionMismatchError(error)).toBe(false)
    })

    it('should return false for FileNotFoundError', () => {
      const error = new FileNotFoundError('/path')
      expect(isVersionMismatchError(error)).toBe(false)
    })

    it('should return false for S3Error', () => {
      const error = new S3Error('s3 error', 'PreconditionFailed', 412, '/path')
      expect(isVersionMismatchError(error)).toBe(false)
    })

    it('should return false for DeltaLakeError', () => {
      const error = new DeltaLakeError('test', 'TEST')
      expect(isVersionMismatchError(error)).toBe(false)
    })

    it('should return false for ConcurrencyError', () => {
      const error = new ConcurrencyError({ expectedVersion: 1, actualVersion: 2 })
      expect(isVersionMismatchError(error)).toBe(false)
    })

    it('should return false for standard Error', () => {
      const error = new Error('version mismatch')
      expect(isVersionMismatchError(error)).toBe(false)
    })

    it('should return false for plain object', () => {
      const obj = {
        message: 'Version mismatch',
        expectedVersion: 'v1',
        actualVersion: 'v2',
        name: 'VersionMismatchError',
      }
      expect(isVersionMismatchError(obj)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isVersionMismatchError(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isVersionMismatchError(undefined)).toBe(false)
    })
  })
})

// =============================================================================
// Error Properties Verification
// =============================================================================

describe('Error properties verification', () => {
  describe('DeltaLakeError', () => {
    it('should have correct name and code', () => {
      const error = new DeltaLakeError('test message', 'TEST_CODE')
      expect(error.name).toBe('DeltaLakeError')
      expect(error.code).toBe('TEST_CODE')
      expect(error.message).toBe('test message')
    })

    it('should preserve cause', () => {
      const cause = new Error('original error')
      const error = new DeltaLakeError('wrapped', 'WRAPPED', cause)
      expect(error.cause).toBe(cause)
    })
  })

  describe('StorageError', () => {
    it('should have correct properties', () => {
      const error = new StorageError('test', '/test/path', 'write')
      expect(error.name).toBe('StorageError')
      expect(error.path).toBe('/test/path')
      expect(error.operation).toBe('write')
      expect(error.code).toBe('STORAGE_ERROR')
    })
  })

  describe('FileNotFoundError', () => {
    it('should have correct properties', () => {
      const error = new FileNotFoundError('/missing/file')
      expect(error.name).toBe('FileNotFoundError')
      expect(error.path).toBe('/missing/file')
      expect(error.operation).toBe('read')
      expect(error.code).toBe('FILE_NOT_FOUND')
      expect(error.message).toBe('File not found: /missing/file')
    })
  })

  describe('VersionMismatchError', () => {
    it('should have correct properties', () => {
      const error = new VersionMismatchError('/path', 'expected-v', 'actual-v')
      expect(error.name).toBe('VersionMismatchError')
      expect(error.path).toBe('/path')
      expect(error.operation).toBe('writeConditional')
      expect(error.code).toBe('VERSION_MISMATCH')
      expect(error.expectedVersion).toBe('expected-v')
      expect(error.actualVersion).toBe('actual-v')
    })
  })

  describe('S3Error', () => {
    it('should have correct properties', () => {
      const error = new S3Error('no such key', 'NoSuchKey', 404, '/bucket/key')
      expect(error.name).toBe('S3Error')
      expect(error.s3Code).toBe('NoSuchKey')
      expect(error.statusCode).toBe(404)
      expect(error.path).toBe('/bucket/key')
      expect(error.code).toBe('S3_NOSUCHKEY')
    })
  })

  describe('ConcurrencyError', () => {
    it('should have retryable set to true', () => {
      const error = new ConcurrencyError('conflict')
      expect(error.name).toBe('ConcurrencyError')
      expect(error.retryable).toBe(true)
      expect(error.code).toBe('CONCURRENCY_ERROR')
    })

    it('should have version info when provided', () => {
      const error = new ConcurrencyError({ expectedVersion: 5, actualVersion: 7 })
      expect(error.expectedVersion).toBe(5)
      expect(error.actualVersion).toBe(7)
    })
  })

  describe('CDCError', () => {
    it('should have correct properties', () => {
      const error = new CDCError('table not found', 'TABLE_NOT_FOUND')
      expect(error.name).toBe('CDCError')
      expect(error.cdcCode).toBe('TABLE_NOT_FOUND')
      expect(error.code).toBe('CDC_TABLE_NOT_FOUND')
    })
  })

  describe('ValidationError', () => {
    it('should have correct properties', () => {
      const error = new ValidationError('invalid value', 'fieldName', 'badValue')
      expect(error.name).toBe('ValidationError')
      expect(error.field).toBe('fieldName')
      expect(error.value).toBe('badValue')
      expect(error.code).toBe('VALIDATION_ERROR')
    })

    it('should handle optional field', () => {
      const error = new ValidationError('invalid')
      expect(error.field).toBeUndefined()
      expect(error.value).toBeUndefined()
    })
  })
})
