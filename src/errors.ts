/**
 * DeltaLake Error Hierarchy
 *
 * Consistent error handling patterns for the entire DeltaLake library.
 * All errors extend from DeltaLakeError for unified error handling.
 *
 * ## Error Hierarchy
 *
 * - DeltaLakeError (base)
 *   - StorageError
 *     - FileNotFoundError
 *     - VersionMismatchError
 *     - S3Error
 *   - ConcurrencyError
 *   - CDCError
 *   - ValidationError
 *
 * @example
 * ```typescript
 * try {
 *   await table.write(rows)
 * } catch (error) {
 *   if (error instanceof DeltaLakeError) {
 *     console.log(`DeltaLake error [${error.code}]: ${error.message}`)
 *   }
 *   if (error instanceof ConcurrencyError) {
 *     // Handle concurrent write conflict
 *   }
 *   if (error instanceof StorageError) {
 *     // Handle storage-specific error
 *   }
 * }
 * ```
 */

// =============================================================================
// BASE ERROR
// =============================================================================

/**
 * Base error class for all DeltaLake errors.
 * Provides a consistent `code` property for programmatic error handling.
 *
 * @public
 */
export class DeltaLakeError extends Error {
  /**
   * Error code for programmatic handling.
   * Each error subclass defines its own set of codes.
   */
  readonly code: string

  /**
   * Optional underlying cause of the error.
   * Useful for wrapping lower-level errors.
   */
  readonly cause?: unknown

  constructor(message: string, code: string, cause?: unknown) {
    super(message)
    this.name = 'DeltaLakeError'
    this.code = code
    this.cause = cause

    // Maintain proper stack trace in V8 environments
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor)
    }
  }
}

// =============================================================================
// STORAGE ERRORS
// =============================================================================

/**
 * Error thrown when storage operations fail.
 * Base class for all storage-related errors.
 *
 * @public
 */
export class StorageError extends DeltaLakeError {
  /** The storage path involved in the error */
  readonly path: string

  /** The operation that failed (e.g., 'read', 'write', 'delete') */
  readonly operation: string

  constructor(message: string, path: string, operation: string, cause?: unknown, code: string = 'STORAGE_ERROR') {
    super(message, code, cause)
    this.name = 'StorageError'
    this.path = path
    this.operation = operation
  }
}

/**
 * Error thrown when a file is not found.
 *
 * @public
 */
export class FileNotFoundError extends StorageError {
  constructor(path: string, operation: string = 'read') {
    super(`File not found: ${path}`, path, operation, undefined, 'FILE_NOT_FOUND')
    this.name = 'FileNotFoundError'
  }
}

/**
 * Error thrown when a conditional write fails due to version mismatch.
 * This indicates another writer has modified the file since we read it.
 *
 * @public
 *
 * @example
 * ```typescript
 * try {
 *   await storage.writeConditional('file.json', data, staleVersion)
 * } catch (error) {
 *   if (error instanceof VersionMismatchError) {
 *     // Retry with current version
 *     const currentVersion = error.actualVersion
 *     await storage.writeConditional('file.json', data, currentVersion)
 *   }
 * }
 * ```
 */
export class VersionMismatchError extends StorageError {
  /** The version we expected */
  readonly expectedVersion: string | null

  /** The actual version found */
  readonly actualVersion: string | null

  constructor(path: string, expectedVersion: string | null, actualVersion: string | null) {
    super(
      `Version mismatch for ${path}: expected ${expectedVersion ?? 'null (create)'}, got ${actualVersion ?? 'null (not found)'}`,
      path,
      'writeConditional',
      undefined,
      'VERSION_MISMATCH'
    )
    this.name = 'VersionMismatchError'
    this.expectedVersion = expectedVersion
    this.actualVersion = actualVersion
  }
}

/**
 * S3-specific error class for wrapping AWS S3 errors with meaningful messages.
 *
 * Common error codes:
 * - `NoSuchKey`: File not found
 * - `NoSuchBucket`: Bucket does not exist
 * - `AccessDenied`: Permission denied
 *
 * @public
 */
export class S3Error extends StorageError {
  /** S3 error code (e.g., 'NoSuchKey', 'AccessDenied') */
  readonly s3Code: string

  /** HTTP status code from S3 */
  readonly statusCode: number

  constructor(message: string, s3Code: string, statusCode: number = 0, path: string = '') {
    super(message, path, 's3', undefined, `S3_${s3Code.toUpperCase()}`)
    this.name = 'S3Error'
    this.s3Code = s3Code
    this.statusCode = statusCode
  }
}

// =============================================================================
// CONCURRENCY ERRORS
// =============================================================================

/**
 * Options for creating a ConcurrencyError with version information.
 *
 * @public
 */
export interface ConcurrencyErrorOptions {
  /** The expected version (what the writer thought the version was) */
  expectedVersion?: number
  /** The actual version found (current version in storage) */
  actualVersion?: number
}

/**
 * Error thrown when a concurrent write conflict is detected.
 *
 * This error is retryable - the operation should be attempted again after
 * refreshing the table state.
 *
 * @public
 *
 * @example
 * ```typescript
 * try {
 *   await table.write(rows)
 * } catch (error) {
 *   if (error instanceof ConcurrencyError) {
 *     await table.refresh()
 *     await table.write(rows)
 *   }
 * }
 * ```
 */
export class ConcurrencyError extends DeltaLakeError {
  /** The expected version (what the writer thought the version was) */
  readonly expectedVersion?: number | undefined

  /** The actual version found (current version in storage) */
  readonly actualVersion?: number | undefined

  /** Indicates this error is retryable */
  readonly retryable: boolean

  constructor(options: ConcurrencyErrorOptions | string, versionInfo?: ConcurrencyErrorOptions) {
    // Determine message and version info before calling super()
    let message: string
    let expectedVer: number | undefined
    let actualVer: number | undefined

    if (typeof options === 'string') {
      message = options
      if (versionInfo) {
        expectedVer = versionInfo.expectedVersion
        actualVer = versionInfo.actualVersion
      }
    } else {
      message =
        `Concurrent modification detected: expected version ${options.expectedVersion}, ` +
        `but found version ${options.actualVersion}. Please refresh and retry.`
      expectedVer = options.expectedVersion
      actualVer = options.actualVersion
    }

    super(message, 'CONCURRENCY_ERROR')
    this.name = 'ConcurrencyError'
    this.expectedVersion = expectedVer
    this.actualVersion = actualVer
    this.retryable = true
  }
}

// =============================================================================
// CDC ERRORS
// =============================================================================

/**
 * CDC error codes for categorized error handling.
 *
 * @public
 */
export type CDCErrorCode =
  | 'INVALID_VERSION_RANGE'
  | 'INVALID_TIME_RANGE'
  | 'TABLE_NOT_FOUND'
  | 'CDC_NOT_ENABLED'
  | 'STORAGE_ERROR'
  | 'PARSE_ERROR'
  | 'EMPTY_WRITE'

/**
 * Error thrown when CDC (Change Data Capture) operations fail.
 *
 * @public
 *
 * @example
 * ```typescript
 * try {
 *   const changes = await cdcReader.readByVersion(0n, 10n)
 * } catch (error) {
 *   if (error instanceof CDCError) {
 *     switch (error.cdcCode) {
 *       case 'TABLE_NOT_FOUND':
 *         console.log('Table does not exist')
 *         break
 *       case 'INVALID_VERSION_RANGE':
 *         console.log('Invalid version range specified')
 *         break
 *     }
 *   }
 * }
 * ```
 */
export class CDCError extends DeltaLakeError {
  /** CDC-specific error code */
  readonly cdcCode: CDCErrorCode

  constructor(message: string, cdcCode: CDCErrorCode, cause?: unknown) {
    super(message, `CDC_${cdcCode}`, cause)
    this.name = 'CDCError'
    this.cdcCode = cdcCode
  }
}

// =============================================================================
// VALIDATION ERRORS
// =============================================================================

/**
 * Error thrown when input validation fails.
 *
 * This includes schema validation, query filter validation,
 * and other input validation errors.
 *
 * @public
 *
 * @example
 * ```typescript
 * try {
 *   await table.write(invalidData)
 * } catch (error) {
 *   if (error instanceof ValidationError) {
 *     console.log(`Validation failed: ${error.message}`)
 *     console.log(`Field: ${error.field}`)
 *   }
 * }
 * ```
 */
export class ValidationError extends DeltaLakeError {
  /** The field or parameter that failed validation (optional) */
  readonly field?: string | undefined

  /** The invalid value that was provided (optional) */
  readonly value?: unknown

  constructor(message: string, field?: string, value?: unknown) {
    super(message, 'VALIDATION_ERROR')
    this.name = 'ValidationError'
    if (field !== undefined) {
      this.field = field
    }
    this.value = value
  }
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if an error is a DeltaLakeError.
 *
 * @public
 */
export function isDeltaLakeError(error: unknown): error is DeltaLakeError {
  return error instanceof DeltaLakeError
}

/**
 * Check if an error is a StorageError.
 *
 * @public
 */
export function isStorageError(error: unknown): error is StorageError {
  return error instanceof StorageError
}

/**
 * Check if an error is a ConcurrencyError.
 *
 * @public
 */
export function isConcurrencyError(error: unknown): error is ConcurrencyError {
  return error instanceof ConcurrencyError || (error instanceof Error && error.name === 'ConcurrencyError')
}

/**
 * Check if an error is a CDCError.
 *
 * @public
 */
export function isCDCError(error: unknown): error is CDCError {
  return error instanceof CDCError
}

/**
 * Check if an error is a ValidationError.
 *
 * @public
 */
export function isValidationError(error: unknown): error is ValidationError {
  return error instanceof ValidationError
}

/**
 * Type guard to check if an error has a retryable property.
 *
 * @internal
 */
export function hasRetryableProperty(error: Error): error is Error & { retryable: boolean } {
  return 'retryable' in error && typeof (error as Error & { retryable: unknown }).retryable === 'boolean'
}

/**
 * Check if an error is retryable.
 * Returns true for ConcurrencyError and any error with `retryable: true`.
 *
 * @public
 */
export function isRetryableError(error: unknown): boolean {
  if (error == null) return false
  if (!(error instanceof Error)) return false
  if (error instanceof ConcurrencyError) return true
  if (hasRetryableProperty(error) && error.retryable === true) return true
  if (error.name === 'ConcurrencyError') return true
  return false
}

/**
 * Check if an error is a FileNotFoundError.
 *
 * @public
 */
export function isFileNotFoundError(error: unknown): error is FileNotFoundError {
  return error instanceof FileNotFoundError
}

/**
 * Check if an error is a VersionMismatchError.
 *
 * @public
 */
export function isVersionMismatchError(error: unknown): error is VersionMismatchError {
  return error instanceof VersionMismatchError
}
