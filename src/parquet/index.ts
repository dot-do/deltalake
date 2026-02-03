/**
 * Parquet Utilities
 *
 * Streaming writer, variant encoding, and zone maps for efficient reads.
 *
 * Variant encoding follows the official Parquet specification:
 * https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 */

import { isComparable } from '../query/index.js'

// =============================================================================
// VARIANT ENCODING (local implementation)
// =============================================================================

export {
  type VariantValue,
  type EncodedVariant,
  encodeVariant,
  decodeVariant,
} from './variant.js'

// =============================================================================
// STREAMING PARQUET WRITER
// =============================================================================

export {
  StreamingParquetWriter,
  type StreamingParquetWriterOptions,
  type RowGroupStats,
  type ColumnStats,
  type ColumnStatValue,
  type WriteResult,
  type BufferPoolStats,
} from './streaming-writer.js'

// =============================================================================
// ASYNC BUFFER (re-exported from storage)
// =============================================================================

// Re-export AsyncBuffer interface and createAsyncBuffer from storage module
// to maintain backwards compatibility for consumers importing from parquet
export type { AsyncBuffer } from '../storage/index.js'
export { createAsyncBuffer } from '../storage/index.js'

// =============================================================================
// PARQUET WRITER OPTIONS
// =============================================================================

export interface ParquetWriterOptions {
  /** Row group size (default: 10000) */
  readonly rowGroupSize?: number

  /** Target file size in bytes (default: 128MB) */
  readonly targetFileSize?: number

  /** Compression codec */
  readonly compression?: 'UNCOMPRESSED' | 'SNAPPY' | 'LZ4' | 'LZ4_RAW' | 'GZIP' | 'ZSTD'

  /** Schema (optional - inferred from first row if not provided) */
  readonly schema?: ParquetSchema

  /** Fields to shred into typed columns for statistics */
  readonly shredFields?: readonly string[]
}

export interface ParquetSchema {
  readonly fields: readonly ParquetField[]
}

export interface ParquetField {
  readonly name: string
  readonly type: 'boolean' | 'int32' | 'int64' | 'float' | 'double' | 'string' | 'binary' | 'variant'
  readonly optional?: boolean
}

// =============================================================================
// PARQUET READER OPTIONS
// =============================================================================

export interface ParquetReadOptions {
  /** Columns to read (default: all) */
  readonly columns?: readonly string[]

  /** Row group indices to read */
  readonly rowGroups?: readonly number[]

  /** Filter predicate for zone map pruning */
  readonly filter?: ZoneMapFilter
}

export interface ParquetMetadata {
  numRows: number
  numRowGroups: number
  schema: ParquetSchema
  keyValueMetadata: Record<string, string>
}

// =============================================================================
// ZONE MAPS (predicate pushdown)
// =============================================================================

export interface ZoneMap {
  column: string
  min: unknown
  max: unknown
  nullCount: number
}

export interface ZoneMapFilter {
  column: string
  operator: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'between'
  value: unknown
  value2?: unknown // For 'between'
}

/**
 * Check if a zone map can be skipped based on filter.
 *
 * Zone maps store min/max statistics for each column in a row group.
 * This function determines if a row group can be safely skipped because
 * no rows could possibly match the filter based on these statistics.
 *
 * Returns true if the entire row group can be skipped (no matching rows).
 *
 * Null/undefined handling:
 * - If zoneMap.min or zoneMap.max is null/undefined, we cannot determine bounds,
 *   so we conservatively return false (do not skip) to avoid missing matches.
 * - If filter.value is null/undefined, we cannot perform comparison,
 *   so we conservatively return false (do not skip).
 * - For 'between' operator, if filter.value2 is null/undefined, we return false.
 * - For 'in' operator, null/undefined values in the array are treated as non-comparable
 *   and won't contribute to skipping the row group.
 *
 * @param zoneMap - Zone map statistics for a column in a row group
 * @param filter - Filter predicate to check
 * @returns True if the row group can be skipped (no matching rows possible)
 *
 * @example
 * ```typescript
 * // Zone map shows age column has values between 25 and 45
 * const zoneMap = { column: 'age', min: 25, max: 45, nullCount: 0 }
 *
 * // Can skip: looking for age < 20, but min is 25
 * canSkipZoneMap(zoneMap, { column: 'age', operator: 'lt', value: 20 }) // true
 *
 * // Cannot skip: looking for age > 30, some values in [25,45] are > 30
 * canSkipZoneMap(zoneMap, { column: 'age', operator: 'gt', value: 30 }) // false
 *
 * // Can skip: looking for age = 50, but max is 45
 * canSkipZoneMap(zoneMap, { column: 'age', operator: 'eq', value: 50 }) // true
 *
 * // Can skip: looking for age in [10, 15, 20], all outside [25, 45]
 * canSkipZoneMap(zoneMap, { column: 'age', operator: 'in', value: [10, 15, 20] }) // true
 *
 * // Between operator: skip if ranges don't overlap
 * canSkipZoneMap(zoneMap, { column: 'age', operator: 'between', value: 50, value2: 60 }) // true
 * ```
 */
export function canSkipZoneMap(zoneMap: ZoneMap, filter: ZoneMapFilter): boolean {
  const { min, max } = zoneMap
  const { operator, value, value2 } = filter

  // Early return if zone map bounds are null/undefined.
  // Without valid bounds, we cannot safely skip any row group.
  if (min === null || min === undefined || max === null || max === undefined) {
    return false
  }

  // Early return if filter value is null/undefined.
  // We cannot perform meaningful comparison without a filter value.
  if (value === null || value === undefined) {
    return false
  }

  switch (operator) {
    case 'eq':
      // For equality, skip if value is outside [min, max] range
      if (!isComparable(min) || !isComparable(max) || !isComparable(value)) {
        return false
      }
      return value < min || value > max

    case 'ne':
      // For not-equal, skip only if ALL values in row group equal the filter value
      // (i.e., min === max === value means every row has this value)
      if (!isComparable(min) || !isComparable(max) || !isComparable(value)) {
        return false
      }
      return min === max && min === value

    case 'gt':
      // For greater-than, skip if max <= value (no values can be > value)
      if (!isComparable(min) || !isComparable(max) || !isComparable(value)) {
        return false
      }
      return max <= value

    case 'gte':
      // For greater-than-or-equal, skip if max < value (no values can be >= value)
      if (!isComparable(min) || !isComparable(max) || !isComparable(value)) {
        return false
      }
      return max < value

    case 'lt':
      // For less-than, skip if min >= value (no values can be < value)
      if (!isComparable(min) || !isComparable(max) || !isComparable(value)) {
        return false
      }
      return min >= value

    case 'lte':
      // For less-than-or-equal, skip if min > value (no values can be <= value)
      if (!isComparable(min) || !isComparable(max) || !isComparable(value)) {
        return false
      }
      return min > value

    case 'in':
      // For 'in' operator, value must be an array
      if (!Array.isArray(value)) return false
      if (!isComparable(min) || !isComparable(max)) return false
      // Skip if ALL values in the array are outside the [min, max] range.
      // Null/undefined values in the array are not comparable and don't contribute
      // to skipping (they are treated as "could potentially match" to be safe).
      return value.every(v => {
        // If v is null/undefined, we can't confirm it's outside range, so return false
        // to prevent skipping (conservative approach)
        if (v === null || v === undefined) return false
        return isComparable(v) && (v < min || v > max)
      })

    case 'between':
      // For between, we need both value (lower bound) and value2 (upper bound)
      // If value2 is null/undefined, we cannot determine the range
      if (value2 === null || value2 === undefined) {
        return false
      }
      if (!isComparable(min) || !isComparable(max) || !isComparable(value) || !isComparable(value2)) {
        return false
      }
      // Skip if ranges don't overlap: zone map max < filter min OR zone map min > filter max
      return max < value || min > value2

    default:
      // Unknown operator - conservatively do not skip
      return false
  }
}

// =============================================================================
// VARIANT SHREDDING (re-export from hyparquet-writer)
// =============================================================================

/**
 * Schema element from hyparquet for shredded columns
 */
export interface ShreddedSchemaElement {
  name: string
  type?: string
  repetition_type?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED'
  num_children?: number
  converted_type?: string
  logical_type?: { type: string }
}

/**
 * Result from createShreddedVariantColumn
 */
export interface ShreddedColumnResult {
  /** Schema elements for the shredded VARIANT column */
  schema: ShreddedSchemaElement[]
  /** Column data by path (e.g., 'col.typed_value.field.typed_value' -> values) */
  columnData: Map<string, unknown[]>
  /** Paths to shredded typed_value columns for statistics */
  shredPaths: string[]
}

/**
 * Options for createShreddedVariantColumn
 */
export interface ShreddedColumnOptions {
  /** Whether the VARIANT column can be null (default: true) */
  nullable?: boolean
  /** Override field types (e.g., { fieldName: 'STRING' | 'INT32' | 'INT64' | 'FLOAT' | 'DOUBLE' | 'BOOLEAN' | 'TIMESTAMP' }) */
  fieldTypes?: Record<string, string>
}

// Re-export createShreddedVariantColumn from hyparquet-writer
// Note: hyparquet-writer provides its own type definitions, so no module augmentation is needed
export { createShreddedVariantColumn } from '@dotdo/hyparquet-writer'

// =============================================================================
// SHREDDING HELPERS
// =============================================================================

/**
 * Get the column paths that have statistics available after shredding.
 * These can be used for predicate pushdown.
 */
export function getStatisticsPaths(columnName: string, shredFields: string[]): string[] {
  return shredFields.map(f => `${columnName}.typed_value.${f}.typed_value`)
}

/**
 * Map a user filter path to the statistics column path.
 * Returns null if the field is not shredded.
 */
export function mapFilterPathToStats(
  filterPath: string,
  columnName: string,
  shredFields: string[]
): string | null {
  // Check if filterPath starts with columnName
  if (!filterPath.startsWith(`${columnName}.`)) {
    return null
  }

  const fieldName = filterPath.slice(columnName.length + 1)
  if (shredFields.includes(fieldName)) {
    return `${columnName}.typed_value.${fieldName}.typed_value`
  }

  return null
}
