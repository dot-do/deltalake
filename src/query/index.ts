/**
 * Query Layer
 *
 * MongoDB-style operators that translate to Parquet predicates.
 */

import { ValidationError } from '../errors.js'

// =============================================================================
// CONSTANTS
// =============================================================================

/** MongoDB comparison operators */
const COMPARISON_OPS = {
  EQ: '$eq',
  NE: '$ne',
  GT: '$gt',
  GTE: '$gte',
  LT: '$lt',
  LTE: '$lte',
  IN: '$in',
  NIN: '$nin',
  EXISTS: '$exists',
  TYPE: '$type',
  REGEX: '$regex',
} as const

/** MongoDB logical operators */
const LOGICAL_OPS = {
  AND: '$and',
  OR: '$or',
  NOT: '$not',
  NOR: '$nor',
} as const

/** Operator prefix character code ('$' = 36) */
const OPERATOR_PREFIX_CODE = 36

/** Nested path separator */
const PATH_SEPARATOR = '.'

/** Maximum input string length for regex matching (ReDoS protection) */
const MAX_REGEX_INPUT_LENGTH = 10000 // 10KB limit

/** Maximum regex pattern length (ReDoS protection) */
const MAX_REGEX_PATTERN_LENGTH = 1000

// =============================================================================
// PRIMITIVE VALUE TYPES
// =============================================================================

/**
 * Primitive values that can be used in filter comparisons.
 *
 * @public
 */
export type PrimitiveValue = string | number | boolean | Date | null | undefined

/**
 * Values that support ordering comparisons (gt, gte, lt, lte).
 *
 * @public
 */
export type ComparableValue = string | number | Date

/**
 * JSON-serializable values for filter conditions.
 *
 * @public
 */
export type FilterValue = PrimitiveValue | PrimitiveValue[] | Record<string, unknown>

// =============================================================================
// FILTER TYPES (MongoDB-style)
// =============================================================================

/**
 * MongoDB-style filter for querying records.
 *
 * @public
 */
export type Filter<T = Record<string, unknown>> = {
  [K in keyof T]?: T[K] | ComparisonOperators<T[K]>
} & LogicalOperators<T>

// =============================================================================
// PROJECTION TYPES
// =============================================================================

/**
 * MongoDB-style projection specification.
 * - Array of field names: ['name', 'age'] - include only these fields
 * - Object with 1 values: { name: 1, age: 1 } - include only these fields
 * - Object with 0 values: { password: 0 } - exclude these fields
 * - Supports nested fields: 'address.city' or { 'address.city': 1 }
 *
 * @public
 */
export type Projection<T = unknown> =
  | (keyof T | string)[]
  | { [K in keyof T | string]?: 1 | 0 }

/**
 * Query options for DeltaTable.query().
 *
 * @public
 */
export interface QueryOptions<T = unknown> {
  readonly projection?: Projection<T>
}

/**
 * MongoDB-style comparison operators.
 *
 * @public
 */
export interface ComparisonOperators<T> {
  $eq?: T
  $ne?: T
  $gt?: T
  $gte?: T
  $lt?: T
  $lte?: T
  $in?: T[]
  $nin?: T[]
  $exists?: boolean
  $type?: string
  $regex?: string | RegExp
}

/**
 * MongoDB-style logical operators.
 *
 * @public
 */
export interface LogicalOperators<T> {
  $and?: Filter<T>[]
  $or?: Filter<T>[]
  $not?: Filter<T>
  $nor?: Filter<T>[]
}

// =============================================================================
// TYPE GUARDS AND COMPARISON HELPERS
// =============================================================================

/**
 * Type guard for comparable values (number, string, Date).
 *
 * @internal
 */
export function isComparable(v: unknown): v is ComparableValue {
  const t = typeof v
  return t === 'number' || t === 'string' || v instanceof Date
}

/**
 * Type guard for primitive values.
 *
 * @internal
 */
export function isPrimitive(v: unknown): v is PrimitiveValue {
  if (v === null || v === undefined) return true
  const t = typeof v
  return t === 'string' || t === 'number' || t === 'boolean' || v instanceof Date
}

/**
 * Type guard for array of primitive values.
 *
 * @internal
 */
export function isPrimitiveArray(v: unknown): v is PrimitiveValue[] {
  return Array.isArray(v) && v.every(isPrimitive)
}

/**
 * Type guard for regex pattern.
 *
 * @internal
 */
export function isRegexPattern(v: unknown): v is string | RegExp {
  return typeof v === 'string' || v instanceof RegExp
}

/**
 * Type guard for boolean value.
 *
 * @internal
 */
export function isBoolean(v: unknown): v is boolean {
  return typeof v === 'boolean'
}

/**
 * Compare two comparable values for ordering.
 *
 * Uses JavaScript's native comparison operators which handle number,
 * string (lexicographic), and Date comparisons correctly.
 *
 * @param a - First value to compare
 * @param b - Second value to compare
 * @returns -1 if a < b, 1 if a > b, 0 if equal
 *
 * @internal
 */
function compareValues(a: ComparableValue, b: ComparableValue): number {
  if (a < b) return -1
  if (a > b) return 1
  return 0
}

// =============================================================================
// FILTER MATCHING
// =============================================================================

/**
 * Check if a document matches a MongoDB-style filter.
 *
 * Supports comparison operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex)
 * and logical operators ($and, $or, $not, $nor). Also supports nested field access
 * using dot notation.
 *
 * @param doc - The document to check
 * @param filter - MongoDB-style filter object
 * @returns True if the document matches the filter
 *
 * @public
 *
 * @example
 * ```typescript
 * // Simple equality
 * matchesFilter({ name: 'Alice', age: 30 }, { name: 'Alice' }) // true
 *
 * // Comparison operators
 * matchesFilter({ age: 30 }, { age: { $gte: 18 } }) // true
 * matchesFilter({ status: 'active' }, { status: { $in: ['active', 'pending'] } }) // true
 *
 * // Logical operators
 * matchesFilter(
 *   { role: 'admin', active: true },
 *   { $and: [{ role: 'admin' }, { active: true }] }
 * ) // true
 *
 * // Nested field access
 * matchesFilter(
 *   { user: { profile: { age: 25 } } },
 *   { 'user.profile.age': { $gte: 18 } }
 * ) // true
 *
 * // Regex matching
 * matchesFilter({ email: 'alice@example.com' }, { email: { $regex: '@example\\.com$' } }) // true
 * ```
 */
export function matchesFilter<T extends Record<string, unknown>>(
  doc: T,
  filter: Filter<T>
): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    // Handle logical operators - check first character for performance
    if (key.charCodeAt(0) === OPERATOR_PREFIX_CODE) {
      switch (key) {
        case LOGICAL_OPS.AND:
          if (!Array.isArray(condition)) return false
          if (!condition.every(f => matchesFilter(doc, f))) return false
          continue
        case LOGICAL_OPS.OR:
          if (!Array.isArray(condition)) return false
          if (!condition.some(f => matchesFilter(doc, f))) return false
          continue
        case LOGICAL_OPS.NOT:
          if (matchesFilter(doc, condition as Filter<T>)) return false
          continue
        case LOGICAL_OPS.NOR:
          if (!Array.isArray(condition)) return false
          if (condition.some(f => matchesFilter(doc, f))) return false
          continue
        default:
          // Unknown operator starting with $ - skip it
          continue
      }
    }

    // Get document value (supports nested paths like 'user.name')
    const docValue = getNestedValue(doc, key)

    // Handle comparison operators
    if (isComparisonObject(condition)) {
      if (!matchesComparisonOperators(docValue, condition)) return false
    } else {
      // Direct equality
      if (docValue !== condition) return false
    }
  }

  return true
}

/**
 * Get a value from an object using a dot-notation path.
 * Optimized for common case of non-nested paths.
 *
 * @param obj - The object to get the value from
 * @param path - The dot-notation path (e.g., 'user.address.city')
 * @returns The value at the path, or undefined if not found
 *
 * @example
 * getNestedValue({ user: { name: 'Alice' } }, 'user.name') // 'Alice'
 * getNestedValue({ a: { b: { c: 1 } } }, 'a.b.c') // 1
 * getNestedValue({ a: 1 }, 'a.b') // undefined
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  // Fast path: non-nested paths (most common case)
  if (!path.includes(PATH_SEPARATOR)) {
    return obj[path]
  }

  // Nested path traversal
  const parts = path.split(PATH_SEPARATOR)
  let value: unknown = obj
  for (let i = 0; i < parts.length; i++) {
    if (value == null || typeof value !== 'object') return undefined
    const part = parts[i]
    if (part === undefined) return undefined
    value = (value as Record<string, unknown>)[part]
  }
  return value
}

/**
 * Check if a value is a comparison operators object.
 *
 * Optimized to check if any key starts with '$' using charCodeAt for performance.
 * This avoids creating intermediate strings or using regex.
 *
 * @param value - Value to check
 * @returns True if value is an object with at least one operator key starting with '$'
 *
 * @internal
 */
function isComparisonObject(value: unknown): value is ComparisonOperators<PrimitiveValue> {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  // Check if any key starts with '$' using charCodeAt for performance
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    if (key !== undefined && key.charCodeAt(0) === OPERATOR_PREFIX_CODE) return true
  }
  return false
}

/**
 * Match a document value against a set of comparison operators.
 *
 * Iterates through each operator in the operators object and checks if the
 * document value satisfies that operator. Short-circuits on first failure
 * for performance.
 *
 * Supported operators:
 * - `$eq`: Exact equality (===)
 * - `$ne`: Not equal (!==)
 * - `$gt`: Greater than (requires comparable values)
 * - `$gte`: Greater than or equal (requires comparable values)
 * - `$lt`: Less than (requires comparable values)
 * - `$lte`: Less than or equal (requires comparable values)
 * - `$in`: Value is in array
 * - `$nin`: Value is not in array
 * - `$exists`: Field exists (not undefined)
 * - `$regex`: String matches regex pattern
 *
 * @param docValue - The value from the document to check
 * @param operators - Object containing operator keys and their expected values
 * @returns True if docValue satisfies all operators, false otherwise
 *
 * @internal
 */
function matchesComparisonOperators<T>(
  docValue: unknown,
  operators: ComparisonOperators<T>
): boolean {
  for (const [op, opValue] of Object.entries(operators)) {
    switch (op) {
      case COMPARISON_OPS.EQ:
        if (docValue !== opValue) return false
        break

      case COMPARISON_OPS.NE:
        if (docValue === opValue) return false
        break

      case COMPARISON_OPS.GT:
        if (!matchesRangeComparison(docValue, opValue, compareValues, (cmp) => cmp > 0)) return false
        break

      case COMPARISON_OPS.GTE:
        if (!matchesRangeComparison(docValue, opValue, compareValues, (cmp) => cmp >= 0)) return false
        break

      case COMPARISON_OPS.LT:
        if (!matchesRangeComparison(docValue, opValue, compareValues, (cmp) => cmp < 0)) return false
        break

      case COMPARISON_OPS.LTE:
        if (!matchesRangeComparison(docValue, opValue, compareValues, (cmp) => cmp <= 0)) return false
        break

      case COMPARISON_OPS.IN:
        if (!Array.isArray(opValue) || !opValue.includes(docValue)) return false
        break

      case COMPARISON_OPS.NIN:
        if (Array.isArray(opValue) && opValue.includes(docValue)) return false
        break

      case COMPARISON_OPS.EXISTS:
        if (opValue && docValue === undefined) return false
        if (!opValue && docValue !== undefined) return false
        break

      case COMPARISON_OPS.REGEX:
        if (!matchesRegex(docValue, opValue)) return false
        break
    }
  }
  return true
}

/**
 * Helper for range comparisons ($gt, $gte, $lt, $lte).
 *
 * Returns false if either value is null/undefined or not comparable.
 * This ensures null values never satisfy range comparisons, which is
 * consistent with MongoDB behavior.
 *
 * @param docValue - The value from the document
 * @param opValue - The value from the operator (the threshold)
 * @param compareFn - Function to compare two comparable values
 * @param predicate - Function to check if the comparison result satisfies the operator
 * @returns True if docValue passes the range comparison against opValue
 *
 * @internal
 */
function matchesRangeComparison(
  docValue: unknown,
  opValue: unknown,
  compareFn: (a: ComparableValue, b: ComparableValue) => number,
  predicate: (comparison: number) => boolean
): boolean {
  // Use type guards to ensure both values are comparable
  if (docValue == null || !isComparable(docValue) || !isComparable(opValue)) {
    return false
  }
  // After type guards, both docValue and opValue are typed as ComparableValue
  return predicate(compareFn(docValue, opValue))
}

/**
 * Helper for regex matching with ReDoS protection.
 *
 * Returns false if docValue is not a string or exceeds the maximum input length.
 * Throws ValidationError if the pattern exceeds the maximum pattern length.
 *
 * ReDoS (Regular Expression Denial of Service) protection:
 * - Input strings are limited to MAX_REGEX_INPUT_LENGTH (10KB)
 * - Pattern strings are limited to MAX_REGEX_PATTERN_LENGTH (1000 chars)
 *
 * @param docValue - The value from the document (must be a string to match)
 * @param pattern - The regex pattern (string or RegExp)
 * @returns True if docValue is a string and matches the pattern
 * @throws {ValidationError} If the pattern string is too long
 *
 * @internal
 */
function matchesRegex(docValue: unknown, pattern: unknown): boolean {
  if (typeof docValue !== 'string') return false

  // Limit input length to prevent ReDoS
  if (docValue.length > MAX_REGEX_INPUT_LENGTH) {
    return false
  }

  // Validate pattern
  if (typeof pattern === 'string') {
    if (pattern.length > MAX_REGEX_PATTERN_LENGTH) {
      throw new ValidationError('Regex pattern too long', 'pattern', {
        length: pattern.length,
        max: MAX_REGEX_PATTERN_LENGTH,
      })
    }
  }

  const regex = pattern instanceof RegExp ? pattern : new RegExp(pattern as string)
  return regex.test(docValue)
}

// =============================================================================
// PARQUET PREDICATE TRANSLATION
// =============================================================================

import type { ZoneMapFilter } from '../parquet/index.js'

/** Mapping from MongoDB operators to ZoneMap operators */
const MONGO_TO_ZONEMAP_OP: Readonly<Record<string, ZoneMapFilter['operator']>> = {
  [COMPARISON_OPS.EQ]: 'eq',
  [COMPARISON_OPS.NE]: 'ne',
  [COMPARISON_OPS.GT]: 'gt',
  [COMPARISON_OPS.GTE]: 'gte',
  [COMPARISON_OPS.LT]: 'lt',
  [COMPARISON_OPS.LTE]: 'lte',
  [COMPARISON_OPS.IN]: 'in',
}

/**
 * Convert a MongoDB-style filter to Parquet zone map filters.
 *
 * This enables predicate pushdown for Parquet files by translating
 * MongoDB-style filters into zone map filters that can skip row groups
 * that don't contain matching data.
 *
 * Only pushable predicates are converted; logical operators like $or, $not,
 * and $nor are skipped as they cannot be efficiently pushed to zone maps.
 *
 * @param filter - MongoDB-style filter object
 * @returns Array of zone map filters for predicate pushdown
 *
 * @public
 *
 * @example
 * ```typescript
 * // Simple equality -> eq predicate
 * filterToParquetPredicate({ status: 'active' })
 * // => [{ column: 'status', operator: 'eq', value: 'active' }]
 *
 * // Range query -> gte and lte predicates with 'between' optimization
 * filterToParquetPredicate({ age: { $gte: 18, $lte: 65 } })
 * // => [
 * //   { column: 'age', operator: 'gte', value: 18 },
 * //   { column: 'age', operator: 'lte', value: 65 },
 * //   { column: 'age', operator: 'between', value: 18, value2: 65 }
 * // ]
 *
 * // $in operator -> in predicate
 * filterToParquetPredicate({ role: { $in: ['admin', 'moderator'] } })
 * // => [{ column: 'role', operator: 'in', value: ['admin', 'moderator'] }]
 *
 * // $and combines predicates from all subfilters
 * filterToParquetPredicate({
 *   $and: [{ status: 'active' }, { age: { $gte: 18 } }]
 * })
 * // => [
 * //   { column: 'status', operator: 'eq', value: 'active' },
 * //   { column: 'age', operator: 'gte', value: 18 }
 * // ]
 * ```
 */
export function filterToParquetPredicate<T>(
  filter: Filter<T>
): ZoneMapFilter[] {
  const predicates: ZoneMapFilter[] = []
  processFilterToPredicates(filter, predicates)
  return predicates
}

/**
 * Recursively process filter entries and add zone map predicates.
 *
 * This function traverses the filter object and extracts predicates that
 * can be pushed down to Parquet zone maps for row group pruning.
 *
 * - Field conditions with pushable operators ($eq, $ne, $gt, $gte, $lt, $lte, $in)
 *   are converted to zone map filters
 * - $and operators are recursively processed (conjunctive predicates)
 * - Other logical operators ($or, $not, $nor) are skipped as they cannot be
 *   efficiently pushed to zone maps
 * - Non-pushable operators ($exists, $regex) are skipped
 *
 * @param filter - The MongoDB-style filter to process
 * @param predicates - Array to append zone map filters to (mutated)
 *
 * @internal
 */
function processFilterToPredicates<T>(
  filter: Filter<T>,
  predicates: ZoneMapFilter[]
): void {
  for (const [key, condition] of Object.entries(filter)) {
    // Check if this is an operator key (starts with '$')
    if (key.charCodeAt(0) === OPERATOR_PREFIX_CODE) {
      // Handle $and by recursively converting each subfilter
      if (key === LOGICAL_OPS.AND && Array.isArray(condition)) {
        for (const subFilter of condition) {
          processFilterToPredicates(subFilter, predicates)
        }
      }
      // Skip other logical operators (not pushable to zone maps)
      continue
    }

    // Process field condition
    if (isComparisonObject(condition)) {
      processComparisonOperators(key, condition, predicates)
    } else {
      // Direct equality - push as 'eq' predicate
      predicates.push({
        column: key,
        operator: 'eq',
        value: condition,
      })
    }
  }
}

/**
 * Process comparison operators for a field and add zone map predicates.
 *
 * Converts pushable MongoDB operators to zone map filter predicates.
 * Also generates an optimized 'between' predicate when both $gte and $lte
 * are present on the same field, enabling more efficient range pruning.
 *
 * Pushable operators: $eq, $ne, $gt, $gte, $lt, $lte, $in
 * Non-pushable operators (skipped): $nin, $exists, $type, $regex
 *
 * @param column - The field name (column) being filtered
 * @param condition - The comparison operators object for this field
 * @param predicates - Array to append zone map filters to (mutated)
 *
 * @internal
 */
function processComparisonOperators(
  column: string,
  condition: ComparisonOperators<PrimitiveValue>,
  predicates: ZoneMapFilter[]
): void {
  let hasGte = false
  let hasLte = false
  let gteValue: ComparableValue | undefined
  let lteValue: ComparableValue | undefined

  for (const [op, value] of Object.entries(condition)) {
    const zoneMapOp = MONGO_TO_ZONEMAP_OP[op]
    if (zoneMapOp) {
      predicates.push({
        column,
        operator: zoneMapOp,
        value,
      })

      // Track range bounds for 'between' optimization
      // Only track comparable values (required for between predicate)
      if (op === COMPARISON_OPS.GTE && isComparable(value)) {
        hasGte = true
        gteValue = value
      } else if (op === COMPARISON_OPS.LTE && isComparable(value)) {
        hasLte = true
        lteValue = value
      }
    }
  }

  // Generate 'between' predicate when both comparable bounds are present
  if (hasGte && hasLte && gteValue !== undefined && lteValue !== undefined) {
    predicates.push({
      column,
      operator: 'between',
      value: gteValue,
      value2: lteValue,
    })
  }
}

// =============================================================================
// PROJECTION HELPERS
// =============================================================================

/**
 * Set a value in an object using a dot-notation path.
 * Optimized for common case of non-nested paths.
 *
 * @param obj - The object to set the value in
 * @param path - The dot-notation path (e.g., 'user.address.city')
 * @param value - The value to set
 *
 * @example
 * setNestedValue({}, 'user.name', 'Alice') // { user: { name: 'Alice' } }
 */
function setNestedValue(obj: Record<string, unknown>, path: string, value: unknown): void {
  // Fast path: non-nested paths
  if (!path.includes(PATH_SEPARATOR)) {
    obj[path] = value
    return
  }

  // Nested path traversal
  const parts = path.split(PATH_SEPARATOR)
  const lastIndex = parts.length - 1
  let current: Record<string, unknown> = obj

  for (let i = 0; i < lastIndex; i++) {
    const part = parts[i]
    if (part === undefined) return
    if (!(part in current) || typeof current[part] !== 'object' || current[part] === null) {
      current[part] = {}
    }
    current = current[part] as Record<string, unknown>
  }

  const lastPart = parts[lastIndex]
  if (lastPart !== undefined) {
    current[lastPart] = value
  }
}

/**
 * Normalize a projection specification to a consistent internal format.
 *
 * Handles both array format (['name', 'age']) and object format ({ name: 1 } or { password: 0 }).
 * Array format is always treated as inclusion. Object format is determined by the first
 * entry's value (1 = inclusion, 0 = exclusion).
 *
 * Note: MongoDB doesn't allow mixing inclusion and exclusion (except for _id), but this
 * implementation simplifies by using the first entry to determine the mode.
 *
 * @param projection - The projection specification to normalize
 * @returns Object with fields array and isInclusion boolean
 *
 * @internal
 */
function normalizeProjection<T>(projection: Projection<T>): { fields: string[]; isInclusion: boolean } {
  // Array format is always inclusion
  if (Array.isArray(projection)) {
    return { fields: projection as string[], isInclusion: true }
  }

  // Object format - determine if inclusion or exclusion
  const entries = Object.entries(projection)
  if (entries.length === 0) {
    return { fields: [], isInclusion: true }
  }

  // Check if this is inclusion (1) or exclusion (0)
  // MongoDB doesn't allow mixing (except for _id), but we'll simplify and check the first value
  const firstEntry = entries[0]
  const isInclusion = firstEntry !== undefined && firstEntry[1] === 1

  const fields = entries
    .filter(([, value]) => (isInclusion ? value === 1 : value === 0))
    .map(([key]) => key)

  return { fields, isInclusion }
}

/**
 * Apply a projection to a single document
 *
 * @param doc - The document to apply projection to
 * @param projection - The projection specification
 * @returns A new document with only the projected fields
 *
 * @example
 * // Include only name and age
 * applyProjectionToDoc({ name: 'Alice', age: 30, email: 'a@b.com' }, ['name', 'age'])
 * // Returns: { name: 'Alice', age: 30 }
 *
 * // Nested field projection
 * applyProjectionToDoc({ user: { name: 'Alice', age: 30 } }, ['user.name'])
 * // Returns: { user: { name: 'Alice' } }
 */
export function applyProjectionToDoc<T extends Record<string, unknown>>(
  doc: T,
  projection: Projection<T>
): Partial<T> {
  const { fields, isInclusion } = normalizeProjection(projection)

  if (fields.length === 0) {
    // No projection specified, return full document
    return { ...doc }
  }

  if (isInclusion) {
    return applyInclusionProjection(doc, fields)
  } else {
    return applyExclusionProjection(doc, fields)
  }
}

/**
 * Apply inclusion projection to a document.
 *
 * Creates a new object containing only the specified fields from the document.
 * Supports nested fields via dot notation (e.g., 'user.name').
 *
 * @param doc - The source document
 * @param fields - Array of field names (including nested paths) to include
 * @returns New object with only the specified fields
 *
 * @internal
 */
function applyInclusionProjection<T extends Record<string, unknown>>(
  doc: T,
  fields: string[]
): Partial<T> {
  const result: Record<string, unknown> = {}

  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]
    if (field === undefined) continue
    const value = getNestedValue(doc, field)
    if (value !== undefined) {
      setNestedValue(result, field, value)
    }
  }

  return result as Partial<T>
}

/**
 * Apply exclusion projection to a document.
 *
 * Creates a shallow copy of the document with the specified fields removed.
 * Supports nested fields via dot notation (e.g., 'user.password').
 * Nested objects are cloned to avoid mutating the original document.
 *
 * @param doc - The source document
 * @param fields - Array of field names (including nested paths) to exclude
 * @returns New object with the specified fields removed
 *
 * @internal
 */
function applyExclusionProjection<T extends Record<string, unknown>>(
  doc: T,
  fields: string[]
): Partial<T> {
  const result: Record<string, unknown> = { ...doc }

  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]
    if (field === undefined) continue
    deleteNestedField(result, field)
  }

  return result as Partial<T>
}

/**
 * Delete a field from an object at a dot-notation path.
 *
 * For top-level fields, performs a simple delete.
 * For nested paths, navigates to the parent object and deletes the final key.
 * Clones intermediate nested objects to avoid mutating the original document.
 *
 * @param obj - The object to delete the field from (may be mutated)
 * @param path - The dot-notation path to the field (e.g., 'user.password')
 *
 * @internal
 */
function deleteNestedField(obj: Record<string, unknown>, path: string): void {
  // Fast path: non-nested paths
  if (!path.includes(PATH_SEPARATOR)) {
    delete obj[path]
    return
  }

  // Nested path - navigate to parent and delete the final key
  const parts = path.split(PATH_SEPARATOR)
  const lastIndex = parts.length - 1
  let current: Record<string, unknown> = obj

  for (let i = 0; i < lastIndex; i++) {
    const part = parts[i]
    if (part === undefined) return
    if (current[part] && typeof current[part] === 'object') {
      // Clone nested object to avoid mutating original
      current[part] = { ...(current[part] as Record<string, unknown>) }
      current = current[part] as Record<string, unknown>
    } else {
      return // Path doesn't exist, nothing to delete
    }
  }

  const lastPart = parts[lastIndex]
  if (lastPart !== undefined) {
    delete current[lastPart]
  }
}

/**
 * Apply a projection to an array of documents.
 *
 * Supports MongoDB-style projections:
 * - Array format: `['name', 'age']` - include only specified fields
 * - Object format (inclusion): `{ name: 1, age: 1 }` - include only specified fields
 * - Object format (exclusion): `{ password: 0 }` - exclude specified fields
 * - Nested fields: `['user.name']` or `{ 'user.name': 1 }`
 *
 * @param docs - Array of documents
 * @param projection - The projection specification
 * @returns Array of documents with only the projected fields
 *
 * @example
 * ```typescript
 * const users = [
 *   { name: 'Alice', age: 30, password: 'secret', profile: { tier: 'premium' } },
 *   { name: 'Bob', age: 25, password: 'secret', profile: { tier: 'free' } }
 * ]
 *
 * // Array inclusion projection
 * applyProjection(users, ['name', 'age'])
 * // => [{ name: 'Alice', age: 30 }, { name: 'Bob', age: 25 }]
 *
 * // Object exclusion projection
 * applyProjection(users, { password: 0 })
 * // => [
 * //   { name: 'Alice', age: 30, profile: { tier: 'premium' } },
 * //   { name: 'Bob', age: 25, profile: { tier: 'free' } }
 * // ]
 *
 * // Nested field projection
 * applyProjection(users, ['name', 'profile.tier'])
 * // => [
 * //   { name: 'Alice', profile: { tier: 'premium' } },
 * //   { name: 'Bob', profile: { tier: 'free' } }
 * // ]
 * ```
 */
export function applyProjection<T extends Record<string, unknown>>(
  docs: T[],
  projection: Projection<T>
): Partial<T>[] {
  return docs.map(doc => applyProjectionToDoc(doc, projection))
}

/**
 * Extract column names from a projection for Parquet column pruning.
 * Returns empty array for exclusion projections (all columns needed).
 *
 * @param projection - The projection specification
 * @returns Array of column names needed for the query
 */
export function getProjectionColumns<T>(projection: Projection<T>): string[] {
  const { fields, isInclusion } = normalizeProjection(projection)

  if (!isInclusion || fields.length === 0) {
    // For exclusion projections, we need all columns (or handle differently)
    // Return empty to signal "all columns needed"
    return []
  }

  // Extract top-level column names from nested paths
  const columns = new Set<string>()
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]
    if (field === undefined) continue
    // Get the root column name for nested paths
    const separatorIndex = field.indexOf(PATH_SEPARATOR)
    const rootColumn = separatorIndex === -1 ? field : field.slice(0, separatorIndex)
    columns.add(rootColumn)
  }

  return Array.from(columns)
}

// =============================================================================
// AGGREGATION TYPES
// =============================================================================

/**
 * MongoDB-style aggregation accumulator operators.
 *
 * @public
 */
export type AccumulatorOperator =
  | '$sum'
  | '$avg'
  | '$min'
  | '$max'
  | '$first'
  | '$last'
  | '$push'
  | '$addToSet'
  | '$count'
  | '$stdDevPop'
  | '$stdDevSamp'

/**
 * Accumulator expression for use in $group stage.
 *
 * @public
 */
export interface AccumulatorExpression {
  /** Sum of numeric values */
  $sum?: string | number | AccumulatorExpression
  /** Average of numeric values */
  $avg?: string | AccumulatorExpression
  /** Minimum value */
  $min?: string | AccumulatorExpression
  /** Maximum value */
  $max?: string | AccumulatorExpression
  /** First value in the group */
  $first?: string | AccumulatorExpression
  /** Last value in the group */
  $last?: string | AccumulatorExpression
  /** Collect all values into an array */
  $push?: string | AccumulatorExpression
  /** Collect unique values into an array */
  $addToSet?: string | AccumulatorExpression
  /** Count documents in the group */
  $count?: Record<string, never>
  /** Population standard deviation */
  $stdDevPop?: string | AccumulatorExpression
  /** Sample standard deviation */
  $stdDevSamp?: string | AccumulatorExpression
}

/**
 * Specification for a $group stage.
 *
 * The _id field specifies the grouping key. Other fields specify
 * accumulator expressions to compute for each group.
 *
 * @public
 *
 * @example
 * ```typescript
 * const groupSpec: GroupSpec = {
 *   _id: '$category',  // Group by category field
 *   totalAmount: { $sum: '$amount' },
 *   avgPrice: { $avg: '$price' },
 *   count: { $count: {} }
 * }
 * ```
 */
export interface GroupSpec {
  /** Grouping key - field path (e.g., '$field') or compound key */
  _id: string | Record<string, string> | null
  /** Additional accumulator fields */
  [field: string]: string | Record<string, string> | AccumulatorExpression | null | undefined
}

/**
 * Sort specification for $sort stage.
 *
 * @public
 */
export interface SortSpec {
  [field: string]: 1 | -1
}

/**
 * $match stage - filters documents.
 *
 * @public
 */
export interface MatchStage<T = Record<string, unknown>> {
  $match: Filter<T>
}

/**
 * $group stage - groups documents and applies accumulators.
 *
 * @public
 */
export interface GroupStage {
  $group: GroupSpec
}

/**
 * $project stage - reshapes documents.
 *
 * @public
 */
export interface ProjectStage<T = Record<string, unknown>> {
  $project: Projection<T>
}

/**
 * $sort stage - sorts documents.
 *
 * @public
 */
export interface SortStage {
  $sort: SortSpec
}

/**
 * $limit stage - limits the number of documents.
 *
 * @public
 */
export interface LimitStage {
  $limit: number
}

/**
 * $skip stage - skips a number of documents.
 *
 * @public
 */
export interface SkipStage {
  $skip: number
}

/**
 * $unwind stage - deconstructs an array field.
 *
 * @public
 */
export interface UnwindStage {
  $unwind: string | { path: string; preserveNullAndEmptyArrays?: boolean }
}

/**
 * Union type of all supported aggregation pipeline stages.
 *
 * @public
 */
export type AggregationStage<T = Record<string, unknown>> =
  | MatchStage<T>
  | GroupStage
  | ProjectStage<T>
  | SortStage
  | LimitStage
  | SkipStage
  | UnwindStage

/**
 * An aggregation pipeline is an array of stages.
 *
 * @public
 */
export type AggregationPipeline<T = Record<string, unknown>> = AggregationStage<T>[]

/**
 * Result of an aggregation operation.
 *
 * @public
 */
export interface AggregationResult<T = Record<string, unknown>> {
  /** The aggregated documents */
  documents: T[]
  /** Execution statistics */
  stats?: {
    /** Number of documents processed */
    documentsProcessed: number
    /** Number of groups created (for $group stage) */
    groupsCreated?: number
    /** Execution time in milliseconds */
    executionTimeMs: number
  }
}

// =============================================================================
// AGGREGATION CONSTANTS
// =============================================================================

/** Aggregation operator prefix character code ('$' = 36) */
const AGG_OPERATOR_PREFIX_CODE = 36

// =============================================================================
// ACCUMULATOR IMPLEMENTATIONS
// =============================================================================

/**
 * Get a field value from a document using a field path.
 *
 * Field paths start with '$' and support dot notation.
 *
 * @internal
 */
function getFieldValue(doc: Record<string, unknown>, fieldPath: string): unknown {
  // If it's a field reference starting with '$', strip it
  const path = fieldPath.charCodeAt(0) === AGG_OPERATOR_PREFIX_CODE
    ? fieldPath.slice(1)
    : fieldPath
  return getNestedValue(doc, path)
}

/**
 * Evaluate an accumulator expression against a set of documents.
 *
 * @internal
 */
function evaluateAccumulator(
  docs: Record<string, unknown>[],
  expr: AccumulatorExpression
): unknown {
  const [operator, operand] = Object.entries(expr)[0] ?? []
  if (!operator) return null

  switch (operator) {
    case '$sum':
      return computeSum(docs, operand)
    case '$avg':
      return computeAvg(docs, operand)
    case '$min':
      return computeMin(docs, operand)
    case '$max':
      return computeMax(docs, operand)
    case '$first':
      return computeFirst(docs, operand)
    case '$last':
      return computeLast(docs, operand)
    case '$push':
      return computePush(docs, operand)
    case '$addToSet':
      return computeAddToSet(docs, operand)
    case '$count':
      return docs.length
    case '$stdDevPop':
      return computeStdDev(docs, operand, false)
    case '$stdDevSamp':
      return computeStdDev(docs, operand, true)
    default:
      return null
  }
}

/**
 * Compute the sum of values for a field across documents.
 *
 * @internal
 */
function computeSum(docs: Record<string, unknown>[], operand: unknown): number {
  // If operand is a number, sum that constant for each doc
  if (typeof operand === 'number') {
    return docs.length * operand
  }

  // If operand is a field path, sum the field values
  if (typeof operand === 'string') {
    let sum = 0
    for (const doc of docs) {
      const value = getFieldValue(doc, operand)
      if (typeof value === 'number') {
        sum += value
      }
    }
    return sum
  }

  // If operand is a nested expression, evaluate it
  if (typeof operand === 'object' && operand !== null) {
    let sum = 0
    for (const doc of docs) {
      const value = evaluateAccumulator([doc], operand as AccumulatorExpression)
      if (typeof value === 'number') {
        sum += value
      }
    }
    return sum
  }

  return 0
}

/**
 * Compute the average of values for a field across documents.
 *
 * @internal
 */
function computeAvg(docs: Record<string, unknown>[], operand: unknown): number | null {
  if (typeof operand !== 'string') return null

  let sum = 0
  let count = 0

  for (const doc of docs) {
    const value = getFieldValue(doc, operand)
    if (typeof value === 'number') {
      sum += value
      count++
    }
  }

  return count > 0 ? sum / count : null
}

/**
 * Find the minimum value for a field across documents.
 *
 * @internal
 */
function computeMin(docs: Record<string, unknown>[], operand: unknown): unknown {
  if (typeof operand !== 'string') return null

  let min: unknown = undefined

  for (const doc of docs) {
    const value = getFieldValue(doc, operand)
    if (value === null || value === undefined) continue

    if (min === undefined) {
      min = value
    } else if (isComparable(value) && isComparable(min)) {
      if (value < min) {
        min = value
      }
    }
  }

  return min === undefined ? null : min
}

/**
 * Find the maximum value for a field across documents.
 *
 * @internal
 */
function computeMax(docs: Record<string, unknown>[], operand: unknown): unknown {
  if (typeof operand !== 'string') return null

  let max: unknown = undefined

  for (const doc of docs) {
    const value = getFieldValue(doc, operand)
    if (value === null || value === undefined) continue

    if (max === undefined) {
      max = value
    } else if (isComparable(value) && isComparable(max)) {
      if (value > max) {
        max = value
      }
    }
  }

  return max === undefined ? null : max
}

/**
 * Get the first value for a field in the group.
 *
 * @internal
 */
function computeFirst(docs: Record<string, unknown>[], operand: unknown): unknown {
  if (typeof operand !== 'string' || docs.length === 0) return null
  const firstDoc = docs[0]
  return firstDoc ? getFieldValue(firstDoc, operand) : null
}

/**
 * Get the last value for a field in the group.
 *
 * @internal
 */
function computeLast(docs: Record<string, unknown>[], operand: unknown): unknown {
  if (typeof operand !== 'string' || docs.length === 0) return null
  const lastDoc = docs[docs.length - 1]
  return lastDoc ? getFieldValue(lastDoc, operand) : null
}

/**
 * Collect all values for a field into an array.
 *
 * @internal
 */
function computePush(docs: Record<string, unknown>[], operand: unknown): unknown[] {
  if (typeof operand !== 'string') return []

  const result: unknown[] = []
  for (const doc of docs) {
    const value = getFieldValue(doc, operand)
    result.push(value)
  }
  return result
}

/**
 * Collect unique values for a field into an array.
 *
 * @internal
 */
function computeAddToSet(docs: Record<string, unknown>[], operand: unknown): unknown[] {
  if (typeof operand !== 'string') return []

  const seen = new Set<string>()
  const result: unknown[] = []

  for (const doc of docs) {
    const value = getFieldValue(doc, operand)
    const key = JSON.stringify(value)
    if (!seen.has(key)) {
      seen.add(key)
      result.push(value)
    }
  }
  return result
}

/**
 * Compute standard deviation of values for a field.
 *
 * @internal
 */
function computeStdDev(
  docs: Record<string, unknown>[],
  operand: unknown,
  isSample: boolean
): number | null {
  if (typeof operand !== 'string') return null

  const values: number[] = []
  for (const doc of docs) {
    const value = getFieldValue(doc, operand)
    if (typeof value === 'number') {
      values.push(value)
    }
  }

  if (values.length === 0) return null
  if (isSample && values.length === 1) return null

  const n = values.length
  const mean = values.reduce((a, b) => a + b, 0) / n
  const squaredDiffs = values.map(v => Math.pow(v - mean, 2))
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / (isSample ? n - 1 : n)

  return Math.sqrt(variance)
}

// =============================================================================
// PIPELINE STAGE IMPLEMENTATIONS
// =============================================================================

/**
 * Execute $group stage.
 *
 * @internal
 */
function executeGroupStage(
  docs: Record<string, unknown>[],
  spec: GroupSpec
): Record<string, unknown>[] {
  // Build groups based on _id
  const groups = new Map<string, Record<string, unknown>[]>()

  for (const doc of docs) {
    const groupKey = computeGroupKey(doc, spec._id)
    const keyStr = JSON.stringify(groupKey)

    let group = groups.get(keyStr)
    if (!group) {
      group = []
      groups.set(keyStr, group)
    }
    group.push(doc)
  }

  // Compute accumulators for each group
  const results: Record<string, unknown>[] = []

  for (const [keyStr, groupDocs] of groups) {
    const result: Record<string, unknown> = {
      _id: JSON.parse(keyStr) as unknown,
    }

    // Process each accumulator field
    for (const [field, expr] of Object.entries(spec)) {
      if (field === '_id') continue

      if (typeof expr === 'object' && expr !== null) {
        result[field] = evaluateAccumulator(groupDocs, expr as AccumulatorExpression)
      }
    }

    results.push(result)
  }

  return results
}

/**
 * Compute the group key for a document.
 *
 * @internal
 */
function computeGroupKey(
  doc: Record<string, unknown>,
  idSpec: string | Record<string, string> | null
): unknown {
  if (idSpec === null) {
    return null
  }

  if (typeof idSpec === 'string') {
    return getFieldValue(doc, idSpec)
  }

  // Compound key
  const key: Record<string, unknown> = {}
  for (const [keyField, valuePath] of Object.entries(idSpec)) {
    key[keyField] = getFieldValue(doc, valuePath)
  }
  return key
}

/**
 * Execute $sort stage.
 *
 * @internal
 */
function executeSortStage(
  docs: Record<string, unknown>[],
  spec: SortSpec
): Record<string, unknown>[] {
  const sortFields = Object.entries(spec)

  return [...docs].sort((a, b) => {
    for (const [field, direction] of sortFields) {
      const aVal = getNestedValue(a, field)
      const bVal = getNestedValue(b, field)

      // Handle null/undefined - treat them as less than any value
      if (aVal === null || aVal === undefined) {
        if (bVal !== null && bVal !== undefined) {
          return direction === 1 ? -1 : 1
        }
        continue
      }
      if (bVal === null || bVal === undefined) {
        return direction === 1 ? 1 : -1
      }

      // Compare values
      let cmp = 0
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal
      } else if (typeof aVal === 'string' && typeof bVal === 'string') {
        cmp = aVal.localeCompare(bVal)
      } else if (aVal instanceof Date && bVal instanceof Date) {
        cmp = aVal.getTime() - bVal.getTime()
      } else {
        // Fallback: convert to string and compare
        cmp = String(aVal).localeCompare(String(bVal))
      }

      if (cmp !== 0) {
        return direction === 1 ? cmp : -cmp
      }
    }
    return 0
  })
}

/**
 * Execute $unwind stage.
 *
 * @internal
 */
function executeUnwindStage(
  docs: Record<string, unknown>[],
  spec: string | { path: string; preserveNullAndEmptyArrays?: boolean }
): Record<string, unknown>[] {
  const path = typeof spec === 'string' ? spec : spec.path
  const preserveNullAndEmpty = typeof spec === 'object' && spec.preserveNullAndEmptyArrays

  // Strip leading '$' from path
  const fieldPath = path.charCodeAt(0) === AGG_OPERATOR_PREFIX_CODE
    ? path.slice(1)
    : path

  const results: Record<string, unknown>[] = []

  for (const doc of docs) {
    const value = getNestedValue(doc, fieldPath)

    if (Array.isArray(value) && value.length > 0) {
      // Unwind the array
      for (const item of value) {
        const newDoc = { ...doc }
        setNestedValueForUnwind(newDoc, fieldPath, item)
        results.push(newDoc)
      }
    } else if (preserveNullAndEmpty) {
      // Preserve document with null/undefined/empty array
      const newDoc = { ...doc }
      setNestedValueForUnwind(newDoc, fieldPath, null)
      results.push(newDoc)
    }
    // Otherwise, skip documents with null/undefined/empty arrays
  }

  return results
}

/**
 * Set a nested value in a document for unwind operations.
 *
 * @internal
 */
function setNestedValueForUnwind(
  doc: Record<string, unknown>,
  path: string,
  value: unknown
): void {
  if (!path.includes(PATH_SEPARATOR)) {
    doc[path] = value
    return
  }

  const parts = path.split(PATH_SEPARATOR)
  let current: Record<string, unknown> = doc

  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]
    if (part === undefined) return
    if (!(part in current) || typeof current[part] !== 'object' || current[part] === null) {
      current[part] = {}
    }
    current = current[part] as Record<string, unknown>
  }

  const lastPart = parts[parts.length - 1]
  if (lastPart !== undefined) {
    current[lastPart] = value
  }
}

// =============================================================================
// MAIN AGGREGATION FUNCTION
// =============================================================================

/**
 * Execute a MongoDB-style aggregation pipeline on an array of documents.
 *
 * Aggregation pipelines process documents through a series of stages, where
 * each stage transforms the documents in some way. Common stages include:
 *
 * - **$match**: Filter documents (uses the same filter syntax as matchesFilter)
 * - **$group**: Group documents and compute aggregates ($sum, $avg, $min, $max, etc.)
 * - **$project**: Reshape documents (include/exclude fields)
 * - **$sort**: Order documents by field values
 * - **$limit**: Restrict the number of documents
 * - **$skip**: Skip a number of documents
 * - **$unwind**: Deconstruct array fields
 *
 * @typeParam T - The input document type
 * @typeParam R - The output document type (defaults to Record<string, unknown>)
 * @param docs - Array of documents to process
 * @param pipeline - Array of aggregation stages
 * @returns Aggregation result with documents and optional statistics
 *
 * @public
 *
 * @example Basic grouping with sum
 * ```typescript
 * const sales = [
 *   { product: 'A', quantity: 5, price: 10 },
 *   { product: 'B', quantity: 3, price: 20 },
 *   { product: 'A', quantity: 2, price: 10 },
 * ]
 *
 * const result = aggregate(sales, [
 *   {
 *     $group: {
 *       _id: '$product',
 *       totalQuantity: { $sum: '$quantity' },
 *       avgPrice: { $avg: '$price' },
 *       count: { $count: {} }
 *     }
 *   }
 * ])
 *
 * // Result:
 * // [
 * //   { _id: 'A', totalQuantity: 7, avgPrice: 10, count: 2 },
 * //   { _id: 'B', totalQuantity: 3, avgPrice: 20, count: 1 }
 * // ]
 * ```
 *
 * @example Pipeline with match, group, and sort
 * ```typescript
 * const orders = [
 *   { status: 'completed', amount: 100, region: 'US' },
 *   { status: 'pending', amount: 50, region: 'US' },
 *   { status: 'completed', amount: 200, region: 'EU' },
 *   { status: 'completed', amount: 150, region: 'US' },
 * ]
 *
 * const result = aggregate(orders, [
 *   { $match: { status: 'completed' } },
 *   {
 *     $group: {
 *       _id: '$region',
 *       totalAmount: { $sum: '$amount' },
 *       orderCount: { $count: {} }
 *     }
 *   },
 *   { $sort: { totalAmount: -1 } }
 * ])
 *
 * // Result:
 * // [
 * //   { _id: 'US', totalAmount: 250, orderCount: 2 },
 * //   { _id: 'EU', totalAmount: 200, orderCount: 1 }
 * // ]
 * ```
 *
 * @example Using $unwind to flatten arrays
 * ```typescript
 * const users = [
 *   { name: 'Alice', tags: ['admin', 'user'] },
 *   { name: 'Bob', tags: ['user'] },
 * ]
 *
 * const result = aggregate(users, [
 *   { $unwind: '$tags' },
 *   {
 *     $group: {
 *       _id: '$tags',
 *       users: { $push: '$name' }
 *     }
 *   }
 * ])
 *
 * // Result:
 * // [
 * //   { _id: 'admin', users: ['Alice'] },
 * //   { _id: 'user', users: ['Alice', 'Bob'] }
 * // ]
 * ```
 */
export function aggregate<T extends Record<string, unknown>, R = Record<string, unknown>>(
  docs: T[],
  pipeline: AggregationPipeline<T>
): AggregationResult<R> {
  const startTime = Date.now()
  const documentsProcessed = docs.length
  let groupsCreated: number | undefined

  // Process documents through each stage
  let currentDocs: Record<string, unknown>[] = docs

  for (const stage of pipeline) {
    if ('$match' in stage) {
      currentDocs = currentDocs.filter(doc =>
        matchesFilter(doc as T, stage.$match)
      )
    } else if ('$group' in stage) {
      currentDocs = executeGroupStage(currentDocs, stage.$group)
      groupsCreated = currentDocs.length
    } else if ('$project' in stage) {
      currentDocs = applyProjection(currentDocs as T[], stage.$project as Projection<T>)
    } else if ('$sort' in stage) {
      currentDocs = executeSortStage(currentDocs, stage.$sort)
    } else if ('$limit' in stage) {
      currentDocs = currentDocs.slice(0, stage.$limit)
    } else if ('$skip' in stage) {
      currentDocs = currentDocs.slice(stage.$skip)
    } else if ('$unwind' in stage) {
      currentDocs = executeUnwindStage(currentDocs, stage.$unwind)
    }
  }

  const executionTimeMs = Date.now() - startTime

  // Build stats object, only including groupsCreated if defined
  const stats: AggregationResult<R>['stats'] = {
    documentsProcessed,
    executionTimeMs,
  }

  if (groupsCreated !== undefined) {
    stats.groupsCreated = groupsCreated
  }

  return {
    documents: currentDocs as R[],
    stats,
  }
}
