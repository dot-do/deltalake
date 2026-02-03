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
