/**
 * Delta Lake Snapshot Support
 *
 * This module contains utilities for working with Delta Lake snapshots,
 * including column mapping support.
 *
 * @module delta/snapshot
 */

import type { MetadataAction } from './types.js'
import { isValidDeltaSchema } from './validators.js'

// =============================================================================
// COLUMN MAPPING SUPPORT
// =============================================================================

/**
 * Build a column mapping from physical names to logical names.
 * Returns null if column mapping is not enabled.
 *
 * Delta Lake column mapping allows renaming columns while maintaining
 * physical Parquet column names:
 * - Mode 'name': Uses physicalName metadata field
 * - Mode 'id': Uses column ID for mapping
 *
 * @param metadata - Delta table metadata
 * @returns Map from physical column names to logical names, or null if not applicable
 */
export function buildColumnMapping(metadata?: MetadataAction['metaData']): Map<string, string> | null {
  // Check if column mapping is enabled
  const columnMappingMode = metadata?.configuration?.['delta.columnMapping.mode']
  if (!columnMappingMode || (columnMappingMode !== 'name' && columnMappingMode !== 'id')) {
    return null
  }

  // Parse schema string to extract column mapping
  if (!metadata?.schemaString) {
    return null
  }

  try {
    const parsed: unknown = JSON.parse(metadata.schemaString)
    if (!isValidDeltaSchema(parsed)) {
      return null
    }

    const mapping = new Map<string, string>()

    for (const field of parsed.fields) {
      const physicalName = field.metadata?.['delta.columnMapping.physicalName']
      if (physicalName) {
        // Map physical name -> logical name
        mapping.set(physicalName, field.name)
      }
    }

    return mapping.size > 0 ? mapping : null
  } catch {
    return null
  }
}

/**
 * Apply column mapping to a row, renaming physical column names to logical names.
 *
 * @param row - Row with physical column names
 * @param mapping - Map from physical to logical column names
 * @returns Row with logical column names
 */
export function applyColumnMapping<T extends Record<string, unknown>>(
  row: T,
  mapping: Map<string, string>
): T {
  const result = {} as T

  for (const [key, value] of Object.entries(row)) {
    // Use logical name if mapping exists, otherwise keep original name
    const logicalName = mapping.get(key) ?? key
    ;(result as Record<string, unknown>)[logicalName] = value
  }

  return result
}
