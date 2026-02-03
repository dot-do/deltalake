/**
 * Delta Lake Type Guards and Validators
 *
 * This module contains all type guard functions for validating Delta Lake data structures.
 *
 * @module delta/validators
 */

import type {
  DeltaAction,
  AddAction,
  RemoveAction,
  MetadataAction,
  ProtocolAction,
  CommitInfoAction,
  DeltaSchemaField,
  DeltaSchema,
  LastCheckpoint,
  FileStats,
} from './types.js'

// =============================================================================
// SCHEMA VALIDATORS
// =============================================================================

/**
 * Type guard to validate DeltaSchemaField from unknown input
 */
export function isValidDeltaSchemaField(obj: unknown): obj is DeltaSchemaField {
  if (typeof obj !== 'object' || obj === null) return false
  const field = obj as Record<string, unknown>
  if (typeof field.name !== 'string') return false
  if (field.type === undefined || field.type === null) return false
  // type can be string or object (complex types)
  if (typeof field.type !== 'string' && typeof field.type !== 'object') return false
  // Optional fields
  if (field.nullable !== undefined && typeof field.nullable !== 'boolean') return false
  if (field.metadata !== undefined && (typeof field.metadata !== 'object' || field.metadata === null)) return false
  return true
}

/**
 * Type guard to validate DeltaSchema from unknown input (e.g., JSON.parse result)
 */
export function isValidDeltaSchema(obj: unknown): obj is DeltaSchema {
  if (typeof obj !== 'object' || obj === null) return false
  const schema = obj as Record<string, unknown>
  if (typeof schema.type !== 'string') return false
  if (!Array.isArray(schema.fields)) return false
  // Validate each field
  for (const field of schema.fields) {
    if (!isValidDeltaSchemaField(field)) return false
  }
  return true
}

// =============================================================================
// CHECKPOINT VALIDATORS
// =============================================================================

/**
 * Type guard to validate LastCheckpoint from unknown input (e.g., JSON.parse result)
 */
export function isValidLastCheckpoint(obj: unknown): obj is LastCheckpoint {
  if (typeof obj !== 'object' || obj === null) return false
  const checkpoint = obj as Record<string, unknown>
  if (typeof checkpoint.version !== 'number') return false
  if (typeof checkpoint.size !== 'number') return false
  // Optional fields
  if (checkpoint.parts !== undefined && typeof checkpoint.parts !== 'number') return false
  if (checkpoint.sizeInBytes !== undefined && typeof checkpoint.sizeInBytes !== 'number') return false
  if (checkpoint.numOfAddFiles !== undefined && typeof checkpoint.numOfAddFiles !== 'number') return false
  return true
}

// =============================================================================
// PARTITION AND STATS VALIDATORS
// =============================================================================

/**
 * Type guard to validate partition values from unknown input (e.g., JSON.parse result)
 * Partition values must be a Record<string, string>
 */
export function isValidPartitionValues(obj: unknown): obj is Record<string, string> {
  if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) return false
  const record = obj as Record<string, unknown>
  for (const [key, value] of Object.entries(record)) {
    if (typeof key !== 'string') return false
    if (typeof value !== 'string') return false
  }
  return true
}

/**
 * Type guard to validate FileStats from unknown input (e.g., JSON.parse result)
 */
export function isValidFileStats(obj: unknown): obj is FileStats {
  if (typeof obj !== 'object' || obj === null) return false
  const stats = obj as Record<string, unknown>
  if (typeof stats.numRecords !== 'number') return false
  if (typeof stats.minValues !== 'object' || stats.minValues === null) return false
  if (typeof stats.maxValues !== 'object' || stats.maxValues === null) return false
  if (typeof stats.nullCount !== 'object' || stats.nullCount === null) return false
  // Validate nullCount values are all numbers
  const nullCount = stats.nullCount as Record<string, unknown>
  for (const value of Object.values(nullCount)) {
    if (typeof value !== 'number') return false
  }
  return true
}

// =============================================================================
// ACTION TYPE GUARDS (for narrowing DeltaAction union types)
// =============================================================================

/**
 * Type guard for AddAction
 */
export function isAddAction(action: DeltaAction): action is AddAction {
  if (!action || typeof action !== 'object') return false
  if (!('add' in action)) return false
  if (!action.add || typeof action.add !== 'object') return false
  return true
}

/**
 * Type guard for RemoveAction
 */
export function isRemoveAction(action: DeltaAction): action is RemoveAction {
  if (!action || typeof action !== 'object') return false
  if (!('remove' in action)) return false
  if (!action.remove || typeof action.remove !== 'object') return false
  return true
}

/**
 * Type guard for MetadataAction
 */
export function isMetadataAction(action: DeltaAction): action is MetadataAction {
  if (!action || typeof action !== 'object') return false
  if (!('metaData' in action)) return false
  if (!action.metaData || typeof action.metaData !== 'object') return false
  return true
}

/**
 * Type guard for ProtocolAction
 */
export function isProtocolAction(action: DeltaAction): action is ProtocolAction {
  if (!action || typeof action !== 'object') return false
  if (!('protocol' in action)) return false
  if (!action.protocol || typeof action.protocol !== 'object') return false
  return true
}

/**
 * Type guard for CommitInfoAction
 */
export function isCommitInfoAction(action: DeltaAction): action is CommitInfoAction {
  if (!action || typeof action !== 'object') return false
  if (!('commitInfo' in action)) return false
  if (!action.commitInfo || typeof action.commitInfo !== 'object') return false
  return true
}

// =============================================================================
// ACTION VALIDATORS (for validating unknown input from JSON.parse)
// =============================================================================

/**
 * Type guard to validate AddAction inner structure from unknown input
 */
export function isValidAddAction(obj: unknown): obj is AddAction['add'] {
  if (typeof obj !== 'object' || obj === null) return false
  const add = obj as Record<string, unknown>
  if (typeof add.path !== 'string') return false
  if (typeof add.size !== 'number') return false
  if (typeof add.modificationTime !== 'number') return false
  if (typeof add.dataChange !== 'boolean') return false
  // Optional fields validation
  if (add.partitionValues !== undefined && (typeof add.partitionValues !== 'object' || add.partitionValues === null)) return false
  if (add.stats !== undefined && typeof add.stats !== 'string') return false
  if (add.tags !== undefined && (typeof add.tags !== 'object' || add.tags === null)) return false
  return true
}

/**
 * Type guard to validate RemoveAction inner structure from unknown input
 */
export function isValidRemoveAction(obj: unknown): obj is RemoveAction['remove'] {
  if (typeof obj !== 'object' || obj === null) return false
  const remove = obj as Record<string, unknown>
  if (typeof remove.path !== 'string') return false
  if (typeof remove.deletionTimestamp !== 'number') return false
  if (typeof remove.dataChange !== 'boolean') return false
  // Optional fields validation
  if (remove.partitionValues !== undefined && (typeof remove.partitionValues !== 'object' || remove.partitionValues === null)) return false
  if (remove.extendedFileMetadata !== undefined && typeof remove.extendedFileMetadata !== 'boolean') return false
  if (remove.size !== undefined && typeof remove.size !== 'number') return false
  return true
}

/**
 * Type guard to validate MetadataAction inner structure from unknown input
 */
export function isValidMetadataAction(obj: unknown): obj is MetadataAction['metaData'] {
  if (typeof obj !== 'object' || obj === null) return false
  const meta = obj as Record<string, unknown>
  if (typeof meta.id !== 'string') return false
  if (typeof meta.schemaString !== 'string') return false
  if (!Array.isArray(meta.partitionColumns)) return false
  if (typeof meta.format !== 'object' || meta.format === null) return false
  const format = meta.format as Record<string, unknown>
  if (typeof format.provider !== 'string') return false
  // Optional fields validation
  if (meta.name !== undefined && typeof meta.name !== 'string') return false
  if (meta.description !== undefined && typeof meta.description !== 'string') return false
  if (meta.createdTime !== undefined && typeof meta.createdTime !== 'number') return false
  if (meta.configuration !== undefined && (typeof meta.configuration !== 'object' || meta.configuration === null)) return false
  return true
}

/**
 * Type guard to validate ProtocolAction inner structure from unknown input
 */
export function isValidProtocolAction(obj: unknown): obj is ProtocolAction['protocol'] {
  if (typeof obj !== 'object' || obj === null) return false
  const protocol = obj as Record<string, unknown>
  if (typeof protocol.minReaderVersion !== 'number') return false
  if (typeof protocol.minWriterVersion !== 'number') return false
  return true
}

/**
 * Type guard to validate CommitInfoAction inner structure from unknown input
 */
export function isValidCommitInfoAction(obj: unknown): obj is CommitInfoAction['commitInfo'] {
  if (typeof obj !== 'object' || obj === null) return false
  const info = obj as Record<string, unknown>
  if (typeof info.timestamp !== 'number') return false
  if (typeof info.operation !== 'string') return false
  // Optional fields validation
  if (info.operationParameters !== undefined && (typeof info.operationParameters !== 'object' || info.operationParameters === null)) return false
  if (info.readVersion !== undefined && typeof info.readVersion !== 'number') return false
  if (info.isolationLevel !== undefined && typeof info.isolationLevel !== 'string') return false
  if (info.isBlindAppend !== undefined && typeof info.isBlindAppend !== 'boolean') return false
  return true
}

/**
 * Type guard to validate a complete DeltaAction from unknown input (e.g., JSON.parse result)
 * This validates both the wrapper structure and inner action data
 */
export function isValidDeltaAction(obj: unknown): obj is DeltaAction {
  if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) return false
  const action = obj as Record<string, unknown>

  // Check for each action type and validate the inner structure
  if ('add' in action) {
    return isValidAddAction(action.add)
  }
  if ('remove' in action) {
    return isValidRemoveAction(action.remove)
  }
  if ('metaData' in action) {
    return isValidMetadataAction(action.metaData)
  }
  if ('protocol' in action) {
    return isValidProtocolAction(action.protocol)
  }
  if ('commitInfo' in action) {
    return isValidCommitInfoAction(action.commitInfo)
  }

  return false
}
