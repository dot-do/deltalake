/**
 * StreamingParquetWriter
 *
 * Streaming wrapper around hyparquet-writer for row-by-row Parquet file creation.
 * Provides automatic row group flushing, schema inference, and column statistics.
 */

import type { ParquetSchema, ParquetField, AsyncBuffer, ZoneMap } from './index.js'
import { encodeVariant, decodeVariant, type VariantValue } from './variant.js'

/** Internal mutable version of ParquetField for schema inference */
interface MutableParquetField {
  name: string
  type: 'boolean' | 'int32' | 'int64' | 'float' | 'double' | 'string' | 'binary' | 'variant'
  optional?: boolean
}

/** Internal mutable version of ParquetSchema for schema inference */
interface MutableParquetSchema {
  fields: MutableParquetField[]
}
import { getNestedValue } from '../query/index.js'
import { ValidationError } from '../errors.js'
import { ByteWriter, ParquetWriter, type SchemaElement } from '@dotdo/hyparquet-writer'
import { parquetReadObjects, type AsyncBuffer as HyparquetAsyncBuffer } from '@dotdo/hyparquet'

// =============================================================================
// CONSTANTS
// =============================================================================

/** Default number of rows per row group */
const DEFAULT_ROW_GROUP_SIZE = 10000

/** Default target row group size in bytes (128 MB) */
const DEFAULT_TARGET_ROW_GROUP_BYTES = 128 * 1024 * 1024

/** Default maximum buffer size in bytes (64 MB) */
const DEFAULT_MAX_BUFFER_BYTES = 64 * 1024 * 1024

/** Default maximum pending flush operations */
const DEFAULT_MAX_PENDING_FLUSHES = 2

/** Estimated bytes per primitive value for buffer size estimation */
const ESTIMATED_BYTES_PER_PRIMITIVE = 8

/** Backpressure wait interval in milliseconds */
const BACKPRESSURE_WAIT_MS = 10

// =============================================================================
// TYPES
// =============================================================================

export interface StreamingParquetWriterOptions {
  /** Number of rows per row group (default: 10000) */
  readonly rowGroupSize?: number | undefined
  /** Target row group size in bytes */
  readonly targetRowGroupBytes?: number | undefined
  /** Maximum buffer size in bytes before forcing flush */
  readonly maxBufferBytes?: number | undefined
  /** Maximum number of pending flush operations */
  readonly maxPendingFlushes?: number | undefined
  /** Enable buffer pooling for memory efficiency */
  readonly enableBufferPooling?: boolean | undefined
  /** Explicit schema (if not provided, inferred from first row) */
  readonly schema?: ParquetSchema | undefined
  /** Enable column statistics */
  readonly statistics?: boolean | undefined
  /** Enable distinct count estimation */
  readonly distinctCountEnabled?: boolean | undefined
  /** Fields to shred from variant columns for statistics */
  readonly shredFields?: readonly string[] | undefined
  /** Compression codec */
  readonly compression?: CompressionCodec | undefined
  /** Key-value metadata to include in file */
  readonly kvMetadata?: readonly KeyValueMetadata[] | undefined
}

/** Supported compression codecs */
export type CompressionCodec = 'UNCOMPRESSED' | 'SNAPPY' | 'LZ4' | 'LZ4_RAW' | 'GZIP' | 'ZSTD'

/** Key-value metadata entry */
export interface KeyValueMetadata {
  readonly key: string
  readonly value: string
}

/** Internal options with all fields required */
interface ResolvedOptions {
  rowGroupSize: number
  targetRowGroupBytes: number
  maxBufferBytes: number
  maxPendingFlushes: number
  enableBufferPooling: boolean
  schema: ParquetSchema | null
  statistics: boolean
  distinctCountEnabled: boolean
  shredFields: string[]
  compression: CompressionCodec
  kvMetadata: KeyValueMetadata[]
}

export interface RowGroupStats {
  numRows: number
  fileOffset: number
  compressedSize: number
  totalUncompressedSize: number
  columnStats: Map<string, ColumnStats>
}

/** Valid types for column statistics min/max values */
export type ColumnStatValue = number | string | bigint | Date | undefined

export interface ColumnStats {
  min: ColumnStatValue
  max: ColumnStatValue
  nullCount: number
  distinctCount?: number
}

export interface WriteResult {
  buffer: ArrayBuffer
  totalRows: number
  rowGroups: RowGroupStats[]
  zoneMaps: ZoneMap[][]
  shreddedColumns: string[]
  metadata: WriteResultMetadata
  createAsyncBuffer(): Promise<AsyncBuffer>
  readRows(asyncBuffer: AsyncBuffer): Promise<Record<string, unknown>[]>
}

interface WriteResultMetadata {
  numRows: number
  keyValueMetadata: KeyValueMetadata[]
  compressionCodec: string
}

export interface BufferPoolStats {
  created: number
  reused: number
  active: number
}

/** Column data for hyparquet-writer */
interface ColumnData {
  name: string
  data: unknown[]
}

/** Row type alias for clarity */
type Row = Record<string, unknown>

// =============================================================================
// TYPE MAPPING
// =============================================================================

/** Parquet type info for schema conversion */
interface ParquetTypeInfo {
  type: 'BOOLEAN' | 'INT32' | 'INT64' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY'
  converted_type?: 'UTF8' | 'JSON'
}

/** Maps ParquetField type to hyparquet SchemaElement type */
const PARQUET_TYPE_MAP: Record<ParquetField['type'], ParquetTypeInfo> = {
  boolean: { type: 'BOOLEAN' },
  int32: { type: 'INT32' },
  int64: { type: 'INT64' },
  float: { type: 'FLOAT' },
  double: { type: 'DOUBLE' },
  string: { type: 'BYTE_ARRAY', converted_type: 'UTF8' },
  binary: { type: 'BYTE_ARRAY' },
  variant: { type: 'BYTE_ARRAY', converted_type: 'JSON' },
}

// =============================================================================
// STREAMING PARQUET WRITER
// =============================================================================

export class StreamingParquetWriter {
  private readonly _options: ResolvedOptions
  private _schema: ParquetSchema | null = null
  private _inferredSchema: MutableParquetSchema | null = null
  private _rows: Row[] = []
  private _writer: ByteWriter | null = null
  private _parquetWriter: ParquetWriter | null = null
  private _rowGroups: RowGroupStats[] = []
  private _zoneMaps: ZoneMap[][] = []
  private _totalRows = 0
  private _completedRowGroups = 0
  private _bytesWritten = 0
  private _isAborted = false
  private _isFinished = false
  private _pendingFlushes = 0
  private _bufferPoolStats: BufferPoolStats = { created: 0, reused: 0, active: 0 }
  private _shreddedColumns: string[]
  private _currentRowGroupStats: Map<string, ColumnStats> = new Map()
  private _cachedBufferSize = 0

  constructor(options?: StreamingParquetWriterOptions) {
    this._options = resolveOptions(options)

    if (this._options.schema) {
      this._schema = this._options.schema
    }

    this._shreddedColumns = this._options.shredFields.length > 0
      ? [...this._options.shredFields]
      : []
  }

  // ===========================================================================
  // PUBLIC GETTERS
  // ===========================================================================

  get options(): StreamingParquetWriterOptions {
    // Return resolved options (which satisfies StreamingParquetWriterOptions)
    return {
      ...this._options,
      schema: this._options.schema ?? undefined,
    }
  }

  get bufferedRowCount(): number {
    return this._rows.length
  }

  get completedRowGroupCount(): number {
    return this._completedRowGroups
  }

  get totalRowsWritten(): number {
    return this._totalRows
  }

  get bytesWritten(): number {
    return this._bytesWritten
  }

  get inferredSchema(): ParquetSchema | null {
    return this._inferredSchema
  }

  get schema(): ParquetSchema | null {
    return this._schema
  }

  get isAborted(): boolean {
    return this._isAborted
  }

  get bufferPoolStats(): BufferPoolStats {
    return this._bufferPoolStats
  }

  // ===========================================================================
  // PUBLIC METHODS
  // ===========================================================================

  async writeRow(row: Row): Promise<void> {
    this.validateState()
    await this.applyBackpressure()

    // Capture state before processing for rollback on error
    const prevRowCount = this._rows.length
    const prevTotalRows = this._totalRows
    const prevCachedBufferSize = this._cachedBufferSize

    try {
      this.processRow(row)

      if (this.shouldFlush()) {
        await this.flushRowGroup()
      }
    } catch (error) {
      // Rollback partial state on error
      this._rows.length = prevRowCount
      this._totalRows = prevTotalRows
      this._cachedBufferSize = prevCachedBufferSize
      throw error
    }
  }

  async finish(): Promise<WriteResult> {
    this.validateState()

    try {
      // Flush any remaining rows
      if (this._rows.length > 0) {
        await this.flushRowGroup()
      }

      // Ensure writer exists (handles empty file case)
      this.ensureWriter()

      // Finalize the parquet file
      if (!this._parquetWriter || !this._writer) {
        throw new ValidationError('Writer not initialized - no rows were written', 'writer', null)
      }
      this._parquetWriter.finish()
      const buffer = this._writer.getBuffer()
      this._bytesWritten = buffer.byteLength
      this._isFinished = true

      return this.createWriteResult(buffer)
    } catch (error) {
      // Clean up resources on error to prevent corrupted state
      await this.cleanupOnError()
      throw error
    }
  }

  async abort(): Promise<void> {
    this._isAborted = true
    this.releaseResources()
  }

  // ===========================================================================
  // RESOURCE CLEANUP
  // ===========================================================================

  /**
   * Clean up resources on error without marking writer as aborted.
   * This allows the error to propagate while ensuring no corrupted state remains.
   */
  private async cleanupOnError(): Promise<void> {
    this.releaseResources()
    // Mark as aborted to prevent further writes
    this._isAborted = true
  }

  /**
   * Release all internal resources and reset state.
   */
  private releaseResources(): void {
    this._rows = []
    this._cachedBufferSize = 0
    this._bufferPoolStats.active = 0
    this._currentRowGroupStats.clear()
    // Release writer references to allow garbage collection
    this._writer = null
    this._parquetWriter = null
  }

  // ===========================================================================
  // STATE VALIDATION
  // ===========================================================================

  private validateState(): void {
    if (this._isAborted) {
      throw new ValidationError('Writer has been aborted', 'state', 'aborted')
    }
    if (this._isFinished) {
      throw new ValidationError('Writer has already finished', 'state', 'finished')
    }
  }

  // ===========================================================================
  // BACKPRESSURE
  // ===========================================================================

  private async applyBackpressure(): Promise<void> {
    while (this._pendingFlushes >= this._options.maxPendingFlushes) {
      await new Promise(resolve => setTimeout(resolve, BACKPRESSURE_WAIT_MS))
    }
  }

  // ===========================================================================
  // ROW PROCESSING
  // ===========================================================================

  private processRow(row: Row): void {
    if (!this._schema && !this._inferredSchema) {
      this._inferredSchema = this.inferSchemaFromRow(row)
    } else if (this._inferredSchema) {
      this.updateSchemaWithRow(row)
    } else if (this._schema) {
      this.validateRow(row)
    }

    // Update cached buffer size with this row's contribution
    this._cachedBufferSize += this.estimateRowSize(row)
    this._rows.push(row)
    this._totalRows++
  }

  // ===========================================================================
  // SCHEMA INFERENCE
  // ===========================================================================

  private inferSchemaFromRow(row: Row): MutableParquetSchema {
    const fields: MutableParquetField[] = []

    for (const [key, value] of Object.entries(row)) {
      fields.push({
        name: key,
        type: inferFieldType(value),
        optional: value === null || value === undefined,
      })
    }

    return { fields }
  }

  private updateSchemaWithRow(row: Row): void {
    if (!this._inferredSchema) return

    const schema = this._inferredSchema

    // Update nullability based on new row
    for (const field of schema.fields) {
      const value = row[field.name]
      if (value === null || value === undefined) {
        field.optional = true
      }
    }

    // Check for schema mismatch
    const rowKeys = Object.keys(row).sort()
    const schemaKeys = schema.fields.map(f => f.name).sort()

    if (!arraysEqual(rowKeys, schemaKeys)) {
      throw new ValidationError('Schema mismatch: row has different fields than inferred schema', 'row', { expected: schemaKeys, actual: rowKeys })
    }

    // Check for type mismatch
    for (const field of schema.fields) {
      const value = row[field.name]
      if (value !== null && value !== undefined) {
        const inferredType = inferFieldType(value)
        if (!isTypeCompatible(inferredType, field.type)) {
          throw new ValidationError(
            `Type mismatch for field ${field.name}: expected ${field.type}, got ${inferredType}`,
            field.name,
            { expected: field.type, actual: inferredType }
          )
        }
      }
    }
  }

  private validateRow(row: Row): void {
    if (!this._schema) return

    for (const field of this._schema.fields) {
      const value = row[field.name]

      // Check required fields
      if (!field.optional && (value === null || value === undefined)) {
        throw new ValidationError(`Required field missing: ${field.name}`, field.name, value)
      }

      // Check type compatibility
      if (value !== null && value !== undefined) {
        const actualType = inferFieldType(value)
        if (!isTypeCompatible(actualType, field.type)) {
          throw new ValidationError(
            `Type mismatch for field ${field.name}: expected ${field.type}, got ${actualType}`,
            field.name,
            { expected: field.type, actual: actualType }
          )
        }
      }
    }
  }

  // ===========================================================================
  // FLUSH DECISION
  // ===========================================================================

  private shouldFlush(): boolean {
    const rowCount = this._rows.length

    // Check row count threshold
    if (rowCount >= this._options.rowGroupSize) {
      return true
    }

    // Check byte size thresholds
    const estimatedBytes = this._cachedBufferSize

    if (estimatedBytes >= this._options.targetRowGroupBytes) {
      return true
    }

    if (estimatedBytes >= this._options.maxBufferBytes) {
      return true
    }

    return false
  }

  private estimateRowSize(row: Row): number {
    let total = 0
    for (const value of Object.values(row)) {
      if (typeof value === 'string') {
        total += value.length
      } else if (value instanceof Uint8Array) {
        total += value.byteLength
      } else {
        total += ESTIMATED_BYTES_PER_PRIMITIVE
      }
    }
    return total
  }

  // ===========================================================================
  // ROW GROUP FLUSHING
  // ===========================================================================

  private async flushRowGroup(): Promise<void> {
    if (this._rows.length === 0) return

    this._pendingFlushes++

    // Capture state before flush for potential rollback
    const rowGroupCountBefore = this._rowGroups.length
    const zoneMapCountBefore = this._zoneMaps.length
    const completedRowGroupsBefore = this._completedRowGroups

    try {
      this.ensureWriter()

      const rowsToFlush = this._rows
      const uncompressedSize = this._cachedBufferSize

      // Convert rows to column data
      const columnData = this.convertRowsToColumns(rowsToFlush)

      // Track row group offset before write
      const offsetBefore = this._writer!.offset

      // Write row group
      this._parquetWriter!.write({
        columnData,
        rowGroupSize: rowsToFlush.length,
      })

      // Update bytes written
      const offsetAfter = this._writer!.offset
      this._bytesWritten = offsetAfter

      // Collect statistics if enabled
      if (this._options.statistics) {
        this.collectStatistics(rowsToFlush)
      }

      // Create row group stats
      const rowGroupStats: RowGroupStats = {
        numRows: rowsToFlush.length,
        fileOffset: offsetBefore,
        compressedSize: offsetAfter - offsetBefore,
        totalUncompressedSize: uncompressedSize,
        columnStats: new Map(this._currentRowGroupStats),
      }
      this._rowGroups.push(rowGroupStats)

      // Create zone map
      if (this._options.statistics) {
        this._zoneMaps.push(this.createZoneMap())
      }

      // Clear buffer
      this._rows = []
      this._cachedBufferSize = 0
      this._currentRowGroupStats.clear()
      this._completedRowGroups++

      // Track buffer pool stats
      if (this._options.enableBufferPooling) {
        this._bufferPoolStats.reused++
      }
    } catch (error) {
      // Rollback any partial state changes on error
      this._rowGroups.length = rowGroupCountBefore
      this._zoneMaps.length = zoneMapCountBefore
      this._completedRowGroups = completedRowGroupsBefore
      this._currentRowGroupStats.clear()
      throw error
    } finally {
      this._pendingFlushes--
    }
  }

  private ensureWriter(): void {
    if (this._writer) return

    // Need a minimal schema for empty file
    if (!this._schema && !this._inferredSchema) {
      this._inferredSchema = { fields: [] }
    }

    this._writer = new ByteWriter()
    this._parquetWriter = new ParquetWriter({
      writer: this._writer,
      schema: this.convertToHyparquetSchema(),
      codec: this._options.compression,
      statistics: this._options.statistics,
      kvMetadata: this._options.kvMetadata,
    })
  }

  // ===========================================================================
  // SCHEMA CONVERSION
  // ===========================================================================

  private convertToHyparquetSchema(): SchemaElement[] {
    const schema = this._schema ?? this._inferredSchema
    if (!schema) {
      throw new ValidationError('No schema available - write at least one row first', 'schema', null)
    }

    const elements: SchemaElement[] = [
      { name: 'root', num_children: schema.fields.length },
    ]

    for (const field of schema.fields) {
      const typeInfo = PARQUET_TYPE_MAP[field.type]
      const element: SchemaElement = {
        name: field.name,
        repetition_type: field.optional ? 'OPTIONAL' : 'REQUIRED',
        type: typeInfo.type,
      }

      if (typeInfo.converted_type) {
        element.converted_type = typeInfo.converted_type
      }

      elements.push(element)
    }

    return elements
  }

  // ===========================================================================
  // DATA CONVERSION
  // ===========================================================================

  private convertRowsToColumns(rows: Row[]): ColumnData[] {
    const schema = this._schema ?? this._inferredSchema
    if (!schema) {
      throw new ValidationError('No schema available - write at least one row first', 'schema', null)
    }

    return schema.fields.map(field => ({
      name: field.name,
      data: rows.map(row => this.convertFieldValue(row[field.name], field)),
    }))
  }

  private convertFieldValue(value: unknown, field: ParquetField): unknown {
    // Handle variant encoding
    if (field.type === 'variant' && value !== null && value !== undefined) {
      const encoded = encodeVariant(value as VariantValue)
      // Store as JSON string for hyparquet-writer
      return JSON.stringify({
        metadata: Array.from(encoded.metadata),
        value: Array.from(encoded.value),
      })
    }

    // Convert dates to microseconds
    if (value instanceof Date) {
      return BigInt(value.getTime()) * 1000n
    }

    return value
  }

  // ===========================================================================
  // STATISTICS COLLECTION
  // ===========================================================================

  private collectStatistics(rows: Row[]): void {
    const schema = this._schema ?? this._inferredSchema
    if (!schema) return

    for (const field of schema.fields) {
      const stats = this.computeFieldStatistics(rows, field)
      this._currentRowGroupStats.set(field.name, stats)
    }
  }

  private computeFieldStatistics(rows: Row[], field: ParquetField): ColumnStats {
    const stats: ColumnStats = {
      min: undefined,
      max: undefined,
      nullCount: 0,
    }

    const values: unknown[] = []

    for (const row of rows) {
      const value = row[field.name]
      if (value === null || value === undefined) {
        stats.nullCount++
      } else {
        values.push(value)
      }
    }

    if (values.length > 0) {
      this.computeMinMax(values, field, stats)

      if (this._options.distinctCountEnabled) {
        stats.distinctCount = this.estimateDistinctCount(values)
      }
    }

    return stats
  }

  private computeMinMax(values: unknown[], field: ParquetField, stats: ColumnStats): void {
    const isNumericType = field.type === 'int32' || field.type === 'int64' ||
                          field.type === 'float' || field.type === 'double'

    if (isNumericType) {
      this.computeNumericMinMax(values, stats)
    } else if (field.type === 'string') {
      this.computeStringMinMax(values, stats)
    }
  }

  private computeNumericMinMax(values: unknown[], stats: ColumnStats): void {
    for (const value of values) {
      if (typeof value === 'number') {
        if (stats.min === undefined || value < (stats.min as number)) {
          stats.min = value
        }
        if (stats.max === undefined || value > (stats.max as number)) {
          stats.max = value
        }
      } else if (typeof value === 'bigint') {
        if (stats.min === undefined || value < (stats.min as bigint)) {
          stats.min = value
        }
        if (stats.max === undefined || value > (stats.max as bigint)) {
          stats.max = value
        }
      } else if (value instanceof Date) {
        const numericValue = value.getTime()
        if (stats.min === undefined || numericValue < (stats.min as number)) {
          stats.min = numericValue
        }
        if (stats.max === undefined || numericValue > (stats.max as number)) {
          stats.max = numericValue
        }
      }
    }
  }

  private computeStringMinMax(values: unknown[], stats: ColumnStats): void {
    for (const value of values) {
      if (typeof value === 'string') {
        if (stats.min === undefined || value < (stats.min as string)) {
          stats.min = value
        }
        if (stats.max === undefined || value > (stats.max as string)) {
          stats.max = value
        }
      }
    }
  }

  private estimateDistinctCount(values: unknown[]): number {
    // Simple distinct count using Set
    // Note: For large datasets, consider HyperLogLog algorithm
    const uniqueValues = new Set<string>()
    for (const v of values) {
      uniqueValues.add(typeof v === 'object' ? JSON.stringify(v) : String(v))
    }
    return uniqueValues.size
  }

  // ===========================================================================
  // ZONE MAPS
  // ===========================================================================

  private createZoneMap(): ZoneMap[] {
    const zoneMap: ZoneMap[] = []

    // Add column statistics
    for (const [column, stats] of this._currentRowGroupStats) {
      zoneMap.push({
        column,
        min: stats.min,
        max: stats.max,
        nullCount: stats.nullCount,
      })
    }

    // Add shredded field zone maps
    if (this._shreddedColumns.length > 0) {
      this.addShreddedZoneMaps(zoneMap)
    }

    return zoneMap
  }

  private addShreddedZoneMaps(zoneMap: ZoneMap[]): void {
    for (const shredPath of this._shreddedColumns) {
      const shredStats = this.extractShredStats(shredPath)
      if (shredStats) {
        zoneMap.push({
          column: shredPath,
          min: shredStats.min,
          max: shredStats.max,
          nullCount: shredStats.nullCount,
        })
      }
    }
  }

  private extractShredStats(path: string): ColumnStats | null {
    if (!path.includes('.')) return null

    const values: unknown[] = []
    let nullCount = 0

    for (const row of this._rows) {
      const value = getNestedValue(row, path)
      if (value !== null && value !== undefined) {
        values.push(value)
      } else {
        nullCount++
      }
    }

    if (values.length === 0) return null

    const stats: ColumnStats = {
      min: undefined,
      max: undefined,
      nullCount,
    }

    for (const value of values) {
      if (typeof value === 'number' || typeof value === 'string') {
        if (stats.min === undefined || stats.min === null || value < stats.min) {
          stats.min = value
        }
        if (stats.max === undefined || stats.max === null || value > stats.max) {
          stats.max = value
        }
      }
    }

    return stats
  }

  // ===========================================================================
  // WRITE RESULT
  // ===========================================================================

  private createWriteResult(buffer: ArrayBuffer): WriteResult {
    const schema = this._schema ?? this._inferredSchema

    return {
      buffer,
      totalRows: this._totalRows,
      rowGroups: this._rowGroups,
      zoneMaps: this._zoneMaps,
      shreddedColumns: this._shreddedColumns,
      metadata: {
        numRows: this._totalRows,
        keyValueMetadata: this._options.kvMetadata,
        compressionCodec: this._options.compression,
      },
      createAsyncBuffer: () => this.createAsyncBufferFromResult(buffer),
      readRows: (asyncBuffer: AsyncBuffer) => this.readRowsFromBuffer(asyncBuffer, schema),
    }
  }

  private async createAsyncBufferFromResult(buffer: ArrayBuffer): Promise<AsyncBuffer> {
    return {
      byteLength: buffer.byteLength,
      slice: (start: number, end?: number) => {
        const endPos = end ?? buffer.byteLength
        // Return ArrayBuffer for hyparquet compatibility
        return buffer.slice(start, endPos)
      },
    }
  }

  private async readRowsFromBuffer(
    asyncBuffer: AsyncBuffer,
    schema: ParquetSchema | null
  ): Promise<Row[]> {
    if (!schema) return []

    // Read parquet file using hyparquet
    // Cast to HyparquetAsyncBuffer since the types are structurally compatible
    const rawRows = await parquetReadObjects({
      file: asyncBuffer as HyparquetAsyncBuffer,
    })

    // Post-process rows to decode variant values and dates
    return rawRows.map(rawRow => this.decodeRow(rawRow, schema))
  }

  private decodeRow(rawRow: Record<string, unknown>, schema: ParquetSchema): Row {
    const row: Row = {}

    for (const field of schema.fields) {
      let value = rawRow[field.name]

      // Decode variant values
      if (field.type === 'variant' && typeof value === 'string') {
        value = this.decodeVariantValue(value)
      } else if (field.type === 'int64' && typeof value === 'bigint') {
        value = this.maybeConvertToDate(value)
      }

      row[field.name] = value
    }

    return row
  }

  private decodeVariantValue(value: string): unknown {
    try {
      const parsed = JSON.parse(value) as { metadata: number[]; value: number[] }
      const encoded = {
        metadata: new Uint8Array(parsed.metadata),
        value: new Uint8Array(parsed.value),
      }
      return decodeVariant(encoded)
    } catch {
      // Keep as string if decode fails
      return value
    }
  }

  private maybeConvertToDate(value: bigint): Date | bigint {
    // Convert microseconds back to milliseconds
    const millis = Number(value / 1000n)
    // If it looks like a reasonable timestamp, convert to Date
    if (millis > 0 && millis < Date.now() * 2) {
      return new Date(millis)
    }
    return value
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Resolve options with defaults
 */
function resolveOptions(options?: StreamingParquetWriterOptions): ResolvedOptions {
  return {
    rowGroupSize: options?.rowGroupSize ?? DEFAULT_ROW_GROUP_SIZE,
    targetRowGroupBytes: options?.targetRowGroupBytes ?? DEFAULT_TARGET_ROW_GROUP_BYTES,
    maxBufferBytes: options?.maxBufferBytes ?? DEFAULT_MAX_BUFFER_BYTES,
    maxPendingFlushes: options?.maxPendingFlushes ?? DEFAULT_MAX_PENDING_FLUSHES,
    enableBufferPooling: options?.enableBufferPooling ?? false,
    schema: options?.schema ?? null,
    statistics: options?.statistics ?? false,
    distinctCountEnabled: options?.distinctCountEnabled ?? false,
    shredFields: options?.shredFields ? [...options.shredFields] : [],
    compression: options?.compression ?? 'SNAPPY',
    kvMetadata: options?.kvMetadata ? [...options.kvMetadata] : [],
  }
}

/**
 * Infer ParquetField type from a JavaScript value
 */
function inferFieldType(value: unknown): ParquetField['type'] {
  if (value === null || value === undefined) return 'string'
  if (typeof value === 'boolean') return 'boolean'
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'int32' : 'double'
  }
  if (typeof value === 'bigint') return 'int64'
  if (typeof value === 'string') return 'string'
  if (value instanceof Uint8Array) return 'binary'
  if (value instanceof Date) return 'int64' // timestamps
  if (Array.isArray(value)) return 'variant'
  if (typeof value === 'object') return 'variant'
  return 'string'
}

/**
 * Check if two types are compatible (allows int32 -> double coercion)
 */
function isTypeCompatible(
  actualType: ParquetField['type'],
  expectedType: ParquetField['type']
): boolean {
  if (actualType === expectedType) return true
  // Allow int32 to be coerced to double
  if (actualType === 'int32' && expectedType === 'double') return true
  return false
}

/**
 * Check if two sorted arrays are equal
 */
function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}
