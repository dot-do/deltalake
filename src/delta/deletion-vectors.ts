/**
 * Delta Lake Deletion Vectors
 *
 * Utilities for working with deletion vectors in Delta Lake tables.
 * Deletion vectors provide soft-delete functionality without rewriting Parquet files.
 *
 * This module provides:
 * - Z85 encoding/decoding (Base85 variant for JSON-safe encoding)
 * - RoaringBitmap parsing (64-bit RoaringTreemap format)
 * - Deletion vector file path resolution
 * - Loading deletion vectors from storage
 *
 * @module delta/deletion-vectors
 */

import type { StorageBackend } from '../storage/index.js'
import type { DeletionVectorDescriptor } from './types.js'
import { ValidationError } from '../errors.js'

// =============================================================================
// Z85 ENCODING/DECODING
// =============================================================================

/**
 * Z85 character set used by Delta Lake for encoding deletion vector UUIDs.
 * Z85 is a Base85 variant that is JSON-friendly (no quotes or backslashes).
 * @see https://rfc.zeromq.org/spec:32/Z85/
 */
export const Z85_CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#'

/**
 * Decode table for Z85: maps character code to value (0-84)
 */
export const Z85_DECODE: number[] = new Array(128).fill(-1)
for (let i = 0; i < Z85_CHARS.length; i++) {
  Z85_DECODE[Z85_CHARS.charCodeAt(i)] = i
}

/**
 * Decode a Z85-encoded string to bytes.
 *
 * Z85 encodes 4 bytes as 5 ASCII characters. The input length must be a multiple of 5.
 *
 * @param encoded - Z85-encoded string
 * @returns Decoded bytes as Uint8Array
 * @throws Error if input length is not a multiple of 5 or contains invalid characters
 */
export function z85Decode(encoded: string): Uint8Array {
  if (encoded.length % 5 !== 0) {
    throw new ValidationError(`Z85 input length must be a multiple of 5, got ${encoded.length}`, 'encoded', encoded.length)
  }

  const outputLen = (encoded.length / 5) * 4
  const result = new Uint8Array(outputLen)
  let outIdx = 0

  for (let i = 0; i < encoded.length; i += 5) {
    // Decode 5 chars to a 32-bit value
    let value = 0
    for (let j = 0; j < 5; j++) {
      const charCode = encoded.charCodeAt(i + j)
      const decoded = charCode < 128 ? Z85_DECODE[charCode] : -1
      if (decoded === undefined || decoded < 0) {
        throw new ValidationError(`Invalid Z85 character at position ${i + j}`, 'encoded', encoded.charAt(i + j))
      }
      value = value * 85 + decoded
    }

    // Write 4 bytes (big-endian)
    result[outIdx++] = (value >>> 24) & 0xff
    result[outIdx++] = (value >>> 16) & 0xff
    result[outIdx++] = (value >>> 8) & 0xff
    result[outIdx++] = value & 0xff
  }

  return result
}

/**
 * Convert a Z85-encoded UUID string to a UUID string format.
 *
 * Delta Lake uses Z85 to encode 16-byte UUIDs as 20-character strings.
 * The pathOrInlineDv field may have an optional prefix before the UUID.
 *
 * @param pathOrInlineDv - The pathOrInlineDv field from a deletion vector descriptor
 * @returns The UUID in standard format (8-4-4-4-12 hex)
 */
export function z85DecodeUuid(pathOrInlineDv: string): string {
  // The last 20 characters are the Z85-encoded UUID
  // Any characters before that are an optional random prefix
  const uuidEncoded = pathOrInlineDv.slice(-20)
  const prefix = pathOrInlineDv.slice(0, -20)

  const bytes = z85Decode(uuidEncoded)

  // Convert 16 bytes to UUID format
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
  const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`

  return prefix ? `${prefix}${uuid}` : uuid
}

// =============================================================================
// DELETION VECTOR PATH UTILITIES
// =============================================================================

/**
 * Get the file path for a deletion vector based on its descriptor.
 *
 * @param tablePath - Base path of the Delta table
 * @param dv - Deletion vector descriptor
 * @returns Full path to the deletion vector file
 */
export function getDeletionVectorPath(tablePath: string, dv: DeletionVectorDescriptor): string {
  if (dv.storageType === 'p') {
    // Absolute path
    return dv.pathOrInlineDv
  } else if (dv.storageType === 'u') {
    // UUID-based relative path
    const uuid = z85DecodeUuid(dv.pathOrInlineDv)
    // The prefix from the pathOrInlineDv becomes part of the filename
    const filename = `deletion_vector_${uuid}.bin`
    return tablePath ? `${tablePath}/${filename}` : filename
  } else {
    // Inline - no file path
    throw new ValidationError('Inline deletion vectors do not have a file path', 'storageType', dv.storageType)
  }
}

// =============================================================================
// ROARING BITMAP PARSING
// =============================================================================

/**
 * Roaring Bitmap serialization format constants.
 * @see https://github.com/RoaringBitmap/RoaringFormatSpec
 */
const SERIAL_COOKIE_NO_RUNCONTAINER = 12346
const SERIAL_COOKIE = 12347

/**
 * Parse a serialized RoaringBitmap (or RoaringTreemap for 64-bit) and return the set of deleted row indices.
 *
 * Delta Lake uses RoaringTreemap (64-bit extension) for deletion vectors.
 * The format is:
 * - Number of 32-bit buckets (as uint64, little-endian)
 * - For each bucket:
 *   - High 32 bits key (as uint32, little-endian)
 *   - Serialized 32-bit RoaringBitmap for low 32 bits
 *
 * @param data - Raw bytes of the serialized deletion vector (after magic/size/checksum header)
 * @returns Set of row indices that are marked as deleted
 */
export function parseRoaringBitmap(data: Uint8Array): Set<number> {
  const deletedRows = new Set<number>()

  // Handle empty or insufficient data
  if (!data || data.byteLength < 4) {
    return deletedRows
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = 0

  // Delta Lake uses a magic number at the start: 0x64 0x01 0x00 0x00 (little-endian: 0x164 = 356 for DeletionVectorStorageType)
  // Actually, looking at the data: the magic number is 4 bytes at the start
  // Format: magic (4 bytes) + roaring treemap data
  const magic = view.getUint32(offset, true)
  offset += 4

  // Magic number 0x00000164 (356) indicates RoaringTreemap format
  // Magic number 0x3a300000 would be something else
  if (magic !== 0x64 && magic !== 0x3a300000 && magic !== 0x303a) {
    // Try parsing without magic - might be a different format
    offset = 0
  }

  // For RoaringTreemap: first 8 bytes are the number of buckets
  // Need at least 8 bytes for the bucket count
  if (offset + 8 > data.byteLength) {
    return deletedRows
  }
  const numBuckets = Number(view.getBigUint64(offset, true))
  offset += 8

  // Sanity check: avoid processing unreasonable number of buckets
  // which could indicate malformed data
  const maxReasonableBuckets = 1000000
  if (numBuckets > maxReasonableBuckets || numBuckets < 0) {
    return deletedRows
  }

  for (let bucket = 0; bucket < numBuckets; bucket++) {
    // Need at least 4 bytes for the high bits key
    if (offset + 4 > data.byteLength) {
      break
    }

    // High 32 bits key
    const highBits = view.getUint32(offset, true)
    offset += 4

    // Parse the 32-bit RoaringBitmap for this bucket
    const remaining = data.subarray(offset)
    if (remaining.byteLength === 0) {
      break
    }
    const { values, bytesRead } = parseRoaringBitmap32(remaining)
    offset += bytesRead

    // Combine high bits with low bits to get full 64-bit row index
    // For most cases, highBits will be 0 (first 4 billion rows)
    const highOffset = highBits * 0x100000000
    for (const lowBits of values) {
      deletedRows.add(highOffset + lowBits)
    }
  }

  return deletedRows
}

/**
 * Parse a 32-bit RoaringBitmap from serialized data.
 *
 * @param data - Raw bytes starting at the RoaringBitmap
 * @returns Object with parsed values and bytes consumed
 */
export function parseRoaringBitmap32(data: Uint8Array): { values: number[]; bytesRead: number } {
  const values: number[] = []

  // Handle empty or insufficient data (need at least 4 bytes for cookie)
  if (!data || data.byteLength < 4) {
    return { values: [], bytesRead: 0 }
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = 0

  // Read cookie (first 4 bytes)
  const cookie = view.getUint32(offset, true)
  offset += 4

  let numContainers: number
  let hasRunContainers = false
  let runContainerBitset: Uint8Array | null = null

  if ((cookie & 0xffff) === SERIAL_COOKIE) {
    // Has potential run containers
    hasRunContainers = true
    numContainers = ((cookie >>> 16) & 0xffff) + 1

    // Read run container bitset
    const bitsetBytes = Math.ceil(numContainers / 8)
    // Bounds check before reading bitset
    if (offset + bitsetBytes > data.byteLength) {
      return { values: [], bytesRead: offset }
    }
    runContainerBitset = data.subarray(offset, offset + bitsetBytes)
    offset += bitsetBytes
  } else if (cookie === SERIAL_COOKIE_NO_RUNCONTAINER) {
    // No run containers - need 4 more bytes for container count
    if (offset + 4 > data.byteLength) {
      return { values: [], bytesRead: offset }
    }
    numContainers = view.getUint32(offset, true)
    offset += 4
  } else {
    // Unknown format - might be empty or different encoding
    return { values: [], bytesRead: offset }
  }

  if (numContainers === 0) {
    return { values: [], bytesRead: offset }
  }

  // Sanity check: limit containers to prevent memory issues
  const maxContainers = 65536 // Max possible with 16-bit keys
  if (numContainers > maxContainers) {
    return { values: [], bytesRead: offset }
  }

  // Read descriptive header: for each container, 16-bit key and 16-bit (cardinality - 1)
  const keys: number[] = []
  const cardinalities: number[] = []

  // Need 4 bytes per container for the header
  const headerSize = numContainers * 4
  if (offset + headerSize > data.byteLength) {
    return { values: [], bytesRead: offset }
  }

  for (let i = 0; i < numContainers; i++) {
    keys.push(view.getUint16(offset, true))
    offset += 2
    cardinalities.push(view.getUint16(offset, true) + 1)
    offset += 2
  }

  // Read offset header if needed (only for SERIAL_COOKIE_NO_RUNCONTAINER or if containers >= 4)
  const hasOffsets = cookie === SERIAL_COOKIE_NO_RUNCONTAINER || numContainers >= 4
  if (hasOffsets) {
    // Need 4 bytes per container for offsets
    const offsetsSize = numContainers * 4
    if (offset + offsetsSize > data.byteLength) {
      return { values: [], bytesRead: offset }
    }
    // Skip reading offsets into array since we don't use containerOffsets
    offset += offsetsSize
  }

  // Read containers
  for (let i = 0; i < numContainers; i++) {
    const key = keys[i]
    if (key === undefined) continue
    const cardinality = cardinalities[i]
    if (cardinality === undefined) continue
    const highBits = key << 16 // key represents the high 16 bits

    // Determine container type
    const bitsetByteIndex = Math.floor(i / 8)
    const isRunContainer = hasRunContainers && runContainerBitset && bitsetByteIndex < runContainerBitset.length && (runContainerBitset[bitsetByteIndex]! & (1 << (i % 8))) !== 0
    const isBitsetContainer = !isRunContainer && cardinality > 4096

    if (isRunContainer) {
      // Run container: number of runs, then pairs of (start, length-1)
      // Need at least 2 bytes for numRuns
      if (offset + 2 > data.byteLength) {
        break
      }
      const numRuns = view.getUint16(offset, true)
      offset += 2

      // Sanity check numRuns (max 2048 runs for 65536 values)
      if (numRuns > 2048) {
        break
      }

      // Need 4 bytes per run
      if (offset + numRuns * 4 > data.byteLength) {
        break
      }

      for (let r = 0; r < numRuns; r++) {
        const start = view.getUint16(offset, true)
        offset += 2
        const length = view.getUint16(offset, true) + 1
        offset += 2

        // Bounds check: start + length should not exceed 65536
        const safeLength = Math.min(length, 65536 - start)
        for (let v = 0; v < safeLength; v++) {
          values.push(highBits | (start + v))
        }
      }
    } else if (isBitsetContainer) {
      // Bitset container: 8KB = 65536 bits = 1024 * 64-bit words
      const bitsetSize = 1024 * 8 // 8KB
      if (offset + bitsetSize > data.byteLength) {
        break
      }

      for (let wordIdx = 0; wordIdx < 1024; wordIdx++) {
        // Use BigInt for 64-bit word to handle all 64 bits correctly
        // JavaScript bitwise ops (<<, &) only work on 32-bit signed integers
        const word = view.getBigUint64(offset, true)
        offset += 8

        // Skip empty words for efficiency
        if (word === 0n) continue

        // Check each bit using BigInt operations
        for (let bit = 0; bit < 64; bit++) {
          if ((word & (1n << BigInt(bit))) !== 0n) {
            values.push(highBits | (wordIdx * 64 + bit))
          }
        }
      }
    } else {
      // Array container: sorted 16-bit values
      // Need 2 bytes per value
      const containerSize = cardinality * 2
      if (offset + containerSize > data.byteLength) {
        break
      }

      for (let j = 0; j < cardinality; j++) {
        const lowBits = view.getUint16(offset, true)
        offset += 2
        values.push(highBits | lowBits)
      }
    }
  }

  return { values, bytesRead: offset }
}

// =============================================================================
// DELETION VECTOR LOADING
// =============================================================================

/**
 * Load and parse a deletion vector from storage.
 *
 * @param storage - Storage backend
 * @param tablePath - Base path of the Delta table
 * @param dv - Deletion vector descriptor
 * @returns Set of row indices that are marked as deleted
 */
export async function loadDeletionVector(
  storage: StorageBackend,
  tablePath: string,
  dv: DeletionVectorDescriptor
): Promise<Set<number>> {
  if (dv.storageType === 'i') {
    // Inline: decode Base85 data directly
    const decoded = z85Decode(dv.pathOrInlineDv)
    return parseRoaringBitmap(decoded)
  }

  // Load from file
  const dvPath = getDeletionVectorPath(tablePath, dv)
  const fileData = await storage.read(dvPath)

  // The deletion vector file format is:
  // - [startOffset bytes to skip (from dv.offset)]
  // - 4 bytes: size (little-endian uint32) - this is redundant with sizeInBytes
  // - 4 bytes: checksum
  // - The actual RoaringTreemap data
  //
  // sizeInBytes includes only the RoaringTreemap data (not size/checksum)
  // so we need to skip the 8-byte header
  const startOffset = dv.offset ?? 0
  const headerSize = 8

  // Skip the header (size + checksum) to get to the actual bitmap data
  const dvData = fileData.subarray(startOffset + headerSize)

  return parseRoaringBitmap(dvData)
}
