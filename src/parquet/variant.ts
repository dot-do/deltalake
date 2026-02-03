/**
 * Parquet VARIANT binary encoder
 *
 * Implements the Variant encoding spec:
 * https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 *
 * A Variant consists of:
 * - metadata: binary field containing the string dictionary
 * - value: binary field containing the self-describing encoded value
 */

import { ValidationError } from '../errors.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

// =============================================================================
// TYPES
// =============================================================================

export type VariantValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Date
  | Uint8Array
  | VariantValue[]
  | { [key: string]: VariantValue }

export interface EncodedVariant {
  /** String dictionary for object keys */
  metadata: Uint8Array
  /** Self-describing encoded value */
  value: Uint8Array
}

// =============================================================================
// ENCODING
// =============================================================================

/**
 * Encode a JavaScript value as a Parquet VARIANT.
 *
 * VARIANT is a semi-structured data type that can store any JSON-compatible value
 * in a self-describing binary format. This enables storing heterogeneous data
 * (objects with varying schemas) efficiently in Parquet files.
 *
 * Supported types:
 * - Primitives: null, boolean, number, bigint, string
 * - Dates: Date objects (stored as timestamp with microsecond precision)
 * - Binary: Uint8Array
 * - Collections: Arrays and objects (nested structures supported)
 *
 * @param value - The JavaScript value to encode
 * @returns Encoded variant with metadata (string dictionary) and value bytes
 *
 * @example
 * ```typescript
 * // Encode a simple object
 * const encoded = encodeVariant({
 *   name: 'Alice',
 *   age: 30,
 *   active: true
 * })
 * // encoded.metadata contains the string dictionary (keys: 'name', 'age', 'active')
 * // encoded.value contains the self-describing binary representation
 *
 * // Encode nested structures
 * const nested = encodeVariant({
 *   user: { name: 'Bob', scores: [95, 87, 92] },
 *   timestamp: new Date()
 * })
 *
 * // Decode back to JavaScript
 * const decoded = decodeVariant(encoded) // { name: 'Alice', age: 30, active: true }
 * ```
 */
export function encodeVariant(value: VariantValue): EncodedVariant {
  // Build string dictionary from all object keys
  const dictionary: string[] = []
  const dictIndex = new Map<string, number>()
  collectStrings(value, dictionary, dictIndex)

  // Encode metadata (dictionary)
  const metadata = encodeMetadata(dictionary)

  // Encode value
  const encodedValue = encodeValue(value, dictIndex)

  return { metadata, value: encodedValue }
}

/**
 * Recursively collect all object keys into the dictionary.
 */
function collectStrings(
  value: VariantValue,
  dictionary: string[],
  dictIndex: Map<string, number>
): void {
  if (value === null || value === undefined) return

  if (Array.isArray(value)) {
    for (const item of value) {
      collectStrings(item, dictionary, dictIndex)
    }
  } else if (value instanceof Date || value instanceof Uint8Array) {
    // Skip dates and binary
  } else if (typeof value === 'object') {
    for (const key of Object.keys(value)) {
      if (!dictIndex.has(key)) {
        dictIndex.set(key, dictionary.length)
        dictionary.push(key)
      }
      const childValue = (value as Record<string, VariantValue>)[key]
      if (childValue !== undefined) {
        collectStrings(childValue, dictionary, dictIndex)
      }
    }
  }
}

/**
 * Encode the metadata (string dictionary).
 */
function encodeMetadata(dictionary: string[]): Uint8Array {
  if (dictionary.length === 0) {
    return new Uint8Array([0x01, 0x00])
  }

  const encodedStrings = dictionary.map(s => encoder.encode(s))
  const totalStringBytes = encodedStrings.reduce((sum, s) => sum + s.length, 0)

  const offsetSize = totalStringBytes <= 255 ? 1 : totalStringBytes <= 65535 ? 2 : 4

  const headerSize = 1
  const dictSizeSize = offsetSize
  const offsetsSize = (dictionary.length + 1) * offsetSize
  const totalSize = headerSize + dictSizeSize + offsetsSize + totalStringBytes

  const buffer = new Uint8Array(totalSize)
  let pos = 0

  const header = 0x01 | ((offsetSize - 1) << 6)
  buffer[pos++] = header

  writeUnsigned(buffer, pos, dictionary.length, offsetSize)
  pos += offsetSize

  let stringOffset = 0
  for (let i = 0; i <= dictionary.length; i++) {
    writeUnsigned(buffer, pos, stringOffset, offsetSize)
    pos += offsetSize
    if (i < dictionary.length) {
      const encoded = encodedStrings[i]
      if (encoded !== undefined) {
        stringOffset += encoded.length
      }
    }
  }

  for (const encoded of encodedStrings) {
    buffer.set(encoded, pos)
    pos += encoded.length
  }

  return buffer
}

/**
 * Encode a value using the Variant binary format.
 */
function encodeValue(value: VariantValue, dictIndex: Map<string, number>): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array([0x00])
  }

  if (typeof value === 'boolean') {
    return new Uint8Array([value ? 0x04 : 0x08])
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return encodeInteger(value)
    } else {
      return encodeDouble(value)
    }
  }

  if (typeof value === 'bigint') {
    return encodeBigInt(value)
  }

  if (typeof value === 'string') {
    return encodeString(value)
  }

  if (value instanceof Date) {
    return encodeTimestamp(value)
  }

  if (value instanceof Uint8Array) {
    return encodeBinary(value)
  }

  if (Array.isArray(value)) {
    return encodeArray(value, dictIndex)
  }

  if (typeof value === 'object') {
    return encodeObject(value as Record<string, VariantValue>, dictIndex)
  }

  return encodeString(String(value))
}

function encodeInteger(value: number): Uint8Array {
  if (value >= -128 && value <= 127) {
    const buf = new Uint8Array(2)
    buf[0] = 0x0C
    buf[1] = value & 0xFF
    return buf
  }
  if (value >= -32768 && value <= 32767) {
    const buf = new Uint8Array(3)
    buf[0] = 0x10
    buf[1] = value & 0xFF
    buf[2] = (value >> 8) & 0xFF
    return buf
  }
  if (value >= -2147483648 && value <= 2147483647) {
    const buf = new Uint8Array(5)
    buf[0] = 0x14
    const view = new DataView(buf.buffer)
    view.setInt32(1, value, true)
    return buf
  }
  return encodeBigInt(BigInt(value))
}

function encodeBigInt(value: bigint): Uint8Array {
  const buf = new Uint8Array(9)
  buf[0] = 0x18
  const view = new DataView(buf.buffer)
  view.setBigInt64(1, value, true)
  return buf
}

function encodeDouble(value: number): Uint8Array {
  const buf = new Uint8Array(9)
  buf[0] = 0x1C
  const view = new DataView(buf.buffer)
  view.setFloat64(1, value, true)
  return buf
}

function encodeString(value: string): Uint8Array {
  const encoded = encoder.encode(value)

  if (encoded.length < 64) {
    const buf = new Uint8Array(1 + encoded.length)
    buf[0] = 0x01 | (encoded.length << 2)
    buf.set(encoded, 1)
    return buf
  }

  const buf = new Uint8Array(5 + encoded.length)
  buf[0] = 0x40
  const view = new DataView(buf.buffer)
  view.setUint32(1, encoded.length, true)
  buf.set(encoded, 5)
  return buf
}

function encodeBinary(value: Uint8Array): Uint8Array {
  const buf = new Uint8Array(5 + value.length)
  buf[0] = 0x3C // basic_type=0, type_id=15 (binary per Variant spec)
  const view = new DataView(buf.buffer)
  view.setUint32(1, value.length, true)
  buf.set(value, 5)
  return buf
}

function encodeTimestamp(value: Date): Uint8Array {
  const micros = BigInt(value.getTime()) * 1000n
  const buf = new Uint8Array(9)
  buf[0] = 0x30
  const view = new DataView(buf.buffer)
  view.setBigInt64(1, micros, true)
  return buf
}

function encodeArray(value: VariantValue[], dictIndex: Map<string, number>): Uint8Array {
  const numElements = value.length
  const isLarge = numElements > 255

  const encodedElements = value.map(v => encodeValue(v, dictIndex))
  const totalValueBytes = encodedElements.reduce((sum, e) => sum + e.length, 0)

  const offsetSize = totalValueBytes <= 255 ? 1 : totalValueBytes <= 65535 ? 2 : 4

  const headerSize = 1
  const numElementsSize = isLarge ? 4 : 1
  const offsetsSize = (numElements + 1) * offsetSize
  const totalSize = headerSize + numElementsSize + offsetsSize + totalValueBytes

  const buf = new Uint8Array(totalSize)
  let pos = 0

  const header = 0x03 | ((offsetSize - 1) << 2) | (isLarge ? 0x10 : 0)
  buf[pos++] = header

  if (isLarge) {
    const view = new DataView(buf.buffer)
    view.setUint32(pos, numElements, true)
    pos += 4
  } else {
    buf[pos++] = numElements
  }

  let offset = 0
  for (let i = 0; i <= numElements; i++) {
    writeUnsigned(buf, pos, offset, offsetSize)
    pos += offsetSize
    if (i < numElements) {
      const elem = encodedElements[i]
      if (elem !== undefined) {
        offset += elem.length
      }
    }
  }

  for (const encoded of encodedElements) {
    buf.set(encoded, pos)
    pos += encoded.length
  }

  return buf
}

function encodeObject(value: Record<string, VariantValue>, dictIndex: Map<string, number>): Uint8Array {
  const keys = Object.keys(value)
  const sortedKeys = [...keys].sort((a, b) => {
    const aName = dictIndex.get(a) ?? 0
    const bName = dictIndex.get(b) ?? 0
    return aName - bName
  })

  const numElements = sortedKeys.length
  const isLarge = numElements > 255

  const encodedValues = sortedKeys.map(k => {
    const val = value[k]
    return encodeValue(val !== undefined ? val : null, dictIndex)
  })
  const totalValueBytes = encodedValues.reduce((sum, e) => sum + e.length, 0)

  const maxFieldId = Math.max(...sortedKeys.map(k => dictIndex.get(k) ?? 0), 0)
  const idSize = maxFieldId <= 255 ? 1 : maxFieldId <= 65535 ? 2 : 4
  const offsetSize = totalValueBytes <= 255 ? 1 : totalValueBytes <= 65535 ? 2 : 4

  const headerSize = 1
  const numElementsSize = isLarge ? 4 : 1
  const fieldIdsSize = numElements * idSize
  const offsetsSize = (numElements + 1) * offsetSize
  const totalSize = headerSize + numElementsSize + fieldIdsSize + offsetsSize + totalValueBytes

  const buf = new Uint8Array(totalSize)
  let pos = 0

  const header = 0x02 | ((offsetSize - 1) << 2) | ((idSize - 1) << 4) | (isLarge ? 0x40 : 0)
  buf[pos++] = header

  if (isLarge) {
    const view = new DataView(buf.buffer)
    view.setUint32(pos, numElements, true)
    pos += 4
  } else {
    buf[pos++] = numElements
  }

  for (const key of sortedKeys) {
    const id = dictIndex.get(key) ?? 0
    writeUnsigned(buf, pos, id, idSize)
    pos += idSize
  }

  let offset = 0
  for (let i = 0; i <= numElements; i++) {
    writeUnsigned(buf, pos, offset, offsetSize)
    pos += offsetSize
    if (i < numElements) {
      const val = encodedValues[i]
      if (val !== undefined) {
        offset += val.length
      }
    }
  }

  for (const encoded of encodedValues) {
    buf.set(encoded, pos)
    pos += encoded.length
  }

  return buf
}

function writeUnsigned(buf: Uint8Array, pos: number, value: number, byteWidth: number): void {
  for (let i = 0; i < byteWidth; i++) {
    buf[pos + i] = (value >> (i * 8)) & 0xFF
  }
}

// =============================================================================
// DECODING
// =============================================================================

/**
 * Decode a Parquet VARIANT back to JavaScript value.
 *
 * Reconstructs the original JavaScript value from the encoded VARIANT format.
 * The string dictionary in metadata is used to restore object key names.
 *
 * @param encoded - The encoded variant (metadata and value bytes)
 * @returns The decoded JavaScript value
 *
 * @example
 * ```typescript
 * // Encode and decode roundtrip
 * const original = { name: 'Alice', tags: ['admin', 'active'] }
 * const encoded = encodeVariant(original)
 * const decoded = decodeVariant(encoded)
 * // decoded === { name: 'Alice', tags: ['admin', 'active'] }
 *
 * // Handles all supported types
 * const complex = encodeVariant({
 *   id: 12345n,                    // bigint
 *   created: new Date(),           // Date
 *   data: new Uint8Array([1,2,3]), // binary
 *   metadata: { version: 1 }       // nested object
 * })
 * const result = decodeVariant(complex)
 * ```
 */
export function decodeVariant(encoded: EncodedVariant): VariantValue {
  const dictionary = decodeMetadata(encoded.metadata)
  return decodeValue(encoded.value, 0, dictionary).value
}

function decodeMetadata(metadata: Uint8Array): string[] {
  if (metadata.length < 2) return []

  const header = metadata[0]
  if (header === undefined) return []
  const offsetSize = ((header >> 6) & 0x03) + 1
  let pos = 1

  const dictSize = readUnsigned(metadata, pos, offsetSize)
  pos += offsetSize

  if (dictSize === 0) return []

  const offsets: number[] = []
  for (let i = 0; i <= dictSize; i++) {
    offsets.push(readUnsigned(metadata, pos, offsetSize))
    pos += offsetSize
  }

  const dictionary: string[] = []
  for (let i = 0; i < dictSize; i++) {
    const startOffset = offsets[i]
    const endOffset = offsets[i + 1]
    if (startOffset !== undefined && endOffset !== undefined) {
      const start = pos + startOffset
      const end = pos + endOffset
      dictionary.push(decoder.decode(metadata.slice(start, end)))
    }
  }

  return dictionary
}

function decodeValue(
  data: Uint8Array,
  pos: number,
  dictionary: string[]
): { value: VariantValue; bytesRead: number } {
  const header = data[pos]
  if (header === undefined) {
    return { value: null, bytesRead: 1 }
  }
  const basicType = header & 0x03

  if (basicType === 0) {
    // Primitive
    const typeId = (header >> 2) & 0x3F
    return decodePrimitive(data, pos, typeId)
  }

  if (basicType === 1) {
    // Short string
    const length = (header >> 2) & 0x3F
    const str = decoder.decode(data.slice(pos + 1, pos + 1 + length))
    return { value: str, bytesRead: 1 + length }
  }

  if (basicType === 2) {
    // Object
    return decodeObject(data, pos, dictionary)
  }

  if (basicType === 3) {
    // Array
    return decodeArray(data, pos, dictionary)
  }

  return { value: null, bytesRead: 1 }
}

function decodePrimitive(
  data: Uint8Array,
  pos: number,
  typeId: number
): { value: VariantValue; bytesRead: number } {
  const view = new DataView(data.buffer, data.byteOffset)

  switch (typeId) {
    case 0: // null
      return { value: null, bytesRead: 1 }
    case 1: // true
      return { value: true, bytesRead: 1 }
    case 2: // false
      return { value: false, bytesRead: 1 }
    case 3: // INT8
      if (pos + 2 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for INT8', 'data', { pos, length: data.length })
      }
      return { value: ((data[pos + 1]!) << 24) >> 24, bytesRead: 2 }
    case 4: // INT16
      if (pos + 3 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for INT16', 'data', { pos, length: data.length })
      }
      return { value: view.getInt16(pos + 1, true), bytesRead: 3 }
    case 5: // INT32
      if (pos + 5 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for INT32', 'data', { pos, length: data.length })
      }
      return { value: view.getInt32(pos + 1, true), bytesRead: 5 }
    case 6: // INT64
      if (pos + 9 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for INT64', 'data', { pos, length: data.length })
      }
      return { value: view.getBigInt64(pos + 1, true), bytesRead: 9 }
    case 7: // DOUBLE
      if (pos + 9 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for DOUBLE', 'data', { pos, length: data.length })
      }
      return { value: view.getFloat64(pos + 1, true), bytesRead: 9 }
    case 12: // TIMESTAMP_MICROS
      if (pos + 9 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for TIMESTAMP_MICROS', 'data', { pos, length: data.length })
      }
      const micros = view.getBigInt64(pos + 1, true)
      return { value: new Date(Number(micros / 1000n)), bytesRead: 9 }
    case 15: // BINARY (per Variant spec)
      if (pos + 5 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for BINARY length', 'data', { pos, length: data.length })
      }
      const binLen = view.getUint32(pos + 1, true)
      if (pos + 5 + binLen > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for BINARY data', 'data', { pos, length: data.length, binLen })
      }
      return { value: data.slice(pos + 5, pos + 5 + binLen), bytesRead: 5 + binLen }
    case 16: // LONG_STRING
      if (pos + 5 > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for LONG_STRING length', 'data', { pos, length: data.length })
      }
      const strLen = view.getUint32(pos + 1, true)
      if (pos + 5 + strLen > data.length) {
        throw new ValidationError('Malformed variant: insufficient bytes for LONG_STRING data', 'data', { pos, length: data.length, strLen })
      }
      const str = decoder.decode(data.slice(pos + 5, pos + 5 + strLen))
      return { value: str, bytesRead: 5 + strLen }
    default:
      return { value: null, bytesRead: 1 }
  }
}

function decodeObject(
  data: Uint8Array,
  pos: number,
  dictionary: string[]
): { value: VariantValue; bytesRead: number } {
  const header = data[pos]
  if (header === undefined) {
    return { value: null, bytesRead: 1 }
  }
  const offsetSize = ((header >> 2) & 0x03) + 1
  const idSize = ((header >> 4) & 0x03) + 1
  const isLarge = (header & 0x40) !== 0

  let p = pos + 1
  const view = new DataView(data.buffer, data.byteOffset)

  const numElements = isLarge ? view.getUint32(p, true) : (data[p] ?? 0)
  p += isLarge ? 4 : 1

  const fieldIds: number[] = []
  for (let i = 0; i < numElements; i++) {
    fieldIds.push(readUnsigned(data, p, idSize))
    p += idSize
  }

  const offsets: number[] = []
  for (let i = 0; i <= numElements; i++) {
    offsets.push(readUnsigned(data, p, offsetSize))
    p += offsetSize
  }

  const valueStart = p
  const obj: Record<string, VariantValue> = {}

  for (let i = 0; i < numElements; i++) {
    const fieldId = fieldIds[i]
    const offset = offsets[i]
    if (fieldId !== undefined && offset !== undefined) {
      const key = dictionary[fieldId] ?? `__field_${fieldId}`
      const { value } = decodeValue(data, valueStart + offset, dictionary)
      obj[key] = value
    }
  }

  const lastOffset = offsets[numElements]
  return { value: obj, bytesRead: valueStart + (lastOffset ?? 0) - pos }
}

function decodeArray(
  data: Uint8Array,
  pos: number,
  dictionary: string[]
): { value: VariantValue; bytesRead: number } {
  const header = data[pos]
  if (header === undefined) {
    return { value: [], bytesRead: 1 }
  }
  const offsetSize = ((header >> 2) & 0x03) + 1
  const isLarge = (header & 0x10) !== 0

  let p = pos + 1
  const view = new DataView(data.buffer, data.byteOffset)

  const numElements = isLarge ? view.getUint32(p, true) : (data[p] ?? 0)
  p += isLarge ? 4 : 1

  const offsets: number[] = []
  for (let i = 0; i <= numElements; i++) {
    offsets.push(readUnsigned(data, p, offsetSize))
    p += offsetSize
  }

  const valueStart = p
  const arr: VariantValue[] = []

  for (let i = 0; i < numElements; i++) {
    const offset = offsets[i]
    if (offset !== undefined) {
      const { value } = decodeValue(data, valueStart + offset, dictionary)
      arr.push(value)
    }
  }

  const lastOffset = offsets[numElements]
  return { value: arr, bytesRead: valueStart + (lastOffset ?? 0) - pos }
}

function readUnsigned(data: Uint8Array, pos: number, byteWidth: number): number {
  let value = 0
  for (let i = 0; i < byteWidth; i++) {
    value |= (data[pos + i] ?? 0) << (i * 8)
  }
  return value >>> 0
}
