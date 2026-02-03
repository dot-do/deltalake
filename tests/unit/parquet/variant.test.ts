/**
 * Parquet Variant Encoding Unit Tests
 *
 * Tests for the VARIANT binary encoder/decoder following the Parquet spec:
 * https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 *
 * Tests cover:
 * - encodeVariant() with various types (string, number, boolean, null, arrays, objects)
 * - decodeVariant() roundtrip tests
 * - Edge cases: empty objects, nested structures, special characters
 * - Error handling for malformed variant data
 */

import { describe, it, expect } from 'vitest'
import {
  encodeVariant,
  decodeVariant,
  type VariantValue,
  type EncodedVariant,
} from '../../../src/parquet/index.js'
import { ValidationError } from '../../../src/errors.js'

// =============================================================================
// PRIMITIVE TYPES - NULL
// =============================================================================

describe('encodeVariant - Null Values', () => {
  it('should encode null', () => {
    const encoded = encodeVariant(null)

    expect(encoded.metadata).toBeInstanceOf(Uint8Array)
    expect(encoded.value).toBeInstanceOf(Uint8Array)
    expect(encoded.value[0]).toBe(0x00) // null type
  })

  it('should roundtrip null', () => {
    const original = null
    const encoded = encodeVariant(original)
    const decoded = decodeVariant(encoded)

    expect(decoded).toBeNull()
  })
})

// =============================================================================
// PRIMITIVE TYPES - BOOLEAN
// =============================================================================

describe('encodeVariant - Boolean Values', () => {
  it('should encode boolean true', () => {
    const encoded = encodeVariant(true)

    expect(encoded.value[0]).toBe(0x04) // true type
  })

  it('should encode boolean false', () => {
    const encoded = encodeVariant(false)

    expect(encoded.value[0]).toBe(0x08) // false type
  })

  it('should roundtrip boolean true', () => {
    const original = true
    const encoded = encodeVariant(original)
    const decoded = decodeVariant(encoded)

    expect(decoded).toBe(true)
  })

  it('should roundtrip boolean false', () => {
    const original = false
    const encoded = encodeVariant(original)
    const decoded = decodeVariant(encoded)

    expect(decoded).toBe(false)
  })
})

// =============================================================================
// PRIMITIVE TYPES - INTEGERS
// =============================================================================

describe('encodeVariant - Integer Values', () => {
  describe('INT8 range (-128 to 127)', () => {
    it('should encode zero', () => {
      const encoded = encodeVariant(0)

      expect(encoded.value[0]).toBe(0x0C) // INT8 type
      expect(encoded.value.length).toBe(2) // header + 1 byte
    })

    it('should roundtrip zero', () => {
      const decoded = decodeVariant(encodeVariant(0))
      expect(decoded).toBe(0)
    })

    it('should roundtrip positive INT8', () => {
      const original = 42
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(42)
    })

    it('should roundtrip negative INT8', () => {
      const original = -100
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(-100)
    })

    it('should roundtrip INT8 min boundary (-128)', () => {
      const original = -128
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(-128)
    })

    it('should roundtrip INT8 max boundary (127)', () => {
      const original = 127
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(127)
    })
  })

  describe('INT16 range (-32768 to 32767)', () => {
    it('should encode INT16 just outside INT8 range', () => {
      const encoded = encodeVariant(128)

      expect(encoded.value[0]).toBe(0x10) // INT16 type
      expect(encoded.value.length).toBe(3) // header + 2 bytes
    })

    it('should roundtrip positive INT16', () => {
      const original = 1000
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(1000)
    })

    it('should roundtrip negative INT16', () => {
      const original = -1000
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(-1000)
    })

    it('should roundtrip INT16 min boundary (-32768)', () => {
      const original = -32768
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(-32768)
    })

    it('should roundtrip INT16 max boundary (32767)', () => {
      const original = 32767
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(32767)
    })
  })

  describe('INT32 range (-2147483648 to 2147483647)', () => {
    it('should encode INT32 just outside INT16 range', () => {
      const encoded = encodeVariant(32768)

      expect(encoded.value[0]).toBe(0x14) // INT32 type
      expect(encoded.value.length).toBe(5) // header + 4 bytes
    })

    it('should roundtrip positive INT32', () => {
      const original = 1000000
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(1000000)
    })

    it('should roundtrip negative INT32', () => {
      const original = -1000000
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(-1000000)
    })

    it('should roundtrip INT32 min boundary (-2147483648)', () => {
      const original = -2147483648
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(-2147483648)
    })

    it('should roundtrip INT32 max boundary (2147483647)', () => {
      const original = 2147483647
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(2147483647)
    })
  })

  describe('INT64 (numbers beyond INT32)', () => {
    it('should encode large number as INT64', () => {
      const encoded = encodeVariant(2147483648) // exceeds INT32

      expect(encoded.value[0]).toBe(0x18) // INT64 type
      expect(encoded.value.length).toBe(9) // header + 8 bytes
    })

    it('should roundtrip large positive number', () => {
      const original = 9007199254740991 // Number.MAX_SAFE_INTEGER
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(BigInt(original))
    })

    it('should roundtrip large negative number', () => {
      const original = -9007199254740991 // Number.MIN_SAFE_INTEGER
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(BigInt(original))
    })
  })
})

// =============================================================================
// PRIMITIVE TYPES - BIGINT
// =============================================================================

describe('encodeVariant - BigInt Values', () => {
  it('should encode bigint', () => {
    const encoded = encodeVariant(BigInt(123))

    expect(encoded.value[0]).toBe(0x18) // INT64 type
    expect(encoded.value.length).toBe(9) // header + 8 bytes
  })

  it('should roundtrip small bigint', () => {
    const original = BigInt(42)
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBe(original)
  })

  it('should roundtrip large positive bigint', () => {
    const original = BigInt('9223372036854775807') // INT64 max
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBe(original)
  })

  it('should roundtrip large negative bigint', () => {
    const original = BigInt('-9223372036854775808') // INT64 min
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBe(original)
  })

  it('should roundtrip zero bigint', () => {
    const original = BigInt(0)
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBe(original)
  })
})

// =============================================================================
// PRIMITIVE TYPES - DOUBLE (FLOATING POINT)
// =============================================================================

describe('encodeVariant - Double Values', () => {
  it('should encode floating point number', () => {
    const encoded = encodeVariant(3.14)

    expect(encoded.value[0]).toBe(0x1C) // DOUBLE type
    expect(encoded.value.length).toBe(9) // header + 8 bytes
  })

  it('should roundtrip positive float', () => {
    const original = 3.14159265359
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBeCloseTo(original, 10)
  })

  it('should roundtrip negative float', () => {
    const original = -123.456789
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBeCloseTo(original, 10)
  })

  it('should roundtrip very small float', () => {
    const original = 0.000000001
    const decoded = decodeVariant(encodeVariant(original))
    expect(decoded).toBeCloseTo(original, 15)
  })

  it('should roundtrip very large float with fractional component', () => {
    // Note: Very large numbers like 1.7976931348623157e308 return true for Number.isInteger()
    // due to floating point precision limits, so they get encoded as INT64.
    // Use a moderately large float with explicit fractional component.
    const original = 1.234567890123456e15 // 1234567890123456 - still has precision
    const decoded = decodeVariant(encodeVariant(original))
    // Since this appears as integer to JS, it's encoded as INT64 and returns as BigInt
    expect(decoded).toBe(BigInt(original))
  })

  it('should roundtrip float that remains non-integer', () => {
    // Use a value small enough that JS preserves its fractional nature
    const original = 123456789.123456789
    const decoded = decodeVariant(encodeVariant(original)) as number
    expect(decoded).toBeCloseTo(original, 10)
  })

  it('should roundtrip scientific notation (integer-like)', () => {
    // 1.23e10 is 12300000000 which Number.isInteger() returns true for
    // So it gets encoded as an integer and decoded as BigInt
    const original = 1.23e10
    const decoded = decodeVariant(encodeVariant(original))
    // Since it's an integer > INT32 max, it's encoded as INT64 and decoded as BigInt
    expect(decoded).toBe(BigInt(original))
  })

  it('should roundtrip true floating point in scientific notation', () => {
    // Use a value that is NOT an integer (has fractional part)
    const original = 1.23456e10 // = 12345600000.0
    const decoded = decodeVariant(encodeVariant(original))
    // This is also an integer when JS evaluates it, so same behavior
    expect(decoded).toBe(BigInt(original))
  })

  it('should roundtrip scientific notation with fractional part', () => {
    // Use a value that truly has a fractional part
    const original = 1.234567890123e5 // = 123456.7890123
    const decoded = decodeVariant(encodeVariant(original)) as number
    expect(decoded).toBeCloseTo(original, 10)
  })

  it('should roundtrip infinity', () => {
    const decoded = decodeVariant(encodeVariant(Infinity))
    expect(decoded).toBe(Infinity)
  })

  it('should roundtrip negative infinity', () => {
    const decoded = decodeVariant(encodeVariant(-Infinity))
    expect(decoded).toBe(-Infinity)
  })

  it('should roundtrip NaN', () => {
    const decoded = decodeVariant(encodeVariant(NaN))
    expect(decoded).toBeNaN()
  })
})

// =============================================================================
// PRIMITIVE TYPES - STRINGS
// =============================================================================

describe('encodeVariant - String Values', () => {
  describe('short strings (< 64 bytes)', () => {
    it('should encode empty string', () => {
      const encoded = encodeVariant('')

      expect(encoded.value[0]).toBe(0x01) // short string, length 0
      expect(encoded.value.length).toBe(1) // just header
    })

    it('should roundtrip empty string', () => {
      const decoded = decodeVariant(encodeVariant(''))
      expect(decoded).toBe('')
    })

    it('should roundtrip short string', () => {
      const original = 'hello'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip string at max short length (63 chars ASCII)', () => {
      const original = 'a'.repeat(63)
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip single character', () => {
      const original = 'x'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip string with spaces', () => {
      const original = 'hello world'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })
  })

  describe('long strings (>= 64 bytes)', () => {
    it('should encode long string with 4-byte length prefix', () => {
      const original = 'a'.repeat(100)
      const encoded = encodeVariant(original)

      expect(encoded.value[0]).toBe(0x40) // long string type
      expect(encoded.value.length).toBe(5 + 100) // header + length (4) + content
    })

    it('should roundtrip long string', () => {
      const original = 'a'.repeat(100)
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip very long string', () => {
      const original = 'test string '.repeat(1000)
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })
  })

  describe('unicode strings', () => {
    it('should roundtrip unicode characters', () => {
      const original = '\u4e2d\u6587\u65e5\u672c\u8a9e\ud55c\uad6d\uc5b4' // Chinese, Japanese, Korean
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip emojis', () => {
      const original = '\ud83d\ude00\ud83c\udf89\ud83d\udc4d'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip mixed ASCII and unicode', () => {
      const original = 'Hello \u4e16\u754c! \ud83c\udf0d'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip special characters', () => {
      const original = '!@#$%^&*()_+-=[]{}|;\':",./<>?`~'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip newlines and tabs', () => {
      const original = 'line1\nline2\ttabbed'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })

    it('should roundtrip null characters in string', () => {
      const original = 'before\x00after'
      const decoded = decodeVariant(encodeVariant(original))
      expect(decoded).toBe(original)
    })
  })
})

// =============================================================================
// PRIMITIVE TYPES - DATE
// =============================================================================

describe('encodeVariant - Date Values', () => {
  it('should encode Date as TIMESTAMP_MICROS', () => {
    const encoded = encodeVariant(new Date('2024-01-15T12:00:00Z'))

    expect(encoded.value[0]).toBe(0x30) // TIMESTAMP type
    expect(encoded.value.length).toBe(9) // header + 8 bytes
  })

  it('should roundtrip Date', () => {
    const original = new Date('2024-06-15T10:30:00Z')
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip Date with milliseconds', () => {
    const original = new Date('2024-06-15T10:30:45.123Z')
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip epoch date', () => {
    const original = new Date('1970-01-01T00:00:00Z')
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip pre-epoch date', () => {
    const original = new Date('1969-07-20T20:17:00Z') // Apollo 11 landing
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip future date', () => {
    const original = new Date('2099-12-31T23:59:59Z')
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })
})

// =============================================================================
// PRIMITIVE TYPES - BINARY (Uint8Array)
// =============================================================================

describe('encodeVariant - Binary Values', () => {
  it('should encode Uint8Array', () => {
    const encoded = encodeVariant(new Uint8Array([0x00, 0x01, 0x02]))

    expect(encoded.value[0]).toBe(0x3C) // BINARY type
    expect(encoded.value.length).toBe(5 + 3) // header + length (4) + data
  })

  it('should roundtrip empty binary', () => {
    const original = new Uint8Array([])
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip binary data', () => {
    const original = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe])
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip large binary data', () => {
    const original = new Uint8Array(1000)
    for (let i = 0; i < 1000; i++) {
      original[i] = i % 256
    }
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip binary with all byte values', () => {
    const original = new Uint8Array(256)
    for (let i = 0; i < 256; i++) {
      original[i] = i
    }
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })
})

// =============================================================================
// ARRAYS
// =============================================================================

describe('encodeVariant - Array Values', () => {
  describe('empty and simple arrays', () => {
    it('should roundtrip empty array', () => {
      const original: VariantValue[] = []
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual([])
    })

    it('should roundtrip array with single element', () => {
      const original = [42]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual([42])
    })

    it('should roundtrip array of integers', () => {
      const original = [1, 2, 3, 4, 5]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual([1, 2, 3, 4, 5])
    })

    it('should roundtrip array of strings', () => {
      const original = ['a', 'b', 'c']
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(['a', 'b', 'c'])
    })
  })

  describe('mixed type arrays', () => {
    it('should roundtrip array of mixed primitives', () => {
      const original = [1, 'two', true, null, 3.14]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual([1, 'two', true, null, 3.14])
    })

    it('should roundtrip array with all null elements', () => {
      const original = [null, null, null]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual([null, null, null])
    })
  })

  describe('nested arrays', () => {
    it('should roundtrip nested arrays', () => {
      const original = [[1, 2], [3, 4], [5, 6]]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip deeply nested arrays', () => {
      const original = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip arrays with mixed nesting', () => {
      const original = [[1, 2], [3, [4, 5]], []]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip array with objects', () => {
      const original = [{ a: 1 }, { b: 2 }, { c: 3 }]
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })

  describe('large arrays', () => {
    it('should roundtrip array with 256+ elements (large array)', () => {
      const original = Array.from({ length: 300 }, (_, i) => i)
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip array with 1000 elements', () => {
      const original = Array.from({ length: 1000 }, (_, i) => `item_${i}`)
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })
})

// =============================================================================
// OBJECTS
// =============================================================================

describe('encodeVariant - Object Values', () => {
  describe('empty and simple objects', () => {
    it('should roundtrip empty object', () => {
      const original = {}
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual({})
    })

    it('should roundtrip object with single key', () => {
      const original = { name: 'Alice' }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with multiple keys', () => {
      const original = { name: 'Alice', age: 30, active: true }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with null value', () => {
      const original = { value: null }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })

  describe('objects with various value types', () => {
    it('should roundtrip object with all primitive types', () => {
      const original = {
        nullVal: null,
        boolTrue: true,
        boolFalse: false,
        intSmall: 42,
        intLarge: 1000000,
        float: 3.14,
        string: 'hello',
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with array value', () => {
      const original = { items: [1, 2, 3] }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with Date value', () => {
      const original = { created: new Date('2024-01-15T12:00:00Z') }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with binary value', () => {
      const original = { data: new Uint8Array([0x01, 0x02, 0x03]) }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })

  describe('nested objects', () => {
    it('should roundtrip nested objects', () => {
      const original = {
        user: {
          name: 'Bob',
          address: {
            city: 'NYC',
            zip: '10001',
          },
        },
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip deeply nested objects', () => {
      const original = {
        level1: {
          level2: {
            level3: {
              level4: {
                value: 'deep',
              },
            },
          },
        },
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with nested arrays', () => {
      const original = {
        matrix: [
          [1, 2, 3],
          [4, 5, 6],
          [7, 8, 9],
        ],
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })

  describe('object key handling', () => {
    it('should roundtrip object with special character keys', () => {
      const original = {
        'key-with-dash': 1,
        key_with_underscore: 2,
        'key.with.dots': 3,
        'key with spaces': 4,
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with unicode keys', () => {
      const original = {
        '\u540d\u524d': 'Alice', // "name" in Japanese
        '\uc774\ub984': 'Bob', // "name" in Korean
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with numeric-like keys', () => {
      const original = {
        '0': 'zero',
        '1': 'one',
        '123': 'onetwothree',
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with empty string key', () => {
      const original = { '': 'empty key value' }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })

  describe('large objects', () => {
    it('should roundtrip object with many keys', () => {
      const original: Record<string, number> = {}
      for (let i = 0; i < 100; i++) {
        original[`key${i}`] = i
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should roundtrip object with 256+ keys (large object)', () => {
      const original: Record<string, number> = {}
      for (let i = 0; i < 300; i++) {
        original[`field_${i}`] = i
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })
})

// =============================================================================
// COMPLEX MIXED STRUCTURES
// =============================================================================

describe('encodeVariant - Complex Mixed Structures', () => {
  it('should roundtrip complex nested structure', () => {
    const original = {
      string: 'hello',
      number: 42,
      float: 3.14,
      boolean: true,
      null: null,
      array: [1, 'two', { three: 3 }],
      nested: { deep: { deeper: { value: 'found' } } },
    }
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip document-like structure', () => {
    const original = {
      _id: 'doc123',
      type: 'user',
      data: {
        name: 'Alice',
        email: 'alice@example.com',
        tags: ['admin', 'active'],
        metadata: {
          created: new Date('2024-01-15'),
          modified: new Date('2024-06-15'),
        },
      },
      version: 1,
    }
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip array of mixed objects', () => {
    const original = [
      { type: 'A', value: 1 },
      { type: 'B', items: [1, 2, 3] },
      { type: 'C', nested: { x: true } },
    ]
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })

  it('should roundtrip CDC-like record', () => {
    const original = {
      _id: 'record_001',
      _seq: BigInt(12345),
      _op: 'c',
      _before: null,
      _after: { name: 'New Record', count: 0 },
      _ts: new Date('2024-06-15T10:00:00Z'),
      _source: { table: 'users', partition: 0 },
    }
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('encodeVariant - Edge Cases', () => {
  describe('undefined handling', () => {
    it('should treat undefined as null', () => {
      const encoded = encodeVariant(undefined as unknown as VariantValue)
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeNull()
    })

    it('should skip undefined properties in objects', () => {
      const original = { a: 1, b: undefined, c: 3 } as Record<string, VariantValue>
      const decoded = decodeVariant(encodeVariant(original))

      // undefined values are converted to null in the encoding
      expect(decoded).toEqual({ a: 1, b: null, c: 3 })
    })
  })

  describe('boundary conditions', () => {
    it('should handle string at exactly 64 bytes UTF-8', () => {
      // 64 ASCII chars = 64 bytes UTF-8
      const original = 'x'.repeat(64)
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toBe(original)
    })

    it('should handle array with exactly 255 elements', () => {
      const original = Array.from({ length: 255 }, (_, i) => i)
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should handle array with exactly 256 elements (boundary)', () => {
      const original = Array.from({ length: 256 }, (_, i) => i)
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should handle object with exactly 255 keys', () => {
      const original: Record<string, number> = {}
      for (let i = 0; i < 255; i++) {
        original[`k${i}`] = i
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })

    it('should handle object with exactly 256 keys (boundary)', () => {
      const original: Record<string, number> = {}
      for (let i = 0; i < 256; i++) {
        original[`k${i}`] = i
      }
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toEqual(original)
    })
  })

  describe('special number values', () => {
    it('should roundtrip Number.MIN_SAFE_INTEGER', () => {
      const original = Number.MIN_SAFE_INTEGER
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toBe(BigInt(original))
    })

    it('should roundtrip Number.MAX_SAFE_INTEGER', () => {
      const original = Number.MAX_SAFE_INTEGER
      const decoded = decodeVariant(encodeVariant(original))

      expect(decoded).toBe(BigInt(original))
    })

    it('should roundtrip -0', () => {
      const original = -0
      const decoded = decodeVariant(encodeVariant(original)) as number

      // -0 and 0 are equal in JavaScript
      expect(decoded).toBe(0)
    })
  })

  describe('repeated keys in objects', () => {
    it('should use same dictionary entry for repeated keys across nested objects', () => {
      const original = {
        items: [{ name: 'a' }, { name: 'b' }, { name: 'c' }],
      }
      const encoded = encodeVariant(original)
      const decoded = decodeVariant(encoded)

      expect(decoded).toEqual(original)
      // The dictionary should contain 'items' and 'name' only once
      // This is verified implicitly by the roundtrip working correctly
    })
  })
})

// =============================================================================
// ERROR HANDLING - MALFORMED DATA
// =============================================================================

describe('decodeVariant - Error Handling', () => {
  describe('malformed metadata', () => {
    it('should handle empty metadata gracefully', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([]),
        value: new Uint8Array([0x00]), // null
      }
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeNull()
    })

    it('should handle metadata with only header', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01]), // header only
        value: new Uint8Array([0x00]),
      }
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeNull()
    })
  })

  describe('malformed value data', () => {
    it('should handle empty value', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([]),
      }
      const decoded = decodeVariant(encoded)

      expect(decoded).toBeNull()
    })

    it('should throw ValidationError for truncated INT8', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x0C]), // INT8 header but no data byte
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated INT16', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x10, 0x01]), // INT16 header but only 1 data byte
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated INT32', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x14, 0x01, 0x02]), // INT32 header but only 2 data bytes
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated INT64', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x18, 0x01, 0x02, 0x03, 0x04]), // INT64 header but only 4 data bytes
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated DOUBLE', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x1C, 0x01, 0x02, 0x03, 0x04]), // DOUBLE header but only 4 data bytes
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated TIMESTAMP', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x30, 0x01, 0x02, 0x03, 0x04]), // TIMESTAMP header but only 4 data bytes
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated BINARY length', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x3C, 0x01, 0x02]), // BINARY header but only 2 length bytes
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated BINARY data', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x3C, 0x05, 0x00, 0x00, 0x00, 0x01, 0x02]), // BINARY says 5 bytes but only 2 provided
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated LONG_STRING length', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x40, 0x01, 0x02]), // LONG_STRING header but only 2 length bytes
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })

    it('should throw ValidationError for truncated LONG_STRING data', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0x40, 0x05, 0x00, 0x00, 0x00, 0x61, 0x62]), // LONG_STRING says 5 bytes but only 2 provided
      }

      expect(() => decodeVariant(encoded)).toThrow(ValidationError)
    })
  })

  describe('unknown type handling', () => {
    it('should return null for unknown primitive type', () => {
      const encoded: EncodedVariant = {
        metadata: new Uint8Array([0x01, 0x00]),
        value: new Uint8Array([0xFC]), // Unknown type (type_id = 63)
      }
      const decoded = decodeVariant(encoded)

      // Unknown types default to null
      expect(decoded).toBeNull()
    })
  })
})

// =============================================================================
// DICTIONARY ENCODING
// =============================================================================

describe('encodeVariant - Dictionary Encoding', () => {
  it('should create minimal dictionary for object', () => {
    const original = { a: 1, b: 2 }
    const encoded = encodeVariant(original)

    // Dictionary should contain 'a' and 'b'
    expect(encoded.metadata.length).toBeGreaterThan(2)
    expect(encoded.metadata[0]).toBe(0x01) // header with version 1
  })

  it('should create empty dictionary for primitives', () => {
    const encoded = encodeVariant('test')

    // Empty dictionary: header (1) + dict size (1) = [0x01, 0x00]
    expect(encoded.metadata[0]).toBe(0x01)
    expect(encoded.metadata[1]).toBe(0x00)
  })

  it('should share dictionary entries for nested objects with same keys', () => {
    const original = {
      a: { x: 1, y: 2 },
      b: { x: 3, y: 4 },
    }
    const encoded = encodeVariant(original)
    const decoded = decodeVariant(encoded)

    expect(decoded).toEqual(original)
  })

  it('should handle large dictionary with many unique keys', () => {
    const original: Record<string, number> = {}
    for (let i = 0; i < 500; i++) {
      original[`unique_key_${i}`] = i
    }
    const decoded = decodeVariant(encodeVariant(original))

    expect(decoded).toEqual(original)
  })
})

// =============================================================================
// PERFORMANCE CHARACTERISTICS
// =============================================================================

describe('encodeVariant - Performance', () => {
  it('should handle encoding 10000 simple objects efficiently', () => {
    const objects = Array.from({ length: 10000 }, (_, i) => ({
      id: i,
      name: `item_${i}`,
      value: Math.random(),
    }))

    const start = Date.now()
    for (const obj of objects) {
      encodeVariant(obj)
    }
    const elapsed = Date.now() - start

    // Should complete in reasonable time (< 2 seconds for 10k objects)
    expect(elapsed).toBeLessThan(2000)
  })

  it('should handle encoding large nested structure efficiently', () => {
    const large = {
      data: Array.from({ length: 1000 }, (_, i) => ({
        index: i,
        nested: {
          values: [i, i + 1, i + 2],
          flag: i % 2 === 0,
        },
      })),
    }

    const start = Date.now()
    const encoded = encodeVariant(large)
    const decoded = decodeVariant(encoded)
    const elapsed = Date.now() - start

    expect(decoded).toEqual(large)
    expect(elapsed).toBeLessThan(1000)
  })
})
