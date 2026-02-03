/**
 * E2E Tests: Table Lifecycle
 *
 * Tests full table lifecycle operations using FileSystemStorage for real file I/O:
 * - Table creation
 * - Writing data
 * - Reading data
 * - Schema inference
 * - Table deletion/cleanup
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DeltaTable } from '../../src/delta/index.js'
import { FileSystemStorage, MemoryStorage, createStorage } from '../../src/storage/index.js'
import { parquetReadObjects } from '@dotdo/hyparquet'
import * as fs from 'node:fs/promises'
import * as path from 'node:path'
import * as os from 'node:os'

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a unique test directory for each test
 */
async function createTestDir(): Promise<string> {
  const testDir = path.join(os.tmpdir(), `deltalake-e2e-${Date.now()}-${Math.random().toString(36).slice(2)}`)
  await fs.mkdir(testDir, { recursive: true })
  return testDir
}

/**
 * Clean up test directory after test
 */
async function cleanupTestDir(testDir: string): Promise<void> {
  try {
    await fs.rm(testDir, { recursive: true, force: true })
  } catch {
    // Ignore cleanup errors
  }
}

/**
 * Helper to read a Parquet file and return its rows
 */
async function readParquetFile(filePath: string): Promise<Record<string, unknown>[]> {
  const data = await fs.readFile(filePath)
  const arrayBuffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
  return await parquetReadObjects({ file: arrayBuffer, utf8: true }) as Record<string, unknown>[]
}

/**
 * Check if data is a valid Parquet file (has PAR1 magic bytes)
 */
function isValidParquetFile(data: Uint8Array): boolean {
  if (data.length < 8) return false
  const header = String.fromCharCode(data[0], data[1], data[2], data[3])
  const footer = String.fromCharCode(
    data[data.length - 4],
    data[data.length - 3],
    data[data.length - 2],
    data[data.length - 1]
  )
  return header === 'PAR1' && footer === 'PAR1'
}

// =============================================================================
// TEST DATA TYPES
// =============================================================================

interface User {
  [key: string]: unknown
  _id: string
  name: string
  email: string
  age: number
  active: boolean
  createdAt?: Date
}

interface Product {
  [key: string]: unknown
  _id: string
  name: string
  price: number
  category: string
  tags: string[]
  metadata: {
    weight: number
    dimensions: { width: number; height: number; depth: number }
  }
}

interface Event {
  [key: string]: unknown
  _id: string
  type: string
  timestamp: number
  payload: Record<string, unknown>
}

// =============================================================================
// TABLE CREATION TESTS
// =============================================================================

describe('E2E: Table Creation', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should create a new table on first write', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Verify Delta log was created
    const logFiles = await storage.list('users/_delta_log/')
    expect(logFiles).toContain('users/_delta_log/00000000000000000000.json')

    // Verify version
    const version = await table.version()
    expect(version).toBe(0)
  })

  it('should create table directory structure', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Check directory exists
    const tableDir = path.join(testDir, 'users')
    const stats = await fs.stat(tableDir)
    expect(stats.isDirectory()).toBe(true)

    // Check _delta_log directory exists
    const logDir = path.join(testDir, 'users', '_delta_log')
    const logStats = await fs.stat(logDir)
    expect(logStats.isDirectory()).toBe(true)
  })

  it('should create protocol and metadata actions on first write', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Read the commit file
    const commitPath = path.join(testDir, 'users', '_delta_log', '00000000000000000000.json')
    const commitContent = await fs.readFile(commitPath, 'utf-8')
    const actions = commitContent.trim().split('\n').map(line => JSON.parse(line))

    // Should have protocol action
    const protocol = actions.find((a: any) => 'protocol' in a)
    expect(protocol).toBeDefined()
    expect(protocol.protocol.minReaderVersion).toBeGreaterThanOrEqual(1)
    expect(protocol.protocol.minWriterVersion).toBeGreaterThanOrEqual(1)

    // Should have metadata action
    const metadata = actions.find((a: any) => 'metaData' in a)
    expect(metadata).toBeDefined()
    expect(metadata.metaData.format.provider).toBe('parquet')
  })

  it('should create Parquet data files', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    const commit = await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Find the add action
    const addAction = commit.actions.find((a: any) => 'add' in a) as any
    expect(addAction).toBeDefined()

    // Verify the Parquet file exists
    const parquetPath = path.join(testDir, addAction.add.path)
    const parquetData = await fs.readFile(parquetPath)
    expect(isValidParquetFile(new Uint8Array(parquetData))).toBe(true)
  })

  it('should handle nested table paths', async () => {
    const table = new DeltaTable<User>(storage, 'db/schema/users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Verify nested path created
    const logFiles = await storage.list('db/schema/users/_delta_log/')
    expect(logFiles.length).toBeGreaterThan(0)
  })

  it('should create table using createStorage helper', async () => {
    // Test createStorage with filesystem type
    const autoStorage = createStorage({ type: 'filesystem', path: testDir })
    const table = new DeltaTable<User>(autoStorage, 'auto-table')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    const version = await table.version()
    expect(version).toBe(0)
  })
})

// =============================================================================
// WRITE AND READ CYCLE TESTS
// =============================================================================

describe('E2E: Write/Read Cycle', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should write and read back simple data', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    const users: User[] = [
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: true },
    ]

    await table.write(users)

    const results = await table.query()

    expect(results).toHaveLength(3)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])
  })

  it('should preserve data types through write/read cycle', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    const user: User = {
      _id: '1',
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      active: true,
    }

    await table.write([user])
    const results = await table.query()

    expect(results).toHaveLength(1)
    expect(typeof results[0]._id).toBe('string')
    expect(typeof results[0].name).toBe('string')
    expect(typeof results[0].age).toBe('number')
    expect(typeof results[0].active).toBe('boolean')
  })

  it('should handle multiple writes and read all data', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    // First write
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Second write
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
    ])

    // Third write
    await table.write([
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: true },
    ])

    const results = await table.query()

    expect(results).toHaveLength(3)

    // Verify versions
    const version = await table.version()
    expect(version).toBe(2)
  })

  it('should write and read complex nested data', async () => {
    const table = new DeltaTable<Product>(storage, 'products')

    const product: Product = {
      _id: 'p1',
      name: 'Laptop',
      price: 999.99,
      category: 'electronics',
      tags: ['portable', 'computer', 'work'],
      metadata: {
        weight: 2.5,
        dimensions: { width: 35, height: 2, depth: 25 },
      },
    }

    await table.write([product])
    const results = await table.query()

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Laptop')
    expect(results[0].tags).toEqual(['portable', 'computer', 'work'])
    expect(results[0].metadata.weight).toBe(2.5)
    expect(results[0].metadata.dimensions).toEqual({ width: 35, height: 2, depth: 25 })
  })

  it('should handle null values', async () => {
    interface NullableUser {
      _id: string
      name: string
      email: string | null
      age: number | null
    }

    const table = new DeltaTable<NullableUser>(storage, 'nullable_users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30 },
      { _id: '2', name: 'Bob', email: null, age: 25 },
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: null },
    ])

    const results = await table.query()

    expect(results).toHaveLength(3)
    expect(results.find(r => r._id === '1')?.email).toBe('alice@example.com')
    expect(results.find(r => r._id === '2')?.email).toBeNull()
    expect(results.find(r => r._id === '3')?.age).toBeNull()
  })

  it('should handle large datasets', async () => {
    const table = new DeltaTable<{ _id: string; value: number }>(storage, 'large_data')

    // Generate 1000 rows
    const rows = Array.from({ length: 1000 }, (_, i) => ({
      _id: `id-${i}`,
      value: i * 2,
    }))

    await table.write(rows)

    const results = await table.query()

    expect(results).toHaveLength(1000)
  })

  it('should read data from reopened table', async () => {
    // Write with one table instance
    const table1 = new DeltaTable<User>(storage, 'users')
    await table1.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
    ])

    // Read with new table instance (simulates restart)
    const table2 = new DeltaTable<User>(storage, 'users')
    const results = await table2.query()

    expect(results).toHaveLength(2)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
  })
})

// =============================================================================
// SCHEMA INFERENCE TESTS
// =============================================================================

describe('E2E: Schema Inference', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should infer schema from first write', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Read back and verify types are preserved
    const results = await table.query()

    expect(typeof results[0]._id).toBe('string')
    expect(typeof results[0].name).toBe('string')
    expect(typeof results[0].age).toBe('number')
    expect(typeof results[0].active).toBe('boolean')
  })

  it('should handle various data types', async () => {
    interface MixedTypes {
      _id: string
      intValue: number
      floatValue: number
      boolValue: boolean
      stringValue: string
      bigintValue: bigint
    }

    const table = new DeltaTable<MixedTypes>(storage, 'mixed_types')

    await table.write([
      {
        _id: '1',
        intValue: 42,
        floatValue: 3.14159,
        boolValue: true,
        stringValue: 'hello world',
        bigintValue: BigInt('9007199254740993'),
      },
    ])

    const results = await table.query()

    expect(results[0].intValue).toBe(42)
    expect(results[0].floatValue).toBeCloseTo(3.14159)
    expect(results[0].boolValue).toBe(true)
    expect(results[0].stringValue).toBe('hello world')
    expect(results[0].bigintValue).toBe(BigInt('9007199254740993'))
  })

  it('should handle arrays', async () => {
    interface ArrayDoc {
      _id: string
      numbers: number[]
      strings: string[]
      nested: { items: number[] }[]
    }

    const table = new DeltaTable<ArrayDoc>(storage, 'arrays')

    await table.write([
      {
        _id: '1',
        numbers: [1, 2, 3, 4, 5],
        strings: ['a', 'b', 'c'],
        nested: [{ items: [1, 2] }, { items: [3, 4] }],
      },
    ])

    const results = await table.query()

    expect(results[0].numbers).toEqual([1, 2, 3, 4, 5])
    expect(results[0].strings).toEqual(['a', 'b', 'c'])
    expect(results[0].nested).toEqual([{ items: [1, 2] }, { items: [3, 4] }])
  })

  it('should handle deeply nested objects', async () => {
    interface DeepNested {
      _id: string
      level1: {
        level2: {
          level3: {
            level4: {
              value: string
            }
          }
        }
      }
    }

    const table = new DeltaTable<DeepNested>(storage, 'deep_nested')

    const doc: DeepNested = {
      _id: '1',
      level1: {
        level2: {
          level3: {
            level4: {
              value: 'deep value',
            },
          },
        },
      },
    }

    await table.write([doc])
    const results = await table.query()

    expect(results[0].level1.level2.level3.level4.value).toBe('deep value')
  })

  it('should handle schema evolution with new fields', async () => {
    interface UserV1 {
      _id: string
      name: string
    }

    interface UserV2 {
      _id: string
      name: string
      email: string
    }

    const table = new DeltaTable<UserV1 | UserV2>(storage, 'users')

    // First write with minimal fields
    await table.write([
      { _id: '1', name: 'Alice' },
    ])

    // Second write with additional field (schema evolution)
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com' },
    ])

    const results = await table.query()

    expect(results).toHaveLength(2)
    expect(results.find(r => r._id === '1')?.name).toBe('Alice')
    expect((results.find(r => r._id === '2') as UserV2)?.email).toBe('bob@example.com')
  })
})

// =============================================================================
// TABLE DELETE/CLEANUP TESTS
// =============================================================================

describe('E2E: Table Operations', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should delete rows from table', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: true },
    ])

    // Delete Bob
    await table.delete({ _id: '2' })

    const results = await table.query()

    expect(results).toHaveLength(2)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
  })

  it('should update rows in table', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Update Alice's age
    await table.update({ _id: '1' }, { age: 31 })

    const results = await table.query()

    expect(results).toHaveLength(1)
    expect(results[0].age).toBe(31)
  })

  it('should handle sequential CRUD operations', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    // Create
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])

    // Update
    await table.update({ _id: '1' }, { age: 31 })

    // Create more
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
    ])

    // Delete
    await table.delete({ _id: '1' })

    // Final query
    const results = await table.query()

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Bob')
  })

  it('should verify delta log tracks all operations', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    // Multiple operations
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
    ])
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
    ])
    await table.delete({ _id: '1' })

    // Check delta log has all versions
    const logFiles = await storage.list('users/_delta_log/')
    const jsonFiles = logFiles.filter(f => f.endsWith('.json'))

    expect(jsonFiles).toContain('users/_delta_log/00000000000000000000.json')
    expect(jsonFiles).toContain('users/_delta_log/00000000000000000001.json')
    expect(jsonFiles).toContain('users/_delta_log/00000000000000000002.json')
  })
})

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

describe('E2E: Concurrent Access', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should handle concurrent reads safely', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false },
    ])

    // Concurrent reads
    const results = await Promise.all([
      table.query(),
      table.query(),
      table.query(),
      table.query(),
      table.query(),
    ])

    // All reads should return same data
    for (const result of results) {
      expect(result).toHaveLength(2)
    }
  })

  it('should handle sequential writes correctly', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    // Sequential writes
    await table.write([{ _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true }])
    await table.write([{ _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false }])
    await table.write([{ _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: true }])

    const results = await table.query()
    expect(results).toHaveLength(3)

    const version = await table.version()
    expect(version).toBe(2)
  })
})
