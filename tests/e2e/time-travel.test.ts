/**
 * E2E Tests: Time Travel Queries
 *
 * Tests time travel functionality (version-based queries):
 * - Query at specific versions
 * - Snapshot consistency
 * - CDC (Change Data Capture) event generation
 * - Version history traversal
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DeltaTable, type DeltaSnapshot } from '../../src/delta/index.js'
import { FileSystemStorage } from '../../src/storage/index.js'
import {
  createCDCDeltaTable,
  type CDCDeltaTable,
  type DeltaCDCRecord,
} from '../../src/cdc/index.js'
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
  const testDir = path.join(os.tmpdir(), `deltalake-e2e-timetravel-${Date.now()}-${Math.random().toString(36).slice(2)}`)
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
 * Helper to wait for a specified time (useful for timestamp-based tests)
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
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
  score: number
}

interface Product {
  [key: string]: unknown
  _id: string
  name: string
  price: number
  quantity: number
}

interface Event {
  [key: string]: unknown
  _id: string
  type: string
  timestamp: number
  data: Record<string, unknown>
}

// =============================================================================
// TIME TRAVEL QUERY TESTS
// =============================================================================

describe('E2E: Time Travel - Version-Based Queries', () => {
  let testDir: string
  let storage: FileSystemStorage
  let table: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    table = new DeltaTable<User>(storage, 'users')
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should query at specific version 0', async () => {
    // Version 0: Initial write
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Version 1: Add more users
    await table.write([
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, score: 91 },
    ])

    // Query at version 0 should only return the first 2 records
    const resultsV0 = await table.query({}, { version: 0 })

    expect(resultsV0).toHaveLength(2)
    expect(resultsV0.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
  })

  it('should query at version 1', async () => {
    // Version 0
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Version 1
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Version 2
    await table.write([
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, score: 91 },
    ])

    const resultsV1 = await table.query({}, { version: 1 })

    expect(resultsV1).toHaveLength(2)
    expect(resultsV1.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
  })

  it('should query latest version by default', async () => {
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])
    await table.write([
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, score: 91 },
    ])

    const results = await table.query()

    expect(results).toHaveLength(3)
  })

  it('should see update effects at correct versions', async () => {
    // Version 0: Initial write
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Version 1: Update Alice
    await table.update({ _id: '1' }, { age: 31, score: 90 })

    // Version 0 should have original values
    const resultsV0 = await table.query({}, { version: 0 })
    expect(resultsV0[0].age).toBe(30)
    expect(resultsV0[0].score).toBe(85)

    // Version 1 should have updated values
    const resultsV1 = await table.query({}, { version: 1 })
    expect(resultsV1[0].age).toBe(31)
    expect(resultsV1[0].score).toBe(90)
  })

  it('should see delete effects at correct versions', async () => {
    // Version 0: Initial write
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Version 1: Delete Bob
    await table.delete({ _id: '2' })

    // Version 0 should have both users
    const resultsV0 = await table.query({}, { version: 0 })
    expect(resultsV0).toHaveLength(2)

    // Version 1 should only have Alice
    const resultsV1 = await table.query({}, { version: 1 })
    expect(resultsV1).toHaveLength(1)
    expect(resultsV1[0].name).toBe('Alice')
  })

  it('should handle filters at specific version', async () => {
    // Version 0: Write users with mixed active status
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: false, score: 72 },
    ])

    // Version 1: Update Bob to active
    await table.update({ _id: '2' }, { active: true })

    // Version 0: Only Alice is active
    const activeV0 = await table.query({ active: true }, { version: 0 })
    expect(activeV0).toHaveLength(1)
    expect(activeV0[0].name).toBe('Alice')

    // Version 1: Both are active
    const activeV1 = await table.query({ active: true }, { version: 1 })
    expect(activeV1).toHaveLength(2)
  })
})

// =============================================================================
// SNAPSHOT CONSISTENCY TESTS
// =============================================================================

describe('E2E: Time Travel - Snapshot Consistency', () => {
  let testDir: string
  let storage: FileSystemStorage
  let table: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    table = new DeltaTable<User>(storage, 'users')
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should return consistent results within same snapshot', async () => {
    // Write initial data
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Get snapshot at version 0
    const snapshot = await table.snapshot(0)

    // Write more data (creates version 1)
    await table.write([
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, score: 91 },
    ])

    // Multiple queries using the same snapshot should return consistent results
    const results1 = await table.query({}, { snapshot })
    const results2 = await table.query({ active: true }, { snapshot })
    const results3 = await table.query({}, { snapshot })

    // All queries should see only version 0 data
    expect(results1).toHaveLength(2)
    expect(results3).toHaveLength(2)
    expect(results2).toHaveLength(2)
  })

  it('should see new data after snapshot refresh', async () => {
    // Write initial data
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    const snapshot1 = await table.snapshot()
    expect(snapshot1.version).toBe(0)

    // Write more data
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Old snapshot still sees 1 record
    const resultsOld = await table.query({}, { snapshot: snapshot1 })
    expect(resultsOld).toHaveLength(1)

    // Get new snapshot
    const snapshot2 = await table.snapshot()
    expect(snapshot2.version).toBe(1)

    // New snapshot sees both records
    const resultsNew = await table.query({}, { snapshot: snapshot2 })
    expect(resultsNew).toHaveLength(2)
  })

  it('should maintain snapshot isolation during concurrent operations', async () => {
    // Write initial data
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    const snapshotBefore = await table.snapshot()

    // Simulate concurrent operations
    await Promise.all([
      table.write([{ _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 }]),
      table.query({}, { snapshot: snapshotBefore }),
      table.query({}, { snapshot: snapshotBefore }),
    ])

    // Original snapshot should still see only 1 record
    const resultsOriginal = await table.query({}, { snapshot: snapshotBefore })
    expect(resultsOriginal).toHaveLength(1)

    // Latest snapshot should see all records
    const snapshotAfter = await table.snapshot()
    const resultsLatest = await table.query({}, { snapshot: snapshotAfter })
    expect(resultsLatest).toHaveLength(2)
  })

  it('should get snapshot files correctly', async () => {
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    const snapshot = await table.snapshot()

    // Snapshot should have version and files
    expect(snapshot.version).toBe(1)
    expect(snapshot.files.length).toBeGreaterThanOrEqual(2)
  })
})

// =============================================================================
// VERSION HISTORY TESTS
// =============================================================================

describe('E2E: Time Travel - Version History', () => {
  let testDir: string
  let storage: FileSystemStorage
  let table: DeltaTable<User>

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
    table = new DeltaTable<User>(storage, 'users')
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should track version numbers correctly', async () => {
    const commit0 = await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])
    expect(commit0.version).toBe(0)

    const commit1 = await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])
    expect(commit1.version).toBe(1)

    const commit2 = await table.update({ _id: '1' }, { age: 31 })
    expect(commit2.version).toBe(2)

    const commit3 = await table.delete({ _id: '2' })
    expect(commit3.version).toBe(3)

    const currentVersion = await table.version()
    expect(currentVersion).toBe(3)
  })

  it('should traverse all versions', async () => {
    // Create multiple versions
    await table.write([{ _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 }])
    await table.write([{ _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 }])
    await table.update({ _id: '1' }, { age: 31 })
    await table.delete({ _id: '2' })
    await table.write([{ _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, score: 91 }])

    // Query each version
    const v0 = await table.query({}, { version: 0 })
    const v1 = await table.query({}, { version: 1 })
    const v2 = await table.query({}, { version: 2 })
    const v3 = await table.query({}, { version: 3 })
    const v4 = await table.query({}, { version: 4 })

    expect(v0).toHaveLength(1)
    expect(v1).toHaveLength(2)
    expect(v2).toHaveLength(2) // Update doesn't change count
    expect(v3).toHaveLength(1) // Delete removes one
    expect(v4).toHaveLength(2) // New write adds one
  })

  it('should maintain correct data at each version point', async () => {
    // Version 0: Alice age 30
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Version 1: Alice age 31
    await table.update({ _id: '1' }, { age: 31 })

    // Version 2: Alice age 32
    await table.update({ _id: '1' }, { age: 32 })

    // Version 3: Alice age 33
    await table.update({ _id: '1' }, { age: 33 })

    // Verify each version has correct age
    const v0 = await table.query({}, { version: 0 })
    expect(v0[0].age).toBe(30)

    const v1 = await table.query({}, { version: 1 })
    expect(v1[0].age).toBe(31)

    const v2 = await table.query({}, { version: 2 })
    expect(v2[0].age).toBe(32)

    const v3 = await table.query({}, { version: 3 })
    expect(v3[0].age).toBe(33)
  })

  it('should handle reopened table with version history', async () => {
    // Write with first table instance
    const table1 = new DeltaTable<User>(storage, 'users')
    await table1.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])
    await table1.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Create new instance (simulates restart)
    const table2 = new DeltaTable<User>(storage, 'users')

    // Should be able to query at any version
    const v0 = await table2.query({}, { version: 0 })
    expect(v0).toHaveLength(1)

    const v1 = await table2.query({}, { version: 1 })
    expect(v1).toHaveLength(2)

    const version = await table2.version()
    expect(version).toBe(1)
  })
})

// =============================================================================
// CDC EVENT GENERATION TESTS
// =============================================================================

describe('E2E: CDC Event Generation', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should generate insert CDC records', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes).toHaveLength(2)
    expect(changes.every(c => c._change_type === 'insert')).toBe(true)
  })

  it('should generate update CDC records with pre and post images', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    await table.update({ _id: '1' }, { age: 31, score: 90 })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const preimage = changes.find(c => c._change_type === 'update_preimage')
    const postimage = changes.find(c => c._change_type === 'update_postimage')

    expect(preimage).toBeDefined()
    expect(postimage).toBeDefined()
    expect(preimage?.data.age).toBe(30)
    expect(preimage?.data.score).toBe(85)
    expect(postimage?.data.age).toBe(31)
    expect(postimage?.data.score).toBe(90)
  })

  it('should generate delete CDC records', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    await table.deleteRows({ _id: '1' })

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    expect(changes).toHaveLength(1)
    expect(changes[0]._change_type).toBe('delete')
    expect(changes[0].data.name).toBe('Alice')
  })

  it('should read CDC changes by version range', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    // Version 0: Insert
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Version 1: Update
    await table.update({ _id: '1' }, { age: 31 })

    // Version 2: Insert
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    const reader = table.getCDCReader()

    // Read all changes
    const allChanges = await reader.readByVersion(0n, 2n)
    expect(allChanges.length).toBeGreaterThanOrEqual(4) // 1 insert + 2 update (pre+post) + 1 insert

    // Read only version 0
    const v0Changes = await reader.readByVersion(0n, 0n)
    expect(v0Changes).toHaveLength(1)
    expect(v0Changes[0]._change_type).toBe('insert')

    // Read version 1 only
    const v1Changes = await reader.readByVersion(1n, 1n)
    expect(v1Changes).toHaveLength(2) // preimage + postimage
  })

  it('should read CDC changes by timestamp range', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    const beforeFirst = new Date()
    await sleep(10)

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    const afterFirst = new Date()
    await sleep(10)

    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    const afterSecond = new Date()

    const reader = table.getCDCReader()

    // Read all changes
    const allChanges = await reader.readByTimestamp(beforeFirst, afterSecond)
    expect(allChanges).toHaveLength(2)

    // Read only first write
    const firstOnly = await reader.readByTimestamp(beforeFirst, afterFirst)
    expect(firstOnly).toHaveLength(1)
    expect(firstOnly[0].data.name).toBe('Alice')
  })

  it('should not generate CDC when disabled', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    // CDC disabled by default

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 0n)

    expect(changes).toHaveLength(0)
  })

  it('should handle CDC enable/disable toggle', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')

    // Enable CDC
    await table.setCDCEnabled(true)
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Disable CDC
    await table.setCDCEnabled(false)
    await table.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    // Re-enable CDC
    await table.setCDCEnabled(true)
    await table.write([
      { _id: '3', name: 'Charlie', email: 'charlie@example.com', age: 35, active: false, score: 91 },
    ])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 2n)

    // Should have CDC for v0 and v2, but not v1
    const versions = [...new Set(changes.map(c => c._commit_version))]

    expect(versions).toContain(0n)
    expect(versions).not.toContain(1n)
    expect(versions).toContain(2n)
  })

  it('should support CDC subscription', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    const receivedChanges: DeltaCDCRecord<User>[] = []

    const reader = table.getCDCReader()
    const unsubscribe = reader.subscribe(async (record) => {
      receivedChanges.push(record)
    })

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    await table.update({ _id: '1' }, { age: 31 })

    // Give time for subscription to process
    await sleep(50)

    unsubscribe()

    // Should have received all changes via subscription
    expect(receivedChanges.length).toBeGreaterThanOrEqual(3) // 1 insert + 2 update
  })

  it('should create _change_data directory with proper file naming', async () => {
    const table = createCDCDeltaTable<User>(storage, 'users')
    await table.setCDCEnabled(true)

    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Check _change_data directory exists and has files
    const cdcFiles = await storage.list('users/_change_data/')
    expect(cdcFiles.length).toBeGreaterThan(0)

    // Files should have proper naming convention
    const hasProperNaming = cdcFiles.some(f => /cdc-\d{20}\.parquet/.test(f))
    expect(hasProperNaming).toBe(true)
  })

  it('should persist CDC enabled state across table reopen', async () => {
    // First instance - enable CDC
    const table1 = createCDCDeltaTable<User>(storage, 'users')
    await table1.setCDCEnabled(true)
    await table1.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    // Second instance - should still have CDC enabled
    const table2 = createCDCDeltaTable<User>(storage, 'users')
    expect(await table2.isCDCEnabled()).toBe(true)

    await table2.write([
      { _id: '2', name: 'Bob', email: 'bob@example.com', age: 25, active: true, score: 72 },
    ])

    const reader = table2.getCDCReader()
    const changes = await reader.readByVersion(0n, 1n)

    expect(changes.length).toBe(2) // Both inserts should have CDC
  })
})

// =============================================================================
// MERGE OPERATIONS WITH CDC
// =============================================================================

describe('E2E: CDC with Merge Operations', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should generate mixed CDC records for merge operation', async () => {
    const table = createCDCDeltaTable<Product>(storage, 'products')
    await table.setCDCEnabled(true)

    // Initial data
    await table.write([
      { _id: 'p1', name: 'Laptop', price: 999.99, quantity: 50 },
      { _id: 'p2', name: 'Mouse', price: 29.99, quantity: 100 },
    ])

    // Merge: Update p1, Insert p3
    await table.merge(
      [
        { _id: 'p1', name: 'Laptop Pro', price: 1299.99, quantity: 40 },
        { _id: 'p3', name: 'Keyboard', price: 79.99, quantity: 75 },
      ],
      (existing, incoming) => existing._id === incoming._id,
      (existing, incoming) => incoming,
      (incoming) => incoming
    )

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const inserts = changes.filter(c => c._change_type === 'insert')
    const updates = changes.filter(c => c._change_type === 'update_preimage' || c._change_type === 'update_postimage')

    expect(inserts).toHaveLength(1)
    expect(inserts[0].data.name).toBe('Keyboard')

    expect(updates).toHaveLength(2) // preimage + postimage
    const preimage = updates.find(u => u._change_type === 'update_preimage')
    const postimage = updates.find(u => u._change_type === 'update_postimage')

    expect(preimage?.data.name).toBe('Laptop')
    expect(preimage?.data.price).toBe(999.99)
    expect(postimage?.data.name).toBe('Laptop Pro')
    expect(postimage?.data.price).toBe(1299.99)
  })

  it('should generate delete CDC for merge with null return', async () => {
    const table = createCDCDeltaTable<Product>(storage, 'products')
    await table.setCDCEnabled(true)

    // Initial data
    await table.write([
      { _id: 'p1', name: 'Old Product', price: 9.99, quantity: 0 },
      { _id: 'p2', name: 'Active Product', price: 49.99, quantity: 50 },
    ])

    // Merge: Delete p1 (quantity 0), Update p2
    await table.merge(
      [
        { _id: 'p1', name: 'Old Product', price: 9.99, quantity: 0 },
        { _id: 'p2', name: 'Active Product Updated', price: 59.99, quantity: 45 },
      ],
      (existing, incoming) => existing._id === incoming._id,
      (existing, incoming) => {
        if (incoming.quantity === 0) return null // Delete if quantity is 0
        return incoming
      },
      (incoming) => incoming
    )

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(1n, 1n)

    const deletes = changes.filter(c => c._change_type === 'delete')
    const updates = changes.filter(c => c._change_type === 'update_postimage')

    expect(deletes).toHaveLength(1)
    expect(deletes[0].data.name).toBe('Old Product')

    expect(updates).toHaveLength(1)
    expect(updates[0].data.price).toBe(59.99)
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('E2E: Time Travel Edge Cases', () => {
  let testDir: string
  let storage: FileSystemStorage

  beforeEach(async () => {
    testDir = await createTestDir()
    storage = new FileSystemStorage({ path: testDir })
  })

  afterEach(async () => {
    await cleanupTestDir(testDir)
  })

  it('should handle query at version 0 of empty-then-populated table', async () => {
    const table = new DeltaTable<User>(storage, 'users')

    // First write creates version 0
    await table.write([
      { _id: '1', name: 'Alice', email: 'alice@example.com', age: 30, active: true, score: 85 },
    ])

    const results = await table.query({}, { version: 0 })

    expect(results).toHaveLength(1)
  })

  it('should handle many rapid writes', async () => {
    const table = new DeltaTable<{ _id: string; seq: number }>(storage, 'sequence')

    // Many sequential writes
    for (let i = 0; i < 10; i++) {
      await table.write([{ _id: `item-${i}`, seq: i }])
    }

    // Query at each version
    for (let v = 0; v < 10; v++) {
      const results = await table.query({}, { version: v })
      expect(results).toHaveLength(v + 1)
    }
  })

  it('should handle large version numbers', async () => {
    const table = new DeltaTable<{ _id: string; value: number }>(storage, 'large_versions')

    // Create many versions
    for (let i = 0; i < 20; i++) {
      await table.write([{ _id: `item-${i}`, value: i }])
    }

    const version = await table.version()
    expect(version).toBe(19)

    // Query at various versions
    const resultsV5 = await table.query({}, { version: 5 })
    expect(resultsV5).toHaveLength(6)

    const resultsV15 = await table.query({}, { version: 15 })
    expect(resultsV15).toHaveLength(16)
  })
})
