/**
 * CDC Delta Table Integration Tests
 *
 * Comprehensive test suite for setCDCEnabled() and isCDCEnabled() on DeltaTable.
 * Tests the integration between CDC (Change Data Capture) and Delta Lake tables.
 *
 * This covers:
 * - Enabling CDC stores flag in metadata
 * - Disabling CDC removes flag
 * - _change_data/ directory creation
 * - CDC state querying (isCDCEnabled())
 * - Metadata persistence across table reopen
 * - Integration with MetadataAction in Delta log
 *
 * [RED] TDD Phase - These tests define expected behavior for CDC Delta integration.
 * Tests should fail initially until implementation is complete.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  createCDCDeltaTable,
  CDCDeltaTable,
  DeltaCDCRecord,
} from '../../../src/cdc/index'
import { DeltaTable, MetadataAction } from '../../../src/delta/index'
import { MemoryStorage, StorageBackend } from '../../../src/storage/index'

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Sample row type for testing
 */
interface TestRow {
  id: string
  name: string
  value: number
}

/**
 * Helper to read and parse Delta log commit file
 */
async function readDeltaCommit(storage: StorageBackend, tablePath: string, version: number): Promise<any[]> {
  const versionStr = version.toString().padStart(20, '0')
  const commitPath = `${tablePath}/_delta_log/${versionStr}.json`

  try {
    const data = await storage.read(commitPath)
    const content = new TextDecoder().decode(data)
    return content.trim().split('\n').map(line => JSON.parse(line))
  } catch {
    return []
  }
}

/**
 * Helper to read CDC config file
 */
async function readCDCConfig(storage: StorageBackend, tablePath: string): Promise<{ enabled: boolean } | null> {
  const configPath = `${tablePath}/_cdc_config.json`

  try {
    const data = await storage.read(configPath)
    return JSON.parse(new TextDecoder().decode(data))
  } catch {
    return null
  }
}

/**
 * Helper to find MetadataAction in commit actions
 */
function findMetadataAction(actions: any[]): MetadataAction | undefined {
  return actions.find(a => 'metaData' in a) as MetadataAction | undefined
}

// =============================================================================
// SET CDC ENABLED - BASIC FUNCTIONALITY TESTS
// =============================================================================

describe('setCDCEnabled() Basic Functionality', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('enabling CDC', () => {
    it('should enable CDC on a table', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)

      expect(await table.isCDCEnabled()).toBe(true)
    })

    it('should store enabled flag in CDC config file', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)

      const config = await readCDCConfig(storage, 'test_table')
      expect(config).not.toBeNull()
      expect(config?.enabled).toBe(true)
    })

    it('should enable CDC before any writes', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)

      // Should be enabled even without writes
      expect(await table.isCDCEnabled()).toBe(true)
    })

    it('should enable CDC after existing writes', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Write before enabling CDC
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      await table.setCDCEnabled(true)

      expect(await table.isCDCEnabled()).toBe(true)
    })

    it('should be idempotent - calling enable multiple times should not fail', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      await table.setCDCEnabled(true)
      await table.setCDCEnabled(true)

      expect(await table.isCDCEnabled()).toBe(true)
    })
  })

  describe('disabling CDC', () => {
    it('should disable CDC on a table', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      await table.setCDCEnabled(false)

      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should update CDC config file when disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      await table.setCDCEnabled(false)

      const config = await readCDCConfig(storage, 'test_table')
      expect(config).not.toBeNull()
      expect(config?.enabled).toBe(false)
    })

    it('should disable CDC that was previously enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      await table.setCDCEnabled(false)

      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should be idempotent - calling disable multiple times should not fail', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(false)
      await table.setCDCEnabled(false)

      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should disable CDC on a table that never had CDC enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Explicitly disable without ever enabling
      await table.setCDCEnabled(false)

      expect(await table.isCDCEnabled()).toBe(false)
    })
  })

  describe('toggling CDC', () => {
    it('should handle enable -> disable -> enable sequence', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      expect(await table.isCDCEnabled()).toBe(true)

      await table.setCDCEnabled(false)
      expect(await table.isCDCEnabled()).toBe(false)

      await table.setCDCEnabled(true)
      expect(await table.isCDCEnabled()).toBe(true)
    })

    it('should handle rapid toggling without data loss', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      for (let i = 0; i < 10; i++) {
        await table.setCDCEnabled(i % 2 === 0)
      }

      // Final state should be false (9 is odd)
      expect(await table.isCDCEnabled()).toBe(false)
    })
  })
})

// =============================================================================
// IS CDC ENABLED - STATE QUERYING TESTS
// =============================================================================

describe('isCDCEnabled() State Querying', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('default state', () => {
    it('should return false by default for new tables', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should return false for tables without CDC config', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Create table with some data but no CDC config
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // New table instance should read false
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      expect(await table2.isCDCEnabled()).toBe(false)
    })
  })

  describe('reading persisted state', () => {
    it('should read enabled state from config file', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // Create new table instance - should read from storage
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      expect(await table2.isCDCEnabled()).toBe(true)
    })

    it('should read disabled state from config file', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)
      await table.setCDCEnabled(false)

      // Create new table instance
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      expect(await table2.isCDCEnabled()).toBe(false)
    })

    it('should cache the enabled state after first read', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // Multiple calls should return consistent results
      expect(await table.isCDCEnabled()).toBe(true)
      expect(await table.isCDCEnabled()).toBe(true)
      expect(await table.isCDCEnabled()).toBe(true)
    })
  })

  describe('concurrent access', () => {
    it('should handle concurrent isCDCEnabled calls', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // Concurrent reads
      const results = await Promise.all([
        table.isCDCEnabled(),
        table.isCDCEnabled(),
        table.isCDCEnabled(),
      ])

      expect(results).toEqual([true, true, true])
    })
  })
})

// =============================================================================
// _change_data/ DIRECTORY TESTS
// =============================================================================

describe('_change_data/ Directory Management', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('directory creation', () => {
    it('should create _change_data/ directory when CDC is enabled and data is written', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBeGreaterThan(0)
    })

    it('should not create _change_data/ directory when CDC is disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      // CDC disabled by default

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBe(0)
    })

    it('should create _change_data/ in the correct table path', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'data/tables/my_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('data/tables/my_table/_change_data/')
      expect(files.length).toBeGreaterThan(0)
    })

    it('should not create _change_data/ before any writes', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // Enable CDC but don't write
      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBe(0)
    })
  })

  describe('directory contents', () => {
    it('should store CDC files in Parquet format', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      expect(files.every(f => f.endsWith('.parquet'))).toBe(true)
    })

    it('should create date-partitioned CDC directories', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')
      // Should have date partition like: _change_data/date=2024-01-15/
      const hasDatePartition = files.some(f => /date=\d{4}-\d{2}-\d{2}/.test(f))
      expect(hasDatePartition).toBe(true)
    })

    it('should create new CDC files for each commit', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      const files = await storage.list('test_table/_change_data/')
      // Should have at least 2 CDC files
      expect(files.length).toBeGreaterThanOrEqual(2)
    })

    it('should stop creating CDC files after CDC is disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      const filesAfterFirstWrite = await storage.list('test_table/_change_data/')

      await table.setCDCEnabled(false)
      await table.write([{ id: '2', name: 'Bob', value: 200 }])
      const filesAfterSecondWrite = await storage.list('test_table/_change_data/')

      // Should not have additional CDC files
      expect(filesAfterSecondWrite.length).toBe(filesAfterFirstWrite.length)
    })
  })

  describe('directory with multiple tables', () => {
    it('should create separate _change_data/ directories for each table', async () => {
      const table1 = createCDCDeltaTable<TestRow>(storage, 'table1')
      const table2 = createCDCDeltaTable<TestRow>(storage, 'table2')

      await table1.setCDCEnabled(true)
      await table2.setCDCEnabled(true)

      await table1.write([{ id: '1', name: 'Alice', value: 100 }])
      await table2.write([{ id: '2', name: 'Bob', value: 200 }])

      const table1Files = await storage.list('table1/_change_data/')
      const table2Files = await storage.list('table2/_change_data/')

      expect(table1Files.length).toBeGreaterThan(0)
      expect(table2Files.length).toBeGreaterThan(0)

      // Ensure no overlap
      expect(table1Files.every(f => f.startsWith('table1/'))).toBe(true)
      expect(table2Files.every(f => f.startsWith('table2/'))).toBe(true)
    })
  })
})

// =============================================================================
// METADATA PERSISTENCE ACROSS TABLE REOPEN TESTS
// =============================================================================

describe('Metadata Persistence Across Table Reopen', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('persisting enabled state', () => {
    it('should persist enabled state across table instance recreation', async () => {
      // Create and enable CDC
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])

      // Create new instance (simulates restart/reopen)
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')

      expect(await table2.isCDCEnabled()).toBe(true)
    })

    it('should persist disabled state across table instance recreation', async () => {
      // Create, enable, then disable CDC
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])
      await table1.setCDCEnabled(false)

      // Create new instance
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')

      expect(await table2.isCDCEnabled()).toBe(false)
    })

    it('should continue generating CDC after reopen if enabled', async () => {
      // Create and enable CDC
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])

      const filesAfterFirstInstance = await storage.list('test_table/_change_data/')

      // Create new instance
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table2.write([{ id: '2', name: 'Bob', value: 200 }])

      const filesAfterSecondInstance = await storage.list('test_table/_change_data/')

      // Should have more CDC files
      expect(filesAfterSecondInstance.length).toBeGreaterThan(filesAfterFirstInstance.length)
    })

    it('should not generate CDC after reopen if disabled', async () => {
      // Create and disable CDC
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])
      await table1.setCDCEnabled(false)

      const filesAfterFirstInstance = await storage.list('test_table/_change_data/')

      // Create new instance
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table2.write([{ id: '2', name: 'Bob', value: 200 }])

      const filesAfterSecondInstance = await storage.list('test_table/_change_data/')

      // Should have same number of CDC files
      expect(filesAfterSecondInstance.length).toBe(filesAfterFirstInstance.length)
    })
  })

  describe('config file persistence', () => {
    it('should store config in _cdc_config.json file', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      const configExists = await storage.exists('test_table/_cdc_config.json')
      expect(configExists).toBe(true)
    })

    it('should update _cdc_config.json when CDC state changes', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.setCDCEnabled(true)
      let config = await readCDCConfig(storage, 'test_table')
      expect(config?.enabled).toBe(true)

      await table.setCDCEnabled(false)
      config = await readCDCConfig(storage, 'test_table')
      expect(config?.enabled).toBe(false)
    })

    it('should store config in correct table path', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'data/tables/events')
      await table.setCDCEnabled(true)

      const configExists = await storage.exists('data/tables/events/_cdc_config.json')
      expect(configExists).toBe(true)
    })
  })

  describe('handling corrupted config', () => {
    it('should default to false if config file is corrupted', async () => {
      // Manually write corrupted config
      await storage.write(
        'test_table/_cdc_config.json',
        new TextEncoder().encode('not valid json')
      )

      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Should default to false on parse error
      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should handle missing config file gracefully', async () => {
      // Create table and write data without CDC config
      await storage.write(
        'test_table/_delta_log/00000000000000000000.json',
        new TextEncoder().encode('{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}')
      )

      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Should default to false
      expect(await table.isCDCEnabled()).toBe(false)
    })

    it('should recover from corrupted config by setting new value', async () => {
      // Manually write corrupted config
      await storage.write(
        'test_table/_cdc_config.json',
        new TextEncoder().encode('corrupted')
      )

      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // Should now have valid config
      const config = await readCDCConfig(storage, 'test_table')
      expect(config?.enabled).toBe(true)
    })
  })
})

// =============================================================================
// INTEGRATION WITH METADATAACTION IN DELTA LOG TESTS
// =============================================================================

describe('Integration with MetadataAction in Delta Log', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('delta.enableChangeDataFeed configuration', () => {
    it('should include delta.enableChangeDataFeed=true in metadata when CDC is enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // Read version 0 commit
      const actions = await readDeltaCommit(storage, 'test_table', 0)
      const metadata = findMetadataAction(actions)

      expect(metadata).toBeDefined()
      expect(metadata?.metaData.configuration?.['delta.enableChangeDataFeed']).toBe('true')
    })

    it('should not include delta.enableChangeDataFeed when CDC is disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      // CDC disabled by default

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const actions = await readDeltaCommit(storage, 'test_table', 0)
      const metadata = findMetadataAction(actions)

      // Should not have the configuration or it should be undefined
      expect(metadata?.metaData.configuration?.['delta.enableChangeDataFeed']).toBeUndefined()
    })

    it('should have metadata configuration as an object', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const actions = await readDeltaCommit(storage, 'test_table', 0)
      const metadata = findMetadataAction(actions)

      expect(metadata?.metaData.configuration).toBeDefined()
      expect(typeof metadata?.metaData.configuration).toBe('object')
    })
  })

  describe('metadata persistence in log', () => {
    it('should persist CDC configuration across table snapshot reads', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // First write (version 0 with metadata)
      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // Second write (version 1, metadata should still be available)
      await table.write([{ id: '2', name: 'Bob', value: 200 }])

      // Read version 0 to verify metadata
      const actions = await readDeltaCommit(storage, 'test_table', 0)
      const metadata = findMetadataAction(actions)

      expect(metadata?.metaData.configuration?.['delta.enableChangeDataFeed']).toBe('true')
    })
  })

  describe('configuration format', () => {
    it('should store delta.enableChangeDataFeed as string "true"', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const actions = await readDeltaCommit(storage, 'test_table', 0)
      const metadata = findMetadataAction(actions)

      // Delta Lake expects string values in configuration
      expect(typeof metadata?.metaData.configuration?.['delta.enableChangeDataFeed']).toBe('string')
      expect(metadata?.metaData.configuration?.['delta.enableChangeDataFeed']).toBe('true')
    })
  })
})

// =============================================================================
// CDC DATA GENERATION TESTS
// =============================================================================

describe('CDC Data Generation with setCDCEnabled', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('INSERT operations with CDC', () => {
    it('should generate CDC records for insert when enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(1)
      expect(changes[0]._change_type).toBe('insert')
    })

    it('should not generate CDC records for insert when disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      // CDC disabled by default

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 0n)

      expect(changes.length).toBe(0)
    })
  })

  describe('UPDATE operations with CDC', () => {
    it('should generate update_preimage and update_postimage when enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const preimage = changes.find(c => c._change_type === 'update_preimage')
      const postimage = changes.find(c => c._change_type === 'update_postimage')

      expect(preimage).toBeDefined()
      expect(postimage).toBeDefined()
      expect(preimage?.data.value).toBe(100)
      expect(postimage?.data.value).toBe(200)
    })

    it('should not generate update CDC records when disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.update({ id: '1' }, { value: 200 })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 1n)

      expect(changes.length).toBe(0)
    })
  })

  describe('DELETE operations with CDC', () => {
    it('should generate delete CDC record when enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.deleteRows({ id: '1' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      expect(changes.length).toBe(1)
      expect(changes[0]._change_type).toBe('delete')
    })

    it('should not generate delete CDC record when disabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      await table.write([{ id: '1', name: 'Alice', value: 100 }])
      await table.deleteRows({ id: '1' })

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 1n)

      expect(changes.length).toBe(0)
    })
  })

  describe('MERGE operations with CDC', () => {
    it('should generate mixed CDC records for merge when enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      await table.merge(
        [
          { id: '1', name: 'Alice Updated', value: 150 }, // Update
          { id: '2', name: 'Bob', value: 200 },           // Insert
        ],
        (existing, incoming) => existing.id === incoming.id,
        (existing, incoming) => incoming,
        (incoming) => incoming
      )

      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(1n, 1n)

      const inserts = changes.filter(c => c._change_type === 'insert')
      const updates = changes.filter(
        c => c._change_type === 'update_preimage' || c._change_type === 'update_postimage'
      )

      expect(inserts.length).toBe(1)
      expect(updates.length).toBe(2) // preimage + postimage
    })
  })
})

// =============================================================================
// CDC WITH VERSION GAPS TESTS
// =============================================================================

describe('CDC with Version Gaps (Enable/Disable/Enable)', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('should handle gaps in CDC when CDC was temporarily disabled', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

    // v0: CDC enabled
    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Alice', value: 100 }])

    // v1: CDC disabled
    await table.setCDCEnabled(false)
    await table.write([{ id: '2', name: 'Bob', value: 200 }])

    // v2: CDC re-enabled
    await table.setCDCEnabled(true)
    await table.write([{ id: '3', name: 'Charlie', value: 300 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 2n)

    // Should have CDC for v0 and v2, but not v1
    const versions = [...new Set(changes.map(c => c._commit_version))]

    expect(versions).toContain(0n)
    expect(versions).not.toContain(1n)
    expect(versions).toContain(2n)
  })

  it('should correctly count CDC records across enable/disable gaps', async () => {
    const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

    await table.setCDCEnabled(true)
    await table.write([{ id: '1', name: 'Row1', value: 1 }])
    await table.write([{ id: '2', name: 'Row2', value: 2 }])

    await table.setCDCEnabled(false)
    await table.write([{ id: '3', name: 'Row3', value: 3 }])
    await table.write([{ id: '4', name: 'Row4', value: 4 }])

    await table.setCDCEnabled(true)
    await table.write([{ id: '5', name: 'Row5', value: 5 }])

    const reader = table.getCDCReader()
    const changes = await reader.readByVersion(0n, 4n)

    // Should have 3 CDC records (v0, v1, v4), not 5
    expect(changes.length).toBe(3)
  })
})

// =============================================================================
// EDGE CASES AND ERROR HANDLING TESTS
// =============================================================================

describe('Edge Cases and Error Handling', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('empty tables', () => {
    it('should handle setCDCEnabled on empty table', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'empty_table')

      // Should not throw
      await table.setCDCEnabled(true)
      expect(await table.isCDCEnabled()).toBe(true)
    })

    it('should handle isCDCEnabled on empty table', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'empty_table')

      // Should not throw
      expect(await table.isCDCEnabled()).toBe(false)
    })
  })

  describe('special characters in table paths', () => {
    it('should handle table paths with dashes', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'my-table-name')
      await table.setCDCEnabled(true)

      expect(await table.isCDCEnabled()).toBe(true)

      const config = await readCDCConfig(storage, 'my-table-name')
      expect(config?.enabled).toBe(true)
    })

    it('should handle nested table paths', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'db/schema/table')
      await table.setCDCEnabled(true)

      expect(await table.isCDCEnabled()).toBe(true)

      const configExists = await storage.exists('db/schema/table/_cdc_config.json')
      expect(configExists).toBe(true)
    })
  })

  describe('concurrent operations', () => {
    it('should handle concurrent setCDCEnabled calls', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Concurrent enable/disable (last write wins)
      await Promise.all([
        table.setCDCEnabled(true),
        table.setCDCEnabled(false),
        table.setCDCEnabled(true),
      ])

      // State should be consistent (though we don't know which one won)
      const state = await table.isCDCEnabled()
      expect(typeof state).toBe('boolean')
    })

    it('should handle concurrent writes with CDC enabled', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      // Concurrent writes
      await Promise.all([
        table.write([{ id: '1', name: 'Alice', value: 100 }]),
        table.write([{ id: '2', name: 'Bob', value: 200 }]),
        table.write([{ id: '3', name: 'Charlie', value: 300 }]),
      ])

      // All CDC records should be present
      const reader = table.getCDCReader()
      const changes = await reader.readByVersion(0n, 10n)

      expect(changes.length).toBe(3)
    })
  })

  describe('storage errors', () => {
    it('should throw on storage write failure when enabling CDC', async () => {
      // Create a mock storage that fails on write
      const failingStorage: StorageBackend = {
        read: () => Promise.reject(new Error('Read error')),
        write: () => Promise.reject(new Error('Write error')),
        list: () => Promise.resolve([]),
        delete: () => Promise.resolve(),
        exists: () => Promise.resolve(false),
        stat: () => Promise.resolve(null),
        readRange: () => Promise.reject(new Error('Read error')),
      }

      const table = createCDCDeltaTable<TestRow>(failingStorage, 'test_table')

      await expect(table.setCDCEnabled(true)).rejects.toThrow()
    })
  })
})

// =============================================================================
// COMPATIBILITY TESTS
// =============================================================================

describe('Delta Lake Compatibility', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('standard Delta Lake CDC format', () => {
    it('should produce CDC files compatible with Delta Lake readers', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // CDC files should be in _change_data/ directory
      const files = await storage.list('test_table/_change_data/')
      expect(files.length).toBeGreaterThan(0)

      // Files should have standard naming convention
      const cdcFile = files[0]
      expect(cdcFile).toMatch(/cdc-\d{20}\.parquet/)
    })

    it('should use 20-digit zero-padded version numbers in CDC file names', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      const files = await storage.list('test_table/_change_data/')

      files.forEach(file => {
        const match = file.match(/cdc-(\d{20})\.parquet/)
        if (match) {
          expect(match[1].length).toBe(20)
        }
      })
    })
  })

  describe('table property format', () => {
    it('should store CDC configuration as table property', async () => {
      const table = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table.setCDCEnabled(true)

      await table.write([{ id: '1', name: 'Alice', value: 100 }])

      // Read the commit and verify configuration format
      const actions = await readDeltaCommit(storage, 'test_table', 0)
      const metadata = findMetadataAction(actions)

      // Should have configuration object
      expect(metadata?.metaData.configuration).toBeDefined()

      // delta.enableChangeDataFeed should be a string "true" per Delta Lake spec
      expect(metadata?.metaData.configuration?.['delta.enableChangeDataFeed']).toBe('true')
    })
  })
})
