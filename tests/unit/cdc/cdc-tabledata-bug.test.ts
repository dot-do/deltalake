/**
 * Test to reproduce the P0 CDC bug:
 * CDCDeltaTableImpl uses in-memory tableData that doesn't sync with actual storage
 *
 * Issue: deltalake-5qb4
 * Location: /Users/nathanclevenger/projects/deltalake/src/cdc/index.ts
 *
 * Problem: The CDCDeltaTableImpl class maintains an in-memory `tableData: T[] = []`
 * array that is never populated from actual storage. The update(), deleteRows(),
 * and merge() methods operate on this empty/stale array when using a new table instance.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { createCDCDeltaTable } from '../../../src/cdc/index'
import { MemoryStorage } from '../../../src/storage/index'

interface TestRow {
  id: string
  name: string
  value: number
}

describe('CDCDeltaTableImpl tableData sync bug', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  describe('update() across table instances', () => {
    it('should find and update rows written by a previous table instance', async () => {
      // Create table, enable CDC, and write data
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])

      // Create a new instance (simulates restart/reopen)
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Try to update the row that was written by table1
      const commit = await table2.update({ id: '1' }, { value: 200 })

      // Check CDC records
      const reader = table2.getCDCReader()
      const changes = await reader.readByVersion(BigInt(commit.version), BigInt(commit.version))

      const preimage = changes.find(c => c._change_type === 'update_preimage')
      const postimage = changes.find(c => c._change_type === 'update_postimage')

      // These assertions will fail if tableData is not synced from storage
      expect(preimage).toBeDefined()
      expect(postimage).toBeDefined()
      expect(preimage?.data.value).toBe(100)
      expect(postimage?.data.value).toBe(200)
    })
  })

  describe('deleteRows() across table instances', () => {
    it('should find and delete rows written by a previous table instance', async () => {
      // Create table, enable CDC, and write data
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])

      // Create a new instance (simulates restart/reopen)
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Try to delete the row that was written by table1
      const commit = await table2.deleteRows({ id: '1' })

      // Check CDC records
      const reader = table2.getCDCReader()
      const changes = await reader.readByVersion(BigInt(commit.version), BigInt(commit.version))

      const deleteRecord = changes.find(c => c._change_type === 'delete')

      // This assertion will fail if tableData is not synced from storage
      expect(deleteRecord).toBeDefined()
      expect(deleteRecord?.data.id).toBe('1')
      expect(deleteRecord?.data.value).toBe(100)
    })
  })

  describe('merge() across table instances', () => {
    it('should find and merge rows written by a previous table instance', async () => {
      // Create table, enable CDC, and write data
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])

      // Create a new instance (simulates restart/reopen)
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Try to merge - should update existing row
      const commit = await table2.merge(
        [{ id: '1', name: 'Alice Updated', value: 150 }],
        (existing, incoming) => existing.id === incoming.id,
        (existing, incoming) => incoming,
        (incoming) => incoming
      )

      // Check CDC records
      const reader = table2.getCDCReader()
      const changes = await reader.readByVersion(BigInt(commit.version), BigInt(commit.version))

      const preimage = changes.find(c => c._change_type === 'update_preimage')
      const postimage = changes.find(c => c._change_type === 'update_postimage')

      // These assertions will fail if tableData is not synced from storage
      expect(preimage).toBeDefined()
      expect(postimage).toBeDefined()
      expect(preimage?.data.name).toBe('Alice')
      expect(preimage?.data.value).toBe(100)
      expect(postimage?.data.name).toBe('Alice Updated')
      expect(postimage?.data.value).toBe(150)
    })

    it('should insert new rows when no match found after table reopen', async () => {
      // Create table, enable CDC, and write data
      const table1 = createCDCDeltaTable<TestRow>(storage, 'test_table')
      await table1.setCDCEnabled(true)
      await table1.write([{ id: '1', name: 'Alice', value: 100 }])

      // Create a new instance (simulates restart/reopen)
      const table2 = createCDCDeltaTable<TestRow>(storage, 'test_table')

      // Try to merge - should insert new row since id='2' doesn't exist
      const commit = await table2.merge(
        [{ id: '2', name: 'Bob', value: 200 }],
        (existing, incoming) => existing.id === incoming.id,
        (existing, incoming) => incoming,
        (incoming) => incoming
      )

      // Check CDC records
      const reader = table2.getCDCReader()
      const changes = await reader.readByVersion(BigInt(commit.version), BigInt(commit.version))

      const insertRecord = changes.find(c => c._change_type === 'insert')

      expect(insertRecord).toBeDefined()
      expect(insertRecord?.data.id).toBe('2')
      expect(insertRecord?.data.name).toBe('Bob')
    })
  })
})
